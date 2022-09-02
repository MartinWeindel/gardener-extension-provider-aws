/*
 * // Copyright (c) 2022 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //      http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 *
 */

package infraflow

import (
	"context"
	"fmt"
	"strings"
	"time"

	awsapi "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws"
	awsapiv1alpha "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws/v1alpha1"
	awsclient "github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	"github.com/gardener/gardener-extension-provider-aws/pkg/controller/infrastructure/infraflow/state"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/gardener/gardener/pkg/utils/flow"
	"github.com/go-logr/logr"
)

const (
	TagKeyName            = "Name"
	TagKeyClusterTemplate = "kubernetes.io/cluster/%s"
	TagKeyRolePublicELB   = "kubernetes.io/role/elb"
	TagKeyRolePrivateELB  = "kubernetes.io/role/internal-elb"
	TagValueCluster       = "1"
	TagValueUse           = "use"

	IdentifierVPC                              = "VPC"
	IdentifierDHCPOptions                      = "DHCPOptions"
	IdentifierDefaultSecurityGroup             = "DefaultSecurityGroup"
	IdentifierInternetGateway                  = "InternetGateway"
	IdentifierMainRouteTable                   = "MainRouteTable"
	IdentifierNodesSecurityGroup               = "NodesSecurityGroup"
	IdentifierZoneSubnetWorkers                = "SubnetWorkers"
	IdentifierZoneSubnetPublic                 = "SubnetPublicUtility"
	IdentifierZoneSubnetPrivate                = "SubnetPrivateUtility"
	IdentifierZoneSuffix                       = "Suffix"
	IdentifierZoneNATGWElasticIP               = "NATGatewayElasticIP"
	IdentifierZoneNATGateway                   = "NATGateway"
	IdentifierZoneRouteTable                   = "ZoneRouteTable"
	IdentifierZoneSubnetPublicRouteTableAssoc  = "SubnetPublicRouteTableAssoc"
	IdentifierZoneSubnetPrivateRouteTableAssoc = "SubnetPrivateRouteTableAssoc"
	IdentifierZoneSubnetWorkersRouteTableAssoc = "SubnetWorkersRouteTableAssoc"
	IdentifierIAMRole                          = "IAMRole"
	IdentifierIAMInstanceProfile               = "IAMInstanceProfile"
	IdentifierIAMRolePolicy                    = "IAMRolePolicy"
	IdentifierKeyPair                          = "KeyPair"

	ChildIdVPCEndpoints = "VPCEndpoints"
	ChildIdZones        = "Zones"

	ObjectMainRouteTable = "MainRouteTable"
	ObjectZoneRouteTable = "ZoneRouteTable"

	MarkerMigratedFromTerraform                   = "MigratedFromTerraform"
	MarkerLoadBalancersAndSecurityGroupsDestroyed = "LoadBalancersAndSecurityGroupsDestroyed"
)

type ReconcileContext struct {
	infra      *extensionsv1alpha1.Infrastructure
	config     *awsapi.InfrastructureConfig
	logger     logr.Logger
	client     awsclient.Interface
	updater    awsclient.Updater
	commonTags awsclient.Tags

	state                   state.Whiteboard
	flowStatePersistor      FlowStatePersistor
	lastPersistedGeneration int64
	lastPersistedAt         time.Time
}

type FlowStatePersistor func(ctx context.Context, flowState *awsapiv1alpha.FlowState) error

func NewReconcileContext(logger logr.Logger, awsClient awsclient.Interface,
	infra *extensionsv1alpha1.Infrastructure, config *awsapi.InfrastructureConfig,
	oldFlowState *awsapi.FlowState, persistor FlowStatePersistor) (*ReconcileContext, error) {
	rc := &ReconcileContext{
		infra:              infra,
		config:             config,
		logger:             logger,
		client:             awsClient,
		updater:            awsclient.NewUpdater(awsClient, config.IgnoreTags),
		state:              state.NewWhiteboard(),
		flowStatePersistor: persistor,
	}
	rc.commonTags = awsclient.Tags{
		rc.tagKeyCluster(): TagValueCluster,
		TagKeyName:         infra.Namespace,
	}

	if oldFlowState != nil && oldFlowState.Version != FlowStateVersion1 {
		return nil, fmt.Errorf("unknown flow state version %s", oldFlowState.Version)
	}

	rc.fillStateFromFlowState(oldFlowState)
	if config != nil && config.Networks.VPC.ID != nil {
		rc.state.SetPtr(IdentifierVPC, config.Networks.VPC.ID)
	}
	return rc, nil
}

func (rc *ReconcileContext) GetInfrastructureConfig() *awsapi.InfrastructureConfig {
	return rc.config
}

func (rc *ReconcileContext) commonTagsWithSuffix(suffix string) awsclient.Tags {
	tags := rc.commonTags.Clone()
	tags[TagKeyName] = fmt.Sprintf("%s-%s", rc.infra.Namespace, suffix)
	return tags
}

func (rc *ReconcileContext) tagKeyCluster() string {
	return fmt.Sprintf(TagKeyClusterTemplate, rc.infra.Namespace)
}

func (rc *ReconcileContext) clusterTags() awsclient.Tags {
	tags := awsclient.Tags{}
	tags[rc.tagKeyCluster()] = TagValueCluster
	return tags
}

func (rc *ReconcileContext) UpdatedFlowState() *awsapiv1alpha.FlowState {
	return &awsapiv1alpha.FlowState{
		Version: FlowStateVersion1,
		Data:    rc.state.ExportAsFlatMap(),
	}
}

func (rc *ReconcileContext) PersistFlowState(ctx context.Context, force bool) error {
	if !force && rc.lastPersistedAt.Add(10*time.Second).After(time.Now()) {
		return nil
	}
	currentGeneration := rc.state.Generation()
	if rc.lastPersistedGeneration == currentGeneration {
		return nil
	}
	if rc.flowStatePersistor != nil {
		newFlowState := rc.UpdatedFlowState()
		if err := rc.flowStatePersistor(ctx, newFlowState); err != nil {
			return err
		}
	}
	rc.lastPersistedGeneration = currentGeneration
	rc.lastPersistedAt = time.Now()
	return nil
}

func (rc *ReconcileContext) PersistingState(fn flow.TaskFn) flow.TaskFn {
	return func(ctx context.Context) error {
		err := fn(ctx)
		if perr := rc.PersistFlowState(ctx, false); perr != nil {
			rc.logger.Error(perr, "persisting state failed")
		}
		return err
	}
}

func (rc *ReconcileContext) fillStateFromFlowState(flowState *awsapi.FlowState) {
	if flowState != nil {
		rc.state.ImportFromFlatMap(flowState.Data)
	}
}

func (rc *ReconcileContext) vpcEndpointServiceNamePrefix() string {
	return fmt.Sprintf("com.amazonaws.%s.", rc.infra.Spec.Region)
}

func (rc *ReconcileContext) extractVpcEndpointName(item *awsclient.VpcEndpoint) string {
	return strings.TrimPrefix(item.ServiceName, rc.vpcEndpointServiceNamePrefix())
}
