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

	awsapi "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws"
	awsapiv1alpha "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws/v1alpha1"
	awsclient "github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	"github.com/gardener/gardener-extension-provider-aws/pkg/controller/infrastructure/infraflow/state"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
)

const (
	TagKeyName            = "Name"
	TagKeyClusterTemplate = "kubernetes.io/cluster/%s"
	TagKeyRoleELB         = "kubernetes.io/role/elb"
	TagValueCluster       = "1"
	TagValueUse           = "use"

	IdentiferVPC                  = "VPC"
	IdentiferDHCPOptions          = "DHCPOptions"
	IdentiferDefaultSecurityGroup = "DefaultSecurityGroup"
	IdentiferInternetGateway      = "InternetGateway"
	IdentiferMainRouteTable       = "MainRouteTable"
	IdentiferNodesSecurityGroup   = "NodesSecurityGroup"
	IdentifierSubnet              = "Subnet"
	IdentifierSubnetSuffix        = "SubnetSuffix"

	ChildIdVPCEndpoints = "VPCEndpoints"
	ChildIdSubnets      = "Subnets"

	MarkerMigratedFromTerraform                   = "MigratedFromTerraform"
	MarkerLoadBalancersAndSecurityGroupsDestroyed = "LoadBalancersAndSecurityGroupsDestroyed"

	Separator = "/"
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
		rc.state.SetIDPtr(IdentiferVPC, config.Networks.VPC.ID)
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
	newFlowState := &awsapiv1alpha.FlowState{
		Version: FlowStateVersion1,
	}

	newFlowState.ResourceIdentifiers = rc.state.GetIDMap()
	fillResourceIdentifiersFromState(newFlowState.ResourceIdentifiers, "", rc.state)

	markers := rc.state.GetCompletedTaskMarkers()
	if len(markers) > 0 {
		newFlowState.CompletedTaskMarkers = markers
	}

	return newFlowState
}

func (rc *ReconcileContext) PersistFlowState(ctx context.Context) error {
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
	return nil
}

func fillResourceIdentifiersFromState(resourceIdentifiers map[string]string, parentPrefix string, whiteboard state.Whiteboard) {
	for _, childKey := range whiteboard.GetChildrenKeys() {
		child := whiteboard.GetChild(childKey)
		childPrefix := parentPrefix + childKey + Separator
		for k, v := range child.GetIDMap() {
			key := childPrefix + k
			resourceIdentifiers[key] = v
		}
		fillResourceIdentifiersFromState(resourceIdentifiers, childPrefix, child)
	}
}

func (rc *ReconcileContext) fillStateFromFlowState(flowState *awsapi.FlowState) {
	if flowState != nil {
		for key, value := range flowState.ResourceIdentifiers {
			parts := strings.Split(key, Separator)
			w := rc.state
			for i := 0; i < len(parts)-1; i++ {
				w = w.GetChild(parts[i])
			}
			w.SetID(parts[len(parts)-1], value)
		}

		for k, v := range flowState.CompletedTaskMarkers {
			rc.state.MarkTaskCompleted(k, v)
		}
	}
}

func (rc *ReconcileContext) vpcEndpointServiceNamePrefix() string {
	return fmt.Sprintf("com.amazonaws.%s.", rc.infra.Spec.Region)
}

func (rc *ReconcileContext) extractVpcEndpointName(item *awsclient.VpcEndpoint) string {
	return strings.TrimPrefix(item.ServiceName, rc.vpcEndpointServiceNamePrefix())
}
