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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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
	NameIAMRole                                = "IAMRoleName"
	NameIAMInstanceProfile                     = "IAMInstanceProfileName"
	NameIAMRolePolicy                          = "IAMRolePolicyName"
	NameKeyPair                                = "KeyPair"
	ARNIAMRole                                 = "IAMRoleARN"

	ChildIdVPCEndpoints = "VPCEndpoints"
	ChildIdZones        = "Zones"

	ObjectMainRouteTable = "MainRouteTable"
	ObjectZoneRouteTable = "ZoneRouteTable"

	MarkerMigratedFromTerraform                   = "MigratedFromTerraform"
	MarkerTerraformCleanedUp                      = "TerraformCleanedUp"
	MarkerLoadBalancersAndSecurityGroupsDestroyed = "LoadBalancersAndSecurityGroupsDestroyed"
)

type FlowContext struct {
	infra      *extensionsv1alpha1.Infrastructure
	config     *awsapi.InfrastructureConfig
	log        logr.Logger
	client     awsclient.Interface
	updater    awsclient.Updater
	commonTags awsclient.Tags

	state                   state.Whiteboard
	flowStatePersistor      FlowStatePersistor
	lastPersistedGeneration int64
	lastPersistedAt         time.Time
}

type FlowStatePersistor func(ctx context.Context, flowState *awsapiv1alpha.FlowState) error

func NewFlowContext(logger logr.Logger, awsClient awsclient.Interface,
	infra *extensionsv1alpha1.Infrastructure, config *awsapi.InfrastructureConfig,
	oldFlowState *awsapi.FlowState, persistor FlowStatePersistor) (*FlowContext, error) {
	rc := &FlowContext{
		infra:              infra,
		config:             config,
		log:                logger,
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

func (c *FlowContext) GetInfrastructureConfig() *awsapi.InfrastructureConfig {
	return c.config
}

func (c *FlowContext) hasVPC() bool {
	return c.state.Has(IdentifierVPC)
}

func (c *FlowContext) logFromContext(ctx context.Context) logr.Logger {
	if log, err := logr.FromContext(ctx); err != nil {
		return c.log
	} else {
		return log
	}
}

func (c *FlowContext) addTask(g *flow.Graph, name string, fn flow.TaskFn, dependencies ...flow.TaskIDer) flow.TaskIDer {
	task := flow.Task{
		Name: name,
		Fn:   c.wrapTaskFn(g.Name(), name, fn),
	}

	if len(dependencies) > 0 {
		task.Dependencies = flow.NewTaskIDs(dependencies...)
	}

	return g.Add(task)
}

func (c *FlowContext) wrapTaskFn(flowName, taskName string, fn flow.TaskFn) flow.TaskFn {
	return func(ctx context.Context) error {
		taskCtx := logf.IntoContext(ctx, c.log.WithValues("flow", flowName, "task", taskName))
		err := fn(taskCtx)
		if err != nil {
			err = fmt.Errorf("failed to %s: %w", taskName, err)
		}
		if perr := c.PersistState(taskCtx, false); perr != nil {
			if err != nil {
				c.log.Error(perr, "persisting state failed")
			} else {
				err = perr
			}
		}
		return err
	}
}

func (c *FlowContext) commonTagsWithSuffix(suffix string) awsclient.Tags {
	tags := c.commonTags.Clone()
	tags[TagKeyName] = fmt.Sprintf("%s-%s", c.infra.Namespace, suffix)
	return tags
}

func (c *FlowContext) tagKeyCluster() string {
	return fmt.Sprintf(TagKeyClusterTemplate, c.infra.Namespace)
}

func (c *FlowContext) clusterTags() awsclient.Tags {
	tags := awsclient.Tags{}
	tags[c.tagKeyCluster()] = TagValueCluster
	return tags
}

func (c *FlowContext) UpdatedFlowState() *awsapiv1alpha.FlowState {
	return &awsapiv1alpha.FlowState{
		Version: FlowStateVersion1,
		Data:    c.state.ExportAsFlatMap(),
	}
}

func (c *FlowContext) PersistState(ctx context.Context, force bool) error {
	if !force && c.lastPersistedAt.Add(10*time.Second).After(time.Now()) {
		return nil
	}
	currentGeneration := c.state.Generation()
	if c.lastPersistedGeneration == currentGeneration {
		return nil
	}
	if c.flowStatePersistor != nil {
		newFlowState := c.UpdatedFlowState()
		if err := c.flowStatePersistor(ctx, newFlowState); err != nil {
			return err
		}
	}
	c.lastPersistedGeneration = currentGeneration
	c.lastPersistedAt = time.Now()
	return nil
}

func (c *FlowContext) fillStateFromFlowState(flowState *awsapi.FlowState) {
	if flowState != nil {
		c.state.ImportFromFlatMap(flowState.Data)
	}
}

func (c *FlowContext) vpcEndpointServiceNamePrefix() string {
	return fmt.Sprintf("com.amazonaws.%s.", c.infra.Spec.Region)
}

func (c *FlowContext) extractVpcEndpointName(item *awsclient.VpcEndpoint) string {
	return strings.TrimPrefix(item.ServiceName, c.vpcEndpointServiceNamePrefix())
}
