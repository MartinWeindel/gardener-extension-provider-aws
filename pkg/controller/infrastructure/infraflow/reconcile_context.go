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
	TagKeyName                            = "Name"
	TagKeyClusterTemplate                 = "kubernetes.io/cluster/%s"
	TaskKeyLoadBalancersAndSecurityGroups = "LoadBalancersAndSecurityGroups"

	TagValueCluster = "1"

	IdentiferVPC                  = "VPC"
	IdentiferDHCPOptions          = "DHCPOptions"
	IdentiferDefaultSecurityGroup = "DefaultSecurityGroup"
	IdentiferInternetGateway      = "InternetGateway"

	ChildIdVPCEndpoints = "VPCEndpoints"
)

type ReconcileContext struct {
	infra      *extensionsv1alpha1.Infrastructure
	config     *awsapi.InfrastructureConfig
	logger     logr.Logger
	client     awsclient.Interface
	updater    awsclient.Updater
	commonTags awsclient.Tags

	state state.Whiteboard
}

func NewReconcileContext(logger logr.Logger, awsClient awsclient.Interface,
	infra *extensionsv1alpha1.Infrastructure, config *awsapi.InfrastructureConfig,
	oldFlowState *awsapi.FlowState) (*ReconcileContext, error) {
	rc := &ReconcileContext{
		infra:   infra,
		config:  config,
		logger:  logger,
		client:  awsClient,
		updater: awsclient.NewUpdater(awsClient, config.IgnoreTags),
		state:   state.NewWhiteboard(),
	}
	rc.commonTags = awsclient.Tags{
		rc.tagKeyCluster(): TagValueCluster,
		TagKeyName:         infra.Namespace,
	}

	if oldFlowState != nil && oldFlowState.Version != FlowStateVersion1 {
		return nil, fmt.Errorf("unknown flow state version %s", oldFlowState.Version)
	}

	if config != nil && config.Networks.VPC.ID != nil {
		rc.state.SetIDPtr(IdentiferVPC, config.Networks.VPC.ID)
	} else if oldFlowState != nil {
		rc.state.SetIDPtr(IdentiferVPC, oldFlowState.VpcId)
		rc.state.SetIDPtr(IdentiferDHCPOptions, oldFlowState.DhcpOptionsId)
		rc.state.SetIDPtr(IdentiferDefaultSecurityGroup, oldFlowState.DefaultSecurityGroupId)
		rc.state.SetIDPtr(IdentiferInternetGateway, oldFlowState.InternetGatewayId)
		if oldFlowState.VPCEndpointIds != nil {
			child := rc.state.GetChild(ChildIdVPCEndpoints)
			for k, v := range oldFlowState.VPCEndpointIds {
				child.SetID(k, v)
			}
		}
	}

	if oldFlowState != nil && oldFlowState.CompletedDeletionTasks != nil {
		rc.state.MarkTaskCompleted(TaskKeyLoadBalancersAndSecurityGroups, oldFlowState.CompletedDeletionTasks[TaskKeyLoadBalancersAndSecurityGroups])
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
		Version:                FlowStateVersion1,
		DhcpOptionsId:          rc.state.GetID(IdentiferDHCPOptions),
		VpcId:                  rc.state.GetID(IdentiferVPC),
		DefaultSecurityGroupId: rc.state.GetID(IdentiferDefaultSecurityGroup),
		InternetGatewayId:      rc.state.GetID(IdentiferInternetGateway),
	}

	if rc.state.HasChild(ChildIdVPCEndpoints) {
		child := rc.state.GetChild(ChildIdVPCEndpoints)
		newFlowState.VPCEndpointIds = child.GetIDMap()
	}

	completedDeletionTasks := map[string]bool{}
	if rc.state.IsTaskMarkedCompleted(TaskKeyLoadBalancersAndSecurityGroups) {
		completedDeletionTasks[TaskKeyLoadBalancersAndSecurityGroups] = true
	}
	if len(completedDeletionTasks) > 0 {
		newFlowState.CompletedDeletionTasks = completedDeletionTasks
	}

	return newFlowState
}

func (rc *ReconcileContext) vpcEndpointServiceNamePrefix() string {
	return fmt.Sprintf("com.amazonaws.%s.", rc.infra.Spec.Region)
}

func (rc *ReconcileContext) extractVpcEndpointName(item *awsclient.VpcEndpoint) string {
	return strings.TrimPrefix(item.ServiceName, rc.vpcEndpointServiceNamePrefix())
}
