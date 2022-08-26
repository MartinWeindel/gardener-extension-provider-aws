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

	awsapi "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws"
	awsapiv1alpha "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws/v1alpha1"
	"github.com/gardener/gardener-extension-provider-aws/pkg/aws"
	awsclient "github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	"k8s.io/utils/pointer"
)

const (
	TagKeyName                                      = "Name"
	TagKeyClusterTemplate                           = "kubernetes.io/cluster/%s"
	CompletedDeletionLoadBalancersAndSecurityGroups = "LoadBalancersAndSecurityGroups"
	deleted                                         = "<deleted>"
)

var deletedMarker = pointer.String(deleted)

type StateWhiteboard struct {
	vpcID         *string
	dhcpOptionsID *string

	deletedKubernetesLoadBalancersAndSecurityGroups bool
}

func (w *StateWhiteboard) hasVPCID() bool {
	return hasValue(w.vpcID)
}

func hasValue(id *string) bool {
	return id != nil && *id != "" && *id != deleted
}

func alreadyDeleted(id *string) bool {
	return id == deletedMarker
}

type ReconcileContext struct {
	infra      *extensionsv1alpha1.Infrastructure
	config     *awsapi.InfrastructureConfig
	logger     logr.Logger
	client     awsclient.Interface
	commonTags awsclient.Tags

	state StateWhiteboard
}

func NewReconcileContext(logger logr.Logger, awsClient awsclient.Interface,
	infra *extensionsv1alpha1.Infrastructure, config *awsapi.InfrastructureConfig,
	oldFlowState *awsapi.FlowState, tfState *TerraformState) (*ReconcileContext, error) {
	rc := &ReconcileContext{
		infra:  infra,
		config: config,
		logger: logger,
		client: awsClient,
		commonTags: awsclient.Tags{
			fmt.Sprintf(TagKeyClusterTemplate, infra.Namespace): "1",
			TagKeyName: infra.Namespace,
		},
	}

	if oldFlowState != nil && oldFlowState.Version != FlowStateVersion1 {
		return nil, fmt.Errorf("unknown flow state version %s", oldFlowState.Version)
	}

	if config != nil && config.Networks.VPC.ID != nil {
		rc.state.vpcID = config.Networks.VPC.ID
	} else {
		if oldFlowState != nil && oldFlowState.VpcId != nil {
			rc.state.vpcID = oldFlowState.VpcId
		} else if tfState != nil {
			value := tfState.Outputs[aws.VPCIDKey].Value
			rc.state.vpcID = &value
		}

		if oldFlowState != nil && oldFlowState.DhcpOptionsId != nil {
			rc.state.dhcpOptionsID = oldFlowState.DhcpOptionsId
		} else if tfState != nil {
			instances := tfState.FindManagedResourceInstances("aws_vpc_dhcp_options", "vpc_dhcp_options")
			if instances != nil && len(instances) == 1 {
				if value, ok := attributeAsString(instances[0].Attributes, AttributeKeyId); ok {
					rc.state.dhcpOptionsID = &value
				}
			}
		}
	}

	if oldFlowState != nil && oldFlowState.CompletedDeletionTasks != nil {
		rc.state.deletedKubernetesLoadBalancersAndSecurityGroups = oldFlowState.CompletedDeletionTasks[CompletedDeletionLoadBalancersAndSecurityGroups]
	}

	return rc, nil
}

func (rc *ReconcileContext) GetInfrastructureConfig() *awsapi.InfrastructureConfig {
	return rc.config
}

func (rc *ReconcileContext) UpdatedFlowState() *awsapiv1alpha.FlowState {
	newFlowState := &awsapiv1alpha.FlowState{
		Version:       FlowStateVersion1,
		DhcpOptionsId: rc.state.dhcpOptionsID,
		VpcId:         rc.state.vpcID,
	}

	completedDeletionTasks := map[string]bool{}
	if rc.state.deletedKubernetesLoadBalancersAndSecurityGroups {
		completedDeletionTasks[CompletedDeletionLoadBalancersAndSecurityGroups] = true
	}
	if len(completedDeletionTasks) > 0 {
		newFlowState.CompletedDeletionTasks = completedDeletionTasks
	}

	return newFlowState
}
