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
	"time"

	awsapiv1alpha "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws/v1alpha1"
	awsclient "github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	gardencorev1beta1helper "github.com/gardener/gardener/pkg/apis/core/v1beta1/helper"
	"github.com/gardener/gardener/pkg/utils/flow"
)

func (rc *ReconcileContext) Delete(ctx context.Context) (*awsapiv1alpha.FlowState, error) {
	g := rc.buildDeleteGraph()
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{}); err != nil {
		return rc.UpdatedFlowState(), flow.Causes(err)
	}
	return rc.UpdatedFlowState(), nil
}

func (rc *ReconcileContext) buildDeleteGraph() *flow.Graph {
	g := flow.NewGraph("AWS infrastructure destruction")

	deleteVPC := rc.config.Networks.VPC.ID == nil && rc.state.HasID(IdentiferVPC)
	destroyLoadBalancersAndSecurityGroups := g.Add(flow.Task{
		Name: "Destroying Kubernetes load balancers and security groups",
		Fn: flow.TaskFn(rc.EnsureKubernetesLoadBalancersAndSecurityGroupsDeleted).
			RetryUntilTimeout(10*time.Second, 5*time.Minute).
			DoIf(rc.state.HasID(IdentiferVPC) && !rc.state.IsTaskMarkedCompleted(TaskKeyLoadBalancersAndSecurityGroups))})
	ensureDeleteGatewayEndpoints := g.Add(flow.Task{
		Name: "ensure deletion of gateway endpoints",
		Fn: flow.TaskFn(rc.EnsureDeletedGatewayEndpoints).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	ensureDeleteInternetGateway := g.Add(flow.Task{
		Name: "ensure deletion of internet gateway",
		Fn: flow.TaskFn(rc.EnsureDeletedInternetGateway).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(ensureDeleteGatewayEndpoints),
	})
	ensureDeleteDefaultSecurityGroup := g.Add(flow.Task{
		Name: "ensure deletion of default security group",
		Fn: flow.TaskFn(rc.EnsureDeletedDefaultSecurityGroup).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(ensureDeleteGatewayEndpoints),
	})
	ensureDeletedVpc := g.Add(flow.Task{
		Name: "ensure deletion of VPC",
		Fn: flow.TaskFn(rc.EnsureDeletedVpc).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(ensureDeleteInternetGateway, ensureDeleteDefaultSecurityGroup, destroyLoadBalancersAndSecurityGroups),
	})
	ensureDeletedDhcpOptions := g.Add(flow.Task{
		Name: "ensure deletion of DHCP options for VPC",
		Fn: flow.TaskFn(rc.EnsureDeletedDhcpOptions).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC && !rc.state.IsIDAlreadyDeleted(IdentiferDHCPOptions)),
		Dependencies: flow.NewTaskIDs(ensureDeletedVpc),
	})
	unused(ensureDeletedDhcpOptions)

	return g
}

func (rc *ReconcileContext) EnsureKubernetesLoadBalancersAndSecurityGroupsDeleted(ctx context.Context) error {
	if err := DestroyKubernetesLoadBalancersAndSecurityGroups(ctx, rc.client, *rc.state.GetID(IdentiferVPC), rc.infra.Namespace); err != nil {
		return gardencorev1beta1helper.DeprecatedDetermineError(fmt.Errorf("Failed to destroy load balancers and security groups: %w", err))
	}

	rc.state.MarkTaskCompleted(TaskKeyLoadBalancersAndSecurityGroups, true)

	return nil
}

func DestroyKubernetesLoadBalancersAndSecurityGroups(ctx context.Context, awsClient awsclient.Interface, vpcID, clusterName string) error {
	for _, v := range []struct {
		listFn   func(context.Context, string, string) ([]string, error)
		deleteFn func(context.Context, string) error
	}{
		{awsClient.ListKubernetesELBs, awsClient.DeleteELB},
		{awsClient.ListKubernetesELBsV2, awsClient.DeleteELBV2},
		{awsClient.ListKubernetesSecurityGroups, awsClient.DeleteSecurityGroup},
	} {
		results, err := v.listFn(ctx, vpcID, clusterName)
		if err != nil {
			return err
		}

		for _, result := range results {
			if err := v.deleteFn(ctx, result); err != nil {
				return err
			}
		}
	}

	return nil
}

func (rc *ReconcileContext) EnsureDeletedDefaultSecurityGroup(ctx context.Context) error {
	// nothing to do, it is deleted automatically together with VPC
	rc.state.SetIDAsDeleted(IdentiferDefaultSecurityGroup)
	return nil
}

func (rc *ReconcileContext) EnsureDeletedInternetGateway(ctx context.Context) error {
	current, err := findExisting(ctx, rc.state.GetID(IdentiferInternetGateway), rc.commonTags,
		rc.client.GetInternetGateway, rc.client.FindInternetGatewaysByTags)
	if err != nil {
		return err
	}
	if current != nil {
		if err := rc.client.DetachInternetGateway(ctx, *rc.state.GetID(IdentiferVPC), current.InternetGatewayId); err != nil {
			return err
		}
		if err := rc.client.DeleteInternetGateway(ctx, current.InternetGatewayId); err != nil {
			return err
		}
		rc.state.SetIDAsDeleted(IdentiferInternetGateway)
		rc.logger.Info("Deleted internet gateway", "id", current.InternetGatewayId)
	}
	return nil
}

func (rc *ReconcileContext) EnsureDeletedGatewayEndpoints(ctx context.Context) error {
	child := rc.state.GetChild(ChildIdVPCEndpoints)
	current, err := rc.collectExistingVPCEndpoints(ctx)
	if err != nil {
		return err
	}

	for _, item := range current {
		if err := rc.client.DeleteVpcEndpoint(ctx, item.VpcEndpointId); err != nil {
			return err
		}
		name := rc.extractVpcEndpointName(item)
		child.SetIDAsDeleted(name)
		rc.logger.Info("Deleted VPC endpoint", "id", item.VpcEndpointId, "name", name)
	}
	// update state of endpoints in state, but not found
	for _, key := range child.GetIDKeys() {
		child.SetIDAsDeleted(key)
	}
	return nil
}

func (rc *ReconcileContext) EnsureDeletedVpc(ctx context.Context) error {
	current, err := findExisting(ctx, rc.state.GetID(IdentiferVPC), rc.commonTags,
		rc.client.GetVpc, rc.client.FindVpcsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.logger.Info("Deleting VPC", "id", current.VpcId)
		if err := rc.client.DeleteVpc(ctx, current.VpcId); err != nil {
			return err
		}
		rc.logger.Info("Deleted VPC", "id", current.VpcId)
	}
	rc.state.SetIDAsDeleted(IdentiferVPC)
	return nil
}

func (rc *ReconcileContext) EnsureDeletedDhcpOptions(ctx context.Context) error {
	current, err := findExisting(ctx, rc.state.GetID(IdentiferDHCPOptions), rc.commonTags,
		rc.client.GetVpcDhcpOptions, rc.client.FindVpcDhcpOptionsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.logger.Info("Deleting DHCP options", "id", current.DhcpOptionsId)
		if err := rc.client.DeleteVpcDhcpOptions(ctx, current.DhcpOptionsId); err != nil {
			return err
		}
		rc.logger.Info("Deleted VPC", "id", current.DhcpOptionsId)
	}
	return nil
}
