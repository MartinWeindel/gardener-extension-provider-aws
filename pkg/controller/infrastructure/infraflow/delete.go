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

	awsclient "github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	gardencorev1beta1helper "github.com/gardener/gardener/pkg/apis/core/v1beta1/helper"
	"github.com/gardener/gardener/pkg/utils/flow"
)

func (c *FlowContext) Delete(ctx context.Context) error {
	g := c.buildDeleteGraph()
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: c.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (c *FlowContext) buildDeleteGraph() *flow.Graph {
	g := flow.NewGraph("AWS infrastructure destruction")

	deleteVPC := c.config.Networks.VPC.ID == nil && c.state.Has(IdentifierVPC)
	destroyLoadBalancersAndSecurityGroups := g.Add(flow.Task{
		Name: "Destroying Kubernetes load balancers and security groups",
		Fn: flow.TaskFn(c.PersistingState(c.deleteKubernetesLoadBalancersAndSecurityGroups)).
			RetryUntilTimeout(10*time.Second, 5*time.Minute).
			DoIf(c.state.Has(IdentifierVPC) && c.state.Get(MarkerLoadBalancersAndSecurityGroupsDestroyed) == nil)})
	_ = g.Add(flow.Task{
		Name: "delete key pair",
		Fn: flow.TaskFn(c.PersistingState(c.deleteKeyPair)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	deleteIAMInstanceProfile := g.Add(flow.Task{
		Name: "delete IAM instance profile",
		Fn: flow.TaskFn(c.PersistingState(c.deleteIAMInstanceProfile)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	deleteIAMRolePolicy := g.Add(flow.Task{
		Name: "delete IAM role policy",
		Fn: flow.TaskFn(c.PersistingState(c.deleteIAMRolePolicy)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	_ = g.Add(flow.Task{
		Name: "delete IAM role",
		Fn: flow.TaskFn(c.PersistingState(c.deleteIAMRole)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(deleteIAMInstanceProfile, deleteIAMRolePolicy),
	})
	deleteSubnets := g.Add(flow.Task{
		Name: "delete zones resources",
		Fn: flow.TaskFn(c.PersistingState(c.deleteZones)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	deleteNodesSecurityGroup := g.Add(flow.Task{
		Name: "delete nodes security group",
		Fn: flow.TaskFn(c.PersistingState(c.deleteNodesSecurityGroup)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(deleteSubnets),
	})
	deleteMainRouteTable := g.Add(flow.Task{
		Name: "delete main route table",
		Fn: flow.TaskFn(c.PersistingState(c.deleteMainRouteTable)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(deleteSubnets),
	})
	deleteGatewayEndpoints := g.Add(flow.Task{
		Name: "delete gateway endpoints",
		Fn: flow.TaskFn(c.PersistingState(c.deleteGatewayEndpoints)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	deleteInternetGateway := g.Add(flow.Task{
		Name: "delete internet gateway",
		Fn: flow.TaskFn(c.PersistingState(c.deleteInternetGateway)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(deleteGatewayEndpoints, deleteMainRouteTable),
	})
	deleteDefaultSecurityGroup := g.Add(flow.Task{
		Name: "delete default security group",
		Fn: flow.TaskFn(c.PersistingState(c.deleteDefaultSecurityGroup)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(deleteGatewayEndpoints),
	})
	deleteVpc := g.Add(flow.Task{
		Name: "delete VPC",
		Fn: flow.TaskFn(c.PersistingState(c.deleteVpc)).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(deleteInternetGateway, deleteDefaultSecurityGroup, deleteNodesSecurityGroup, destroyLoadBalancersAndSecurityGroups),
	})
	_ = g.Add(flow.Task{
		Name: "delete DHCP options for VPC",
		Fn: flow.TaskFn(c.PersistingState(c.deleteDhcpOptions)).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC && !c.state.IsAlreadyDeleted(IdentifierDHCPOptions)),
		Dependencies: flow.NewTaskIDs(deleteVpc),
	})

	return g
}

func (c *FlowContext) deleteKubernetesLoadBalancersAndSecurityGroups(ctx context.Context) error {
	if err := DestroyKubernetesLoadBalancersAndSecurityGroups(ctx, c.client, *c.state.Get(IdentifierVPC), c.infra.Namespace); err != nil {
		return gardencorev1beta1helper.DeprecatedDetermineError(fmt.Errorf("Failed to destroy load balancers and security groups: %w", err))
	}

	c.state.Set(MarkerLoadBalancersAndSecurityGroupsDestroyed, "true")

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

func (c *FlowContext) deleteDefaultSecurityGroup(ctx context.Context) error {
	// nothing to do, it is deleted automatically together with VPC
	c.state.SetAsDeleted(IdentifierDefaultSecurityGroup)
	return nil
}

func (c *FlowContext) deleteInternetGateway(ctx context.Context) error {
	current, err := findExisting(ctx, c.state.Get(IdentifierInternetGateway), c.commonTags,
		c.client.GetInternetGateway, c.client.FindInternetGatewaysByTags)
	if err != nil {
		return err
	}
	if current != nil {
		if err := c.client.DetachInternetGateway(ctx, *c.state.Get(IdentifierVPC), current.InternetGatewayId); err != nil {
			return err
		}
		if err := c.client.DeleteInternetGateway(ctx, current.InternetGatewayId); err != nil {
			return err
		}
		c.state.SetAsDeleted(IdentifierInternetGateway)
		c.logger.Info("Deleted internet gateway", "id", current.InternetGatewayId)
	}
	return nil
}

func (c *FlowContext) deleteGatewayEndpoints(ctx context.Context) error {
	child := c.state.GetChild(ChildIdVPCEndpoints)
	current, err := c.collectExistingVPCEndpoints(ctx)
	if err != nil {
		return err
	}

	for _, item := range current {
		if err := c.client.DeleteVpcEndpoint(ctx, item.VpcEndpointId); err != nil {
			return err
		}
		name := c.extractVpcEndpointName(item)
		child.SetAsDeleted(name)
		c.logger.Info("Deleted VPC endpoint", "id", item.VpcEndpointId, "name", name)
	}
	// update state of endpoints in state, but not found
	for _, key := range child.Keys() {
		child.SetAsDeleted(key)
	}
	return nil
}

func (c *FlowContext) deleteVpc(ctx context.Context) error {
	current, err := findExisting(ctx, c.state.Get(IdentifierVPC), c.commonTags,
		c.client.GetVpc, c.client.FindVpcsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		c.logger.Info("Deleting VPC", "id", current.VpcId)
		if err := c.client.DeleteVpc(ctx, current.VpcId); err != nil {
			return err
		}
		c.logger.Info("Deleted VPC", "id", current.VpcId)
	}
	c.state.SetAsDeleted(IdentifierVPC)
	return nil
}

func (c *FlowContext) deleteDhcpOptions(ctx context.Context) error {
	current, err := findExisting(ctx, c.state.Get(IdentifierDHCPOptions), c.commonTags,
		c.client.GetVpcDhcpOptions, c.client.FindVpcDhcpOptionsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		c.logger.Info("Deleting DHCP options", "id", current.DhcpOptionsId)
		if err := c.client.DeleteVpcDhcpOptions(ctx, current.DhcpOptionsId); err != nil {
			return err
		}
		c.logger.Info("Deleted VPC", "id", current.DhcpOptionsId)
	}
	return nil
}

func (c *FlowContext) deleteMainRouteTable(ctx context.Context) error {
	current, err := findExisting(ctx, c.state.Get(IdentifierMainRouteTable), c.commonTags,
		c.client.GetRouteTable, c.client.FindRouteTablesByTags)
	if err != nil {
		return err
	}
	if current != nil {
		if err := c.client.DeleteRouteTable(ctx, current.RouteTableId); err != nil {
			return err
		}
		c.state.SetAsDeleted(IdentifierMainRouteTable)
	} else {
		c.state.SetPtr(IdentifierMainRouteTable, nil)
	}
	return nil
}

func (c *FlowContext) deleteNodesSecurityGroup(ctx context.Context) error {
	groupName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	current, err := findExisting(ctx, c.state.Get(IdentifierNodesSecurityGroup), c.commonTagsWithSuffix("nodes"),
		c.client.GetSecurityGroup, c.client.FindSecurityGroupsByTags,
		func(item *awsclient.SecurityGroup) bool { return item.GroupName == groupName })
	if err != nil {
		return err
	}
	if current != nil {
		if err := c.client.DeleteSecurityGroup(ctx, current.GroupId); err != nil {
			return err
		}
		c.state.SetAsDeleted(IdentifierNodesSecurityGroup)
	} else {
		c.state.SetPtr(IdentifierNodesSecurityGroup, nil)
	}
	return nil
}

func (c *FlowContext) deleteZones(ctx context.Context) error {
	current, err := c.collectExistingSubnets(ctx)
	if err != nil {
		return err
	}
	g := flow.NewGraph("AWS infrastructure destruction: zones")
	c.addZoneDeletionTasksBySubnets(g, current)
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: c.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (c *FlowContext) deleteIAMRole(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(IdentifierIAMRole) {
		return nil
	}

	roleName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	if err := c.client.DeleteIAMRole(ctx, roleName); err != nil {
		return err
	}
	c.state.SetAsDeleted(IdentifierIAMRole)
	return nil
}

func (c *FlowContext) deleteIAMInstanceProfile(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(IdentifierIAMInstanceProfile) {
		return nil
	}
	instanceProfileName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	if err := c.client.DeleteIAMInstanceProfile(ctx, instanceProfileName); err != nil {
		return err
	}
	c.state.SetAsDeleted(IdentifierIAMInstanceProfile)
	return nil
}

func (c *FlowContext) deleteIAMRolePolicy(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(IdentifierIAMRolePolicy) {
		return nil
	}

	policyName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	roleName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	if err := c.client.RemoveRoleFromIAMInstanceProfile(ctx, policyName, roleName); err != nil {
		return err
	}
	if err := c.client.DeleteIAMRolePolicy(ctx, policyName, roleName); err != nil {
		return err
	}
	c.state.SetAsDeleted(IdentifierIAMRolePolicy)
	return nil
}

func (c *FlowContext) deleteKeyPair(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(IdentifierKeyPair) {
		return nil
	}

	keyName := fmt.Sprintf("%s-ssh-publickey", c.infra.Namespace)
	if err := c.client.DeleteKeyPair(ctx, keyName); err != nil {
		return err
	}
	c.state.SetAsDeleted(IdentifierKeyPair)
	return nil
}
