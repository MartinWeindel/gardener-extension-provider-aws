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
	if err := f.Run(ctx, flow.Opts{Log: c.log}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (c *FlowContext) buildDeleteGraph() *flow.Graph {
	g := flow.NewGraph("AWS infrastructure destruction")

	deleteVPC := c.config.Networks.VPC.ID == nil && c.state.Has(IdentifierVPC)

	destroyLoadBalancersAndSecurityGroups := c.addTask(g, "Destroying Kubernetes load balancers and security groups",
		flow.TaskFn(c.deleteKubernetesLoadBalancersAndSecurityGroups).
			RetryUntilTimeout(10*time.Second, 5*time.Minute).
			DoIf(c.hasVPC() && c.state.Get(MarkerLoadBalancersAndSecurityGroupsDestroyed) == nil))

	_ = c.addTask(g, "delete key pair",
		flow.TaskFn(c.deleteKeyPair).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout))

	deleteIAMInstanceProfile := c.addTask(g, "delete IAM instance profile",
		flow.TaskFn(c.deleteIAMInstanceProfile).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout))

	deleteIAMRolePolicy := c.addTask(g, "delete IAM role policy",
		flow.TaskFn(c.deleteIAMRolePolicy).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout))

	_ = c.addTask(g, "delete IAM role",
		flow.TaskFn(c.deleteIAMRole).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		deleteIAMInstanceProfile, deleteIAMRolePolicy)

	deleteSubnets := c.addTask(g, "delete zones resources",
		flow.TaskFn(c.deleteZones).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(c.hasVPC()))

	deleteNodesSecurityGroup := c.addTask(g, "delete nodes security group",
		flow.TaskFn(c.deleteNodesSecurityGroup).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(c.hasVPC()),
		deleteSubnets)

	deleteMainRouteTable := c.addTask(g, "delete main route table",
		flow.TaskFn(c.deleteMainRouteTable).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(c.hasVPC()),
		deleteSubnets)

	deleteGatewayEndpoints := c.addTask(g, "delete gateway endpoints",
		flow.TaskFn(c.deleteGatewayEndpoints).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(c.hasVPC()))

	deleteInternetGateway := c.addTask(g, "delete internet gateway",
		flow.TaskFn(c.deleteInternetGateway).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(deleteVPC && c.hasVPC()),
		deleteGatewayEndpoints, deleteMainRouteTable)

	deleteDefaultSecurityGroup := c.addTask(g, "delete default security group",
		flow.TaskFn(c.deleteDefaultSecurityGroup).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(deleteVPC && c.hasVPC()),
		deleteGatewayEndpoints)

	deleteVpc := c.addTask(g, "delete VPC",
		flow.TaskFn(c.deleteVpc).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC && c.hasVPC()),
		deleteInternetGateway, deleteDefaultSecurityGroup, deleteNodesSecurityGroup, destroyLoadBalancersAndSecurityGroups)

	_ = c.addTask(g, "delete DHCP options for VPC",
		flow.TaskFn(c.deleteDhcpOptions).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC && !c.state.IsAlreadyDeleted(IdentifierDHCPOptions)),
		deleteVpc)

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
	if c.state.IsAlreadyDeleted(IdentifierInternetGateway) {
		return nil
	}
	log := c.logFromContext(ctx)
	current, err := findExisting(ctx, c.state.Get(IdentifierInternetGateway), c.commonTags,
		c.client.GetInternetGateway, c.client.FindInternetGatewaysByTags)
	if err != nil {
		return err
	}
	if current != nil {
		log.Info("detaching...")
		if err := c.client.DetachInternetGateway(ctx, *c.state.Get(IdentifierVPC), current.InternetGatewayId); err != nil {
			return err
		}
		log.Info("deleting...", "InternetGatewayId", current.InternetGatewayId)
		if err := c.client.DeleteInternetGateway(ctx, current.InternetGatewayId); err != nil {
			return err
		}
		c.state.SetAsDeleted(IdentifierInternetGateway)
	}
	return nil
}

func (c *FlowContext) deleteGatewayEndpoints(ctx context.Context) error {
	log := c.logFromContext(ctx)
	child := c.state.GetChild(ChildIdVPCEndpoints)
	current, err := c.collectExistingVPCEndpoints(ctx)
	if err != nil {
		return err
	}

	for _, item := range current {
		log.Info("deleting...", "ServiceName", item.ServiceName)
		if err := c.client.DeleteVpcEndpoint(ctx, item.VpcEndpointId); err != nil {
			return err
		}
		name := c.extractVpcEndpointName(item)
		child.SetAsDeleted(name)
	}
	// update state of endpoints in state, but not found
	for _, key := range child.Keys() {
		child.SetAsDeleted(key)
	}
	return nil
}

func (c *FlowContext) deleteVpc(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(IdentifierVPC) {
		return nil
	}
	log := c.logFromContext(ctx)
	current, err := findExisting(ctx, c.state.Get(IdentifierVPC), c.commonTags,
		c.client.GetVpc, c.client.FindVpcsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		log.Info("deleting...", "VpcId", current.VpcId)
		if err := c.client.DeleteVpc(ctx, current.VpcId); err != nil {
			return err
		}
	}
	c.state.SetAsDeleted(IdentifierVPC)
	return nil
}

func (c *FlowContext) deleteDhcpOptions(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(IdentifierDHCPOptions) {
		return nil
	}
	log := c.logFromContext(ctx)
	current, err := findExisting(ctx, c.state.Get(IdentifierDHCPOptions), c.commonTags,
		c.client.GetVpcDhcpOptions, c.client.FindVpcDhcpOptionsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		log.Info("deleting...", "DhcpOptionsId", current.DhcpOptionsId)
		if err := c.client.DeleteVpcDhcpOptions(ctx, current.DhcpOptionsId); err != nil {
			return err
		}
		c.state.SetAsDeleted(IdentifierDHCPOptions)
	}
	return nil
}

func (c *FlowContext) deleteMainRouteTable(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(IdentifierMainRouteTable) {
		return nil
	}
	log := c.logFromContext(ctx)
	current, err := findExisting(ctx, c.state.Get(IdentifierMainRouteTable), c.commonTags,
		c.client.GetRouteTable, c.client.FindRouteTablesByTags)
	if err != nil {
		return err
	}
	if current != nil {
		log.Info("deleting...", "RouteTableId", current.RouteTableId)
		if err := c.client.DeleteRouteTable(ctx, current.RouteTableId); err != nil {
			return err
		}
	}
	c.state.SetAsDeleted(IdentifierMainRouteTable)
	return nil
}

func (c *FlowContext) deleteNodesSecurityGroup(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(IdentifierNodesSecurityGroup) {
		return nil
	}
	log := c.logFromContext(ctx)
	groupName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	current, err := findExisting(ctx, c.state.Get(IdentifierNodesSecurityGroup), c.commonTagsWithSuffix("nodes"),
		c.client.GetSecurityGroup, c.client.FindSecurityGroupsByTags,
		func(item *awsclient.SecurityGroup) bool { return item.GroupName == groupName })
	if err != nil {
		return err
	}
	if current != nil {
		log.Info("deleting...", "GroupId", current.GroupId)
		if err := c.client.DeleteSecurityGroup(ctx, current.GroupId); err != nil {
			return err
		}
	}
	c.state.SetAsDeleted(IdentifierNodesSecurityGroup)
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
	if err := f.Run(ctx, flow.Opts{Log: c.log}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (c *FlowContext) deleteIAMRole(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(NameIAMRole) {
		return nil
	}

	log := c.logFromContext(ctx)
	roleName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	log.Info("deleting...", "RoleName", roleName)
	if err := c.client.DeleteIAMRole(ctx, roleName); err != nil {
		return err
	}
	c.state.SetAsDeleted(NameIAMRole)
	c.state.Set(ARNIAMRole, "")
	return nil
}

func (c *FlowContext) deleteIAMInstanceProfile(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(NameIAMInstanceProfile) {
		return nil
	}
	log := c.logFromContext(ctx)
	instanceProfileName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	log.Info("deleting...", "InstanceProfileName", instanceProfileName)
	if err := c.client.DeleteIAMInstanceProfile(ctx, instanceProfileName); err != nil {
		return err
	}
	c.state.SetAsDeleted(NameIAMInstanceProfile)
	return nil
}

func (c *FlowContext) deleteIAMRolePolicy(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(NameIAMRolePolicy) {
		return nil
	}
	log := c.logFromContext(ctx)
	policyName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	roleName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	log.Info("removing from profile...")
	if err := c.client.RemoveRoleFromIAMInstanceProfile(ctx, policyName, roleName); err != nil {
		return err
	}
	log.Info("deleting...", "PolicyName", policyName, "RoleName", roleName)
	if err := c.client.DeleteIAMRolePolicy(ctx, policyName, roleName); err != nil {
		return err
	}
	c.state.SetAsDeleted(NameIAMRolePolicy)
	return nil
}

func (c *FlowContext) deleteKeyPair(ctx context.Context) error {
	if c.state.IsAlreadyDeleted(NameKeyPair) {
		return nil
	}
	log := c.logFromContext(ctx)
	keyName := fmt.Sprintf("%s-ssh-publickey", c.infra.Namespace)
	log.Info("deleting...", "KeyName", keyName)
	if err := c.client.DeleteKeyPair(ctx, keyName); err != nil {
		return err
	}
	c.state.SetAsDeleted(NameKeyPair)
	return nil
}
