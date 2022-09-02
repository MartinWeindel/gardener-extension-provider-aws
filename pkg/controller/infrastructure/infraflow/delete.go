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

func (rc *ReconcileContext) Delete(ctx context.Context) error {
	g := rc.buildDeleteGraph()
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: rc.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (rc *ReconcileContext) buildDeleteGraph() *flow.Graph {
	g := flow.NewGraph("AWS infrastructure destruction")

	deleteVPC := rc.config.Networks.VPC.ID == nil && rc.state.Has(IdentifierVPC)
	destroyLoadBalancersAndSecurityGroups := g.Add(flow.Task{
		Name: "Destroying Kubernetes load balancers and security groups",
		Fn: flow.TaskFn(rc.EnsureKubernetesLoadBalancersAndSecurityGroupsDeleted).
			RetryUntilTimeout(10*time.Second, 5*time.Minute).
			DoIf(rc.state.Has(IdentifierVPC) && rc.state.Get(MarkerLoadBalancersAndSecurityGroupsDestroyed) == nil)})
	_ = g.Add(flow.Task{
		Name: "ensure deletion of key pair",
		Fn: flow.TaskFn(rc.EnsureDeletedKeyPair).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	ensureDeletedIAMInstanceProfile := g.Add(flow.Task{
		Name: "ensure deletion of IAM instance profile",
		Fn: flow.TaskFn(rc.EnsureDeletedIAMInstanceProfile).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	ensureDeletedIAMRolePolicy := g.Add(flow.Task{
		Name: "ensure deletion of IAM role policy",
		Fn: flow.TaskFn(rc.EnsureDeletedIAMRolePolicy).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	_ = g.Add(flow.Task{
		Name: "ensure deletion of IAM role",
		Fn: flow.TaskFn(rc.EnsureDeletedIAMRole).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDeletedIAMInstanceProfile, ensureDeletedIAMRolePolicy),
	})
	ensureDeletedSubnets := g.Add(flow.Task{
		Name: "ensure deletion of zones resources",
		Fn: flow.TaskFn(rc.EnsureDeletedZones).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	ensureDeletedNodesSecurityGroup := g.Add(flow.Task{
		Name: "ensure deletion of nodes security group",
		Fn: flow.TaskFn(rc.EnsureDeletedNodesSecurityGroup).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDeletedSubnets),
	})
	ensureDeletedMainRouteTable := g.Add(flow.Task{
		Name: "ensure deletion of main route table",
		Fn: flow.TaskFn(rc.EnsureDeletedMainRouteTable).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDeletedSubnets),
	})
	ensureDeletedGatewayEndpoints := g.Add(flow.Task{
		Name: "ensure deletion of gateway endpoints",
		Fn: flow.TaskFn(rc.EnsureDeletedGatewayEndpoints).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	ensureDeletedInternetGateway := g.Add(flow.Task{
		Name: "ensure deletion of internet gateway",
		Fn: flow.TaskFn(rc.EnsureDeletedInternetGateway).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(ensureDeletedGatewayEndpoints, ensureDeletedMainRouteTable),
	})
	ensureDeletedDefaultSecurityGroup := g.Add(flow.Task{
		Name: "ensure deletion of default security group",
		Fn: flow.TaskFn(rc.EnsureDeletedDefaultSecurityGroup).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(ensureDeletedGatewayEndpoints),
	})
	ensureDeletedVpc := g.Add(flow.Task{
		Name: "ensure deletion of VPC",
		Fn: flow.TaskFn(rc.EnsureDeletedVpc).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(ensureDeletedInternetGateway, ensureDeletedDefaultSecurityGroup, ensureDeletedNodesSecurityGroup, destroyLoadBalancersAndSecurityGroups),
	})
	_ = g.Add(flow.Task{
		Name: "ensure deletion of DHCP options for VPC",
		Fn: flow.TaskFn(rc.EnsureDeletedDhcpOptions).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC && !rc.state.IsAlreadyDeleted(IdentifierDHCPOptions)),
		Dependencies: flow.NewTaskIDs(ensureDeletedVpc),
	})

	return g
}

func (rc *ReconcileContext) EnsureKubernetesLoadBalancersAndSecurityGroupsDeleted(ctx context.Context) error {
	if err := DestroyKubernetesLoadBalancersAndSecurityGroups(ctx, rc.client, *rc.state.Get(IdentifierVPC), rc.infra.Namespace); err != nil {
		return gardencorev1beta1helper.DeprecatedDetermineError(fmt.Errorf("Failed to destroy load balancers and security groups: %w", err))
	}

	rc.state.Set(MarkerLoadBalancersAndSecurityGroupsDestroyed, "true")

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
	rc.state.SetAsDeleted(IdentifierDefaultSecurityGroup)
	return nil
}

func (rc *ReconcileContext) EnsureDeletedInternetGateway(ctx context.Context) error {
	current, err := findExisting(ctx, rc.state.Get(IdentifierInternetGateway), rc.commonTags,
		rc.client.GetInternetGateway, rc.client.FindInternetGatewaysByTags)
	if err != nil {
		return err
	}
	if current != nil {
		if err := rc.client.DetachInternetGateway(ctx, *rc.state.Get(IdentifierVPC), current.InternetGatewayId); err != nil {
			return err
		}
		if err := rc.client.DeleteInternetGateway(ctx, current.InternetGatewayId); err != nil {
			return err
		}
		rc.state.SetAsDeleted(IdentifierInternetGateway)
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
		child.SetAsDeleted(name)
		rc.logger.Info("Deleted VPC endpoint", "id", item.VpcEndpointId, "name", name)
	}
	// update state of endpoints in state, but not found
	for _, key := range child.Keys() {
		child.SetAsDeleted(key)
	}
	return nil
}

func (rc *ReconcileContext) EnsureDeletedVpc(ctx context.Context) error {
	current, err := findExisting(ctx, rc.state.Get(IdentifierVPC), rc.commonTags,
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
	rc.state.SetAsDeleted(IdentifierVPC)
	return nil
}

func (rc *ReconcileContext) EnsureDeletedDhcpOptions(ctx context.Context) error {
	current, err := findExisting(ctx, rc.state.Get(IdentifierDHCPOptions), rc.commonTags,
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

func (rc *ReconcileContext) EnsureDeletedMainRouteTable(ctx context.Context) error {
	current, err := findExisting(ctx, rc.state.Get(IdentifierMainRouteTable), rc.commonTags,
		rc.client.GetRouteTable, rc.client.FindRouteTablesByTags)
	if err != nil {
		return err
	}
	if current != nil {
		if err := rc.client.DeleteRouteTable(ctx, current.RouteTableId); err != nil {
			return err
		}
		rc.state.SetAsDeleted(IdentifierMainRouteTable)
	} else {
		rc.state.SetPtr(IdentifierMainRouteTable, nil)
	}
	return nil
}

func (rc *ReconcileContext) EnsureDeletedNodesSecurityGroup(ctx context.Context) error {
	groupName := fmt.Sprintf("%s-nodes", rc.infra.Namespace)
	current, err := findExisting(ctx, rc.state.Get(IdentifierNodesSecurityGroup), rc.commonTagsWithSuffix("nodes"),
		rc.client.GetSecurityGroup, rc.client.FindSecurityGroupsByTags,
		func(item *awsclient.SecurityGroup) bool { return item.GroupName == groupName })
	if err != nil {
		return err
	}
	if current != nil {
		if err := rc.client.DeleteSecurityGroup(ctx, current.GroupId); err != nil {
			return err
		}
		rc.state.SetAsDeleted(IdentifierNodesSecurityGroup)
	} else {
		rc.state.SetPtr(IdentifierNodesSecurityGroup, nil)
	}
	return nil
}

func (rc *ReconcileContext) EnsureDeletedZones(ctx context.Context) error {
	current, err := rc.collectExistingSubnets(ctx)
	if err != nil {
		return err
	}
	g := flow.NewGraph("AWS infrastructure destruction: zones")
	rc.addZoneDeletionTasksBySubnets(g, current)
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: rc.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (rc *ReconcileContext) EnsureDeletedIAMRole(ctx context.Context) error {
	if rc.state.IsAlreadyDeleted(IdentifierIAMRole) {
		return nil
	}

	roleName := fmt.Sprintf("%s-nodes", rc.infra.Namespace)
	if err := rc.client.DeleteIAMRole(ctx, roleName); err != nil {
		return err
	}
	rc.state.SetAsDeleted(IdentifierIAMRole)
	return nil
}

func (rc *ReconcileContext) EnsureDeletedIAMInstanceProfile(ctx context.Context) error {
	if rc.state.IsAlreadyDeleted(IdentifierIAMInstanceProfile) {
		return nil
	}
	instanceProfileName := fmt.Sprintf("%s-nodes", rc.infra.Namespace)
	if err := rc.client.DeleteIAMInstanceProfile(ctx, instanceProfileName); err != nil {
		return err
	}
	rc.state.SetAsDeleted(IdentifierIAMInstanceProfile)
	return nil
}

func (rc *ReconcileContext) EnsureDeletedIAMRolePolicy(ctx context.Context) error {
	if rc.state.IsAlreadyDeleted(IdentifierIAMRolePolicy) {
		return nil
	}

	policyName := fmt.Sprintf("%s-nodes", rc.infra.Namespace)
	roleName := fmt.Sprintf("%s-nodes", rc.infra.Namespace)
	if err := rc.client.RemoveRoleFromIAMInstanceProfile(ctx, policyName, roleName); err != nil {
		return err
	}
	if err := rc.client.DeleteIAMRolePolicy(ctx, policyName, roleName); err != nil {
		return err
	}
	rc.state.SetAsDeleted(IdentifierIAMRolePolicy)
	return nil
}

func (rc *ReconcileContext) EnsureDeletedKeyPair(ctx context.Context) error {
	if rc.state.IsAlreadyDeleted(IdentifierKeyPair) {
		return nil
	}

	keyName := fmt.Sprintf("%s-ssh-publickey", rc.infra.Namespace)
	if err := rc.client.DeleteKeyPair(ctx, keyName); err != nil {
		return err
	}
	rc.state.SetAsDeleted(IdentifierKeyPair)
	return nil
}
