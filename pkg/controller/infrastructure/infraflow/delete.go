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

func (rc *ReconcileContext) Delete() (*awsapiv1alpha.FlowState, error) {
	g := rc.buildDeleteGraph()
	f := g.Compile()
	if err := f.Run(rc.ctx, flow.Opts{}); err != nil {
		return rc.UpdatedFlowState(), flow.Causes(err)
	}
	return rc.UpdatedFlowState(), nil
}

func (rc *ReconcileContext) buildDeleteGraph() *flow.Graph {
	g := flow.NewGraph("AWS infrastructure destruction")

	deleteVPC := rc.config.Networks.VPC.ID == nil && rc.state.hasVPCID()
	destroyLoadBalancersAndSecurityGroups := g.Add(flow.Task{
		Name: "Destroying Kubernetes load balancers and security groups",
		Fn: flow.TaskFn(rc.EnsureKubernetesLoadBalancersAndSecurityGroupsDeleted).
			RetryUntilTimeout(10*time.Second, 5*time.Minute).
			DoIf(rc.state.hasVPCID() && !rc.state.deletedKubernetesLoadBalancersAndSecurityGroups)})
	ensureDeletedVpc := g.Add(flow.Task{
		Name: "ensure deletion of VPC",
		Fn: flow.TaskFn(rc.EnsureDeletedVpc).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC),
		Dependencies: flow.NewTaskIDs(destroyLoadBalancersAndSecurityGroups),
	})
	ensureDeletedDhcpOptions := g.Add(flow.Task{
		Name: "ensure deletion of DHCP options for VPC",
		Fn: flow.TaskFn(rc.EnsureDeletedDhcpOptions).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(deleteVPC && !alreadyDeleted(rc.state.dhcpOptionsID)),
		Dependencies: flow.NewTaskIDs(ensureDeletedVpc),
	})
	unused(ensureDeletedDhcpOptions)

	return g
}

func (rc *ReconcileContext) EnsureKubernetesLoadBalancersAndSecurityGroupsDeleted(ctx context.Context) error {
	if err := DestroyKubernetesLoadBalancersAndSecurityGroups(ctx, rc.client, *rc.state.vpcID, rc.infra.Namespace); err != nil {
		return gardencorev1beta1helper.DeprecatedDetermineError(fmt.Errorf("Failed to destroy load balancers and security groups: %w", err))
	}

	rc.state.deletedKubernetesLoadBalancersAndSecurityGroups = true

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

func (rc *ReconcileContext) EnsureDeletedVpc(ctx context.Context) error {
	found, err := rc.client.DescribeVpcs(ctx, rc.state.vpcID, rc.commonTags)
	if err != nil {
		return err
	}
	for _, vpc := range found {
		rc.logger.Info("Deleting VPC", "id", vpc.VpcId)
		if err := rc.client.DeleteVpc(ctx, vpc.VpcId); err != nil {
			return err
		}
		rc.logger.Info("Deleted VPC", "id", vpc.VpcId)
	}
	rc.state.vpcID = deletedMarker
	return nil
}

func (rc *ReconcileContext) EnsureDeletedDhcpOptions(ctx context.Context) error {
	found, err := rc.client.DescribeVpcDhcpOptions(ctx, rc.state.dhcpOptionsID, rc.commonTags)
	if err != nil {
		return err
	}
	for _, options := range found {
		rc.logger.Info("Deleting DHCP options", "id", options.DhcpOptionsId)
		if err := rc.client.DeleteVpcDhcpOptions(ctx, options.DhcpOptionsId); err != nil {
			return err
		}
		rc.logger.Info("Deleted VPC", "id", options.DhcpOptionsId)
	}
	return nil

}
