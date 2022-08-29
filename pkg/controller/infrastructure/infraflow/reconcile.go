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
	"github.com/gardener/gardener/pkg/utils/flow"
)

const (
	FlowStateVersion1    = "1"
	defaultRetryInterval = 5 * time.Second
	defaultRetryTimeout  = 90 * time.Second
)

func (rc *ReconcileContext) Reconcile(ctx context.Context) (*awsapiv1alpha.FlowState, error) {
	g := rc.buildReconcileGraph()
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{}); err != nil {
		return rc.UpdatedFlowState(), flow.Causes(err)
	}
	return rc.UpdatedFlowState(), nil
}

func (rc *ReconcileContext) buildReconcileGraph() *flow.Graph {
	g := flow.NewGraph("AWS infrastructure reconcilation")

	createVPC := rc.config.Networks.VPC.ID == nil
	ensureDhcpOptions := g.Add(flow.Task{
		Name: "ensure DHCP options for VPC",
		Fn: flow.TaskFn(rc.EnsureDhcpOptions).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(createVPC)})
	ensureVpc := g.Add(flow.Task{
		Name: "ensure VPC",
		Fn: flow.TaskFn(rc.EnsureVpc).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDhcpOptions),
	})
	ensureDefaultSecurityGroup := g.Add(flow.Task{
		Name: "ensure default security group",
		Fn: flow.TaskFn(rc.EnsureDefaultSecurityGroup).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(createVPC),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	ensureInternetGateway := g.Add(flow.Task{
		Name: "ensure internet gateway",
		Fn: flow.TaskFn(rc.EnsureInternetGateway).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(createVPC),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	ensureGatewayEndpoints := g.Add(flow.Task{
		Name: "ensure gateway endpoints",
		Fn: flow.TaskFn(rc.EnsureGatewayEndpoints).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureDefaultSecurityGroup, ensureInternetGateway),
	})

	unused(ensureGatewayEndpoints)

	return g
}

func (rc *ReconcileContext) EnsureDhcpOptions(ctx context.Context) error {
	dhcpDomainName := "ec2.internal"
	if rc.infra.Spec.Region != "us-east-1" {
		dhcpDomainName = fmt.Sprintf("%s.compute.internal", rc.infra.Spec.Region)
	}

	desired := &awsclient.DhcpOptions{
		Tags: rc.commonTags,
		DhcpConfigurations: map[string][]string{
			"domain-name":         {dhcpDomainName},
			"domain-name-servers": {"AmazonProvidedDNS"},
		},
	}
	current, err := findExisting(ctx, rc.state.GetID(IdentiferDHCPOptions), rc.commonTags,
		rc.client.GetVpcDhcpOptions, rc.client.FindVpcDhcpOptionsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.SetID(IdentiferDHCPOptions, current.DhcpOptionsId)
		if _, err := rc.updater.UpdateEC2Tags(ctx, current.DhcpOptionsId, rc.commonTags, current.Tags); err != nil {
			return err
		}
	} else {
		created, err := rc.client.CreateVpcDhcpOptions(ctx, desired)
		if err != nil {
			return err
		}
		rc.state.SetID(IdentiferDHCPOptions, created.DhcpOptionsId)
	}

	return nil
}

func (rc *ReconcileContext) EnsureVpc(ctx context.Context) error {
	if rc.config.Networks.VPC.ID != nil {
		rc.state.SetIDPtr(IdentiferVPC, rc.config.Networks.VPC.ID)
		return rc.ensureExistingVpc(ctx, *rc.config.Networks.VPC.ID)
	}
	desired := &awsclient.VPC{
		Tags:               rc.commonTags,
		EnableDnsSupport:   true,
		EnableDnsHostnames: true,
		DhcpOptionsId:      rc.state.GetID(IdentiferDHCPOptions),
	}
	if rc.config.Networks.VPC.CIDR == nil {
		return fmt.Errorf("missing VPC CIDR")
	}
	desired.CidrBlock = *rc.config.Networks.VPC.CIDR
	current, err := findExisting(ctx, rc.state.GetID(IdentiferVPC), rc.commonTags,
		rc.client.GetVpc, rc.client.FindVpcsByTags)
	if err != nil {
		return err
	}

	if current != nil {
		rc.state.SetID(IdentiferVPC, current.VpcId)
		if _, err := rc.updater.UpdateVpc(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := rc.client.CreateVpc(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.SetID(IdentiferVPC, created.VpcId)
	if _, err := rc.updater.UpdateVpc(ctx, desired, created); err != nil {
		return err
	}
	return nil
}

func (rc *ReconcileContext) ensureExistingVpc(ctx context.Context, vpcID string) error {
	current, err := rc.client.GetVpc(ctx, vpcID)
	if err != nil {
		return err
	}
	if current != nil {
		return fmt.Errorf("VPC %s has not been found", vpcID)
	}
	return nil
}

func (rc *ReconcileContext) EnsureDefaultSecurityGroup(ctx context.Context) error {
	current, err := rc.client.FindDefaultSecurityGroupByVpcId(ctx, *rc.state.GetID(IdentiferVPC))
	if err != nil {
		return err
	}
	if current == nil {
		return fmt.Errorf("default security group not found")
	}

	rc.state.SetID(IdentiferDefaultSecurityGroup, current.GroupId)
	desired := current.Clone()
	desired.Tags = rc.commonTags
	desired.Rules = nil
	if _, err := rc.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
		return err
	}
	return nil
}

func (rc *ReconcileContext) EnsureInternetGateway(ctx context.Context) error {
	desired := &awsclient.InternetGateway{
		Tags:  rc.commonTags,
		VpcId: rc.state.GetID(IdentiferVPC),
	}
	current, err := findExisting(ctx, rc.state.GetID(IdentiferInternetGateway), rc.commonTags,
		rc.client.GetInternetGateway, rc.client.FindInternetGatewaysByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.SetID(IdentiferInternetGateway, current.InternetGatewayId)
		if err := rc.client.AttachInternetGateway(ctx, *rc.state.GetID(IdentiferVPC), current.InternetGatewayId); err != nil {
			return err
		}
		if _, err := rc.updater.UpdateEC2Tags(ctx, current.InternetGatewayId, rc.commonTags, current.Tags); err != nil {
			return err
		}
		return nil
	}

	created, err := rc.client.CreateInternetGateway(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.SetID(IdentiferInternetGateway, created.InternetGatewayId)
	if err := rc.client.AttachInternetGateway(ctx, *rc.state.GetID(IdentiferVPC), created.InternetGatewayId); err != nil {
		return err
	}
	return nil
}

func (rc *ReconcileContext) EnsureGatewayEndpoints(ctx context.Context) error {
	child := rc.state.GetChild(ChildIdVPCEndpoints)
	var desired []*awsclient.VpcEndpoint
	for _, endpoint := range rc.config.Networks.VPC.GatewayEndpoints {
		desired = append(desired, &awsclient.VpcEndpoint{
			Tags:        rc.commonTagsWithSuffix(fmt.Sprintf("gw-%s", endpoint)),
			VpcId:       rc.state.GetID(IdentiferVPC),
			ServiceName: rc.vpcEndpointServiceNamePrefix() + endpoint,
		})
	}
	current, err := rc.collectExistingVPCEndpoints(ctx)
	if err != nil {
		return err
	}

	toBeDeleted, toBeCreated, toBeChecked := diffByID(desired, current, rc.extractVpcEndpointName)
	for _, item := range toBeDeleted {
		if err := rc.client.DeleteVpcEndpoint(ctx, item.VpcEndpointId); err != nil {
			return err
		}
		child.SetIDPtr(rc.extractVpcEndpointName(item), nil)
	}
	for _, item := range toBeCreated {
		created, err := rc.client.CreateVpcEndpoint(ctx, item)
		if err != nil {
			return err
		}
		child.SetID(rc.extractVpcEndpointName(item), created.VpcEndpointId)
		if _, err := rc.updater.UpdateEC2Tags(ctx, created.VpcEndpointId, item.Tags, created.Tags); err != nil {
			return err
		}
	}
	for _, item := range toBeChecked {
		commonTags := rc.commonTagsWithSuffix(fmt.Sprintf("gw-%s", rc.extractVpcEndpointName(item)))
		if _, err := rc.updater.UpdateEC2Tags(ctx, item.VpcEndpointId, commonTags, item.Tags); err != nil {
			return err
		}
	}
	return nil
}

func (rc *ReconcileContext) collectExistingVPCEndpoints(ctx context.Context) ([]*awsclient.VpcEndpoint, error) {
	child := rc.state.GetChild(ChildIdVPCEndpoints)
	var ids []string
	for _, id := range child.GetIDMap() {
		ids = append(ids, id)
	}
	var current []*awsclient.VpcEndpoint
	if len(ids) > 0 {
		found, err := rc.client.GetVpcEndpoints(ctx, ids)
		if err != nil {
			return nil, err
		}
		current = found
	}
	foundByTags, err := rc.client.FindVpcEndpointsByTags(ctx, rc.clusterTags())
	if err != nil {
		return nil, err
	}
outer:
	for _, item := range foundByTags {
		for _, currentItem := range current {
			if item.VpcEndpointId == currentItem.VpcEndpointId {
				continue outer
			}
		}
		current = append(current, item)
	}
	return current, nil
}
