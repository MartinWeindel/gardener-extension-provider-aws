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
	"github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	"github.com/gardener/gardener/pkg/utils/flow"
)

const (
	FlowStateVersion1 = "1"
)

func (rc *ReconcileContext) Reconcile() (*awsapiv1alpha.FlowState, error) {
	g := rc.buildReconcileGraph()
	f := g.Compile()
	if err := f.Run(rc.ctx, flow.Opts{}); err != nil {
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
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(createVPC)})
	ensureVpc := g.Add(flow.Task{
		Name: "ensure VPC",
		Fn: flow.TaskFn(rc.EnsureVpc).
			RetryUntilTimeout(5*time.Second, 5*time.Minute).
			DoIf(createVPC),
		Dependencies: flow.NewTaskIDs(ensureDhcpOptions),
	})
	unused(ensureVpc)

	return g
}

func (rc *ReconcileContext) EnsureDhcpOptions(ctx context.Context) error {
	dhcpDomainName := "ec2.internal"
	if rc.infra.Spec.Region != "us-east-1" {
		dhcpDomainName = fmt.Sprintf("%s.compute.internal", rc.infra.Spec.Region)
	}

	desired := &client.DhcpOptions{
		Tags: rc.commonTags,
		DhcpConfigurations: map[string][]string{
			"domain-name":         {dhcpDomainName},
			"domain-name-servers": {"AmazonProvidedDNS"},
		},
	}
	found, err := rc.client.DescribeVpcDhcpOptions(ctx, rc.state.dhcpOptionsID, rc.commonTags)
	if err != nil {
		return err
	}
	switch len(found) {
	case 1:
		rc.state.dhcpOptionsID = &found[0].DhcpOptionsId
		if err = rc.ensureEC2Tags(ctx, found[0].DhcpOptionsId, found[0].Tags); err != nil {
			return err
		}
	case 0:
		created, err := rc.client.CreateVpcDhcpOptions(ctx, desired)
		if err != nil {
			return err
		}
		rc.state.dhcpOptionsID = &created.DhcpOptionsId
	default:
		return fmt.Errorf("multiple DHCP options found by tags: %d", len(found))
	}

	return nil
}

func (rc *ReconcileContext) ensureEC2Tags(ctx context.Context, id string, current client.Tags) error {
	toBeDeleted := client.Tags{}
	toBeCreated := client.Tags{}
	for k, v := range current {
		if cv, ok := rc.commonTags[k]; ok {
			if cv != v {
				toBeDeleted[k] = v
				toBeCreated[k] = cv
			}
		} else if !ignoreTag(rc.config.IgnoreTags, k) {
			toBeDeleted[k] = v
		}
	}

	if len(toBeDeleted) > 0 {
		if err := rc.client.DeleteEC2Tags(ctx, []string{id}, toBeDeleted); err != nil {
			return err
		}
	}
	if len(toBeCreated) > 0 {
		if err := rc.client.CreateEC2Tags(ctx, []string{id}, toBeCreated); err != nil {
			return err
		}
	}
	return nil
}

func (rc *ReconcileContext) EnsureVpc(ctx context.Context) error {
	desired := &client.VPC{
		Tags:               rc.commonTags,
		EnableDnsSupport:   true,
		EnableDnsHostnames: true,
		DhcpOptionsId:      rc.state.dhcpOptionsID,
	}
	if rc.config.Networks.VPC.CIDR == nil {
		return fmt.Errorf("missing VPC CIDR")
	}
	desired.CidrBlock = *rc.config.Networks.VPC.CIDR
	found, err := rc.client.DescribeVpcs(ctx, rc.state.vpcID, rc.commonTags)
	if err != nil {
		return err
	}
	switch len(found) {
	case 1:
		rc.state.vpcID = &found[0].VpcId
		if _, err := rc.client.UpdateVpc(ctx, desired, found[0]); err != nil {
			return err
		}
		if err = rc.ensureEC2Tags(ctx, found[0].VpcId, found[0].Tags); err != nil {
			return err
		}
	case 0:
		created, err := rc.client.CreateVpc(ctx, desired)
		if err != nil {
			return err
		}
		rc.state.vpcID = &created.VpcId
	default:
		return fmt.Errorf("multiple VPCs found by tags: %d", len(found))
	}

	return nil
}

func ignoreTag(ignoreTags *awsapi.IgnoreTags, key string) bool {
	if ignoreTags == nil {
		return false
	}
	for _, ignoreKey := range ignoreTags.Keys {
		if ignoreKey == key {
			return true
		}
	}
	for _, ignoreKeyPrefix := range ignoreTags.KeyPrefixes {
		if strings.HasPrefix(key, ignoreKeyPrefix) {
			return true
		}
	}
	return false
}
