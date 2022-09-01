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

	"github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws"
	awsclient "github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	"github.com/gardener/gardener-extension-provider-aws/pkg/controller/infrastructure/infraflow/state"
	"github.com/gardener/gardener/pkg/utils/flow"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/pointer"
)

const (
	FlowStateVersion1    = "1"
	defaultRetryInterval = 5 * time.Second
	defaultRetryTimeout  = 2 * time.Minute
)

func (rc *ReconcileContext) Reconcile(ctx context.Context) error {
	g := rc.buildReconcileGraph()
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: rc.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
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
	ensureMainRouteTable := g.Add(flow.Task{
		Name: "ensure main route table",
		Fn: flow.TaskFn(rc.EnsureMainRouteTable).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureDefaultSecurityGroup, ensureInternetGateway),
	})
	ensureNodesSecurityGroup := g.Add(flow.Task{
		Name: "ensure nodes security group",
		Fn: flow.TaskFn(rc.EnsureNodesSecurityGroup).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	ensureSubnets := g.Add(flow.Task{
		Name: "ensure subnets",
		Fn: flow.TaskFn(rc.EnsureSubnets).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureNodesSecurityGroup, ensureMainRouteTable),
	})
	unused(ensureSubnets)
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
	current, err := findExisting(ctx, rc.state.Get(IdentiferDHCPOptions), rc.commonTags,
		rc.client.GetVpcDhcpOptions, rc.client.FindVpcDhcpOptionsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.Set(IdentiferDHCPOptions, current.DhcpOptionsId)
		if _, err := rc.updater.UpdateEC2Tags(ctx, current.DhcpOptionsId, rc.commonTags, current.Tags); err != nil {
			return err
		}
	} else {
		created, err := rc.client.CreateVpcDhcpOptions(ctx, desired)
		if err != nil {
			return err
		}
		rc.state.Set(IdentiferDHCPOptions, created.DhcpOptionsId)
	}

	return nil
}

func (rc *ReconcileContext) EnsureVpc(ctx context.Context) error {
	if rc.config.Networks.VPC.ID != nil {
		rc.state.SetPtr(IdentiferVPC, rc.config.Networks.VPC.ID)
		return rc.ensureExistingVpc(ctx, *rc.config.Networks.VPC.ID)
	}
	desired := &awsclient.VPC{
		Tags:               rc.commonTags,
		EnableDnsSupport:   true,
		EnableDnsHostnames: true,
		DhcpOptionsId:      rc.state.Get(IdentiferDHCPOptions),
	}
	if rc.config.Networks.VPC.CIDR == nil {
		return fmt.Errorf("missing VPC CIDR")
	}
	desired.CidrBlock = *rc.config.Networks.VPC.CIDR
	current, err := findExisting(ctx, rc.state.Get(IdentiferVPC), rc.commonTags,
		rc.client.GetVpc, rc.client.FindVpcsByTags)
	if err != nil {
		return err
	}

	if current != nil {
		rc.state.Set(IdentiferVPC, current.VpcId)
		if _, err := rc.updater.UpdateVpc(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := rc.client.CreateVpc(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.Set(IdentiferVPC, created.VpcId)
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
	current, err := rc.client.FindDefaultSecurityGroupByVpcId(ctx, *rc.state.Get(IdentiferVPC))
	if err != nil {
		return err
	}
	if current == nil {
		return fmt.Errorf("default security group not found")
	}

	rc.state.Set(IdentiferDefaultSecurityGroup, current.GroupId)
	desired := current.Clone()
	desired.Rules = nil
	if _, err := rc.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
		return err
	}
	return nil
}

func (rc *ReconcileContext) EnsureInternetGateway(ctx context.Context) error {
	desired := &awsclient.InternetGateway{
		Tags:  rc.commonTags,
		VpcId: rc.state.Get(IdentiferVPC),
	}
	current, err := findExisting(ctx, rc.state.Get(IdentiferInternetGateway), rc.commonTags,
		rc.client.GetInternetGateway, rc.client.FindInternetGatewaysByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.Set(IdentiferInternetGateway, current.InternetGatewayId)
		if err := rc.client.AttachInternetGateway(ctx, *rc.state.Get(IdentiferVPC), current.InternetGatewayId); err != nil {
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
	rc.state.Set(IdentiferInternetGateway, created.InternetGatewayId)
	if err := rc.client.AttachInternetGateway(ctx, *rc.state.Get(IdentiferVPC), created.InternetGatewayId); err != nil {
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
			VpcId:       rc.state.Get(IdentiferVPC),
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
		child.SetPtr(rc.extractVpcEndpointName(item), nil)
	}
	for _, item := range toBeCreated {
		created, err := rc.client.CreateVpcEndpoint(ctx, item)
		if err != nil {
			return err
		}
		child.Set(rc.extractVpcEndpointName(item), created.VpcEndpointId)
		if _, err := rc.updater.UpdateEC2Tags(ctx, created.VpcEndpointId, item.Tags, created.Tags); err != nil {
			return err
		}
	}
	for _, pair := range toBeChecked {
		child.Set(rc.extractVpcEndpointName(pair.current), pair.current.VpcEndpointId)
		if _, err := rc.updater.UpdateEC2Tags(ctx, pair.current.VpcEndpointId, pair.desired.Tags, pair.current.Tags); err != nil {
			return err
		}
	}
	return nil
}

func (rc *ReconcileContext) collectExistingVPCEndpoints(ctx context.Context) ([]*awsclient.VpcEndpoint, error) {
	child := rc.state.GetChild(ChildIdVPCEndpoints)
	var ids []string
	for _, id := range child.AsMap() {
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

func (rc *ReconcileContext) EnsureMainRouteTable(ctx context.Context) error {
	desired := &awsclient.RouteTable{
		Tags:  rc.commonTags,
		VpcId: rc.state.Get(IdentiferVPC),
		Routes: []*awsclient.Route{
			{
				DestinationCidrBlock: "0.0.0.0/0",
				GatewayId:            rc.state.Get(IdentiferInternetGateway),
			},
		},
	}
	current, err := findExisting(ctx, rc.state.Get(IdentiferMainRouteTable), rc.commonTags,
		rc.client.GetRouteTable, rc.client.FindRouteTablesByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.Set(IdentiferMainRouteTable, current.RouteTableId)
		if _, err := rc.updater.UpdateEC2Tags(ctx, current.RouteTableId, rc.commonTags, current.Tags); err != nil {
			return err
		}
		return nil
	}

	created, err := rc.client.CreateRouteTable(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.Set(IdentiferMainRouteTable, created.RouteTableId)
	return nil
}

func (rc *ReconcileContext) EnsureNodesSecurityGroup(ctx context.Context) error {
	groupName := fmt.Sprintf("%s-nodes", rc.infra.Namespace)
	desired := &awsclient.SecurityGroup{
		Tags:        rc.commonTagsWithSuffix("nodes"),
		GroupName:   groupName,
		VpcId:       rc.state.Get(IdentiferVPC),
		Description: pointer.String("Security group for nodes"),
		Rules: []*awsclient.SecurityGroupRule{
			{
				Type:     awsclient.SecurityGroupRuleTypeIngress,
				Protocol: "-1",
				Self:     true,
			},
			{
				Type:       awsclient.SecurityGroupRuleTypeIngress,
				FromPort:   30000,
				ToPort:     32767,
				Protocol:   "tcp",
				CidrBlocks: []string{"0.0.0.0/0"},
			},
			{
				Type:       awsclient.SecurityGroupRuleTypeIngress,
				FromPort:   30000,
				ToPort:     32767,
				Protocol:   "udp",
				CidrBlocks: []string{"0.0.0.0/0"},
			},
			{
				Type:       awsclient.SecurityGroupRuleTypeEgress,
				Protocol:   "-1",
				CidrBlocks: []string{"0.0.0.0/0"},
			},
		},
	}
	for _, zone := range rc.config.Networks.Zones {
		desired.Rules = append(desired.Rules,
			&awsclient.SecurityGroupRule{
				Type:       awsclient.SecurityGroupRuleTypeIngress,
				FromPort:   30000,
				ToPort:     32767,
				Protocol:   "tcp",
				CidrBlocks: []string{zone.Internal},
			},
			&awsclient.SecurityGroupRule{
				Type:       awsclient.SecurityGroupRuleTypeIngress,
				FromPort:   30000,
				ToPort:     32767,
				Protocol:   "udp",
				CidrBlocks: []string{zone.Internal},
			},
			&awsclient.SecurityGroupRule{
				Type:       awsclient.SecurityGroupRuleTypeIngress,
				FromPort:   30000,
				ToPort:     32767,
				Protocol:   "tcp",
				CidrBlocks: []string{zone.Public},
			},
			&awsclient.SecurityGroupRule{
				Type:       awsclient.SecurityGroupRuleTypeIngress,
				FromPort:   30000,
				ToPort:     32767,
				Protocol:   "udp",
				CidrBlocks: []string{zone.Public},
			})
	}
	current, err := findExisting(ctx, rc.state.Get(IdentiferNodesSecurityGroup), rc.commonTags,
		rc.client.GetSecurityGroup, rc.client.FindSecurityGroupsByTags,
		func(item *awsclient.SecurityGroup) bool { return item.GroupName == groupName })
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.Set(IdentiferNodesSecurityGroup, current.GroupId)
		if _, err := rc.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := rc.client.CreateSecurityGroup(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.Set(IdentiferNodesSecurityGroup, created.GroupId)
	current, err = rc.client.GetSecurityGroup(ctx, created.GroupId)
	if err != nil {
		return err
	}
	if _, err := rc.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
		return err
	}
	return nil
}

func (rc *ReconcileContext) EnsureSubnets(ctx context.Context) error {
	var desired []*awsclient.Subnet
	for _, zone := range rc.config.Networks.Zones {
		suffix := rc.subnetSuffix(zone.Name)
		tagsWorkers := rc.commonTagsWithSuffix(fmt.Sprintf("nodes-%s", suffix))
		tagsPublic := rc.commonTagsWithSuffix(fmt.Sprintf("public-utility-%s", suffix))
		tagsPublic[TagKeyRolePublicELB] = TagValueUse
		tagsPrivate := rc.commonTagsWithSuffix(fmt.Sprintf("private-utility-%s", suffix))
		tagsPrivate[TagKeyRolePrivateELB] = TagValueUse
		desired = append(desired,
			&awsclient.Subnet{
				Tags:             tagsWorkers,
				VpcId:            rc.state.Get(IdentiferVPC),
				CidrBlock:        zone.Workers,
				AvailabilityZone: zone.Name,
			},
			&awsclient.Subnet{
				Tags:             tagsPublic,
				VpcId:            rc.state.Get(IdentiferVPC),
				CidrBlock:        zone.Public,
				AvailabilityZone: zone.Name,
			},
			&awsclient.Subnet{
				Tags:             tagsPrivate,
				VpcId:            rc.state.Get(IdentiferVPC),
				CidrBlock:        zone.Internal,
				AvailabilityZone: zone.Name,
			})
	}
	// update flow state if subnet suffixes have been added
	if err := rc.PersistFlowState(ctx); err != nil {
		return err
	}
	current, err := rc.collectExistingSubnets(ctx)
	if err != nil {
		return err
	}
	toBeDeleted, toBeCreated, toBeChecked := diffByID(desired, current, getZoneName)
	g := flow.NewGraph("AWS infrastructure reconcilation: subnets")
	for _, item := range toBeDeleted {
		rc.addSubnetDeletionTasks(g, item)
	}
	for _, item := range toBeCreated {
		rc.addSubnetReconcileTasks(g, item, nil)
	}
	for _, pair := range toBeChecked {
		rc.addSubnetReconcileTasks(g, pair.desired, pair.current)
	}
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: rc.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (rc *ReconcileContext) collectExistingSubnets(ctx context.Context) ([]*awsclient.Subnet, error) {
	child := rc.state.GetChild(ChildIdZones)
	var ids []string
	for _, zoneKey := range child.GetChildrenKeys() {
		zoneChild := child.GetChild(zoneKey)
		if id := zoneChild.Get(IdentifierZoneSubnetWorkers); id != nil {
			ids = append(ids, *id)
		}
		if id := zoneChild.Get(IdentifierZoneSubnetPublic); id != nil {
			ids = append(ids, *id)
		}
		if id := zoneChild.Get(IdentifierZoneSubnetPrivate); id != nil {
			ids = append(ids, *id)
		}
	}
	var current []*awsclient.Subnet
	if len(ids) > 0 {
		found, err := rc.client.GetSubnets(ctx, ids)
		if err != nil {
			return nil, err
		}
		current = found
	}
	foundByTags, err := rc.client.FindSubnetsByTags(ctx, rc.clusterTags())
	if err != nil {
		return nil, err
	}
outer:
	for _, item := range foundByTags {
		for _, currentItem := range current {
			if item.SubnetId == currentItem.SubnetId {
				continue outer
			}
		}
		current = append(current, item)
	}
	return current, nil
}

func (rc *ReconcileContext) subnetSuffix(zoneName string) string {
	zoneChild := rc.getSubnetZoneChild(zoneName)
	if suffix := zoneChild.Get(IdentifierZoneSuffix); suffix != nil {
		return *suffix
	}
	zones := rc.state.GetChild(ChildIdZones)
	existing := sets.String{}
	for _, key := range zones.GetChildrenKeys() {
		otherChild := zones.GetChild(key)
		if suffix := otherChild.Get(IdentifierZoneSuffix); suffix != nil {
			existing.Insert(*suffix)
		}
	}
	for i := 0; ; i++ {
		suffix := fmt.Sprintf("z%d", i)
		if !existing.Has(suffix) {
			zoneChild.Set(IdentifierZoneSuffix, suffix)
			return suffix
		}
	}
}

func (rc *ReconcileContext) addSubnetReconcileTasks(g *flow.Graph, desired, current *awsclient.Subnet) {
	subnetKey := rc.getSubnetKey(desired)
	suffix := fmt.Sprintf("%s-%s", getZoneName(desired), subnetKey)
	isPublicSubnet := subnetKey == IdentifierZoneSubnetPublic
	ensureSubnet := g.Add(flow.Task{
		Name: "ensure subnet resource " + suffix,
		Fn: flow.TaskFn(rc.EnsureSubnet(desired, current)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	ensureElasticIP := g.Add(flow.Task{
		Name: "ensure NAT gateway elastic IP " + suffix,
		Fn: flow.TaskFn(rc.EnsureElasticIP(desired)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(isPublicSubnet),
	})
	g.Add(flow.Task{
		Name: "ensure NAT gateway " + suffix,
		Fn: flow.TaskFn(rc.EnsureNATGateway(desired)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(isPublicSubnet),
		Dependencies: flow.NewTaskIDs(ensureSubnet, ensureElasticIP),
	})
}

func (rc *ReconcileContext) addSubnetDeletionTasks(g *flow.Graph, item *awsclient.Subnet) {
	subnetKey := rc.getSubnetKey(item)
	suffix := fmt.Sprintf("%s-%s", getZoneName(item), subnetKey)
	isPublicSubnet := subnetKey == IdentifierZoneSubnetPublic
	ensureDeletedNATGateway := g.Add(flow.Task{
		Name: "ensure deletion of NAT gateway " + suffix,
		Fn: flow.TaskFn(rc.EnsureDeletedNATGateway(item)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(isPublicSubnet),
	})
	g.Add(flow.Task{
		Name: "ensure deletion of NAT gateway elastic IP " + suffix,
		Fn: flow.TaskFn(rc.EnsureDeletedElasticIP(item)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(isPublicSubnet),
		Dependencies: flow.NewTaskIDs(ensureDeletedNATGateway),
	})
	g.Add(flow.Task{
		Name: "ensure deletion of subnet resource " + suffix,
		Fn: flow.TaskFn(rc.EnsureDeletedSubnet(item)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDeletedNATGateway),
	})
}

func (rc *ReconcileContext) EnsureDeletedSubnet(item *awsclient.Subnet) flow.TaskFn {
	zoneChild := rc.getSubnetZoneChildByItem(item)
	subnetKey := rc.getSubnetKey(item)
	return func(ctx context.Context) error {
		if err := rc.client.DeleteSubnet(ctx, item.SubnetId); err != nil {
			return err
		}
		zoneChild.SetAsDeleted(subnetKey)
		return nil
	}
}

func (rc *ReconcileContext) EnsureSubnet(desired, current *awsclient.Subnet) flow.TaskFn {
	zoneChild := rc.getSubnetZoneChildByItem(desired)
	subnetKey := rc.getSubnetKey(desired)
	if current == nil {
		return func(ctx context.Context) error {
			created, err := rc.client.CreateSubnet(ctx, desired)
			if err != nil {
				return err
			}
			zoneChild.Set(subnetKey, created.SubnetId)
			return nil
		}
	}
	return func(ctx context.Context) error {
		zoneChild.Set(subnetKey, current.SubnetId)
		if _, err := rc.updater.UpdateSubnet(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}
}

func (rc *ReconcileContext) EnsureElasticIP(desired *awsclient.Subnet) flow.TaskFn {
	return func(ctx context.Context) error {
		zone := rc.getZone(desired)
		if zone == nil || zone.ElasticIPAllocationID != nil {
			return nil
		}
		suffix := rc.subnetSuffix(zone.Name)
		eipSuffix := fmt.Sprintf("eip-natgw-%s", suffix)
		child := rc.getSubnetZoneChild(zone.Name)
		id := child.Get(IdentifierZoneNATGWElasticIP)
		tags := rc.commonTagsWithSuffix(eipSuffix)
		current, err := findExisting(ctx, id, tags, rc.client.GetElasticIP, rc.client.FindElasticIPsByTags)
		if err != nil {
			return err
		}
		if current != nil {
			child.Set(IdentifierZoneNATGWElasticIP, current.AllocationId)
			if _, err := rc.updater.UpdateEC2Tags(ctx, current.AllocationId, tags, current.Tags); err != nil {
				return err
			}
		}
		desiredEIP := &awsclient.ElasticIP{
			Tags: tags,
			Vpc:  true,
		}
		created, err := rc.client.CreateElasticIP(ctx, desiredEIP)
		if err != nil {
			return err
		}
		child.Set(IdentifierZoneNATGWElasticIP, created.AllocationId)
		return rc.PersistFlowState(ctx)
	}
}

func (rc *ReconcileContext) EnsureDeletedElasticIP(desired *awsclient.Subnet) flow.TaskFn {
	return func(ctx context.Context) error {
		zone := rc.getZone(desired)
		if zone == nil || zone.ElasticIPAllocationID != nil {
			return nil
		}
		suffix := rc.subnetSuffix(zone.Name)
		eipSuffix := fmt.Sprintf("eip-natgw-%s", suffix)
		child := rc.getSubnetZoneChild(zone.Name)
		id := child.Get(IdentifierZoneNATGWElasticIP)
		tags := rc.commonTagsWithSuffix(eipSuffix)
		current, err := findExisting(ctx, id, tags, rc.client.GetElasticIP, rc.client.FindElasticIPsByTags)
		if err != nil {
			return err
		}
		if current != nil {
			if err := rc.client.DeleteElasticIP(ctx, current.AllocationId); err != nil {
				return err
			}
		}
		child.SetAsDeleted(IdentifierZoneNATGWElasticIP)
		return nil
	}
}

func (rc *ReconcileContext) EnsureNATGateway(desiredSubnet *awsclient.Subnet) flow.TaskFn {
	return func(ctx context.Context) error {
		zoneName := getZoneName(desiredSubnet)
		subnetKey := rc.getSubnetKey(desiredSubnet)
		child := rc.getSubnetZoneChild(zoneName)
		id := child.Get(IdentifierZoneNATGateway)
		tags := rc.commonTagsWithSuffix(fmt.Sprintf("natgw-%s", rc.subnetSuffix(zoneName)))
		current, err := findExisting(ctx, id, tags, rc.client.GetNATGateway, rc.client.FindNATGatewaysByTags)
		if err != nil {
			return err
		}
		if current != nil {
			child.Set(IdentifierZoneNATGateway, current.NATGatewayId)
			if _, err := rc.updater.UpdateEC2Tags(ctx, current.NATGatewayId, tags, current.Tags); err != nil {
				return err
			}
		}
		desired := &awsclient.NATGateway{
			Tags:     tags,
			SubnetId: *child.Get(subnetKey),
		}
		zone := rc.getZone(desiredSubnet)
		if zone != nil && zone.ElasticIPAllocationID != nil {
			desired.EIPAllocationId = *zone.ElasticIPAllocationID
		} else if child.Get(IdentifierZoneNATGWElasticIP) != nil {
			desired.EIPAllocationId = *child.Get(IdentifierZoneNATGWElasticIP)
		}
		created, err := rc.client.CreateNATGateway(ctx, desired)
		if err != nil {
			return err
		}
		child.Set(IdentifierZoneNATGateway, created.NATGatewayId)
		return nil
	}
}

func (rc *ReconcileContext) EnsureDeletedNATGateway(desiredSubnet *awsclient.Subnet) flow.TaskFn {
	return func(ctx context.Context) error {
		zoneName := getZoneName(desiredSubnet)
		child := rc.getSubnetZoneChild(zoneName)
		id := child.Get(IdentifierZoneNATGateway)
		tags := rc.commonTagsWithSuffix(fmt.Sprintf("natgw-%s", rc.subnetSuffix(zoneName)))
		current, err := findExisting(ctx, id, tags, rc.client.GetNATGateway, rc.client.FindNATGatewaysByTags)
		if err != nil {
			return err
		}
		if current != nil {
			if err := rc.client.DeleteNATGateway(ctx, current.NATGatewayId); err != nil {
				return err
			}
		}
		child.SetAsDeleted(IdentifierZoneNATGateway)
		return nil
	}
}

func (rc *ReconcileContext) getSubnetZoneChildByItem(item *awsclient.Subnet) state.Whiteboard {
	return rc.getSubnetZoneChild(getZoneName(item))
}

func (rc *ReconcileContext) getSubnetZoneChild(zoneName string) state.Whiteboard {
	return rc.state.GetChild(ChildIdZones).GetChild(zoneName)
}

func (rc *ReconcileContext) getSubnetKey(item *awsclient.Subnet) string {
	zone := rc.getZone(item)
	if zone == nil {
		return "?unknown-zone?"
	}
	if zone.Workers == item.CidrBlock {
		return IdentifierZoneSubnetWorkers
	}
	if zone.Public == item.CidrBlock {
		return IdentifierZoneSubnetPublic
	}
	if zone.Internal == item.CidrBlock {
		return IdentifierZoneSubnetPrivate
	}
	return "?unknown-subnet?"
}

func (rc *ReconcileContext) getZone(item *awsclient.Subnet) *aws.Zone {
	zoneName := getZoneName(item)
	for _, zone := range rc.config.Networks.Zones {
		if zone.Name == zoneName {
			return &zone
		}
	}
	return nil
}

func getZoneName(item *awsclient.Subnet) string {
	return item.AvailabilityZone
}
