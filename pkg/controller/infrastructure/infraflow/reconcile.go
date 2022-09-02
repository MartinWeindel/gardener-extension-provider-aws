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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"text/template"
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

func (c *FlowContext) Reconcile(ctx context.Context) error {
	g := c.buildReconcileGraph()
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: c.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (c *FlowContext) buildReconcileGraph() *flow.Graph {
	g := flow.NewGraph("AWS infrastructure reconcilation")

	createVPC := c.config.Networks.VPC.ID == nil
	ensureDhcpOptions := g.Add(flow.Task{
		Name: "ensure DHCP options for VPC",
		Fn: flow.TaskFn(c.ensureDhcpOptions).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(createVPC)})
	ensureVpc := g.Add(flow.Task{
		Name: "ensure VPC",
		Fn: flow.TaskFn(c.PersistingState(c.ensureVpc)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDhcpOptions),
	})
	ensureDefaultSecurityGroup := g.Add(flow.Task{
		Name: "ensure default security group",
		Fn: flow.TaskFn(c.PersistingState(c.ensureDefaultSecurityGroup)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(createVPC),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	ensureInternetGateway := g.Add(flow.Task{
		Name: "ensure internet gateway",
		Fn: flow.TaskFn(c.PersistingState(c.ensureInternetGateway)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(createVPC),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	_ = g.Add(flow.Task{
		Name: "ensure gateway endpoints",
		Fn: flow.TaskFn(c.PersistingState(c.ensureGatewayEndpoints)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureDefaultSecurityGroup, ensureInternetGateway),
	})
	ensureMainRouteTable := g.Add(flow.Task{
		Name: "ensure main route table",
		Fn: flow.TaskFn(c.PersistingState(c.ensureMainRouteTable)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureDefaultSecurityGroup, ensureInternetGateway),
	})
	ensureNodesSecurityGroup := g.Add(flow.Task{
		Name: "ensure nodes security group",
		Fn: flow.TaskFn(c.PersistingState(c.ensureNodesSecurityGroup)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	_ = g.Add(flow.Task{
		Name: "ensure zones resources",
		Fn: flow.TaskFn(c.PersistingState(c.ensureZones)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureNodesSecurityGroup, ensureMainRouteTable),
	})
	ensureIAMRole := g.Add(flow.Task{
		Name: "ensure IAM role",
		Fn: flow.TaskFn(c.PersistingState(c.ensureIAMRole)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	_ = g.Add(flow.Task{
		Name: "ensure IAM instance profile",
		Fn: flow.TaskFn(c.PersistingState(c.ensureIAMInstanceProfile)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureIAMRole),
	})
	_ = g.Add(flow.Task{
		Name: "ensure IAM role policy",
		Fn: flow.TaskFn(c.PersistingState(c.ensureIAMRolePolicy)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureIAMRole),
	})
	_ = g.Add(flow.Task{
		Name: "ensure key pair",
		Fn: flow.TaskFn(c.PersistingState(c.ensureKeyPair)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})

	return g
}

func (c *FlowContext) ensureDhcpOptions(ctx context.Context) error {
	dhcpDomainName := "ec2.internal"
	if c.infra.Spec.Region != "us-east-1" {
		dhcpDomainName = fmt.Sprintf("%s.compute.internal", c.infra.Spec.Region)
	}

	desired := &awsclient.DhcpOptions{
		Tags: c.commonTags,
		DhcpConfigurations: map[string][]string{
			"domain-name":         {dhcpDomainName},
			"domain-name-servers": {"AmazonProvidedDNS"},
		},
	}
	current, err := findExisting(ctx, c.state.Get(IdentifierDHCPOptions), c.commonTags,
		c.client.GetVpcDhcpOptions, c.client.FindVpcDhcpOptionsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		c.state.Set(IdentifierDHCPOptions, current.DhcpOptionsId)
		if _, err := c.updater.UpdateEC2Tags(ctx, current.DhcpOptionsId, c.commonTags, current.Tags); err != nil {
			return err
		}
	} else {
		created, err := c.client.CreateVpcDhcpOptions(ctx, desired)
		if err != nil {
			return err
		}
		c.state.Set(IdentifierDHCPOptions, created.DhcpOptionsId)
	}

	return nil
}

func (c *FlowContext) ensureVpc(ctx context.Context) error {
	if c.config.Networks.VPC.ID != nil {
		c.state.SetPtr(IdentifierVPC, c.config.Networks.VPC.ID)
		return c.ensureExistingVpc(ctx, *c.config.Networks.VPC.ID)
	}
	desired := &awsclient.VPC{
		Tags:               c.commonTags,
		EnableDnsSupport:   true,
		EnableDnsHostnames: true,
		DhcpOptionsId:      c.state.Get(IdentifierDHCPOptions),
	}
	if c.config.Networks.VPC.CIDR == nil {
		return fmt.Errorf("missing VPC CIDR")
	}
	desired.CidrBlock = *c.config.Networks.VPC.CIDR
	current, err := findExisting(ctx, c.state.Get(IdentifierVPC), c.commonTags,
		c.client.GetVpc, c.client.FindVpcsByTags)
	if err != nil {
		return err
	}

	if current != nil {
		c.state.Set(IdentifierVPC, current.VpcId)
		if _, err := c.updater.UpdateVpc(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := c.client.CreateVpc(ctx, desired)
	if err != nil {
		return err
	}
	c.state.Set(IdentifierVPC, created.VpcId)
	if _, err := c.updater.UpdateVpc(ctx, desired, created); err != nil {
		return err
	}
	return nil
}

func (c *FlowContext) ensureExistingVpc(ctx context.Context, vpcID string) error {
	current, err := c.client.GetVpc(ctx, vpcID)
	if err != nil {
		return err
	}
	if current != nil {
		return fmt.Errorf("VPC %s has not been found", vpcID)
	}
	return nil
}

func (c *FlowContext) ensureDefaultSecurityGroup(ctx context.Context) error {
	current, err := c.client.FindDefaultSecurityGroupByVpcId(ctx, *c.state.Get(IdentifierVPC))
	if err != nil {
		return err
	}
	if current == nil {
		return fmt.Errorf("default security group not found")
	}

	c.state.Set(IdentifierDefaultSecurityGroup, current.GroupId)
	desired := current.Clone()
	desired.Rules = nil
	if _, err := c.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
		return err
	}
	return nil
}

func (c *FlowContext) ensureInternetGateway(ctx context.Context) error {
	desired := &awsclient.InternetGateway{
		Tags:  c.commonTags,
		VpcId: c.state.Get(IdentifierVPC),
	}
	current, err := findExisting(ctx, c.state.Get(IdentifierInternetGateway), c.commonTags,
		c.client.GetInternetGateway, c.client.FindInternetGatewaysByTags)
	if err != nil {
		return err
	}
	if current != nil {
		c.state.Set(IdentifierInternetGateway, current.InternetGatewayId)
		if err := c.client.AttachInternetGateway(ctx, *c.state.Get(IdentifierVPC), current.InternetGatewayId); err != nil {
			return err
		}
		if _, err := c.updater.UpdateEC2Tags(ctx, current.InternetGatewayId, c.commonTags, current.Tags); err != nil {
			return err
		}
		return nil
	}

	created, err := c.client.CreateInternetGateway(ctx, desired)
	if err != nil {
		return err
	}
	c.state.Set(IdentifierInternetGateway, created.InternetGatewayId)
	if err := c.client.AttachInternetGateway(ctx, *c.state.Get(IdentifierVPC), created.InternetGatewayId); err != nil {
		return err
	}
	return nil
}

func (c *FlowContext) ensureGatewayEndpoints(ctx context.Context) error {
	child := c.state.GetChild(ChildIdVPCEndpoints)
	var desired []*awsclient.VpcEndpoint
	for _, endpoint := range c.config.Networks.VPC.GatewayEndpoints {
		desired = append(desired, &awsclient.VpcEndpoint{
			Tags:        c.commonTagsWithSuffix(fmt.Sprintf("gw-%s", endpoint)),
			VpcId:       c.state.Get(IdentifierVPC),
			ServiceName: c.vpcEndpointServiceNamePrefix() + endpoint,
		})
	}
	current, err := c.collectExistingVPCEndpoints(ctx)
	if err != nil {
		return err
	}

	toBeDeleted, toBeCreated, toBeChecked := diffByID(desired, current, c.extractVpcEndpointName)
	for _, item := range toBeDeleted {
		if err := c.client.DeleteVpcEndpoint(ctx, item.VpcEndpointId); err != nil {
			return err
		}
		child.SetPtr(c.extractVpcEndpointName(item), nil)
	}
	for _, item := range toBeCreated {
		created, err := c.client.CreateVpcEndpoint(ctx, item)
		if err != nil {
			return err
		}
		child.Set(c.extractVpcEndpointName(item), created.VpcEndpointId)
		if _, err := c.updater.UpdateEC2Tags(ctx, created.VpcEndpointId, item.Tags, created.Tags); err != nil {
			return err
		}
	}
	for _, pair := range toBeChecked {
		child.Set(c.extractVpcEndpointName(pair.current), pair.current.VpcEndpointId)
		if _, err := c.updater.UpdateEC2Tags(ctx, pair.current.VpcEndpointId, pair.desired.Tags, pair.current.Tags); err != nil {
			return err
		}
	}
	return nil
}

func (c *FlowContext) collectExistingVPCEndpoints(ctx context.Context) ([]*awsclient.VpcEndpoint, error) {
	child := c.state.GetChild(ChildIdVPCEndpoints)
	var ids []string
	for _, id := range child.AsMap() {
		ids = append(ids, id)
	}
	var current []*awsclient.VpcEndpoint
	if len(ids) > 0 {
		found, err := c.client.GetVpcEndpoints(ctx, ids)
		if err != nil {
			return nil, err
		}
		current = found
	}
	foundByTags, err := c.client.FindVpcEndpointsByTags(ctx, c.clusterTags())
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

func (c *FlowContext) ensureMainRouteTable(ctx context.Context) error {
	desired := &awsclient.RouteTable{
		Tags:  c.commonTags,
		VpcId: c.state.Get(IdentifierVPC),
		Routes: []*awsclient.Route{
			{
				DestinationCidrBlock: "0.0.0.0/0",
				GatewayId:            c.state.Get(IdentifierInternetGateway),
			},
		},
	}
	current, err := findExisting(ctx, c.state.Get(IdentifierMainRouteTable), c.commonTags,
		c.client.GetRouteTable, c.client.FindRouteTablesByTags)
	if err != nil {
		return err
	}
	if current != nil {
		c.state.Set(IdentifierMainRouteTable, current.RouteTableId)
		c.state.SetObject(ObjectMainRouteTable, current)
		updated, err := c.updater.UpdateRouteTable(ctx, desired, current)
		if err != nil {
			return err
		}
		c.state.SetObject(ObjectMainRouteTable, updated)
		return nil
	}

	created, err := c.client.CreateRouteTable(ctx, desired)
	if err != nil {
		return err
	}
	c.state.Set(IdentifierMainRouteTable, created.RouteTableId)
	c.state.SetObject(ObjectMainRouteTable, created)
	updated, err := c.updater.UpdateRouteTable(ctx, desired, created)
	if err != nil {
		return err
	}
	c.state.SetObject(ObjectMainRouteTable, updated)
	return nil
}

func (c *FlowContext) ensureNodesSecurityGroup(ctx context.Context) error {
	groupName := fmt.Sprintf("%s-nodes", c.infra.Namespace)
	desired := &awsclient.SecurityGroup{
		Tags:        c.commonTagsWithSuffix("nodes"),
		GroupName:   groupName,
		VpcId:       c.state.Get(IdentifierVPC),
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
	for _, zone := range c.config.Networks.Zones {
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
	current, err := findExisting(ctx, c.state.Get(IdentifierNodesSecurityGroup), c.commonTags,
		c.client.GetSecurityGroup, c.client.FindSecurityGroupsByTags,
		func(item *awsclient.SecurityGroup) bool { return item.GroupName == groupName })
	if err != nil {
		return err
	}
	if current != nil {
		c.state.Set(IdentifierNodesSecurityGroup, current.GroupId)
		if _, err := c.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := c.client.CreateSecurityGroup(ctx, desired)
	if err != nil {
		return err
	}
	c.state.Set(IdentifierNodesSecurityGroup, created.GroupId)
	current, err = c.client.GetSecurityGroup(ctx, created.GroupId)
	if err != nil {
		return err
	}
	if _, err := c.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
		return err
	}
	return nil
}

func (c *FlowContext) ensureZones(ctx context.Context) error {
	var desired []*awsclient.Subnet
	for _, zone := range c.config.Networks.Zones {
		suffix := c.subnetSuffix(zone.Name)
		tagsWorkers := c.commonTagsWithSuffix(fmt.Sprintf("nodes-%s", suffix))
		tagsPublic := c.commonTagsWithSuffix(fmt.Sprintf("public-utility-%s", suffix))
		tagsPublic[TagKeyRolePublicELB] = TagValueUse
		tagsPrivate := c.commonTagsWithSuffix(fmt.Sprintf("private-utility-%s", suffix))
		tagsPrivate[TagKeyRolePrivateELB] = TagValueUse
		desired = append(desired,
			&awsclient.Subnet{
				Tags:             tagsWorkers,
				VpcId:            c.state.Get(IdentifierVPC),
				CidrBlock:        zone.Workers,
				AvailabilityZone: zone.Name,
			},
			&awsclient.Subnet{
				Tags:             tagsPublic,
				VpcId:            c.state.Get(IdentifierVPC),
				CidrBlock:        zone.Public,
				AvailabilityZone: zone.Name,
			},
			&awsclient.Subnet{
				Tags:             tagsPrivate,
				VpcId:            c.state.Get(IdentifierVPC),
				CidrBlock:        zone.Internal,
				AvailabilityZone: zone.Name,
			})
	}
	// update flow state if subnet suffixes have been added
	if err := c.PersistState(ctx, true); err != nil {
		return err
	}
	current, err := c.collectExistingSubnets(ctx)
	if err != nil {
		return err
	}
	toBeDeleted, toBeCreated, toBeChecked := diffByID(desired, current, func(item *awsclient.Subnet) string {
		return item.AvailabilityZone + "-" + item.CidrBlock
	})

	g := flow.NewGraph("AWS infrastructure reconcilation: zones")

	c.addZoneDeletionTasksBySubnets(g, toBeDeleted)

	dependencies := newZoneDependencies()
	for _, item := range toBeCreated {
		taskID := c.addSubnetReconcileTasks(g, item, nil)
		dependencies.Insert(item.AvailabilityZone, taskID)
	}
	for _, pair := range toBeChecked {
		taskID := c.addSubnetReconcileTasks(g, pair.desired, pair.current)
		dependencies.Insert(pair.desired.AvailabilityZone, taskID)
	}
	for _, zone := range c.config.Networks.Zones {
		c.addZoneReconcileTasks(g, &zone, dependencies.Get(zone.Name))
	}
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: c.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (c *FlowContext) addZoneDeletionTasksBySubnets(g *flow.Graph, toBeDeleted []*awsclient.Subnet) {
	toBeDeletedZones := sets.NewString()
	for _, item := range toBeDeleted {
		toBeDeletedZones.Insert(getZoneName(item))
	}
	dependencies := newZoneDependencies()
	for zoneName := range toBeDeletedZones {
		taskID := c.addZoneDeletionTasks(g, zoneName)
		dependencies.Insert(zoneName, taskID)
	}
	for _, item := range toBeDeleted {
		c.addSubnetDeletionTasks(g, item, dependencies.Get(item.AvailabilityZone))
	}
}

func (c *FlowContext) collectExistingSubnets(ctx context.Context) ([]*awsclient.Subnet, error) {
	child := c.state.GetChild(ChildIdZones)
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
		found, err := c.client.GetSubnets(ctx, ids)
		if err != nil {
			return nil, err
		}
		current = found
	}
	foundByTags, err := c.client.FindSubnetsByTags(ctx, c.clusterTags())
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

func (c *FlowContext) subnetSuffix(zoneName string) string {
	zoneChild := c.getSubnetZoneChild(zoneName)
	if suffix := zoneChild.Get(IdentifierZoneSuffix); suffix != nil {
		return *suffix
	}
	zones := c.state.GetChild(ChildIdZones)
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

func (c *FlowContext) addSubnetReconcileTasks(g *flow.Graph, desired, current *awsclient.Subnet) flow.TaskID {
	zoneName, subnetKey := c.getSubnetKey(desired)
	suffix := fmt.Sprintf("%s-%s", zoneName, subnetKey)
	return g.Add(flow.Task{
		Name: "ensure subnet " + suffix,
		Fn: flow.TaskFn(c.PersistingState(c.ensureSubnet(desired, current))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
}

func (c *FlowContext) addZoneReconcileTasks(g *flow.Graph, zone *aws.Zone, dependencies flow.TaskIDs) {
	ensureElasticIP := g.Add(flow.Task{
		Name: "ensure NAT gateway elastic IP " + zone.Name,
		Fn: flow.TaskFn(c.PersistingState(c.ensureElasticIP(zone))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies,
	})
	ensureNATGateway := g.Add(flow.Task{
		Name: "ensure NAT gateway " + zone.Name,
		Fn: flow.TaskFn(c.PersistingState(c.ensureNATGateway(zone))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies.Copy().Insert(ensureElasticIP),
	})
	ensureRoutingTable := g.Add(flow.Task{
		Name: "ensure route table " + zone.Name,
		Fn: flow.TaskFn(c.PersistingState(c.ensurePrivateRoutingTable(zone.Name))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies.Copy().Insert(ensureNATGateway),
	})
	g.Add(flow.Task{
		Name: "ensure route table associations " + zone.Name,
		Fn: flow.TaskFn(c.PersistingState(c.ensureRoutingTableAssociations(zone.Name))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies.Copy().Insert(ensureRoutingTable),
	})
}

func (c *FlowContext) addZoneDeletionTasks(g *flow.Graph, zoneName string) flow.TaskID {
	deleteRoutingTableAssocs := g.Add(flow.Task{
		Name: "delete route table associations " + zoneName,
		Fn: flow.TaskFn(c.PersistingState(c.deleteRoutingTableAssociations(zoneName))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	deleteRoutingTable := g.Add(flow.Task{
		Name: "delete route table " + zoneName,
		Fn: flow.TaskFn(c.PersistingState(c.deletePrivateRoutingTable(zoneName))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(deleteRoutingTableAssocs),
	})
	deleteNATGateway := g.Add(flow.Task{
		Name: "delete NAT gateway " + zoneName,
		Fn: flow.TaskFn(c.PersistingState(c.deleteNATGateway(zoneName))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(deleteRoutingTable),
	})
	g.Add(flow.Task{
		Name: "delete NAT gateway elastic IP " + zoneName,
		Fn: flow.TaskFn(c.PersistingState(c.deleteElasticIP(zoneName))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(deleteNATGateway),
	})
	return deleteNATGateway
}

func (c *FlowContext) addSubnetDeletionTasks(g *flow.Graph, item *awsclient.Subnet, dependencies flow.TaskIDs) {
	zoneName, subnetKey := c.getSubnetKey(item)
	suffix := fmt.Sprintf("%s-%s", zoneName, subnetKey)
	g.Add(flow.Task{
		Name: "delete subnet resource " + suffix,
		Fn: flow.TaskFn(c.PersistingState(c.deleteSubnet(item))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies,
	})
}

func (c *FlowContext) deleteSubnet(item *awsclient.Subnet) flow.TaskFn {
	zoneChild := c.getSubnetZoneChildByItem(item)
	_, subnetKey := c.getSubnetKey(item)
	return func(ctx context.Context) error {
		if err := c.client.DeleteSubnet(ctx, item.SubnetId); err != nil {
			return err
		}
		zoneChild.SetAsDeleted(subnetKey)
		return nil
	}
}

func (c *FlowContext) ensureSubnet(desired, current *awsclient.Subnet) flow.TaskFn {
	zoneChild := c.getSubnetZoneChildByItem(desired)
	_, subnetKey := c.getSubnetKey(desired)
	if current == nil {
		return func(ctx context.Context) error {
			created, err := c.client.CreateSubnet(ctx, desired)
			if err != nil {
				return err
			}
			zoneChild.Set(subnetKey, created.SubnetId)
			return nil
		}
	}
	return func(ctx context.Context) error {
		zoneChild.Set(subnetKey, current.SubnetId)
		if _, err := c.updater.UpdateSubnet(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}
}

func (c *FlowContext) ensureElasticIP(zone *aws.Zone) flow.TaskFn {
	return func(ctx context.Context) error {
		if zone.ElasticIPAllocationID != nil {
			return nil
		}
		suffix := c.subnetSuffix(zone.Name)
		eipSuffix := fmt.Sprintf("eip-natgw-%s", suffix)
		child := c.getSubnetZoneChild(zone.Name)
		id := child.Get(IdentifierZoneNATGWElasticIP)
		desired := &awsclient.ElasticIP{
			Tags: c.commonTagsWithSuffix(eipSuffix),
			Vpc:  true,
		}
		current, err := findExisting(ctx, id, desired.Tags, c.client.GetElasticIP, c.client.FindElasticIPsByTags)
		if err != nil {
			return err
		}

		if current != nil {
			child.Set(IdentifierZoneNATGWElasticIP, current.AllocationId)
			if _, err := c.updater.UpdateEC2Tags(ctx, current.AllocationId, desired.Tags, current.Tags); err != nil {
				return err
			}
			return nil
		}

		created, err := c.client.CreateElasticIP(ctx, desired)
		if err != nil {
			return err
		}
		child.Set(IdentifierZoneNATGWElasticIP, created.AllocationId)
		return nil
	}
}

func (c *FlowContext) deleteElasticIP(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		suffix := c.subnetSuffix(zoneName)
		eipSuffix := fmt.Sprintf("eip-natgw-%s", suffix)
		child := c.getSubnetZoneChild(zoneName)
		id := child.Get(IdentifierZoneNATGWElasticIP)
		tags := c.commonTagsWithSuffix(eipSuffix)
		current, err := findExisting(ctx, id, tags, c.client.GetElasticIP, c.client.FindElasticIPsByTags)
		if err != nil {
			return err
		}
		if current != nil {
			if err := c.client.DeleteElasticIP(ctx, current.AllocationId); err != nil {
				return err
			}
		}
		child.SetAsDeleted(IdentifierZoneNATGWElasticIP)
		return nil
	}
}

func (c *FlowContext) ensureNATGateway(zone *aws.Zone) flow.TaskFn {
	return func(ctx context.Context) error {
		child := c.getSubnetZoneChild(zone.Name)
		id := child.Get(IdentifierZoneNATGateway)
		desired := &awsclient.NATGateway{
			Tags:     c.commonTagsWithSuffix(fmt.Sprintf("natgw-%s", c.subnetSuffix(zone.Name))),
			SubnetId: *child.Get(IdentifierZoneSubnetPublic),
		}
		if zone.ElasticIPAllocationID != nil {
			desired.EIPAllocationId = *zone.ElasticIPAllocationID
		} else {
			desired.EIPAllocationId = *child.Get(IdentifierZoneNATGWElasticIP)
		}
		current, err := findExisting(ctx, id, desired.Tags, c.client.GetNATGateway, c.client.FindNATGatewaysByTags)
		if err != nil {
			return err
		}

		if current != nil {
			child.Set(IdentifierZoneNATGateway, current.NATGatewayId)
			if _, err := c.updater.UpdateEC2Tags(ctx, current.NATGatewayId, desired.Tags, current.Tags); err != nil {
				return err
			}
			return nil
		}

		created, err := c.client.CreateNATGateway(ctx, desired)
		if err != nil {
			return err
		}
		child.Set(IdentifierZoneNATGateway, created.NATGatewayId)
		return nil
	}
}

func (c *FlowContext) deleteNATGateway(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		child := c.getSubnetZoneChild(zoneName)
		id := child.Get(IdentifierZoneNATGateway)
		tags := c.commonTagsWithSuffix(fmt.Sprintf("natgw-%s", c.subnetSuffix(zoneName)))
		current, err := findExisting(ctx, id, tags, c.client.GetNATGateway, c.client.FindNATGatewaysByTags)
		if err != nil {
			return err
		}
		if current != nil {
			if err := c.client.DeleteNATGateway(ctx, current.NATGatewayId); err != nil {
				return err
			}
		}
		child.SetAsDeleted(IdentifierZoneNATGateway)
		return nil
	}
}

func (c *FlowContext) ensurePrivateRoutingTable(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		child := c.getSubnetZoneChild(zoneName)
		id := child.Get(IdentifierZoneRouteTable)
		desired := &awsclient.RouteTable{
			Tags:  c.commonTagsWithSuffix(fmt.Sprintf("private-%s", zoneName)),
			VpcId: c.state.Get(IdentifierVPC),
			Routes: []*awsclient.Route{
				{
					DestinationCidrBlock: "0.0.0.0/0",
					NatGatewayId:         child.Get(IdentifierZoneNATGateway),
				},
			},
		}
		current, err := findExisting(ctx, id, desired.Tags, c.client.GetRouteTable, c.client.FindRouteTablesByTags)
		if err != nil {
			return err
		}

		if current != nil {
			child.Set(IdentifierZoneRouteTable, current.RouteTableId)
			child.SetObject(ObjectZoneRouteTable, current)
			if _, err := c.updater.UpdateRouteTable(ctx, desired, current); err != nil {
				return err
			}
			return nil
		}

		created, err := c.client.CreateRouteTable(ctx, desired)
		if err != nil {
			return err
		}
		child.Set(IdentifierZoneRouteTable, created.RouteTableId)
		child.SetObject(ObjectZoneRouteTable, created)
		if _, err := c.updater.UpdateRouteTable(ctx, desired, created); err != nil {
			return err
		}
		return nil
	}
}

func (c *FlowContext) deletePrivateRoutingTable(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		child := c.getSubnetZoneChild(zoneName)
		id := child.Get(IdentifierZoneRouteTable)
		tags := c.commonTagsWithSuffix(fmt.Sprintf("private-%s", zoneName))
		current, err := findExisting(ctx, id, tags, c.client.GetRouteTable, c.client.FindRouteTablesByTags)
		if err != nil {
			return err
		}
		if current != nil {
			if err := c.client.DeleteRouteTable(ctx, current.RouteTableId); err != nil {
				return err
			}
		}
		child.SetAsDeleted(IdentifierZoneRouteTable)
		return nil
	}
}

func (c *FlowContext) ensureRoutingTableAssociations(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		if err := c.ensureZoneRoutingTableAssociation(ctx, zoneName, false,
			IdentifierZoneSubnetPublic, IdentifierZoneSubnetPublicRouteTableAssoc); err != nil {
			return err
		}
		if err := c.ensureZoneRoutingTableAssociation(ctx, zoneName, true,
			IdentifierZoneSubnetPrivate, IdentifierZoneSubnetPrivateRouteTableAssoc); err != nil {
			return err
		}
		if err := c.ensureZoneRoutingTableAssociation(ctx, zoneName, true,
			IdentifierZoneSubnetWorkers, IdentifierZoneSubnetWorkersRouteTableAssoc); err != nil {
			return err
		}
		return nil
	}
}

func (c *FlowContext) ensureZoneRoutingTableAssociation(ctx context.Context, zoneName string,
	zoneRouteTable bool, subnetKey, assocKey string) error {
	child := c.getSubnetZoneChild(zoneName)
	assocID := child.Get(assocKey)
	if assocID != nil {
		return nil
	}
	subnetID := child.Get(subnetKey)
	if subnetID == nil {
		return fmt.Errorf("missing subnet id")
	}
	var obj any
	if zoneRouteTable {
		obj = child.GetObject(ObjectZoneRouteTable)
	} else {
		obj = c.state.GetObject(ObjectMainRouteTable)
	}
	if obj == nil {
		return fmt.Errorf("missing route table object")
	}
	routeTable := obj.(*awsclient.RouteTable)
	for _, assoc := range routeTable.Associations {
		if reflect.DeepEqual(assoc.SubnetId, subnetID) {
			child.Set(assocKey, assoc.RouteTableAssociationId)
			return nil
		}
	}
	assocID, err := c.client.CreateRouteTableAssociation(ctx, routeTable.RouteTableId, *subnetID)
	if err != nil {
		return err
	}
	child.Set(assocKey, *assocID)
	return nil
}

func (c *FlowContext) deleteRoutingTableAssociations(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		if err := c.deleteZoneRoutingTableAssociation(ctx, zoneName, false,
			IdentifierZoneSubnetPublic, IdentifierZoneSubnetPublicRouteTableAssoc); err != nil {
			return err
		}
		if err := c.deleteZoneRoutingTableAssociation(ctx, zoneName, true,
			IdentifierZoneSubnetPrivate, IdentifierZoneSubnetPrivateRouteTableAssoc); err != nil {
			return err
		}
		if err := c.deleteZoneRoutingTableAssociation(ctx, zoneName, true,
			IdentifierZoneSubnetWorkers, IdentifierZoneSubnetWorkersRouteTableAssoc); err != nil {
			return err
		}
		return nil
	}
}

func (c *FlowContext) deleteZoneRoutingTableAssociation(ctx context.Context, zoneName string,
	zoneRouteTable bool, subnetKey, assocKey string) error {
	child := c.getSubnetZoneChild(zoneName)
	if child.IsAlreadyDeleted(assocKey) {
		return nil
	}
	subnetID := child.Get(subnetKey)
	if subnetID == nil {
		return fmt.Errorf("missing subnet id")
	}
	assocID := child.Get(assocKey)
	if assocID == nil {
		// unclear situation: load route table to search for association
		var routeTableID *string
		if zoneRouteTable {
			routeTableID = child.Get(IdentifierZoneRouteTable)
		} else {
			routeTableID = c.state.Get(IdentifierMainRouteTable)
		}
		if routeTableID != nil {
			routeTable, err := c.client.GetRouteTable(ctx, *routeTableID)
			if err != nil {
				return err
			}
			for _, assoc := range routeTable.Associations {
				if reflect.DeepEqual(subnetID, assoc.SubnetId) {
					assocID = &assoc.RouteTableAssociationId
					break
				}
			}
		}
	}
	if assocID == nil {
		child.SetAsDeleted(assocKey)
		return nil
	}
	if err := c.client.DeleteRouteTableAssociation(ctx, *assocID); err != nil {
		return err
	}
	child.SetAsDeleted(assocKey)
	return nil
}

func (c *FlowContext) ensureIAMRole(ctx context.Context) error {
	desired := &awsclient.IAMRole{
		RoleName: fmt.Sprintf("%s-nodes", c.infra.Namespace),
		Path:     "/",
		AssumeRolePolicyDocument: `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}`,
	}
	current, err := c.client.GetIAMRole(ctx, desired.RoleName)
	if err != nil {
		return err
	}

	if current != nil {
		c.state.Set(IdentifierIAMRole, current.RoleId)
		return nil
	}

	created, err := c.client.CreateIAMRole(ctx, desired)
	if err != nil {
		return err
	}
	c.state.Set(IdentifierIAMRole, created.RoleId)
	return nil
}

func (c *FlowContext) ensureIAMInstanceProfile(ctx context.Context) error {
	desired := &awsclient.IAMInstanceProfile{
		InstanceProfileName: fmt.Sprintf("%s-nodes", c.infra.Namespace),
		Path:                "/",
		RoleName:            fmt.Sprintf("%s-nodes", c.infra.Namespace),
	}
	current, err := c.client.GetIAMInstanceProfile(ctx, desired.InstanceProfileName)
	if err != nil {
		return err
	}

	if current != nil {
		c.state.Set(IdentifierIAMInstanceProfile, current.InstanceProfileId)
		if _, err := c.updater.UpdateIAMInstanceProfile(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := c.client.CreateIAMInstanceProfile(ctx, desired)
	if err != nil {
		return err
	}
	c.state.Set(IdentifierIAMInstanceProfile, created.InstanceProfileId)
	if _, err := c.updater.UpdateIAMInstanceProfile(ctx, desired, created); err != nil {
		return err
	}
	return nil
}

const iamRolePolicyTemplate = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeInstances"
      ],
      "Resource": [
        "*"
      ]
    }{{ if .enableECRAccess }},
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:GetRepositoryPolicy",
        "ecr:DescribeRepositories",
        "ecr:ListImages",
        "ecr:BatchGetImage"
      ],
      "Resource": [
        "*"
      ]
    }{{ end }}
  ]
}`

func (c *FlowContext) ensureIAMRolePolicy(ctx context.Context) error {
	enableECRAccess := true
	if v := c.config.EnableECRAccess; v != nil {
		enableECRAccess = *v
	}
	t, err := template.New("policyDocument").Parse(iamRolePolicyTemplate)
	if err != nil {
		return fmt.Errorf("parsing policyDocument template failed: %s", err)
	}
	var buffer bytes.Buffer
	if err := t.Execute(&buffer, map[string]any{"enableECRAccess": enableECRAccess}); err != nil {
		return fmt.Errorf("executing policyDocument template failed: %s", err)
	}

	desired := &awsclient.IAMRolePolicy{
		PolicyName:     fmt.Sprintf("%s-nodes", c.infra.Namespace),
		RoleName:       fmt.Sprintf("%s-nodes", c.infra.Namespace),
		PolicyDocument: buffer.String(),
	}
	current, err := c.client.GetIAMRolePolicy(ctx, desired.PolicyName, desired.RoleName)
	if err != nil {
		return err
	}

	if current != nil {
		c.state.Set(IdentifierIAMRolePolicy, "true")
		if current.PolicyDocument != desired.PolicyDocument {
			if err := c.client.PutIAMRolePolicy(ctx, desired); err != nil {
				return err
			}
		}
		return nil
	}

	if err := c.client.PutIAMRolePolicy(ctx, desired); err != nil {
		return err
	}
	c.state.Set(IdentifierIAMRolePolicy, "true")
	return nil
}

func (c *FlowContext) ensureKeyPair(ctx context.Context) error {
	desired := &awsclient.KeyPairInfo{
		Tags:    c.commonTags,
		KeyName: fmt.Sprintf("%s-ssh-publickey", c.infra.Namespace),
	}
	current, err := c.client.GetKeyPair(ctx, desired.KeyName)
	if err != nil {
		return err
	}

	if current != nil {
		c.state.Set(IdentifierKeyPair, desired.KeyName)
		return nil
	}

	if _, err := c.client.ImportKeyPair(ctx, desired.KeyName, c.infra.Spec.SSHPublicKey, c.commonTags); err != nil {
		return err
	}
	c.state.Set(IdentifierKeyPair, desired.KeyName)
	return nil
}

func (c *FlowContext) getSubnetZoneChildByItem(item *awsclient.Subnet) state.Whiteboard {
	return c.getSubnetZoneChild(getZoneName(item))
}

func (c *FlowContext) getSubnetZoneChild(zoneName string) state.Whiteboard {
	return c.state.GetChild(ChildIdZones).GetChild(zoneName)
}

func (c *FlowContext) getSubnetKey(item *awsclient.Subnet) (zoneName, subnetKey string) {
	zone := c.getZone(item)
	if zone == nil {
		return
	}
	zoneName = zone.Name
	switch item.CidrBlock {
	case zone.Workers:
		subnetKey = IdentifierZoneSubnetWorkers
	case zone.Public:
		subnetKey = IdentifierZoneSubnetPublic
	case zone.Internal:
		subnetKey = IdentifierZoneSubnetPrivate
	}
	return
}

func (c *FlowContext) getZone(item *awsclient.Subnet) *aws.Zone {
	zoneName := getZoneName(item)
	for _, zone := range c.config.Networks.Zones {
		if zone.Name == zoneName {
			return &zone
		}
	}
	return nil
}

func getZoneName(item *awsclient.Subnet) string {
	return item.AvailabilityZone
}
