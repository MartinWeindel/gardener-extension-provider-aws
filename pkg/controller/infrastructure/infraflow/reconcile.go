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
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureVpc)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDhcpOptions),
	})
	ensureDefaultSecurityGroup := g.Add(flow.Task{
		Name: "ensure default security group",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureDefaultSecurityGroup)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(createVPC),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	ensureInternetGateway := g.Add(flow.Task{
		Name: "ensure internet gateway",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureInternetGateway)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout).
			DoIf(createVPC),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	_ = g.Add(flow.Task{
		Name: "ensure gateway endpoints",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureGatewayEndpoints)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureDefaultSecurityGroup, ensureInternetGateway),
	})
	ensureMainRouteTable := g.Add(flow.Task{
		Name: "ensure main route table",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureMainRouteTable)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureDefaultSecurityGroup, ensureInternetGateway),
	})
	ensureNodesSecurityGroup := g.Add(flow.Task{
		Name: "ensure nodes security group",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureNodesSecurityGroup)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc),
	})
	_ = g.Add(flow.Task{
		Name: "ensure zones resources",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureZones)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureVpc, ensureNodesSecurityGroup, ensureMainRouteTable),
	})
	ensureIAMRole := g.Add(flow.Task{
		Name: "ensure IAM role",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureIAMRole)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	_ = g.Add(flow.Task{
		Name: "ensure IAM instance profile",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureIAMInstanceProfile)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureIAMRole),
	})
	_ = g.Add(flow.Task{
		Name: "ensure IAM role policy",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureIAMRolePolicy)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureIAMRole),
	})
	_ = g.Add(flow.Task{
		Name: "ensure key pair",
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureKeyPair)).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})

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
	current, err := findExisting(ctx, rc.state.Get(IdentifierDHCPOptions), rc.commonTags,
		rc.client.GetVpcDhcpOptions, rc.client.FindVpcDhcpOptionsByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.Set(IdentifierDHCPOptions, current.DhcpOptionsId)
		if _, err := rc.updater.UpdateEC2Tags(ctx, current.DhcpOptionsId, rc.commonTags, current.Tags); err != nil {
			return err
		}
	} else {
		created, err := rc.client.CreateVpcDhcpOptions(ctx, desired)
		if err != nil {
			return err
		}
		rc.state.Set(IdentifierDHCPOptions, created.DhcpOptionsId)
	}

	return nil
}

func (rc *ReconcileContext) EnsureVpc(ctx context.Context) error {
	if rc.config.Networks.VPC.ID != nil {
		rc.state.SetPtr(IdentifierVPC, rc.config.Networks.VPC.ID)
		return rc.ensureExistingVpc(ctx, *rc.config.Networks.VPC.ID)
	}
	desired := &awsclient.VPC{
		Tags:               rc.commonTags,
		EnableDnsSupport:   true,
		EnableDnsHostnames: true,
		DhcpOptionsId:      rc.state.Get(IdentifierDHCPOptions),
	}
	if rc.config.Networks.VPC.CIDR == nil {
		return fmt.Errorf("missing VPC CIDR")
	}
	desired.CidrBlock = *rc.config.Networks.VPC.CIDR
	current, err := findExisting(ctx, rc.state.Get(IdentifierVPC), rc.commonTags,
		rc.client.GetVpc, rc.client.FindVpcsByTags)
	if err != nil {
		return err
	}

	if current != nil {
		rc.state.Set(IdentifierVPC, current.VpcId)
		if _, err := rc.updater.UpdateVpc(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := rc.client.CreateVpc(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.Set(IdentifierVPC, created.VpcId)
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
	current, err := rc.client.FindDefaultSecurityGroupByVpcId(ctx, *rc.state.Get(IdentifierVPC))
	if err != nil {
		return err
	}
	if current == nil {
		return fmt.Errorf("default security group not found")
	}

	rc.state.Set(IdentifierDefaultSecurityGroup, current.GroupId)
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
		VpcId: rc.state.Get(IdentifierVPC),
	}
	current, err := findExisting(ctx, rc.state.Get(IdentifierInternetGateway), rc.commonTags,
		rc.client.GetInternetGateway, rc.client.FindInternetGatewaysByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.Set(IdentifierInternetGateway, current.InternetGatewayId)
		if err := rc.client.AttachInternetGateway(ctx, *rc.state.Get(IdentifierVPC), current.InternetGatewayId); err != nil {
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
	rc.state.Set(IdentifierInternetGateway, created.InternetGatewayId)
	if err := rc.client.AttachInternetGateway(ctx, *rc.state.Get(IdentifierVPC), created.InternetGatewayId); err != nil {
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
			VpcId:       rc.state.Get(IdentifierVPC),
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
		VpcId: rc.state.Get(IdentifierVPC),
		Routes: []*awsclient.Route{
			{
				DestinationCidrBlock: "0.0.0.0/0",
				GatewayId:            rc.state.Get(IdentifierInternetGateway),
			},
		},
	}
	current, err := findExisting(ctx, rc.state.Get(IdentifierMainRouteTable), rc.commonTags,
		rc.client.GetRouteTable, rc.client.FindRouteTablesByTags)
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.Set(IdentifierMainRouteTable, current.RouteTableId)
		rc.state.SetObject(ObjectMainRouteTable, current)
		updated, err := rc.updater.UpdateRouteTable(ctx, desired, current)
		if err != nil {
			return err
		}
		rc.state.SetObject(ObjectMainRouteTable, updated)
		return nil
	}

	created, err := rc.client.CreateRouteTable(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.Set(IdentifierMainRouteTable, created.RouteTableId)
	rc.state.SetObject(ObjectMainRouteTable, created)
	updated, err := rc.updater.UpdateRouteTable(ctx, desired, created)
	if err != nil {
		return err
	}
	rc.state.SetObject(ObjectMainRouteTable, updated)
	return nil
}

func (rc *ReconcileContext) EnsureNodesSecurityGroup(ctx context.Context) error {
	groupName := fmt.Sprintf("%s-nodes", rc.infra.Namespace)
	desired := &awsclient.SecurityGroup{
		Tags:        rc.commonTagsWithSuffix("nodes"),
		GroupName:   groupName,
		VpcId:       rc.state.Get(IdentifierVPC),
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
	current, err := findExisting(ctx, rc.state.Get(IdentifierNodesSecurityGroup), rc.commonTags,
		rc.client.GetSecurityGroup, rc.client.FindSecurityGroupsByTags,
		func(item *awsclient.SecurityGroup) bool { return item.GroupName == groupName })
	if err != nil {
		return err
	}
	if current != nil {
		rc.state.Set(IdentifierNodesSecurityGroup, current.GroupId)
		if _, err := rc.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := rc.client.CreateSecurityGroup(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.Set(IdentifierNodesSecurityGroup, created.GroupId)
	current, err = rc.client.GetSecurityGroup(ctx, created.GroupId)
	if err != nil {
		return err
	}
	if _, err := rc.updater.UpdateSecurityGroup(ctx, desired, current); err != nil {
		return err
	}
	return nil
}

func (rc *ReconcileContext) EnsureZones(ctx context.Context) error {
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
				VpcId:            rc.state.Get(IdentifierVPC),
				CidrBlock:        zone.Workers,
				AvailabilityZone: zone.Name,
			},
			&awsclient.Subnet{
				Tags:             tagsPublic,
				VpcId:            rc.state.Get(IdentifierVPC),
				CidrBlock:        zone.Public,
				AvailabilityZone: zone.Name,
			},
			&awsclient.Subnet{
				Tags:             tagsPrivate,
				VpcId:            rc.state.Get(IdentifierVPC),
				CidrBlock:        zone.Internal,
				AvailabilityZone: zone.Name,
			})
	}
	// update flow state if subnet suffixes have been added
	if err := rc.PersistFlowState(ctx, true); err != nil {
		return err
	}
	current, err := rc.collectExistingSubnets(ctx)
	if err != nil {
		return err
	}
	toBeDeleted, toBeCreated, toBeChecked := diffByID(desired, current, func(item *awsclient.Subnet) string {
		return item.AvailabilityZone + "-" + item.CidrBlock
	})

	g := flow.NewGraph("AWS infrastructure reconcilation: zones")

	rc.addZoneDeletionTasksBySubnets(g, toBeDeleted)

	dependencies := newZoneDependencies()
	for _, item := range toBeCreated {
		taskID := rc.addSubnetReconcileTasks(g, item, nil)
		dependencies.Insert(item.AvailabilityZone, taskID)
	}
	for _, pair := range toBeChecked {
		taskID := rc.addSubnetReconcileTasks(g, pair.desired, pair.current)
		dependencies.Insert(pair.desired.AvailabilityZone, taskID)
	}
	for _, zone := range rc.config.Networks.Zones {
		rc.addZoneReconcileTasks(g, &zone, dependencies.Get(zone.Name))
	}
	f := g.Compile()
	if err := f.Run(ctx, flow.Opts{Log: rc.logger}); err != nil {
		return flow.Causes(err)
	}
	return nil
}

func (rc *ReconcileContext) addZoneDeletionTasksBySubnets(g *flow.Graph, toBeDeleted []*awsclient.Subnet) {
	toBeDeletedZones := sets.NewString()
	for _, item := range toBeDeleted {
		toBeDeletedZones.Insert(getZoneName(item))
	}
	dependencies := newZoneDependencies()
	for zoneName := range toBeDeletedZones {
		taskID := rc.addZoneDeletionTasks(g, zoneName)
		dependencies.Insert(zoneName, taskID)
	}
	for _, item := range toBeDeleted {
		rc.addSubnetDeletionTasks(g, item, dependencies.Get(item.AvailabilityZone))
	}
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

func (rc *ReconcileContext) addSubnetReconcileTasks(g *flow.Graph, desired, current *awsclient.Subnet) flow.TaskID {
	zoneName, subnetKey := rc.getSubnetKey(desired)
	suffix := fmt.Sprintf("%s-%s", zoneName, subnetKey)
	return g.Add(flow.Task{
		Name: "ensure subnet " + suffix,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureSubnet(desired, current))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
}

func (rc *ReconcileContext) addZoneReconcileTasks(g *flow.Graph, zone *aws.Zone, dependencies flow.TaskIDs) {
	ensureElasticIP := g.Add(flow.Task{
		Name: "ensure NAT gateway elastic IP " + zone.Name,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureElasticIP(zone))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies,
	})
	ensureNATGateway := g.Add(flow.Task{
		Name: "ensure NAT gateway " + zone.Name,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureNATGateway(zone))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies.Copy().Insert(ensureElasticIP),
	})
	ensureRoutingTable := g.Add(flow.Task{
		Name: "ensure route table " + zone.Name,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsurePrivateRoutingTable(zone.Name))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies.Copy().Insert(ensureNATGateway),
	})
	g.Add(flow.Task{
		Name: "ensure route table associations " + zone.Name,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureRoutingTableAssociations(zone.Name))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies.Copy().Insert(ensureRoutingTable),
	})
}

func (rc *ReconcileContext) addZoneDeletionTasks(g *flow.Graph, zoneName string) flow.TaskID {
	ensureDeletedRoutingTableAssocs := g.Add(flow.Task{
		Name: "ensure deletion of route table associations " + zoneName,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureDeletedRoutingTableAssociations(zoneName))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
	})
	ensureDeletedRoutingTable := g.Add(flow.Task{
		Name: "ensure deletion of route table " + zoneName,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureDeletedPrivateRoutingTable(zoneName))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDeletedRoutingTableAssocs),
	})
	ensureDeletedNATGateway := g.Add(flow.Task{
		Name: "ensure deletion of NAT gateway " + zoneName,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureDeletedNATGateway(zoneName))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDeletedRoutingTable),
	})
	g.Add(flow.Task{
		Name: "ensure deletion of NAT gateway elastic IP " + zoneName,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureDeletedElasticIP(zoneName))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: flow.NewTaskIDs(ensureDeletedNATGateway),
	})
	return ensureDeletedNATGateway
}

func (rc *ReconcileContext) addSubnetDeletionTasks(g *flow.Graph, item *awsclient.Subnet, dependencies flow.TaskIDs) {
	zoneName, subnetKey := rc.getSubnetKey(item)
	suffix := fmt.Sprintf("%s-%s", zoneName, subnetKey)
	g.Add(flow.Task{
		Name: "ensure deletion of subnet resource " + suffix,
		Fn: flow.TaskFn(rc.PersistingState(rc.EnsureDeletedSubnet(item))).
			RetryUntilTimeout(defaultRetryInterval, defaultRetryTimeout),
		Dependencies: dependencies,
	})
}

func (rc *ReconcileContext) EnsureDeletedSubnet(item *awsclient.Subnet) flow.TaskFn {
	zoneChild := rc.getSubnetZoneChildByItem(item)
	_, subnetKey := rc.getSubnetKey(item)
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
	_, subnetKey := rc.getSubnetKey(desired)
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

func (rc *ReconcileContext) EnsureElasticIP(zone *aws.Zone) flow.TaskFn {
	return func(ctx context.Context) error {
		if zone.ElasticIPAllocationID != nil {
			return nil
		}
		suffix := rc.subnetSuffix(zone.Name)
		eipSuffix := fmt.Sprintf("eip-natgw-%s", suffix)
		child := rc.getSubnetZoneChild(zone.Name)
		id := child.Get(IdentifierZoneNATGWElasticIP)
		desired := &awsclient.ElasticIP{
			Tags: rc.commonTagsWithSuffix(eipSuffix),
			Vpc:  true,
		}
		current, err := findExisting(ctx, id, desired.Tags, rc.client.GetElasticIP, rc.client.FindElasticIPsByTags)
		if err != nil {
			return err
		}

		if current != nil {
			child.Set(IdentifierZoneNATGWElasticIP, current.AllocationId)
			if _, err := rc.updater.UpdateEC2Tags(ctx, current.AllocationId, desired.Tags, current.Tags); err != nil {
				return err
			}
			return nil
		}

		created, err := rc.client.CreateElasticIP(ctx, desired)
		if err != nil {
			return err
		}
		child.Set(IdentifierZoneNATGWElasticIP, created.AllocationId)
		return nil
	}
}

func (rc *ReconcileContext) EnsureDeletedElasticIP(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		suffix := rc.subnetSuffix(zoneName)
		eipSuffix := fmt.Sprintf("eip-natgw-%s", suffix)
		child := rc.getSubnetZoneChild(zoneName)
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

func (rc *ReconcileContext) EnsureNATGateway(zone *aws.Zone) flow.TaskFn {
	return func(ctx context.Context) error {
		child := rc.getSubnetZoneChild(zone.Name)
		id := child.Get(IdentifierZoneNATGateway)
		desired := &awsclient.NATGateway{
			Tags:     rc.commonTagsWithSuffix(fmt.Sprintf("natgw-%s", rc.subnetSuffix(zone.Name))),
			SubnetId: *child.Get(IdentifierZoneSubnetPublic),
		}
		if zone.ElasticIPAllocationID != nil {
			desired.EIPAllocationId = *zone.ElasticIPAllocationID
		} else {
			desired.EIPAllocationId = *child.Get(IdentifierZoneNATGWElasticIP)
		}
		current, err := findExisting(ctx, id, desired.Tags, rc.client.GetNATGateway, rc.client.FindNATGatewaysByTags)
		if err != nil {
			return err
		}

		if current != nil {
			child.Set(IdentifierZoneNATGateway, current.NATGatewayId)
			if _, err := rc.updater.UpdateEC2Tags(ctx, current.NATGatewayId, desired.Tags, current.Tags); err != nil {
				return err
			}
			return nil
		}

		created, err := rc.client.CreateNATGateway(ctx, desired)
		if err != nil {
			return err
		}
		child.Set(IdentifierZoneNATGateway, created.NATGatewayId)
		return nil
	}
}

func (rc *ReconcileContext) EnsureDeletedNATGateway(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
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

func (rc *ReconcileContext) EnsurePrivateRoutingTable(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		child := rc.getSubnetZoneChild(zoneName)
		id := child.Get(IdentifierZoneRouteTable)
		desired := &awsclient.RouteTable{
			Tags:  rc.commonTagsWithSuffix(fmt.Sprintf("private-%s", zoneName)),
			VpcId: rc.state.Get(IdentifierVPC),
			Routes: []*awsclient.Route{
				{
					DestinationCidrBlock: "0.0.0.0/0",
					NatGatewayId:         child.Get(IdentifierZoneNATGateway),
				},
			},
		}
		current, err := findExisting(ctx, id, desired.Tags, rc.client.GetRouteTable, rc.client.FindRouteTablesByTags)
		if err != nil {
			return err
		}

		if current != nil {
			child.Set(IdentifierZoneRouteTable, current.RouteTableId)
			child.SetObject(ObjectZoneRouteTable, current)
			if _, err := rc.updater.UpdateRouteTable(ctx, desired, current); err != nil {
				return err
			}
			return nil
		}

		created, err := rc.client.CreateRouteTable(ctx, desired)
		if err != nil {
			return err
		}
		child.Set(IdentifierZoneRouteTable, created.RouteTableId)
		child.SetObject(ObjectZoneRouteTable, created)
		if _, err := rc.updater.UpdateRouteTable(ctx, desired, created); err != nil {
			return err
		}
		return nil
	}
}

func (rc *ReconcileContext) EnsureDeletedPrivateRoutingTable(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		child := rc.getSubnetZoneChild(zoneName)
		id := child.Get(IdentifierZoneRouteTable)
		tags := rc.commonTagsWithSuffix(fmt.Sprintf("private-%s", zoneName))
		current, err := findExisting(ctx, id, tags, rc.client.GetRouteTable, rc.client.FindRouteTablesByTags)
		if err != nil {
			return err
		}
		if current != nil {
			if err := rc.client.DeleteRouteTable(ctx, current.RouteTableId); err != nil {
				return err
			}
		}
		child.SetAsDeleted(IdentifierZoneRouteTable)
		return nil
	}
}

func (rc *ReconcileContext) EnsureRoutingTableAssociations(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		if err := rc.ensureZoneRoutingTableAssociation(ctx, zoneName, false,
			IdentifierZoneSubnetPublic, IdentifierZoneSubnetPublicRouteTableAssoc); err != nil {
			return err
		}
		if err := rc.ensureZoneRoutingTableAssociation(ctx, zoneName, true,
			IdentifierZoneSubnetPrivate, IdentifierZoneSubnetPrivateRouteTableAssoc); err != nil {
			return err
		}
		if err := rc.ensureZoneRoutingTableAssociation(ctx, zoneName, true,
			IdentifierZoneSubnetWorkers, IdentifierZoneSubnetWorkersRouteTableAssoc); err != nil {
			return err
		}
		return nil
	}
}

func (rc *ReconcileContext) ensureZoneRoutingTableAssociation(ctx context.Context, zoneName string,
	zoneRouteTable bool, subnetKey, assocKey string) error {
	child := rc.getSubnetZoneChild(zoneName)
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
		obj = rc.state.GetObject(ObjectMainRouteTable)
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
	assocID, err := rc.client.CreateRouteTableAssociation(ctx, routeTable.RouteTableId, *subnetID)
	if err != nil {
		return err
	}
	child.Set(assocKey, *assocID)
	return nil
}

func (rc *ReconcileContext) EnsureDeletedRoutingTableAssociations(zoneName string) flow.TaskFn {
	return func(ctx context.Context) error {
		if err := rc.ensureDeletedZoneRoutingTableAssociation(ctx, zoneName, false,
			IdentifierZoneSubnetPublic, IdentifierZoneSubnetPublicRouteTableAssoc); err != nil {
			return err
		}
		if err := rc.ensureDeletedZoneRoutingTableAssociation(ctx, zoneName, true,
			IdentifierZoneSubnetPrivate, IdentifierZoneSubnetPrivateRouteTableAssoc); err != nil {
			return err
		}
		if err := rc.ensureDeletedZoneRoutingTableAssociation(ctx, zoneName, true,
			IdentifierZoneSubnetWorkers, IdentifierZoneSubnetWorkersRouteTableAssoc); err != nil {
			return err
		}
		return nil
	}
}

func (rc *ReconcileContext) ensureDeletedZoneRoutingTableAssociation(ctx context.Context, zoneName string,
	zoneRouteTable bool, subnetKey, assocKey string) error {
	child := rc.getSubnetZoneChild(zoneName)
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
			routeTableID = rc.state.Get(IdentifierMainRouteTable)
		}
		if routeTableID != nil {
			routeTable, err := rc.client.GetRouteTable(ctx, *routeTableID)
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
	if err := rc.client.DeleteRouteTableAssociation(ctx, *assocID); err != nil {
		return err
	}
	child.SetAsDeleted(assocKey)
	return nil
}

func (rc *ReconcileContext) EnsureIAMRole(ctx context.Context) error {
	desired := &awsclient.IAMRole{
		RoleName: fmt.Sprintf("%s-nodes", rc.infra.Namespace),
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
	current, err := rc.client.GetIAMRole(ctx, desired.RoleName)
	if err != nil {
		return err
	}

	if current != nil {
		rc.state.Set(IdentifierIAMRole, current.RoleId)
		return nil
	}

	created, err := rc.client.CreateIAMRole(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.Set(IdentifierIAMRole, created.RoleId)
	return nil
}

func (rc *ReconcileContext) EnsureIAMInstanceProfile(ctx context.Context) error {
	desired := &awsclient.IAMInstanceProfile{
		InstanceProfileName: fmt.Sprintf("%s-nodes", rc.infra.Namespace),
		Path:                "/",
		RoleName:            fmt.Sprintf("%s-nodes", rc.infra.Namespace),
	}
	current, err := rc.client.GetIAMInstanceProfile(ctx, desired.InstanceProfileName)
	if err != nil {
		return err
	}

	if current != nil {
		rc.state.Set(IdentifierIAMInstanceProfile, current.InstanceProfileId)
		if _, err := rc.updater.UpdateIAMInstanceProfile(ctx, desired, current); err != nil {
			return err
		}
		return nil
	}

	created, err := rc.client.CreateIAMInstanceProfile(ctx, desired)
	if err != nil {
		return err
	}
	rc.state.Set(IdentifierIAMInstanceProfile, created.InstanceProfileId)
	if _, err := rc.updater.UpdateIAMInstanceProfile(ctx, desired, created); err != nil {
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

func (rc *ReconcileContext) EnsureIAMRolePolicy(ctx context.Context) error {
	enableECRAccess := true
	if v := rc.config.EnableECRAccess; v != nil {
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
		PolicyName:     fmt.Sprintf("%s-nodes", rc.infra.Namespace),
		RoleName:       fmt.Sprintf("%s-nodes", rc.infra.Namespace),
		PolicyDocument: buffer.String(),
	}
	current, err := rc.client.GetIAMRolePolicy(ctx, desired.PolicyName, desired.RoleName)
	if err != nil {
		return err
	}

	if current != nil {
		rc.state.Set(IdentifierIAMRolePolicy, "true")
		if current.PolicyDocument != desired.PolicyDocument {
			if err := rc.client.PutIAMRolePolicy(ctx, desired); err != nil {
				return err
			}
		}
		return nil
	}

	if err := rc.client.PutIAMRolePolicy(ctx, desired); err != nil {
		return err
	}
	rc.state.Set(IdentifierIAMRolePolicy, "true")
	return nil
}

func (rc *ReconcileContext) EnsureKeyPair(ctx context.Context) error {
	desired := &awsclient.KeyPairInfo{
		Tags:    rc.commonTags,
		KeyName: fmt.Sprintf("%s-ssh-publickey", rc.infra.Namespace),
	}
	current, err := rc.client.GetKeyPair(ctx, desired.KeyName)
	if err != nil {
		return err
	}

	if current != nil {
		rc.state.Set(IdentifierKeyPair, desired.KeyName)
		return nil
	}

	if _, err := rc.client.ImportKeyPair(ctx, desired.KeyName, rc.infra.Spec.SSHPublicKey, rc.commonTags); err != nil {
		return err
	}
	rc.state.Set(IdentifierKeyPair, desired.KeyName)
	return nil
}

func (rc *ReconcileContext) getSubnetZoneChildByItem(item *awsclient.Subnet) state.Whiteboard {
	return rc.getSubnetZoneChild(getZoneName(item))
}

func (rc *ReconcileContext) getSubnetZoneChild(zoneName string) state.Whiteboard {
	return rc.state.GetChild(ChildIdZones).GetChild(zoneName)
}

func (rc *ReconcileContext) getSubnetKey(item *awsclient.Subnet) (zoneName, subnetKey string) {
	zone := rc.getZone(item)
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
