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

package client

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/service/ec2"
)

type Updater interface {
	UpdateVpc(ctx context.Context, desired, current *VPC) (*VPC, error)
	UpdateSecurityGroup(ctx context.Context, desired, current *SecurityGroup) (*SecurityGroup, error)
	UpdateRouteTable(ctx context.Context, desired, current *RouteTable) (*RouteTable, error)
	UpdateIAMInstanceProfile(ctx context.Context, desired, current *IAMInstanceProfile) (*IAMInstanceProfile, error)
}

type updater struct {
	client Interface
}

func NewUpdater(client Interface) Updater {
	return &updater{
		client: client,
	}
}

func (u *updater) UpdateVpc(ctx context.Context, desired, current *VPC) (*VPC, error) {
	if desired.CidrBlock != current.CidrBlock {
		return nil, fmt.Errorf("cannot change CIDR block")
	}
	new, err := u.updateVpcAttributes(ctx, desired, current)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(desired.DhcpOptionsId, current.DhcpOptionsId) {
		if err := u.client.AddVpcDhcpOptionAssociation(current.VpcId, desired.DhcpOptionsId); err != nil {
			return nil, err
		}
		new.DhcpOptionsId = desired.DhcpOptionsId
	}
	return new, nil
}

func (u *updater) updateVpcAttributes(ctx context.Context, desired, current *VPC) (*VPC, error) {
	new := *current
	if desired.EnableDnsSupport != current.EnableDnsSupport {
		if err := u.client.UpdateVpcAttribute(ctx, current.VpcId, ec2.VpcAttributeNameEnableDnsSupport, desired.EnableDnsSupport); err != nil {
			return nil, err
		}
		new.EnableDnsSupport = desired.EnableDnsSupport
	}
	if desired.EnableDnsHostnames != current.EnableDnsHostnames {
		if err := u.client.UpdateVpcAttribute(ctx, current.VpcId, ec2.VpcAttributeNameEnableDnsHostnames, desired.EnableDnsHostnames); err != nil {
			return nil, err
		}
		new.EnableDnsHostnames = desired.EnableDnsHostnames
	}
	return &new, nil
}

func (u *updater) UpdateSecurityGroup(ctx context.Context, desired, current *SecurityGroup) (*SecurityGroup, error) {
	if desired.EquivalentRulesTo(current) {
		return current, nil
	}
	if err := u.client.UpdateSecurityGroupRules(ctx, desired); err != nil {
		return nil, err
	}
	list, err := u.client.DescribeSecurityGroups(ctx, &current.GroupId, nil)
	if err != nil {
		return nil, err
	}
	if len(list) != 1 {
		return nil, fmt.Errorf("security group %s not found", current.GroupId)
	}
	return list[0], nil
}

func (u *updater) UpdateRouteTable(ctx context.Context, desired, current *RouteTable) (*RouteTable, error) {
outerDelete:
	for _, cr := range current.Routes {
		for _, dr := range desired.Routes {
			if reflect.DeepEqual(cr, dr) {
				continue outerDelete
			}
		}
		if err := u.client.DeleteRoute(ctx, current.RouteTableId, cr); err != nil {
			return nil, err
		}
	}
outerCreate:
	for _, dr := range desired.Routes {
		for _, cr := range current.Routes {
			if reflect.DeepEqual(cr, dr) {
				continue outerCreate
			}
		}
		if err := u.client.CreateRoute(ctx, current.RouteTableId, dr); err != nil {
			return nil, err
		}
	}
	updated := *current
	updated.Routes = nil
	for _, dr := range desired.Routes {
		updated.Routes = append(updated.Routes, dr)
	}
	return &updated, nil
}

func (u *updater) UpdateIAMInstanceProfile(ctx context.Context, desired, current *IAMInstanceProfile) (*IAMInstanceProfile, error) {
	if current.RoleName == desired.RoleName {
		return current, nil
	}
	if desired.RoleName != "" {
		if err := u.client.AddRoleToIAMInstanceProfile(ctx, current.InstanceProfileName, desired.RoleName); err != nil {
			return nil, err
		}
	}
	if current.RoleName != "" {
		if err := u.client.RemoveRoleFromIAMInstanceProfile(ctx, current.InstanceProfileName, current.RoleName); err != nil {
			return nil, err
		}
	}
	return u.client.GetIAMInstanceProfile(ctx, current.InstanceProfileName)
}
