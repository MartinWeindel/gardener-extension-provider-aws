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
	"strings"

	"github.com/aws/aws-sdk-go/service/ec2"
	awsapi "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws"
)

type Updater interface {
	UpdateVpc(ctx context.Context, desired, current *VPC) (*VPC, error)
	UpdateSecurityGroup(ctx context.Context, desired, current *SecurityGroup) (*SecurityGroup, error)
	UpdateRouteTable(ctx context.Context, desired, current *RouteTable) (*RouteTable, error)
	UpdateSubnet(ctx context.Context, desired, current *Subnet) (*Subnet, error)
	UpdateIAMInstanceProfile(ctx context.Context, desired, current *IAMInstanceProfile) (*IAMInstanceProfile, error)
	UpdateEC2Tags(ctx context.Context, id string, desired, current Tags) (Tags, error)
}

type updater struct {
	client     Interface
	ignoreTags *awsapi.IgnoreTags
}

func NewUpdater(client Interface, ignoreTags *awsapi.IgnoreTags) Updater {
	return &updater{
		client:     client,
		ignoreTags: ignoreTags,
	}
}

func (u *updater) UpdateVpc(ctx context.Context, desired, current *VPC) (*VPC, error) {
	if desired.CidrBlock != current.CidrBlock {
		return nil, fmt.Errorf("cannot change CIDR block")
	}
	updated, err := u.updateVpcAttributes(ctx, desired, current)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(desired.DhcpOptionsId, current.DhcpOptionsId) {
		if err := u.client.AddVpcDhcpOptionAssociation(current.VpcId, desired.DhcpOptionsId); err != nil {
			return nil, err
		}
		updated.DhcpOptionsId = desired.DhcpOptionsId
	}

	updatedTags, err := u.UpdateEC2Tags(ctx, current.VpcId, desired.Tags, current.Tags)
	if err != nil {
		return updated, err
	}
	updated.Tags = updatedTags
	return updated, nil
}

func (u *updater) updateVpcAttributes(ctx context.Context, desired, current *VPC) (*VPC, error) {
	updated := *current
	if desired.EnableDnsSupport != current.EnableDnsSupport {
		if err := u.client.UpdateVpcAttribute(ctx, current.VpcId, ec2.VpcAttributeNameEnableDnsSupport, desired.EnableDnsSupport); err != nil {
			return nil, err
		}
		updated.EnableDnsSupport = desired.EnableDnsSupport
	}
	if desired.EnableDnsHostnames != current.EnableDnsHostnames {
		if err := u.client.UpdateVpcAttribute(ctx, current.VpcId, ec2.VpcAttributeNameEnableDnsHostnames, desired.EnableDnsHostnames); err != nil {
			return nil, err
		}
		updated.EnableDnsHostnames = desired.EnableDnsHostnames
	}
	return &updated, nil
}

func (u *updater) UpdateSecurityGroup(ctx context.Context, desired, current *SecurityGroup) (*SecurityGroup, error) {
	added, removed := desired.DiffRules(current)
	if len(added) == 0 && len(removed) == 0 {
		return current, nil
	}
	if err := u.client.RevokeSecurityGroupRules(ctx, current.GroupId, removed); err != nil {
		return nil, err
	}
	if err := u.client.AuthorizeSecurityGroupRules(ctx, current.GroupId, added); err != nil {
		return nil, err
	}
	if _, err := u.UpdateEC2Tags(ctx, current.GroupId, desired.Tags, current.Tags); err != nil {
		return nil, err
	}
	return u.client.GetSecurityGroup(ctx, current.GroupId)
}

func (u *updater) UpdateRouteTable(ctx context.Context, desired, current *RouteTable) (*RouteTable, error) {
outerDelete:
	for _, cr := range current.Routes {
		for _, dr := range desired.Routes {
			if reflect.DeepEqual(cr, dr) {
				continue outerDelete
			}
		}
		if cr.GatewayId != nil && *cr.GatewayId == "local" {
			// ignore local gateway route
			continue outerDelete
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

func (u *updater) UpdateSubnet(ctx context.Context, desired, current *Subnet) (*Subnet, error) {
	if _, err := u.UpdateEC2Tags(ctx, current.SubnetId, desired.Tags, current.Tags); err != nil {
		return current, err
	}
	return desired, nil
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

func (u *updater) UpdateEC2Tags(ctx context.Context, id string, desired, current Tags) (Tags, error) {
	toBeDeleted := Tags{}
	toBeCreated := Tags{}
	toBeIgnored := Tags{}
	for k, v := range current {
		if dv, ok := desired[k]; ok {
			if dv != v {
				toBeDeleted[k] = v
				toBeCreated[k] = dv
			}
		} else if u.ignoreTag(k) {
			toBeIgnored[k] = v
		} else {
			toBeDeleted[k] = v
		}
	}
	for k, v := range desired {
		if _, ok := current[k]; !ok && !u.ignoreTag(k) {
			toBeCreated[k] = v
		}
	}

	if len(toBeDeleted) > 0 {
		if err := u.client.DeleteEC2Tags(ctx, []string{id}, toBeDeleted); err != nil {
			return nil, err
		}
	}
	if len(toBeCreated) > 0 {
		if err := u.client.CreateEC2Tags(ctx, []string{id}, toBeCreated); err != nil {
			return nil, err
		}
		updated := desired.Clone()
		for k, v := range toBeCreated {
			updated[k] = v
		}
		return updated, nil
	} else {
		return desired, nil
	}

	return desired.Clone(), nil
}

func (u *updater) ignoreTag(key string) bool {
	if u.ignoreTags == nil {
		return false
	}
	for _, ignoreKey := range u.ignoreTags.Keys {
		if ignoreKey == key {
			return true
		}
	}
	for _, ignoreKeyPrefix := range u.ignoreTags.KeyPrefixes {
		if strings.HasPrefix(key, ignoreKeyPrefix) {
			return true
		}
	}
	return false
}
