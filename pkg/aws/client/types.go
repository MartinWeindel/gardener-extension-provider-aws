// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"sort"

	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	// AWS-SDK is missing these constant. So, added here till the time it comes from
	// upstream AWS-SDK-GO

	// errCodeBucketNotEmpty for service response error code
	// "BucketNotEmpty".
	//
	// The specified bucket us exist.
	errCodeBucketNotEmpty = "BucketNotEmpty"
)

// Interface is an interface which must be implemented by AWS clients.
type Interface interface {
	GetAccountID(ctx context.Context) (string, error)
	GetVPCInternetGateway(ctx context.Context, vpcID string) (string, error)
	GetVPCAttribute(ctx context.Context, vpcID string, attribute string) (bool, error)
	GetElasticIPsAssociationIDForAllocationIDs(ctx context.Context, allocationIDs []string) (map[string]*string, error)
	GetNATGatewayAddressAllocations(ctx context.Context, shootNamespace string) (sets.String, error)

	// S3 wrappers
	DeleteObjectsWithPrefix(ctx context.Context, bucket, prefix string) error
	CreateBucketIfNotExists(ctx context.Context, bucket, region string) error
	DeleteBucketIfExists(ctx context.Context, bucket string) error

	// Route53 wrappers
	GetDNSHostedZones(ctx context.Context) (map[string]string, error)
	CreateOrUpdateDNSRecordSet(ctx context.Context, zoneId, name, recordType string, values []string, ttl int64) error
	DeleteDNSRecordSet(ctx context.Context, zoneId, name, recordType string, values []string, ttl int64) error

	// The following functions are only temporary needed due to https://github.com/gardener/gardener/issues/129.
	ListKubernetesELBs(ctx context.Context, vpcID, clusterName string) ([]string, error)
	ListKubernetesELBsV2(ctx context.Context, vpcID, clusterName string) ([]string, error)
	ListKubernetesSecurityGroups(ctx context.Context, vpcID, clusterName string) ([]string, error)
	DeleteELB(ctx context.Context, name string) error
	DeleteELBV2(ctx context.Context, arn string) error

	// VPCs
	CreateVpcDhcpOptions(ctx context.Context, options *DhcpOptions) (*DhcpOptions, error)
	GetVpcDhcpOptions(ctx context.Context, id string) (*DhcpOptions, error)
	FindVpcDhcpOptionsByTags(ctx context.Context, tags Tags) ([]*DhcpOptions, error)
	DeleteVpcDhcpOptions(ctx context.Context, id string) error
	CreateVpc(ctx context.Context, vpc *VPC) (*VPC, error)
	AddVpcDhcpOptionAssociation(vpcId string, dhcpOptionsId *string) error
	UpdateVpcAttribute(ctx context.Context, vpcId, attributeName string, value bool) error
	DeleteVpc(ctx context.Context, id string) error
	GetVpc(ctx context.Context, id string) (*VPC, error)
	FindVpcsByTags(ctx context.Context, tags Tags) ([]*VPC, error)

	// Security groups
	CreateSecurityGroup(ctx context.Context, sg *SecurityGroup) (*SecurityGroup, error)
	GetSecurityGroup(ctx context.Context, id string) (*SecurityGroup, error)
	FindSecurityGroupsByTags(ctx context.Context, tags Tags) ([]*SecurityGroup, error)
	FindDefaultSecurityGroupByVpcId(ctx context.Context, vpcId string) (*SecurityGroup, error)
	UpdateSecurityGroupRules(ctx context.Context, sg *SecurityGroup) error
	DeleteSecurityGroup(ctx context.Context, id string) error

	// Internet gateways
	CreateInternetGateway(ctx context.Context, gateway *InternetGateway) (*InternetGateway, error)
	GetInternetGateway(ctx context.Context, id string) (*InternetGateway, error)
	FindInternetGatewaysByTags(ctx context.Context, tags Tags) ([]*InternetGateway, error)
	DeleteInternetGateway(ctx context.Context, id string) error
	AttachInternetGateway(ctx context.Context, vpcId, internetGatewayId string) error
	DetachInternetGateway(ctx context.Context, vpcId, internetGatewayId string) error

	// VPC Endpoints
	CreateVpcEndpoint(ctx context.Context, endpoint *VpcEndpoint) (*VpcEndpoint, error)
	GetVpcEndpoints(ctx context.Context, ids []string) ([]*VpcEndpoint, error)
	FindVpcEndpointsByTags(ctx context.Context, tags Tags) ([]*VpcEndpoint, error)
	DeleteVpcEndpoint(ctx context.Context, id string) error

	// Route tables
	CreateRouteTable(ctx context.Context, routeTable *RouteTable) (*RouteTable, error)
	DescribeRouteTables(ctx context.Context, id *string, tags Tags) ([]*RouteTable, error)
	DeleteRouteTable(ctx context.Context, id string) error
	CreateRoute(ctx context.Context, routeTableId string, route *Route) error
	DeleteRoute(ctx context.Context, routeTableId string, route *Route) error

	// Subnets
	CreateSubnet(ctx context.Context, subnet *Subnet) (*Subnet, error)
	DescribeSubnets(ctx context.Context, id *string, tags Tags) ([]*Subnet, error)
	DeleteSubnet(ctx context.Context, id string) error

	// Route table associations
	CreateRouteTableAssociation(ctx context.Context, routeTableId, subnetId string) (associationId *string, err error)
	DeleteRouteTableAssociation(ctx context.Context, associationId string) error

	// Elastic IP
	CreateElasticIP(ctx context.Context, eip *ElasticIP) (*ElasticIP, error)
	DescribeElasticIPs(ctx context.Context, id *string, tags Tags) ([]*ElasticIP, error)
	DeleteElasticIP(ctx context.Context, id string) error

	// Internet gateways
	CreateNATGateway(ctx context.Context, gateway *NATGateway) (*NATGateway, error)
	DescribeNATGateways(ctx context.Context, id *string, tags Tags) ([]*NATGateway, error)
	DeleteNATGateway(ctx context.Context, id string) error

	// Key pairs
	ImportKeyPair(ctx context.Context, keyName string, publicKey []byte, tags Tags) (*KeyPairInfo, error)
	DescribeKeyPairs(ctx context.Context, keyName *string, tags Tags) ([]*KeyPairInfo, error)
	DeleteKeyPair(ctx context.Context, keyName string) error

	// IAM Role
	CreateIAMRole(ctx context.Context, role *IAMRole) (*IAMRole, error)
	GetIAMRole(ctx context.Context, roleName string) (*IAMRole, error)
	DeleteIAMRole(ctx context.Context, roleName string) error

	// IAM Instance Profile
	CreateIAMInstanceProfile(ctx context.Context, profile *IAMInstanceProfile) (*IAMInstanceProfile, error)
	GetIAMInstanceProfile(ctx context.Context, profileName string) (*IAMInstanceProfile, error)
	DeleteIAMInstanceProfile(ctx context.Context, profileName string) error
	AddRoleToIAMInstanceProfile(ctx context.Context, profileName, roleName string) error
	RemoveRoleFromIAMInstanceProfile(ctx context.Context, profileName, roleName string) error

	// IAM Role Policy
	PutIAMRolePolicy(ctx context.Context, policy *IAMRolePolicy) error
	GetIAMRolePolicy(ctx context.Context, policyName, roleName string) (*IAMRolePolicy, error)
	DeleteIAMRolePolicy(ctx context.Context, policyName, roleName string) error

	// EC2 tags
	CreateEC2Tags(ctx context.Context, resources []string, tags Tags) error
	DeleteEC2Tags(ctx context.Context, resources []string, tags Tags) error
}

// Factory creates instances of Interface.
type Factory interface {
	// NewClient creates a new instance of Interface for the given AWS credentials and region.
	NewClient(accessKeyID, secretAccessKey, region string) (Interface, error)
}

// FactoryFunc is a function that implements Factory.
type FactoryFunc func(accessKeyID, secretAccessKey, region string) (Interface, error)

// NewClient creates a new instance of Interface for the given AWS credentials and region.
func (f FactoryFunc) NewClient(accessKeyID, secretAccessKey, region string) (Interface, error) {
	return f(accessKeyID, secretAccessKey, region)
}

type DhcpOptions struct {
	Tags
	DhcpOptionsId      string
	DhcpConfigurations map[string][]string
}

type VPC struct {
	Tags
	VpcId              string
	CidrBlock          string
	EnableDnsSupport   bool
	EnableDnsHostnames bool
	DhcpOptionsId      *string
}

type SecurityGroup struct {
	Tags
	GroupId     string
	GroupName   string
	VpcId       *string
	Description *string
	Rules       []*SecurityGroupRule
}

func (sg *SecurityGroup) Clone() *SecurityGroup {
	copy := *sg
	copy.Rules = copyArray(sg.Rules)
	return &copy
}

func (sg *SecurityGroup) SortedClone() *SecurityGroup {
	copy := sg.Clone()
	sort.Slice(copy.Rules, func(i, j int) bool {
		ri := copy.Rules[i].SortedClone()
		rj := copy.Rules[j].SortedClone()
		return ri.LessThan(rj)
	})
	return copy
}

func (sg *SecurityGroup) EquivalentRulesTo(other *SecurityGroup) bool {
	if len(sg.Rules) != len(other.Rules) {
		return false
	}
	a := sg.SortedClone()
	b := other.SortedClone()
	for i := range a.Rules {
		ra := a.Rules[i]
		rb := b.Rules[i]
		if ra.LessThan(rb) || rb.LessThan(ra) {
			return false
		}
	}
	return true
}

type SecurityGroupRuleType string

const (
	SecurityGroupRuleTypeIngress SecurityGroupRuleType = "ingress"
	SecurityGroupRuleTypeEgress  SecurityGroupRuleType = "egress"
)

type SecurityGroupRule struct {
	Type       SecurityGroupRuleType
	FromPort   int
	ToPort     int
	Protocol   string
	CidrBlocks []string
}

func (sgr *SecurityGroupRule) Clone() *SecurityGroupRule {
	copy := *sgr
	copy.CidrBlocks = copyArray(sgr.CidrBlocks)
	return &copy
}

func (sgr *SecurityGroupRule) SortedClone() *SecurityGroupRule {
	copy := sgr.Clone()
	sort.Strings(copy.CidrBlocks)
	return copy
}

func (sgr *SecurityGroupRule) LessThan(other *SecurityGroupRule) bool {
	if sgr.Type < other.Type {
		return true
	}
	if sgr.Type > other.Type {
		return false
	}
	if sgr.Protocol < other.Protocol {
		return true
	}
	if sgr.Protocol > other.Protocol {
		return false
	}
	if sgr.FromPort < other.FromPort {
		return true
	}
	if sgr.FromPort > other.FromPort {
		return false
	}
	if sgr.ToPort < other.ToPort {
		return true
	}
	if sgr.ToPort > other.ToPort {
		return false
	}
	if len(sgr.CidrBlocks) < len(other.CidrBlocks) {
		return true
	}
	if len(sgr.CidrBlocks) > len(other.CidrBlocks) {
		return false
	}
	for i := range sgr.CidrBlocks {
		if sgr.CidrBlocks[i] < other.CidrBlocks[i] {
			return true
		}
		if sgr.CidrBlocks[i] > other.CidrBlocks[i] {
			return false
		}
	}
	return true
}

type InternetGateway struct {
	Tags
	InternetGatewayId string
	VpcId             *string
}

type VpcEndpoint struct {
	Tags
	VpcEndpointId string
	VpcId         *string
	ServiceName   string
}

type RouteTable struct {
	Tags
	RouteTableId string
	VpcId        *string
	Routes       []*Route
}

type Route struct {
	DestinationCidrBlock string
	GatewayId            *string
	NatGatewayId         *string
}

type Subnet struct {
	Tags
	SubnetId         string
	VpcId            string
	CidrBlock        string
	AvailabilityZone string
}

type ElasticIP struct {
	Tags
	AllocationId string
	PublicIp     string
	Vpc          bool
}

type NATGateway struct {
	Tags
	NATGatewayId    string
	EIPAllocationId string
	PublicIP        string
	SubnetId        string
}

type KeyPairInfo struct {
	Tags
	KeyName        string
	KeyFingerprint string
}

type IAMRole struct {
	Tags
	RoleId                   string
	RoleName                 string
	Path                     string
	AssumeRolePolicyDocument string
}

type IAMInstanceProfile struct {
	Tags
	InstanceProfileId   string
	InstanceProfileName string
	Path                string
	RoleName            string
}

type IAMRolePolicy struct {
	PolicyName     string
	RoleName       string
	PolicyDocument string
}
