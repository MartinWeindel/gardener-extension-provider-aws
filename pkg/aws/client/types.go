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
	DescribeVpcDhcpOptions(ctx context.Context, id *string, tags Tags) ([]*DhcpOptions, error)
	DeleteVpcDhcpOptions(ctx context.Context, id string) error
	CreateVpc(ctx context.Context, vpc *VPC) (*VPC, error)
	UpdateVpc(ctx context.Context, desired, current *VPC) (*VPC, error)
	DeleteVpc(ctx context.Context, id string) error
	DescribeVpcs(ctx context.Context, id *string, tags Tags) ([]*VPC, error)

	// Security groups
	CreateSecurityGroup(ctx context.Context, sg *SecurityGroup) (*SecurityGroup, error)
	DescribeSecurityGroups(ctx context.Context, id *string, tags Tags) ([]*SecurityGroup, error)
	ModifySecurityGroup(ctx context.Context, sg *SecurityGroup) (*SecurityGroup, error)
	DeleteSecurityGroup(ctx context.Context, id string) error

	// Internet gateways
	CreateInternetGateway(ctx context.Context, gateway *InternetGateway) (*InternetGateway, error)
	DescribeInternetGateways(ctx context.Context, id *string, tags Tags) ([]*InternetGateway, error)
	DeleteInternetGateway(ctx context.Context, id string) error

	// VPC Endpoints
	CreateVpcEndpoint(ctx context.Context, endpoint *VpcEndpoint) (*VpcEndpoint, error)
	DescribeVpcEndpoints(ctx context.Context, id *string, tags Tags) ([]*VpcEndpoint, error)
	DeleteVpcEndpoint(ctx context.Context, id string) error

	// Route tables
	CreateRouteTable(ctx context.Context, routeTable *RouteTable) (*RouteTable, error)
	UpdateRouteTable(ctx context.Context, desired, current *RouteTable) (*RouteTable, error)
	DescribeRouteTables(ctx context.Context, id *string, tags Tags) ([]*RouteTable, error)
	DeleteRouteTable(ctx context.Context, id string) error

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
	UpdateIAMInstanceProfile(ctx context.Context, desired, current *IAMInstanceProfile) (*IAMInstanceProfile, error)
	DeleteIAMInstanceProfile(ctx context.Context, profileName string) error

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
	VpcId       *string
	GroupName   string
	Description *string
	Rules       []*SecurityGroupRule
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

type InternetGateway struct {
	Tags
	InternetGatewayId string
	VpcId             string
}

type VpcEndpoint struct {
	Tags
	VpcEndpointId string
	VpcId         string
	ServiceName   string
}

type RouteTable struct {
	Tags
	RouteTableId string
	VpcId        string
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
