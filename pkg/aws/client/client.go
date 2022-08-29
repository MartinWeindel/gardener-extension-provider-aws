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
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/aws/aws-sdk-go/service/elb"
	"github.com/aws/aws-sdk-go/service/elb/elbiface"
	"github.com/aws/aws-sdk-go/service/elbv2"
	"github.com/aws/aws-sdk-go/service/elbv2/elbv2iface"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
	"github.com/aws/aws-sdk-go/service/route53"
	"github.com/aws/aws-sdk-go/service/route53/route53iface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/sts/stsiface"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	DefaultDhcpOptionsId     = "default"
	DefaultSecurityGroupName = "default"
)

// Client is a struct containing several clients for the different AWS services it needs to interact with.
// * EC2 is the standard client for the EC2 service.
// * STS is the standard client for the STS service.
// * IAM is the standard client for the IAM service.
// * S3 is the standard client for the S3 service.
// * ELB is the standard client for the ELB service.
// * ELBv2 is the standard client for the ELBv2 service.
// * Route53 is the standard client for the Route53 service.
type Client struct {
	EC2                           ec2iface.EC2API
	STS                           stsiface.STSAPI
	IAM                           iamiface.IAMAPI
	S3                            s3iface.S3API
	ELB                           elbiface.ELBAPI
	ELBv2                         elbv2iface.ELBV2API
	Route53                       route53iface.Route53API
	Route53RateLimiter            *rate.Limiter
	Route53RateLimiterWaitTimeout time.Duration
	Logger                        logr.Logger
	PollInterval                  time.Duration
}

var _ Interface = &Client{}

// NewInterface creates a new instance of Interface for the given AWS credentials and region.
func NewInterface(accessKeyID, secretAccessKey, region string) (Interface, error) {
	return NewClient(accessKeyID, secretAccessKey, region)
}

// NewClient creates a new Client for the given AWS credentials <accessKeyID>, <secretAccessKey>, and
// the AWS region <region>.
// It initializes the clients for the various services like EC2, ELB, etc.
func NewClient(accessKeyID, secretAccessKey, region string) (*Client, error) {
	var (
		awsConfig = &aws.Config{
			Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
		}
		config = &aws.Config{Region: aws.String(region)}
	)

	s, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}

	return &Client{
		EC2:                           ec2.New(s, config),
		ELB:                           elb.New(s, config),
		ELBv2:                         elbv2.New(s, config),
		IAM:                           iam.New(s, config),
		STS:                           sts.New(s, config),
		S3:                            s3.New(s, config),
		Route53:                       route53.New(s, config),
		Route53RateLimiter:            rate.NewLimiter(rate.Inf, 0),
		Route53RateLimiterWaitTimeout: 1 * time.Second,
		Logger:                        log.Log.WithName("aws-client"),
		PollInterval:                  5 * time.Second,
	}, nil
}

// GetAccountID returns the ID of the AWS account the Client is interacting with.
func (c *Client) GetAccountID(ctx context.Context) (string, error) {
	getCallerIdentityOutput, err := c.STS.GetCallerIdentityWithContext(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return *getCallerIdentityOutput.Account, nil
}

// GetVPCInternetGateway returns the ID of the internet gateway attached to the given VPC <vpcID>.
// If there is no internet gateway attached, the returned string will be empty.
func (c *Client) GetVPCInternetGateway(ctx context.Context, vpcID string) (string, error) {
	describeInternetGatewaysInput := &ec2.DescribeInternetGatewaysInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("attachment.vpc-id"),
				Values: []*string{
					aws.String(vpcID),
				},
			},
		},
	}
	describeInternetGatewaysOutput, err := c.EC2.DescribeInternetGatewaysWithContext(ctx, describeInternetGatewaysInput)
	if err != nil {
		return "", ignoreNotFound(err)
	}

	if len(describeInternetGatewaysOutput.InternetGateways) > 0 {
		return aws.StringValue(describeInternetGatewaysOutput.InternetGateways[0].InternetGatewayId), nil
	}
	return "", nil
}

// GetElasticIPsAssociationIDForAllocationIDs list existing elastic IP addresses for the given allocationIDs.
// returns a map[elasticIPAllocationID]elasticIPAssociationID or an error
func (c *Client) GetElasticIPsAssociationIDForAllocationIDs(ctx context.Context, allocationIDs []string) (map[string]*string, error) {
	describeAddressesInput := &ec2.DescribeAddressesInput{
		AllocationIds: aws.StringSlice(allocationIDs),
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("domain"),
				Values: aws.StringSlice([]string{"vpc"}),
			},
		},
	}

	describeAddressesOutput, err := c.EC2.DescribeAddressesWithContext(ctx, describeAddressesInput)
	if err != nil {
		return nil, ignoreNotFound(err)
	}

	if len(describeAddressesOutput.Addresses) == 0 {
		return nil, nil
	}

	result := make(map[string]*string, len(describeAddressesOutput.Addresses))
	for _, addr := range describeAddressesOutput.Addresses {
		if addr.AllocationId == nil {
			continue
		}
		result[*addr.AllocationId] = addr.AssociationId
	}

	return result, nil
}

// GetNATGatewayAddressAllocations get the allocation IDs for the NAT Gateway addresses for each existing NAT Gateway in the vpc
// returns a slice of allocation IDs or an error
func (c *Client) GetNATGatewayAddressAllocations(ctx context.Context, shootNamespace string) (sets.String, error) {
	describeAddressesInput := &ec2.DescribeNatGatewaysInput{
		Filter: []*ec2.Filter{{
			Name: aws.String(fmt.Sprintf("tag:kubernetes.io/cluster/%s", shootNamespace)),
			Values: []*string{
				aws.String("1"),
			},
		}},
	}

	describeNatGatewaysOutput, err := c.EC2.DescribeNatGatewaysWithContext(ctx, describeAddressesInput)
	if err != nil {
		return nil, ignoreNotFound(err)
	}

	result := sets.NewString()
	if len(describeNatGatewaysOutput.NatGateways) == 0 {
		return result, nil
	}

	for _, natGateway := range describeNatGatewaysOutput.NatGateways {
		if natGateway.NatGatewayAddresses == nil || len(natGateway.NatGatewayAddresses) == 0 {
			continue
		}

		// add all allocation IDS for the addresses for this NAT Gateway
		// these are the allocation IDS which identify the associated EIP
		for _, address := range natGateway.NatGatewayAddresses {
			if address == nil {
				continue
			}
			result.Insert(*address.AllocationId)
		}
	}

	return result, nil
}

// GetVPCAttribute returns the value of the specified VPC attribute.
func (c *Client) GetVPCAttribute(ctx context.Context, vpcID string, attribute string) (bool, error) {
	vpcAttribute, err := c.EC2.DescribeVpcAttributeWithContext(ctx, &ec2.DescribeVpcAttributeInput{VpcId: &vpcID, Attribute: aws.String(attribute)})
	if err != nil {
		return false, ignoreNotFound(err)
	}

	switch attribute {
	case "enableDnsSupport":
		return vpcAttribute.EnableDnsSupport != nil && vpcAttribute.EnableDnsSupport.Value != nil && *vpcAttribute.EnableDnsSupport.Value, nil
	case "enableDnsHostnames":
		return vpcAttribute.EnableDnsHostnames != nil && vpcAttribute.EnableDnsHostnames.Value != nil && *vpcAttribute.EnableDnsHostnames.Value, nil
	default:
		return false, nil
	}
}

// DeleteObjectsWithPrefix deletes the s3 objects with the specific <prefix> from <bucket>. If it does not exist,
// no error is returned.
func (c *Client) DeleteObjectsWithPrefix(ctx context.Context, bucket, prefix string) error {
	in := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	var delErr error
	if err := c.S3.ListObjectsPagesWithContext(ctx, in, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		objectIDs := make([]*s3.ObjectIdentifier, 0)
		for _, key := range page.Contents {
			obj := &s3.ObjectIdentifier{
				Key: key.Key,
			}
			objectIDs = append(objectIDs, obj)
		}

		if len(objectIDs) != 0 {
			if _, delErr = c.S3.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3.Delete{
					Objects: objectIDs,
					Quiet:   aws.Bool(true),
				},
			}); delErr != nil {
				return false
			}
		}
		return !lastPage
	}); err != nil {
		return err
	}

	if delErr != nil {
		if aerr, ok := delErr.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return nil
		}
		return delErr
	}
	return nil
}

// CreateBucketIfNotExists creates the s3 bucket with name <bucket> in <region>. If it already exist,
// no error is returned.
func (c *Client) CreateBucketIfNotExists(ctx context.Context, bucket, region string) error {
	createBucketInput := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		ACL:    aws.String(s3.BucketCannedACLPrivate),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region),
		},
	}

	if region == "us-east-1" {
		createBucketInput.CreateBucketConfiguration = nil
	}

	if _, err := c.S3.CreateBucketWithContext(ctx, createBucketInput); err != nil {
		if aerr, ok := err.(awserr.Error); !ok {
			return err
		} else if aerr.Code() != s3.ErrCodeBucketAlreadyExists && aerr.Code() != s3.ErrCodeBucketAlreadyOwnedByYou {
			return err
		}
	}

	// Enable default server side encryption using AES256 algorithm. Key will be managed by S3
	if _, err := c.S3.PutBucketEncryptionWithContext(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(bucket),
		ServerSideEncryptionConfiguration: &s3.ServerSideEncryptionConfiguration{
			Rules: []*s3.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &s3.ServerSideEncryptionByDefault{
						SSEAlgorithm: aws.String("AES256"),
					},
				},
			},
		},
	}); err != nil {
		return err
	}

	// Set lifecycle rule to purge incomplete multipart upload orphaned because of force shutdown or rescheduling or networking issue with etcd-backup-restore.
	putBucketLifecycleConfigurationInput := &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					// Note: Though as per documentation at https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html the Filter field is
					// optional, if not specified the SDK API fails with `Malformed XML` error code. Cross verified same behavior with aws-cli client as well.
					// Please do not remove it.
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(7),
					},
					Status: aws.String(s3.ExpirationStatusEnabled),
				},
			},
		},
	}

	_, err := c.S3.PutBucketLifecycleConfigurationWithContext(ctx, putBucketLifecycleConfigurationInput)
	return err
}

// DeleteBucketIfExists deletes the s3 bucket with name <bucket>. If it does not exist,
// no error is returned.
func (c *Client) DeleteBucketIfExists(ctx context.Context, bucket string) error {
	if _, err := c.S3.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return nil
			}
			if aerr.Code() == errCodeBucketNotEmpty {
				if err := c.DeleteObjectsWithPrefix(ctx, bucket, ""); err != nil {
					return err
				}
				return c.DeleteBucketIfExists(ctx, bucket)
			}
		}
		return err
	}
	return nil
}

// The following functions are only temporary needed due to https://github.com/gardener/gardener/issues/129.

// ListKubernetesELBs returns the list of ELB loadbalancers in the given <vpcID> tagged with <clusterName>.
func (c *Client) ListKubernetesELBs(ctx context.Context, vpcID, clusterName string) ([]string, error) {
	var (
		loadBalancerNamesInVPC      []*string
		loadBalancerNamesForCluster []string
	)

	if err := c.ELB.DescribeLoadBalancersPagesWithContext(ctx, &elb.DescribeLoadBalancersInput{}, func(page *elb.DescribeLoadBalancersOutput, lastPage bool) bool {
		for _, lb := range page.LoadBalancerDescriptions {
			if lb.VPCId != nil && *lb.VPCId == vpcID {
				loadBalancerNamesInVPC = append(loadBalancerNamesInVPC, lb.LoadBalancerName)
			}
		}
		return !lastPage
	}); err != nil {
		return nil, err
	}

	if len(loadBalancerNamesInVPC) == 0 {
		return nil, nil
	}

	const chunkSize = 20
	loadBalancerNamesChunks := chunkSlice(loadBalancerNamesInVPC, chunkSize)
	for _, loadBalancerNamesChunk := range loadBalancerNamesChunks {
		tags, err := c.ELB.DescribeTagsWithContext(ctx, &elb.DescribeTagsInput{LoadBalancerNames: loadBalancerNamesChunk})
		if err != nil {
			return nil, err
		}

		for _, description := range tags.TagDescriptions {
			for _, tag := range description.Tags {
				if tag.Key != nil && *tag.Key == fmt.Sprintf("kubernetes.io/cluster/%s", clusterName) &&
					tag.Value != nil && *tag.Value == "owned" &&
					description.LoadBalancerName != nil {
					loadBalancerNamesForCluster = append(loadBalancerNamesForCluster, *description.LoadBalancerName)
					break
				}
			}
		}
	}

	return loadBalancerNamesForCluster, nil
}

// DeleteELB deletes the loadbalancer with the specific <name>. If it does not exist,
// no error is returned.
func (c *Client) DeleteELB(ctx context.Context, name string) error {
	_, err := c.ELB.DeleteLoadBalancerWithContext(ctx, &elb.DeleteLoadBalancerInput{LoadBalancerName: aws.String(name)})
	return ignoreNotFound(err)
}

// ListKubernetesELBsV2 returns the list of ELBv2 loadbalancers in the given <vpcID> tagged with <clusterName>.
func (c *Client) ListKubernetesELBsV2(ctx context.Context, vpcID, clusterName string) ([]string, error) {
	var (
		loadBalancerARNsInVPC      []*string
		loadBalancerARNsForCluster []string
	)

	if err := c.ELBv2.DescribeLoadBalancersPagesWithContext(ctx, &elbv2.DescribeLoadBalancersInput{}, func(page *elbv2.DescribeLoadBalancersOutput, lastPage bool) bool {
		for _, lb := range page.LoadBalancers {
			if lb.VpcId != nil && *lb.VpcId == vpcID {
				loadBalancerARNsInVPC = append(loadBalancerARNsInVPC, lb.LoadBalancerArn)
			}
		}
		return !lastPage
	}); err != nil {
		return nil, err
	}

	if len(loadBalancerARNsInVPC) == 0 {
		return nil, nil
	}

	const chunkSize = 20
	loadBalancerARNsChunks := chunkSlice(loadBalancerARNsInVPC, chunkSize)
	for _, loadBalancerARNsChunk := range loadBalancerARNsChunks {
		tags, err := c.ELBv2.DescribeTagsWithContext(ctx, &elbv2.DescribeTagsInput{ResourceArns: loadBalancerARNsChunk})
		if err != nil {
			return nil, err
		}

		for _, description := range tags.TagDescriptions {
			for _, tag := range description.Tags {
				if tag.Key != nil && *tag.Key == fmt.Sprintf("kubernetes.io/cluster/%s", clusterName) &&
					tag.Value != nil && *tag.Value == "owned" &&
					description.ResourceArn != nil {
					loadBalancerARNsForCluster = append(loadBalancerARNsForCluster, *description.ResourceArn)
				}
			}
		}
	}

	return loadBalancerARNsForCluster, nil
}

// DeleteELBV2 deletes the loadbalancer (NLB or ALB) as well as its target groups with its Amazon Resource Name (ARN). If it does not exist,
// no error is returned.
func (c *Client) DeleteELBV2(ctx context.Context, arn string) error {
	targetGroups, err := c.ELBv2.DescribeTargetGroups(&elbv2.DescribeTargetGroupsInput{LoadBalancerArn: &arn})
	if err != nil {
		return fmt.Errorf("could not list loadbalancer target groups for arn %s: %w", arn, err)
	}

	if _, err := c.ELBv2.DeleteLoadBalancerWithContext(ctx, &elbv2.DeleteLoadBalancerInput{LoadBalancerArn: &arn}); ignoreNotFound(err) != nil {
		return fmt.Errorf("could not delete loadbalancer for arn %s: %w", arn, err)
	}

	for _, group := range targetGroups.TargetGroups {
		if _, err := c.ELBv2.DeleteTargetGroup(&elbv2.DeleteTargetGroupInput{TargetGroupArn: group.TargetGroupArn}); err != nil {
			return fmt.Errorf("could not delete target groups after deleting loadbalancer for arn %s: %w", arn, err)
		}
	}

	return nil
}

// ListKubernetesSecurityGroups returns the list of security groups in the given <vpcID> tagged with <clusterName>.
func (c *Client) ListKubernetesSecurityGroups(ctx context.Context, vpcID, clusterName string) ([]string, error) {
	groups, err := c.EC2.DescribeSecurityGroupsWithContext(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("vpc-id"),
				Values: []*string{aws.String(vpcID)},
			},
			{
				Name:   aws.String("tag-key"),
				Values: []*string{aws.String(fmt.Sprintf("kubernetes.io/cluster/%s", clusterName))},
			},
			{
				Name:   aws.String("tag-value"),
				Values: []*string{aws.String("owned")},
			},
		},
	})
	if err != nil {
		return nil, ignoreNotFound(err)
	}

	var results []string
	for _, group := range groups.SecurityGroups {
		results = append(results, *group.GroupId)
	}

	return results, nil
}

func (c *Client) CreateVpcDhcpOptions(ctx context.Context, options *DhcpOptions) (*DhcpOptions, error) {
	var newConfigs []*ec2.NewDhcpConfiguration

	for key, values := range options.DhcpConfigurations {
		newConfigs = append(newConfigs, &ec2.NewDhcpConfiguration{
			Key:    aws.String(key),
			Values: aws.StringSlice(values),
		})
	}
	input := &ec2.CreateDhcpOptionsInput{
		DhcpConfigurations: newConfigs,
		TagSpecifications:  options.ToTagSpecifications(ec2.ResourceTypeDhcpOptions),
	}
	output, err := c.EC2.CreateDhcpOptionsWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return fromDhcpOptions(output.DhcpOptions), nil
}

func (c *Client) GetVpcDhcpOptions(ctx context.Context, id string) (*DhcpOptions, error) {
	input := &ec2.DescribeDhcpOptionsInput{DhcpOptionsIds: []*string{aws.String(id)}}
	output, err := c.describeVpcDhcpOptions(ctx, input)
	return single(output, err)
}

func (c *Client) FindVpcDhcpOptionsByTags(ctx context.Context, tags Tags) ([]*DhcpOptions, error) {
	input := &ec2.DescribeDhcpOptionsInput{Filters: tags.ToFilters()}
	return c.describeVpcDhcpOptions(ctx, input)
}

func (c *Client) describeVpcDhcpOptions(ctx context.Context, input *ec2.DescribeDhcpOptionsInput) ([]*DhcpOptions, error) {
	output, err := c.EC2.DescribeDhcpOptionsWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var options []*DhcpOptions
	for _, item := range output.DhcpOptions {
		options = append(options, fromDhcpOptions(item))
	}
	return options, nil
}

func (c *Client) DeleteVpcDhcpOptions(ctx context.Context, id string) error {
	_, err := c.EC2.DeleteDhcpOptionsWithContext(ctx, &ec2.DeleteDhcpOptionsInput{DhcpOptionsId: aws.String(id)})
	return ignoreNotFound(err)
}

func (c *Client) CreateVpc(ctx context.Context, desired *VPC) (*VPC, error) {
	input := &ec2.CreateVpcInput{
		CidrBlock: aws.String(desired.CidrBlock),

		TagSpecifications: desired.ToTagSpecifications(ec2.ResourceTypeVpc),
	}
	output, err := c.EC2.CreateVpc(input)
	if err != nil {
		return nil, err
	}
	vpcID := *output.Vpc.VpcId
	return c.GetVpc(ctx, vpcID)
}

func (c *Client) UpdateVpcAttribute(ctx context.Context, vpcId, attributeName string, value bool) error {
	switch attributeName {
	case ec2.VpcAttributeNameEnableDnsSupport:
		input := &ec2.ModifyVpcAttributeInput{
			EnableDnsSupport: &ec2.AttributeBooleanValue{
				Value: aws.Bool(value),
			},
			VpcId: aws.String(vpcId),
		}
		if _, err := c.EC2.ModifyVpcAttribute(input); err != nil {
			return err
		}
		if err := c.Wait(ctx, func(ctx context.Context) (bool, error) {
			b, err := c.describeVpcAttributeWithContext(ctx, aws.String(vpcId), ec2.VpcAttributeNameEnableDnsSupport)
			return b == value, err
		}); err != nil {
			return err
		}
		return nil
	case ec2.VpcAttributeNameEnableDnsHostnames:
		input := &ec2.ModifyVpcAttributeInput{
			EnableDnsHostnames: &ec2.AttributeBooleanValue{
				Value: aws.Bool(value),
			},
			VpcId: aws.String(vpcId),
		}
		if _, err := c.EC2.ModifyVpcAttribute(input); err != nil {
			return err
		}
		if err := c.Wait(ctx, func(ctx context.Context) (bool, error) {
			b, err := c.describeVpcAttributeWithContext(ctx, aws.String(vpcId), ec2.VpcAttributeNameEnableDnsHostnames)
			return b == value, err
		}); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown attribute name: %s", attributeName)
	}
}

func (c *Client) AddVpcDhcpOptionAssociation(vpcId string, dhcpOptionsId *string) error {
	if dhcpOptionsId == nil {
		// AWS does not provide an API to disassociate a DHCP Options set from a VPC.
		// So, we do this by setting the VPC to the default DHCP Options Set.
		id := DefaultDhcpOptionsId
		dhcpOptionsId = &id
	}
	_, err := c.EC2.AssociateDhcpOptions(&ec2.AssociateDhcpOptionsInput{
		DhcpOptionsId: dhcpOptionsId,
		VpcId:         aws.String(vpcId),
	})
	return err
}

func (c *Client) DeleteVpc(ctx context.Context, id string) error {
	_, err := c.EC2.DeleteVpcWithContext(ctx, &ec2.DeleteVpcInput{VpcId: aws.String(id)})
	return ignoreNotFound(err)
}

func (c *Client) GetVpc(ctx context.Context, id string) (*VPC, error) {
	input := &ec2.DescribeVpcsInput{VpcIds: []*string{aws.String(id)}}
	output, err := c.describeVpcs(ctx, input)
	return single(output, err)
}

func (c *Client) FindVpcsByTags(ctx context.Context, tags Tags) ([]*VPC, error) {
	input := &ec2.DescribeVpcsInput{Filters: tags.ToFilters()}
	return c.describeVpcs(ctx, input)
}

func (c *Client) describeVpcs(ctx context.Context, input *ec2.DescribeVpcsInput) ([]*VPC, error) {
	output, err := c.EC2.DescribeVpcs(input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var vpcList []*VPC
	for _, item := range output.Vpcs {
		vpc, err := c.fromVpc(ctx, item, true)
		if err != nil {
			return nil, err
		}
		vpcList = append(vpcList, vpc)
	}
	return vpcList, nil
}

func (c *Client) fromVpc(ctx context.Context, item *ec2.Vpc, withAttributes bool) (*VPC, error) {
	vpc := &VPC{
		VpcId:         aws.StringValue(item.VpcId),
		Tags:          FromTags(item.Tags),
		CidrBlock:     aws.StringValue(item.CidrBlock),
		DhcpOptionsId: item.DhcpOptionsId,
	}
	var err error
	if withAttributes {
		if vpc.EnableDnsHostnames, err = c.describeVpcAttributeWithContext(ctx, item.VpcId, ec2.VpcAttributeNameEnableDnsHostnames); err != nil {
			return nil, err
		}
		if vpc.EnableDnsSupport, err = c.describeVpcAttributeWithContext(ctx, item.VpcId, ec2.VpcAttributeNameEnableDnsSupport); err != nil {
			return nil, err
		}
	}
	return vpc, nil
}

func (c *Client) describeVpcAttributeWithContext(ctx context.Context, vpcId *string, attributeName string) (bool, error) {
	output, err := c.EC2.DescribeVpcAttributeWithContext(ctx, &ec2.DescribeVpcAttributeInput{
		Attribute: aws.String(attributeName),
		VpcId:     vpcId,
	})
	if err != nil {
		return false, ignoreNotFound(err)
	}
	switch attributeName {
	case ec2.VpcAttributeNameEnableDnsHostnames:
		return *output.EnableDnsHostnames.Value, nil
	case ec2.VpcAttributeNameEnableDnsSupport:
		return *output.EnableDnsSupport.Value, nil
	default:
		return false, fmt.Errorf("unknown attribute: %s", attributeName)
	}
}

func (c *Client) CreateSecurityGroup(ctx context.Context, sg *SecurityGroup) (*SecurityGroup, error) {
	input := &ec2.CreateSecurityGroupInput{
		GroupName:         aws.String(sg.GroupName),
		TagSpecifications: sg.ToTagSpecifications(ec2.ResourceTypeSecurityGroup),
		VpcId:             sg.VpcId,
		Description:       sg.Description,
	}
	output, err := c.EC2.CreateSecurityGroupWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	created := *sg
	created.Rules = nil
	created.GroupId = *output.GroupId
	if err = c.UpdateSecurityGroupRules(ctx, sg); err != nil {
		return &created, err
	}
	for _, rule := range sg.Rules {
		r := *rule
		created.Rules = append(created.Rules, &r)
	}
	return &created, nil
}

func (c *Client) UpdateSecurityGroupRules(ctx context.Context, sg *SecurityGroup) error {
	inputIngress := &ec2.UpdateSecurityGroupRuleDescriptionsIngressInput{
		GroupId: aws.String(sg.GroupId),
	}
	inputEgress := &ec2.UpdateSecurityGroupRuleDescriptionsEgressInput{
		GroupId: aws.String(sg.GroupId),
	}
	for _, rule := range sg.Rules {
		ipPerm := &ec2.IpPermission{
			IpProtocol:       aws.String(rule.Protocol),
			IpRanges:         nil,
			PrefixListIds:    nil,
			UserIdGroupPairs: nil,
		}
		if rule.FromPort != 0 {
			ipPerm.FromPort = aws.Int64(int64(rule.FromPort))
		}
		if rule.ToPort != 0 {
			ipPerm.ToPort = aws.Int64(int64(rule.ToPort))
		}
		for _, block := range rule.CidrBlocks {
			ipPerm.IpRanges = append(ipPerm.IpRanges, &ec2.IpRange{CidrIp: aws.String(block)})
		}
		switch rule.Type {
		case SecurityGroupRuleTypeIngress:
			inputIngress.IpPermissions = append(inputIngress.IpPermissions, ipPerm)
		case SecurityGroupRuleTypeEgress:
			inputEgress.IpPermissions = append(inputEgress.IpPermissions, ipPerm)
		default:
			return fmt.Errorf("unknown security group rule type: %s", rule.Type)
		}
	}
	var current *ec2.SecurityGroup
	if len(inputIngress.IpPermissions) == 0 || len(inputEgress.IpPermissions) == 0 {
		input := &ec2.DescribeSecurityGroupsInput{GroupIds: []*string{aws.String(sg.GroupId)}}
		output, err := c.EC2.DescribeSecurityGroupsWithContext(ctx, input)
		if err != nil {
			return err
		}
		if len(output.SecurityGroups) == 0 {
			return fmt.Errorf("security group %s not found", sg.GroupId)
		}
		current = output.SecurityGroups[0]
	}
	if len(inputIngress.IpPermissions) > 0 {
		if _, err := c.EC2.UpdateSecurityGroupRuleDescriptionsIngressWithContext(ctx, inputIngress); err != nil {
			return err
		}
	} else {
		if _, err := c.EC2.RevokeSecurityGroupIngressWithContext(ctx, &ec2.RevokeSecurityGroupIngressInput{
			GroupId:       aws.String(sg.GroupId),
			IpPermissions: current.IpPermissions,
		}); err != nil {
			return err
		}
	}
	if len(inputEgress.IpPermissions) > 0 {
		if _, err := c.EC2.UpdateSecurityGroupRuleDescriptionsEgressWithContext(ctx, inputEgress); err != nil {
			return err
		}
	} else {
		if _, err := c.EC2.RevokeSecurityGroupEgressWithContext(ctx, &ec2.RevokeSecurityGroupEgressInput{
			GroupId:       aws.String(sg.GroupId),
			IpPermissions: current.IpPermissionsEgress,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) GetSecurityGroup(ctx context.Context, id string) (*SecurityGroup, error) {
	input := &ec2.DescribeSecurityGroupsInput{GroupIds: []*string{aws.String(id)}}
	output, err := c.describeSecurityGroups(ctx, input)
	return single(output, err)
}

func (c *Client) FindSecurityGroupsByTags(ctx context.Context, tags Tags) ([]*SecurityGroup, error) {
	input := &ec2.DescribeSecurityGroupsInput{Filters: tags.ToFilters()}
	return c.describeSecurityGroups(ctx, input)
}

func (c *Client) describeSecurityGroups(ctx context.Context, input *ec2.DescribeSecurityGroupsInput) ([]*SecurityGroup, error) {
	output, err := c.EC2.DescribeSecurityGroupsWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var sgList []*SecurityGroup
	for _, item := range output.SecurityGroups {
		sg := &SecurityGroup{
			Tags:        FromTags(item.Tags),
			GroupId:     aws.StringValue(item.GroupId),
			GroupName:   aws.StringValue(item.GroupName),
			VpcId:       item.VpcId,
			Description: item.Description,
		}
		for _, ipPerm := range item.IpPermissions {
			rule := fromIpPermission(ipPerm, SecurityGroupRuleTypeIngress)
			sg.Rules = append(sg.Rules, rule)
		}
		sgList = append(sgList, sg)
	}
	return sgList, nil
}

func (c *Client) FindDefaultSecurityGroupByVpcId(ctx context.Context, vpcId string) (*SecurityGroup, error) {
	input := &ec2.DescribeSecurityGroupsInput{
		Filters: []*ec2.Filter{
			{Name: aws.String("vpc-id"), Values: aws.StringSlice([]string{vpcId})},
		},
	}
	groups, err := c.describeSecurityGroups(ctx, input)
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		if group.GroupName == "default" {
			return group, nil
		}
	}
	return nil, nil
}

// DeleteSecurityGroup deletes the security group with the specific <id>. If it does not exist, no error is returned.
func (c *Client) DeleteSecurityGroup(ctx context.Context, id string) error {
	_, err := c.EC2.DeleteSecurityGroupWithContext(ctx, &ec2.DeleteSecurityGroupInput{GroupId: aws.String(id)})
	return ignoreNotFound(err)
}

func (c *Client) CreateInternetGateway(ctx context.Context, gateway *InternetGateway) (*InternetGateway, error) {
	input := &ec2.CreateInternetGatewayInput{
		TagSpecifications: gateway.ToTagSpecifications(ec2.ResourceTypeInternetGateway),
	}
	output, err := c.EC2.CreateInternetGatewayWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return &InternetGateway{
		Tags:              FromTags(output.InternetGateway.Tags),
		InternetGatewayId: aws.StringValue(output.InternetGateway.InternetGatewayId),
	}, nil
}

func (c *Client) AttachInternetGateway(ctx context.Context, vpcId, internetGatewayId string) error {
	input := &ec2.AttachInternetGatewayInput{
		InternetGatewayId: aws.String(internetGatewayId),
		VpcId:             aws.String(vpcId),
	}
	_, err := c.EC2.AttachInternetGatewayWithContext(ctx, input)
	return err
}

func (c *Client) DetachInternetGateway(ctx context.Context, vpcId, internetGatewayId string) error {
	input := &ec2.DetachInternetGatewayInput{
		InternetGatewayId: aws.String(internetGatewayId),
		VpcId:             aws.String(vpcId),
	}
	_, err := c.EC2.DetachInternetGatewayWithContext(ctx, input)
	return err
}

func (c *Client) GetInternetGateway(ctx context.Context, id string) (*InternetGateway, error) {
	input := &ec2.DescribeInternetGatewaysInput{InternetGatewayIds: []*string{aws.String(id)}}
	output, err := c.describeInternetGateways(ctx, input)
	return single(output, err)
}

func (c *Client) FindInternetGatewaysByTags(ctx context.Context, tags Tags) ([]*InternetGateway, error) {
	input := &ec2.DescribeInternetGatewaysInput{Filters: tags.ToFilters()}
	return c.describeInternetGateways(ctx, input)
}

func (c *Client) describeInternetGateways(ctx context.Context, input *ec2.DescribeInternetGatewaysInput) ([]*InternetGateway, error) {
	output, err := c.EC2.DescribeInternetGatewaysWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var gateways []*InternetGateway
	for _, item := range output.InternetGateways {
		gw := &InternetGateway{
			Tags:              FromTags(item.Tags),
			InternetGatewayId: aws.StringValue(item.InternetGatewayId),
		}
		for _, attachment := range item.Attachments {
			gw.VpcId = attachment.VpcId
			break
		}
		gateways = append(gateways, gw)
	}
	return gateways, nil
}

func (c *Client) DeleteInternetGateway(ctx context.Context, id string) error {
	input := &ec2.DeleteInternetGatewayInput{
		InternetGatewayId: aws.String(id),
	}
	_, err := c.EC2.DeleteInternetGatewayWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateVpcEndpoint(ctx context.Context, endpoint *VpcEndpoint) (*VpcEndpoint, error) {
	input := &ec2.CreateVpcEndpointInput{
		ServiceName: aws.String(endpoint.ServiceName),
		//TagSpecifications: endpoint.ToTagSpecifications(ec2.ResourceTypeClientVpnEndpoint),
		VpcId: endpoint.VpcId,
	}
	output, err := c.EC2.CreateVpcEndpointWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return &VpcEndpoint{
		//Tags:          FromTags(output.VpcEndpoint.Tags),
		VpcEndpointId: aws.StringValue(output.VpcEndpoint.VpcEndpointId),
		VpcId:         output.VpcEndpoint.VpcId,
		ServiceName:   aws.StringValue(output.VpcEndpoint.ServiceName),
	}, nil
}

func (c *Client) GetVpcEndpoints(ctx context.Context, ids []string) ([]*VpcEndpoint, error) {
	input := &ec2.DescribeVpcEndpointsInput{VpcEndpointIds: aws.StringSlice(ids)}
	return c.describeVpcEndpoints(ctx, input)
}

func (c *Client) FindVpcEndpointsByTags(ctx context.Context, tags Tags) ([]*VpcEndpoint, error) {
	input := &ec2.DescribeVpcEndpointsInput{Filters: tags.ToFilters()}
	return c.describeVpcEndpoints(ctx, input)
}

func (c *Client) describeVpcEndpoints(ctx context.Context, input *ec2.DescribeVpcEndpointsInput) ([]*VpcEndpoint, error) {
	output, err := c.EC2.DescribeVpcEndpointsWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var endpoints []*VpcEndpoint
	for _, item := range output.VpcEndpoints {
		endpoint := &VpcEndpoint{
			Tags:          FromTags(item.Tags),
			VpcEndpointId: aws.StringValue(item.VpcEndpointId),
			VpcId:         item.VpcId,
			ServiceName:   aws.StringValue(item.ServiceName),
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints, nil
}

func (c *Client) DeleteVpcEndpoint(ctx context.Context, id string) error {
	input := &ec2.DeleteVpcEndpointsInput{
		VpcEndpointIds: []*string{&id},
	}
	_, err := c.EC2.DeleteVpcEndpointsWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateRouteTable(ctx context.Context, routeTable *RouteTable) (*RouteTable, error) {
	input := &ec2.CreateRouteTableInput{
		TagSpecifications: routeTable.ToTagSpecifications(ec2.ResourceTypeRouteTable),
		VpcId:             routeTable.VpcId,
	}
	output, err := c.EC2.CreateRouteTableWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	created := &RouteTable{
		Tags:         FromTags(output.RouteTable.Tags),
		RouteTableId: aws.StringValue(output.RouteTable.RouteTableId),
		VpcId:        output.RouteTable.VpcId,
	}

	return created, nil
}

func (c *Client) CreateRoute(ctx context.Context, routeTableId string, route *Route) error {
	input := &ec2.CreateRouteInput{
		DestinationCidrBlock: aws.String(route.DestinationCidrBlock),
		GatewayId:            route.GatewayId,
		NatGatewayId:         route.NatGatewayId,
		RouteTableId:         aws.String(routeTableId),
	}
	_, err := c.EC2.CreateRouteWithContext(ctx, input)
	return err
}

func (c *Client) DeleteRoute(ctx context.Context, routeTableId string, route *Route) error {
	input := &ec2.DeleteRouteInput{
		DestinationCidrBlock: aws.String(route.DestinationCidrBlock),
		RouteTableId:         aws.String(routeTableId),
	}
	_, err := c.EC2.DeleteRouteWithContext(ctx, input)
	return err
}

func (c *Client) DescribeRouteTables(ctx context.Context, id *string, tags Tags) ([]*RouteTable, error) {
	input := &ec2.DescribeRouteTablesInput{}
	if id != nil {
		input.RouteTableIds = []*string{id}
	} else {
		input.Filters = tags.ToFilters()
	}
	output, err := c.EC2.DescribeRouteTablesWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var tables []*RouteTable
	for _, item := range output.RouteTables {
		table := &RouteTable{
			Tags:         FromTags(item.Tags),
			RouteTableId: aws.StringValue(item.RouteTableId),
			VpcId:        item.VpcId,
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (c *Client) DeleteRouteTable(ctx context.Context, id string) error {
	input := &ec2.DeleteRouteTableInput{
		RouteTableId: aws.String(id),
	}
	_, err := c.EC2.DeleteRouteTableWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateSubnet(ctx context.Context, subnet *Subnet) (*Subnet, error) {
	input := &ec2.CreateSubnetInput{
		AvailabilityZone:  aws.String(subnet.AvailabilityZone),
		CidrBlock:         aws.String(subnet.CidrBlock),
		TagSpecifications: subnet.ToTagSpecifications(ec2.ResourceTypeSubnet),
		VpcId:             aws.String(subnet.VpcId),
	}
	output, err := c.EC2.CreateSubnetWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return fromSubnet(output.Subnet), nil
}

func (c *Client) DescribeSubnets(ctx context.Context, id *string, tags Tags) ([]*Subnet, error) {
	input := &ec2.DescribeSubnetsInput{}
	if id != nil {
		input.SubnetIds = []*string{id}
	} else {
		input.Filters = tags.ToFilters()
	}
	output, err := c.EC2.DescribeSubnetsWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var subnets []*Subnet
	for _, item := range output.Subnets {
		subnets = append(subnets, fromSubnet(item))
	}
	return subnets, nil
}

func (c *Client) DeleteSubnet(ctx context.Context, id string) error {
	input := &ec2.DeleteSubnetInput{
		SubnetId: aws.String(id),
	}
	_, err := c.EC2.DeleteSubnetWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateElasticIP(ctx context.Context, eip *ElasticIP) (*ElasticIP, error) {
	domainOpt := ""
	if eip.Vpc {
		domainOpt = ec2.DomainTypeVpc
	}
	input := &ec2.AllocateAddressInput{
		Domain:            aws.String(domainOpt),
		TagSpecifications: eip.ToTagSpecifications(ec2.ResourceTypeElasticIp),
	}
	output, err := c.EC2.AllocateAddressWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return &ElasticIP{
		Tags:         eip.Tags.Clone(),
		Vpc:          eip.Vpc,
		AllocationId: aws.StringValue(output.AllocationId),
		PublicIp:     aws.StringValue(output.PublicIp),
	}, nil
}

func (c *Client) DescribeElasticIPs(ctx context.Context, id *string, tags Tags) ([]*ElasticIP, error) {
	input := &ec2.DescribeAddressesInput{}
	if id != nil {
		input.AllocationIds = []*string{id}
	} else {
		input.Filters = tags.ToFilters()
	}
	output, err := c.EC2.DescribeAddressesWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var eips []*ElasticIP
	for _, item := range output.Addresses {
		eips = append(eips, fromAddress(item))
	}
	return eips, nil
}

func (c *Client) DeleteElasticIP(ctx context.Context, id string) error {
	input := &ec2.ReleaseAddressInput{
		AllocationId: aws.String(id),
	}
	_, err := c.EC2.ReleaseAddressWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateNATGateway(ctx context.Context, gateway *NATGateway) (*NATGateway, error) {
	input := &ec2.CreateNatGatewayInput{
		AllocationId:      aws.String(gateway.EIPAllocationId),
		SubnetId:          aws.String(gateway.SubnetId),
		TagSpecifications: gateway.ToTagSpecifications(ec2.ResourceTypeNatgateway),
	}
	output, err := c.EC2.CreateNatGatewayWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return fromNatGateway(output.NatGateway), nil
}

func (c *Client) DescribeNATGateways(ctx context.Context, id *string, tags Tags) ([]*NATGateway, error) {
	input := &ec2.DescribeNatGatewaysInput{}
	if id != nil {
		input.NatGatewayIds = []*string{id}
	} else {
		input.Filter = tags.ToFilters()
	}
	output, err := c.EC2.DescribeNatGatewaysWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var gateways []*NATGateway
	for _, item := range output.NatGateways {
		gateways = append(gateways, fromNatGateway(item))
	}
	return gateways, nil
}

func (c *Client) DeleteNATGateway(ctx context.Context, id string) error {
	input := &ec2.DeleteNatGatewayInput{
		NatGatewayId: aws.String(id),
	}
	_, err := c.EC2.DeleteNatGatewayWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) ImportKeyPair(ctx context.Context, keyName string, publicKey []byte, tags Tags) (*KeyPairInfo, error) {
	input := &ec2.ImportKeyPairInput{
		KeyName:           aws.String(keyName),
		PublicKeyMaterial: publicKey,
		TagSpecifications: tags.ToTagSpecifications(ec2.ResourceTypeKeyPair),
	}
	output, err := c.EC2.ImportKeyPairWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return &KeyPairInfo{
		Tags:           FromTags(output.Tags),
		KeyName:        aws.StringValue(output.KeyName),
		KeyFingerprint: aws.StringValue(output.KeyFingerprint),
	}, nil
}

func (c *Client) DescribeKeyPairs(ctx context.Context, keyName *string, tags Tags) ([]*KeyPairInfo, error) {
	input := &ec2.DescribeKeyPairsInput{}
	if keyName != nil {
		input.KeyNames = []*string{keyName}
	} else {
		input.Filters = tags.ToFilters()
	}
	output, err := c.EC2.DescribeKeyPairsWithContext(ctx, input)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	var pairs []*KeyPairInfo
	for _, item := range output.KeyPairs {
		pairs = append(pairs, fromKeyPairInfo(item))
	}
	return pairs, nil
}

func (c *Client) DeleteKeyPair(ctx context.Context, keyName string) error {
	input := &ec2.DeleteKeyPairInput{
		KeyName: aws.String(keyName),
	}
	_, err := c.EC2.DeleteKeyPairWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateRouteTableAssociation(ctx context.Context, routeTableId, subnetId string) (*string, error) {
	input := &ec2.AssociateRouteTableInput{
		RouteTableId: aws.String(routeTableId),
		SubnetId:     aws.String(subnetId),
	}
	output, err := c.EC2.AssociateRouteTableWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return output.AssociationId, nil
}

func (c *Client) DeleteRouteTableAssociation(ctx context.Context, associationId string) error {
	input := &ec2.DisassociateRouteTableInput{
		AssociationId: aws.String(associationId),
	}
	_, err := c.EC2.DisassociateRouteTableWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateIAMRole(ctx context.Context, role *IAMRole) (*IAMRole, error) {
	input := &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(role.AssumeRolePolicyDocument),
		Path:                     aws.String(role.Path),
		RoleName:                 aws.String(role.RoleName),
		Tags:                     role.ToIAMTags(),
	}
	output, err := c.IAM.CreateRoleWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return fromIAMRole(output.Role), nil
}

func (c *Client) GetIAMRole(ctx context.Context, roleName string) (*IAMRole, error) {
	input := &iam.GetRoleInput{
		RoleName: aws.String(roleName),
	}
	output, err := c.IAM.GetRoleWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return fromIAMRole(output.Role), nil
}

func (c *Client) DeleteIAMRole(ctx context.Context, roleName string) error {
	input := &iam.DeleteRoleInput{
		RoleName: aws.String(roleName),
	}
	_, err := c.IAM.DeleteRoleWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateIAMInstanceProfile(ctx context.Context, profile *IAMInstanceProfile) (*IAMInstanceProfile, error) {
	input := &iam.CreateInstanceProfileInput{
		InstanceProfileName: aws.String(profile.InstanceProfileName),
		Path:                aws.String(profile.Path),
		Tags:                profile.ToIAMTags(),
	}
	output, err := c.IAM.CreateInstanceProfileWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	profileName := aws.StringValue(output.InstanceProfile.InstanceProfileName)
	if err = c.Wait(ctx, func(ctx context.Context) (done bool, err error) {
		if item, err := c.GetIAMInstanceProfile(ctx, profileName); err != nil {
			return false, err
		} else {
			return item != nil, nil
		}
	}); err != nil {
		return nil, err
	}
	created := *profile
	created.RoleName = ""
	if err = c.AddRoleToIAMInstanceProfile(ctx, profileName, profile.RoleName); err != nil {
		return &created, err
	}
	return c.GetIAMInstanceProfile(ctx, profileName)
}

func (c *Client) GetIAMInstanceProfile(ctx context.Context, profileName string) (*IAMInstanceProfile, error) {
	input := &iam.GetInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
	}
	output, err := c.IAM.GetInstanceProfileWithContext(ctx, input)
	if err != nil {
		if IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	return fromIAMInstanceProfile(output.InstanceProfile), nil
}

func (c *Client) AddRoleToIAMInstanceProfile(ctx context.Context, profileName, roleName string) error {
	input := &iam.AddRoleToInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
		RoleName:            aws.String(roleName),
	}
	_, err := c.IAM.AddRoleToInstanceProfileWithContext(ctx, input)
	return err
}

func (c *Client) RemoveRoleFromIAMInstanceProfile(ctx context.Context, profileName, roleName string) error {
	input := &iam.RemoveRoleFromInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
		RoleName:            aws.String(roleName),
	}
	_, err := c.IAM.RemoveRoleFromInstanceProfileWithContext(ctx, input)
	return err
}

func (c *Client) DeleteIAMInstanceProfile(ctx context.Context, profileName string) error {
	input := &iam.DeleteInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
	}
	_, err := c.IAM.DeleteInstanceProfileWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) PutIAMRolePolicy(ctx context.Context, policy *IAMRolePolicy) error {
	input := &iam.PutRolePolicyInput{
		PolicyDocument: aws.String(policy.PolicyDocument),
		PolicyName:     aws.String(policy.PolicyName),
		RoleName:       aws.String(policy.RoleName),
	}
	_, err := c.IAM.PutRolePolicyWithContext(ctx, input)
	return err
}

func (c *Client) GetIAMRolePolicy(ctx context.Context, policyName, roleName string) (*IAMRolePolicy, error) {
	input := &iam.GetRolePolicyInput{
		PolicyName: aws.String(policyName),
		RoleName:   aws.String(roleName),
	}
	output, err := c.IAM.GetRolePolicyWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return &IAMRolePolicy{
		PolicyName:     aws.StringValue(output.PolicyName),
		RoleName:       aws.StringValue(output.RoleName),
		PolicyDocument: aws.StringValue(output.PolicyDocument),
	}, nil
}

func (c *Client) DeleteIAMRolePolicy(ctx context.Context, policyName, roleName string) error {
	input := &iam.DeleteRolePolicyInput{
		PolicyName: aws.String(policyName),
		RoleName:   aws.String(roleName),
	}
	_, err := c.IAM.DeleteRolePolicyWithContext(ctx, input)
	return ignoreNotFound(err)
}

func (c *Client) CreateEC2Tags(ctx context.Context, resources []string, tags Tags) error {
	input := &ec2.CreateTagsInput{
		Resources: aws.StringSlice(resources),
		Tags:      tags.ToEC2Tags(),
	}
	_, err := c.EC2.CreateTagsWithContext(ctx, input)
	return err
}

func (c *Client) DeleteEC2Tags(ctx context.Context, resources []string, tags Tags) error {
	input := &ec2.DeleteTagsInput{
		Resources: aws.StringSlice(resources),
		Tags:      tags.ToEC2Tags(),
	}
	_, err := c.EC2.DeleteTagsWithContext(ctx, input)
	return err
}

func (c *Client) Wait(ctx context.Context, condition wait.ConditionWithContextFunc) error {
	return wait.PollImmediateUntilWithContext(ctx, c.PollInterval, condition)
}

// IsNotFoundError returns true if the given error is a awserr.Error indicating that a AWS resource was not found.
func IsNotFoundError(err error) bool {
	if aerr, ok := err.(awserr.Error); ok && (aerr.Code() == elb.ErrCodeAccessPointNotFoundException ||
		strings.HasSuffix(aerr.Code(), ".NotFound")) {
		return true
	}
	return false
}

func ignoreNotFound(err error) error {
	if err == nil || IsNotFoundError(err) {
		return nil
	}
	return err
}

func chunkSlice(slice []*string, chunkSize int) [][]*string {
	var chunks [][]*string
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		if end > len(slice) {
			end = len(slice)
		}

		chunks = append(chunks, slice[i:end])
	}

	return chunks
}

func fromDhcpOptions(item *ec2.DhcpOptions) *DhcpOptions {
	config := map[string][]string{}
	for _, cfg := range item.DhcpConfigurations {
		var values []string
		for _, av := range cfg.Values {
			values = append(values, *av.Value)
		}
		config[*cfg.Key] = values
	}
	return &DhcpOptions{
		Tags:               FromTags(item.Tags),
		DhcpOptionsId:      aws.StringValue(item.DhcpOptionsId),
		DhcpConfigurations: config,
	}
}

func fromIpPermission(ipPerm *ec2.IpPermission, ruleType SecurityGroupRuleType) *SecurityGroupRule {
	var blocks []string
	for _, block := range ipPerm.IpRanges {
		blocks = append(blocks, *block.CidrIp)
	}
	rule := &SecurityGroupRule{
		Type:       ruleType,
		Protocol:   aws.StringValue(ipPerm.IpProtocol),
		CidrBlocks: blocks,
	}
	if ipPerm.FromPort != nil {
		rule.FromPort = int(*ipPerm.FromPort)
	}
	if ipPerm.ToPort != nil {
		rule.ToPort = int(*ipPerm.ToPort)
	}
	return rule
}

func fromSubnet(item *ec2.Subnet) *Subnet {
	return &Subnet{
		Tags:             FromTags(item.Tags),
		SubnetId:         aws.StringValue(item.SubnetId),
		VpcId:            aws.StringValue(item.VpcId),
		AvailabilityZone: aws.StringValue(item.AvailabilityZone),
		CidrBlock:        aws.StringValue(item.CidrBlock),
	}
}

func fromAddress(item *ec2.Address) *ElasticIP {
	return &ElasticIP{
		Tags:         FromTags(item.Tags),
		Vpc:          aws.StringValue(item.Domain) == ec2.DomainTypeVpc,
		AllocationId: aws.StringValue(item.AllocationId),
		PublicIp:     aws.StringValue(item.PublicIp),
	}
}

func fromNatGateway(item *ec2.NatGateway) *NATGateway {
	var allocationId, publicIP string
	for _, address := range item.NatGatewayAddresses {
		allocationId = aws.StringValue(address.AllocationId)
		publicIP = aws.StringValue(address.PublicIp)
		break
	}
	return &NATGateway{
		Tags:            FromTags(item.Tags),
		NATGatewayId:    aws.StringValue(item.NatGatewayId),
		EIPAllocationId: allocationId,
		PublicIP:        publicIP,
		SubnetId:        aws.StringValue(item.SubnetId),
	}
}

func fromKeyPairInfo(item *ec2.KeyPairInfo) *KeyPairInfo {
	return &KeyPairInfo{
		Tags:           FromTags(item.Tags),
		KeyName:        aws.StringValue(item.KeyName),
		KeyFingerprint: aws.StringValue(item.KeyFingerprint),
	}
}

func fromIAMRole(item *iam.Role) *IAMRole {
	return &IAMRole{
		Tags:                     FromIAMTags(item.Tags),
		RoleId:                   aws.StringValue(item.RoleId),
		RoleName:                 aws.StringValue(item.RoleName),
		Path:                     aws.StringValue(item.Path),
		AssumeRolePolicyDocument: aws.StringValue(item.AssumeRolePolicyDocument),
	}
}

func fromIAMInstanceProfile(item *iam.InstanceProfile) *IAMInstanceProfile {
	var roleName string
	for _, role := range item.Roles {
		roleName = aws.StringValue(role.RoleName)
		break
	}
	return &IAMInstanceProfile{
		Tags:                FromIAMTags(item.Tags),
		InstanceProfileId:   aws.StringValue(item.InstanceProfileId),
		InstanceProfileName: aws.StringValue(item.InstanceProfileName),
		Path:                aws.StringValue(item.Path),
		RoleName:            roleName,
	}
}

func single[T any](list []*T, err error) (*T, error) {
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}
