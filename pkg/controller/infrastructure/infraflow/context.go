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
	"fmt"
	"strings"

	awsapi "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws"
	awsclient "github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	"github.com/gardener/gardener-extension-provider-aws/pkg/controller/infrastructure/infraflow/shared"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	TagKeyName            = "Name"
	TagKeyClusterTemplate = "kubernetes.io/cluster/%s"
	TagKeyRolePublicELB   = "kubernetes.io/role/elb"
	TagKeyRolePrivateELB  = "kubernetes.io/role/internal-elb"
	TagValueCluster       = "1"
	TagValueUse           = "use"

	IdentifierVPC                              = "VPC"
	IdentifierDHCPOptions                      = "DHCPOptions"
	IdentifierDefaultSecurityGroup             = "DefaultSecurityGroup"
	IdentifierInternetGateway                  = "InternetGateway"
	IdentifierMainRouteTable                   = "MainRouteTable"
	IdentifierNodesSecurityGroup               = "NodesSecurityGroup"
	IdentifierZoneSubnetWorkers                = "SubnetWorkers"
	IdentifierZoneSubnetPublic                 = "SubnetPublicUtility"
	IdentifierZoneSubnetPrivate                = "SubnetPrivateUtility"
	IdentifierZoneSuffix                       = "Suffix"
	IdentifierZoneNATGWElasticIP               = "NATGatewayElasticIP"
	IdentifierZoneNATGateway                   = "NATGateway"
	IdentifierZoneRouteTable                   = "ZoneRouteTable"
	IdentifierZoneSubnetPublicRouteTableAssoc  = "SubnetPublicRouteTableAssoc"
	IdentifierZoneSubnetPrivateRouteTableAssoc = "SubnetPrivateRouteTableAssoc"
	IdentifierZoneSubnetWorkersRouteTableAssoc = "SubnetWorkersRouteTableAssoc"
	NameIAMRole                                = "IAMRoleName"
	NameIAMInstanceProfile                     = "IAMInstanceProfileName"
	NameIAMRolePolicy                          = "IAMRolePolicyName"
	NameKeyPair                                = "KeyPair"
	ARNIAMRole                                 = "IAMRoleARN"

	ChildIdVPCEndpoints = "VPCEndpoints"
	ChildIdZones        = "Zones"

	ObjectMainRouteTable = "MainRouteTable"
	ObjectZoneRouteTable = "ZoneRouteTable"

	MarkerMigratedFromTerraform                   = "MigratedFromTerraform"
	MarkerTerraformCleanedUp                      = "TerraformCleanedUp"
	MarkerLoadBalancersAndSecurityGroupsDestroyed = "LoadBalancersAndSecurityGroupsDestroyed"
)

type FlowContext struct {
	shared.BasicFlowContext
	state      shared.Whiteboard
	infra      *extensionsv1alpha1.Infrastructure
	config     *awsapi.InfrastructureConfig
	client     awsclient.Interface
	updater    awsclient.Updater
	commonTags awsclient.Tags
}

func NewFlowContext(log logr.Logger, awsClient awsclient.Interface,
	infra *extensionsv1alpha1.Infrastructure, config *awsapi.InfrastructureConfig,
	oldState shared.FlatMap, persistor shared.FlowStatePersistor) (*FlowContext, error) {

	whiteboard := shared.NewWhiteboard()
	if oldState != nil {
		whiteboard.ImportFromFlatMap(oldState)
	}

	flowContext := &FlowContext{
		BasicFlowContext: *shared.NewBasicFlowContext(log, whiteboard, persistor),
		state:            whiteboard,
		infra:            infra,
		config:           config,
		client:           awsClient,
		updater:          awsclient.NewUpdater(awsClient, config.IgnoreTags),
	}
	flowContext.commonTags = awsclient.Tags{
		flowContext.tagKeyCluster(): TagValueCluster,
		TagKeyName:                  infra.Namespace,
	}
	if config != nil && config.Networks.VPC.ID != nil {
		flowContext.state.SetPtr(IdentifierVPC, config.Networks.VPC.ID)
	}
	return flowContext, nil
}

func (c *FlowContext) GetInfrastructureConfig() *awsapi.InfrastructureConfig {
	return c.config
}

func (c *FlowContext) hasVPC() bool {
	return c.state.Has(IdentifierVPC)
}

func (c *FlowContext) commonTagsWithSuffix(suffix string) awsclient.Tags {
	tags := c.commonTags.Clone()
	tags[TagKeyName] = fmt.Sprintf("%s-%s", c.infra.Namespace, suffix)
	return tags
}

func (c *FlowContext) tagKeyCluster() string {
	return fmt.Sprintf(TagKeyClusterTemplate, c.infra.Namespace)
}

func (c *FlowContext) clusterTags() awsclient.Tags {
	tags := awsclient.Tags{}
	tags[c.tagKeyCluster()] = TagValueCluster
	return tags
}

func (c *FlowContext) vpcEndpointServiceNamePrefix() string {
	return fmt.Sprintf("com.amazonaws.%s.", c.infra.Spec.Region)
}

func (c *FlowContext) extractVpcEndpointName(item *awsclient.VpcEndpoint) string {
	return strings.TrimPrefix(item.ServiceName, c.vpcEndpointServiceNamePrefix())
}

func (c *FlowContext) subnetSuffixHelper(zoneName string) *SubnetSuffixHelper {
	zoneChild := c.getSubnetZoneChild(zoneName)
	if suffix := zoneChild.Get(IdentifierZoneSuffix); suffix != nil {
		return &SubnetSuffixHelper{suffix: *suffix}
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
			return &SubnetSuffixHelper{suffix: suffix}
		}
	}
}

type SubnetSuffixHelper struct {
	suffix string
}

func (h *SubnetSuffixHelper) GetSuffixSubnetWorkers() string {
	return fmt.Sprintf("nodes-%s", h.suffix)
}

func (h *SubnetSuffixHelper) GetSuffixSubnetPublic() string {
	return fmt.Sprintf("public-utility-%s", h.suffix)
}

func (h *SubnetSuffixHelper) GetSuffixSubnetPrivate() string {
	return fmt.Sprintf("private-utility-%s", h.suffix)
}

func (h *SubnetSuffixHelper) GetSuffixElasticIP() string {
	return fmt.Sprintf("eip-natgw-%s", h.suffix)
}

func (h *SubnetSuffixHelper) GetSuffixNATGateway() string {
	return fmt.Sprintf("natgw-%s", h.suffix)
}
