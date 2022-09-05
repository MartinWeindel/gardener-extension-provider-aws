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
	"encoding/base64"
	"fmt"
	"strings"

	awsapi "github.com/gardener/gardener-extension-provider-aws/pkg/apis/aws"
	"github.com/gardener/gardener-extension-provider-aws/pkg/aws"
	"github.com/gardener/gardener-extension-provider-aws/pkg/controller/infrastructure/infraflow/state"
	"github.com/gardener/gardener/extensions/pkg/terraformer"
	"k8s.io/apimachinery/pkg/runtime"
)

func MigrateTerraformStateToFlowState(rawExtension *runtime.RawExtension, zones []awsapi.Zone) (*awsapi.FlowState, error) {
	var (
		tfRawState *terraformer.RawState
		tfState    *TerraformState
		err        error
	)

	flowState := &awsapi.FlowState{
		Version: FlowStateVersion1,
		Data:    map[string]string{},
	}

	if rawExtension == nil {
		return flowState, nil
	}

	if tfRawState, err = GetTerraformerRawState(rawExtension); err != nil {
		return nil, err
	}
	var data []byte
	switch tfRawState.Encoding {
	case "base64":
		data, err = base64.StdEncoding.DecodeString(tfRawState.Data)
		if err != nil {
			return nil, fmt.Errorf("could not decode terraform raw state data: %w", err)
		}
	case "none":
		data = []byte(tfRawState.Data)
	default:
		return nil, fmt.Errorf("unknown encoding of Terraformer raw state: %s", tfRawState.Encoding)
	}
	if tfState, err = UnmarshalTerraformState(data); err != nil {
		return nil, fmt.Errorf("could not decode terraform state: %w", err)
	}

	if tfState.Outputs == nil {
		return flowState, nil
	}

	value := tfState.Outputs[aws.VPCIDKey].Value
	if value != "" {
		setFlowStateData(flowState, IdentifierVPC, &value)
	}
	setFlowStateData(flowState, IdentifierDHCPOptions,
		tfState.GetManagedResourceInstanceID("aws_vpc_dhcp_options", "vpc_dhcp_options"))
	setFlowStateData(flowState, IdentifierDefaultSecurityGroup,
		tfState.GetManagedResourceInstanceID("aws_default_security_group", "default"))
	setFlowStateData(flowState, IdentifierInternetGateway,
		tfState.GetManagedResourceInstanceID("aws_internet_gateway", "igw"))
	setFlowStateData(flowState, IdentifierMainRouteTable,
		tfState.GetManagedResourceInstanceID("aws_route_table", "public"))
	setFlowStateData(flowState, IdentifierNodesSecurityGroup,
		tfState.GetManagedResourceInstanceID("aws_security_group", "nodes"))

	if instances := tfState.GetManagedResourceInstances("aws_vpc_endpoint"); len(instances) > 0 {
		for name, id := range instances {
			key := ChildIdVPCEndpoints + state.Separator + strings.TrimPrefix(name, "vpc_gwep_")
			setFlowStateData(flowState, key, &id)
		}
	}

	tfNamePrefixes := []string{"nodes_", "private_utility_", "public_utility"}
	flowNames := []string{IdentifierZoneSubnetWorkers, IdentifierZoneSubnetPrivate, IdentifierZoneSubnetPublic}
	for i, zone := range zones {
		keyPrefix := ChildIdZones + state.Separator + zone.Name + state.Separator
		suffix := fmt.Sprintf("z%d", i)
		setFlowStateData(flowState, keyPrefix+IdentifierZoneSuffix, &suffix)

		for j := 0; j < len(tfNamePrefixes); j++ {
			setFlowStateData(flowState, keyPrefix+flowNames[j],
				tfState.GetManagedResourceInstanceID("aws_subnet", tfNamePrefixes[j]+suffix))
		}
		setFlowStateData(flowState, keyPrefix+IdentifierZoneNATGWElasticIP,
			tfState.GetManagedResourceInstanceID("aws_eip", "eip_natgw_"+suffix))
		setFlowStateData(flowState, keyPrefix+IdentifierZoneNATGateway,
			tfState.GetManagedResourceInstanceID("aws_nat_gateway", "natgw_"+suffix))
		setFlowStateData(flowState, keyPrefix+IdentifierZoneNATGateway,
			tfState.GetManagedResourceInstanceID("aws_route_table", "private_utility_"+suffix))

		setFlowStateData(flowState, keyPrefix+IdentifierZoneSubnetPublicRouteTableAssoc,
			tfState.GetManagedResourceInstanceID("aws_route_table_association", "routetable_main_association_public_utility_"+suffix))
		setFlowStateData(flowState, keyPrefix+IdentifierZoneSubnetPrivateRouteTableAssoc,
			tfState.GetManagedResourceInstanceID("aws_route_table_association", "routetable_private_utility_"+suffix+"_association_private_utility_"+suffix))
		setFlowStateData(flowState, keyPrefix+IdentifierZoneSubnetWorkersRouteTableAssoc,
			tfState.GetManagedResourceInstanceID("aws_route_table_association", "routetable_private_utility_"+suffix+"_association_nodes_"+suffix))
	}

	setFlowStateData(flowState, NameIAMRole,
		tfState.GetManagedResourceInstanceName("aws_iam_role", "nodes"))
	setFlowStateData(flowState, NameIAMInstanceProfile,
		tfState.GetManagedResourceInstanceName("aws_iam_instance_profile", "nodes"))
	setFlowStateData(flowState, NameIAMRolePolicy,
		tfState.GetManagedResourceInstanceName("aws_iam_role_policy", "nodes"))

	setFlowStateData(flowState, NameKeyPair,
		tfState.GetManagedResourceInstanceAttribute("aws_key_pair", "nodes", "key_pair_id"))

	flowState.Data[MarkerMigratedFromTerraform] = "true"

	return flowState, nil
}

func setFlowStateData(state *awsapi.FlowState, key string, id *string) {
	if id == nil {
		delete(state.Data, key)
	} else {
		state.Data[key] = *id
	}
}

func GetTerraformerRawState(state *runtime.RawExtension) (*terraformer.RawState, error) {
	if state == nil {
		return nil, nil
	}
	tfRawState, err := terraformer.UnmarshalRawState(state)
	if err != nil {
		return nil, fmt.Errorf("could not decode terraform raw state: %+v", err)
	}
	return tfRawState, nil
}
