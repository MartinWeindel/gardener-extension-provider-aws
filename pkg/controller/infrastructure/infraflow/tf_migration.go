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
	"github.com/gardener/gardener-extension-provider-aws/pkg/aws"
	"github.com/gardener/gardener/extensions/pkg/terraformer"
	"k8s.io/apimachinery/pkg/runtime"
)

func MigrateTerraformStateToFlowState(state *runtime.RawExtension) (*awsapi.FlowState, error) {
	var (
		tfRawState *terraformer.RawState
		tfState    *TerraformState
		err        error
	)

	flowState := &awsapi.FlowState{
		Version: FlowStateVersion1,
		Data:    map[string]string{},
	}

	if state == nil {
		return flowState, nil
	}

	if tfRawState, err = GetTerraformerRawState(state); err != nil {
		return nil, err
	}
	data, err := tfRawState.Marshal()
	if err != nil {
		return nil, fmt.Errorf("could not marshal terraform raw state: %+v", err)
	}
	if tfState, err = UnmarshalTerraformState(data); err != nil {
		return nil, fmt.Errorf("could not decode terraform state: %+v", err)
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
		tfState.GetManagedResourceInstanceID("aws_route", "public"))
	setFlowStateData(flowState, IdentifierNodesSecurityGroup,
		tfState.GetManagedResourceInstanceID("aws_security_group", "nodes"))

	if instances := tfState.GetManagedResourceInstances("aws_vpc_endpoint"); len(instances) > 0 {
		for name, id := range instances {
			key := ChildIdVPCEndpoints + "/" + strings.TrimPrefix(name, "vpc_gwep_")
			setFlowStateData(flowState, key, &id)
		}
	}

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
