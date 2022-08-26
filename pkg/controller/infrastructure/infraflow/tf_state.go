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
	"encoding/json"
	"fmt"
)

const (
	ModeManaged    = "managed"
	AttributeKeyId = "id"
)

type TerraformState struct {
	Version          int                 `json:"version"`
	TerraformVersion string              `json:"terraform_version"`
	Serial           int                 `json:"serial"`
	Lineage          string              `json:"lineage"`
	Outputs          map[string]TFOutput `json:"outputs,omitempty"`
	Resources        []TFResource        `json:"resources,omitempty"`
}

type TFOutput struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

type TFResource struct {
	Mode      string `json:"mode"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Instances []TFInstance
}

type TFInstance struct {
	SchemaVersion       int                    `json:"schema_version"`
	Attributes          map[string]interface{} `json:"attributes,omitempty"`
	SensitiveAttributes []string               `json:"sensitive_attributes,omitempty"`
	Private             string                 `json:"private,omitempty"`
	Dependencies        []string               `json:"dependencies"`
}

func LoadTerraformStateFromConfigMapData(data map[string]string) (*TerraformState, error) {
	content := data["terraform.tfstate"]
	if content == "" {
		return nil, fmt.Errorf("key 'terraform.tfstate' not found")
	}

	return UnmarshalTerraformState([]byte(content))
}

func UnmarshalTerraformState(data []byte) (*TerraformState, error) {
	state := &TerraformState{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, err
	}
	return state, nil
}

func (ts *TerraformState) FindManagedResourceInstances(tfType, name string) []TFInstance {
	for i := range ts.Resources {
		resource := &ts.Resources[i]
		if resource.Mode == ModeManaged && resource.Type == tfType && resource.Name == name {
			return resource.Instances
		}
	}
	return nil
}

func AttributeAsString(attributes map[string]interface{}, key string) (svalue string, found bool) {
	if attributes == nil {
		return
	}
	value, ok := attributes[key]
	if !ok {
		return
	}
	if s, ok := value.(string); ok {
		svalue = s
		found = true
	}
	return
}
