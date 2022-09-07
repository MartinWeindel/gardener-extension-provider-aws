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
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
)

type Tags map[string]string

func FromTags(ec2Tags []*ec2.Tag) Tags {
	tags := Tags{}
	for _, et := range ec2Tags {
		tags[aws.StringValue(et.Key)] = aws.StringValue(et.Value)
	}
	return tags
}

func (tags Tags) ToTagSpecification(resourceType string) *ec2.TagSpecification {
	tagspec := &ec2.TagSpecification{
		ResourceType: aws.String(resourceType),
	}
	for k, v := range tags {
		tagspec.Tags = append(tagspec.Tags, &ec2.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return tagspec
}

func (tags Tags) ToTagSpecifications(resourceType string) []*ec2.TagSpecification {
	if tags == nil {
		return nil
	}
	return []*ec2.TagSpecification{tags.ToTagSpecification(resourceType)}
}

func (tags Tags) ToEC2Tags() []*ec2.Tag {
	var copy []*ec2.Tag
	for k, v := range tags {
		copy = append(copy, &ec2.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return copy
}

func (tags Tags) ToFilters() []*ec2.Filter {
	if tags == nil {
		return nil
	}
	var filters []*ec2.Filter
	for k, v := range tags {
		filters = append(filters, &ec2.Filter{Name: aws.String(fmt.Sprintf("tag:%s", k)), Values: []*string{aws.String(v)}})
	}
	return filters
}

func (tags Tags) Clone() Tags {
	copy := Tags{}
	for k, v := range tags {
		copy[k] = v
	}
	return copy
}
