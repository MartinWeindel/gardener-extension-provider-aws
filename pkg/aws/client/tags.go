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
	"github.com/aws/aws-sdk-go/service/iam"
)

type Tag struct {
	Key   string
	Value string
}

type Tags []Tag

func FromTags(ec2Tags []*ec2.Tag) Tags {
	var tags Tags
	for _, et := range ec2Tags {
		tags = append(tags, Tag{Key: aws.StringValue(et.Key), Value: aws.StringValue(et.Value)})
	}
	return tags
}

func FromIAMTags(iamTags []*iam.Tag) Tags {
	var tags Tags
	for _, et := range iamTags {
		tags = append(tags, Tag{Key: aws.StringValue(et.Key), Value: aws.StringValue(et.Value)})
	}
	return tags
}

func (tags Tags) ToTagSpecification() *ec2.TagSpecification {
	tagspec := &ec2.TagSpecification{}
	for _, t := range tags {
		tagspec.Tags = append(tagspec.Tags, &ec2.Tag{Key: aws.String(t.Key), Value: aws.String(t.Value)})
	}
	return tagspec
}

func (tags Tags) ToTagSpecifications() []*ec2.TagSpecification {
	if tags == nil {
		return nil
	}
	return []*ec2.TagSpecification{tags.ToTagSpecification()}
}

func (tags Tags) ToIAMTags() []*iam.Tag {
	var copy []*iam.Tag
	for _, tag := range tags {
		copy = append(copy, &iam.Tag{
			Key:   aws.String(tag.Key),
			Value: aws.String(tag.Value),
		})
	}
	return copy
}

func (tags Tags) ToFilters() []*ec2.Filter {
	if tags == nil {
		return nil
	}
	var filters []*ec2.Filter
	for _, t := range tags {
		filters = append(filters, &ec2.Filter{Name: aws.String(fmt.Sprintf("tag:%s", t.Key)), Values: []*string{aws.String(t.Value)}})
	}
	return filters
}

func (tags Tags) Clone() Tags {
	var copy Tags
	for _, tag := range tags {
		copy = append(copy, tag)
	}
	return copy
}
