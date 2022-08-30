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
	"context"
	"fmt"
	"reflect"
	"sort"

	awsclient "github.com/gardener/gardener-extension-provider-aws/pkg/aws/client"
	"k8s.io/apimachinery/pkg/util/sets"
)

func PartialEqualExcluding[T any](a, b T, fields ...string) (equal bool, firstDiffField string, err error) {
	av := reflect.ValueOf(a)
	if av.Kind() == reflect.Ptr {
		av = av.Elem()
	}
	bv := reflect.ValueOf(b)
	if bv.Kind() == reflect.Ptr {
		bv = bv.Elem()
	}

	if av.Type() != bv.Type() {
		err = fmt.Errorf("different input types: %s != %s", av.Type(), bv.Type())
		return
	}
	if av.Kind() != reflect.Struct {
		err = fmt.Errorf("not a struct type: %s, kind: %s", av.Type(), av.Kind())
		return
	}

	excluded := sets.NewString(fields...)
	for i := 0; i < av.NumField(); i++ {
		fieldName := av.Type().Field(i).Name
		if excluded.Has(fieldName) {
			continue
		}

		if !reflect.DeepEqual(av.Field(i).Interface(), bv.Field(i).Interface()) {
			firstDiffField = fieldName
			return
		}
	}

	equal = true
	return
}

func unused(_ interface{}) {
}

func sortedKeys[V any](m map[string]V) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func diffByID[T any](desired, current []T, unique func(item T) string) (toBeDeleted, toBeCreated, toBeChecked []T) {
outerDelete:
	for _, c := range current {
		cuniq := unique(c)
		for _, d := range desired {
			if cuniq == unique(d) {
				toBeChecked = append(toBeChecked, c)
				continue outerDelete
			}
		}
		toBeDeleted = append(toBeDeleted, c)
	}
outerCreate:
	for _, d := range desired {
		duniq := unique(d)
		for _, c := range current {
			if duniq == unique(c) {
				continue outerCreate
			}
		}
		toBeCreated = append(toBeCreated, d)
	}
	return
}

func findExisting[T any](ctx context.Context, id *string, tags awsclient.Tags,
	getter func(ctx context.Context, id string) (*T, error),
	finder func(ctx context.Context, tags awsclient.Tags) ([]*T, error),
	selector ...func(item *T) bool) (*T, error) {

	if id != nil {
		found, err := getter(ctx, *id)
		if err != nil {
			return nil, err
		}
		return found, nil
	}

	found, err := finder(ctx, tags)
	if err != nil {
		return nil, err
	}
	if len(found) == 0 {
		return nil, nil
	}
	if selector != nil {
		for _, item := range found {
			if selector[0](item) {
				return item, nil
			}
		}
		return nil, nil
	}
	return found[0], nil
}
