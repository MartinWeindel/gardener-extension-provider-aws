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
	"reflect"

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
