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

package state

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/utils/pointer"
)

const (
	deleted   = "<deleted>"
	separator = "/"
)

type Whiteboard interface {
	IsEmpty() bool

	GetChild(key string) Whiteboard
	HasChild(key string) bool
	GetChildrenKeys() []string

	Get(key string) *string
	Has(key string) bool
	Set(key, id string)
	SetPtr(key string, id *string)
	IsAlreadyDeleted(key string) bool
	SetAsDeleted(key string)
	Keys() []string
	AsMap() map[string]string

	ImportFromFlatMap(data map[string]string)
	ExportAsFlatMap() map[string]string

	// Generation returns modification generation
	Generation() int64
}

type whiteboard struct {
	sync.Mutex

	children   map[string]*whiteboard
	data       map[string]string
	completed  map[string]bool
	generation *atomic.Int64
}

var _ Whiteboard = &whiteboard{}

func NewWhiteboard() Whiteboard {
	return newWhiteboard(&atomic.Int64{})
}

func newWhiteboard(generation *atomic.Int64) *whiteboard {
	return &whiteboard{
		children:   map[string]*whiteboard{},
		data:       map[string]string{},
		completed:  map[string]bool{},
		generation: generation,
	}
}

func (w *whiteboard) Generation() int64 {
	return w.generation.Load()
}

func (w *whiteboard) IsEmpty() bool {
	w.Lock()
	defer w.Unlock()

	if len(w.data) != 0 || len(w.completed) != 0 {
		return false
	}
	for _, child := range w.children {
		if !child.IsEmpty() {
			return false
		}
	}
	return true
}

func (w *whiteboard) GetChild(key string) Whiteboard {
	return w.getChild(key)
}

func (w *whiteboard) getChild(key string) *whiteboard {
	w.Lock()
	defer w.Unlock()

	child := w.children[key]
	if child == nil {
		child = newWhiteboard(w.generation)
		w.children[key] = child
	}
	return child
}

func (w *whiteboard) HasChild(key string) bool {
	w.Lock()
	defer w.Unlock()

	child := w.children[key]
	return child != nil && !child.IsEmpty()
}

func (w *whiteboard) GetChildrenKeys() []string {
	w.Lock()
	defer w.Unlock()

	return sortedKeys(w.children)
}

func (w *whiteboard) Keys() []string {
	w.Lock()
	defer w.Unlock()

	return sortedKeys(w.data)
}

func (w *whiteboard) AsMap() map[string]string {
	w.Lock()
	defer w.Unlock()

	m := map[string]string{}
	for key, value := range w.data {
		if value != "" && value != deleted {
			m[key] = value
		}
	}
	return m
}

func (w *whiteboard) Get(key string) *string {
	w.Lock()
	defer w.Unlock()
	id := w.data[key]
	if id == deleted || id == "" {
		return nil
	}
	return &id
}

func (w *whiteboard) Set(key, id string) {
	w.Lock()
	defer w.Unlock()
	oldId := w.data[key]
	if id != "" {
		w.data[key] = id
	} else {
		delete(w.data, key)
	}
	if oldId != id {
		w.modified()
	}
}

func (w *whiteboard) SetPtr(key string, id *string) {
	w.Set(key, pointer.StringDeref(id, ""))
}

func (w *whiteboard) Has(key string) bool {
	return w.Get(key) != nil
}

func (w *whiteboard) IsAlreadyDeleted(key string) bool {
	w.Lock()
	defer w.Unlock()
	return w.data[key] == deleted
}

func (w *whiteboard) SetAsDeleted(key string) {
	w.Set(key, deleted)
}

func (w *whiteboard) ImportFromFlatMap(data map[string]string) {
	for key, value := range data {
		parts := strings.Split(key, separator)
		level := w
		for i := 0; i < len(parts)-1; i++ {
			level = level.getChild(parts[i])
		}
		level.Set(parts[len(parts)-1], value)
	}
}

func (w *whiteboard) ExportAsFlatMap() map[string]string {
	data := map[string]string{}
	w.copyMap(data, "")
	fillDataFromChildren(data, "", w)
	return data
}

func (w *whiteboard) copyMap(data map[string]string, prefix string) {
	w.Lock()
	defer w.Unlock()
	for k, v := range w.data {
		data[prefix+k] = v
	}
}

func fillDataFromChildren(data map[string]string, parentPrefix string, parent *whiteboard) {
	for _, childKey := range parent.GetChildrenKeys() {
		child := parent.getChild(childKey)
		childPrefix := parentPrefix + childKey + separator
		child.copyMap(data, childPrefix)
		fillDataFromChildren(data, childPrefix, child)
	}
}

func (w *whiteboard) modified() {
	w.generation.Add(1)
}

func sortedKeys[V any](m map[string]V) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
