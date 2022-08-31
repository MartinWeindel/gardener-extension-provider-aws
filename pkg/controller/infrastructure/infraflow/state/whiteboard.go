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
	"sync"
	"sync/atomic"

	"k8s.io/utils/pointer"
)

const (
	deleted = "<deleted>"
)

type Whiteboard interface {
	IsEmpty() bool

	GetChild(key string) Whiteboard
	HasChild(key string) bool
	GetChildrenKeys() []string

	GetID(key string) *string
	HasID(key string) bool
	SetID(key, id string)
	SetIDPtr(key string, id *string)
	IsIDAlreadyDeleted(key string) bool
	SetIDAsDeleted(key string)
	GetIDKeys() []string
	GetIDMap() map[string]string

	IsTaskMarkedCompleted(key string) bool
	MarkTaskCompleted(key string, completed bool)
	GetCompletedTaskMarkersKeys() []string
	GetCompletedTaskMarkers() map[string]bool

	// Generation returns modification generation
	Generation() int64
}

type whiteboard struct {
	sync.Mutex

	children   map[string]*whiteboard
	ids        map[string]string
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
		ids:        map[string]string{},
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

	if len(w.ids) != 0 || len(w.completed) != 0 {
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

func (w *whiteboard) GetIDKeys() []string {
	w.Lock()
	defer w.Unlock()

	return sortedKeys(w.ids)
}

func (w *whiteboard) GetIDMap() map[string]string {
	w.Lock()
	defer w.Unlock()

	m := map[string]string{}
	for key, value := range w.ids {
		if value != "" && value != deleted {
			m[key] = value
		}
	}
	return m
}

func (w *whiteboard) GetCompletedTaskMarkersKeys() []string {
	w.Lock()
	defer w.Unlock()

	return sortedKeys(w.completed)
}

func (w *whiteboard) GetCompletedTaskMarkers() map[string]bool {
	w.Lock()
	defer w.Unlock()

	m := map[string]bool{}
	for key, value := range w.completed {
		m[key] = value
	}
	return m
}

func (w *whiteboard) GetID(key string) *string {
	w.Lock()
	defer w.Unlock()
	id := w.ids[key]
	if id == deleted || id == "" {
		return nil
	}
	return &id
}

func (w *whiteboard) SetID(key, id string) {
	w.Lock()
	defer w.Unlock()
	oldId := w.ids[key]
	if id != "" {
		w.ids[key] = id
	} else {
		delete(w.ids, key)
	}
	if oldId != id {
		w.modified()
	}
}

func (w *whiteboard) modified() {
	w.generation.Add(1)
}

func (w *whiteboard) SetIDPtr(key string, id *string) {
	w.SetID(key, pointer.StringDeref(id, ""))
}

func (w *whiteboard) HasID(key string) bool {
	return w.GetID(key) != nil
}

func (w *whiteboard) IsIDAlreadyDeleted(key string) bool {
	w.Lock()
	defer w.Unlock()
	return w.ids[key] == deleted
}

func (w *whiteboard) SetIDAsDeleted(key string) {
	w.SetID(key, deleted)
}

func (w *whiteboard) IsTaskMarkedCompleted(key string) bool {
	w.Lock()
	defer w.Unlock()
	return w.completed[key]
}

func (w *whiteboard) MarkTaskCompleted(key string, completed bool) {
	w.Lock()
	defer w.Unlock()
	oldCompleted := w.completed[key]
	w.completed[key] = completed
	if oldCompleted != completed {
		w.modified()
	}
}

func sortedKeys[V any](m map[string]V) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
