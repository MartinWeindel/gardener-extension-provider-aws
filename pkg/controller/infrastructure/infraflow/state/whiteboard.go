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
	"sync"
)

const (
	deleted = "<deleted>"
)

type Whiteboard interface {
	GetID(key string) *string
	HasID(key string) bool
	SetID(key, id string)
	SetIDPtr(key string, id *string)
	IsIDAlreadyDeleted(key string) bool
	SetIDAsDeleted(key string)

	IsTaskMarkedCompleted(key string) bool
	MarkTaskCompleted(key string, completed bool)
}

type whiteboard struct {
	sync.Mutex

	ids       map[string]string
	completed map[string]bool
}

var _ Whiteboard = &whiteboard{}

func NewWhiteboard() Whiteboard {
	return &whiteboard{
		ids:       map[string]string{},
		completed: map[string]bool{},
	}
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
	if id != "" {
		w.ids[key] = id
	} else {
		delete(w.ids, key)
	}
}

func (w *whiteboard) SetIDPtr(key string, id *string) {
	w.Lock()
	defer w.Unlock()
	if id != nil {
		w.ids[key] = *id
	} else {
		delete(w.ids, key)
	}
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
	w.Lock()
	defer w.Unlock()
	w.ids[key] = deleted
}

func (w *whiteboard) IsTaskMarkedCompleted(key string) bool {
	w.Lock()
	defer w.Unlock()
	return w.completed[key]
}

func (w *whiteboard) MarkTaskCompleted(key string, completed bool) {
	w.Lock()
	defer w.Unlock()
	w.completed[key] = completed
}
