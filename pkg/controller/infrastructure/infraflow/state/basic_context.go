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
	"context"
	"fmt"
	"time"

	"github.com/gardener/gardener/pkg/utils/flow"
	"github.com/go-logr/logr"
	"k8s.io/utils/pointer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type FlowStatePersistor func(ctx context.Context, flatMap FlatMap) error

type TaskOption struct {
	Dependencies []flow.TaskIDer
	Timeout      time.Duration
	DoIf         *bool
}

func Dependencies(dependencies ...flow.TaskIDer) TaskOption {
	return TaskOption{Dependencies: dependencies}
}

func Timeout(timeout time.Duration) TaskOption {
	return TaskOption{Timeout: timeout}
}

func DoIf(condition bool) TaskOption {
	return TaskOption{DoIf: pointer.Bool(condition)}
}

type BasicFlowContext struct {
	Log   logr.Logger
	State Whiteboard

	flowStatePersistor      FlowStatePersistor
	lastPersistedGeneration int64
	lastPersistedAt         time.Time
}

func NewBasicFlowContext(logger logr.Logger, oldState FlatMap, persistor FlowStatePersistor) *BasicFlowContext {
	flowContext := &BasicFlowContext{
		Log:                logger,
		State:              NewWhiteboard(),
		flowStatePersistor: persistor,
	}
	if oldState != nil {
		flowContext.State.ImportFromFlatMap(oldState)
	}
	return flowContext
}

func (c *BasicFlowContext) PersistState(ctx context.Context, force bool) error {
	if !force && c.lastPersistedAt.Add(10*time.Second).After(time.Now()) {
		return nil
	}
	currentGeneration := c.State.Generation()
	if c.lastPersistedGeneration == currentGeneration {
		return nil
	}
	if c.flowStatePersistor != nil {
		newState := c.State.ExportAsFlatMap()
		if err := c.flowStatePersistor(ctx, newState); err != nil {
			return err
		}
	}
	c.lastPersistedGeneration = currentGeneration
	c.lastPersistedAt = time.Now()
	return nil
}

func (c *BasicFlowContext) LogFromContext(ctx context.Context) logr.Logger {
	if log, err := logr.FromContext(ctx); err != nil {
		return c.Log
	} else {
		return log
	}
}

func (c *BasicFlowContext) AddTask(g *flow.Graph, name string, fn flow.TaskFn, options ...TaskOption) flow.TaskIDer {
	allOptions := TaskOption{}
	for _, opt := range options {
		if len(opt.Dependencies) > 0 {
			allOptions.Dependencies = append(allOptions.Dependencies, opt.Dependencies...)
		}
		if opt.Timeout > 0 {
			allOptions.Timeout = opt.Timeout
		}
		if opt.DoIf != nil {
			condition := true
			if allOptions.DoIf != nil {
				condition = *allOptions.DoIf
			}
			condition = condition && *opt.DoIf
			allOptions.DoIf = pointer.Bool(condition)
		}
	}

	tunedFn := fn
	if allOptions.DoIf != nil {
		tunedFn = tunedFn.DoIf(*allOptions.DoIf)
		if !*allOptions.DoIf {
			name = "[Skipped] " + name
		}
	}
	if allOptions.Timeout > 0 {
		tunedFn = tunedFn.Timeout(allOptions.Timeout)
	}
	task := flow.Task{
		Name: name,
		Fn:   c.wrapTaskFn(g.Name(), name, tunedFn),
	}

	if len(allOptions.Dependencies) > 0 {
		task.Dependencies = flow.NewTaskIDs(allOptions.Dependencies...)
	}

	return g.Add(task)
}

func (c *BasicFlowContext) wrapTaskFn(flowName, taskName string, fn flow.TaskFn) flow.TaskFn {
	return func(ctx context.Context) error {
		taskCtx := logf.IntoContext(ctx, c.Log.WithValues("flow", flowName, "task", taskName))
		err := fn(taskCtx)
		if err != nil {
			// don't wrap error with '%w', as otherwise the error context get lost
			err = fmt.Errorf("failed to %s: %s", taskName, err)
		}
		if perr := c.PersistState(taskCtx, false); perr != nil {
			if err != nil {
				c.Log.Error(perr, "persisting state failed")
			} else {
				err = perr
			}
		}
		return err
	}
}
