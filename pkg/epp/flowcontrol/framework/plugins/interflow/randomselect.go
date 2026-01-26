/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package interflow provides `framework.InterFlowDispatchPolicy` implementations that define fairness and dispatch
// ordering between different flows within a priority band.
//
// This file implements a simple RandomSelect policy that randomly selects from non-empty queues. It serves as a
// learning example to demonstrate the InterFlowDispatchPolicy interface and registration patterns.
package interflow

import (
	"context"
	"encoding/json"
	"math/rand/v2"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/framework"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
)

// RandomSelectPolicyName is the name of the RandomSelect policy implementation.
const RandomSelectPolicyName = "RandomSelect"

func init() {
	fwkplugin.Register(RandomSelectPolicyName,
		func(name string, _ json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
			return newRandomSelect(name), nil
		})
}

// randomSelect implements the `framework.InterFlowDispatchPolicy` interface with a random selection strategy.
//
// This is a stateless policy that demonstrates the simplest possible implementation:
// - No internal state management
// - No thread-safety concerns (stateless)
// - Simple iteration and selection pattern
//
// Purpose: This policy serves as a learning example for understanding how to implement the InterFlowDispatchPolicy
// interface. It is NOT recommended for production use as it provides no fairness guarantees.
type randomSelect struct {
	name string
}

// NewState initializes the policy state for a specific priority band.
// RandomSelect is stateless, so it returns nil.
func (p *randomSelect) NewState(_ context.Context) any {
	return nil // Stateless policy - no state needed
}

func newRandomSelect(name string) framework.FairnessPolicy {
	if name == "" {
		name = RandomSelectPolicyName
	}
	return &randomSelect{name: name}
}

// TypedName returns the type and name tuple of this plugin instance.
func (p *randomSelect) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{
		Type: RandomSelectPolicyName,
		Name: p.name,
	}
}

// Pick randomly selects a non-empty queue from the given priority band.
//
// Algorithm:
//  1. Iterate through all queues in the band
//  2. Collect all non-empty queues
//  3. If any non-empty queues exist, randomly select one
//  4. Return nil if all queues are empty
//
// This demonstrates:
// - How to use PriorityBandAccessor.IterateQueues()
// - How to check queue emptiness with queue.Len()
// - How to handle the nil band case
// - How to return nil when no suitable queue is found
//
// Returns:
//   - FlowQueueAccessor: A randomly selected non-empty queue, or nil if all queues are empty or band is nil
//   - error: Always nil for this simple policy (errors are reserved for unrecoverable conditions)
func (p *randomSelect) Pick(
	_ context.Context,
	flowGroup framework.PriorityBandAccessor,
) (framework.FlowQueueAccessor, error) {
	// Handle nil band - return nil queue with no error
	if flowGroup == nil {
		return nil, nil
	}

	// Collect all non-empty queues
	var nonEmptyQueues []framework.FlowQueueAccessor

	flowGroup.IterateQueues(func(queue framework.FlowQueueAccessor) (keepIterating bool) {
		// Skip nil queues and empty queues
		if queue != nil && queue.Len() > 0 {
			nonEmptyQueues = append(nonEmptyQueues, queue)
		}
		// Always continue iterating to find all non-empty queues
		return true
	})

	// If no non-empty queues found, return nil
	if len(nonEmptyQueues) == 0 {
		return nil, nil
	}

	// Randomly select one of the non-empty queues
	selectedIndex := rand.IntN(len(nonEmptyQueues))
	return nonEmptyQueues[selectedIndex], nil
}
