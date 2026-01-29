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

package interflow

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/framework"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/types"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
)

// WDRRPolicyName is the name of the Weighted Deficit Round Robin policy implementation.
const WDRRPolicyName = "WDRR"

func init() {
	fwkplugin.Register(WDRRPolicyName,
		func(name string, _ json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
			return NewWDRR(name, DefaultWDRRConfig()), nil
		})
}

// weightedDeficitRoundRobin implements the `framework.FairnessPolicy` interface using the
// Weighted Deficit Round Robin (WDRR) algorithm.
//
// WDRR provides anti-starvation guarantees while supporting weighted priorities.
//
// Algorithm overview:
//  1. Each flow maintains a "deficit counter" that accumulates credits each round
//  2. Each round, flows receive a "quantum" of work they're allowed to do (quantum = BaseQuantum * weight)
//  3. Higher-weight flows receive larger quantums
//  4. Anti-starvation: every flow gets at least MinQuantum, ensuring bounded wait time
//
// Thread-safety: All state mutations are protected by a single mutex.
type weightedDeficitRoundRobin struct {
	name   string      // Plugin instance name
	config *WDRRConfig // Immutable configuration (can be read without lock after construction)
}

// wdrrState holds the mutable state for a specific priority band.
// It is initialized via NewState and stored on the PriorityBandAccessor.
type wdrrState struct {
	mu           sync.Mutex       // Protects all fields below
	deficits     map[string]int64 // FlowID -> accumulated deficit counter
	lastSelected *types.FlowKey   // Last selected flow for round-robin iteration
}

// NewWDRR creates a new WDRR policy with the given configuration.
//
// The configuration is validated during construction. If validation fails, this function panics
// (following the pattern of init-time registration where errors must be surfaced immediately).
//
// Note: The config is treated as immutable after construction for thread-safety.
func NewWDRR(name string, config *WDRRConfig) framework.FairnessPolicy {
	if name == "" {
		name = WDRRPolicyName
	}
	if config == nil {
		config = DefaultWDRRConfig()
	}
	if err := config.Validate(); err != nil {
		panic(err) // Configuration errors are programming errors, fail fast
	}

	return &weightedDeficitRoundRobin{
		name:   name,
		config: config,
	}
}

// TypedName returns the type and name tuple of this plugin instance.
func (p *weightedDeficitRoundRobin) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{
		Type: WDRRPolicyName,
		Name: p.name,
	}
}

// NewState initializes the policy state for a specific priority band.
func (p *weightedDeficitRoundRobin) NewState(_ context.Context) any {
	return &wdrrState{
		deficits: make(map[string]int64),
	}
}

// Pick implements the WDRR selection algorithm.
// It retrieves the band-specific state, locks it, and selects a flow using weighted deficit round-robin.
//
// Algorithm:
//  1. Sort flow keys for deterministic iteration order
//  2. If the last selected flow still has positive deficit and is non-empty:
//     a. Continue serving from it (deduct 1 from deficit and return)
//  3. Otherwise, advance to next flow in round-robin order:
//     a. Find starting position (after last selected flow)
//     b. Find the next non-empty queue
//     c. Refill its deficit with quantum (quantum = BaseQuantum * weight)
//     d. Select it (deduct 1 from deficit and return)
//  4. If no non-empty queue found, clean up deficits for inactive flows
//
// Anti-starvation mechanism:
//   - Round-robin iteration ensures all flows are visited in bounded time
//   - Each flow gets quantum credits when selected (quantum = BaseQuantum * weight)
//   - Higher-weight flows serve more items per round (larger quantum)
//   - Even weight=1 flows get MinQuantum guarantee
//
// Weighted fairness:
//   - Flow with weight W serves W*BaseQuantum items per round
//   - Long-term ratio matches weight ratio (e.g., 10:5:1 weights → 62.5%:31.25%:6.25% distribution)
//
// Returns:
//   - FlowQueueAccessor: The selected queue, or nil if all queues are empty
//   - error: Error if state type is invalid, nil otherwise
func (p *weightedDeficitRoundRobin) Pick(
	_ context.Context,
	band framework.PriorityBandAccessor,
) (framework.FlowQueueAccessor, error) {
	// Handle nil band
	if band == nil {
		return nil, nil
	}

	// Retrieve and validate state
	v := band.PolicyState()
	state, ok := v.(*wdrrState)
	if !ok {
		return nil, fmt.Errorf("invalid state type for WDRR policy: expected *wdrrState, got %T", v)
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	keys := band.FlowKeys()
	// No keys: early return
	if len(keys) == 0 {
		state.lastSelected = nil
		return nil, nil
	}

	// Sort flow keys for deterministic ordering
	slices.SortFunc(keys, func(a, b types.FlowKey) int { return a.Compare(b) })

	// First, check if lastSelected flow still has deficit and is non-empty
	// Stay on the current flow until its deficit is depleted (classic DRR behavior)
	if state.lastSelected != nil {
		queue := band.Queue(state.lastSelected.ID)
		if queue != nil && queue.Len() > 0 {
			deficit := state.deficits[state.lastSelected.ID]
			if deficit > 0 {
				// Continue serving from this flow
				state.deficits[state.lastSelected.ID]--
				return queue, nil
			}
		}
	}

	// Current flow depleted or empty, advance to next flow in round-robin order
	// Find starting index (after lastSelected)
	startIdx := 0
	if state.lastSelected != nil {
		for i, k := range keys {
			if k.ID == state.lastSelected.ID {
				startIdx = (i + 1) % len(keys)
				break
			}
		}
	}

	// Iterate through flows in round-robin order to find the next non-empty flow
	for i := 0; i < len(keys); i++ {
		idx := (startIdx + i) % len(keys)
		key := keys[idx]
		queue := band.Queue(key.ID)
		if queue == nil || queue.Len() == 0 {
			continue
		}

		// Refill deficit with quantum for this flow
		quantum := p.calculateQuantum(queue)
		state.deficits[key.ID] = quantum

		// Select this flow and deduct one unit
		state.deficits[key.ID]--
		state.lastSelected = &key
		return queue, nil
	}

	// No queue selected (all queues empty)
	// Clean up deficits for flows no longer in the band
	p.cleanupDeficits(state, keys)

	return nil, nil
}

// calculateQuantum computes the quantum for a given queue based on its weight.
//
// Formula:
//
//	weight = FlowWeights[flowID] or DefaultWeight if not specified
//	quantum = BaseQuantum * weight
//	return max(quantum, MinQuantum)
//
// This ensures:
//  1. Higher-weight flows get more quantum (weighted fairness)
//  2. Every flow gets at least MinQuantum (anti-starvation)
func (p *weightedDeficitRoundRobin) calculateQuantum(queue framework.FlowQueueAccessor) int64 {
	flowID := queue.FlowKey().ID
	weight := p.weight(flowID)
	quantum := p.config.BaseQuantum * int64(weight)

	// Ensure minimum quantum (anti-starvation guarantee)
	return max(quantum, p.config.MinQuantum)
}

// weight returns the weight for a given flow ID.
// Returns the configured weight if present in FlowWeights, otherwise returns DefaultWeight.
func (p *weightedDeficitRoundRobin) weight(flowID string) int {
	if weight, ok := p.config.FlowWeights[flowID]; ok {
		return weight
	}
	return p.config.DefaultWeight
}

// cleanupDeficits removes deficit entries for flows that are no longer present in the band.
// This prevents unbounded memory growth when flows are dynamically added/removed.
//
// Must be called with state.mu held.
func (p *weightedDeficitRoundRobin) cleanupDeficits(state *wdrrState, activeKeys []types.FlowKey) {
	// Build set of active flow IDs
	activeFlows := make(map[string]struct{}, len(activeKeys))
	for _, key := range activeKeys {
		activeFlows[key.ID] = struct{}{}
	}

	// Remove deficits for inactive flows
	for flowID := range state.deficits {
		if _, exists := activeFlows[flowID]; !exists {
			delete(state.deficits, flowID)
		}
	}
}
