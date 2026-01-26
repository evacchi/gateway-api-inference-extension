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
	"math"
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
// WDRR provides anti-starvation guarantees while supporting weighted priorities and load-aware adaptation.
//
// Algorithm overview:
//  1. Each flow maintains a "deficit counter" that accumulates credits each round
//  2. Each round, flows receive a "quantum" of work they're allowed to do
//  3. Higher-weight flows receive larger quantums
//  4. Load-aware: quantum increases for high-load queues
//  5. Anti-starvation: every flow gets at least MinQuantum, ensuring bounded wait time
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
// It retrieves the band-specific state, locks it, and selects the flow with the highest deficit.
//
// Algorithm:
//  1. Sort flow keys for deterministic iteration order
//  2. Add quantum to flows with depleted deficit
//  3. Select flow with highest positive deficit
//  4. Deduct 1 unit from selected flow's deficit
//  5. Cap deficits at MaxDeficit to prevent unbounded growth
//  6. Clean up deficits for flows no longer in the band
//
// Returns:
//   - FlowQueueAccessor: The selected queue, or nil if no queue is suitable
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

	// Get and sort flow keys for deterministic ordering
	keys := band.FlowKeys()
	if len(keys) == 0 {
		state.lastSelected = nil
		return nil, nil
	}
	slices.SortFunc(keys, func(a, b types.FlowKey) int { return a.Compare(b) })

	// First pass: Add quantum only to flows that have depleted their deficit (need refill)
	for _, key := range keys {
		queue := band.Queue(key.ID)
		if queue != nil && queue.Len() > 0 {
			// Only add quantum if deficit is non-positive (flow has used up its credits)
			if state.deficits[key.ID] <= 0 {
				quantum := p.calculateQuantum(queue)
				state.deficits[key.ID] += quantum

				// Cap deficit to prevent unbounded growth
				if state.deficits[key.ID] > p.config.MaxDeficit {
					state.deficits[key.ID] = p.config.MaxDeficit
				}
			}
		}
	}

	// Second pass: Select the flow with the highest positive deficit
	// This ensures flows with higher weights (larger quantums) are selected more often
	// Break ties using the flow key ordering for determinism
	var selectedQueue framework.FlowQueueAccessor
	var selectedKey *types.FlowKey
	maxDeficit := int64(0)

	for _, key := range keys {
		queue := band.Queue(key.ID)

		// Skip nil or empty queues
		if queue == nil || queue.Len() == 0 {
			continue
		}

		flowID := key.ID
		currentDeficit := state.deficits[flowID]

		// Select the flow with the highest positive deficit
		if currentDeficit > maxDeficit {
			maxDeficit = currentDeficit
			selectedQueue = queue
			selectedKeyCopy := key // Copy to avoid pointer issues
			selectedKey = &selectedKeyCopy
		}
	}

	// If we found a flow with positive deficit, select it and deduct 1 unit
	if selectedQueue != nil && selectedKey != nil {
		state.deficits[selectedKey.ID]--
		state.lastSelected = selectedKey
		return selectedQueue, nil
	}

	// No queue selected (all queues either empty or insufficient deficit)
	// Clean up deficits for flows no longer in the band
	p.cleanupDeficits(state, keys)

	return nil, nil
}

// calculateQuantum computes the quantum for a given queue based on its weight and current load.
//
// Formula:
//
//	weight = FlowWeights[flowID] or DefaultWeight if not specified
//	baseQuantum = BaseQuantum * weight
//	currentLoad = queue.Len() or queue.ByteSize() (depending on UseByteSize)
//
//	if currentLoad > LoadThreshold:
//	  loadFactor = currentLoad / LoadThreshold
//	  boost = (loadFactor - 1.0) * LoadBoostFactor
//	  adjustedQuantum = baseQuantum * (1.0 + boost)
//	else:
//	  adjustedQuantum = baseQuantum
//
//	return max(adjustedQuantum, MinQuantum)
//
// This ensures:
//  1. Higher-weight flows get more quantum (weighted fairness)
//  2. High-load queues get boosted quantum (load adaptation)
//  3. Every flow gets at least MinQuantum (anti-starvation)
func (p *weightedDeficitRoundRobin) calculateQuantum(queue framework.FlowQueueAccessor) int64 {
	flowID := queue.FlowKey().ID

	// Get weight for this flow
	weight := p.getWeight(flowID)

	// Calculate base quantum
	baseQuantum := p.config.BaseQuantum * int64(weight)

	// Get current load
	var currentLoad int
	if p.config.UseByteSize {
		currentLoad = int(queue.ByteSize())
	} else {
		currentLoad = queue.Len()
	}

	// Apply load-based boost if above threshold
	adjustedQuantum := baseQuantum
	if currentLoad > p.config.LoadThreshold && p.config.LoadBoostFactor > 0 {
		loadFactor := float64(currentLoad) / float64(p.config.LoadThreshold)
		boost := (loadFactor - 1.0) * p.config.LoadBoostFactor
		adjustedQuantum = int64(math.Round(float64(baseQuantum) * (1.0 + boost)))
	}

	// Ensure minimum quantum (anti-starvation guarantee)
	if adjustedQuantum < p.config.MinQuantum {
		adjustedQuantum = p.config.MinQuantum
	}

	return adjustedQuantum
}

// getWeight returns the weight for a given flow ID.
// Returns the configured weight if present in FlowWeights, otherwise returns DefaultWeight.
func (p *weightedDeficitRoundRobin) getWeight(flowID string) int {
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
