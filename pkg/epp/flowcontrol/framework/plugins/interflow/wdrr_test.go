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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/framework"
	frameworkmocks "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/framework/mocks"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/types"
)

// Test configuration validation
func TestWDRRConfig_Validate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		config      *WDRRConfig
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid default config",
			config:      DefaultWDRRConfig(),
			expectError: false,
		},
		{
			name: "Valid custom config",
			config: &WDRRConfig{
				FlowWeights:     map[string]int{"flow1": 5, "flow2": 1},
				DefaultWeight:   2,
				BaseQuantum:     20,
				MinQuantum:      2,
				MaxDeficit:      200,
			},
			expectError: false,
		},
		{
			name: "Invalid DefaultWeight (zero)",
			config: &WDRRConfig{
				FlowWeights:     make(map[string]int),
				DefaultWeight:   0,
				BaseQuantum:     10,
				MinQuantum:      1,
				MaxDeficit:      100,
			},
			expectError: true,
			errorMsg:    "DefaultWeight must be positive",
		},
		{
			name: "Invalid DefaultWeight (negative)",
			config: &WDRRConfig{
				FlowWeights:     make(map[string]int),
				DefaultWeight:   -1,
				BaseQuantum:     10,
				MinQuantum:      1,
				MaxDeficit:      100,
			},
			expectError: true,
			errorMsg:    "DefaultWeight must be positive",
		},
		{
			name: "Invalid BaseQuantum (zero)",
			config: &WDRRConfig{
				FlowWeights:     make(map[string]int),
				DefaultWeight:   1,
				BaseQuantum:     0,
				MinQuantum:      1,
				MaxDeficit:      100,
			},
			expectError: true,
			errorMsg:    "BaseQuantum must be positive",
		},
		{
			name: "Invalid MinQuantum (zero)",
			config: &WDRRConfig{
				FlowWeights:     make(map[string]int),
				DefaultWeight:   1,
				BaseQuantum:     10,
				MinQuantum:      0,
				MaxDeficit:      100,
			},
			expectError: true,
			errorMsg:    "MinQuantum must be positive",
		},
		{
			name: "Invalid MaxDeficit (zero)",
			config: &WDRRConfig{
				FlowWeights:     make(map[string]int),
				DefaultWeight:   1,
				BaseQuantum:     10,
				MinQuantum:      1,
				MaxDeficit:      0,
			},
			expectError: true,
			errorMsg:    "MaxDeficit must be positive",
		},
		{
			name: "Invalid flow weight (zero)",
			config: &WDRRConfig{
				FlowWeights:     map[string]int{"flow1": 0},
				DefaultWeight:   1,
				BaseQuantum:     10,
				MinQuantum:      1,
				MaxDeficit:      100,
			},
			expectError: true,
			errorMsg:    "weight for flow",
		},
		{
			name: "Invalid flow weight (negative)",
			config: &WDRRConfig{
				FlowWeights:     map[string]int{"flow1": -5},
				DefaultWeight:   1,
				BaseQuantum:     10,
				MinQuantum:      1,
				MaxDeficit:      100,
			},
			expectError: true,
			errorMsg:    "weight for flow",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.config.Validate()
			if tc.expectError {
				require.Error(t, err, "Expected validation error")
				assert.Contains(t, err.Error(), tc.errorMsg, "Error message should contain expected text")
			} else {
				require.NoError(t, err, "Expected no validation error")
			}
		})
	}
}

func TestWDRR_Name(t *testing.T) {
	t.Parallel()
	policy := NewWDRR("test-wdrr", nil)
	assert.Equal(t, "test-wdrr", policy.TypedName().Name, "Name should match the provided name")
	assert.Equal(t, WDRRPolicyName, policy.TypedName().Type, "Type should match the policy's constant")
}

func TestWDRR_DefaultConfig(t *testing.T) {
	t.Parallel()
	policy := NewWDRR("", nil)
	assert.NotNil(t, policy, "Policy should be created with nil config")
	assert.Equal(t, WDRRPolicyName, policy.TypedName().Name, "Policy should have correct name")
}

func TestWDRR_SelectQueue_NilBand(t *testing.T) {
	t.Parallel()
	policy := NewWDRR("", nil)
	ctx := context.Background()

	selected, err := policy.Pick(ctx, nil)
	require.NoError(t, err, "Pick should not error on nil band")
	assert.Nil(t, selected, "Pick should return nil when band is nil")
}

func TestWDRR_SelectQueue_EmptyBand(t *testing.T) {
	t.Parallel()
	policy := NewWDRR("", nil)
	ctx := context.Background()
	state := policy.NewState(ctx)

	mockBand := &frameworkmocks.MockPriorityBandAccessor{
		PolicyStateV: state,
		FlowKeysFunc: func() []types.FlowKey { return []types.FlowKey{} },
		QueueFunc:    func(id string) framework.FlowQueueAccessor { return nil },
		IterateQueuesFunc: func(iterator func(flow framework.FlowQueueAccessor) bool) {},
	}

	selected, err := policy.Pick(ctx, mockBand)
	require.NoError(t, err, "Pick should not error on empty band")
	assert.Nil(t, selected, "Pick should return nil when band is empty")
}

func TestWDRR_SelectQueue_AllEmptyQueues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	policy := NewWDRR("", nil)

	// Three empty queues
	emptyFlow1Key := types.FlowKey{ID: "empty1", Priority: 0}
	emptyFlow2Key := types.FlowKey{ID: "empty2", Priority: 0}
	emptyFlow3Key := types.FlowKey{ID: "empty3", Priority: 0}

	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 0, FlowKeyV: emptyFlow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 0, FlowKeyV: emptyFlow2Key}
	queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 0, FlowKeyV: emptyFlow3Key}

	mockBand := newTestBand(queue1, queue2, queue3)
	mockBand.PolicyStateV = policy.NewState(ctx)

	selected, err := policy.Pick(ctx, mockBand)
	require.NoError(t, err, "SelectQueue should not error when all queues are empty")
	assert.Nil(t, selected, "SelectQueue should return nil when all queues are empty")
}

func TestWDRR_SelectQueue_EqualWeights(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// All flows have equal weight (default=1)
	policy := NewWDRR("", DefaultWDRRConfig())

	flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
	flow2Key := types.FlowKey{ID: "flow2", Priority: 0}
	flow3Key := types.FlowKey{ID: "flow3", Priority: 0}

	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: flow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: flow2Key}
	queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: flow3Key}

	mockBand := newTestBand(queue1, queue2, queue3)
	mockBand.PolicyStateV = policy.NewState(ctx)

	// Track selections over multiple rounds
	selectionCounts := make(map[string]int)
	for i := range 30 {
		selected, err := policy.Pick(ctx, mockBand)
		require.NoError(t, err, "SelectQueue should not error on iteration %d", i)
		require.NotNil(t, selected, "SelectQueue should select a queue on iteration %d", i)
		selectionCounts[selected.FlowKey().ID]++
	}

	// With equal weights, distribution should be roughly equal
	// Each flow should get roughly 10 selections (30 total / 3 flows)
	for flowID, count := range selectionCounts {
		assert.GreaterOrEqual(t, count, 5, "Flow %s should get at least 5 selections", flowID)
		assert.LessOrEqual(t, count, 15, "Flow %s should get at most 15 selections", flowID)
	}

	t.Logf("Equal weight distribution: flow1=%d, flow2=%d, flow3=%d",
		selectionCounts["flow1"], selectionCounts["flow2"], selectionCounts["flow3"])
}

func TestWDRR_SelectQueue_WeightedPriority(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Configure weights: flow1=5, flow2=1, flow3=1
	config := DefaultWDRRConfig()
	config.FlowWeights = map[string]int{
		"flow1": 5,
		"flow2": 1,
		"flow3": 1,
	}
	policy := NewWDRR("", config)

	flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
	flow2Key := types.FlowKey{ID: "flow2", Priority: 0}
	flow3Key := types.FlowKey{ID: "flow3", Priority: 0}

	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: flow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: flow2Key}
	queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: flow3Key}

	mockBand := newTestBand(queue1, queue2, queue3)

	mockBand.PolicyStateV = policy.NewState(ctx)
	// Track selections over many rounds
	selectionCounts := make(map[string]int)
	numSelections := 70 // Total weight is 5+1+1=7, so 70 selections = 10 full weight cycles
	for i := range numSelections {
		selected, err := policy.Pick(ctx, mockBand)
		require.NoError(t, err, "SelectQueue should not error on iteration %d", i)
		require.NotNil(t, selected, "SelectQueue should select a queue on iteration %d", i)
		selectionCounts[selected.FlowKey().ID]++
	}

	// Verify flow1 gets approximately 5x more selections than flow2 or flow3
	// Expected: flow1 ≈ 50, flow2 ≈ 10, flow3 ≈ 10
	assert.GreaterOrEqual(t, selectionCounts["flow1"], 40, "High-weight flow should get more selections")
	assert.GreaterOrEqual(t, selectionCounts["flow2"], 5, "Low-weight flow should still get service")
	assert.GreaterOrEqual(t, selectionCounts["flow3"], 5, "Low-weight flow should still get service")

	// Verify ratio is approximately correct (5:1:1)
	// Allow for some variance due to deficit accumulation dynamics
	ratio1to2 := float64(selectionCounts["flow1"]) / float64(selectionCounts["flow2"])
	assert.Greater(t, ratio1to2, 3.0, "Ratio of flow1:flow2 should be at least 3:1")

	t.Logf("Weighted distribution: flow1=%d (weight=5), flow2=%d (weight=1), flow3=%d (weight=1)",
		selectionCounts["flow1"], selectionCounts["flow2"], selectionCounts["flow3"])
}

func TestWDRR_SelectQueue_AntiStarvation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Configure: highPriority=10, lowPriority=1
	config := DefaultWDRRConfig()
	config.FlowWeights = map[string]int{
		"highPriority": 10,
		"lowPriority":  1,
	}
	config.MinQuantum = 1 // Guarantee minimum service
	policy := NewWDRR("", config)

	highPriorityKey := types.FlowKey{ID: "highPriority", Priority: 0}
	lowPriorityKey := types.FlowKey{ID: "lowPriority", Priority: 0}

	// Both queues have items
	queueHigh := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: highPriorityKey}
	queueLow := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: lowPriorityKey}

	mockBand := newTestBand(queueHigh, queueLow)

	// Track selections
	mockBand.PolicyStateV = policy.NewState(ctx)
	selectionCounts := make(map[string]int)
	lowPriorityFirstSelection := -1
	numSelections := 100

	for i := range numSelections {
		selected, err := policy.Pick(ctx, mockBand)
		require.NoError(t, err, "SelectQueue should not error on iteration %d", i)
		require.NotNil(t, selected, "SelectQueue should select a queue on iteration %d", i)

		flowID := selected.FlowKey().ID
		selectionCounts[flowID]++

		if flowID == "lowPriority" && lowPriorityFirstSelection == -1 {
			lowPriorityFirstSelection = i
		}
	}

	// Verify anti-starvation: low priority flow MUST get service
	assert.Greater(t, selectionCounts["lowPriority"], 0,
		"Low-priority flow must receive service (anti-starvation guarantee)")

	// Verify low priority got serviced within reasonable time
	// With weight ratio 10:1, high priority gets quantum=100, low priority gets quantum=10
	// Low priority should get service once high priority's deficit is depleted (around iteration 100)
	assert.Less(t, lowPriorityFirstSelection, numSelections,
		"Low-priority flow should be serviced within the test period (first selection at iteration %d)", lowPriorityFirstSelection)

	// High priority should get significantly more selections due to 10:1 weight ratio
	// Expected ratio is approximately 10:1
	assert.Greater(t, selectionCounts["highPriority"], selectionCounts["lowPriority"]*5,
		"High-priority flow should get significantly more selections (ratio should be >> 5:1)")

	t.Logf("Anti-starvation test: highPriority=%d, lowPriority=%d (first at iteration %d)",
		selectionCounts["highPriority"], selectionCounts["lowPriority"], lowPriorityFirstSelection)
}

func TestWDRR_SelectQueue_DeficitCapping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Configure with small MaxDeficit to test capping
	config := DefaultWDRRConfig()
	config.MaxDeficit = 30
	config.BaseQuantum = 20
	policy := NewWDRR("", config)

	flowKey := types.FlowKey{ID: "flow1", Priority: 0}
	queue := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: flowKey}

	state := policy.NewState(ctx).(*wdrrState)
	mockBand := &frameworkmocks.MockPriorityBandAccessor{
		PolicyStateV: state,
		FlowKeysFunc: func() []types.FlowKey { return []types.FlowKey{flowKey} },
		QueueFunc:    func(id string) framework.FlowQueueAccessor {
			if id == "flow1" {
				return queue
			}
			return nil
		},
	}

	// Make several selections to build up deficit beyond MaxDeficit
	// First call: deficit = 0, gets refilled to 20 (1*20), selected, deficit = 19
	// Second call: deficit = 19, selected, deficit = 18
	// ...
	// When deficit hits 0, it gets refilled to 20 again
	// We want to verify it never exceeds MaxDeficit=30

	for i := range 50 {
		selected, err := policy.Pick(ctx, mockBand)
		require.NoError(t, err, "SelectQueue should not error on iteration %d", i)
		require.NotNil(t, selected, "SelectQueue should select a queue on iteration %d", i)

		// Check that deficit is capped at MaxDeficit
		state.mu.Lock()
		deficit := state.deficits["flow1"]
		state.mu.Unlock()

		assert.LessOrEqual(t, deficit, config.MaxDeficit,
			"Deficit should be capped at MaxDeficit=%d, but got %d at iteration %d",
			config.MaxDeficit, deficit, i)
	}
}

func TestWDRR_SelectQueue_DynamicFlows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	policy := NewWDRR("", DefaultWDRRConfig())

	flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
	flow2Key := types.FlowKey{ID: "flow2", Priority: 0}
	flow3Key := types.FlowKey{ID: "flow3", Priority: 0}

	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: flow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: flow2Key}

	// Start with two flows
	mockBand := newTestBand(queue1, queue2)

	// Make some selections
	mockBand.PolicyStateV = policy.NewState(ctx)
	for i := range 5 {
		_, err := policy.Pick(ctx, mockBand)
		require.NoError(t, err, "SelectQueue should not error on initial iteration %d", i)
	}

	// Add a third flow
	queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: flow3Key}
	mockBand = newTestBand(queue1, queue2, queue3)
	mockBand.PolicyStateV = policy.NewState(ctx)

	// Continue selections - should handle new flow gracefully
	selectionCounts := make(map[string]int)
	for i := range 15 {
		selected, err := policy.Pick(ctx, mockBand)
		require.NoError(t, err, "SelectQueue should not error on iteration %d", i)
		if selected != nil {
			selectionCounts[selected.FlowKey().ID]++
		}
	}

	// All three flows should get some selections
	assert.Greater(t, selectionCounts["flow1"], 0, "flow1 should be selected")
	assert.Greater(t, selectionCounts["flow2"], 0, "flow2 should be selected")
	assert.Greater(t, selectionCounts["flow3"], 0, "flow3 should be selected")

	t.Logf("Dynamic flows: flow1=%d, flow2=%d, flow3=%d",
		selectionCounts["flow1"], selectionCounts["flow2"], selectionCounts["flow3"])
}

func TestWDRR_SelectQueue_Concurrency(t *testing.T) {
	t.Parallel()

	// Run this test multiple times to catch race conditions
	for i := range 3 {
		t.Run(fmt.Sprintf("Iteration%d", i), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			policy := NewWDRR("", DefaultWDRRConfig())

			flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
			flow2Key := types.FlowKey{ID: "flow2", Priority: 0}
			flow3Key := types.FlowKey{ID: "flow3", Priority: 0}

			queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: flow1Key}
			queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: flow2Key}
			queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: flow3Key}

			mockBand := newTestBand(queue1, queue2, queue3)

			var wg sync.WaitGroup
			mockBand.PolicyStateV = policy.NewState(ctx)
			numGoroutines := 10
			selectionsPerGoroutine := 30
			totalSelections := int64(numGoroutines * selectionsPerGoroutine)

			var selectionCounts sync.Map

			wg.Add(numGoroutines)
			for range numGoroutines {
				go func() {
					defer wg.Done()
					for range selectionsPerGoroutine {
						selected, err := policy.Pick(ctx, mockBand)
						if err == nil && selected != nil {
							val, _ := selectionCounts.LoadOrStore(selected.FlowKey().ID, new(atomic.Int64))
							val.(*atomic.Int64).Add(1)
						}
					}
				}()
			}
			wg.Wait()

			var finalCount int64
			countsStr := ""
			selectionCounts.Range(func(key, value any) bool {
				count := value.(*atomic.Int64).Load()
				finalCount += count
				countsStr += fmt.Sprintf("%s: %d, ", key, count)
				return true
			})

			assert.Equal(t, totalSelections, finalCount, "Total selections should match expected")
			t.Logf("Concurrent selection distribution: %s", countsStr)
		})
	}
}

func TestWDRR_SelectQueue_SkipsEmptyQueueWithPositiveDeficit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Configure flow1 with higher weight to build up deficit faster
	config := DefaultWDRRConfig()
	config.FlowWeights = map[string]int{
		"flow1": 10,
		"flow2": 1,
	}
	policy := NewWDRR("", config)

	flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
	flow2Key := types.FlowKey{ID: "flow2", Priority: 0}

	// Start with both queues having items
	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: flow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 100, FlowKeyV: flow2Key}

	mockBand := newTestBand(queue1, queue2)
	mockBand.PolicyStateV = policy.NewState(ctx)

	// Make a selection - flow1 should be selected and build up deficit
	selected, err := policy.Pick(ctx, mockBand)
	require.NoError(t, err, "First Pick should not error")
	require.NotNil(t, selected, "First Pick should select a queue")
	assert.Equal(t, "flow1", selected.FlowKey().ID, "flow1 should be selected first (higher weight)")

	// Now empty flow1's queue (simulating external consumption)
	queue1.LenV = 0

	// Make another selection - flow2 should be selected, NOT flow1 despite flow1 having positive deficit
	selected, err = policy.Pick(ctx, mockBand)
	require.NoError(t, err, "Second Pick should not error")
	require.NotNil(t, selected, "Second Pick should select a queue")
	assert.Equal(t, "flow2", selected.FlowKey().ID, "flow2 should be selected when flow1 is empty, even though flow1 has positive deficit")

	// Continue selecting - should keep selecting flow2
	for i := 0; i < 10; i++ {
		selected, err = policy.Pick(ctx, mockBand)
		require.NoError(t, err, "Pick should not error on iteration %d", i)
		require.NotNil(t, selected, "Pick should select a queue on iteration %d", i)
		assert.Equal(t, "flow2", selected.FlowKey().ID, "Only flow2 should be selected when flow1 is empty on iteration %d", i)
	}
}
