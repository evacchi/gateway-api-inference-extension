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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	frameworkmocks "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/framework/mocks"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/types"
)

func TestRandomSelect_Name(t *testing.T) {
	t.Parallel()
	policy := newRandomSelect()
	assert.Equal(t, RandomSelectPolicyName, policy.Name(), "Name should match the policy's constant")
}

func TestRandomSelect_SelectQueue_NilBand(t *testing.T) {
	t.Parallel()
	policy := newRandomSelect()

	selected, err := policy.SelectQueue(nil)
	require.NoError(t, err, "SelectQueue should not error on nil band")
	assert.Nil(t, selected, "SelectQueue should return nil when band is nil")
}

func TestRandomSelect_SelectQueue_EmptyBand(t *testing.T) {
	t.Parallel()
	policy := newRandomSelect()

	mockBand := newTestBand() // Empty band with no queues

	selected, err := policy.SelectQueue(mockBand)
	require.NoError(t, err, "SelectQueue should not error on empty band")
	assert.Nil(t, selected, "SelectQueue should return nil when band is empty")
}

func TestRandomSelect_SelectQueue_AllEmptyQueues(t *testing.T) {
	t.Parallel()
	policy := newRandomSelect()

	// Setup: Three flows but all with empty queues
	emptyFlow1Key := types.FlowKey{ID: "empty1", Priority: 0}
	emptyFlow2Key := types.FlowKey{ID: "empty2", Priority: 0}
	emptyFlow3Key := types.FlowKey{ID: "empty3", Priority: 0}

	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 0, FlowKeyV: emptyFlow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 0, FlowKeyV: emptyFlow2Key}
	queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 0, FlowKeyV: emptyFlow3Key}

	mockBand := newTestBand(queue1, queue2, queue3)

	selected, err := policy.SelectQueue(mockBand)
	require.NoError(t, err, "SelectQueue should not error when all queues are empty")
	assert.Nil(t, selected, "SelectQueue should return nil when all queues are empty")
}

func TestRandomSelect_SelectQueue_SingleNonEmptyQueue(t *testing.T) {
	t.Parallel()
	policy := newRandomSelect()

	// Setup: Only one non-empty queue
	flowKey := types.FlowKey{ID: "flow1", Priority: 0}
	queue := &frameworkmocks.MockFlowQueueAccessor{LenV: 5, FlowKeyV: flowKey}

	mockBand := newTestBand(queue)

	selected, err := policy.SelectQueue(mockBand)
	require.NoError(t, err, "SelectQueue should not error with a single non-empty queue")
	require.NotNil(t, selected, "SelectQueue should select the only non-empty queue")
	assert.Equal(t, "flow1", selected.FlowKey().ID, "Should select flow1")
}

func TestRandomSelect_SelectQueue_MultipleNonEmptyQueues(t *testing.T) {
	t.Parallel()
	policy := newRandomSelect()

	// Setup: Three non-empty queues
	flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
	flow2Key := types.FlowKey{ID: "flow2", Priority: 0}
	flow3Key := types.FlowKey{ID: "flow3", Priority: 0}

	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 1, FlowKeyV: flow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 2, FlowKeyV: flow2Key}
	queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 3, FlowKeyV: flow3Key}

	mockBand := newTestBand(queue1, queue2, queue3)

	// Perform multiple selections to verify a queue is always selected
	for i := range 10 {
		selected, err := policy.SelectQueue(mockBand)
		require.NoError(t, err, "SelectQueue should not error on iteration %d", i)
		require.NotNil(t, selected, "SelectQueue should select a queue on iteration %d", i)

		// Verify the selected queue is one of the three non-empty queues
		selectedID := selected.FlowKey().ID
		assert.Contains(t, []string{"flow1", "flow2", "flow3"}, selectedID,
			"Selected queue ID should be one of the available queues on iteration %d", i)
	}
}

func TestRandomSelect_SelectQueue_SkipsEmptyQueues(t *testing.T) {
	t.Parallel()
	policy := newRandomSelect()

	// Setup: Mix of empty and non-empty queues
	flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
	emptyFlow1Key := types.FlowKey{ID: "empty1", Priority: 0}
	flow2Key := types.FlowKey{ID: "flow2", Priority: 0}
	emptyFlow2Key := types.FlowKey{ID: "empty2", Priority: 0}

	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 1, FlowKeyV: flow1Key}
	queueEmpty1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 0, FlowKeyV: emptyFlow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 2, FlowKeyV: flow2Key}
	queueEmpty2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 0, FlowKeyV: emptyFlow2Key}

	mockBand := newTestBand(queue1, queueEmpty1, queue2, queueEmpty2)

	// Perform multiple selections to verify only non-empty queues are selected
	for i := range 20 {
		selected, err := policy.SelectQueue(mockBand)
		require.NoError(t, err, "SelectQueue should not error on iteration %d", i)
		require.NotNil(t, selected, "SelectQueue should select a queue on iteration %d", i)

		selectedID := selected.FlowKey().ID
		assert.Contains(t, []string{"flow1", "flow2"}, selectedID,
			"Selected queue should be one of the non-empty queues on iteration %d", i)
		assert.NotContains(t, []string{"empty1", "empty2"}, selectedID,
			"Selected queue should never be an empty queue on iteration %d", i)
	}
}

func TestRandomSelect_SelectQueue_RandomDistribution(t *testing.T) {
	t.Parallel()
	policy := newRandomSelect()

	// Setup: Three non-empty queues with different lengths
	flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
	flow2Key := types.FlowKey{ID: "flow2", Priority: 0}
	flow3Key := types.FlowKey{ID: "flow3", Priority: 0}

	queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 10, FlowKeyV: flow1Key}
	queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 20, FlowKeyV: flow2Key}
	queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 30, FlowKeyV: flow3Key}

	mockBand := newTestBand(queue1, queue2, queue3)

	// Perform many selections to check distribution
	numSelections := 300
	selectionCounts := make(map[string]int)

	for range numSelections {
		selected, err := policy.SelectQueue(mockBand)
		require.NoError(t, err, "SelectQueue should not error")
		require.NotNil(t, selected, "SelectQueue should select a queue")

		selectionCounts[selected.FlowKey().ID]++
	}

	// Verify all queues were selected at least once
	assert.Contains(t, selectionCounts, "flow1", "flow1 should be selected at least once")
	assert.Contains(t, selectionCounts, "flow2", "flow2 should be selected at least once")
	assert.Contains(t, selectionCounts, "flow3", "flow3 should be selected at least once")

	// Verify distribution is relatively even (each queue should get roughly 1/3 of selections)
	// We use a generous tolerance since random selection can have variance
	expectedCount := numSelections / 3
	minExpectedCount := expectedCount / 3 // At least 1/9 of total selections
	maxExpectedCount := expectedCount * 2  // At most 2/3 of total selections

	for flowID, count := range selectionCounts {
		assert.GreaterOrEqual(t, count, minExpectedCount,
			"Queue %s was selected %d times, expected at least %d", flowID, count, minExpectedCount)
		assert.LessOrEqual(t, count, maxExpectedCount,
			"Queue %s was selected %d times, expected at most %d", flowID, count, maxExpectedCount)
	}

	t.Logf("Selection distribution over %d selections: flow1=%d, flow2=%d, flow3=%d",
		numSelections, selectionCounts["flow1"], selectionCounts["flow2"], selectionCounts["flow3"])
}

func TestRandomSelect_SelectQueue_Concurrency(t *testing.T) {
	t.Parallel()
	// Run this test multiple times to increase the chance of catching race conditions
	for i := range 5 {
		t.Run(fmt.Sprintf("Iteration%d", i), func(t *testing.T) {
			t.Parallel()
			policy := newRandomSelect()

			// Setup: Three non-empty queues
			flow1Key := types.FlowKey{ID: "flow1", Priority: 0}
			flow2Key := types.FlowKey{ID: "flow2", Priority: 0}
			flow3Key := types.FlowKey{ID: "flow3", Priority: 0}

			queue1 := &frameworkmocks.MockFlowQueueAccessor{LenV: 1, FlowKeyV: flow1Key}
			queue2 := &frameworkmocks.MockFlowQueueAccessor{LenV: 2, FlowKeyV: flow2Key}
			queue3 := &frameworkmocks.MockFlowQueueAccessor{LenV: 3, FlowKeyV: flow3Key}

			mockBand := newTestBand(queue1, queue2, queue3)

			var wg sync.WaitGroup
			numGoroutines := 10
			selectionsPerGoroutine := 30
			totalSelections := int64(numGoroutines * selectionsPerGoroutine)

			var selectionCounts sync.Map // Used like a concurrent map[string]*atomic.Int64

			wg.Add(numGoroutines)
			for range numGoroutines {
				go func() {
					defer wg.Done()
					for range selectionsPerGoroutine {
						selected, err := policy.SelectQueue(mockBand)
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

				// Check that each queue got a reasonable share
				// Since it's random, we allow for significant variance
				minExpectedCount := totalSelections / 10 // At least 10% of total
				assert.True(t, count > minExpectedCount,
					"Queue %s was selected only %d times, expected at least %d", key, count, minExpectedCount)
				return true
			})

			assert.Equal(t, totalSelections, finalCount, "Total selections should match the expected number")
			t.Logf("Selection distribution: %s", countsStr)
		})
	}
}
