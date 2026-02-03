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

package saturation

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/flowcontrol"
)

// mockHandle implements QueueItemHandle for testing
type mockHandle struct {
	id string
}

func (m *mockHandle) Handle() any {
	return m.id
}

func (m *mockHandle) Invalidate() {
	// no-op
}

func (m *mockHandle) IsInvalidated() bool {
	return false
}

// mockEvictableQueue is a minimal mock for testing eviction
type mockEvictableQueue struct {
	removedHandles []string
}

func (m *mockEvictableQueue) Remove(handle flowcontrol.QueueItemHandle) (flowcontrol.QueueItemAccessor, error) {
	m.removedHandles = append(m.removedHandles, handle.Handle().(string))
	return nil, nil
}

// mockQueueItem is a minimal mock for testing
type mockQueueItem struct {
	handle flowcontrol.QueueItemHandle
}

func (m *mockQueueItem) Handle() flowcontrol.QueueItemHandle {
	return m.handle
}

func (m *mockQueueItem) OriginalRequest() flowcontrol.FlowControlRequest {
	return nil
}

func (m *mockQueueItem) EnqueueTime() time.Time {
	return time.Time{}
}

func (m *mockQueueItem) EffectiveTTL() time.Duration {
	return 0
}

func (m *mockQueueItem) SetHandle(handle flowcontrol.QueueItemHandle) {
	m.handle = handle
}

func TestBatchEvictor_OnlyEvictsNegativePriorities(t *testing.T) {
	evictor := NewBatchEvictor()
	ctx := context.Background()

	queue := &mockEvictableQueue{}

	// Schedule items at various priorities
	evictor.ScheduleEvictionCandidate(ctx, queue, &mockQueueItem{handle: &mockHandle{id: "item1"}}, 10, 0.8)   // high priority - should NOT evict
	evictor.ScheduleEvictionCandidate(ctx, queue, &mockQueueItem{handle: &mockHandle{id: "item2"}}, 0, 0.8)    // zero priority - should NOT evict
	evictor.ScheduleEvictionCandidate(ctx, queue, &mockQueueItem{handle: &mockHandle{id: "item3"}}, -5, 0.8)   // negative priority - SHOULD evict
	evictor.ScheduleEvictionCandidate(ctx, queue, &mockQueueItem{handle: &mockHandle{id: "item4"}}, -10, 0.8)  // negative priority - SHOULD evict

	evicted, err := evictor.ProcessScheduled(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if evicted != 2 {
		t.Errorf("Expected 2 evictions (only negative priorities), got %d", evicted)
	}

	if len(queue.removedHandles) != 2 {
		t.Errorf("Expected 2 items removed from queue, got %d", len(queue.removedHandles))
	}
}

func TestBatchEvictor_EvictsInLIFOOrder(t *testing.T) {
	evictor := NewBatchEvictor()
	ctx := context.Background()

	queue := &mockEvictableQueue{}

	// Schedule items in order (FIFO)
	evictor.ScheduleEvictionCandidate(ctx, queue, &mockQueueItem{handle: &mockHandle{id: "first"}}, -10, 0.8)
	evictor.ScheduleEvictionCandidate(ctx, queue, &mockQueueItem{handle: &mockHandle{id: "second"}}, -10, 0.8)
	evictor.ScheduleEvictionCandidate(ctx, queue, &mockQueueItem{handle: &mockHandle{id: "third"}}, -10, 0.8)

	evicted, err := evictor.ProcessScheduled(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if evicted != 3 {
		t.Errorf("Expected 3 evictions, got %d", evicted)
	}

	// Verify LIFO order: most recent first (third, second, first)
	if len(queue.removedHandles) != 3 {
		t.Fatalf("Expected 3 removed handles, got %d", len(queue.removedHandles))
	}

	if queue.removedHandles[0] != "third" {
		t.Errorf("Expected first eviction to be 'third' (LIFO), got %v", queue.removedHandles[0])
	}
	if queue.removedHandles[1] != "second" {
		t.Errorf("Expected second eviction to be 'second', got %v", queue.removedHandles[1])
	}
	if queue.removedHandles[2] != "first" {
		t.Errorf("Expected third eviction to be 'first', got %v", queue.removedHandles[2])
	}
}

func TestBatchEvictor_ClearsScheduleAfterProcessing(t *testing.T) {
	evictor := NewBatchEvictor()
	ctx := context.Background()

	queue := &mockEvictableQueue{}

	// Schedule and process
	evictor.ScheduleEvictionCandidate(ctx, queue, &mockQueueItem{handle: &mockHandle{id: "item1"}}, -10, 0.8)
	evicted, _ := evictor.ProcessScheduled(ctx)

	if evicted != 1 {
		t.Errorf("Expected 1 eviction, got %d", evicted)
	}

	// Process again - should evict nothing (schedule was cleared)
	evicted, _ = evictor.ProcessScheduled(ctx)

	if evicted != 0 {
		t.Errorf("Expected 0 evictions on second call (schedule should be cleared), got %d", evicted)
	}
}

func TestBatchEvictor_EmptySchedule(t *testing.T) {
	evictor := NewBatchEvictor()
	ctx := context.Background()

	// Process with no scheduled candidates
	evicted, err := evictor.ProcessScheduled(ctx)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if evicted != 0 {
		t.Errorf("Expected 0 evictions (no candidates), got %d", evicted)
	}
}

func TestBatchEvictor_TypedName(t *testing.T) {
	evictor := NewBatchEvictor()
	typedName := evictor.TypedName()

	if typedName.Type != BatchEvictorType {
		t.Errorf("Expected type %s, got %s", BatchEvictorType, typedName.Type)
	}

	if typedName.Name != BatchEvictorType {
		t.Errorf("Expected name %s, got %s", BatchEvictorType, typedName.Name)
	}
}
