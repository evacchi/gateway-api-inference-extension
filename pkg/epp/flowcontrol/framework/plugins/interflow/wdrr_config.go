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

import "fmt"

// WDRRConfig holds the configuration parameters for the Weighted Deficit Round Robin (WDRR) policy.
//
// WDRR is an anti-starvation policy that provides weighted fairness between flows while adapting to load conditions.
// It uses a deficit counter mechanism to track accumulated credits for each flow, ensuring that all flows receive
// service proportional to their weights while guaranteeing minimum service for low-priority flows.
type WDRRConfig struct {
	// FlowWeights defines the relative priority weights for specific flows.
	// Keys are flow IDs, values are positive integer weights.
	// Flows with higher weights receive proportionally more service.
	// If a flow is not in this map, it receives the DefaultWeight.
	// Default: empty map (all flows use DefaultWeight)
	FlowWeights map[string]int

	// DefaultWeight is the weight assigned to flows not explicitly listed in FlowWeights.
	// This must be a positive integer.
	// Default: 1
	DefaultWeight int

	// BaseQuantum is the base amount of work (in abstract units) that a flow with weight=1 receives per round.
	// The actual quantum for a flow is: BaseQuantum * weight * load_adjustment
	// This must be a positive integer.
	// Default: 10
	BaseQuantum int64

	// MinQuantum is the minimum quantum any flow receives, regardless of weight or load.
	// This provides the anti-starvation guarantee: every non-empty flow will be serviced when its
	// deficit reaches at least MinQuantum.
	// This must be a positive integer and typically should be <= BaseQuantum.
	// Default: 1
	MinQuantum int64

	// LoadThreshold is the queue length (in number of items) above which a queue is considered "high load".
	// When a queue's length exceeds this threshold, its quantum is boosted by LoadBoostFactor.
	// This must be a positive integer.
	// Default: 10
	LoadThreshold int

	// LoadBoostFactor is the multiplier applied to quantum boost for high-load queues.
	// When a queue's load exceeds LoadThreshold, the boost is calculated as:
	//   boost = (currentLoad/LoadThreshold - 1.0) * LoadBoostFactor
	//   adjustedQuantum = baseQuantum * (1.0 + boost)
	// This must be non-negative. A value of 0 disables load-based boosting.
	// Default: 0.5 (50% boost when load is 2x threshold, 100% boost at 3x, etc.)
	LoadBoostFactor float64

	// MaxDeficit caps the maximum deficit that can accumulate for any flow.
	// This prevents unbounded deficit growth in pathological scenarios (e.g., a flow that's repeatedly
	// skipped due to insufficient deficit). When deficit would exceed this value, it's capped.
	// This must be a positive integer and should be >> MinQuantum.
	// Default: 100
	MaxDeficit int64

	// UseByteSize determines whether to use ByteSize() or Len() for load calculations.
	// - If true, uses queue.ByteSize() (useful for byte-based fairness)
	// - If false, uses queue.Len() (useful for request-count-based fairness)
	// Default: false (use Len())
	UseByteSize bool
}

// DefaultWDRRConfig returns a WDRRConfig with sensible defaults that work out-of-the-box.
//
// Default behavior:
//   - Equal weights for all flows (DefaultWeight=1, empty FlowWeights)
//   - Anti-starvation guarantee (MinQuantum=1)
//   - Load-aware quantum boosting enabled (LoadBoostFactor=0.5)
//   - Deficit capping to prevent unbounded growth (MaxDeficit=100)
//   - Request-count-based fairness (UseByteSize=false)
func DefaultWDRRConfig() *WDRRConfig {
	return &WDRRConfig{
		FlowWeights:     make(map[string]int),
		DefaultWeight:   1,
		BaseQuantum:     10,
		MinQuantum:      1,
		LoadThreshold:   10,
		LoadBoostFactor: 0.5,
		MaxDeficit:      100,
		UseByteSize:     false,
	}
}

// Validate checks the configuration for validity and returns an error if any parameter is invalid.
//
// Validation rules:
//   - DefaultWeight must be positive
//   - BaseQuantum must be positive
//   - MinQuantum must be positive
//   - LoadThreshold must be positive
//   - LoadBoostFactor must be non-negative
//   - MaxDeficit must be positive
//   - All weights in FlowWeights must be positive
//   - MinQuantum should typically be <= BaseQuantum (warning in docs, not enforced)
func (c *WDRRConfig) Validate() error {
	if c.DefaultWeight <= 0 {
		return fmt.Errorf("DefaultWeight must be positive, got %d", c.DefaultWeight)
	}
	if c.BaseQuantum <= 0 {
		return fmt.Errorf("BaseQuantum must be positive, got %d", c.BaseQuantum)
	}
	if c.MinQuantum <= 0 {
		return fmt.Errorf("MinQuantum must be positive, got %d", c.MinQuantum)
	}
	if c.LoadThreshold <= 0 {
		return fmt.Errorf("LoadThreshold must be positive, got %d", c.LoadThreshold)
	}
	if c.LoadBoostFactor < 0 {
		return fmt.Errorf("LoadBoostFactor must be non-negative, got %f", c.LoadBoostFactor)
	}
	if c.MaxDeficit <= 0 {
		return fmt.Errorf("MaxDeficit must be positive, got %d", c.MaxDeficit)
	}

	// Validate individual flow weights
	for flowID, weight := range c.FlowWeights {
		if weight <= 0 {
			return fmt.Errorf("weight for flow %q must be positive, got %d", flowID, weight)
		}
	}

	return nil
}
