# InterFlow Dispatch Policies

This document describes the available InterFlow dispatch policies that control fairness and ordering between different flows within a priority band.

## Available Policies

### 1. RandomSelect (Learning Example)

**Purpose**: Educational policy demonstrating the simplest possible InterFlowDispatchPolicy implementation.

**Behavior**: Randomly selects from non-empty queues with uniform probability.

**Use Cases**:
- Learning and understanding the InterFlowDispatchPolicy interface
- Quick prototyping
- Load testing scenarios where fairness is not critical

**Characteristics**:
- ✅ Stateless (no internal state management)
- ✅ Thread-safe (no synchronization needed)
- ✅ Simple implementation (~100 lines)
- ❌ No fairness guarantees
- ❌ Not recommended for production

**Example**:
```go
import "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/framework/plugins/interflow"

// Policy is automatically registered via init()
policy, _ := interflow.NewPolicyFromName("RandomSelect")
```

---

### 2. WDRR (Weighted Deficit Round Robin)

**Purpose**: Production-ready anti-starvation policy with weighted priorities and load-aware adaptation.

**Behavior**:
- Flows accumulate "deficit credits" based on their weight and current load
- Selects the flow with the highest positive deficit
- Higher-weight flows receive proportionally more service
- All flows guaranteed service (anti-starvation)

**Use Cases**:
- Production environments requiring fairness guarantees
- Multi-tenant systems with different priority levels
- Preventing flow starvation under high load
- Load-aware traffic management

**Characteristics**:
- ✅ Weighted priority (configurable per flow)
- ✅ Anti-starvation guarantees
- ✅ Load-aware quantum adjustment
- ✅ Thread-safe (mutex-protected state)
- ✅ Memory efficient (automatic cleanup)
- ✅ Zero-config defaults
- ✅ Fully configurable

**Configuration**:

```go
import "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/framework/plugins/interflow"

// Option 1: Use default configuration (all flows equal weight)
policy := interflow.NewWDRR(nil)

// Option 2: Custom configuration
config := &interflow.WDRRConfig{
    FlowWeights: map[string]int{
        "interactive": 5,  // High priority: 5x more service
        "batch":       1,  // Low priority: baseline service
    },
    DefaultWeight:   1,     // Weight for unlisted flows
    BaseQuantum:     10,    // Base work units per round
    MinQuantum:      1,     // Minimum service guarantee (anti-starvation)
    LoadThreshold:   10,    // Queue length considered "high load"
    LoadBoostFactor: 0.5,   // 50% boost for high-load queues
    MaxDeficit:      100,   // Cap on accumulated deficit
    UseByteSize:     false, // Use request count vs byte size
}
policy := interflow.NewWDRR(config)
```

**Default Configuration**:
- `FlowWeights`: Empty map (all flows equal)
- `DefaultWeight`: 1
- `BaseQuantum`: 10
- `MinQuantum`: 1
- `LoadThreshold`: 10 items
- `LoadBoostFactor`: 0.5 (50% boost)
- `MaxDeficit`: 100
- `UseByteSize`: false (use `Len()`)

**How It Works**:

1. **Deficit Accumulation**: Each flow accumulates deficit credits each round
   - `deficit += BaseQuantum × weight × load_factor`
   - Higher weights = more credits = more selections

2. **Load-Aware Quantum**:
   ```
   if queue.Len() > LoadThreshold:
       boost = (queue.Len() / LoadThreshold - 1) × LoadBoostFactor
       quantum = BaseQuantum × weight × (1 + boost)
   else:
       quantum = BaseQuantum × weight
   ```

3. **Selection**: Choose flow with highest positive deficit
   - Deduct 1 unit per selection
   - Refill when deficit ≤ 0

4. **Anti-Starvation**: Every flow guaranteed `MinQuantum` credits
   - Ensures all flows get service within bounded time
   - Prevents indefinite starvation

**Example Scenarios**:

**Scenario 1: Interactive vs Batch Workloads**
```go
config := &interflow.WDRRConfig{
    FlowWeights: map[string]int{
        "user-queries":  10,  // High priority
        "batch-jobs":     1,  // Low priority
    },
}
```
Result: User queries get ~10x more service, but batch jobs never starve.

**Scenario 2: Load-Based Adaptation**
```
High-load queue (30 items): quantum = 10 × 1 × (1 + (30/10 - 1) × 0.5) = 20
Low-load queue (5 items):   quantum = 10 × 1 × 1 = 10
```
Result: High-load queue gets 2x quantum boost.

**Scenario 3: Anti-Starvation with 100:1 Ratio**
```go
config := &interflow.WDRRConfig{
    FlowWeights: map[string]int{
        "critical": 100,
        "background": 1,
    },
    MinQuantum: 1,  // Guarantee background gets service
}
```
Result: Critical flow gets ~100x more service, but background guaranteed service every ~100 rounds.

---

### 3. RoundRobin (Existing)

Simple round-robin selection ensuring equal service rotation between flows.

---

### 4. BestHead (Existing)

Greedy policy that selects the queue with the highest-priority head item (bypasses fairness).

---

## Policy Selection Guide

| Requirement | Recommended Policy |
|-------------|-------------------|
| Learning/Education | **RandomSelect** |
| Equal fairness | **RoundRobin** |
| Weighted priorities | **WDRR** |
| Anti-starvation | **WDRR** |
| Load-aware scheduling | **WDRR** |
| Maximum throughput | **BestHead** |
| Production multi-tenant | **WDRR** |

## Registration

All policies are automatically registered via `init()` functions and available through the factory:

```go
import "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/flowcontrol/framework/plugins/interflow"

// List all registered policies
for name := range interflow.RegisteredPolicies {
    fmt.Println(name)
}

// Create a policy by name
policy, err := interflow.NewPolicyFromName("WDRR")
```

## Testing

All policies are automatically tested via the conformance test suite:

```bash
go test ./pkg/epp/flowcontrol/framework/plugins/interflow -v -run TestInterFlowDispatchPolicyConformance
```

## Further Reading

- **RandomSelect**: See `randomselect.go` for implementation details
- **WDRR**: See `wdrr.go` for algorithm, `wdrr_config.go` for configuration
- **Tests**: See `*_test.go` files for usage examples and test scenarios
