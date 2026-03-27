# Replica State Machine

## Purpose

Define the replicated replica-local rules for descriptor changes, config changes,
splits, and apply ordering before Raft code exists.

## Core Replicated Records

```text
RangeDescriptor {
  range_id
  generation
  start_key
  end_key
  replicas[]
  leaseholder_hint
}

ReplicaState {
  range_id
  descriptor
  applied_index
  truncated_index
  lease
  gc_threshold
}
```

Rules:

- `generation` changes on every split, merge, or membership change that mutates
  the effective descriptor
- `applied_index` is monotonically increasing
- replicated descriptor state is authoritative for routing and config validation

## Command Classes

The replica state machine applies the following command families:

- MVCC data writes
- intent resolution
- lease updates
- transaction record updates
- config changes
- split triggers
- snapshot application side effects

Each applied command must be deterministic with respect to replicated inputs.

## Apply Ordering

Per range:

- commands apply in committed log order
- no later command may become visible before all earlier committed commands are
  applied
- side effects derived from a command must be idempotent with respect to replay

Local background work may observe apply results, but it may not reorder them.

## Descriptor Generation Gate

Every config-changing proposal must carry:

- `expected_generation`
- the target change payload

Apply rule:

- if `expected_generation != descriptor.generation`, reject the change as stale
- if it matches, apply the change and increment `descriptor.generation`

This generation check lives in the replicated state machine. Allocator-side
locking is only an optimization to reduce needless conflicting proposals.

## Membership Change Rules

ChronosDB allows one major membership reconfiguration in flight per range.

Rules:

- learner addition must commit before learner promotion
- replica removal must not strand the range below its declared survival goal
- config changes must preserve quorum safety at every intermediate step
- stale proposals must fail generation validation rather than partially apply

## Split Trigger Rules

A split trigger applies atomically with the command that installs it.

On split:

1. parent descriptor is replaced with a left descriptor using a new generation
2. right-hand descriptor is created with its own `range_id` and generation
3. replicated metadata needed to route to both children becomes durable before
   the split is externally visible

Post-split requirements:

- parent and child spans are disjoint and contiguous
- right child starts with a snapshot-consistent view of replicated state
- stale pre-split descriptors must fail routing or generation validation

## Snapshot Rules

Snapshot application must install a self-consistent replica image:

- descriptor
- replicated MVCC state
- applied index / truncated state
- lease record if present

A snapshot may replace stale local state, but it must not create a descriptor
that conflicts with a newer generation already durably applied.

## Rebalance Rules

Rebalancing is a sequence of state-machine-safe steps:

1. add learner
2. catch up via snapshot/log
3. promote learner
4. optionally transfer lease
5. remove old replica

At every step:

- routing remains based on the authoritative descriptor
- the allocator must re-check generation before each subsequent step

## Failure Rules

### Stale allocator action

If the allocator proposes against an old descriptor generation:

- reject with a retriable stale-descriptor-style error
- do not partially apply any config side effect

### Apply crash / replay

If a node crashes during apply:

- replay must be safe and idempotent
- externally visible state is defined by the durable replicated log and applied
  state, not by in-memory partial work

## Tests Required

- committed commands apply in log order
- stale generation config changes are rejected
- split creates disjoint contiguous child spans
- rebalance steps must re-check generation between phases
- replay after crash does not double-apply side effects
