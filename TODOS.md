# ChronosDB TODOs

This file tracks the next concrete milestones under the final-version
architecture. It must not reintroduce temporary MVP assumptions that conflict
with `ARCHITECTURE.md` or `IMPLEMENTATION_PLAN.md`.

## Pre-Code Blockers

### [x] Publish Phase 0 Spec Pack

**What:** Write the protocol-level specs required before any code lands.

**Why:** `ARCHITECTURE.md` explains the system shape, but code implementation
must follow exact state-machine and contract rules instead of inventing them.

**Must produce:**

- `specs/phase0/KEYSPACE.md`
- `specs/phase0/REPLICA_STATE_MACHINE.md`
- `specs/phase0/LEASE_STATE_MACHINE.md`
- `specs/phase0/TXN_STATE_MACHINE.md`
- `specs/phase0/TIMESTAMPS.md`
- `specs/phase0/ERRORS.md`
- `specs/phase0/RETRY_CONTRACT.md`
- `specs/phase0/CLOSED_TIMESTAMPS.md`
- `specs/phase0/PLACEMENT_AND_SURVIVAL.md`
- `specs/phase0/INVARIANTS.md`

**Must freeze:**

- one Pebble engine with logical namespaces for Raft and MVCC state
- separate leaseholder and Raft leader concepts, with co-location by policy
- lease `sequence` as the epoch-bumped transfer barrier
- `TxnRecord.status` including `STAGING`
- parallel commit and `STAGING` recovery semantics
- routing retry contract and range cache invalidation rules
- config-change generation checks in the replicated state machine
- compaction escalation triggers when Pebble approaches write-stall pressure
- closed timestamp publication as the follower-read gate

**Status:** Completed in the Phase 0 spec-pack PR.

**Depends on / blocked by:** Nothing. This must be completed before Phase 1 code.

### [x] Freeze README as the Public Baseline

**What:** Keep the repository README aligned with the architecture contract.

**Why:** The remote repository should explain the system correctly before any code
appears, especially the parts people usually get wrong in distributed databases.

**README must keep explicit:**

- leaseholder and Raft leader are different concepts
- routing truth comes from meta ranges, not gossip
- follower reads are historical and bounded by closed timestamps
- SQL is the product interface; KV is the internal substrate

**Status:** Completed before implementation code.

**Depends on / blocked by:** `ARCHITECTURE.md`, `IMPLEMENTATION_PLAN.md`

## Delivery Phases

### [ ] Phase 1: Single-Node Storage Core

Deliver:

- [ ] Pebble wrapper
- [ ] MVCC key encoding
- [ ] logical namespaces for `/raft/...` and `/mvcc/...`
- [ ] snapshots and recovery semantics
- [ ] intent representation
- [ ] store-versioning hooks before on-disk evolution

Exit criteria:

- storage behavior is testable without Raft
- store-versioning exists before on-disk evolution begins

### [ ] Phase 2: Single-Range Replication and Fast Reads

Deliver:

- shared MultiRaft scheduler
- batched `WriteBatch` persistence
- replica apply loop
- lease records and epoch-bumped transfer semantics
- leaseholder-local fast reads
- `ReadIndex` fallback
- commit-wait ordering
- compaction escalation triggers

Exit criteria:

- one range can replicate, fail over, transfer lease, and read safely
- the scheduler owns batching and fsync amortization

### [ ] Phase 3: Meta, Routing, and Membership

Deliver:

- meta ranges
- range cache
- routing refresh and invalidation
- liveness records and epochs
- split triggers
- learner/snapshot/rebalance flow
- generation-checked config changes
- optional advisory gossip dissemination

Exit criteria:

- routing remains correct through split and rebalance
- stale allocator decisions cannot win

### [ ] Phase 4: Transaction Core

Deliver:

- lock table
- contention handling
- wound-wait
- retryable restart handling
- refresh spans
- one-phase commit fast path
- client-visible retry/error mapping

Exit criteria:

- transaction semantics are centralized and testable
- callers do not invent their own retry or lock behavior

### [ ] Phase 5: Multi-Range Transactions

Deliver:

- anchored `TxnRecord`
- distributed intents
- coordinator recovery
- async intent resolution
- `STAGING` recovery
- parallel commit

Exit criteria:

- encountering-request and async recovery produce deterministic outcomes
- `STAGING -> COMMITTED/ABORTED` follows the written state-machine rule

### [ ] Phase 6: SQL Front Door

Deliver:

- PostgreSQL wire protocol
- parser integration
- catalog descriptors
- binder and semantic analysis
- cost-based planning skeleton
- logical-to-KV mapping
- distributed flow planning for scans, joins, and aggregations

Exit criteria:

- SQL uses the existing KV/routing/txn substrate
- SQL layers do not bypass protocol contracts

### [ ] Phase 7: Locality and Follower Reads

Deliver:

- closed timestamp publication
- follower historical reads
- lease preferences
- placement classes
- home-region semantics

Exit criteria:

- follower reads are freshness-bounded and observable
- placement policy is user-expressible and internally consistent

### [ ] Phase 8: Hardening and Operability

Deliver:

- admission control
- better balancing
- snapshot tuning
- observability dashboards
- large-scale simulation
- chaos and Jepsen testing

Exit criteria:

- the system can be operated, profiled, and failure-tested at realistic scale

## Ongoing Discipline

### [ ] Keep the Plan Ahead of the Code

Rule:

- any protocol or scope change must update `IMPLEMENTATION_PLAN.md` first
- every code commit must map to a phase or sub-phase
- tests for a behavior land in the same change as the behavior
- no mixed current-phase and future-phase implementation commits
