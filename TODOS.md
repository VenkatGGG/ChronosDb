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

### [x] Phase 1: Single-Node Storage Core

Deliver:

- [x] Pebble wrapper
- [x] MVCC key encoding
- [x] logical namespaces for `/raft/...` and `/mvcc/...`
- [x] snapshots and recovery semantics
- [x] intent representation
- [x] store-versioning hooks before on-disk evolution

Exit criteria:

- storage behavior is testable without Raft
- store-versioning exists before on-disk evolution begins

**Status:** Completed with `internal/storage` and `internal/hlc`, covered by
`go test ./...`.

### [x] Phase 2: Single-Range Replication and Fast Reads

Deliver:

- [x] shared MultiRaft scheduler
- [x] batched `WriteBatch` persistence
- [x] replica apply loop
- [x] lease records and epoch-bumped transfer semantics
- [x] leaseholder-local fast reads
- [x] `ReadIndex` fallback
- [x] commit-wait ordering
- [x] compaction escalation triggers

Exit criteria:

- one range can replicate, fail over, transfer lease, and read safely
- the scheduler owns batching and fsync amortization

**Status:** Completed with `internal/multiraft`, `internal/replica`,
`internal/lease`, and Phase 2 storage extensions, covered by `go test ./...`.

### [x] Phase 3: Meta, Routing, and Membership

Deliver:

- [x] meta ranges
- [x] range cache
- [x] routing refresh and invalidation
- [x] liveness records and epochs
- [x] split triggers
- [x] learner/snapshot/rebalance flow
- [x] generation-checked config changes
- [ ] optional advisory gossip dissemination (deferred)

Exit criteria:

- routing remains correct through split and rebalance
- stale allocator decisions cannot win

**Status:** Core deliverables are complete. `internal/meta`, `internal/routing`,
authoritative meta1/meta2 layout bootstrapping, cache-backed routing,
generation checks, split-trigger application, rebalance-safe membership
transitions, and snapshot image installation are implemented. Optional advisory
gossip dissemination remains open by design because correctness does not depend
on it.

### [x] Phase 4: Transaction Core

Deliver:

- [x] lock table
- [x] contention handling
- [x] wound-wait
- [x] retryable restart handling
- [x] refresh spans
- [x] one-phase commit fast path
- [x] client-visible retry/error mapping

Exit criteria:

- transaction semantics are centralized and testable
- callers do not invent their own retry or lock behavior

**Status:** Core deliverables are complete. The canonical transaction package
now owns the base transaction record, retry/restart rules, lock table state,
wait queues, wound-wait decisions, refresh spans, one-phase commit, and
client-visible retry/error mapping.

### [ ] Phase 5: Multi-Range Transactions

Deliver:

- [x] anchored `TxnRecord`
- [x] distributed intents
- [x] coordinator recovery
- [x] async intent resolution
- [x] `STAGING` recovery
- [x] parallel commit

Exit criteria:

- encountering-request and async recovery produce deterministic outcomes
- `STAGING -> COMMITTED/ABORTED` follows the written state-machine rule

**Status:** Core deliverables are complete. The canonical transaction layer now
owns anchored records, required distributed intents, `PENDING -> STAGING`
transitions, coordinator-death recovery, async intent-resolution planning, and
the deterministic `STAGING -> COMMITTED/ABORTED` decision rule.

### [x] Phase 6: SQL Front Door

Deliver:

- [x] PostgreSQL wire protocol
- [x] parser integration
- [x] catalog descriptors
- [x] binder and semantic analysis
- [x] cost-based planning skeleton
- [x] logical-to-KV mapping
- distributed flow planning for scans, joins, and aggregations

Exit criteria:

- SQL uses the existing KV/routing/txn substrate
- SQL layers do not bypass protocol contracts

**Status:** Planning deliverables are complete. The SQL front-door now has parser-backed planning for
simple single-table `SELECT` and `INSERT`, catalog descriptors,
binding/semantic validation, primary-key KV mapping, and an explicit
cost-based planning skeleton with candidate ranking over physical KV access
paths. A standalone PostgreSQL wire-protocol foundation now exists in
`internal/pgwire` for startup, simple-query framing, and backend responses. The
wire layer is now connected to the SQL planner for simple-query validation and
row-description metadata, and the distributed SQL layer has a first
flow-planner boundary for scan and mutation plans with stage/operator
vocabulary reserved for future join and aggregation planning. The pgwire layer
now also has a real connection-serving loop with startup negotiation, SSL
rejection, simple-query dispatch, and listener integration. Single-table
aggregate planning for `GROUP BY`, `COUNT`, and `SUM` now exists and maps onto
distributed partial/final aggregate flow stages. Two-table inner equi-join
planning now also exists, with alias-aware binding and distributed hash-join
flow stages over independently distributed left/right scans. Flow plans now
also carry explicit fragment boundaries and typed result schemas that propagate
through the pgwire description path. Full execution of those distributed flows
is still a later concern, but the SQL front-door planning surface itself is now
in place.

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

**Status:** In progress. The closed-timestamp publication core and follower-read
eligibility checks now exist as a dedicated package, including monotonic
publication, lease-sequence invalidation, intent-backlog stalling, and
fail-closed behavior under clock-offset violations. Placement policy
normalization and allocator-facing compilation now exist as a dedicated package
for `REGIONAL`, `HOME_REGION`, and `GLOBAL` declarations, and leaseholder
selection can now follow compiled region preferences. Replica-local routing can
also choose between follower-historical and leaseholder reads based on closed
timestamp safety. SQL table descriptors, distributed flow stages, and range
descriptors now carry validated placement policy and home-region hints. The
remaining Phase 7 work is wiring those locality policies through more of the
live replica/routing surfaces. Closed timestamp publications can now also be
encoded and persisted through the storage engine under the global system
namespace, and the replica state machine now applies lease-bound closed
timestamp publications as live replica state and can serve exact historical
reads when the closed-timestamp proof permits it.

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

**Status:** In progress. A dedicated admission controller now exists with
critical/normal/background tiers, reserved capacity for critical work, and
compaction escalation when storage pressure crosses configured thresholds. The
allocator now also has a placement-aware rebalance scorer that avoids violating
survival-region constraints while moving replicas off hotter nodes. A new
deterministic `internal/sim` replica harness now exists for applying real
replica commands across multiple in-memory engines, including lease changes and
closed-timestamp-gated follower reads, snapshot-style learner catch-up, and
promotion through the replicated membership state machine. A dedicated
`internal/observability` package now also exposes a real operator HTTP surface
with Prometheus metrics, `healthz`/`readyz`, pprof handlers, and a structured
overview endpoint. Broader simulation coverage and chaos validation are still
open.

## Ongoing Discipline

### [ ] Keep the Plan Ahead of the Code

Rule:

- any protocol or scope change must update `IMPLEMENTATION_PLAN.md` first
- every code commit must map to a phase or sub-phase
- tests for a behavior land in the same change as the behavior
- no mixed current-phase and future-phase implementation commits

## Remaining Execution Plan

### [ ] Phase 6 Remaining Execution

- [x] 6.1 Add single-table aggregate planning (`GROUP BY`, `COUNT`, `SUM`) and map it to distributed flow stages
- [x] 6.2 Add join-aware logical planning and distributed hash-join flow stages for supported equi-joins
- [x] 6.3 Add explicit flow-fragment boundaries and result schemas so distributed plans can move toward execution

### [ ] Phase 7 Remaining Execution

- [ ] 7.1 Push placement and home-region policy deeper into live routing decisions, not only descriptors and flow hints
- [ ] 7.2 Make leaseholder/follower read routing expose locality reasons and freshness gaps as first-class outputs
- [ ] 7.3 Thread locality policy through more replica movement and cache-refresh surfaces

### [ ] Phase 8 Remaining Execution

- [ ] 8.1 Expand deterministic simulation to cover split races, lease churn, and multi-range transaction recovery
- [ ] 8.2 Add snapshot-pressure and allocator-observability metrics around the new operator HTTP surface
- [ ] 8.3 Add a chaos/system-test harness skeleton for partitions, crash/restart, and ambiguous commit timing
