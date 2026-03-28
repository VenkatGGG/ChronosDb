# ChronosDB Architecture Plan

Status: Draft  
Scope: Architecture and implementation planning only. No code in this document.  
Audience: Builders of ChronosDB

`IMPLEMENTATION_PLAN.md` is the execution contract. This document explains the
target architecture and rationale that implementation must preserve.

## 1. Thesis

ChronosDB is a **geo-distributed, strictly serializable SQL database** built on a
replicated MVCC KV substrate.

This document plans against the **final target system**, not just the first
shippable slice. Delivery may still be phased, but the architecture itself is
designed for the end state:

- PostgreSQL-compatible SQL surface
- locality-aware data placement
- multi-range transactions with parallel commit
- leaseholder-local reads in the steady state
- follower historical reads via closed timestamps
- authoritative routing and liveness metadata
- online split, rebalance, recovery, and observability

ChronosDB may be delivered incrementally, but it should not be architected as a
throwaway KV prototype that later needs a conceptual rewrite.

## 2. Product Shape

### External product

ChronosDB’s end-state product surface is:

- PostgreSQL wire compatibility
- relational schemas and indexes
- strictly serializable transactions across ranges
- locality-aware placement for regional and multi-region workloads
- historical and follower-readable snapshots where the consistency model allows it
- distributed SQL execution for scans, joins, and aggregations

### Internal substrate

ChronosDB is internally a replicated MVCC KV store with:

- range-based sharding
- Raft replication
- authoritative meta ranges
- lease records and liveness records
- transaction records, intents, and lock-table state

The KV substrate is not the product, but it is the foundation everything else
depends on.

## 3. Goals

### End-state goals

- Strict serializability across ranges and regions under bounded clock skew
- No lost acknowledged writes
- Zone-failure survival by default; region-failure survival where configured
- Leaseholder-local reads on the steady-state fast path
- Follower historical reads using closed timestamps
- Online range split, merge, rebalance, lease transfer, and replica repair
- Distributed SQL execution over the KV substrate
- Explicit failure handling, debugability, and model-checkable invariants

### Non-goals

- Byzantine fault tolerance
- One universal latency SLO independent of placement policy
- Perfect compatibility with the entire PostgreSQL extension ecosystem
- Hidden safety tradeoffs in exchange for lower median latency

## 4. Design Principles

1. **Correctness before latency tricks**
   Start with the safest protocol that is easy to reason about. Optimize only after
   the invariants are stable.

2. **Authoritative metadata, never gossip-routed data**
   Range descriptors, liveness, and placement live in replicated system ranges.
   Discovery may be weakly consistent later; routing may not.

3. **Boring by default**
   Reuse proven components for storage, consensus, RPC, tracing, and testing.

4. **Explicit state machines over implicit behavior**
   Lease, transaction, intent, and replica lifecycles must be written down before
   they are encoded in code.

5. **Multi-region aware from the beginning**
   Delivery may be phased, but lease, routing, transaction, and locality
   primitives are designed for the final geo-distributed system.

## 5. Topology and SLOs

### Target deployment model

ChronosDB is designed for:

- 3-replica regional ranges for zone-failure survival
- 5-replica multi-region ranges where region-failure survival is required
- lease preferences and replica constraints based on locality
- configurable survival goals per database, table, or placement policy

### SLO framing

ChronosDB does not use one cluster-wide latency promise for all data. SLOs are
defined by placement class:

- **Regional data**: in-region leaseholder reads should target `< 10 ms` P99
- **Regional writes**: in-region quorum writes should target low double-digit ms
- **Follower historical reads**: local reads target closed-timestamp freshness lag
- **Multi-region writes**: latency is explicitly a function of survival goal and
  replica placement

### Placement classes

ChronosDB plans for distinct placement modes:

- **Regional**: quorum and leaseholder stay in one region
- **Home-region multi-region**: writes coordinate through a chosen home region
- **Global**: explicit, slower, placement-aware data model with clearer user tradeoffs

## 6. System Model

ChronosDB is designed for:

- crash-stop node failures
- delayed, dropped, duplicated, and reordered packets
- asymmetric network partitions
- process pauses
- disk stalls

ChronosDB is **not** designed for Byzantine behavior.

### Clock model

ChronosDB uses a hybrid logical timestamp structure. The architecture keeps this
as an explicit logical type; packed encodings are an implementation detail, not a
protocol dependency.

Nodes must operate under a bounded clock skew. If a node exceeds the configured
maximum offset, it fails closed:

- it stops serving lease-based reads
- it stops participating in new transaction timestamp assignment
- it reports itself unhealthy
- it may exit if the condition persists

Recovery after a clock violation is conservative:

- no automatic self-re-admission after a skew violation
- re-admission requires a fresh process start and join handshake
- node liveness epoch is bumped before the node may serve traffic again
- the violation path is treated as operator-mediated recovery, not a
  supervisor-driven auto-restart loop

ChronosDB prefers temporary unavailability over silent corruption.

## 7. High-Level Architecture

All nodes run the same binary. Logical roles emerge from lease ownership and
replica placement rather than from separate binaries.

```text
                    +----------------------+
                    |     KV / SQL Client  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Gateway / Session    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Range Cache          |
                    +----------+-----------+
                               |
                    meta lookup|
                               v
                    +----------------------+
                    | Meta Ranges          |
                    | (authoritative)      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Leaseholder Replica  |
                    +----------+-----------+
                               |
                     +---------+----------+
                     | Raft Replication   |
                     | (per range)        |
                     +---------+----------+
                               |
                               v
                    +----------------------+
                    | Pebble Storage       |
                    | MVCC + raft state    |
                    +----------------------+
```

### Key architectural choice

The **leaseholder and Raft leader are separate concepts**.

ChronosDB will strongly prefer co-location for the common case because it reduces
hops and keeps the write path tight, but correctness does not depend on permanent
identity between the two roles. Lease ownership is a replicated state-machine
concept; leadership is a Raft concept.

## 8. Node Anatomy

Each node contains the following major subsystems:

1. **RPC Layer**
   gRPC services for client KV requests, replica traffic, snapshots, and admin RPCs.

2. **Store**
   Owns one Pebble engine, local replicas, and range lifecycle operations.

3. **Replica Manager**
   Tracks in-memory replica state, applies raft commands, and coordinates snapshots.

4. **MultiRaft Scheduler**
   Runs `etcd/raft` `RawNode` instances via a shared tick and ready loop.

5. **Concurrency Manager**
   Owns lock-table state, intent observation, and transaction waiting behavior.

6. **Range Cache**
   Caches range descriptors learned from the authoritative meta ranges.

7. **Background Work**
   Compaction admission, snapshot sending, intent resolution, liveness heartbeats,
   GC, and rebalancing tasks.

### Background work priorities

Background work is divided into tiers from the replicated phase onward:

- **Critical**: liveness heartbeats, Raft progress, lease maintenance
- **Normal**: intent resolution, GC, range-cache repair
- **Background**: snapshot send, rebalance moves, non-urgent maintenance

Compaction is managed maintenance work: it must be rate-limited, but it cannot be
treated as indefinitely deferrable background noise.

When Pebble approaches write-stall conditions, compaction priority must escalate
out of the background tier. The Phase 2 spec must define explicit triggers using
Pebble pressure signals such as L0 file count, sublevel growth, pending
compaction debt, or direct write-stall proximity.

## 9. Core Technology Choices

### Storage engine

Use `github.com/cockroachdb/pebble`.

Why:

- mature LSM engine
- strong iterator support
- predictable compaction hooks
- operationally closer to the needs of MVCC and replicated state

### Storage layout

ChronosDB uses **one Pebble engine** with separate logical key
namespaces for Raft state and MVCC state.

```text
/raft/...  -> HardState, log entries, truncated state
/mvcc/...  -> user versions, intents, locks, range-local metadata
```

Pebble does not expose RocksDB-style column families, so the practical design is
prefix-separated namespaces inside one engine rather than "two column families."

Why this is the default architecture choice:

- one WAL
- one unified fsync path
- easier cross-range batching with a shared `WriteBatch`
- less operational complexity in the first system

If compaction interference or tuning conflicts become severe, ChronosDB may later
split Raft and MVCC state into separate engines. That is not the starting point.

### Consensus

Use `go.etcd.io/raft/v3`.

Why:

- mature Raft library
- clear separation between state machine and transport
- supports building a proper shared MultiRaft scheduler

### RPC

Use `grpc-go` with Protobuf.

Why:

- boring choice
- clear schema contracts
- strong ecosystem

### Discovery

Use static bootstrap for cluster formation. Do not add a gossip plane in the
current architecture. Revisit only if we later observe a concrete need for
faster non-authoritative dissemination of node and locality hints.

Why:

- bootstrap should stay boring and explicit
- an authoritative liveness model matters more than a fancy membership protocol
- routing and lease correctness cannot depend on a secondary hint plane
- extra membership machinery is not justified until measured latency or operability needs appear

Any gossip or memberlist-style mechanism is advisory only. Routing, leases,
liveness epochs, and placement truth live in replicated system state.

## 10. System Keyspace

ChronosDB reserves internal key prefixes for system state:

```text
/meta/       -> range descriptors and routing metadata
/liveness/   -> node liveness and epochs
/lease/      -> lease records and sequencing
/txn/        -> transaction records
/lock/       -> lock table entries and wait metadata
/raft/       -> hard state, log state, truncated state
/range/      -> range-local metadata
/user/       -> user KV data
```

The exact key encoding is a Phase 0 design task and must be written down before
implementation starts.

## 11. Core State Machines

The following state records are first-class architecture concepts:

### RangeDescriptor

```text
RangeDescriptor {
  range_id
  start_key
  end_key
  replicas[]
  generation
}
```

### ReplicaDescriptor

```text
ReplicaDescriptor {
  node_id
  store_id
  replica_id
}
```

### Lease

```text
Lease {
  holder_replica_id
  start_ts
  expiration_ts
  sequence          // epoch-bumped transfer barrier
}
```

### TxnRecord

```text
TxnRecord {
  txn_id
  status            // PENDING | STAGING | COMMITTED | ABORTED
  write_ts
  epoch
  anchor_range_id
  touched_ranges[]
}
```

### Intent

```text
Intent {
  txn_id
  key
  provisional_value
  provisional_ts
}
```

### NodeLiveness

```text
NodeLiveness {
  node_id
  epoch
  expiration
  draining
}
```

Every one of these objects must have an explicit state transition diagram before
ChronosDB reaches implementation.

`STAGING` is part of the target transaction architecture because ChronosDB plans
for parallel commit in the final system, even if early delivery phases initially
ship a simpler commit path.

## 12. Metadata and Routing

ChronosDB uses **meta ranges** as the authoritative directory for routing.

### Why this matters

Gossip is eventually consistent and is therefore unsuitable as the source of truth
for:

- range split visibility
- range movement visibility
- leaseholder routing
- transaction anchor resolution

### Routing flow

```text
client request
  -> local range cache
  -> cache miss
  -> meta range lookup
  -> cache fill
  -> target leaseholder
```

If the target range has moved or split, the server returns a retriable routing
error and the client refreshes from meta.

### Client retry contract

Every KV request carries a context deadline.

On routing errors such as:

- `RANGE_NOT_HERE`
- `RANGE_SPLIT`
- `LEADER_CHANGED`

the client:

1. invalidates the stale cache entry
2. refreshes from meta
3. retries until the request deadline expires

The first retry after a routing refresh should be immediate. Exponential backoff
with jitter applies only to repeated failures, not to the first descriptor refresh.

There is no fixed retry count; the deadline is the contract boundary.

### Range cache invalidation

ChronosDB uses:

- error-driven invalidation on routing failures
- a background TTL with jitter as a safety net

The exact TTL is a tuning parameter, not a permanent architecture constant.
ChronosDB does not rely on background topology polling for correctness.

## 13. Replication Model

ChronosDB partitions data into ranges. Each range is replicated by Raft.

### Initial range sizing

ChronosDB does **not** start with 64 MiB ranges.

Initial target range size:

- `256 MiB` lower bound target
- option to revisit to `512 MiB` after real benchmarks

Ranges may split based on:

- size
- sustained QPS
- hot-key or hot-range pressure

### Replica layout

By default:

- 3 replicas per regional range
- 5 replicas where region-failure survival is required
- one leaseholder per range
- quorum commit before acknowledging writes
- policy-driven preference to co-locate leaseholder and Raft leader

### Raft implementation note

ChronosDB will not create one independent goroutine-heavy raft stack per range.
It will run a shared MultiRaft scheduler using `RawNode`.

This keeps:

- scheduling explicit
- batching possible
- wakeups bounded
- fsync pressure visible

### Batched durability

On each scheduler tick, ChronosDB gathers all non-empty `Ready` states and writes
their HardState and log updates in a single Pebble `WriteBatch`.

```text
tick
  -> collect Ready from active ranges
  -> build one WriteBatch
  -> fsync once
  -> hand committed entries back to replica state machines
```

This is the core throughput argument for the shared MultiRaft scheduler.

## 14. Read Path

ChronosDB supports three read modes in the target architecture.

### Mode 1: leaseholder fast reads

The steady-state fast path is a local read on the current leaseholder without
round-tripping through `ReadIndex`.

This requires:

- validated lease ownership
- epoch-bumped lease transfer semantics via `Lease.sequence`
- bounded clock uncertainty
- read timestamp checks against the lease interval
- explicit stale-read tests around transfer and failover

### Mode 2: `ReadIndex` fallback

When lease validity is uncertain, immediately after leadership churn, or during
recovery, ChronosDB falls back to Raft `ReadIndex`.

`ReadIndex` is the safety fallback, not the intended steady-state fast path.

### Mode 3: follower historical reads

Follower reads are supported using closed timestamps or an equivalent replicated
freshness frontier.

These reads are:

- local to the follower
- bounded by closed-timestamp lag
- valid only for timestamps at or below the published closed frontier

### Lease transfer protocol

Lease transfer is a replicated lease-state transition, not merely a Raft
leadership change.

The target protocol is:

```text
old holder owns lease at sequence N
  -> transfer proposed
  -> new holder receives lease at sequence N+1
  -> old holder may not serve local reads at sequence N after observing N+1
  -> leader placement policy attempts to follow lease placement
```

Leadership transfer and lease transfer should usually be coordinated, but they
are not the same concept.

## 15. Write Path

Single-key and single-range writes follow the basic replicated flow:

```text
client
  -> leaseholder
  -> leader / raft propose
  -> quorum replicate
  -> apply to Pebble MVCC state
  -> commit-wait as needed for timestamp visibility ordering
  -> acknowledge client
```

Acknowledged writes must already be durable enough to survive any single-node loss
in the replica set.

## 16. Transaction Model

ChronosDB plans for a full distributed transaction model.

### Fast paths

- **One-phase commit** for single-range transactions that can commit in one round
- **Parallel commit** for multi-range transactions when all intents and the
  transaction record can be staged consistently

### General transaction machinery

Transactions use:

- transaction records anchored on one range
- write intents on participant ranges
- lock table / wait queues
- wound-wait for deadlock prevention
- transaction heartbeats
- refresh spans and retryable restart handling
- explicit intent resolution

### `STAGING` recovery

`STAGING` exists to support parallel commit safely.

Recovery may be driven by:

- an encountering transaction or reader
- an async resolver
- a GC or recovery worker

Decision rule:

- if all required intents are present and uncontested, `STAGING -> COMMITTED`
- if any required intent is missing, reverted, or irreconcilably conflicted,
  `STAGING -> ABORTED`

## 17. Concurrency Control

ChronosDB uses MVCC plus pessimistic intent tracking.

### Principles

- readers should not block all writers by default
- conflicting writes must serialize clearly
- waiting behavior must be explicit
- abandoned intents must be observable and cleanable

### Early policy

- write/write conflicts use intents plus a lock table
- wound-wait decides who aborts under contention
- long waits are surfaced in metrics and traces

## 18. Range Split and Rebalance

Range lifecycle events are system-critical operations, not background trivia.

### Split flow

```text
range exceeds threshold
  -> leaseholder proposes split trigger
  -> split descriptor written atomically
  -> meta ranges updated
  -> left and right ranges become routable
  -> clients refresh on mismatch
```

### Rebalance flow

```text
allocator chooses move
  -> add learner / snapshot
  -> catch up
  -> promote replica
  -> transfer lease if needed
  -> remove old replica
```

Only one major membership change per range should be in flight at a time for a
given range state machine.

### Allocator serialization

Allocator decisions are serialized per range.

The allocator path uses:

- a local per-range allocator lock to prevent duplicate work by the allocator loop
- replicated range generation checks in the Raft config-change state machine so
  stale allocator decisions cannot win

Each config-change proposal carries an expected range generation. Apply-time
validation rejects proposals whose expected generation does not match current
state.

The local lock is a convenience optimization. The replicated generation check in
the state machine is the correctness boundary.

## 19. Bootstrap and Liveness

ChronosDB uses a boring bootstrap story.

### Bootstrap

- static join addresses
- cluster ID created once
- node/store IDs persisted locally

### Liveness

Node liveness is stored in replicated system state, not inferred solely from
ephemeral heartbeats.

Liveness is used for:

- allocator decisions
- lease transfer safety
- decommissioning
- draining

## 20. SQL Layer Plan

The SQL layer is a first-class part of the target product.

### SQL architecture

```text
SQL client
  -> SQL gateway
  -> session / txn state
  -> parser
  -> binder / catalog lookup
  -> logical plan
  -> cost-based optimizer
  -> physical plan
  -> distributed flow planning
  -> KV operations over ranges
  -> merge / aggregate
```

### Parser choice

ChronosDB should target PostgreSQL semantics and wire compatibility.

Preferred options:

- `pg_query_go`
- another PostgreSQL-compatible parser front-end if needed

ChronosDB should not build a SQL parser from scratch unless every reuse option is
provably inadequate.

## 21. Observability

ChronosDB must be operable from day one.

### Required signals

- OpenTelemetry tracing
- structured logs with range and txn IDs
- pprof endpoints
- metrics for:
  - Raft ready/apply lag
  - lease transfers
  - intent counts
  - lock wait time
  - split and snapshot duration
  - Pebble compaction pressure
  - range cache miss rate

### First-class debug questions

Operators must be able to answer:

- Why did this read route to the wrong range?
- Why is this transaction waiting?
- Which range is hot?
- Which replica is behind?
- Which node is unhealthy and why?

### Error taxonomy

ChronosDB adopts an explicit error taxonomy in Phase 0.

Initial top-level buckets:

- **Retriable**: the request may succeed unchanged after refreshed metadata,
  leadership movement, or bounded waiting
- **Client / semantic**: the request is well-formed enough to parse, but the
  requested operation is invalid for current user-visible state or API semantics
- **Resource / policy**: the request is rejected because of cluster policy,
  safety, or temporary resource pressure rather than logical invalidity
- **Fatal**: local or replicated state is suspect enough that continuing risks
  corruption or invariant breakage

Exact error codes live in the Phase 0 spec, but every later phase must map to
this taxonomy.

Example classification:

- `QUOTA_EXCEEDED` is **Resource / policy**
- `KEY_NOT_FOUND` is **Client / semantic** only for APIs that require existence
- routing movement and leadership churn are **Retriable**
- corruption and invariant breaks are **Fatal**

## 22. Testing and Verification Strategy

ChronosDB needs multiple test layers.

### Layer 1: deterministic local tests

- MVCC correctness
- lock table behavior
- lease transition behavior
- intent cleanup

### Layer 2: simulation tests

Use a deterministic transport and clock where possible.

Test:

- message delays
- drops
- node crashes
- lease races
- split and rebalance races
- lease transfer under leadership change
- clock skew fail-closed behavior
- re-admission after skew violation
- pending intent recovery after coordinator failure

### Layer 3: integration tests

Real Pebble, real Raft, real RPCs inside one region.

### Layer 4: chaos / Jepsen-style testing

Add later, after the state machines stop moving weekly.

Jepsen is required eventually, but it is not the first correctness tool.

### Property-based invariants

Go property-based and simulation-driven invariants are primary test tools, not
optional polish.

At minimum, ChronosDB will check:

1. A read at timestamp `T` never returns a value from a transaction that became
   committed after `T`.
2. No two committed transactions can observe contradictory orderings for the same
   key history.
3. Once a write is committed and externally visible, later successful reads cannot
   return an older version unless the API explicitly requested historical time.

TLA+ is valuable, but it is not allowed to block Phase 0.

These invariants must be written down as Phase 0 artifacts in prose or
pseudocode, then mapped to executable simulation or property tests. They are not
allowed to exist only as intentions in planning text.

## 23. Delivery Phases Toward the Final Architecture

The phases below are delivery order, not a statement that the final architecture
is simpler than the target system described above.

### Phase 0: invariants and specs

Write:

- keyspace layout
- replica state machine
- lease state machine
- transaction state machine
- timestamp rules
- error taxonomy
- retry contract
- written invariants artifact with checkable statements
- closed-timestamp publication rules
- placement and survival-goal rules

### Phase 1: single-node storage core

Build:

- Pebble wrapper
- MVCC key encoding
- snapshots
- WAL recovery semantics
- intent representation

### Phase 2: single-range replication

Build:

- MultiRaft scheduler skeleton
- replicated range state
- lease records and epoch-bumped transfer semantics
- `ReadIndex` fallback path
- leaseholder-local fast reads
- replicated writes
- commit-wait ordering
- leader/leaseholder co-location policy

### Phase 3: metadata and routing

Build:

- meta ranges
- range cache
- routing refresh
- liveness records and epochs
- range split
- range membership changes
- no advisory gossip in the current plan; reconsider only if justified by measured need

### Phase 4: transaction core

Build:

- lock table
- contention handling
- retries
- clear client-visible errors
- one-phase commit fast path
- refresh-span logic

### Phase 5: multi-range transactions

Build:

- transaction records
- distributed intent tracking
- coordinator recovery
- wound-wait integration
- `STAGING` recovery
- parallel commit

### Phase 6: SQL front door

Build:

- PostgreSQL wire protocol
- parser integration
- catalog descriptors
- binder and semantic analysis
- cost-based planning skeleton
- logical to KV mapping
- point lookup support
- range scan support
- joins, aggregations, and distributed flow planning

### Phase 7: locality and follower reads

Build:

- closed-timestamp publication
- follower historical reads
- lease preferences
- placement classes
- home-region semantics

### Phase 8: hardening and operability

Build:

- admission control
- better balancing
- snapshot tuning
- observability dashboards
- large-scale simulation
- chaos and Jepsen testing

Current evidence in the repo:

- `internal/systemtest/local_controller.go` provides a live in-process cluster controller with real pgwire and observability listeners
- `internal/systemtest/external_controller.go` provides a process-backed controller that launches separate child node processes
- `cmd/chronos-node/main.go` and `cmd/chronos-chaos-runner/main.go` provide the simple external node and chaos-runner binaries
- `internal/systemtest/artifacts.go` persists versioned scenario artifacts
- `internal/systemtest/assertions.go` validates retained artifacts for write visibility, follower-read freshness, `STAGING` outcomes, and metadata monotonicity
- `internal/systemtest/matrix.go` executes the first built-in local fault matrix and writes `fault-matrix.json` plus per-scenario artifacts
- `internal/systemtest/handoff.go` and `docs/systemtest/EXTERNAL_HANDOFF.md` define the external-runner handoff contract
- `docs/operations/DASHBOARDS.md` and `docs/operations/RUNBOOKS.md` define the operator-facing Phase 8 view

Architectural decision:

- the in-repo Phase 8 closure plan is complete
- the top-level Phase 8 program is complete for the current project scope because the repo now contains a simple external process runner and tested process-backed fault-matrix execution
- a full Jepsen implementation remains a future extension rather than a current architectural blocker

## 24. Open Decisions

These questions remain open even in the final-version plan:

1. What exact uncertainty-window and commit-wait rules establish strict
   serializability under bounded clock skew?
2. What closed-timestamp publication cadence and lag budget best balance follower
   read freshness against write-path overhead?
3. What schema-placement abstraction best exposes regional, home-region, and global
   placement to SQL users without leaking too much storage detail?
4. What benchmark harness defines "hot range" and triggers load-based splitting?
5. At what scale do Raft and MVCC state need to split into separate Pebble engines
   for tuning isolation?

## 25. Current Architectural Call

If ChronosDB is planned as the final system from day one, the architecture to
plan against is:

- PostgreSQL-facing SQL database on top of a replicated MVCC KV layer
- Pebble storage with logical namespaces for Raft and MVCC state
- `etcd/raft` `RawNode` with shared MultiRaft batching
- authoritative meta ranges and liveness ranges
- separate leaseholder and Raft leader concepts with co-location by policy
- leaseholder fast reads in the steady state, `ReadIndex` as fallback
- follower historical reads via closed timestamps
- parallel commit with `STAGING` in the transaction model
- locality-aware placement classes and multi-region survival goals

That is the architecture to plan against, even if delivery still happens in phases.
