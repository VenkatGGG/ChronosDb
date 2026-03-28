# ChronosDB Implementation Plan

Status: Execution Contract  
Scope: This file is the implementation contract for the repository.  
Rule: If code needs behavior not already covered here, `IMPLEMENTATION_PLAN.md`
must be updated in a docs commit before the code change lands.

## 1. Summary

ChronosDB is a geo-distributed, strictly serializable SQL database built on a
replicated MVCC KV substrate.

The final target system includes:

- PostgreSQL wire compatibility
- locality-aware placement for regional and multi-region workloads
- leaseholder-local fast reads
- follower historical reads via closed timestamps
- multi-range transactions with parallel commit
- authoritative meta ranges and liveness ranges
- online split, rebalance, recovery, and distributed SQL execution

Delivery is phased, but the architecture is not provisional. Code must evolve
toward the final system described in `ARCHITECTURE.md`, not toward a temporary
throwaway MVP.

## 2. Repository Rules

Before any `.go`, `.proto`, or generated code lands:

- `ARCHITECTURE.md` remains the rationale and high-level design source.
- `IMPLEMENTATION_PLAN.md` is the delivery and execution contract.
- `TODOS.md` tracks the next concrete docs and implementation milestones.
- `README.md` is the public architecture baseline for the remote repository.

Implementation guardrails:

- No code may bypass the written lease, transaction, routing, or error contracts.
- No code commit may introduce work outside the current planned phase.
- Every code commit message must reference the exact phase item(s) it closes.
- Any protocol or scope change requires an `IMPLEMENTATION_PLAN.md` update first.
- Docs-only commits are required before architectural changes in code.
- No mixed commits that combine unrelated future-phase work with current-phase work.
- Push commits directly to `main` unless the user explicitly requests a PR-based flow.

## 3. Canonical Subsystem Boundaries

ChronosDB is implemented as the following subsystems. Code must respect these
boundaries and should not leak lower-layer details upward.

### 3.1 Storage / Pebble

Responsibilities:

- own the Pebble engine
- encode MVCC and system keys
- persist Raft state and MVCC state in logical namespaces
- expose snapshots, batch writes, and recovery hooks

Must not own:

- lease policy
- routing policy
- SQL semantics

### 3.2 MultiRaft Scheduler

Responsibilities:

- host shared `etcd/raft` `RawNode` instances
- drain `Ready` state
- build the per-tick shared `WriteBatch`
- pace fsync, apply, and snapshot handoff

Must not own:

- SQL/session behavior
- transaction retry policy

### 3.3 Replica State Machine

Responsibilities:

- apply replicated commands
- own range-local metadata
- enforce range generation checks on config changes
- coordinate split, rebalance, learner promotion, and removal

Must not own:

- client retry loops
- SQL planning

### 3.4 Meta + Routing

Responsibilities:

- authoritative range descriptors
- authoritative leaseholder targeting data
- range cache fill and invalidation contract
- liveness records and epochs

Must not depend on:

- gossip as correctness source

### 3.5 Lease System

Responsibilities:

- lease records and lease transfer protocol
- `Lease.sequence` epoch barrier semantics
- leaseholder fast-read eligibility
- `ReadIndex` fallback boundary

Must not be collapsed into:

- Raft leadership identity

### 3.6 Transactions

Responsibilities:

- `TxnRecord`
- intents
- lock table
- wound-wait
- retries and refresh spans
- `STAGING` and parallel commit recovery

Must not be reinvented inside:

- SQL executor
- ad hoc RPC handlers

### 3.7 Closed Timestamps

Responsibilities:

- publish closed timestamp frontiers
- gate follower historical reads
- surface freshness lag and publication health

Must not be approximated by:

- arbitrary stale reads
- lease expiry guesses

### 3.8 SQL Gateway / Planner

Responsibilities:

- PostgreSQL wire protocol
- session state
- parser integration
- binder/catalog
- logical and physical planning
- distributed flow planning

Must not bypass:

- KV routing contracts
- transaction machinery

## 4. Interfaces and Types Frozen Before Code

### External interface

- PostgreSQL wire protocol
- internal KV/admin RPCs may exist for bootstrap and testing, but SQL is the
  long-term product interface

### Internal protocol records

- `RangeDescriptor`
- `ReplicaDescriptor`
- `Lease`
- `TxnRecord`
- `Intent`
- `NodeLiveness`
- closed timestamp publication record

### Frozen semantics

- `Lease.sequence` is the epoch-bumped transfer barrier
- `TxnRecord.status` includes `PENDING`, `STAGING`, `COMMITTED`, `ABORTED`
- routing truth comes from meta ranges, not gossip
- config-change proposals carry an expected range generation
- compaction escalation is required when Pebble approaches write-stall conditions
- follower reads are historical and bounded by the published closed timestamp

### Error classes

- `Retriable`
- `Client/Semantic`
- `Resource/Policy`
- `Fatal`

## 5. Delivery Phases

These phases are the implementation order. They do not redefine the target
architecture.

### Phase 0: Specs and invariants

Deliver:

- spec artifacts under `specs/phase0/`
- keyspace layout
- replica state machine
- lease state machine
- transaction state machine
- timestamp rules
- retry contract
- error taxonomy
- closed timestamp publication rules
- placement and survival-goal rules
- written invariant statements with executable test mapping

Definition of done:

- an implementer can write Phase 1-2 code without inventing protocol behavior
- lease transfer, `STAGING`, routing retry, and error classes are decision complete

### Phase 1: Single-node storage core

Deliver:

- Pebble wrapper
- MVCC key encoding
- logical namespaces for `/raft/...` and `/mvcc/...`
- snapshots and recovery semantics
- intent representation

Definition of done:

- storage operations are testable without Raft or SQL
- store-versioning hooks exist before format evolution begins

### Phase 2: Single-range replication and fast reads

Deliver:

- shared MultiRaft scheduler
- batched `WriteBatch` persistence
- replica apply loop
- lease records and epoch-bumped transfer protocol
- leaseholder-local fast reads
- `ReadIndex` fallback
- commit-wait ordering
- compaction escalation triggers

Definition of done:

- one range can replicate, fail over, transfer lease, and read safely
- the scheduler owns batching and durability

### Phase 3: Meta, routing, and membership

Deliver:

- meta ranges
- range cache
- routing refresh and invalidation
- liveness records and epochs
- split triggers
- config-change generation checks
- learner/snapshot/rebalance flow
- no advisory gossip in the current plan; revisit only if a measured hint-dissemination need appears

Definition of done:

- routing survives splits and movements without gossip as truth
- allocator decisions cannot win with stale generation state

### Phase 4: Transaction core

Deliver:

- lock table
- contention handling
- wound-wait
- retryable restart handling
- refresh spans
- one-phase commit fast path
- client-visible retry/error mapping

Definition of done:

- single-range transactions are safe and debuggable
- transaction semantics are owned centrally, not by callers

### Phase 5: Multi-range transactions

Deliver:

- anchored `TxnRecord`
- distributed intents
- coordinator recovery
- async intent resolution
- `STAGING` recovery
- parallel commit

Definition of done:

- coordinator death and encountering-request recovery are deterministic
- `STAGING` resolution obeys the written state-machine rule

### Phase 6: SQL front door

Deliver:

- PostgreSQL wire protocol
- parser integration
- catalog descriptors
- binder and semantic analysis
- cost-based planning skeleton
- logical-to-KV mapping
- distributed flow planning for scans, joins, and aggregations

Definition of done:

- SQL uses the KV, routing, and transaction substrate rather than bypassing it

### Phase 7: Locality and follower reads

Deliver:

- closed timestamp publication
- follower historical reads
- lease preferences
- placement classes
- home-region semantics

Definition of done:

- follower reads are freshness-bounded and observable
- placement policy is expressible without leaking raw storage mechanics to users

### Phase 8: Hardening and operability

Deliver:

- admission control
- better balancing
- snapshot tuning
- observability dashboards
- large-scale simulation
- chaos and Jepsen testing

Definition of done:

- the system can be operated, profiled, and failure-tested at realistic scale

## 6. Acceptance Gates

Before any code commit is allowed:

- `origin/main` contains the docs-only baseline commits
- `README.md` renders correctly on GitHub, including Mermaid diagrams
- `IMPLEMENTATION_PLAN.md` is detailed enough that an implementer does not choose
  protocol behavior themselves
- `TODOS.md` does not conflict with `ARCHITECTURE.md`
- no `.go`, `.proto`, or generated source files have been added

During implementation:

- each commit maps to one phase or sub-phase
- no mixed docs/code/future-phase commit bundles
- any architecture change lands as a docs commit before the code change

## 7. Commit Policy

Initial repository history:

1. docs: add implementation plan and align planning docs
2. docs: add architecture overview and diagrams to readme

After that:

- docs-only commits come before protocol changes
- code commits are phase-scoped
- test additions land in the same commit as the behavior they validate
- agent-generated changes push directly to `main` unless the user explicitly requests a PR-based flow
- commits stay incremental and scoped to one coherent change
- plan and protocol updates land before code that depends on them

## 8. Assumptions and Defaults

- `origin` is `https://github.com/VenkatGGG/ChronosDb`
- pushes go directly to `main` unless the user explicitly requests a PR-based flow
- the current local docs seed the remote history
- docs-first is mandatory
- GitHub Actions is the default CI when implementation begins
- licensing is intentionally left undecided until explicitly chosen
