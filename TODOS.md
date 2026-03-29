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
- [x] optional advisory gossip dissemination decision: do not implement for now

Exit criteria:

- routing remains correct through split and rebalance
- stale allocator decisions cannot win

**Status:** Core deliverables are complete. `internal/meta`, `internal/routing`,
authoritative meta1/meta2 layout bootstrapping, cache-backed routing,
generation checks, split-trigger application, rebalance-safe membership
transitions, and snapshot image installation are implemented. Optional advisory
gossip dissemination has now been explicitly rejected for the current plan
because correctness does not depend on it and the measured need for a separate
hint plane is not yet strong enough to justify the extra operational surface.

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

### [x] Phase 5: Multi-Range Transactions

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

### [x] Phase 7: Locality and Follower Reads

Deliver:

- closed timestamp publication
- follower historical reads
- lease preferences
- placement classes
- home-region semantics

Exit criteria:

- follower reads are freshness-bounded and observable
- placement policy is user-expressible and internally consistent

**Status:** Core deliverables are complete. The closed-timestamp publication core and follower-read
eligibility checks now exist as a dedicated package, including monotonic
publication, lease-sequence invalidation, intent-backlog stalling, and
fail-closed behavior under clock-offset violations. Placement policy
normalization and allocator-facing compilation now exist as a dedicated package
for `REGIONAL`, `HOME_REGION`, and `GLOBAL` declarations, and leaseholder
selection can now follow compiled region preferences. Replica-local routing can
also choose between follower-historical and leaseholder reads based on closed
timestamp safety. SQL table descriptors, distributed flow stages, and range
descriptors now carry validated placement policy and home-region hints. Closed
timestamp publications can now also be
encoded and persisted through the storage engine under the global system
namespace, and the replica state machine now applies lease-bound closed
timestamp publications as live replica state and can serve exact historical
reads when the closed-timestamp proof permits it. Live routing decisions now
also consult descriptor placement policy instead of treating locality as
descriptor-only metadata. Historical read routing exposes local vs.
leaseholder region, the chosen target region, preferred-region alignment, the
closed/applied frontier, and an explicit freshness gap when a follower read
must fall back to the leaseholder. The resolver/refresh path now also returns
placement and home-region hints alongside authoritative descriptors, and
rebalance decisions now preserve locality intent as explicit output instead of
hiding it inside a load score.

### [x] Phase 8: Hardening and Operability

Deliver:

- admission control
- better balancing
- snapshot tuning
- observability dashboards
- large-scale simulation
- chaos and Jepsen testing

Exit criteria:

- the system can be operated, profiled, and failure-tested at realistic scale

**Status:** Complete for the current project scope. The repo now contains a
simple external process runner that launches real child node processes, applies
the exported fault steps, and retains artifacts outside the in-process local
harness. A dedicated admission controller now exists with
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
overview endpoint. The deterministic simulator now also covers stale
generation rejection after split triggers and repeated lease churn with stale
closed-timestamp publication rejection. The operator metrics surface now also
exposes explicit snapshot-pressure and allocator-decision signals alongside the
earlier Raft, split, compaction, and routing metrics. The simulator also now
has a deterministic multi-range coordinator-recovery harness for `STAGING`
transactions observed across participant ranges. A typed
`internal/systemtest` harness now also exists for partition, crash/restart,
wait, and ambiguous-commit scenarios against a pluggable cluster controller.
That harness now also has a built-in catalog of canonical partition, restart,
and ambiguous-commit recovery scenarios. Runner executions now also emit
structured reports and support post-step and post-run assertion hooks. The repo
now also includes `cmd/chronos-node`, `cmd/chronos-chaos-runner`,
`ExternalProcessController`, and helper-process-backed tests that execute the
fault matrix against separate OS processes while retaining artifacts and
`handoff.json` bundles. A full Jepsen implementation remains future expansion,
not a blocker for this tracked phase.

## Ongoing Discipline

### [ ] Keep the Plan Ahead of the Code

Rule:

- any protocol or scope change must update `IMPLEMENTATION_PLAN.md` first
- every code commit must map to a phase or sub-phase
- tests for a behavior land in the same change as the behavior
- no mixed current-phase and future-phase implementation commits

## Remaining Execution Plan

### [x] Phase 6 Remaining Execution

- [x] 6.1 Add single-table aggregate planning (`GROUP BY`, `COUNT`, `SUM`) and map it to distributed flow stages
- [x] 6.2 Add join-aware logical planning and distributed hash-join flow stages for supported equi-joins
- [x] 6.3 Add explicit flow-fragment boundaries and result schemas so distributed plans can move toward execution

### [x] Phase 7 Remaining Execution

- [x] 7.1 Push placement and home-region policy deeper into live routing decisions, not only descriptors and flow hints
- [x] 7.2 Make leaseholder/follower read routing expose locality reasons and freshness gaps as first-class outputs
- [x] 7.3 Thread locality policy through more replica movement and cache-refresh surfaces

### [x] Phase 8 Remaining Execution

- [x] 8.1 Expand deterministic simulation to cover split races, lease churn, and multi-range transaction recovery
- [x] 8.2 Add snapshot-pressure and allocator-observability metrics around the new operator HTTP surface
- [x] 8.3 Add a chaos/system-test harness skeleton for partitions, crash/restart, and ambiguous commit timing

### [x] Phase 8 Follow-On Execution

- [x] 8.4 Add a built-in catalog of canonical chaos scenarios for partition, crash/restart, and ambiguous-commit recovery
- [x] 8.5 Add structured execution reports and assertion hooks to the system-test runner
- [x] 8.6 Add a Jepsen/chaos handoff manifest format so external fault runners can consume the same scenarios

## Completion Plan For Remaining Open Work

This section began as a planning artifact. It now records the implemented Phase 8
closure work and the explicit remaining gap for the still-open top-level Phase 8
checkbox.

### [x] 9. Phase 8 Closure Plan

- [x] 9.1 Implement a real cluster-backed `internal/systemtest` controller
  Status: complete via `LocalController`, which now starts real pgwire and observability listeners per node, supports crash/restart and partition/heal controls, and provides one-shot ambiguous-commit fault injection against the live query path.
  so manifests and built-in scenarios can run against an actual multi-process
  ChronosDB cluster instead of only a recording stub
- [x] 9.2 Add a persistent run artifact format for chaos runs
  Status: complete via `RunArtifacts`, which now persists `manifest.json`, `report.json`, `summary.json`, and per-node `node-logs/node-<id>.json` files, with `LocalController` exposing structured node-event logs for bundle export.
  including scenario manifest, structured runner report, per-node logs, and a
  final pass/fail summary that can be attached to CI or retained for manual review
- [x] 9.3 Define and implement assertion packs for external correctness checks
  Status: complete via artifact-level correctness assertions for acknowledged-write visibility, follower-read freshness, deterministic `STAGING` outcomes, and lease/descriptor monotonicity, plus standardized node-log markers for external runners to emit.
  including no acknowledged-write loss, no stale follower read beyond closed
  timestamp, deterministic `STAGING` recovery outcome, and lease/descriptor
  generation monotonicity under churn
- [x] 9.4 Execute a first fault matrix over the real cluster controller
  Status: complete via `ExecuteFaultMatrix`, which now runs the built-in Phase 8 scenario set over fresh `LocalController` instances, validates each persisted artifact bundle with the correctness assertion pack, and writes a root `fault-matrix.json` plus per-scenario artifact directories.
  covering minority partition, majority partition, crash during lease transfer,
  crash during learner snapshot catch-up, crash during `STAGING`, ambiguous
  commit response loss, and split/rebalance during concurrent traffic
- [x] 9.5 Integrate an external Jepsen/chaos runner handoff path
  Status: complete via `handoff.json` export per scenario artifact directory, `BuildHandoffBundle`/`WriteHandoffBundle`, and documented action-to-operation mapping in `docs/systemtest/EXTERNAL_HANDOFF.md`.
  so exported manifests can be consumed by the external fault toolchain with a
  documented mapping from manifest steps to fault injector operations
- [x] 9.6 Add operator-facing observability dashboards and runbooks
  Status: complete via `docs/operations/DASHBOARDS.md`, `docs/operations/RUNBOOKS.md`, and the observability metric additions for retry pressure and recovery outcomes.
  for snapshot pressure, allocator decisions, closed timestamp lag, lease
  churn, retry/error rates, and recovery outcomes so Phase 8 is operationally
  complete instead of only code-complete
- [x] 9.7 Close Phase 8 with evidence
  Status: complete. Evidence now lives in `internal/systemtest/matrix_test.go`,
  `internal/systemtest/handoff.go`, `docs/systemtest/EXTERNAL_HANDOFF.md`,
  `docs/operations/DASHBOARDS.md`, and `docs/operations/RUNBOOKS.md`. Final
  decision: the closure plan is complete, but the top-level Phase 8 checkbox
  remains open until an external Jepsen/chaos runner consumes the exported
  `handoff.json` contract and produces retained run artifacts outside the local
  harness.
  by updating `README.md`, `ARCHITECTURE.md`, and this file with the executed
  fault matrix, evidence locations, observed gaps, and the final decision on
  whether Phase 8 can be marked complete

### [ ] 10. Deferred Optional Work

- [x] 10.1 Decide whether advisory gossip dissemination is still worth doing
  Status: decided no for the current architecture. ChronosDB should stay on
  static bootstrap plus authoritative replicated metadata and liveness. Revisit
  only if a measured need appears for faster non-authoritative locality or
  topology hint fanout.
  now that correctness, routing truth, and placement all depend on authoritative
  metadata instead of gossip
- [ ] 10.2 If that decision changes, implement advisory gossip strictly as a hint plane
  for liveness suspicion and topology hints only, with an explicit guarantee
  that it cannot override meta-range truth or lease/routing decisions
- [ ] 10.3 If that decision changes, add validation and failure tests for advisory gossip
  covering stale hints, GC-pause false suspicion, and disagreement with
  authoritative metadata so the feature stays non-authoritative by construction

### [x] 11. Cluster Console and Real-Time Operations UI

Deliver:

- typed admin API contracts
- node-level admin read endpoints
- cluster snapshot aggregator
- live event stream
- authoritative range placement API
- key location lookup
- operator web UI
- scenario and artifact viewer

Exit criteria:

- an operator can see nodes, ranges, replicas, leaseholders, placement, and
  recent operations in real time from a single UI
- the UI answers "where does key K live?" from authoritative metadata instead
  of inference
- the UI can drill into scenario runs and retained artifacts

**Status:** Complete. The frontend shell now exists as a real React/TypeScript
console app with overview, nodes, ranges, and events surfaces, and
`chronos-console` can optionally serve the built UI with SPA fallback. The
range placement surface now also visualizes leaseholder and replica residency
from merged range descriptors, and the console now supports key-location lookup
that drives the placement drilldown from the containing range. Retained
scenario browsing is also exposed through the console API and UI, including
manifest/report/handoff surfaces and recent node-log evidence.

### [x] Phase 11 Remaining Execution

- [x] 11.1 Freeze admin API contracts and typed view models for nodes, ranges, replicas, key location, cluster snapshots, and events
- [x] 11.2 Add node-level admin read endpoints for node summary, range inventory, and recent events
- [x] 11.3 Add a cluster snapshot aggregator service that polls nodes and exposes a unified API
- [x] 11.4 Add an SSE event stream for cluster operations and scenario activity
- [x] 11.5 Add a frontend shell with overview, nodes, ranges, and events pages
- [x] 11.6 Add range placement and leaseholder visualization from authoritative descriptors
- [x] 11.7 Add key-location lookup and placement drilldown
- [x] 11.8 Add scenario/artifact browsing for retained fault runs

### [x] 12. Console Topology and Operational Forensics

Deliver:

- topology graph and placement summary APIs
- node drilldown API with hosted ranges, lease ownership, and recent related events
- range drilldown API with replica residency, lease state, placement policy, and related events
- event-correlation views keyed by node and range
- console drilldown pages and deep links for node/range investigation
- retained-run correlation surfaces so scenario artifacts can jump into affected nodes and ranges

Exit criteria:

- an operator can move from the cluster overview to a specific node or range
  and understand current ownership, residency, and recent changes without
  leaving the console
- topology and drilldown views are derived from authoritative descriptors and
  live node state instead of UI-side inference

**Status:** Complete. Phase 12 now exposes topology summary, node/range
drilldown APIs, correlated recent events, deep-linkable drilldown pages,
retained-scenario live correlation, and route-level UI tests. The console can
now move directly from cluster posture to node/range investigation and back
through retained scenario evidence without requiring UI-side inference.

### [x] Phase 12 Remaining Execution

- [x] 12.1 Freeze topology and drilldown view contracts plus HTTP endpoints
- [x] 12.2 Add backend topology summary, node detail, and range detail APIs
- [x] 12.3 Correlate recent events by node and range on the backend
- [x] 12.4 Add console topology and drilldown pages with deep-linkable routes
- [x] 12.5 Link retained scenario artifacts to affected nodes and ranges in the console
- [x] 12.6 Add focused tests for topology merges, drilldown correctness, and deep-link behavior

### [ ] 13. Live Data Plane and Integrated Runtime

Deliver:

- a real node runtime that opens Pebble, bootstraps store identity, hosts live
  replicas, and runs MultiRaft in the normal process path
- inter-node Raft transport so separate `chronos-node` processes actually
  replicate state instead of only exposing local demo shells
- authoritative bootstrap of meta ranges, liveness, user ranges, replica
  assignments, and initial leaseholders
- a real KV execution path for point gets, scans, puts, intents, and
  transaction records against the live replicated substrate
- a SQL execution path that runs physical flow operators instead of only
  planning them and returns real rows over pgwire
- persistent catalog and descriptor storage so table metadata is not hardcoded
  in the demo runtime
- restart and recovery behavior that reconstructs stores, hosted replicas,
  leases, and descriptor state from disk and cluster metadata
- background workers that make split, rebalance, lease, liveness, and closed
  timestamp machinery part of the live runtime instead of library-only logic
- a seeded demo bootstrap that makes range placement and live query execution
  visible in the console and through `psql`

Exit criteria:

- a fresh 3-node cluster can be bootstrapped from the CLI without manual code
  wiring and exposes real ranges/replicas in the console
- `psql` can `INSERT` and `SELECT` real rows through the distributed runtime
  instead of only receiving planner metadata and command tags
- data survives process restart and remains queryable after a node crash/restart
- range placement, leaseholders, and key location in the console are sourced
  from the live replicated runtime rather than static node config

**Status:** In progress. The runtime-backed `chronos-node`, persistent
bootstrap, live inter-node Raft transport, descriptor-backed hosting, point KV
reads/writes, and the first real pgwire-backed point `INSERT`/`SELECT` path now
exist. Remaining work is the broader KV/transaction surface, distributed SQL
execution, persisted catalog descriptors, background services, recovery
hardening, and a one-command seeded demo bootstrap.

### [ ] Phase 13 Remaining Execution

- [x] 13.1 Replace the planner-only `chronos-node` shell with a real runtime assembly layer
  that opens the storage engine, bootstraps store identity, constructs hosted
  replicas, and wires MultiRaft, leases, metadata, and observability together
- [x] 13.2 Add a persistent node/store bootstrap path
  that creates cluster/store identities, bootstraps meta ranges, and records
  the first authoritative range descriptors and leaseholders on disk
- [x] 13.3 Add real inter-node Raft transport and message handling
  so outbound `ProcessReady` messages are delivered across processes and
  inbound messages are stepped into the correct local groups
- [x] 13.4 Replace static `ProcessNodeConfig.Ranges` with live descriptor-backed hosting
  so node/range views come from the actual replicated runtime instead of
  synthetic local config
- [ ] 13.5 Build the KV request path
  for point lookup, range scan, put, intent write, intent resolution, and txn
  record operations against live replicas and the MVCC storage engine
  - point lookup, point put, and descriptor-aware range scan are now live
    through runtime-hosted replicas and node control RPCs
  - the replicated substrate for intent writes/deletes and durable txn record
    storage now also exists, including a real system-span bootstrap range and
    local runtime helpers for intent and txn record proposals
- [ ] 13.6 Wire the transaction coordinator into the live KV path
  including begin/heartbeat/commit/abort, lock acquisition, refresh/retry, and
  coordinator recovery in the normal request flow
- [ ] 13.7 Build the first real SQL executor slice
  that can run point lookups and inserts end-to-end through pgwire, the planner,
  KV routing, leases, transactions, and storage, returning real rows/results
  - point `SELECT`, simple range-scan `SELECT`, and `INSERT` now execute through
    the live runtime and return real rows/results; transaction wiring and
    broader operator coverage remain
- [ ] 13.8 Extend SQL execution to distributed scans, aggregates, and joins
  by executing the physical flow operators built in `internal/sql/flow.go`
  across leaseholders and gateway merge stages
- [ ] 13.9 Persist catalog and SQL descriptors in the cluster
  so the demo no longer depends on the hardcoded `users` and `orders` tables in
  the systemtest catalog bootstrap path
- [ ] 13.10 Promote background subsystem logic into live services
  for liveness heartbeats, lease maintenance, closed timestamp publication,
  split triggers, allocator decisions, learner snapshot catch-up, and rebalance
- [ ] 13.11 Implement restart and recovery wiring
  so a restarted node reopens its engine, reconstructs hosted groups, reloads
  descriptors and applied indexes, rejoins the cluster, and resumes serving
- [ ] 13.12 Add a real seeded demo/bootstrap command
  that starts a 3-node cluster with pre-seeded range descriptors, visible range
  placement in the console, and a repeatable `psql` smoke test for `INSERT` and
  `SELECT`
