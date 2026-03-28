# ChronosDB

ChronosDB is a geo-distributed, strictly serializable SQL database built on a
replicated MVCC KV substrate.

Implementation is underway. The repository already contains:

- the Phase 0 protocol spec pack
- the Phase 1 Pebble-backed storage core
- the Phase 2 single-range replication and lease/read foundations
- the first Phase 3 meta and routing primitives

The repo still follows a plan-first discipline: protocol and scope changes
should be written down before the corresponding code lands.

## Why ChronosDB Exists

ChronosDB aims to combine:

- SQL as the product interface
- strict serializability across ranges
- locality-aware placement for regional and multi-region workloads
- leaseholder-local fast reads in the steady state
- follower historical reads via closed timestamps
- online split, rebalance, repair, and distributed SQL execution

The design is explicit about the hard parts:

- leaseholder and Raft leader are **different concepts**
- routing truth comes from **meta ranges**, not gossip
- follower reads are **historical and freshness-bounded**, not arbitrary stale reads

## Core Guarantees

- No lost acknowledged writes
- Strict serializability under bounded clock skew
- Zone-failure survival by default; region-failure survival where configured
- Routing based on authoritative replicated metadata
- Parallel-commit-capable transaction model with `STAGING`
- Closed-timestamp-gated follower historical reads

## Detailed Architecture Diagram

```mermaid
flowchart LR
    C["Clients"] --> G["SQL Gateway / Session Layer"]
    G --> P["Parser / Binder / Optimizer"]
    P --> TC["Transaction Coordinator"]
    G --> RC["Range Cache"]
    RC --> MR["Meta Ranges\n(authoritative routing + placement)"]

    TC --> LH["Leaseholder"]
    LH -. "policy prefers co-location" .- RL["Raft Leader"]
    RL --> RF["Raft Followers"]
    RL --> PB["Pebble Storage\nMVCC + Raft logical namespaces"]

    CT["Closed Timestamp Publisher"] --> LH
    CT --> RF

    AR["Allocator / Rebalancer"] --> MR
    AR --> RL
    AR --> RF

    O["Observability Plane\nmetrics / tracing / profiling"] -.-> G
    O -.-> TC
    O -.-> LH
    O -.-> RL
    O -.-> PB
```

## Request Flow Summary

### Write / Transaction Path

```mermaid
sequenceDiagram
    participant Client as SQL Client
    participant Gateway as SQL Gateway
    participant Planner as Parser/Binder/Optimizer
    participant Txn as Transaction Coordinator
    participant Cache as Range Cache
    participant Meta as Meta Ranges
    participant Lease as Leaseholder
    participant Leader as Raft Leader
    participant Follower as Raft Followers
    participant Record as TxnRecord / Intents

    Client->>Gateway: SQL write request
    Gateway->>Planner: parse / bind / plan
    Gateway->>Txn: begin or join transaction
    Txn->>Cache: resolve participant ranges
    Cache->>Meta: descriptor lookup on miss
    Meta-->>Cache: authoritative descriptors
    Txn->>Lease: write intents on participant ranges
    Lease->>Leader: propose replicated command
    Leader->>Follower: replicate to quorum
    Follower-->>Leader: quorum ack
    Leader-->>Lease: command committed
    Txn->>Record: update txn state (PENDING / STAGING / COMMITTED)
    Txn->>Lease: resolve or finalize intents
    Txn-->>Gateway: commit outcome
    Gateway-->>Client: SQL result
```

### Read Path

```mermaid
flowchart TD
    A["Read request"] --> B["Range cache lookup"]
    B --> C["Meta range lookup on miss"]
    C --> D["Target replica set"]

    D --> E{"Read mode"}
    E -->|Steady state| F["Leaseholder fast read"]
    E -->|Lease uncertainty / churn| G["ReadIndex fallback through Raft"]
    E -->|Historical read at closed frontier| H["Follower historical read"]

    F --> I["Return current value"]
    G --> I
    H --> J["Return bounded historical value"]
```

## Development Roadmap

1. **Phase 0**: freeze keyspace, state machines, retry contract, errors, closed timestamps, placement, and invariants. Status: complete.
2. **Phase 1**: Pebble-backed single-node storage core. Status: complete.
3. **Phase 2**: shared MultiRaft scheduler, lease system, fast reads, and durability batching. Status: complete.
4. **Phase 3**: meta ranges, routing, liveness, split, rebalance, and membership changes. Status: in progress.
5. **Phase 4**: transaction core
6. **Phase 5**: multi-range transactions, `STAGING`, and parallel commit
7. **Phase 6**: PostgreSQL wire protocol and distributed SQL front door
8. **Phase 7**: locality semantics and follower historical reads
9. **Phase 8**: hardening, large-scale simulation, and chaos/Jepsen testing

## Phase 8 Evidence

The in-repo Phase 8 closure work is now implemented and tested:

- the live local controller is in [`internal/systemtest/local_controller.go`](./internal/systemtest/local_controller.go)
- the persistent artifact bundle is in [`internal/systemtest/artifacts.go`](./internal/systemtest/artifacts.go)
- the artifact assertion pack is in [`internal/systemtest/assertions.go`](./internal/systemtest/assertions.go)
- the built-in local fault matrix is in [`internal/systemtest/matrix.go`](./internal/systemtest/matrix.go) and exercised in [`internal/systemtest/matrix_test.go`](./internal/systemtest/matrix_test.go)
- the external handoff contract is documented in [`docs/systemtest/EXTERNAL_HANDOFF.md`](./docs/systemtest/EXTERNAL_HANDOFF.md)
- the operator dashboard and runbook docs live in [`docs/operations/DASHBOARDS.md`](./docs/operations/DASHBOARDS.md) and [`docs/operations/RUNBOOKS.md`](./docs/operations/RUNBOOKS.md)

Current decision:

- the Phase 8 closure plan is complete in-repo
- the top-level Phase 8 roadmap item is **not** marked complete yet, because an external Jepsen/chaos runner has not yet consumed the exported `handoff.json` contract and produced retained fault-run artifacts outside the local harness

## Repository Contract

Current source-of-truth docs:

- [`ARCHITECTURE.md`](./ARCHITECTURE.md) explains the target system and rationale
- [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md) is the execution contract
- [`TODOS.md`](./TODOS.md) tracks the next concrete milestones
- [`rules.md`](./rules.md) defines the repo workflow rules
- [`docs/systemtest/EXTERNAL_HANDOFF.md`](./docs/systemtest/EXTERNAL_HANDOFF.md) defines the external chaos-runner mapping
- [`docs/operations/DASHBOARDS.md`](./docs/operations/DASHBOARDS.md) and [`docs/operations/RUNBOOKS.md`](./docs/operations/RUNBOOKS.md) define the Phase 8 operator view

Implementation rule:

- if code needs a protocol or scope change not already written down, update
  `IMPLEMENTATION_PLAN.md` first in a docs commit, then write code
- push directly to `main` unless a PR is explicitly requested
