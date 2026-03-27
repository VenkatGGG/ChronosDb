# ChronosDB

ChronosDB is a geo-distributed, strictly serializable SQL database built on a
replicated MVCC KV substrate.

This repository is intentionally **docs-first** right now. The architecture,
execution contract, and roadmap are being frozen before any implementation code
lands so the system does not drift into accidental shortcuts.

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

1. **Phase 0**: freeze keyspace, state machines, retry contract, errors, closed timestamps, placement, and invariants
2. **Phase 1**: Pebble-backed single-node storage core
3. **Phase 2**: shared MultiRaft scheduler, lease system, fast reads, and durability batching
4. **Phase 3**: meta ranges, routing, liveness, split, rebalance, and membership changes
5. **Phase 4**: transaction core
6. **Phase 5**: multi-range transactions, `STAGING`, and parallel commit
7. **Phase 6**: PostgreSQL wire protocol and distributed SQL front door
8. **Phase 7**: locality semantics and follower historical reads
9. **Phase 8**: hardening, large-scale simulation, and chaos/Jepsen testing

## Repository Contract

Before any code lands:

- [`ARCHITECTURE.md`](./ARCHITECTURE.md) explains the target system and rationale
- [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md) is the execution contract
- [`TODOS.md`](./TODOS.md) tracks the next concrete milestones
- [`rules.md`](./rules.md) stores persistent Codex/agent workflow rules

Implementation rule:

- if code needs a protocol or scope change not already written down, update
  `IMPLEMENTATION_PLAN.md` first in a docs commit, then write code

Git workflow rule:

- agent-generated changes should land through pull requests instead of direct
  pushes to `main`; a new branch is not required for every task, and work can
  continue after the PR is opened while review happens asynchronously
