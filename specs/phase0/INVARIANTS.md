# Invariants

## Purpose

These invariants are the minimum correctness properties that must be encoded as
tests. They are not optional aspirations.

## Invariant 1: No Future Commit Read

Statement:

> A read at timestamp `T` must never return a value from a transaction that
> became committed after `T`.

Why it matters:

- prevents time-travel corruption
- is foundational for strict serializability and historical reads

Test mapping:

- property test over MVCC histories
- simulation with delayed commit application and historical reads

## Invariant 2: Single-Key Order Consistency

Statement:

> No two committed transactions may observe contradictory committed orderings of
> the same key history.

Why it matters:

- prevents split-brain or stale-read style anomalies on one logical key

Test mapping:

- simulation with lease transfer, leadership churn, and conflicting writes

## Invariant 3: Monotonic Visibility After Commit

Statement:

> Once a write is committed and externally visible, later successful reads at
> timestamps greater than or equal to the write timestamp must not return an
> older committed version unless the API explicitly asked for historical time.

Why it matters:

- protects read-after-write expectations
- validates commit-wait and routing correctness

Test mapping:

- integration tests for write then read
- simulation with retries, lease churn, and descriptor refresh

## Invariant 4: Lease Sequence Exclusion

Statement:

> Two different replicas must never both serve leaseholder-local fast reads under
> the same effective lease epoch.

Why it matters:

- fast reads depend on exclusive lease ownership

Test mapping:

- simulation with transfer races and delayed replication

## Invariant 5: Closed Timestamp Safety

Statement:

> A follower read at timestamp `T` is valid only if `T` is less than or equal to
> the published closed timestamp for the current lease sequence and the follower
> has applied state through that publication.

Why it matters:

- distinguishes safe historical reads from arbitrary stale reads

Test mapping:

- simulation with publication lag, lease transfer, and unresolved intents

## Invariant 6: Config-Change Generation Safety

Statement:

> A membership or range-config change with an expected generation that does not
> match the current generation must not apply.

Why it matters:

- prevents stale allocator actions from winning after split or rebalance churn

Test mapping:

- simulation with duplicate allocator proposals and split races

## Invariant 7: Terminal Transaction Immutability

Statement:

> Once a transaction reaches `COMMITTED` or `ABORTED`, its outcome must never
> change, even if cleanup is still pending.

Why it matters:

- recovery must be deterministic

Test mapping:

- transaction recovery tests with crashes during cleanup

## Required Test Layers

- property-based tests in Go
- deterministic transport/clock simulation tests
- integration tests for end-to-end confirmation

TLA+ is useful if later added, but it is not a prerequisite for Phase 1 code.
