# Lease State Machine

## Purpose

Define lease ownership, fast-read eligibility, and transfer behavior before any
replica code is written.

## Lease Record

```text
Lease {
  holder_replica_id
  start_ts
  expiration_ts
  sequence
  holder_liveness_epoch
}
```

Field rules:

- `sequence` is the epoch-bumped transfer barrier
- `holder_liveness_epoch` binds the lease to a specific liveness incarnation of
  the holder node
- `start_ts` and `expiration_ts` are HLC timestamps

## Conceptual States

### `UNLEASED`

No replica may serve local fast reads.

### `OWNED`

One replica holds the lease and may serve local fast reads if all read checks pass.

### `TRANSFERRING`

A new lease with `sequence = old.sequence + 1` has been proposed but not yet
applied by all relevant replicas. This is an implementation transition state,
not a separately persisted status field.

### `EXPIRED`

The lease record exists, but the local replica may not use it for fast reads
because time or liveness conditions are no longer valid.

## Fast-Read Eligibility

A replica may serve a leaseholder-local fast read only if all of the following
are true:

1. the replica is the current `holder_replica_id`
2. the lease record has the latest applied `sequence`
3. the holder node's current liveness epoch matches `holder_liveness_epoch`
4. local `now_hlc` is within `[start_ts, expiration_ts)`
5. local clock is within configured max offset
6. the read timestamp is not greater than the safe local lease-read frontier

If any condition fails, the replica must fall back to `ReadIndex` or return a
retryable error.

## Transfer Protocol

Lease transfer is a replicated lease-state transition. It is not identical to
Raft leadership transfer, though the system should usually coordinate them.

Transfer steps:

1. current holder selects target replica
2. if leader placement is not already aligned, the system may coordinate Raft
   leadership transfer toward the target
3. current holder proposes a new lease record with:
   - `holder_replica_id = target`
   - `sequence = old.sequence + 1`
   - `start_ts >= max(old.start_ts, now_hlc)`
   - fresh `expiration_ts`
   - target node's current `holder_liveness_epoch`
4. once the new record is committed and applied:
   - the new holder may serve fast reads when `now_hlc >= start_ts`
   - the old holder must stop serving fast reads immediately after observing the
     higher `sequence`

## Transfer Invariants

- `sequence` is strictly monotonic
- two different holders may never both serve local fast reads for the same
  `sequence`
- a lower `sequence` holder must lose to a higher `sequence` holder even if its
  local wall clock has not yet expired
- a lease tied to an old `holder_liveness_epoch` becomes invalid after liveness
  epoch bump

## Leader Interaction

- leaseholder and Raft leader are separate concepts
- policy strongly prefers co-location
- if leadership and lease diverge, writes still route through the Raft leader,
  but fast reads depend only on valid lease ownership
- if divergence makes correctness checks ambiguous, the read path must use
  `ReadIndex`

## Renewal Rules

Renewal is a lease update by the current holder with:

- same `holder_replica_id`
- same `holder_liveness_epoch`
- same or higher `start_ts`
- later `expiration_ts`
- same `sequence`

Renewal does not create a new transfer epoch.

## Failure Rules

### Clock violation

If the holder exceeds max clock offset:

- it must stop serving fast reads
- it must stop assigning transaction timestamps
- it may continue only after operator-mediated recovery and liveness epoch bump

### Liveness epoch bump

If node liveness epoch changes:

- all leases tied to the old epoch become invalid for local fast reads
- transfer or reacquisition requires a new lease record

### Uncertain lease state

If the replica cannot prove lease validity:

- use `ReadIndex`
- do not guess from local time alone

## Tests Required

- transfer with sequence bump
- stale old-holder read rejection after new sequence observed
- renewal without sequence bump
- liveness epoch mismatch invalidates local fast reads
- clock violation forces `ReadIndex` fallback or retry
