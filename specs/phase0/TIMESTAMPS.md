# Timestamps

## Purpose

Define the timestamp model used by MVCC, leases, transaction restarts, and
commit visibility before storage or transaction code exists.

## HLC Type

ChronosDB timestamps are hybrid logical timestamps:

```text
Timestamp {
  wall_time
  logical
}
```

Rules:

- ordering is lexicographic by `(wall_time, logical)`
- the in-memory and protocol model is a structured type, not a packed integer
- binary packing is an implementation detail and may not leak into protocol
  semantics

## Local Clock Update Rule

On local event:

```text
now = max(local_physical_time, last_hlc.wall_time)
```

If `now == last_hlc.wall_time`, increment `logical`.

On receiving a remote timestamp `remote`:

```text
wall_time = max(local_physical_time, last.wall_time, remote.wall_time)
logical =
  0                         if wall_time is strictly greater than both sources
  max(last.logical, remote.logical) + 1   otherwise
```

The exact helper implementation may vary, but it must preserve causal monotonicity.

## Max Offset Rule

Each node operates with a configured `max_offset`.

If a node detects clock uncertainty beyond `max_offset`:

- it must stop serving leaseholder-local fast reads
- it must stop assigning fresh transaction timestamps
- it must advertise itself unhealthy
- it may terminate to enforce fail-closed recovery

Re-entry is not automatic. Recovery requires a fresh process start, join
handshake, and liveness-epoch bump.

## MVCC Timestamp Rules

- committed versions are stored at their committed write timestamp
- historical reads may request any timestamp at or below the policy-allowed
  visibility frontier
- unresolved intents block reads according to transaction conflict rules

For the same logical key:

- newer committed versions sort ahead of older committed versions
- an intent is not treated as a committed version

## Transaction Timestamp Rules

Each transaction tracks:

- `read_ts`
- `write_ts`
- `min_commit_ts`

Rules:

- `write_ts >= read_ts`
- `min_commit_ts >= write_ts`
- a retry may advance `read_ts` and must keep `write_ts >= min_commit_ts`

On write-too-old or serialization restart:

- the restart chooses a higher timestamp
- the transaction epoch increments

## Lease Timestamp Rules

Lease records use HLC timestamps for:

- `start_ts`
- `expiration_ts`

Rules:

- a local fast read is valid only if `now_hlc` is within the lease interval and
  the replica has the latest `sequence`
- lease transfer may set `start_ts` in the future to prevent overlap during handoff

## Commit-Wait Rule

When required for external visibility ordering, ChronosDB must wait until local
time is at least the transaction's chosen commit timestamp before acknowledging
the commit as externally visible.

Commit-wait may not be skipped merely because replication has completed.

## Closed Timestamp Interaction

Closed timestamp publication must remain behind:

- the last safely applied committed timestamp
- the holder's uncertainty window
- unresolved low-timestamp intents

Followers may not infer a safe historical timestamp beyond the published frontier.

## Tests Required

- HLC monotonicity under local and remote events
- restart advances timestamp on retryable conflict
- max-offset violation disables fast reads and new timestamp assignment
- commit-wait preserves visibility ordering
- closed timestamp frontier never exceeds uncertainty or unresolved-intent limits
