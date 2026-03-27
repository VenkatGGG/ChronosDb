# Closed Timestamps

## Purpose

Define follower historical read eligibility and publication behavior.

Follower reads in ChronosDB are historical reads, not arbitrary stale reads.

## Closed Timestamp Record

```text
ClosedTimestamp {
  range_id
  lease_sequence
  closed_ts
  published_at
}
```

Field rules:

- `lease_sequence` binds the record to a specific lease epoch
- `closed_ts` is the maximum timestamp at which a follower may safely serve a read
- `published_at` is the holder-local publication timestamp

## Publication Owner

Only the current leaseholder may publish or advance the closed timestamp for a
range.

If lease sequence changes:

- publications from older sequences are invalid
- followers must ignore them once a newer lease sequence is known

## Publication Rule

The leaseholder publishes a new closed timestamp every `200ms` by default.

Candidate frontier:

```text
candidate = min(
  last_applied_committed_ts,
  now_hlc - max_offset,
  oldest_unresolved_intent_ts - 1 logical tick
)
```

Interpretation:

- do not close beyond data that has not yet been applied
- do not close beyond the uncertainty window implied by clock skew
- do not close past unresolved intents

The published `closed_ts` is monotonic and may only advance.

## Follower Read Eligibility

A follower may serve a historical read only if:

1. the read timestamp `read_ts <= closed_ts`
2. the follower has applied all state up to the publication being used
3. the publication `lease_sequence` matches the follower's current known lease
   sequence for the range

If any condition fails:

- reject with `FOLLOWER_READ_TOO_FRESH`, or
- reroute to the leaseholder path

## Freshness Model

Follower reads trade freshness for locality.

They are:

- local
- bounded by publication lag
- safe only at or below the published frontier

They are not:

- arbitrarily stale reads chosen by convenience
- substitutes for the current leaseholder fast path

## Failure Rules

### Lease transfer

On lease transfer:

- older sequence publications become invalid
- new holder starts publishing for the new `lease_sequence`

### Clock violation

If the leaseholder has clock uncertainty beyond max offset:

- do not advance the closed timestamp
- fall back to safer read paths until the node is recovered or replaced

### Intent backlog

If unresolved intents accumulate at low timestamps:

- closed timestamp advancement may stall
- this is correct behavior, not a bug

## Default Publication Targets

- cadence: `200ms`
- expected follower-read lag target: sub-second under healthy conditions
- publication transport may be piggybacked on replica traffic or dedicated metadata updates

## Tests Required

- monotonic publication
- no follower read above closed frontier
- lease sequence invalidates stale publications
- unresolved intents stall frontier advancement
- publication stalls under holder clock violation
