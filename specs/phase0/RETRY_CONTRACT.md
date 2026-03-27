# Retry Contract

## Purpose

Define exactly when ChronosDB retries, refreshes metadata, backs off, or fails.

## Global Rule

Every client request carries a context deadline. There is no fixed retry count.
The deadline is the retry boundary.

## Routing Retry Flow

On routing errors:

- `RANGE_NOT_HERE`
- `RANGE_SPLIT`
- `DESCRIPTOR_STALE`
- `LEADER_CHANGED`

the client or gateway must:

1. invalidate the affected cache entry
2. refresh from authoritative meta ranges
3. retry immediately once using the fresh descriptor
4. if the retried request fails again for a retriable reason, use exponential
   backoff with jitter until the deadline expires

## Backoff Rules

Default retry schedule:

- first retry after fresh metadata: immediate
- subsequent retries: exponential backoff starting at `25ms`
- multiplier: `2x`
- max sleep between retries: `1s`
- jitter: `+-20%`

These values are defaults and may be tuned, but the shape of the policy must stay:

- immediate retry after new information
- backoff only on repeated failure

## Transaction Retry Rules

Automatic transaction retry is allowed only for retriable transaction errors:

- `TXN_RETRY_SERIALIZATION`
- `TXN_RETRY_WRITE_TOO_OLD`
- `CONTENTION_BACKOFF`

On retry:

- increment transaction epoch
- advance timestamps as required by the error
- preserve client deadline

Automatic retry is not allowed for:

- `TXN_ABORTED`
- any `Fatal` error
- non-retriable semantic errors

## Follower Read Retry Rules

If a follower historical read is above the closed timestamp frontier:

- return `FOLLOWER_READ_TOO_FRESH`
- do not silently serve stale data
- client may retry later, reroute to leaseholder, or lower read timestamp by policy

## Resource/Policy Errors

`Resource/Policy` errors are not automatically equivalent to infinite retry.

Defaults:

- `QUOTA_EXCEEDED`: do not auto-retry without external policy
- `ADMISSION_REJECTED`: may retry with backoff until deadline
- `NODE_DRAINING`: reroute if possible, else retry until deadline

## Retry Visibility

Retry loops must be observable.

Required visibility:

- retry attempt count
- last error code
- cumulative retry delay
- final failure reason if deadline expires

## Tests Required

- immediate retry after descriptor refresh
- exponential backoff only on repeated failures
- deadline stops retries even when count is low
- transaction epoch bump on retryable restart
- no auto-retry on fatal or semantic errors
