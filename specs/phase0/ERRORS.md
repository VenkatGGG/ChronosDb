# Errors

## Purpose

Define the error taxonomy and the initial canonical error set before protocol
messages or client retry loops are implemented.

## Top-Level Classes

### `Retriable`

The request may succeed unchanged after refreshed metadata, lease/leadership
movement, or bounded waiting.

### `Client/Semantic`

The request is syntactically valid enough to parse, but the requested operation
is invalid for the current visible state or API semantics.

### `Resource/Policy`

The request is rejected because of policy, admission, quota, or temporary
resource pressure rather than logical invalidity.

### `Fatal`

Local or replicated state is suspect enough that continuing risks corruption or
invariant breakage.

## Canonical Error Codes

### Retriable

- `RANGE_NOT_HERE`
- `RANGE_SPLIT`
- `LEADER_CHANGED`
- `LEASE_TRANSFER_IN_PROGRESS`
- `READ_INDEX_REQUIRED`
- `TXN_RETRY_SERIALIZATION`
- `TXN_RETRY_WRITE_TOO_OLD`
- `CONTENTION_BACKOFF`
- `DESCRIPTOR_STALE`

### Client/Semantic

- `BAD_REQUEST`
- `KEY_NOT_FOUND`
- `TXN_ABORTED`
- `UNSUPPORTED_OPERATION`
- `PLACEMENT_CONSTRAINT_INVALID`

### Resource/Policy

- `QUOTA_EXCEEDED`
- `ADMISSION_REJECTED`
- `NODE_DRAINING`
- `CLOCK_UNAVAILABLE`
- `FOLLOWER_READ_TOO_FRESH`

### Fatal

- `INVARIANT_VIOLATION`
- `CORRUPTION_DETECTED`
- `RAFT_STATE_BROKEN`
- `KEYSPACE_ENCODING_BUG`

## Classification Rules

- topology movement, temporary uncertainty, and retry-safe serialization failures
  are `Retriable`
- invalid user/API requests are `Client/Semantic`
- policy and pressure gates are `Resource/Policy`
- any condition that questions state integrity is `Fatal`

## Handling Rules

- `Retriable` errors must expose enough context for safe retry
- `Client/Semantic` errors must not be retried automatically by the client
- `Resource/Policy` errors may be retried by policy, but not blindly or forever
- `Fatal` errors must surface loudly and may fail closed

## Required Error Metadata

Every error payload must carry:

- `code`
- top-level class
- human-readable message
- request or transaction correlation ID when available

Optional payload by error type:

- new descriptor generation or range hint
- leader/leaseholder hint
- retry delay hint
- transaction restart timestamp

## Tests Required

- classification table tests
- retry/non-retry mapping tests
- routing errors include enough context for refresh
- fatal errors never map to silent retries
