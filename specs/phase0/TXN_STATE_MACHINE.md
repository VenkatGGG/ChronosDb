# Transaction State Machine

## Purpose

Define transaction states, commit paths, retry behavior, and recovery rules before
lock-table or coordinator code exists.

## Txn Record

```text
TxnRecord {
  txn_id
  status            // PENDING | STAGING | COMMITTED | ABORTED
  read_ts
  write_ts
  min_commit_ts
  epoch
  priority
  anchor_range_id
  touched_ranges[]
  last_heartbeat_ts
  deadline_ts
}
```

## States

### `PENDING`

The transaction may acquire locks, lay down intents, and continue work.

### `STAGING`

The transaction has entered parallel-commit resolution. Required intents must
already exist before `STAGING` is durable.

### `COMMITTED`

The transaction outcome is final. Remaining work is cleanup only.

### `ABORTED`

The transaction outcome is final. Remaining work is cleanup only.

## Commit Paths

### One-phase commit

Use when all writes are confined to one range and the transaction can commit in a
single round.

Path:

`PENDING -> COMMITTED`

### Parallel commit

Use for multi-range work when:

- all required intents have been written
- the transaction record can advance to `STAGING`
- commit preconditions hold

Path:

`PENDING -> STAGING -> COMMITTED`

## State Transitions

### Begin

Transaction creation writes or materializes a `PENDING` record anchored on one
range.

### Heartbeat

While `PENDING` or `STAGING`, the coordinator or recovery owner updates
`last_heartbeat_ts`.

### Retry epoch bump

On retryable restart, the transaction keeps `txn_id` but increments `epoch`.
Older intents from the same `txn_id` and lower `epoch` are considered stale.

### Abort

Any of the following may produce `ABORTED`:

- wound-wait victimization
- explicit client abort
- deadline exceeded
- recovery proving commit cannot succeed

### Commit

`COMMITTED` is entered only after the transaction state machine proves that the
required commit path has completed.

## Recovery Rules

Recovery actors:

- encountering transaction or reader
- async intent resolver
- GC/recovery worker

### `STAGING` recovery decision rule

- if all required intents are present and uncontested, `STAGING -> COMMITTED`
- if any required intent is missing, reverted, or irreconcilably conflicted,
  `STAGING -> ABORTED`

No actor may leave `STAGING` unresolved once it has enough information to decide.

### Coordinator death

If the original coordinator disappears:

- recovery uses the durable `TxnRecord`
- recovery actors inspect participant intents
- outcome is derived from `TxnRecord` + required intents, not from coordinator memory

## Intents

Each write intent carries:

- `txn_id`
- `epoch`
- provisional timestamp
- provisional value

Intent rules:

- intents from older epochs of the same transaction are stale
- readers at timestamps above the intent timestamp must either resolve, wait, or
  restart per conflict policy
- intent cleanup is required after terminal states

## Conflict Policy

ChronosDB uses wound-wait.

Rules:

- older transaction wounds younger conflicting holder
- younger transaction waits behind older conflicting holder
- waits are explicit and observable
- retryable errors carry enough information to restart safely

## Retry / Restart Rules

A transaction restart is retryable only if:

- the error is classified as retriable
- the transaction has not already reached a terminal state
- the client deadline has not expired

On restart:

- `epoch` increments
- `read_ts` may advance
- `write_ts` must be at least `min_commit_ts`

## Terminal-State Invariants

- `COMMITTED` and `ABORTED` are terminal
- terminal outcome is immutable
- cleanup may continue after terminal state, but outcome may not change

## Tests Required

- one-phase commit path
- `PENDING -> STAGING -> COMMITTED`
- `STAGING -> ABORTED` on missing/conflicting required intent
- coordinator death during `STAGING`
- stale-intent rejection after epoch bump
- wound-wait victimization and restart behavior
