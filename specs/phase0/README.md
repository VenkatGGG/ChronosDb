# Phase 0 Spec Pack

This directory contains the protocol-level specs that must be treated as
normative before any implementation code is written.

Read in this order:

1. [`KEYSPACE.md`](./KEYSPACE.md)
2. [`REPLICA_STATE_MACHINE.md`](./REPLICA_STATE_MACHINE.md)
3. [`LEASE_STATE_MACHINE.md`](./LEASE_STATE_MACHINE.md)
4. [`TXN_STATE_MACHINE.md`](./TXN_STATE_MACHINE.md)
5. [`TIMESTAMPS.md`](./TIMESTAMPS.md)
6. [`ERRORS.md`](./ERRORS.md)
7. [`RETRY_CONTRACT.md`](./RETRY_CONTRACT.md)
8. [`CLOSED_TIMESTAMPS.md`](./CLOSED_TIMESTAMPS.md)
9. [`PLACEMENT_AND_SURVIVAL.md`](./PLACEMENT_AND_SURVIVAL.md)
10. [`INVARIANTS.md`](./INVARIANTS.md)

Implementation rule:

- if code needs behavior not specified here, update the relevant spec first
- if a spec conflicts with a code path, the spec wins until deliberately revised
