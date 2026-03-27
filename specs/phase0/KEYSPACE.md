# Keyspace

## Purpose

Define the logical and physical key layout for ChronosDB before any storage code
lands.

ChronosDB uses one Pebble engine with two logical namespaces:

- `/raft/...` for Raft durability state
- `/mvcc/...` for replicated MVCC and local metadata state

All keys are encoded as ordered tuples and serialized into memcomparable byte
sequences. The exact binary codec is an implementation detail, but ordering must
preserve tuple order.

## Physical Pebble Namespaces

### `/raft/...`

Persistent Raft state for each range:

- `/raft/range/{range_id}/hardstate`
- `/raft/range/{range_id}/entry/{log_index}`
- `/raft/range/{range_id}/truncated`
- `/raft/range/{range_id}/snapshot/{snapshot_id}`

Rules:

- Raft entries are addressed by monotonically increasing `log_index`
- `hardstate` and the entries written in a scheduler tick are persisted in the
  same shared `WriteBatch`
- the Raft namespace is not exposed to SQL or client KV APIs

### `/mvcc/...`

Replicated and local MVCC-addressable data:

- `/mvcc/global/...` for replicated ordered data
- `/mvcc/local/...` for local store/range metadata not exposed as user data

## Global Ordered Keyspace

The replicated ordered keyspace is split into system and user spans.

```text
/mvcc/global/meta1/...
/mvcc/global/meta2/...
/mvcc/global/system/...
/mvcc/global/table/...
```

### Meta levels

- `meta1` maps `meta2` spans to descriptor locations
- `meta2` maps user and system key spans to `RangeDescriptor`s

Lookup contract:

1. user/system key -> `meta2`
2. `meta2` miss/range lookup -> `meta1`
3. resolved descriptor -> leaseholder targeting

### System replicated spans

Reserved system subspaces:

- `/mvcc/global/system/desc/...` catalog descriptors
- `/mvcc/global/system/liveness/...` `NodeLiveness`
- `/mvcc/global/system/lease/...` lease records
- `/mvcc/global/system/txn/...` `TxnRecord`
- `/mvcc/global/system/lock/...` replicated lock metadata when required
- `/mvcc/global/system/closedts/...` closed timestamp publications
- `/mvcc/global/system/placement/...` placement and survival policy records

### User SQL data

User-visible relational data lives under ordered table/index spans:

- `/mvcc/global/table/{table_id}/primary/{encoded_primary_key}`
- `/mvcc/global/table/{table_id}/index/{index_id}/{encoded_index_key}`

This allows:

- ordered scans by primary key
- ordered scans by secondary index
- range splits based on actual SQL key locality

## MVCC Version Encoding

Each logical key may have multiple versions.

Conceptual layout:

```text
{logical_key}@{wall_time,logical_counter}
```

Ordering rules:

- versions for the same logical key sort newest-to-oldest within that key
- the zero-version sentinel sorts after all versions for intent lookup and
  existence checks
- MVCC timestamps use HLC components: `wall_time`, `logical_counter`

## Intents

Write intents are stored in the MVCC namespace next to the affected key, not in a
separate logical user table.

Each intent stores:

- `txn_id`
- provisional value
- provisional write timestamp
- intent strength

Intent lookup must be possible from the key alone without a secondary index.

## Local Metadata Keys

Local keys are not part of the globally ordered SQL/KV keyspace.

Examples:

- `/mvcc/local/store/ident`
- `/mvcc/local/store/version`
- `/mvcc/local/range/{range_id}/applied_state`
- `/mvcc/local/range/{range_id}/lease_applied_sequence`

Rules:

- local metadata may be colocated in the same engine
- local keys do not participate in user range scans
- local keys may still be range-scoped for recovery and debugging

## Frozen Decisions

- one Pebble engine, not separate engines by default
- logical namespaces, not RocksDB-style column families
- two-level meta addressing (`meta1`, `meta2`)
- SQL table/index keyspace is the end-state user-data model
- intents live adjacent to keyed MVCC data

## Out of Scope for This Spec

- the exact byte codec implementation
- descriptor schema internals beyond required key prefixes
- multi-tenant key prefixes
