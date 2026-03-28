# External Chaos Runner Handoff

ChronosDB exports `handoff.json` next to each scenario artifact directory so an
external chaos toolchain can consume the same scenario manifest that the local
Phase 8 matrix runner uses.

## Files

- `manifest.json`: validated scenario steps and parameters
- `handoff.json`: manifest plus the action-to-operation mapping below
- `report.json`: structured runner output
- `summary.json`: pass/fail synopsis
- `node-logs/node-<id>.json`: per-node event log stream

## Action Mapping

| Manifest action | External operation | Meaning |
| --- | --- | --- |
| `partition` | `network.partition_bidirectional` | Isolate every node in `partition_left` from every node in `partition_right` |
| `heal` | `network.heal_all` | Remove all active network filters and restore connectivity |
| `crash_node` | `process.stop` | Stop the target node abruptly |
| `restart_node` | `process.start` | Start or re-admit the target node after a crash |
| `ambiguous_commit` | `gateway.inject_ambiguous_commit` | Arm a one-shot gateway fault using `gateway_node_id`, `txn_label`, `ack_delay`, and `drop_response` |
| `wait` | `control.wait` | Sleep for the supplied duration before the next step |

## Runner Contract

1. Load `handoff.json`.
2. Execute manifest steps in order.
3. Translate each `action` into the mapped `external_operation`.
4. Preserve the manifest parameters exactly when invoking the fault injector.
5. Emit node logs using the standardized assertion markers from `internal/systemtest/assertions.go` when a run wants built-in artifact validation.
6. Persist the resulting `report.json`, `summary.json`, and node logs back into the artifact directory.

## Notes

- The handoff path is intentionally non-authoritative. Routing, lease, and transaction correctness still come from ChronosDB state machines, not from the external runner.
- `handoff.json` is designed for Jepsen-style or bespoke chaos harnesses that need a stable, versioned mapping rather than access to Go code directly.
