# Phase 8 Runbooks

These runbooks align to the Phase 8 hardening dashboards and the live operator
surface exposed by `/metrics`, `/healthz`, `/readyz`, `/debug/chronos/overview`,
and `pprof`.

## Snapshot Pressure

Signals:

- `chronosdb_snapshot_pressure`
- `/debug/chronos/overview`

Immediate checks:

1. Identify the highest-pressure `store` and `phase`.
2. Confirm whether learner install backlog or sender saturation is dominant.
3. Inspect `pprof` for goroutine or heap growth that correlates with snapshot activity.

First response:

- slow rebalance/snapshot scheduling if foreground traffic is being starved
- verify the affected learner can still make progress
- check for network partitions or repeated restart churn on the target node

## Allocator Decisions and Rebalance Drift

Signals:

- `chronosdb_allocator_decisions_total`
- `chronosdb_allocator_rebalance_score`

Immediate checks:

1. Confirm whether decisions are increasing but scores stay bad.
2. Compare `preferred=true` versus `preferred=false` outcomes.
3. Check recent range movements and any overlapping failure event.

First response:

- suspend aggressive movement if the cluster is already under fault recovery
- inspect placement policy mismatches before forcing more rebalances

## Closed Timestamp Lag and Follower Historical Reads

Signals:

- `chronosdb_follower_read_lag_seconds`
- follower-read artifact assertions

Immediate checks:

1. Find the worst `scope`.
2. Determine whether the lag is lease churn, publication delay, or replica apply delay.
3. Correlate with lease transfer and snapshot pressure dashboards.

First response:

- route reads back to the leaseholder if lag breaches the historical-read target
- inspect the current lease sequence and any stale publication evidence

## Lease Churn

Signals:

- `chronosdb_lease_transfer_duration_seconds`
- `/debug/chronos/overview`

Immediate checks:

1. Look at transfer rate and p95 duration together.
2. Correlate churn with routing retries and allocator activity.
3. Check whether a single range or locality policy is driving most transfers.

First response:

- stop voluntary lease movement if churn is self-inflicted
- look for clock, liveness, or network instability before changing placement

## Retry Pressure

Signals:

- `chronosdb_request_retries_total`

Immediate checks:

1. Break down retry reasons.
2. Separate routing churn from contention churn.
3. Correlate with splits, rebalances, and lease transfers.

First response:

- if dominated by routing errors, inspect descriptor invalidation and recent membership changes
- if dominated by contention, inspect lock waits and hot keys

## Recovery Outcomes

Signals:

- `chronosdb_recovery_outcomes_total`
- systemtest artifact summaries and node logs

Immediate checks:

1. Identify the failing workflow and result label.
2. Pull the matching artifact directory and inspect `summary.json`, `report.json`, and node logs.
3. Check whether failures align to `STAGING` recovery, snapshot catch-up, or crash restart.

First response:

- if failures are isolated to a workflow, pause the triggering chaos or repair loop
- if failures are broad, treat the cluster state machine as suspect and halt additional disruption
