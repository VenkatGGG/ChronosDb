# Phase 8 Dashboards

These dashboards are the operator-facing view for Phase 8 hardening. They map
directly onto the metrics exported by the ChronosDB observability surface.

## Cluster Hardening Overview

Primary panels:

- Snapshot pressure by store and phase
  - metric: `chronosdb_snapshot_pressure`
  - use: spot sender/install backlog and throttle pressure before learner catch-up stalls
- Allocator decision rate by action and locality alignment
  - metric: `chronosdb_allocator_decisions_total`
  - use: verify rebalances are happening and whether they honor placement goals
- Allocator rebalance score by scope
  - metric: `chronosdb_allocator_rebalance_score`
  - use: identify hot or imbalanced ranges before movement starts
- Follower historical-read lag
  - metric: `chronosdb_follower_read_lag_seconds`
  - use: detect closed timestamp publication lag and stale-read risk boundaries
- Lease churn rate and p95 transfer time
  - metric: `chronosdb_lease_transfer_duration_seconds`
  - use: quantify lease instability and slow transfer paths
- Request retry rate by reason
  - metric: `chronosdb_request_retries_total`
  - use: catch routing churn, lease churn, and contention amplification
- Recovery outcomes by workflow
  - metric: `chronosdb_recovery_outcomes_total`
  - use: track `STAGING` recovery, intent resolution, and crash-recovery success/failure
- Pebble compaction pressure by store and priority
  - metric: `chronosdb_pebble_compaction_pressure`
  - use: watch for approaching write stalls and emergency compaction escalation

## Suggested PromQL

- Snapshot pressure:
  - `max by (store, phase) (chronosdb_snapshot_pressure)`
- Allocator decisions:
  - `sum by (action, preferred) (rate(chronosdb_allocator_decisions_total[5m]))`
- Closed timestamp lag:
  - `max by (scope) (chronosdb_follower_read_lag_seconds)`
- Lease churn:
  - `sum(rate(chronosdb_lease_transfer_duration_seconds_count[5m]))`
- Lease transfer p95:
  - `histogram_quantile(0.95, sum by (le, result) (rate(chronosdb_lease_transfer_duration_seconds_bucket[5m])))`
- Request retries:
  - `sum by (reason) (rate(chronosdb_request_retries_total[5m]))`
- Recovery outcomes:
  - `sum by (workflow, result) (rate(chronosdb_recovery_outcomes_total[5m]))`
- Compaction pressure:
  - `max by (store, priority) (chronosdb_pebble_compaction_pressure)`

## Alert Candidates

- Snapshot pressure above `0.8` for 10m on any `install` phase
- Follower read lag above SLO-derived threshold for 5m
- Lease churn rate above baseline with p95 transfer time rising
- Retry rate spike dominated by `leader_changed`, `range_not_here`, or contention reasons
- Recovery outcomes showing non-zero `result="failed"` for any workflow
- Compaction pressure in `priority="critical"` for more than 5m
