package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics owns the operator-facing Prometheus collectors for core ChronosDB signals.
type Metrics struct {
	registry *prometheus.Registry

	raftReadyLag             *prometheus.HistogramVec
	raftApplyLag             *prometheus.HistogramVec
	leaseTransferDuration    *prometheus.HistogramVec
	pendingIntents           *prometheus.GaugeVec
	lockWaitDuration         *prometheus.HistogramVec
	splitDuration            *prometheus.HistogramVec
	snapshotDuration         *prometheus.HistogramVec
	snapshotPressure         *prometheus.GaugeVec
	pebbleCompactionPressure *prometheus.GaugeVec
	rangeCacheLookups        *prometheus.CounterVec
	followerReadLag          *prometheus.GaugeVec
	allocatorRebalanceScore  *prometheus.GaugeVec
	allocatorDecisions       *prometheus.CounterVec
	requestRetries           *prometheus.CounterVec
	recoveryOutcomes         *prometheus.CounterVec
}

// NewMetrics constructs a fresh registry with ChronosDB collectors registered.
func NewMetrics() *Metrics {
	return NewMetricsWithRegistry(prometheus.NewRegistry())
}

// NewMetricsWithRegistry registers ChronosDB collectors into the provided registry.
func NewMetricsWithRegistry(registry *prometheus.Registry) *Metrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	m := &Metrics{
		registry: registry,
		raftReadyLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "chronosdb",
			Name:      "raft_ready_lag_entries",
			Help:      "Ready backlog per replica/range scheduling scope.",
			Buckets:   []float64{0, 1, 2, 4, 8, 16, 32, 64, 128},
		}, []string{"scope"}),
		raftApplyLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "chronosdb",
			Name:      "raft_apply_lag_entries",
			Help:      "Apply backlog per replica/range scheduling scope.",
			Buckets:   []float64{0, 1, 2, 4, 8, 16, 32, 64, 128},
		}, []string{"scope"}),
		leaseTransferDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "chronosdb",
			Name:      "lease_transfer_duration_seconds",
			Help:      "Lease transfer duration by result.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"result"}),
		pendingIntents: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "chronosdb",
			Name:      "pending_intents",
			Help:      "Current unresolved intent count by scope.",
		}, []string{"scope"}),
		lockWaitDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "chronosdb",
			Name:      "lock_wait_duration_seconds",
			Help:      "Observed lock wait duration by result.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"result"}),
		splitDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "chronosdb",
			Name:      "split_duration_seconds",
			Help:      "Range split duration by result.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"result"}),
		snapshotDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "chronosdb",
			Name:      "snapshot_duration_seconds",
			Help:      "Snapshot send/install duration by direction and result.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"direction", "result"}),
		snapshotPressure: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "chronosdb",
			Name:      "snapshot_pressure",
			Help:      "Current snapshot backpressure by store and phase.",
		}, []string{"store", "phase"}),
		pebbleCompactionPressure: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "chronosdb",
			Name:      "pebble_compaction_pressure",
			Help:      "Current Pebble compaction pressure score by store and priority tier.",
		}, []string{"store", "priority"}),
		rangeCacheLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "chronosdb",
			Name:      "range_cache_lookups_total",
			Help:      "Range cache lookups partitioned by hit or miss outcome.",
		}, []string{"outcome"}),
		followerReadLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "chronosdb",
			Name:      "follower_read_lag_seconds",
			Help:      "Follower historical-read lag behind the leaseholder frontier by scope.",
		}, []string{"scope"}),
		allocatorRebalanceScore: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "chronosdb",
			Name:      "allocator_rebalance_score",
			Help:      "Allocator-visible rebalance score by scope and preferred-region alignment.",
		}, []string{"scope", "preferred"}),
		allocatorDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "chronosdb",
			Name:      "allocator_decisions_total",
			Help:      "Allocator decisions partitioned by action and locality outcome.",
		}, []string{"action", "preferred"}),
		requestRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "chronosdb",
			Name:      "request_retries_total",
			Help:      "Request retries partitioned by reason.",
		}, []string{"reason"}),
		recoveryOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "chronosdb",
			Name:      "recovery_outcomes_total",
			Help:      "Observed recovery outcomes partitioned by workflow and result.",
		}, []string{"workflow", "result"}),
	}

	registry.MustRegister(
		m.raftReadyLag,
		m.raftApplyLag,
		m.leaseTransferDuration,
		m.pendingIntents,
		m.lockWaitDuration,
		m.splitDuration,
		m.snapshotDuration,
		m.snapshotPressure,
		m.pebbleCompactionPressure,
		m.rangeCacheLookups,
		m.followerReadLag,
		m.allocatorRebalanceScore,
		m.allocatorDecisions,
		m.requestRetries,
		m.recoveryOutcomes,
	)
	return m
}

// Registry returns the collector registry associated with the metrics set.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// ObserveRaftReadyLag records the current Ready backlog size.
func (m *Metrics) ObserveRaftReadyLag(scope string, lag float64) {
	m.raftReadyLag.WithLabelValues(scope).Observe(lag)
}

// ObserveRaftApplyLag records the current apply backlog size.
func (m *Metrics) ObserveRaftApplyLag(scope string, lag float64) {
	m.raftApplyLag.WithLabelValues(scope).Observe(lag)
}

// ObserveLeaseTransfer records lease transfer duration by outcome.
func (m *Metrics) ObserveLeaseTransfer(result string, duration time.Duration) {
	m.leaseTransferDuration.WithLabelValues(result).Observe(duration.Seconds())
}

// SetPendingIntents sets the current unresolved intent count for the given scope.
func (m *Metrics) SetPendingIntents(scope string, count int) {
	m.pendingIntents.WithLabelValues(scope).Set(float64(count))
}

// ObserveLockWait records lock wait time by outcome.
func (m *Metrics) ObserveLockWait(result string, duration time.Duration) {
	m.lockWaitDuration.WithLabelValues(result).Observe(duration.Seconds())
}

// ObserveSplit records split duration by outcome.
func (m *Metrics) ObserveSplit(result string, duration time.Duration) {
	m.splitDuration.WithLabelValues(result).Observe(duration.Seconds())
}

// ObserveSnapshot records snapshot send/install duration.
func (m *Metrics) ObserveSnapshot(direction, result string, duration time.Duration) {
	m.snapshotDuration.WithLabelValues(direction, result).Observe(duration.Seconds())
}

// SetSnapshotPressure records snapshot pressure by store and phase.
func (m *Metrics) SetSnapshotPressure(store, phase string, value float64) {
	m.snapshotPressure.WithLabelValues(store, phase).Set(value)
}

// SetPebbleCompactionPressure records a compaction-pressure score for the store.
func (m *Metrics) SetPebbleCompactionPressure(store, priority string, value float64) {
	m.pebbleCompactionPressure.WithLabelValues(store, priority).Set(value)
}

// ObserveRangeCacheLookup records whether the range cache hit or missed.
func (m *Metrics) ObserveRangeCacheLookup(hit bool) {
	outcome := "miss"
	if hit {
		outcome = "hit"
	}
	m.rangeCacheLookups.WithLabelValues(outcome).Inc()
}

// SetFollowerReadLag records follower historical-read freshness lag.
func (m *Metrics) SetFollowerReadLag(scope string, duration time.Duration) {
	m.followerReadLag.WithLabelValues(scope).Set(duration.Seconds())
}

// SetAllocatorRebalanceScore records allocator scoring by scope and locality alignment.
func (m *Metrics) SetAllocatorRebalanceScore(scope string, preferred bool, value float64) {
	m.allocatorRebalanceScore.WithLabelValues(scope, boolLabel(preferred)).Set(value)
}

// ObserveAllocatorDecision records one allocator action and whether it aligned with locality policy.
func (m *Metrics) ObserveAllocatorDecision(action string, preferred bool) {
	m.allocatorDecisions.WithLabelValues(action, boolLabel(preferred)).Inc()
}

// ObserveRequestRetry records one request retry by reason.
func (m *Metrics) ObserveRequestRetry(reason string) {
	m.requestRetries.WithLabelValues(reason).Inc()
}

// ObserveRecoveryOutcome records one recovery workflow result.
func (m *Metrics) ObserveRecoveryOutcome(workflow, result string) {
	m.recoveryOutcomes.WithLabelValues(workflow, result).Inc()
}

func boolLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
