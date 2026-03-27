package storage

// CompactionPriority describes how urgently background compaction work should run.
type CompactionPriority uint8

const (
	CompactionPriorityBackground CompactionPriority = iota + 1
	CompactionPriorityNormal
	CompactionPriorityCritical
)

// CompactionMetrics is the subset of Pebble pressure signals used to prioritize work.
type CompactionMetrics struct {
	L0Files                int
	L0Sublevels            int
	PendingCompactionBytes uint64
	WriteStallCount        int
}

// CompactionPolicy defines escalation thresholds for write-stall avoidance.
type CompactionPolicy struct {
	NormalL0Files                  int
	CriticalL0Files                int
	NormalL0Sublevels              int
	CriticalL0Sublevels            int
	NormalPendingCompactionBytes   uint64
	CriticalPendingCompactionBytes uint64
}

// DefaultCompactionPolicy returns conservative thresholds for early development.
func DefaultCompactionPolicy() CompactionPolicy {
	return CompactionPolicy{
		NormalL0Files:                  8,
		CriticalL0Files:                20,
		NormalL0Sublevels:              4,
		CriticalL0Sublevels:            10,
		NormalPendingCompactionBytes:   64 << 20,
		CriticalPendingCompactionBytes: 512 << 20,
	}
}

// Classify returns the priority tier that background work should use.
func (p CompactionPolicy) Classify(metrics CompactionMetrics) CompactionPriority {
	if metrics.WriteStallCount > 0 ||
		metrics.L0Files >= p.CriticalL0Files ||
		metrics.L0Sublevels >= p.CriticalL0Sublevels ||
		metrics.PendingCompactionBytes >= p.CriticalPendingCompactionBytes {
		return CompactionPriorityCritical
	}
	if metrics.L0Files >= p.NormalL0Files ||
		metrics.L0Sublevels >= p.NormalL0Sublevels ||
		metrics.PendingCompactionBytes >= p.NormalPendingCompactionBytes {
		return CompactionPriorityNormal
	}
	return CompactionPriorityBackground
}
