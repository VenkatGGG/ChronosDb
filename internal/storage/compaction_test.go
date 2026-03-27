package storage

import "testing"

func TestCompactionPolicyEscalation(t *testing.T) {
	t.Parallel()

	policy := DefaultCompactionPolicy()
	if got := policy.Classify(CompactionMetrics{}); got != CompactionPriorityBackground {
		t.Fatalf("background classify = %v, want %v", got, CompactionPriorityBackground)
	}
	if got := policy.Classify(CompactionMetrics{L0Files: policy.NormalL0Files}); got != CompactionPriorityNormal {
		t.Fatalf("normal classify = %v, want %v", got, CompactionPriorityNormal)
	}
	if got := policy.Classify(CompactionMetrics{PendingCompactionBytes: policy.CriticalPendingCompactionBytes}); got != CompactionPriorityCritical {
		t.Fatalf("critical classify = %v, want %v", got, CompactionPriorityCritical)
	}
	if got := policy.Classify(CompactionMetrics{WriteStallCount: 1}); got != CompactionPriorityCritical {
		t.Fatalf("write-stall classify = %v, want %v", got, CompactionPriorityCritical)
	}
}
