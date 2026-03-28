package admission

import (
	"errors"
	"testing"
)

func TestControllerReservesCriticalCapacity(t *testing.T) {
	t.Parallel()

	controller := testController(t)

	bg1, err := controller.Acquire(Request{Priority: PriorityBackground, Work: WorkSnapshotSend})
	if err != nil {
		t.Fatalf("acquire background 1: %v", err)
	}
	defer bg1.Release()
	bg2, err := controller.Acquire(Request{Priority: PriorityBackground, Work: WorkSnapshotSend})
	if err != nil {
		t.Fatalf("acquire background 2: %v", err)
	}
	defer bg2.Release()

	if _, err := controller.Acquire(Request{Priority: PriorityBackground, Work: WorkRebalance}); !errors.Is(err, ErrRejected) {
		t.Fatalf("third background acquire error = %v, want %v", err, ErrRejected)
	}
	critical, err := controller.Acquire(Request{Priority: PriorityCritical, Work: WorkRaftProgress})
	if err != nil {
		t.Fatalf("acquire critical: %v", err)
	}
	defer critical.Release()
	if snapshot := controller.Snapshot(); snapshot.TotalInFlight != 3 || snapshot.CriticalInFlight != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

func TestControllerReservesNormalCapacity(t *testing.T) {
	t.Parallel()

	controller := testController(t)

	bg1, err := controller.Acquire(Request{Priority: PriorityBackground, Work: WorkSnapshotSend})
	if err != nil {
		t.Fatalf("acquire background 1: %v", err)
	}
	defer bg1.Release()
	bg2, err := controller.Acquire(Request{Priority: PriorityBackground, Work: WorkRebalance})
	if err != nil {
		t.Fatalf("acquire background 2: %v", err)
	}
	defer bg2.Release()

	if _, err := controller.Acquire(Request{Priority: PriorityBackground, Work: WorkRebalance}); !errors.Is(err, ErrRejected) {
		t.Fatalf("third background acquire error = %v, want %v", err, ErrRejected)
	}
	normal, err := controller.Acquire(Request{Priority: PriorityNormal, Work: WorkIntentResolution})
	if err != nil {
		t.Fatalf("acquire normal: %v", err)
	}
	defer normal.Release()
	if snapshot := controller.Snapshot(); snapshot.NormalInFlight != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

func TestControllerReleaseFreesCapacity(t *testing.T) {
	t.Parallel()

	controller := testController(t)
	grant, err := controller.Acquire(Request{Priority: PriorityNormal, Work: WorkGC, Units: 2})
	if err != nil {
		t.Fatalf("acquire normal: %v", err)
	}
	grant.Release()

	if snapshot := controller.Snapshot(); snapshot.TotalInFlight != 0 {
		t.Fatalf("snapshot after release = %+v, want empty", snapshot)
	}
	if _, err := controller.Acquire(Request{Priority: PriorityBackground, Work: WorkSnapshotSend, Units: 2}); err != nil {
		t.Fatalf("background acquire after release: %v", err)
	}
}

func TestCompactionEscalatesAtThreshold(t *testing.T) {
	t.Parallel()

	controller := testController(t)

	priority, err := controller.PriorityForWork(WorkCompaction, PriorityBackground, CompactionPressure{
		L0Files: 7,
	})
	if err != nil {
		t.Fatalf("priority for work: %v", err)
	}
	if priority != PriorityNormal {
		t.Fatalf("priority = %q, want %q", priority, PriorityNormal)
	}

	priority, err = controller.PriorityForWork(WorkCompaction, PriorityBackground, CompactionPressure{
		PendingCompactionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("priority for work: %v", err)
	}
	if priority != PriorityNormal {
		t.Fatalf("priority = %q, want %q", priority, PriorityNormal)
	}
}

func testController(t *testing.T) *Controller {
	t.Helper()

	controller, err := NewController(Config{
		TotalSlots:                      4,
		CriticalReserve:                 1,
		NormalReserve:                   1,
		CompactionL0Threshold:           6,
		CompactionPendingBytesThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	return controller
}
