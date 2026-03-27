package txn

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

func TestRecordRestartAdvancesEpochAndTimestamps(t *testing.T) {
	t.Parallel()

	record := Record{
		ID:          txnID(1),
		Status:      StatusPending,
		ReadTS:      hlc.Timestamp{WallTime: 10, Logical: 1},
		WriteTS:     hlc.Timestamp{WallTime: 10, Logical: 2},
		MinCommitTS: hlc.Timestamp{WallTime: 12, Logical: 0},
		Epoch:       1,
		Priority:    5,
	}

	restarted, err := record.Restart(hlc.Timestamp{WallTime: 11, Logical: 0})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if restarted.Epoch != 2 {
		t.Fatalf("epoch = %d, want 2", restarted.Epoch)
	}
	if got := restarted.ReadTS; got.Compare(hlc.Timestamp{WallTime: 11, Logical: 0}) != 0 {
		t.Fatalf("read ts = %s, want 11.0", got)
	}
	if got := restarted.WriteTS; got.Compare(hlc.Timestamp{WallTime: 12, Logical: 0}) != 0 {
		t.Fatalf("write ts = %s, want 12.0", got)
	}
}

func TestRecordRestartRejectsTerminalTxn(t *testing.T) {
	t.Parallel()

	record := Record{
		ID:       txnID(2),
		Status:   StatusCommitted,
		ReadTS:   hlc.Timestamp{WallTime: 10, Logical: 1},
		WriteTS:  hlc.Timestamp{WallTime: 10, Logical: 1},
		Priority: 1,
	}
	if _, err := record.Restart(hlc.Timestamp{WallTime: 11, Logical: 0}); err == nil {
		t.Fatalf("expected restart error for terminal transaction")
	}
}

func TestRecordAnchorHeartbeatAndTouchedRanges(t *testing.T) {
	t.Parallel()

	record := Record{
		ID:       txnID(3),
		Status:   StatusPending,
		ReadTS:   hlc.Timestamp{WallTime: 10, Logical: 0},
		WriteTS:  hlc.Timestamp{WallTime: 10, Logical: 1},
		Priority: 1,
	}

	anchored, err := record.Anchor(7, hlc.Timestamp{WallTime: 11, Logical: 0})
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	if anchored.AnchorRangeID != 7 {
		t.Fatalf("anchor range = %d, want 7", anchored.AnchorRangeID)
	}
	if len(anchored.TouchedRanges) != 1 || anchored.TouchedRanges[0] != 7 {
		t.Fatalf("touched ranges = %+v, want [7]", anchored.TouchedRanges)
	}

	touched, err := anchored.TouchRange(9)
	if err != nil {
		t.Fatalf("touch range: %v", err)
	}
	if len(touched.TouchedRanges) != 2 || touched.TouchedRanges[1] != 9 {
		t.Fatalf("touched ranges = %+v, want [7 9]", touched.TouchedRanges)
	}

	heartbeat, err := touched.Heartbeat(hlc.Timestamp{WallTime: 12, Logical: 0})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if heartbeat.LastHeartbeatTS.Compare(hlc.Timestamp{WallTime: 12, Logical: 0}) != 0 {
		t.Fatalf("heartbeat ts = %s, want 12.0", heartbeat.LastHeartbeatTS)
	}
}

func TestRecordTouchRangeRequiresAnchor(t *testing.T) {
	t.Parallel()

	record := Record{
		ID:       txnID(4),
		Status:   StatusPending,
		ReadTS:   hlc.Timestamp{WallTime: 10, Logical: 0},
		WriteTS:  hlc.Timestamp{WallTime: 10, Logical: 1},
		Priority: 1,
	}
	if _, err := record.TouchRange(8); err == nil {
		t.Fatalf("expected touch range error for unanchored transaction")
	}
}
