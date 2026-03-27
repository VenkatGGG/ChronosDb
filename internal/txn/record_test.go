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
