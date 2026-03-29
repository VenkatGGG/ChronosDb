package txn

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestRecordBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	record := Record{
		ID:              storage.TxnID{1, 2, 3, 4, 5},
		Status:          StatusStaging,
		ReadTS:          hlc.Timestamp{WallTime: 10, Logical: 1},
		WriteTS:         hlc.Timestamp{WallTime: 12, Logical: 0},
		MinCommitTS:     hlc.Timestamp{WallTime: 13, Logical: 2},
		Epoch:           2,
		Priority:        7,
		AnchorRangeID:   44,
		TouchedRanges:   []uint64{44, 45, 99},
		LastHeartbeatTS: hlc.Timestamp{WallTime: 14, Logical: 1},
		DeadlineTS:      hlc.Timestamp{WallTime: 50, Logical: 0},
	}

	payload, err := record.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	var decoded Record
	if err := decoded.UnmarshalBinary(payload); err != nil {
		t.Fatalf("unmarshal record: %v", err)
	}
	if decoded.ID != record.ID {
		t.Fatalf("txn id = %v, want %v", decoded.ID, record.ID)
	}
	if decoded.Status != record.Status || decoded.ReadTS != record.ReadTS || decoded.WriteTS != record.WriteTS || decoded.MinCommitTS != record.MinCommitTS {
		t.Fatalf("decoded record timestamps/status = %+v, want %+v", decoded, record)
	}
	if decoded.Epoch != record.Epoch || decoded.Priority != record.Priority || decoded.AnchorRangeID != record.AnchorRangeID {
		t.Fatalf("decoded record core fields = %+v, want %+v", decoded, record)
	}
	if len(decoded.TouchedRanges) != len(record.TouchedRanges) {
		t.Fatalf("touched ranges len = %d, want %d", len(decoded.TouchedRanges), len(record.TouchedRanges))
	}
	for i := range record.TouchedRanges {
		if decoded.TouchedRanges[i] != record.TouchedRanges[i] {
			t.Fatalf("touched range[%d] = %d, want %d", i, decoded.TouchedRanges[i], record.TouchedRanges[i])
		}
	}
	if decoded.LastHeartbeatTS != record.LastHeartbeatTS || decoded.DeadlineTS != record.DeadlineTS {
		t.Fatalf("heartbeat/deadline = %+v, want %+v", decoded, record)
	}
}
