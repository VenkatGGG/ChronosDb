package lease

import (
	"errors"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

func TestTransferBumpsSequenceAndMovesHolder(t *testing.T) {
	t.Parallel()

	record, err := NewRecord(1, 10, hlc.Timestamp{WallTime: 100, Logical: 1}, hlc.Timestamp{WallTime: 200, Logical: 0}, 7)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}
	transferred, err := record.Transfer(2, 20, hlc.Timestamp{WallTime: 150, Logical: 0}, hlc.Timestamp{WallTime: 250, Logical: 0})
	if err != nil {
		t.Fatalf("transfer lease: %v", err)
	}
	if transferred.HolderReplicaID != 2 {
		t.Fatalf("holder = %d, want 2", transferred.HolderReplicaID)
	}
	if transferred.Sequence != record.Sequence+1 {
		t.Fatalf("sequence = %d, want %d", transferred.Sequence, record.Sequence+1)
	}
	if transferred.StartTS.Compare(hlc.Timestamp{WallTime: 150, Logical: 0}) != 0 {
		t.Fatalf("start ts = %v, want 150.0", transferred.StartTS)
	}
}

func TestRenewKeepsSequence(t *testing.T) {
	t.Parallel()

	record, err := NewRecord(1, 10, hlc.Timestamp{WallTime: 100, Logical: 1}, hlc.Timestamp{WallTime: 200, Logical: 0}, 7)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}
	renewed, err := record.Renew(record.StartTS, hlc.Timestamp{WallTime: 300, Logical: 0})
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	if renewed.Sequence != record.Sequence {
		t.Fatalf("renewed sequence = %d, want %d", renewed.Sequence, record.Sequence)
	}
}

func TestFastReadRequiresMatchingLivenessAndSequence(t *testing.T) {
	t.Parallel()

	record, err := NewRecord(1, 10, hlc.Timestamp{WallTime: 100, Logical: 1}, hlc.Timestamp{WallTime: 200, Logical: 0}, 7)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}

	err = record.CanServeFastRead(FastReadRequest{
		ReplicaID:            1,
		AppliedLeaseSequence: 7,
		CurrentLivenessEpoch: 11,
		Now:                  hlc.Timestamp{WallTime: 150, Logical: 0},
		ReadTimestamp:        hlc.Timestamp{WallTime: 150, Logical: 0},
		SafeReadFrontier:     hlc.Timestamp{WallTime: 160, Logical: 0},
	})
	if !errors.Is(err, ErrReadIndexRequired) {
		t.Fatalf("liveness mismatch error = %v, want %v", err, ErrReadIndexRequired)
	}

	err = record.CanServeFastRead(FastReadRequest{
		ReplicaID:            1,
		AppliedLeaseSequence: 8,
		CurrentLivenessEpoch: 10,
		Now:                  hlc.Timestamp{WallTime: 150, Logical: 0},
		ReadTimestamp:        hlc.Timestamp{WallTime: 150, Logical: 0},
		SafeReadFrontier:     hlc.Timestamp{WallTime: 160, Logical: 0},
	})
	if !errors.Is(err, ErrReadIndexRequired) {
		t.Fatalf("sequence mismatch error = %v, want %v", err, ErrReadIndexRequired)
	}
}

func TestFastReadFallsBackOnClockViolation(t *testing.T) {
	t.Parallel()

	record, err := NewRecord(1, 10, hlc.Timestamp{WallTime: 100, Logical: 1}, hlc.Timestamp{WallTime: 200, Logical: 0}, 7)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}

	err = record.CanServeFastRead(FastReadRequest{
		ReplicaID:            1,
		AppliedLeaseSequence: 7,
		CurrentLivenessEpoch: 10,
		Now:                  hlc.Timestamp{WallTime: 150, Logical: 0},
		ReadTimestamp:        hlc.Timestamp{WallTime: 150, Logical: 0},
		SafeReadFrontier:     hlc.Timestamp{WallTime: 160, Logical: 0},
		ClockOffsetExceeded:  true,
	})
	if !errors.Is(err, ErrReadIndexRequired) {
		t.Fatalf("clock violation error = %v, want %v", err, ErrReadIndexRequired)
	}
}

func TestLeaseBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	record, err := NewRecord(1, 10, hlc.Timestamp{WallTime: 100, Logical: 1}, hlc.Timestamp{WallTime: 200, Logical: 0}, 7)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}
	payload, err := record.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}
	var decoded Record
	if err := decoded.UnmarshalBinary(payload); err != nil {
		t.Fatalf("unmarshal lease: %v", err)
	}
	if decoded != record {
		t.Fatalf("decoded lease = %+v, want %+v", decoded, record)
	}
}
