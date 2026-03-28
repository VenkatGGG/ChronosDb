package closedts

import (
	"errors"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
)

func TestPublishAdvancesMonotonicallyWithinLeaseSequence(t *testing.T) {
	t.Parallel()

	record := baseLeaseRecord(t)
	publisher := NewPublisher()

	first, advanced, err := publisher.Publish(nil, PublishInput{
		RangeID:     7,
		Lease:       record,
		LastApplied: hlc.Timestamp{WallTime: 90, Logical: 0},
		Now:         hlc.Timestamp{WallTime: 100, Logical: 0},
		MaxOffset:   5,
	})
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if !advanced {
		t.Fatalf("expected first publication to advance")
	}

	second, advanced, err := publisher.Publish(&first, PublishInput{
		RangeID:     7,
		Lease:       record,
		LastApplied: hlc.Timestamp{WallTime: 95, Logical: 0},
		Now:         hlc.Timestamp{WallTime: 110, Logical: 0},
		MaxOffset:   5,
	})
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	if !advanced {
		t.Fatalf("expected second publication to advance")
	}
	if second.ClosedTS.Compare(first.ClosedTS) <= 0 {
		t.Fatalf("closed timestamp did not advance: first=%v second=%v", first.ClosedTS, second.ClosedTS)
	}
}

func TestPublishStallsOnIntentBacklog(t *testing.T) {
	t.Parallel()

	record := baseLeaseRecord(t)
	publisher := NewPublisher()
	oldestIntent := hlc.Timestamp{WallTime: 80, Logical: 2}

	published, advanced, err := publisher.Publish(nil, PublishInput{
		RangeID:                  7,
		Lease:                    record,
		LastApplied:              hlc.Timestamp{WallTime: 100, Logical: 0},
		Now:                      hlc.Timestamp{WallTime: 120, Logical: 0},
		MaxOffset:                5,
		OldestUnresolvedIntentTS: &oldestIntent,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !advanced {
		t.Fatalf("expected publication with intent backlog to still publish")
	}
	if published.ClosedTS.Compare(hlc.Timestamp{WallTime: 80, Logical: 1}) != 0 {
		t.Fatalf("closed timestamp = %v, want 80.1", published.ClosedTS)
	}
}

func TestPublishStallsUnderClockViolation(t *testing.T) {
	t.Parallel()

	record := baseLeaseRecord(t)
	publisher := NewPublisher()
	previous := Record{
		RangeID:       7,
		LeaseSequence: record.Sequence,
		ClosedTS:      hlc.Timestamp{WallTime: 90, Logical: 0},
		PublishedAt:   hlc.Timestamp{WallTime: 95, Logical: 0},
	}

	published, advanced, err := publisher.Publish(&previous, PublishInput{
		RangeID:             7,
		Lease:               record,
		LastApplied:         hlc.Timestamp{WallTime: 100, Logical: 0},
		Now:                 hlc.Timestamp{WallTime: 120, Logical: 0},
		MaxOffset:           5,
		ClockOffsetExceeded: true,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if advanced {
		t.Fatalf("clock violation must not advance closed timestamp")
	}
	if published != previous {
		t.Fatalf("publication changed during clock violation: got %+v want %+v", published, previous)
	}
}

func TestLeaseSequenceInvalidatesStalePublication(t *testing.T) {
	t.Parallel()

	record := baseLeaseRecord(t)
	publisher := NewPublisher()

	first, advanced, err := publisher.Publish(nil, PublishInput{
		RangeID:     7,
		Lease:       record,
		LastApplied: hlc.Timestamp{WallTime: 90, Logical: 0},
		Now:         hlc.Timestamp{WallTime: 100, Logical: 0},
		MaxOffset:   5,
	})
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if !advanced {
		t.Fatalf("expected first publication to advance")
	}

	transferred, err := record.Transfer(2, 9, hlc.Timestamp{WallTime: 101, Logical: 0}, hlc.Timestamp{WallTime: 200, Logical: 0})
	if err != nil {
		t.Fatalf("transfer lease: %v", err)
	}
	second, advanced, err := publisher.Publish(&first, PublishInput{
		RangeID:     7,
		Lease:       transferred,
		LastApplied: hlc.Timestamp{WallTime: 92, Logical: 0},
		Now:         hlc.Timestamp{WallTime: 102, Logical: 0},
		MaxOffset:   5,
	})
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	if !advanced {
		t.Fatalf("expected new lease sequence to publish independently")
	}
	if second.LeaseSequence != transferred.Sequence {
		t.Fatalf("lease sequence = %d, want %d", second.LeaseSequence, transferred.Sequence)
	}
	if err := first.CanServeFollowerRead(FollowerReadRequest{
		ReadTS:             first.ClosedTS,
		AppliedThrough:     first.ClosedTS,
		KnownLeaseSequence: transferred.Sequence,
	}); !errors.Is(err, ErrClosedTimestampLeaseMismatch) {
		t.Fatalf("old publication should be rejected after transfer, got %v", err)
	}
}

func TestFollowerReadRejectsTooFreshReads(t *testing.T) {
	t.Parallel()

	record := Record{
		RangeID:       7,
		LeaseSequence: 3,
		ClosedTS:      hlc.Timestamp{WallTime: 90, Logical: 0},
		PublishedAt:   hlc.Timestamp{WallTime: 95, Logical: 0},
	}
	err := record.CanServeFollowerRead(FollowerReadRequest{
		ReadTS:             hlc.Timestamp{WallTime: 91, Logical: 0},
		AppliedThrough:     hlc.Timestamp{WallTime: 90, Logical: 0},
		KnownLeaseSequence: 3,
	})
	if !errors.Is(err, ErrFollowerReadTooFresh) {
		t.Fatalf("expected follower read too fresh error, got %v", err)
	}
}

func TestFollowerReadRequiresAppliedPublication(t *testing.T) {
	t.Parallel()

	record := Record{
		RangeID:       7,
		LeaseSequence: 3,
		ClosedTS:      hlc.Timestamp{WallTime: 90, Logical: 0},
		PublishedAt:   hlc.Timestamp{WallTime: 95, Logical: 0},
	}
	err := record.CanServeFollowerRead(FollowerReadRequest{
		ReadTS:             hlc.Timestamp{WallTime: 90, Logical: 0},
		AppliedThrough:     hlc.Timestamp{WallTime: 89, Logical: 0},
		KnownLeaseSequence: 3,
	})
	if !errors.Is(err, ErrClosedTimestampNotApplied) {
		t.Fatalf("expected not-applied error, got %v", err)
	}
}

func TestClosedTimestampBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	record := Record{
		RangeID:       7,
		LeaseSequence: 3,
		ClosedTS:      hlc.Timestamp{WallTime: 90, Logical: 1},
		PublishedAt:   hlc.Timestamp{WallTime: 95, Logical: 0},
	}
	payload, err := record.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	var decoded Record
	if err := decoded.UnmarshalBinary(payload); err != nil {
		t.Fatalf("unmarshal record: %v", err)
	}
	if decoded != record {
		t.Fatalf("decoded record = %+v, want %+v", decoded, record)
	}
}

func baseLeaseRecord(t *testing.T) lease.Record {
	t.Helper()

	record, err := lease.NewRecord(
		1,
		7,
		hlc.Timestamp{WallTime: 50, Logical: 0},
		hlc.Timestamp{WallTime: 150, Logical: 0},
		3,
	)
	if err != nil {
		t.Fatalf("new lease record: %v", err)
	}
	return record
}
