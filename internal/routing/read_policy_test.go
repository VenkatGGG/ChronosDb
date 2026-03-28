package routing

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

func TestDecideHistoricalReadUsesLeaseholderWhenLocalReplicaOwnsLease(t *testing.T) {
	t.Parallel()

	decision, err := DecideHistoricalRead(HistoricalReadRequest{
		Descriptor:     testReadDescriptor(),
		LocalReplicaID: 1,
		ReadTS:         hlc.Timestamp{WallTime: 50, Logical: 0},
	})
	if err != nil {
		t.Fatalf("decide read: %v", err)
	}
	if decision.Mode != ReadRouteLeaseholder || decision.TargetReplicaID != 1 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestDecideHistoricalReadUsesFollowerWhenClosedTimestampAllows(t *testing.T) {
	t.Parallel()

	decision, err := DecideHistoricalRead(HistoricalReadRequest{
		Descriptor:         testReadDescriptor(),
		LocalReplicaID:     2,
		ReadTS:             hlc.Timestamp{WallTime: 50, Logical: 0},
		AppliedThrough:     hlc.Timestamp{WallTime: 55, Logical: 0},
		KnownLeaseSequence: 4,
		ClosedTimestamp: &closedts.Record{
			RangeID:       7,
			LeaseSequence: 4,
			ClosedTS:      hlc.Timestamp{WallTime: 50, Logical: 0},
			PublishedAt:   hlc.Timestamp{WallTime: 55, Logical: 0},
		},
	})
	if err != nil {
		t.Fatalf("decide read: %v", err)
	}
	if decision.Mode != ReadRouteFollowerHistorical || decision.TargetReplicaID != 2 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestDecideHistoricalReadFallsBackToLeaseholderWhenTooFresh(t *testing.T) {
	t.Parallel()

	decision, err := DecideHistoricalRead(HistoricalReadRequest{
		Descriptor:         testReadDescriptor(),
		LocalReplicaID:     2,
		ReadTS:             hlc.Timestamp{WallTime: 60, Logical: 0},
		AppliedThrough:     hlc.Timestamp{WallTime: 55, Logical: 0},
		KnownLeaseSequence: 4,
		ClosedTimestamp: &closedts.Record{
			RangeID:       7,
			LeaseSequence: 4,
			ClosedTS:      hlc.Timestamp{WallTime: 50, Logical: 0},
			PublishedAt:   hlc.Timestamp{WallTime: 55, Logical: 0},
		},
	})
	if err != nil {
		t.Fatalf("decide read: %v", err)
	}
	if decision.Mode != ReadRouteLeaseholder || decision.TargetReplicaID != 1 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func testReadDescriptor() meta.RangeDescriptor {
	return meta.RangeDescriptor{
		RangeID:    7,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("z"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 3, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
}
