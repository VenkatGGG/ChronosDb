package routing

import (
	"strings"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
)

func TestDecideHistoricalReadUsesLeaseholderWhenLocalReplicaOwnsLease(t *testing.T) {
	t.Parallel()

	decision, err := DecideHistoricalRead(HistoricalReadRequest{
		Descriptor:        testPlacedReadDescriptor(),
		LocalReplicaID:    1,
		ReadTS:            hlc.Timestamp{WallTime: 50, Logical: 0},
		ReplicaLocalities: testReplicaLocalities(),
	})
	if err != nil {
		t.Fatalf("decide read: %v", err)
	}
	if decision.Mode != ReadRouteLeaseholder || decision.TargetReplicaID != 1 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.LeaseholderRegion != "us-east1" || !decision.LocalityPreferred {
		t.Fatalf("decision locality = %+v, want preferred us-east1 leaseholder", decision)
	}
}

func TestDecideHistoricalReadUsesFollowerWhenClosedTimestampAllowsInPreferredRegion(t *testing.T) {
	t.Parallel()

	decision, err := DecideHistoricalRead(HistoricalReadRequest{
		Descriptor:         testPlacedReadDescriptor(),
		LocalReplicaID:     2,
		ReadTS:             hlc.Timestamp{WallTime: 50, Logical: 0},
		AppliedThrough:     hlc.Timestamp{WallTime: 55, Logical: 0},
		KnownLeaseSequence: 4,
		ReplicaLocalities: []ReplicaLocality{
			{ReplicaID: 1, Region: "us-east1"},
			{ReplicaID: 2, Region: "us-east1"},
			{ReplicaID: 3, Region: "us-west1"},
		},
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
	if !decision.LocalityPreferred || decision.TargetRegion != "us-east1" || decision.PreferredRegion != "us-east1" {
		t.Fatalf("decision locality = %+v, want preferred local follower in us-east1", decision)
	}
}

func TestDecideHistoricalReadPrefersLeaseholderWhenFollowerRegionIsNotPreferred(t *testing.T) {
	t.Parallel()

	decision, err := DecideHistoricalRead(HistoricalReadRequest{
		Descriptor:         testPlacedReadDescriptor(),
		LocalReplicaID:     2,
		ReadTS:             hlc.Timestamp{WallTime: 50, Logical: 0},
		AppliedThrough:     hlc.Timestamp{WallTime: 55, Logical: 0},
		KnownLeaseSequence: 4,
		ReplicaLocalities:  testReplicaLocalities(),
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
	if !strings.Contains(decision.Reason, "placement prefers leaseholder region") {
		t.Fatalf("reason = %q, want placement-aware leaseholder routing", decision.Reason)
	}
}

func TestDecideHistoricalReadFallsBackToLeaseholderWhenTooFresh(t *testing.T) {
	t.Parallel()

	decision, err := DecideHistoricalRead(HistoricalReadRequest{
		Descriptor:         testPlacedReadDescriptor(),
		LocalReplicaID:     2,
		ReadTS:             hlc.Timestamp{WallTime: 60, Logical: 0},
		AppliedThrough:     hlc.Timestamp{WallTime: 55, Logical: 0},
		KnownLeaseSequence: 4,
		ReplicaLocalities:  testReplicaLocalities(),
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
	if decision.FreshnessGap.WallTime != 10 || !strings.Contains(decision.Reason, "freshness gap 10.0") {
		t.Fatalf("decision freshness = %+v, want 10.0 wall-time gap", decision)
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

func testPlacedReadDescriptor() meta.RangeDescriptor {
	desc := testReadDescriptor()
	desc.PlacementPolicy = &placement.Policy{
		PlacementMode:    placement.ModeHomeRegion,
		HomeRegion:       "us-east1",
		PreferredRegions: []string{"us-east1", "us-west1", "europe-west1"},
	}
	return desc
}

func testReplicaLocalities() []ReplicaLocality {
	return []ReplicaLocality{
		{ReplicaID: 1, Region: "us-east1"},
		{ReplicaID: 2, Region: "us-west1"},
		{ReplicaID: 3, Region: "europe-west1"},
	}
}
