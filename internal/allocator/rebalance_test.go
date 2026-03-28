package allocator

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
)

func TestChooseRebalanceRespectsRegionFailure(t *testing.T) {
	t.Parallel()

	desc := testRangeDescriptor(&placement.Policy{
		PlacementMode:    placement.ModeHomeRegion,
		HomeRegion:       "us-east1",
		PreferredRegions: []string{"us-east1", "us-west1", "europe-west1"},
	})
	decision, err := ChooseRebalance(desc, []NodeLoad{
		{NodeID: 1, Region: "us-east1", LoadScore: 0.95},
		{NodeID: 2, Region: "us-west1", LoadScore: 0.50},
		{NodeID: 3, Region: "europe-west1", LoadScore: 0.45},
		{NodeID: 4, Region: "us-east1", LoadScore: 0.10},
	})
	if err != nil {
		t.Fatalf("choose rebalance: %v", err)
	}
	if decision.SourceReplica.NodeID != 1 {
		t.Fatalf("source node = %d, want 1", decision.SourceReplica.NodeID)
	}
	if decision.TargetNode.NodeID != 4 {
		t.Fatalf("target node = %d, want 4", decision.TargetNode.NodeID)
	}
}

func TestChooseRebalanceAvoidsViolatingRegionCount(t *testing.T) {
	t.Parallel()

	desc := testRangeDescriptor(&placement.Policy{
		PlacementMode:    placement.ModeHomeRegion,
		HomeRegion:       "us-east1",
		PreferredRegions: []string{"us-east1", "us-west1", "europe-west1"},
	})
	_, err := ChooseRebalance(desc, []NodeLoad{
		{NodeID: 1, Region: "us-east1", LoadScore: 0.95},
		{NodeID: 2, Region: "us-west1", LoadScore: 0.05},
		{NodeID: 3, Region: "europe-west1", LoadScore: 0.04},
		{NodeID: 4, Region: "us-west1", LoadScore: 0.10},
	})
	if err == nil {
		t.Fatalf("expected rebalance to fail when only move collapses distinct-region count")
	}
}

func TestChooseRebalanceAvoidsLeaseholderWhenAlternativeExists(t *testing.T) {
	t.Parallel()

	desc := testRangeDescriptor(&placement.Policy{
		PlacementMode:    placement.ModeRegional,
		PreferredRegions: []string{"us-east1"},
	})
	desc.LeaseholderReplicaID = 1
	decision, err := ChooseRebalance(desc, []NodeLoad{
		{NodeID: 1, Region: "us-east1", LoadScore: 0.95},
		{NodeID: 2, Region: "us-east1", LoadScore: 0.90},
		{NodeID: 3, Region: "us-east1", LoadScore: 0.50},
		{NodeID: 4, Region: "us-east1", LoadScore: 0.10},
	})
	if err != nil {
		t.Fatalf("choose rebalance: %v", err)
	}
	if decision.SourceReplica.NodeID != 2 {
		t.Fatalf("source node = %d, want non-leaseholder node 2", decision.SourceReplica.NodeID)
	}
}

func testRangeDescriptor(policy *placement.Policy) meta.RangeDescriptor {
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
		LeaseholderReplicaID: 2,
		PlacementPolicy:      policy,
	}
}
