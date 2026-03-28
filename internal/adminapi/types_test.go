package adminapi

import (
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
)

func TestRangeViewFromDescriptor(t *testing.T) {
	t.Parallel()

	view := RangeViewFromDescriptor(meta.RangeDescriptor{
		RangeID:    7,
		Generation: 3,
		StartKey:   []byte("a"),
		EndKey:     []byte("m"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 11, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 12, Role: meta.ReplicaRoleLearner},
		},
		LeaseholderReplicaID: 1,
		PlacementPolicy: &placement.Policy{
			PlacementMode:    placement.ModeRegional,
			PreferredRegions: []string{"us-east1"},
			LeasePreferences: []string{"us-east1"},
		},
	}, "node")

	if view.RangeID != 7 || view.Generation != 3 {
		t.Fatalf("range view header = %+v", view)
	}
	if view.StartKey != "61" || view.EndKey != "6d" {
		t.Fatalf("encoded keys = start %q end %q, want 61/6d", view.StartKey, view.EndKey)
	}
	if view.LeaseholderNodeID != 11 {
		t.Fatalf("leaseholder node = %d, want 11", view.LeaseholderNodeID)
	}
	if len(view.Replicas) != 2 || view.Replicas[1].Role != "learner" {
		t.Fatalf("replicas = %+v", view.Replicas)
	}
	if view.PlacementMode == "" || len(view.PreferredRegions) != 1 || view.PreferredRegions[0] != "us-east1" {
		t.Fatalf("placement = %+v", view)
	}
}

func TestNormalizeEventProducesStableID(t *testing.T) {
	t.Parallel()

	left := NormalizeEvent(ClusterEvent{
		Timestamp: time.Unix(100, 0).UTC(),
		Type:      "partition_applied",
		NodeID:    7,
		Message:   "partition applied",
		Fields: map[string]string{
			"right": "2,3",
			"left":  "1",
		},
	})
	right := NormalizeEvent(ClusterEvent{
		Timestamp: time.Unix(100, 0).UTC(),
		Type:      "partition_applied",
		NodeID:    7,
		Message:   "partition applied",
		Fields: map[string]string{
			"left":  "1",
			"right": "2,3",
		},
	})
	if left.ID == "" {
		t.Fatal("expected normalized event id to be set")
	}
	if left.ID != right.ID {
		t.Fatalf("stable event ids mismatch: %q vs %q", left.ID, right.ID)
	}
}
