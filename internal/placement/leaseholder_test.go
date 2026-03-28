package placement

import "testing"

func TestSelectLeaseholderFollowsHomeRegionPreference(t *testing.T) {
	t.Parallel()

	compiled, err := Compile(Policy{
		PlacementMode:    ModeHomeRegion,
		HomeRegion:       "us-east1",
		PreferredRegions: []string{"us-east1", "us-west1", "europe-west1"},
	})
	if err != nil {
		t.Fatalf("compile policy: %v", err)
	}

	selected, err := SelectLeaseholder(compiled, []ReplicaCandidate{
		{ReplicaID: 1, Region: "us-west1", Voter: true},
		{ReplicaID: 2, Region: "us-east1", Voter: true},
		{ReplicaID: 3, Region: "europe-west1", Voter: true},
	})
	if err != nil {
		t.Fatalf("select leaseholder: %v", err)
	}
	if selected.ReplicaID != 2 {
		t.Fatalf("selected replica = %d, want 2", selected.ReplicaID)
	}
}

func TestSelectLeaseholderFallsBackToAnyVoter(t *testing.T) {
	t.Parallel()

	compiled := CompiledPolicy{
		PlacementMode:    ModeGlobal,
		SurvivalGoal:     SurvivalRegionFailure,
		LeasePreferences: []string{"us-east1"},
	}

	selected, err := SelectLeaseholder(compiled, []ReplicaCandidate{
		{ReplicaID: 1, Region: "us-west1", Voter: true},
		{ReplicaID: 2, Region: "us-east1", Voter: false},
	})
	if err != nil {
		t.Fatalf("select leaseholder: %v", err)
	}
	if selected.ReplicaID != 1 {
		t.Fatalf("selected replica = %d, want 1", selected.ReplicaID)
	}
}

func TestSelectLeaseholderRejectsNoVoters(t *testing.T) {
	t.Parallel()

	compiled := CompiledPolicy{
		PlacementMode:    ModeGlobal,
		SurvivalGoal:     SurvivalRegionFailure,
		LeasePreferences: []string{"us-east1"},
	}

	if _, err := SelectLeaseholder(compiled, []ReplicaCandidate{
		{ReplicaID: 1, Region: "us-east1", Voter: false},
	}); err == nil {
		t.Fatalf("expected no-voter placement to fail")
	}
}
