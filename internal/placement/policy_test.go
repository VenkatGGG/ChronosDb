package placement

import "testing"

func TestCompileRegionalZoneFailure(t *testing.T) {
	t.Parallel()

	compiled, err := Compile(Policy{
		PlacementMode:    ModeRegional,
		PreferredRegions: []string{"us-east1"},
	})
	if err != nil {
		t.Fatalf("compile regional policy: %v", err)
	}
	if compiled.ReplicaCount != 3 {
		t.Fatalf("replica count = %d, want 3", compiled.ReplicaCount)
	}
	if compiled.MinDistinctRegions != 1 {
		t.Fatalf("min distinct regions = %d, want 1", compiled.MinDistinctRegions)
	}
	if len(compiled.LeasePreferences) != 1 || compiled.LeasePreferences[0] != "us-east1" {
		t.Fatalf("lease preferences = %+v, want [us-east1]", compiled.LeasePreferences)
	}
}

func TestCompileHomeRegionRegionFailure(t *testing.T) {
	t.Parallel()

	compiled, err := Compile(Policy{
		PlacementMode:    ModeHomeRegion,
		HomeRegion:       "us-east1",
		PreferredRegions: []string{"us-west1", "europe-west1", "us-east1"},
	})
	if err != nil {
		t.Fatalf("compile home-region policy: %v", err)
	}
	if compiled.ReplicaCount != 5 {
		t.Fatalf("replica count = %d, want 5", compiled.ReplicaCount)
	}
	if compiled.MinDistinctRegions != 3 {
		t.Fatalf("min distinct regions = %d, want 3", compiled.MinDistinctRegions)
	}
	if len(compiled.LeasePreferences) != 1 || compiled.LeasePreferences[0] != "us-east1" {
		t.Fatalf("lease preferences = %+v, want [us-east1]", compiled.LeasePreferences)
	}
}

func TestCompileGlobalRegionFailure(t *testing.T) {
	t.Parallel()

	compiled, err := Compile(Policy{
		PlacementMode:    ModeGlobal,
		PreferredRegions: []string{"us-east1", "us-west1", "europe-west1"},
	})
	if err != nil {
		t.Fatalf("compile global policy: %v", err)
	}
	if compiled.ReplicaCount != 5 {
		t.Fatalf("replica count = %d, want 5", compiled.ReplicaCount)
	}
	if compiled.MinDistinctRegions != 3 {
		t.Fatalf("min distinct regions = %d, want 3", compiled.MinDistinctRegions)
	}
}

func TestCompileRejectsInvalidRegionalPolicy(t *testing.T) {
	t.Parallel()

	if _, err := Compile(Policy{
		PlacementMode:    ModeRegional,
		PreferredRegions: []string{"us-east1", "us-west1"},
	}); err == nil {
		t.Fatalf("expected invalid REGIONAL placement to be rejected")
	}
}

func TestCompileRejectsInsufficientRegionsForRegionFailure(t *testing.T) {
	t.Parallel()

	if _, err := Compile(Policy{
		PlacementMode:    ModeGlobal,
		PreferredRegions: []string{"us-east1", "us-west1"},
	}); err == nil {
		t.Fatalf("expected insufficient regions for REGION_FAILURE")
	}
}
