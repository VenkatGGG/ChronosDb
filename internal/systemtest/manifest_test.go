package systemtest

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildManifestFromScenario(t *testing.T) {
	t.Parallel()

	scenario, err := AmbiguousCommitRecoveryScenario(AmbiguousCommitRecoveryConfig{
		Name:          "manifested-transfer",
		Nodes:         []uint64{1, 2, 3},
		GatewayNodeID: 1,
		TxnLabel:      "transfer-7",
		AckDelay:      250 * time.Millisecond,
		SettleFor:     2 * time.Second,
		PartitionBefore: &PartitionSpec{
			Left:  []uint64{1},
			Right: []uint64{2, 3},
		},
	})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	manifest, err := BuildManifest(scenario)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	if manifest.Version != manifestVersion || manifest.Scenario != scenario.Name {
		t.Fatalf("manifest header = %+v", manifest)
	}
	if len(manifest.Steps) != 4 {
		t.Fatalf("steps = %+v, want 4", manifest.Steps)
	}
	if manifest.Steps[0].Action != ActionPartition || manifest.Steps[1].Action != ActionAmbiguousCommit {
		t.Fatalf("manifest steps = %+v", manifest.Steps)
	}
	if manifest.Steps[1].AckDelay != "250ms" || manifest.Steps[1].TxnLabel != "transfer-7" {
		t.Fatalf("ambiguous manifest step = %+v", manifest.Steps[1])
	}
}

func TestExportManifestProducesJSON(t *testing.T) {
	t.Parallel()

	scenario, err := CrashRecoveryScenario(CrashRecoveryConfig{
		Name:      "restart-node-2",
		Nodes:     []uint64{1, 2, 3},
		NodeID:    2,
		Downtime:  5 * time.Second,
		SettleFor: 4 * time.Second,
	})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	payload, err := ExportManifest(scenario)
	if err != nil {
		t.Fatalf("export manifest: %v", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		t.Fatalf("decode manifest json: %v", err)
	}
	if manifest.Steps[0].Action != ActionCrashNode || manifest.Steps[0].NodeID != 2 {
		t.Fatalf("first step = %+v, want crash node 2", manifest.Steps[0])
	}
	if manifest.Steps[1].Duration != "5s" || manifest.Steps[3].Duration != "4s" {
		t.Fatalf("wait durations = %+v, want 5s and 4s", manifest.Steps)
	}
}

func TestScenarioFromManifestRoundTrip(t *testing.T) {
	t.Parallel()

	original, err := AmbiguousCommitRecoveryScenario(AmbiguousCommitRecoveryConfig{
		Name:          "manifest-roundtrip",
		Nodes:         []uint64{1, 2, 3},
		GatewayNodeID: 1,
		TxnLabel:      "transfer-8",
		AckDelay:      200 * time.Millisecond,
		SettleFor:     time.Second,
		PartitionBefore: &PartitionSpec{
			Left:  []uint64{1},
			Right: []uint64{2, 3},
		},
	})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	manifest, err := BuildManifest(original)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	roundTrip, err := ScenarioFromManifest(manifest)
	if err != nil {
		t.Fatalf("scenario from manifest: %v", err)
	}
	if roundTrip.Name != original.Name || len(roundTrip.Steps) != len(original.Steps) {
		t.Fatalf("round-trip scenario = %+v, want %+v", roundTrip, original)
	}
	if roundTrip.Steps[1].AmbiguousCommit == nil || roundTrip.Steps[1].AmbiguousCommit.TxnLabel != "transfer-8" {
		t.Fatalf("ambiguous step = %+v, want transfer-8", roundTrip.Steps[1])
	}
}
