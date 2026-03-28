package systemtest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildHandoffBundleMapsManifestActions(t *testing.T) {
	t.Parallel()

	scenario, err := AmbiguousCommitRecoveryScenario(AmbiguousCommitRecoveryConfig{
		Name:          "handoff-ambiguous",
		Nodes:         []uint64{1, 2, 3},
		GatewayNodeID: 1,
		TxnLabel:      "transfer-42",
		AckDelay:      50 * time.Millisecond,
		SettleFor:     100 * time.Millisecond,
		PartitionBefore: &PartitionSpec{
			Left:  []uint64{1},
			Right: []uint64{2, 3},
		},
	})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	bundle, err := BuildHandoffBundle(scenario)
	if err != nil {
		t.Fatalf("build handoff bundle: %v", err)
	}
	if bundle.Version != handoffVersion {
		t.Fatalf("bundle version = %q, want %q", bundle.Version, handoffVersion)
	}
	if len(bundle.Operations) != 6 {
		t.Fatalf("operations = %+v, want 6 action mappings", bundle.Operations)
	}
	if bundle.Manifest.Steps[1].Action != ActionAmbiguousCommit {
		t.Fatalf("manifest steps = %+v, want ambiguous commit step", bundle.Manifest.Steps)
	}
}

func TestWriteHandoffBundlePersistsJSON(t *testing.T) {
	t.Parallel()

	scenario, err := CrashRecoveryScenario(CrashRecoveryConfig{
		Name:      "handoff-crash",
		Nodes:     []uint64{1, 2, 3},
		NodeID:    2,
		Downtime:  time.Second,
		SettleFor: time.Second,
	})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	root := t.TempDir()
	if err := WriteHandoffBundle(root, scenario); err != nil {
		t.Fatalf("write handoff bundle: %v", err)
	}
	payload, err := os.ReadFile(filepath.Join(root, "handoff.json"))
	if err != nil {
		t.Fatalf("read handoff json: %v", err)
	}
	var bundle HandoffBundle
	if err := json.Unmarshal(payload, &bundle); err != nil {
		t.Fatalf("unmarshal handoff json: %v", err)
	}
	if bundle.Manifest.Steps[0].Action != ActionCrashNode {
		t.Fatalf("first handoff step = %+v, want crash node", bundle.Manifest.Steps[0])
	}
}

func TestLoadHandoffBundleRoundTrip(t *testing.T) {
	t.Parallel()

	scenario, err := PartitionRecoveryScenario(PartitionRecoveryConfig{
		Name:  "handoff-load",
		Nodes: []uint64{1, 2, 3},
		Partition: PartitionSpec{
			Left:  []uint64{1},
			Right: []uint64{2, 3},
		},
		SettleFor: time.Second,
	})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	root := t.TempDir()
	if err := WriteHandoffBundle(root, scenario); err != nil {
		t.Fatalf("write handoff: %v", err)
	}
	bundle, err := LoadHandoffBundle(filepath.Join(root, "handoff.json"))
	if err != nil {
		t.Fatalf("load handoff bundle: %v", err)
	}
	if bundle.Manifest.Scenario != scenario.Name {
		t.Fatalf("handoff manifest scenario = %q, want %q", bundle.Manifest.Scenario, scenario.Name)
	}
	if _, err := os.Stat(filepath.Join(root, "handoff.json")); err != nil {
		t.Fatalf("stat handoff.json: %v", err)
	}
}
