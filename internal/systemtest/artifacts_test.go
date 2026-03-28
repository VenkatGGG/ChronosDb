package systemtest

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestBuildRunArtifactsPassAndFailSummary(t *testing.T) {
	t.Parallel()

	scenario, err := CrashRecoveryScenario(CrashRecoveryConfig{
		Name:      "artifacts-crash",
		Nodes:     []uint64{1, 2, 3},
		NodeID:    2,
		Downtime:  time.Second,
		SettleFor: time.Second,
	})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	report := RunReport{
		ScenarioName: scenario.Name,
		StartedAt:    time.Unix(100, 0).UTC(),
		FinishedAt:   time.Unix(110, 0).UTC(),
		Steps: []StepReport{
			{Index: 1, Action: ActionCrashNode},
			{Index: 2, Action: ActionWait, Error: "timeout waiting for restart"},
		},
	}
	artifacts, err := BuildRunArtifacts(scenario, report, map[uint64][]NodeLogEntry{
		1: {{Timestamp: time.Unix(101, 0).UTC(), Message: "healthy"}},
		2: {{Timestamp: time.Unix(102, 0).UTC(), Message: "crashed"}},
	})
	if err != nil {
		t.Fatalf("build run artifacts: %v", err)
	}
	if artifacts.Summary.Status != "fail" {
		t.Fatalf("summary status = %q, want fail", artifacts.Summary.Status)
	}
	if artifacts.Summary.FailedStep != 2 || artifacts.Summary.Failure != "timeout waiting for restart" {
		t.Fatalf("summary failure = %+v", artifacts.Summary)
	}
	if artifacts.Summary.NodeLogCount != 2 {
		t.Fatalf("node log count = %d, want 2", artifacts.Summary.NodeLogCount)
	}
}

func TestWriteAndLoadRunArtifacts(t *testing.T) {
	t.Parallel()

	scenario, err := PartitionRecoveryScenario(PartitionRecoveryConfig{
		Name:  "artifacts-partition",
		Nodes: []uint64{1, 2, 3},
		Partition: PartitionSpec{
			Left:  []uint64{1},
			Right: []uint64{2, 3},
		},
		SettleFor: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	report := RunReport{
		ScenarioName: scenario.Name,
		StartedAt:    time.Unix(200, 0).UTC(),
		FinishedAt:   time.Unix(205, 0).UTC(),
		Steps: []StepReport{
			{Index: 1, Action: ActionPartition},
			{Index: 2, Action: ActionWait},
			{Index: 3, Action: ActionHeal},
		},
	}
	artifacts, err := BuildRunArtifacts(scenario, report, map[uint64][]NodeLogEntry{
		1: {{Timestamp: time.Unix(201, 0).UTC(), Message: "partition applied"}},
		2: {{Timestamp: time.Unix(201, 0).UTC(), Message: "isolated from node 1"}},
	})
	if err != nil {
		t.Fatalf("build run artifacts: %v", err)
	}

	root := t.TempDir()
	if err := artifacts.WriteDir(root); err != nil {
		t.Fatalf("write dir: %v", err)
	}
	for _, path := range []string{
		filepath.Join(root, "manifest.json"),
		filepath.Join(root, "report.json"),
		filepath.Join(root, "summary.json"),
		filepath.Join(root, "node-logs", "node-1.json"),
		filepath.Join(root, "node-logs", "node-2.json"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
	}

	loaded, err := LoadRunArtifacts(root)
	if err != nil {
		t.Fatalf("load run artifacts: %v", err)
	}
	if !reflect.DeepEqual(loaded, artifacts) {
		t.Fatalf("loaded artifacts = %+v, want %+v", loaded, artifacts)
	}
}

func TestLocalControllerSnapshotNodeLogs(t *testing.T) {
	t.Parallel()

	controller := newLocalController(t, 1, 2)
	if err := controller.Partition(context.Background(), PartitionSpec{
		Left:  []uint64{1},
		Right: []uint64{2},
	}); err != nil {
		t.Fatalf("partition: %v", err)
	}

	logs := controller.SnapshotNodeLogs()
	if len(logs[1]) == 0 || len(logs[2]) == 0 {
		t.Fatalf("snapshot logs = %+v, want entries for both nodes", logs)
	}
	if logs[1][0].Message == "" {
		t.Fatalf("node 1 first log = %+v, want populated message", logs[1][0])
	}
}
