package main

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

func TestScenarioStoreListAndLoad(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := writeScenarioArtifacts(root, "minority-partition", "pass", 1, ""); err != nil {
		t.Fatalf("write pass artifacts: %v", err)
	}
	if err := writeScenarioArtifacts(root, "crash-during-staging", "fail", 3, "staging recovery mismatch"); err != nil {
		t.Fatalf("write fail artifacts: %v", err)
	}

	store, err := newScenarioStore(root)
	if err != nil {
		t.Fatalf("new scenario store: %v", err)
	}

	runs, err := store.ListRuns()
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("run count = %d, want 2", len(runs))
	}
	if runs[0].RunID != "crash-during-staging" || runs[1].RunID != "minority-partition" {
		t.Fatalf("runs = %+v, want newest crash-during-staging first", runs)
	}

	detail, err := store.LoadRun("crash-during-staging")
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if detail.Run.Status != "fail" || detail.Manifest.Scenario != "crash-during-staging" {
		t.Fatalf("detail = %+v", detail)
	}
	if detail.Handoff == nil || detail.Handoff.Manifest.Scenario != "crash-during-staging" {
		t.Fatalf("handoff = %+v, want crash-during-staging handoff", detail.Handoff)
	}

	if _, err := store.LoadRun("missing-run"); !errors.Is(err, adminapi.ErrScenarioRunNotFound) {
		t.Fatalf("missing run err = %v, want ErrScenarioRunNotFound", err)
	}
}

func writeScenarioArtifacts(root, name, status string, failedStep int, failure string) error {
	scenario := systemtest.Scenario{
		Name:  name,
		Nodes: []uint64{1, 2, 3},
		Steps: []systemtest.Step{{Action: systemtest.ActionWait, Duration: 10 * time.Millisecond}},
	}
	report := systemtest.RunReport{
		ScenarioName: name,
		StartedAt:    time.Unix(10, 0).UTC(),
		FinishedAt:   time.Unix(10+int64(failedStep), 0).UTC(),
		Steps: []systemtest.StepReport{{
			Index:      1,
			Action:     systemtest.ActionWait,
			StartedAt:  time.Unix(10, 0).UTC(),
			FinishedAt: time.Unix(11, 0).UTC(),
		}},
	}
	if status == "fail" {
		report.Steps[0].Error = failure
	}
	artifacts, err := systemtest.BuildRunArtifacts(scenario, report, map[uint64][]systemtest.NodeLogEntry{
		1: {{Timestamp: time.Unix(10, 0).UTC(), Message: "node up"}},
	})
	if err != nil {
		return err
	}
	if status == "fail" {
		artifacts.Summary.Status = "fail"
		artifacts.Summary.FailedStep = failedStep
		artifacts.Summary.Failure = failure
		artifacts.Summary.FinishedAt = time.Unix(100+int64(failedStep), 0).UTC()
	} else {
		artifacts.Summary.FinishedAt = time.Unix(90, 0).UTC()
	}
	runRoot := filepath.Join(root, name)
	if err := artifacts.WriteDir(runRoot); err != nil {
		return err
	}
	return systemtest.WriteHandoffBundle(runRoot, scenario)
}
