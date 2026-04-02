package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

type scenarioStore struct {
	root string
}

func newScenarioStore(root string) (*scenarioStore, error) {
	if root == "" {
		return nil, fmt.Errorf("console: scenario artifact root must not be empty")
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("console: scenario artifact root %q is not a directory", root)
	}
	return &scenarioStore{root: root}, nil
}

func (s *scenarioStore) ListRuns() ([]adminapi.ScenarioRunView, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	runs := make([]adminapi.ScenarioRunView, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		view, err := s.loadRunView(entry.Name())
		if err != nil {
			return nil, err
		}
		runs = append(runs, view)
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].FinishedAt.After(runs[j].FinishedAt)
	})
	return runs, nil
}

func (s *scenarioStore) LoadRun(runID string) (adminapi.ScenarioRunDetail, error) {
	if runID == "" {
		return adminapi.ScenarioRunDetail{}, fmt.Errorf("console: scenario run id must not be empty")
	}
	root := filepath.Join(s.root, runID)
	if info, err := os.Stat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return adminapi.ScenarioRunDetail{}, fmt.Errorf("%w: %s", adminapi.ErrScenarioRunNotFound, runID)
		}
		return adminapi.ScenarioRunDetail{}, err
	} else if !info.IsDir() {
		return adminapi.ScenarioRunDetail{}, fmt.Errorf("%w: %s", adminapi.ErrScenarioRunNotFound, runID)
	}
	artifacts, err := systemtest.LoadRunArtifacts(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return adminapi.ScenarioRunDetail{}, fmt.Errorf("%w: %s", adminapi.ErrScenarioRunNotFound, runID)
		}
		return adminapi.ScenarioRunDetail{}, err
	}
	detail := adminapi.ScenarioRunDetail{
		Run:      viewFromArtifacts(runID, artifacts),
		Manifest: manifestView(artifacts.Manifest),
		Report:   reportView(artifacts.Report),
		Summary:  summaryView(artifacts.Summary),
		NodeLogs: nodeLogViews(artifacts.NodeLogs),
	}
	handoff, err := systemtest.LoadHandoffBundle(filepath.Join(root, "handoff.json"))
	if err == nil {
		next := handoffView(handoff)
		detail.Handoff = &next
	}
	return detail, nil
}

func (s *scenarioStore) loadRunView(runID string) (adminapi.ScenarioRunView, error) {
	artifacts, err := systemtest.LoadRunArtifacts(filepath.Join(s.root, runID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return adminapi.ScenarioRunView{}, fmt.Errorf("%w: %s", adminapi.ErrScenarioRunNotFound, runID)
		}
		return adminapi.ScenarioRunView{}, err
	}
	return viewFromArtifacts(runID, artifacts), nil
}

func viewFromArtifacts(runID string, artifacts systemtest.RunArtifacts) adminapi.ScenarioRunView {
	return adminapi.ScenarioRunView{
		RunID:        runID,
		ScenarioName: artifacts.Summary.ScenarioName,
		Status:       artifacts.Summary.Status,
		StartedAt:    artifacts.Summary.StartedAt,
		FinishedAt:   artifacts.Summary.FinishedAt,
		StepCount:    artifacts.Summary.StepCount,
		FailedStep:   artifacts.Summary.FailedStep,
		Failure:      artifacts.Summary.Failure,
		NodeCount:    artifacts.Summary.NodeCount,
		NodeLogCount: artifacts.Summary.NodeLogCount,
	}
}

func manifestView(manifest systemtest.Manifest) adminapi.ScenarioManifest {
	steps := make([]adminapi.ScenarioManifestStep, 0, len(manifest.Steps))
	for _, step := range manifest.Steps {
		steps = append(steps, adminapi.ScenarioManifestStep{
			Index:          step.Index,
			Action:         string(step.Action),
			PartitionLeft:  append([]uint64(nil), step.PartitionLeft...),
			PartitionRight: append([]uint64(nil), step.PartitionRight...),
			NodeID:         step.NodeID,
			Duration:       step.Duration,
			GatewayNodeID:  step.GatewayNodeID,
			TxnLabel:       step.TxnLabel,
			AckDelay:       step.AckDelay,
			DropResponse:   step.DropResponse,
		})
	}
	return adminapi.ScenarioManifest{
		Version:  manifest.Version,
		Scenario: manifest.Scenario,
		Nodes:    append([]uint64(nil), manifest.Nodes...),
		Steps:    steps,
	}
}

func handoffView(bundle systemtest.HandoffBundle) adminapi.ScenarioHandoffBundle {
	operations := make([]adminapi.ScenarioHandoffOperation, 0, len(bundle.Operations))
	for _, operation := range bundle.Operations {
		operations = append(operations, adminapi.ScenarioHandoffOperation{
			Action:            string(operation.Action),
			ExternalOperation: operation.ExternalOperation,
			Description:       operation.Description,
		})
	}
	return adminapi.ScenarioHandoffBundle{
		Version:    bundle.Version,
		Manifest:   manifestView(bundle.Manifest),
		Operations: operations,
	}
}

func reportView(report systemtest.RunReport) adminapi.ScenarioRunReport {
	steps := make([]adminapi.ScenarioStepReport, 0, len(report.Steps))
	for _, step := range report.Steps {
		steps = append(steps, adminapi.ScenarioStepReport{
			Index:      step.Index,
			Action:     string(step.Action),
			StartedAt:  step.StartedAt,
			FinishedAt: step.FinishedAt,
			Error:      step.Error,
		})
	}
	return adminapi.ScenarioRunReport{
		ScenarioName: report.ScenarioName,
		StartedAt:    report.StartedAt,
		FinishedAt:   report.FinishedAt,
		Steps:        steps,
	}
}

func summaryView(summary systemtest.RunArtifactSummary) adminapi.ScenarioRunSummary {
	return adminapi.ScenarioRunSummary{
		Version:      summary.Version,
		ScenarioName: summary.ScenarioName,
		Status:       summary.Status,
		StartedAt:    summary.StartedAt,
		FinishedAt:   summary.FinishedAt,
		StepCount:    summary.StepCount,
		FailedStep:   summary.FailedStep,
		Failure:      summary.Failure,
		NodeCount:    summary.NodeCount,
		NodeLogCount: summary.NodeLogCount,
	}
}

func nodeLogViews(logs map[uint64][]systemtest.NodeLogEntry) map[uint64][]adminapi.ScenarioNodeLogEntry {
	copyLogs := make(map[uint64][]adminapi.ScenarioNodeLogEntry, len(logs))
	for nodeID, entries := range logs {
		next := make([]adminapi.ScenarioNodeLogEntry, 0, len(entries))
		for _, entry := range entries {
			next = append(next, adminapi.ScenarioNodeLogEntry{
				Timestamp: entry.Timestamp,
				Message:   entry.Message,
			})
		}
		copyLogs[nodeID] = next
	}
	return copyLogs
}
