package systemtest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const artifactVersion = "chronosdb.systemtest.artifacts.v1"

// NodeLogEntry is one timestamped controller/node event attached to a run artifact.
type NodeLogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

// RunArtifactSummary is the final pass/fail synopsis attached to one chaos run.
type RunArtifactSummary struct {
	Version      string    `json:"version"`
	ScenarioName string    `json:"scenario_name"`
	Status       string    `json:"status"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	StepCount    int       `json:"step_count"`
	FailedStep   int       `json:"failed_step,omitempty"`
	Failure      string    `json:"failure,omitempty"`
	NodeCount    int       `json:"node_count"`
	NodeLogCount int       `json:"node_log_count"`
}

// RunArtifacts is the persistent bundle for one chaos/system-test run.
type RunArtifacts struct {
	Manifest Manifest                  `json:"manifest"`
	Report   RunReport                 `json:"report"`
	Summary  RunArtifactSummary        `json:"summary"`
	NodeLogs map[uint64][]NodeLogEntry `json:"node_logs"`
}

// BuildRunArtifacts validates the scenario and assembles a persistent artifact bundle.
func BuildRunArtifacts(scenario Scenario, report RunReport, nodeLogs map[uint64][]NodeLogEntry) (RunArtifacts, error) {
	if err := scenario.Validate(); err != nil {
		return RunArtifacts{}, err
	}
	if report.ScenarioName != scenario.Name {
		return RunArtifacts{}, fmt.Errorf("systemtest: report scenario %q does not match %q", report.ScenarioName, scenario.Name)
	}
	manifest, err := BuildManifest(scenario)
	if err != nil {
		return RunArtifacts{}, err
	}
	logCopies := make(map[uint64][]NodeLogEntry, len(nodeLogs))
	logCount := 0
	for nodeID, entries := range nodeLogs {
		logCopies[nodeID] = append([]NodeLogEntry(nil), entries...)
		logCount += len(entries)
	}
	summary := RunArtifactSummary{
		Version:      artifactVersion,
		ScenarioName: scenario.Name,
		Status:       "pass",
		StartedAt:    report.StartedAt,
		FinishedAt:   report.FinishedAt,
		StepCount:    len(report.Steps),
		NodeCount:    len(logCopies),
		NodeLogCount: logCount,
	}
	for _, step := range report.Steps {
		if step.Error == "" {
			continue
		}
		summary.Status = "fail"
		summary.FailedStep = step.Index
		summary.Failure = step.Error
		break
	}
	return RunArtifacts{
		Manifest: manifest,
		Report:   report,
		Summary:  summary,
		NodeLogs: logCopies,
	}, nil
}

// WriteDir persists the artifact bundle in a stable directory layout.
func (a RunArtifacts) WriteDir(root string) error {
	if root == "" {
		return fmt.Errorf("systemtest: artifact root must not be empty")
	}
	if err := os.MkdirAll(filepath.Join(root, "node-logs"), 0o755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(root, "manifest.json"), a.Manifest); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(root, "report.json"), a.Report); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(root, "summary.json"), a.Summary); err != nil {
		return err
	}

	nodeIDs := make([]uint64, 0, len(a.NodeLogs))
	for nodeID := range a.NodeLogs {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	for _, nodeID := range nodeIDs {
		filename := filepath.Join(root, "node-logs", fmt.Sprintf("node-%d.json", nodeID))
		if err := writeJSON(filename, a.NodeLogs[nodeID]); err != nil {
			return err
		}
	}
	return nil
}

// LoadRunArtifacts reads a persisted artifact bundle from disk.
func LoadRunArtifacts(root string) (RunArtifacts, error) {
	var artifacts RunArtifacts
	if err := readJSON(filepath.Join(root, "manifest.json"), &artifacts.Manifest); err != nil {
		return RunArtifacts{}, err
	}
	if err := readJSON(filepath.Join(root, "report.json"), &artifacts.Report); err != nil {
		return RunArtifacts{}, err
	}
	if err := readJSON(filepath.Join(root, "summary.json"), &artifacts.Summary); err != nil {
		return RunArtifacts{}, err
	}
	logDir := filepath.Join(root, "node-logs")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return RunArtifacts{}, err
	}
	artifacts.NodeLogs = make(map[uint64][]NodeLogEntry, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var nodeID uint64
		if _, err := fmt.Sscanf(entry.Name(), "node-%d.json", &nodeID); err != nil {
			return RunArtifacts{}, fmt.Errorf("systemtest: unexpected node log file %q", entry.Name())
		}
		var logs []NodeLogEntry
		if err := readJSON(filepath.Join(logDir, entry.Name()), &logs); err != nil {
			return RunArtifacts{}, err
		}
		artifacts.NodeLogs[nodeID] = logs
	}
	return artifacts, nil
}

func writeJSON(path string, value any) error {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(payload, '\n'), 0o644)
}

func readJSON(path string, target any) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, target)
}
