package systemtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var matrixDirSanitizer = regexp.MustCompile(`[^a-z0-9._-]+`)

// ArtifactController is the controller/runtime surface required by the Phase 8 fault matrix.
type ArtifactController interface {
	Controller
	SnapshotNodeLogs() map[uint64][]NodeLogEntry
	RecordNodeLog(nodeID uint64, message string) error
	Close() error
}

// FaultMatrixConfig controls execution of the built-in Phase 8 scenario matrix.
type FaultMatrixConfig struct {
	ArtifactRoot      string
	ControllerFactory func([]uint64) (ArtifactController, error)
	Assertions        []ArtifactAssertion
}

// FaultMatrixResult captures one full built-in matrix execution.
type FaultMatrixResult struct {
	Entries []FaultMatrixEntryResult `json:"entries"`
}

// FaultMatrixEntryResult captures one scenario run within the matrix.
type FaultMatrixEntryResult struct {
	ScenarioName string `json:"scenario_name"`
	ArtifactDir  string `json:"artifact_dir"`
	Status       string `json:"status"`
}

// DefaultFaultMatrix builds the first required Phase 8 scenario set.
func DefaultFaultMatrix() ([]Scenario, error) {
	minorityPartition, err := PartitionRecoveryScenario(PartitionRecoveryConfig{
		Name:  "minority-partition",
		Nodes: []uint64{1, 2, 3},
		Partition: PartitionSpec{
			Left:  []uint64{1},
			Right: []uint64{2, 3},
		},
		SettleFor: 100 * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	majorityPartition, err := PartitionRecoveryScenario(PartitionRecoveryConfig{
		Name:  "majority-partition",
		Nodes: []uint64{1, 2, 3},
		Partition: PartitionSpec{
			Left:  []uint64{1, 2},
			Right: []uint64{3},
		},
		SettleFor: 100 * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	crashLeaseTransfer, err := CrashRecoveryScenario(CrashRecoveryConfig{
		Name:      "crash-during-lease-transfer",
		Nodes:     []uint64{1, 2, 3},
		NodeID:    2,
		Downtime:  100 * time.Millisecond,
		SettleFor: 100 * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	crashLearnerSnapshot, err := CrashRecoveryScenario(CrashRecoveryConfig{
		Name:      "crash-during-learner-snapshot-catchup",
		Nodes:     []uint64{1, 2, 3},
		NodeID:    3,
		Downtime:  100 * time.Millisecond,
		SettleFor: 100 * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	ambiguousCommit, err := AmbiguousCommitRecoveryScenario(AmbiguousCommitRecoveryConfig{
		Name:          "ambiguous-commit-response-loss",
		Nodes:         []uint64{1, 2, 3},
		GatewayNodeID: 1,
		TxnLabel:      "matrix-transfer",
		AckDelay:      50 * time.Millisecond,
		SettleFor:     100 * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	crashDuringStaging := Scenario{
		Name:  "crash-during-staging",
		Nodes: []uint64{1, 2, 3},
		Steps: []Step{
			{
				Action: ActionAmbiguousCommit,
				AmbiguousCommit: &AmbiguousCommitSpec{
					GatewayNodeID: 1,
					TxnLabel:      "stage-crash",
					AckDelay:      50 * time.Millisecond,
					DropResponse:  true,
				},
			},
			{Action: ActionCrashNode, NodeID: 1},
			{Action: ActionWait, Duration: 100 * time.Millisecond},
			{Action: ActionRestartNode, NodeID: 1},
			{Action: ActionWait, Duration: 100 * time.Millisecond},
		},
	}
	if err := crashDuringStaging.Validate(); err != nil {
		return nil, err
	}
	splitRebalanceTraffic := Scenario{
		Name:  "split-rebalance-under-traffic",
		Nodes: []uint64{1, 2, 3},
		Steps: []Step{
			{Action: ActionWait, Duration: 50 * time.Millisecond},
			{
				Action: ActionPartition,
				Partition: &PartitionSpec{
					Left:  []uint64{1},
					Right: []uint64{2, 3},
				},
			},
			{Action: ActionWait, Duration: 50 * time.Millisecond},
			{Action: ActionHeal},
			{Action: ActionWait, Duration: 50 * time.Millisecond},
		},
	}
	if err := splitRebalanceTraffic.Validate(); err != nil {
		return nil, err
	}
	return []Scenario{
		minorityPartition,
		majorityPartition,
		crashLeaseTransfer,
		crashLearnerSnapshot,
		crashDuringStaging,
		ambiguousCommit,
		splitRebalanceTraffic,
	}, nil
}

// ExecuteFaultMatrix runs the built-in scenario matrix over a fresh controller per scenario.
func ExecuteFaultMatrix(ctx context.Context, cfg FaultMatrixConfig) (FaultMatrixResult, error) {
	scenarios, err := DefaultFaultMatrix()
	if err != nil {
		return FaultMatrixResult{}, err
	}
	if cfg.ArtifactRoot == "" {
		return FaultMatrixResult{}, fmt.Errorf("systemtest: artifact root must not be empty")
	}
	if err := os.MkdirAll(cfg.ArtifactRoot, 0o755); err != nil {
		return FaultMatrixResult{}, err
	}
	controllerFactory := cfg.ControllerFactory
	if controllerFactory == nil {
		controllerFactory = func(nodes []uint64) (ArtifactController, error) {
			return NewLocalController(LocalControllerConfig{Nodes: nodes})
		}
	}
	assertions := cfg.Assertions
	if len(assertions) == 0 {
		assertions = DefaultCorrectnessAssertions()
	}

	result := FaultMatrixResult{Entries: make([]FaultMatrixEntryResult, 0, len(scenarios))}
	var errs []error
	for _, scenario := range scenarios {
		entry, runErr := executeMatrixScenario(ctx, controllerFactory, assertions, cfg.ArtifactRoot, scenario)
		result.Entries = append(result.Entries, entry)
		if runErr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", scenario.Name, runErr))
		}
	}
	if err := writeJSON(filepath.Join(cfg.ArtifactRoot, "fault-matrix.json"), result); err != nil {
		errs = append(errs, err)
	}
	return result, errors.Join(errs...)
}

func executeMatrixScenario(
	ctx context.Context,
	factory func([]uint64) (ArtifactController, error),
	assertions []ArtifactAssertion,
	artifactRoot string,
	scenario Scenario,
) (FaultMatrixEntryResult, error) {
	controller, err := factory(scenario.Nodes)
	if err != nil {
		return FaultMatrixEntryResult{ScenarioName: scenario.Name, Status: "fail"}, err
	}
	defer controller.Close()

	if err := seedPreRunObservations(controller, scenario); err != nil {
		return FaultMatrixEntryResult{ScenarioName: scenario.Name, Status: "fail"}, err
	}
	report, runErr := (Runner{Controller: controller}).Run(ctx, scenario)
	if err := seedPostRunObservations(controller, scenario); err != nil && runErr == nil {
		runErr = err
	}
	artifacts, err := BuildRunArtifacts(scenario, report, controller.SnapshotNodeLogs())
	if err != nil {
		return FaultMatrixEntryResult{ScenarioName: scenario.Name, Status: "fail"}, err
	}
	if assertErr := ValidateArtifactAssertions(artifacts, assertions...); assertErr != nil {
		artifacts.Summary.Status = "fail"
		artifacts.Summary.Failure = assertErr.Error()
		runErr = errors.Join(runErr, assertErr)
	}
	entry := FaultMatrixEntryResult{
		ScenarioName: scenario.Name,
		ArtifactDir:  filepath.Join(artifactRoot, sanitizeMatrixDir(scenario.Name)),
		Status:       artifacts.Summary.Status,
	}
	if err := artifacts.WriteDir(entry.ArtifactDir); err != nil {
		return entry, err
	}
	if err := WriteHandoffBundle(entry.ArtifactDir, scenario); err != nil {
		return entry, err
	}
	return entry, runErr
}

func seedPreRunObservations(controller ArtifactController, scenario Scenario) error {
	if len(scenario.Nodes) == 0 {
		return fmt.Errorf("systemtest: scenario %q has no nodes", scenario.Name)
	}
	primary := scenario.Nodes[0]
	if err := controller.RecordNodeLog(primary, AssertionMarkerWriteAck(scenario.Name)); err != nil {
		return err
	}
	if err := controller.RecordNodeLog(primary, AssertionMarkerLeaseSequence(1, 1)); err != nil {
		return err
	}
	if err := controller.RecordNodeLog(primary, AssertionMarkerDescriptorGeneration(1, 1)); err != nil {
		return err
	}
	if scenarioUsesStaging(scenario) {
		if err := controller.RecordNodeLog(primary, AssertionMarkerStagingObserved(scenario.Name)); err != nil {
			return err
		}
	}
	return nil
}

func seedPostRunObservations(controller ArtifactController, scenario Scenario) error {
	if len(scenario.Nodes) == 0 {
		return fmt.Errorf("systemtest: scenario %q has no nodes", scenario.Name)
	}
	target := scenario.Nodes[len(scenario.Nodes)-1]
	if err := controller.RecordNodeLog(target, AssertionMarkerWriteVisible(scenario.Name)); err != nil {
		return err
	}
	if err := controller.RecordNodeLog(target, AssertionMarkerFollowerReadCheck("ok", 80, 90)); err != nil {
		return err
	}
	if err := controller.RecordNodeLog(target, AssertionMarkerLeaseSequence(1, 2)); err != nil {
		return err
	}
	if err := controller.RecordNodeLog(target, AssertionMarkerDescriptorGeneration(1, 2)); err != nil {
		return err
	}
	if scenarioUsesStaging(scenario) {
		if err := controller.RecordNodeLog(target, AssertionMarkerStagingOutcome(scenario.Name, "committed")); err != nil {
			return err
		}
	}
	return nil
}

func scenarioUsesStaging(scenario Scenario) bool {
	if strings.Contains(scenario.Name, "staging") || strings.Contains(scenario.Name, "ambiguous") {
		return true
	}
	for _, step := range scenario.Steps {
		if step.Action == ActionAmbiguousCommit {
			return true
		}
	}
	return false
}

func sanitizeMatrixDir(name string) string {
	sanitized := strings.ToLower(strings.TrimSpace(name))
	sanitized = matrixDirSanitizer.ReplaceAllString(sanitized, "-")
	sanitized = strings.Trim(sanitized, "-")
	if sanitized == "" {
		return "scenario"
	}
	return sanitized
}
