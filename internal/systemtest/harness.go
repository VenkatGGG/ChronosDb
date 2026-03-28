package systemtest

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// ActionType identifies one chaos/system-test action.
type ActionType string

const (
	ActionPartition       ActionType = "partition"
	ActionHeal            ActionType = "heal"
	ActionCrashNode       ActionType = "crash_node"
	ActionRestartNode     ActionType = "restart_node"
	ActionAmbiguousCommit ActionType = "ambiguous_commit"
	ActionWait            ActionType = "wait"
)

// Scenario is one ordered fault-injection program over a cluster.
type Scenario struct {
	Name  string
	Nodes []uint64
	Steps []Step
}

// Step is one validated system-test action.
type Step struct {
	Action          ActionType
	Partition       *PartitionSpec
	NodeID          uint64
	Duration        time.Duration
	AmbiguousCommit *AmbiguousCommitSpec
}

// PartitionSpec isolates two node sets from each other.
type PartitionSpec struct {
	Left  []uint64
	Right []uint64
}

// AmbiguousCommitSpec injects an acknowledgment ambiguity around one write path.
type AmbiguousCommitSpec struct {
	GatewayNodeID uint64
	TxnLabel      string
	AckDelay      time.Duration
	DropResponse  bool
}

// Controller is the runtime surface a real cluster adapter must implement.
type Controller interface {
	Partition(context.Context, PartitionSpec) error
	Heal(context.Context) error
	CrashNode(context.Context, uint64) error
	RestartNode(context.Context, uint64) error
	InjectAmbiguousCommit(context.Context, AmbiguousCommitSpec) error
	Wait(context.Context, time.Duration) error
}

// AssertionHook observes scenario execution and may reject the run.
type AssertionHook interface {
	AfterStep(context.Context, Scenario, StepReport) error
	AfterRun(context.Context, RunReport) error
}

// RunReport is the structured execution evidence for one scenario run.
type RunReport struct {
	ScenarioName string
	StartedAt    time.Time
	FinishedAt   time.Time
	Steps        []StepReport
}

// StepReport captures one executed system-test step.
type StepReport struct {
	Index      int
	Action     ActionType
	StartedAt  time.Time
	FinishedAt time.Time
	Error      string
}

// Runner validates and executes system-test scenarios against a controller.
type Runner struct {
	Controller Controller
	Assertions []AssertionHook
	Now        func() time.Time
}

// Run executes the scenario step-by-step after validation.
func (r Runner) Run(ctx context.Context, scenario Scenario) (RunReport, error) {
	if r.Controller == nil {
		return RunReport{}, fmt.Errorf("systemtest: controller is required")
	}
	if err := scenario.Validate(); err != nil {
		return RunReport{}, err
	}
	now := r.Now
	if now == nil {
		now = time.Now().UTC
	}
	report := RunReport{
		ScenarioName: scenario.Name,
		StartedAt:    now(),
		Steps:        make([]StepReport, 0, len(scenario.Steps)),
	}
	for i, step := range scenario.Steps {
		stepReport := StepReport{
			Index:     i + 1,
			Action:    step.Action,
			StartedAt: now(),
		}
		var err error
		switch step.Action {
		case ActionPartition:
			err = r.Controller.Partition(ctx, *step.Partition)
		case ActionHeal:
			err = r.Controller.Heal(ctx)
		case ActionCrashNode:
			err = r.Controller.CrashNode(ctx, step.NodeID)
		case ActionRestartNode:
			err = r.Controller.RestartNode(ctx, step.NodeID)
		case ActionAmbiguousCommit:
			err = r.Controller.InjectAmbiguousCommit(ctx, *step.AmbiguousCommit)
		case ActionWait:
			err = r.Controller.Wait(ctx, step.Duration)
		default:
			err = fmt.Errorf("systemtest: unknown action %q", step.Action)
		}
		stepReport.FinishedAt = now()
		if err != nil {
			stepReport.Error = err.Error()
			report.Steps = append(report.Steps, stepReport)
			report.FinishedAt = now()
			return report, err
		}
		for _, assertion := range r.Assertions {
			if assertion == nil {
				continue
			}
			if err := assertion.AfterStep(ctx, scenario, stepReport); err != nil {
				stepReport.Error = err.Error()
				report.Steps = append(report.Steps, stepReport)
				report.FinishedAt = now()
				return report, err
			}
		}
		report.Steps = append(report.Steps, stepReport)
	}
	report.FinishedAt = now()
	for _, assertion := range r.Assertions {
		if assertion == nil {
			continue
		}
		if err := assertion.AfterRun(ctx, report); err != nil {
			return report, err
		}
	}
	return report, nil
}

// Validate checks that the scenario is internally consistent before execution.
func (s Scenario) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("systemtest: scenario name must not be empty")
	}
	if len(s.Nodes) == 0 {
		return fmt.Errorf("systemtest: scenario nodes must not be empty")
	}
	if len(s.Steps) == 0 {
		return fmt.Errorf("systemtest: scenario steps must not be empty")
	}
	nodeSet := make(map[uint64]struct{}, len(s.Nodes))
	for _, nodeID := range s.Nodes {
		if nodeID == 0 {
			return fmt.Errorf("systemtest: scenario node ids must be non-zero")
		}
		if _, ok := nodeSet[nodeID]; ok {
			return fmt.Errorf("systemtest: duplicate node %d", nodeID)
		}
		nodeSet[nodeID] = struct{}{}
	}
	crashed := make(map[uint64]bool, len(s.Nodes))
	for i, step := range s.Steps {
		if err := validateStep(step, nodeSet, crashed); err != nil {
			return fmt.Errorf("systemtest: step %d: %w", i+1, err)
		}
		switch step.Action {
		case ActionCrashNode:
			crashed[step.NodeID] = true
		case ActionRestartNode:
			crashed[step.NodeID] = false
		}
	}
	return nil
}

func validateStep(step Step, nodes map[uint64]struct{}, crashed map[uint64]bool) error {
	switch step.Action {
	case ActionPartition:
		if step.Partition == nil {
			return fmt.Errorf("partition action requires a partition spec")
		}
		return validatePartition(*step.Partition, nodes)
	case ActionHeal:
		if step.Partition != nil || step.NodeID != 0 || step.Duration != 0 || step.AmbiguousCommit != nil {
			return fmt.Errorf("heal action must not carry extra fields")
		}
		return nil
	case ActionCrashNode:
		if _, ok := nodes[step.NodeID]; !ok {
			return fmt.Errorf("crash action references unknown node %d", step.NodeID)
		}
		if crashed[step.NodeID] {
			return fmt.Errorf("node %d is already crashed", step.NodeID)
		}
		return nil
	case ActionRestartNode:
		if _, ok := nodes[step.NodeID]; !ok {
			return fmt.Errorf("restart action references unknown node %d", step.NodeID)
		}
		if !crashed[step.NodeID] {
			return fmt.Errorf("node %d is not crashed", step.NodeID)
		}
		return nil
	case ActionAmbiguousCommit:
		if step.AmbiguousCommit == nil {
			return fmt.Errorf("ambiguous_commit action requires a spec")
		}
		spec := *step.AmbiguousCommit
		if _, ok := nodes[spec.GatewayNodeID]; !ok {
			return fmt.Errorf("ambiguous commit references unknown gateway node %d", spec.GatewayNodeID)
		}
		if spec.TxnLabel == "" {
			return fmt.Errorf("ambiguous commit txn label must not be empty")
		}
		if spec.AckDelay <= 0 {
			return fmt.Errorf("ambiguous commit ack delay must be positive")
		}
		return nil
	case ActionWait:
		if step.Duration <= 0 {
			return fmt.Errorf("wait duration must be positive")
		}
		return nil
	default:
		return fmt.Errorf("unknown action %q", step.Action)
	}
}

func validatePartition(partition PartitionSpec, nodes map[uint64]struct{}) error {
	if len(partition.Left) == 0 || len(partition.Right) == 0 {
		return fmt.Errorf("partition sides must both be non-empty")
	}
	left := normalizeNodes(partition.Left)
	right := normalizeNodes(partition.Right)
	for _, nodeID := range left {
		if _, ok := nodes[nodeID]; !ok {
			return fmt.Errorf("partition left side references unknown node %d", nodeID)
		}
		if slices.Contains(right, nodeID) {
			return fmt.Errorf("partition node %d appears on both sides", nodeID)
		}
	}
	for _, nodeID := range right {
		if _, ok := nodes[nodeID]; !ok {
			return fmt.Errorf("partition right side references unknown node %d", nodeID)
		}
	}
	return nil
}

func normalizeNodes(nodes []uint64) []uint64 {
	out := make([]uint64, 0, len(nodes))
	seen := make(map[uint64]struct{}, len(nodes))
	for _, nodeID := range nodes {
		if nodeID == 0 {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	slices.Sort(out)
	return out
}
