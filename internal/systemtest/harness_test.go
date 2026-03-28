package systemtest

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestScenarioValidateRejectsInvalidActionSequence(t *testing.T) {
	t.Parallel()

	scenario := Scenario{
		Name:  "invalid-restart",
		Nodes: []uint64{1, 2, 3},
		Steps: []Step{
			{Action: ActionRestartNode, NodeID: 2},
		},
	}
	if err := scenario.Validate(); err == nil {
		t.Fatal("expected restart of running node to fail validation")
	}
}

func TestScenarioValidateRejectsUnknownPartitionNode(t *testing.T) {
	t.Parallel()

	scenario := Scenario{
		Name:  "bad-partition",
		Nodes: []uint64{1, 2, 3},
		Steps: []Step{
			{
				Action: ActionPartition,
				Partition: &PartitionSpec{
					Left:  []uint64{1},
					Right: []uint64{4},
				},
			},
		},
	}
	if err := scenario.Validate(); err == nil {
		t.Fatal("expected unknown partition node to fail validation")
	}
}

func TestRunnerExecutesScenarioInOrder(t *testing.T) {
	t.Parallel()

	scenario := Scenario{
		Name:  "partition-crash-ambiguous-restore",
		Nodes: []uint64{1, 2, 3},
		Steps: []Step{
			{
				Action: ActionPartition,
				Partition: &PartitionSpec{
					Left:  []uint64{1},
					Right: []uint64{2, 3},
				},
			},
			{Action: ActionCrashNode, NodeID: 2},
			{
				Action: ActionAmbiguousCommit,
				AmbiguousCommit: &AmbiguousCommitSpec{
					GatewayNodeID: 1,
					TxnLabel:      "funds-transfer",
					AckDelay:      250 * time.Millisecond,
					DropResponse:  true,
				},
			},
			{Action: ActionWait, Duration: 2 * time.Second},
			{Action: ActionRestartNode, NodeID: 2},
			{Action: ActionHeal},
		},
	}
	controller := &recordingController{}
	report, err := (Runner{Controller: controller}).Run(context.Background(), scenario)
	if err != nil {
		t.Fatalf("run scenario: %v", err)
	}
	want := []string{
		"partition:[1]|[2 3]",
		"crash:2",
		"ambiguous:1:funds-transfer:250ms:true",
		"wait:2s",
		"restart:2",
		"heal",
	}
	if !reflect.DeepEqual(controller.calls, want) {
		t.Fatalf("calls = %+v, want %+v", controller.calls, want)
	}
	if len(report.Steps) != len(want) || report.ScenarioName != scenario.Name {
		t.Fatalf("report = %+v, want %d steps for %q", report, len(want), scenario.Name)
	}
	for _, step := range report.Steps {
		if step.StartedAt.IsZero() || step.FinishedAt.IsZero() {
			t.Fatalf("step report timestamps must be populated: %+v", step)
		}
	}
}

func TestRunnerInvokesAssertionHooks(t *testing.T) {
	t.Parallel()

	scenario := Scenario{
		Name:  "asserted-restart",
		Nodes: []uint64{1, 2},
		Steps: []Step{
			{Action: ActionCrashNode, NodeID: 2},
			{Action: ActionWait, Duration: time.Second},
			{Action: ActionRestartNode, NodeID: 2},
		},
	}
	controller := &recordingController{}
	assertion := &recordingAssertion{}
	report, err := (Runner{
		Controller: controller,
		Assertions: []AssertionHook{assertion},
	}).Run(context.Background(), scenario)
	if err != nil {
		t.Fatalf("run scenario with assertions: %v", err)
	}
	if assertion.afterRunName != scenario.Name {
		t.Fatalf("after run scenario = %q, want %q", assertion.afterRunName, scenario.Name)
	}
	if !reflect.DeepEqual(assertion.afterStepActions, []ActionType{ActionCrashNode, ActionWait, ActionRestartNode}) {
		t.Fatalf("after-step actions = %+v", assertion.afterStepActions)
	}
	if len(report.Steps) != 3 {
		t.Fatalf("report steps = %+v, want 3", report.Steps)
	}
}

type recordingController struct {
	calls []string
}

type recordingAssertion struct {
	afterStepActions []ActionType
	afterRunName     string
}

func (r *recordingAssertion) AfterStep(_ context.Context, _ Scenario, step StepReport) error {
	r.afterStepActions = append(r.afterStepActions, step.Action)
	return nil
}

func (r *recordingAssertion) AfterRun(_ context.Context, report RunReport) error {
	r.afterRunName = report.ScenarioName
	return nil
}

func (r *recordingController) Partition(_ context.Context, spec PartitionSpec) error {
	r.calls = append(r.calls, "partition:"+formatNodes(spec.Left)+"|"+formatNodes(spec.Right))
	return nil
}

func (r *recordingController) Heal(context.Context) error {
	r.calls = append(r.calls, "heal")
	return nil
}

func (r *recordingController) CrashNode(_ context.Context, nodeID uint64) error {
	r.calls = append(r.calls, "crash:"+itoa(nodeID))
	return nil
}

func (r *recordingController) RestartNode(_ context.Context, nodeID uint64) error {
	r.calls = append(r.calls, "restart:"+itoa(nodeID))
	return nil
}

func (r *recordingController) InjectAmbiguousCommit(_ context.Context, spec AmbiguousCommitSpec) error {
	r.calls = append(
		r.calls,
		"ambiguous:"+itoa(spec.GatewayNodeID)+":"+spec.TxnLabel+":"+spec.AckDelay.String()+":"+boolString(spec.DropResponse),
	)
	return nil
}

func (r *recordingController) Wait(_ context.Context, d time.Duration) error {
	r.calls = append(r.calls, "wait:"+d.String())
	return nil
}

func formatNodes(nodes []uint64) string {
	return "[" + joinNodes(nodes) + "]"
}

func joinNodes(nodes []uint64) string {
	if len(nodes) == 0 {
		return ""
	}
	out := itoa(nodes[0])
	for _, nodeID := range nodes[1:] {
		out += " " + itoa(nodeID)
	}
	return out
}

func itoa(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
