package systemtest

import (
	"reflect"
	"testing"
	"time"
)

func TestPartitionRecoveryScenarioBuildsValidProgram(t *testing.T) {
	t.Parallel()

	scenario, err := PartitionRecoveryScenario(PartitionRecoveryConfig{
		Name:  "region-partition",
		Nodes: []uint64{1, 2, 3},
		Partition: PartitionSpec{
			Left:  []uint64{1},
			Right: []uint64{2, 3},
		},
		SettleFor: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("partition recovery scenario: %v", err)
	}
	if len(scenario.Steps) != 3 {
		t.Fatalf("steps = %+v, want partition, wait, heal", scenario.Steps)
	}
	if scenario.Steps[0].Action != ActionPartition || scenario.Steps[1].Action != ActionWait || scenario.Steps[2].Action != ActionHeal {
		t.Fatalf("unexpected steps = %+v", scenario.Steps)
	}
}

func TestCrashRecoveryScenarioBuildsValidProgram(t *testing.T) {
	t.Parallel()

	scenario, err := CrashRecoveryScenario(CrashRecoveryConfig{
		Name:      "single-node-restart",
		Nodes:     []uint64{1, 2, 3},
		NodeID:    2,
		Downtime:  5 * time.Second,
		SettleFor: 4 * time.Second,
	})
	if err != nil {
		t.Fatalf("crash recovery scenario: %v", err)
	}
	want := []ActionType{ActionCrashNode, ActionWait, ActionRestartNode, ActionWait}
	got := []ActionType{
		scenario.Steps[0].Action,
		scenario.Steps[1].Action,
		scenario.Steps[2].Action,
		scenario.Steps[3].Action,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("actions = %+v, want %+v", got, want)
	}
}

func TestAmbiguousCommitRecoveryScenarioBuildsPartitionedProgram(t *testing.T) {
	t.Parallel()

	scenario, err := AmbiguousCommitRecoveryScenario(AmbiguousCommitRecoveryConfig{
		Name:          "ambiguous-partitioned-transfer",
		Nodes:         []uint64{1, 2, 3},
		GatewayNodeID: 1,
		TxnLabel:      "transfer-42",
		AckDelay:      250 * time.Millisecond,
		SettleFor:     2 * time.Second,
		PartitionBefore: &PartitionSpec{
			Left:  []uint64{1},
			Right: []uint64{2, 3},
		},
	})
	if err != nil {
		t.Fatalf("ambiguous commit recovery scenario: %v", err)
	}
	want := []ActionType{ActionPartition, ActionAmbiguousCommit, ActionWait, ActionHeal}
	got := []ActionType{
		scenario.Steps[0].Action,
		scenario.Steps[1].Action,
		scenario.Steps[2].Action,
		scenario.Steps[3].Action,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("actions = %+v, want %+v", got, want)
	}
	if scenario.Steps[1].AmbiguousCommit == nil || !scenario.Steps[1].AmbiguousCommit.DropResponse {
		t.Fatalf("ambiguous step = %+v, want drop-response fault", scenario.Steps[1])
	}
}
