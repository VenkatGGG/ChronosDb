package systemtest

import (
	"fmt"
	"time"
)

// PartitionRecoveryConfig builds a canonical partition-then-heal scenario.
type PartitionRecoveryConfig struct {
	Name      string
	Nodes     []uint64
	Partition PartitionSpec
	SettleFor time.Duration
}

// CrashRecoveryConfig builds a canonical crash-then-restart scenario.
type CrashRecoveryConfig struct {
	Name      string
	Nodes     []uint64
	NodeID    uint64
	Downtime  time.Duration
	SettleFor time.Duration
}

// AmbiguousCommitRecoveryConfig builds a canonical ambiguous-commit scenario.
type AmbiguousCommitRecoveryConfig struct {
	Name            string
	Nodes           []uint64
	GatewayNodeID   uint64
	TxnLabel        string
	AckDelay        time.Duration
	SettleFor       time.Duration
	PartitionBefore *PartitionSpec
}

// PartitionRecoveryScenario constructs a validated partition-and-heal scenario.
func PartitionRecoveryScenario(cfg PartitionRecoveryConfig) (Scenario, error) {
	if cfg.SettleFor <= 0 {
		return Scenario{}, fmt.Errorf("systemtest: partition settle duration must be positive")
	}
	scenario := Scenario{
		Name:  cfg.Name,
		Nodes: append([]uint64(nil), cfg.Nodes...),
		Steps: []Step{
			{
				Action:    ActionPartition,
				Partition: clonePartition(cfg.Partition),
			},
			{Action: ActionWait, Duration: cfg.SettleFor},
			{Action: ActionHeal},
		},
	}
	return scenario, scenario.Validate()
}

// CrashRecoveryScenario constructs a validated crash-and-restart scenario.
func CrashRecoveryScenario(cfg CrashRecoveryConfig) (Scenario, error) {
	if cfg.Downtime <= 0 {
		return Scenario{}, fmt.Errorf("systemtest: crash downtime must be positive")
	}
	if cfg.SettleFor <= 0 {
		return Scenario{}, fmt.Errorf("systemtest: crash settle duration must be positive")
	}
	scenario := Scenario{
		Name:  cfg.Name,
		Nodes: append([]uint64(nil), cfg.Nodes...),
		Steps: []Step{
			{Action: ActionCrashNode, NodeID: cfg.NodeID},
			{Action: ActionWait, Duration: cfg.Downtime},
			{Action: ActionRestartNode, NodeID: cfg.NodeID},
			{Action: ActionWait, Duration: cfg.SettleFor},
		},
	}
	return scenario, scenario.Validate()
}

// AmbiguousCommitRecoveryScenario constructs a validated ambiguous-commit scenario.
func AmbiguousCommitRecoveryScenario(cfg AmbiguousCommitRecoveryConfig) (Scenario, error) {
	if cfg.AckDelay <= 0 {
		return Scenario{}, fmt.Errorf("systemtest: ambiguous commit ack delay must be positive")
	}
	if cfg.SettleFor <= 0 {
		return Scenario{}, fmt.Errorf("systemtest: ambiguous commit settle duration must be positive")
	}
	steps := make([]Step, 0, 4)
	if cfg.PartitionBefore != nil {
		steps = append(steps, Step{
			Action:    ActionPartition,
			Partition: clonePartition(*cfg.PartitionBefore),
		})
	}
	steps = append(steps,
		Step{
			Action: ActionAmbiguousCommit,
			AmbiguousCommit: &AmbiguousCommitSpec{
				GatewayNodeID: cfg.GatewayNodeID,
				TxnLabel:      cfg.TxnLabel,
				AckDelay:      cfg.AckDelay,
				DropResponse:  true,
			},
		},
		Step{Action: ActionWait, Duration: cfg.SettleFor},
	)
	if cfg.PartitionBefore != nil {
		steps = append(steps, Step{Action: ActionHeal})
	}
	scenario := Scenario{
		Name:  cfg.Name,
		Nodes: append([]uint64(nil), cfg.Nodes...),
		Steps: steps,
	}
	return scenario, scenario.Validate()
}

func clonePartition(spec PartitionSpec) *PartitionSpec {
	return &PartitionSpec{
		Left:  append([]uint64(nil), spec.Left...),
		Right: append([]uint64(nil), spec.Right...),
	}
}
