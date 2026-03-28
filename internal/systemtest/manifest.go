package systemtest

import (
	"encoding/json"
	"fmt"
)

const manifestVersion = "chronosdb.systemtest.v1"

// Manifest is the versioned handoff format for external chaos runners.
type Manifest struct {
	Version  string         `json:"version"`
	Scenario string         `json:"scenario"`
	Nodes    []uint64       `json:"nodes"`
	Steps    []ManifestStep `json:"steps"`
}

// ManifestStep is the flattened representation of one scenario step.
type ManifestStep struct {
	Index          int        `json:"index"`
	Action         ActionType `json:"action"`
	PartitionLeft  []uint64   `json:"partition_left,omitempty"`
	PartitionRight []uint64   `json:"partition_right,omitempty"`
	NodeID         uint64     `json:"node_id,omitempty"`
	Duration       string     `json:"duration,omitempty"`
	GatewayNodeID  uint64     `json:"gateway_node_id,omitempty"`
	TxnLabel       string     `json:"txn_label,omitempty"`
	AckDelay       string     `json:"ack_delay,omitempty"`
	DropResponse   bool       `json:"drop_response,omitempty"`
}

// BuildManifest validates the scenario and converts it into the handoff format.
func BuildManifest(scenario Scenario) (Manifest, error) {
	if err := scenario.Validate(); err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{
		Version:  manifestVersion,
		Scenario: scenario.Name,
		Nodes:    append([]uint64(nil), scenario.Nodes...),
		Steps:    make([]ManifestStep, 0, len(scenario.Steps)),
	}
	for i, step := range scenario.Steps {
		entry := ManifestStep{
			Index:  i + 1,
			Action: step.Action,
		}
		switch step.Action {
		case ActionPartition:
			entry.PartitionLeft = append([]uint64(nil), step.Partition.Left...)
			entry.PartitionRight = append([]uint64(nil), step.Partition.Right...)
		case ActionCrashNode, ActionRestartNode:
			entry.NodeID = step.NodeID
		case ActionWait:
			entry.Duration = step.Duration.String()
		case ActionAmbiguousCommit:
			entry.GatewayNodeID = step.AmbiguousCommit.GatewayNodeID
			entry.TxnLabel = step.AmbiguousCommit.TxnLabel
			entry.AckDelay = step.AmbiguousCommit.AckDelay.String()
			entry.DropResponse = step.AmbiguousCommit.DropResponse
		case ActionHeal:
		default:
			return Manifest{}, fmt.Errorf("systemtest: unsupported action %q in manifest export", step.Action)
		}
		manifest.Steps = append(manifest.Steps, entry)
	}
	return manifest, nil
}

// ExportManifest encodes the validated handoff manifest as pretty JSON.
func ExportManifest(scenario Scenario) ([]byte, error) {
	manifest, err := BuildManifest(scenario)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(manifest, "", "  ")
}
