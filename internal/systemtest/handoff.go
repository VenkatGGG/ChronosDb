package systemtest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const handoffVersion = "chronosdb.systemtest.handoff.v1"

// HandoffBundle describes how an external chaos runner should interpret a manifest.
type HandoffBundle struct {
	Version    string             `json:"version"`
	Manifest   Manifest           `json:"manifest"`
	Operations []HandoffOperation `json:"operations"`
}

// HandoffOperation maps one manifest action onto an external fault-injector operation.
type HandoffOperation struct {
	Action            ActionType `json:"action"`
	ExternalOperation string     `json:"external_operation"`
	Description       string     `json:"description"`
}

// BuildHandoffBundle converts a validated scenario into the external-runner handoff contract.
func BuildHandoffBundle(scenario Scenario) (HandoffBundle, error) {
	manifest, err := BuildManifest(scenario)
	if err != nil {
		return HandoffBundle{}, err
	}
	return HandoffBundle{
		Version:  handoffVersion,
		Manifest: manifest,
		Operations: []HandoffOperation{
			{
				Action:            ActionPartition,
				ExternalOperation: "network.partition_bidirectional",
				Description:       "isolate every node in partition_left from every node in partition_right",
			},
			{
				Action:            ActionHeal,
				ExternalOperation: "network.heal_all",
				Description:       "remove all active network filters and restore full connectivity",
			},
			{
				Action:            ActionCrashNode,
				ExternalOperation: "process.stop",
				Description:       "stop the target node process without a graceful drain",
			},
			{
				Action:            ActionRestartNode,
				ExternalOperation: "process.start",
				Description:       "start or re-admit the target node process after a crash action",
			},
			{
				Action:            ActionAmbiguousCommit,
				ExternalOperation: "gateway.inject_ambiguous_commit",
				Description:       "arm a one-shot gateway fault using gateway_node_id, txn_label, ack_delay, and drop_response",
			},
			{
				Action:            ActionWait,
				ExternalOperation: "control.wait",
				Description:       "sleep for the supplied duration before advancing to the next step",
			},
		},
	}, nil
}

// ExportHandoffBundle encodes the handoff bundle as pretty JSON.
func ExportHandoffBundle(scenario Scenario) ([]byte, error) {
	bundle, err := BuildHandoffBundle(scenario)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(bundle, "", "  ")
}

// WriteHandoffBundle persists handoff.json for one scenario artifact directory.
func WriteHandoffBundle(root string, scenario Scenario) error {
	if root == "" {
		return fmt.Errorf("systemtest: handoff root must not be empty")
	}
	payload, err := ExportHandoffBundle(scenario)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "handoff.json"), append(payload, '\n'), 0o644)
}
