package adminapi

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

// ReplicaView is the UI/API-safe rendering of a replica descriptor.
type ReplicaView struct {
	ReplicaID uint64 `json:"replica_id"`
	NodeID    uint64 `json:"node_id"`
	Role      string `json:"role"`
}

// RangeView is the UI/API-safe rendering of a range descriptor.
type RangeView struct {
	RangeID              uint64        `json:"range_id"`
	Generation           uint64        `json:"generation"`
	StartKey             string        `json:"start_key"`
	EndKey               string        `json:"end_key,omitempty"`
	Replicas             []ReplicaView `json:"replicas"`
	LeaseholderReplicaID uint64        `json:"leaseholder_replica_id,omitempty"`
	LeaseholderNodeID    uint64        `json:"leaseholder_node_id,omitempty"`
	PlacementMode        string        `json:"placement_mode,omitempty"`
	PreferredRegions     []string      `json:"preferred_regions,omitempty"`
	LeasePreferences     []string      `json:"lease_preferences,omitempty"`
	Source               string        `json:"source,omitempty"`
}

// NodeView is the node/operator view exposed to the UI.
type NodeView struct {
	NodeID           uint64    `json:"node_id"`
	PGAddr           string    `json:"pg_addr,omitempty"`
	ObservabilityURL string    `json:"observability_url,omitempty"`
	ControlURL       string    `json:"control_url,omitempty"`
	Status           string    `json:"status"`
	StartedAt        time.Time `json:"started_at,omitempty"`
	PartitionedFrom  []uint64  `json:"partitioned_from,omitempty"`
	Notes            []string  `json:"notes,omitempty"`
	ReplicaCount     int       `json:"replica_count"`
	LeaseCount       int       `json:"lease_count"`
}

// ClusterEvent is one UI-visible real-time event.
type ClusterEvent struct {
	ID        string            `json:"id,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Type      string            `json:"type"`
	NodeID    uint64            `json:"node_id,omitempty"`
	RangeID   uint64            `json:"range_id,omitempty"`
	Severity  string            `json:"severity,omitempty"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// ClusterSnapshot is the top-level snapshot returned by a future aggregator.
type ClusterSnapshot struct {
	GeneratedAt time.Time      `json:"generated_at"`
	Nodes       []NodeView     `json:"nodes"`
	Ranges      []RangeView    `json:"ranges"`
	Events      []ClusterEvent `json:"events,omitempty"`
}

// TopologyEdgeView is one replica-placement edge in the cluster topology graph.
type TopologyEdgeView struct {
	NodeID      uint64 `json:"node_id"`
	RangeID     uint64 `json:"range_id"`
	ReplicaID   uint64 `json:"replica_id"`
	Role        string `json:"role"`
	Leaseholder bool   `json:"leaseholder"`
}

// ClusterTopologyView is the graph-friendly console view of live placement.
type ClusterTopologyView struct {
	GeneratedAt time.Time          `json:"generated_at"`
	Nodes       []NodeView         `json:"nodes"`
	Ranges      []RangeView        `json:"ranges"`
	Edges       []TopologyEdgeView `json:"edges"`
}

// NodeHostedRangeView is one range residency row in a node drilldown view.
type NodeHostedRangeView struct {
	RangeID       uint64 `json:"range_id"`
	Generation    uint64 `json:"generation"`
	StartKey      string `json:"start_key"`
	EndKey        string `json:"end_key,omitempty"`
	ReplicaID     uint64 `json:"replica_id"`
	ReplicaRole   string `json:"replica_role"`
	Leaseholder   bool   `json:"leaseholder"`
	PlacementMode string `json:"placement_mode,omitempty"`
}

// NodeDetailView is the console drilldown for one node.
type NodeDetailView struct {
	Node         NodeView              `json:"node"`
	HostedRanges []NodeHostedRangeView `json:"hosted_ranges"`
	RecentEvents []ClusterEvent        `json:"recent_events"`
}

// RangeReplicaNodeView ties one replica to its current node surface.
type RangeReplicaNodeView struct {
	Replica     ReplicaView `json:"replica"`
	Node        *NodeView   `json:"node,omitempty"`
	Leaseholder bool        `json:"leaseholder"`
}

// RangeDetailView is the console drilldown for one range.
type RangeDetailView struct {
	Range        RangeView              `json:"range"`
	ReplicaNodes []RangeReplicaNodeView `json:"replica_nodes"`
	RecentEvents []ClusterEvent         `json:"recent_events"`
}

// ScenarioManifest is the console-facing export of a retained chaos manifest.
type ScenarioManifest struct {
	Version  string                 `json:"version"`
	Scenario string                 `json:"scenario"`
	Nodes    []uint64               `json:"nodes"`
	Steps    []ScenarioManifestStep `json:"steps"`
}

// ScenarioManifestStep is the console-facing form of one retained scenario step.
type ScenarioManifestStep struct {
	Index          int      `json:"index"`
	Action         string   `json:"action"`
	PartitionLeft  []uint64 `json:"partition_left,omitempty"`
	PartitionRight []uint64 `json:"partition_right,omitempty"`
	NodeID         uint64   `json:"node_id,omitempty"`
	Duration       string   `json:"duration,omitempty"`
	GatewayNodeID  uint64   `json:"gateway_node_id,omitempty"`
	TxnLabel       string   `json:"txn_label,omitempty"`
	AckDelay       string   `json:"ack_delay,omitempty"`
	DropResponse   bool     `json:"drop_response,omitempty"`
}

// ScenarioHandoffOperation is the console-facing form of one external handoff mapping.
type ScenarioHandoffOperation struct {
	Action            string `json:"action"`
	ExternalOperation string `json:"external_operation"`
	Description       string `json:"description"`
}

// ScenarioHandoffBundle is the console-facing handoff contract for one retained run.
type ScenarioHandoffBundle struct {
	Version    string                     `json:"version"`
	Manifest   ScenarioManifest           `json:"manifest"`
	Operations []ScenarioHandoffOperation `json:"operations"`
}

// ScenarioStepReport is the console-facing form of one executed scenario step.
type ScenarioStepReport struct {
	Index      int       `json:"index"`
	Action     string    `json:"action"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Error      string    `json:"error,omitempty"`
}

// ScenarioRunReport is the console-facing retained execution report.
type ScenarioRunReport struct {
	ScenarioName string               `json:"scenario_name"`
	StartedAt    time.Time            `json:"started_at"`
	FinishedAt   time.Time            `json:"finished_at"`
	Steps        []ScenarioStepReport `json:"steps"`
}

// ScenarioNodeLogEntry is one retained node log entry for a scenario run.
type ScenarioNodeLogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

// ScenarioRunSummary is the console-facing pass/fail synopsis for one retained run.
type ScenarioRunSummary struct {
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

// ScenarioRunView is the console-facing summary for one retained scenario run.
type ScenarioRunView struct {
	RunID        string    `json:"run_id"`
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

// ScenarioLiveCorrelation ties one retained run to the current live topology.
type ScenarioLiveCorrelation struct {
	GeneratedAt    time.Time   `json:"generated_at"`
	Source         string      `json:"source"`
	Nodes          []NodeView  `json:"nodes"`
	Ranges         []RangeView `json:"ranges"`
	MissingNodeIDs []uint64    `json:"missing_node_ids,omitempty"`
}

// ScenarioRunDetail is the console-facing detail surface for one retained run.
type ScenarioRunDetail struct {
	Run             ScenarioRunView                   `json:"run"`
	Manifest        ScenarioManifest                  `json:"manifest"`
	Handoff         *ScenarioHandoffBundle            `json:"handoff,omitempty"`
	Report          ScenarioRunReport                 `json:"report"`
	Summary         ScenarioRunSummary                `json:"summary"`
	NodeLogs        map[uint64][]ScenarioNodeLogEntry `json:"node_logs"`
	LiveCorrelation *ScenarioLiveCorrelation          `json:"live_correlation,omitempty"`
}

// ScenarioReader exposes retained scenario summaries and detail bundles to the console API.
type ScenarioReader interface {
	ListRuns() ([]ScenarioRunView, error)
	LoadRun(runID string) (ScenarioRunDetail, error)
}

// KeyLocationView is the authoritative lookup result for one logical key.
type KeyLocationView struct {
	Key      string    `json:"key"`
	Encoding string    `json:"encoding,omitempty"`
	Range    RangeView `json:"range"`
}

// RangeViewFromDescriptor converts a meta descriptor into an API-safe range view.
func RangeViewFromDescriptor(desc meta.RangeDescriptor, source string) RangeView {
	view := RangeView{
		RangeID:              desc.RangeID,
		Generation:           desc.Generation,
		StartKey:             encodeKey(desc.StartKey),
		EndKey:               encodeKey(desc.EndKey),
		Replicas:             make([]ReplicaView, 0, len(desc.Replicas)),
		LeaseholderReplicaID: desc.LeaseholderReplicaID,
		Source:               source,
	}
	for _, replica := range desc.Replicas {
		view.Replicas = append(view.Replicas, ReplicaView{
			ReplicaID: replica.ReplicaID,
			NodeID:    replica.NodeID,
			Role:      string(replica.Role),
		})
		if replica.ReplicaID == desc.LeaseholderReplicaID {
			view.LeaseholderNodeID = replica.NodeID
		}
	}
	if compiled, ok, err := desc.CompiledPlacement(); err == nil && ok {
		view.PlacementMode = string(compiled.PlacementMode)
		view.PreferredRegions = append([]string(nil), compiled.PreferredRegions...)
		view.LeasePreferences = append([]string(nil), compiled.LeasePreferences...)
	}
	return view
}

// NormalizeEvent ensures the event has a stable identity for replay and streaming.
func NormalizeEvent(event ClusterEvent) ClusterEvent {
	copyEvent := event
	if copyEvent.Timestamp.IsZero() {
		copyEvent.Timestamp = time.Now().UTC()
	} else {
		copyEvent.Timestamp = copyEvent.Timestamp.UTC()
	}
	if copyEvent.Fields != nil {
		fields := make(map[string]string, len(copyEvent.Fields))
		for key, value := range copyEvent.Fields {
			fields[key] = value
		}
		copyEvent.Fields = fields
	}
	if copyEvent.ID == "" {
		copyEvent.ID = eventID(copyEvent)
	}
	return copyEvent
}

func encodeKey(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return hex.EncodeToString(key)
}

func decodeKey(raw string) ([]byte, error) {
	if raw == "" {
		return nil, nil
	}
	return hex.DecodeString(raw)
}

func rangeContainsKey(view RangeView, key []byte) (bool, error) {
	startKey, err := decodeKey(view.StartKey)
	if err != nil {
		return false, fmt.Errorf("adminapi: decode start key for range %d: %w", view.RangeID, err)
	}
	if bytes.Compare(key, startKey) < 0 {
		return false, nil
	}
	if view.EndKey == "" {
		return true, nil
	}
	endKey, err := decodeKey(view.EndKey)
	if err != nil {
		return false, fmt.Errorf("adminapi: decode end key for range %d: %w", view.RangeID, err)
	}
	return bytes.Compare(key, endKey) < 0, nil
}

func eventID(event ClusterEvent) string {
	type fieldPair struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	ordered := make([]fieldPair, 0, len(event.Fields))
	for key, value := range event.Fields {
		ordered = append(ordered, fieldPair{Key: key, Value: value})
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Key < ordered[j].Key
	})
	payload, err := json.Marshal(struct {
		Timestamp string      `json:"timestamp"`
		Type      string      `json:"type"`
		NodeID    uint64      `json:"node_id,omitempty"`
		RangeID   uint64      `json:"range_id,omitempty"`
		Severity  string      `json:"severity,omitempty"`
		Message   string      `json:"message"`
		Fields    []fieldPair `json:"fields,omitempty"`
	}{
		Timestamp: event.Timestamp.UTC().Format(time.RFC3339Nano),
		Type:      event.Type,
		NodeID:    event.NodeID,
		RangeID:   event.RangeID,
		Severity:  event.Severity,
		Message:   event.Message,
		Fields:    ordered,
	})
	if err != nil {
		return fallbackEventID(event)
	}
	sum := sha1.Sum(payload)
	return "evt_" + hex.EncodeToString(sum[:])
}

func fallbackEventID(event ClusterEvent) string {
	return fmt.Sprintf(
		"evt_fallback_%s_%d_%d_%d_%s",
		strings.ReplaceAll(event.Timestamp.UTC().Format(time.RFC3339Nano), ":", "_"),
		event.NodeID,
		event.RangeID,
		len(event.Message),
		event.Type,
	)
}
