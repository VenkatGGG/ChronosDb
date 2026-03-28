package adminapi

import (
	"encoding/hex"
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

// KeyLocationView is the authoritative lookup result for one logical key.
type KeyLocationView struct {
	Key   string    `json:"key"`
	Range RangeView `json:"range"`
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

func encodeKey(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return hex.EncodeToString(key)
}
