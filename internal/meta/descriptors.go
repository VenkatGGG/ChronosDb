package meta

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
)

// ReplicaRole describes a replica's membership role in a range.
type ReplicaRole string

const (
	ReplicaRoleVoter   ReplicaRole = "voter"
	ReplicaRoleLearner ReplicaRole = "learner"
)

// ReplicaDescriptor identifies one replica in a range descriptor.
type ReplicaDescriptor struct {
	ReplicaID uint64      `json:"replica_id"`
	NodeID    uint64      `json:"node_id"`
	Role      ReplicaRole `json:"role"`
}

// RangeDescriptor is the routing descriptor for one contiguous range.
type RangeDescriptor struct {
	RangeID              uint64              `json:"range_id"`
	Generation           uint64              `json:"generation"`
	StartKey             []byte              `json:"start_key"`
	EndKey               []byte              `json:"end_key,omitempty"`
	Replicas             []ReplicaDescriptor `json:"replicas"`
	LeaseholderReplicaID uint64              `json:"leaseholder_replica_id,omitempty"`
	PlacementPolicy      *placement.Policy   `json:"placement_policy,omitempty"`
}

// NodeLiveness is the epoch-bearing liveness record for one node.
type NodeLiveness struct {
	NodeID    uint64        `json:"node_id"`
	Epoch     uint64        `json:"epoch"`
	Draining  bool          `json:"draining,omitempty"`
	UpdatedAt hlc.Timestamp `json:"updated_at"`
}

// Validate checks the descriptor for obvious corruption.
func (d RangeDescriptor) Validate() error {
	if d.RangeID == 0 {
		return fmt.Errorf("range descriptor: range id must be non-zero")
	}
	if d.Generation == 0 {
		return fmt.Errorf("range descriptor: generation must be non-zero")
	}
	if len(d.Replicas) == 0 {
		return fmt.Errorf("range descriptor: replicas must not be empty")
	}
	if len(d.EndKey) > 0 && bytes.Compare(d.StartKey, d.EndKey) >= 0 {
		return fmt.Errorf("range descriptor: start key must sort before end key")
	}
	replicaIDs := make(map[uint64]struct{}, len(d.Replicas))
	for _, replica := range d.Replicas {
		if replica.ReplicaID == 0 || replica.NodeID == 0 {
			return fmt.Errorf("range descriptor: replica ids must be non-zero")
		}
		if _, exists := replicaIDs[replica.ReplicaID]; exists {
			return fmt.Errorf("range descriptor: duplicate replica id %d", replica.ReplicaID)
		}
		replicaIDs[replica.ReplicaID] = struct{}{}
		switch replica.Role {
		case ReplicaRoleVoter, ReplicaRoleLearner:
		default:
			return fmt.Errorf("range descriptor: unknown replica role %q", replica.Role)
		}
	}
	if d.LeaseholderReplicaID != 0 {
		if _, ok := replicaIDs[d.LeaseholderReplicaID]; !ok {
			return fmt.Errorf("range descriptor: leaseholder replica %d not present", d.LeaseholderReplicaID)
		}
	}
	if _, _, err := d.CompiledPlacement(); err != nil {
		return err
	}
	return nil
}

// ContainsKey reports whether the descriptor spans the given logical key.
func (d RangeDescriptor) ContainsKey(key []byte) bool {
	if bytes.Compare(key, d.StartKey) < 0 {
		return false
	}
	if len(d.EndKey) == 0 {
		return true
	}
	return bytes.Compare(key, d.EndKey) < 0
}

// MarshalBinary encodes the descriptor into a stable JSON form.
func (d RangeDescriptor) MarshalBinary() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(d)
}

// UnmarshalBinary decodes the descriptor from its JSON form.
func (d *RangeDescriptor) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, d); err != nil {
		return fmt.Errorf("decode range descriptor: %w", err)
	}
	return d.Validate()
}

// CompiledPlacement resolves the descriptor's placement policy, if present.
func (d RangeDescriptor) CompiledPlacement() (placement.CompiledPolicy, bool, error) {
	if d.PlacementPolicy == nil {
		return placement.CompiledPolicy{}, false, nil
	}
	compiled, err := placement.Compile(*d.PlacementPolicy)
	if err != nil {
		return placement.CompiledPolicy{}, false, fmt.Errorf("range descriptor: invalid placement policy: %w", err)
	}
	return compiled, true, nil
}

// Validate checks the liveness record for obvious corruption.
func (l NodeLiveness) Validate() error {
	if l.NodeID == 0 {
		return fmt.Errorf("node liveness: node id must be non-zero")
	}
	if l.Epoch == 0 {
		return fmt.Errorf("node liveness: epoch must be non-zero")
	}
	if l.UpdatedAt.IsZero() {
		return fmt.Errorf("node liveness: updated_at must be non-zero")
	}
	return nil
}

// MarshalBinary encodes the liveness record into a stable JSON form.
func (l NodeLiveness) MarshalBinary() ([]byte, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(l)
}

// UnmarshalBinary decodes a NodeLiveness record.
func (l *NodeLiveness) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, l); err != nil {
		return fmt.Errorf("decode node liveness: %w", err)
	}
	return l.Validate()
}
