package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/multiraft"
	"github.com/VenkatGGG/ChronosDb/internal/replica"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/cockroachdb/pebble"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// RangeStatus reports the local replica state for one hosted range.
type RangeStatus struct {
	RangeID          uint64
	LocalReplicaID   uint64
	LeaderReplicaID  uint64
	AppliedIndex     uint64
	Descriptor       meta.RangeDescriptor
	DescriptorSource string
	Hosted           bool
}

// SetPendingReplicaTarget installs a temporary replica-id to node-id mapping for outbound routing.
func (h *Host) SetPendingReplicaTarget(rangeID, replicaID, nodeID uint64) {
	if rangeID == 0 || replicaID == 0 || nodeID == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	targets := h.pendingReplicaTargets[rangeID]
	if targets == nil {
		targets = make(map[uint64]uint64)
		h.pendingReplicaTargets[rangeID] = targets
	}
	targets[replicaID] = nodeID
}

// ClearPendingReplicaTarget removes one temporary outbound routing override.
func (h *Host) ClearPendingReplicaTarget(rangeID, replicaID uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	targets := h.pendingReplicaTargets[rangeID]
	if len(targets) == 0 {
		return
	}
	delete(targets, replicaID)
	if len(targets) == 0 {
		delete(h.pendingReplicaTargets, rangeID)
	}
}

// RangeStatus returns the local scheduler and descriptor state for one range.
func (h *Host) RangeStatus(rangeID uint64) (RangeStatus, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	repl, err := h.scheduler.Replica(rangeID)
	if err != nil {
		return RangeStatus{RangeID: rangeID}, nil
	}
	leader, err := h.scheduler.Leader(rangeID)
	if err != nil {
		return RangeStatus{}, err
	}
	desc, source := h.descriptorForRangeLocked(rangeID, repl.Descriptor())
	localReplicaID, _ := localReplicaID(desc, h.ident.NodeID)
	return RangeStatus{
		RangeID:          rangeID,
		LocalReplicaID:   localReplicaID,
		LeaderReplicaID:  leader,
		AppliedIndex:     repl.AppliedIndex(),
		Descriptor:       desc,
		DescriptorSource: source,
		Hosted:           true,
	}, nil
}

// EnsureReplica prepares a local dormant replica group so it can accept inbound Raft traffic.
func (h *Host) EnsureReplica(rangeID, replicaID uint64, hint *meta.RangeDescriptor) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hint != nil {
		if err := hint.Validate(); err != nil {
			return err
		}
		if hint.RangeID != rangeID {
			return fmt.Errorf("runtime: prepare hint range %d does not match range %d", hint.RangeID, rangeID)
		}
		h.preparedDescriptors[rangeID] = cloneDescriptor(*hint)
		targets := make(map[uint64]uint64, len(hint.Replicas))
		for _, repl := range hint.Replicas {
			targets[repl.ReplicaID] = repl.NodeID
		}
		h.pendingReplicaTargets[rangeID] = targets
	}
	return h.scheduler.EnsureGroup(multiraft.GroupConfig{
		RangeID:   rangeID,
		ReplicaID: replicaID,
	})
}

// TransferLeader asks the current leader to hand leadership to another replica.
func (h *Host) TransferLeader(ctx context.Context, rangeID, transferee uint64) error {
	h.mu.Lock()
	if err := h.scheduler.TransferLeader(rangeID, transferee); err != nil {
		h.mu.Unlock()
		return err
	}
	outbound, err := h.processReadyLocked()
	h.mu.Unlock()
	if err != nil {
		return err
	}
	return h.dispatchOutbound(ctx, outbound)
}

// ProposeConfChange appends one membership change to the live Raft group.
func (h *Host) ProposeConfChange(ctx context.Context, rangeID uint64, cc raftpb.ConfChange, targetNodeID uint64) error {
	h.mu.Lock()
	desc, err := h.currentDescriptorLocked(rangeID)
	if err != nil {
		h.mu.Unlock()
		return err
	}
	if cc.NodeID != 0 && targetNodeID != 0 {
		targets := h.pendingReplicaTargets[rangeID]
		if targets == nil {
			targets = make(map[uint64]uint64)
			h.pendingReplicaTargets[rangeID] = targets
		}
		targets[cc.NodeID] = targetNodeID
	}
	if err := h.scheduler.ProposeConfChange(rangeID, cc); err != nil {
		localReplica, ok := localReplicaID(desc, h.ident.NodeID)
		if !ok || localReplica != desc.LeaseholderReplicaID {
			h.mu.Unlock()
			return err
		}
		h.mu.Unlock()
		campaignErr := h.Campaign(ctx, rangeID)
		h.mu.Lock()
		if campaignErr != nil {
			h.mu.Unlock()
			return err
		}
		if retryErr := h.scheduler.ProposeConfChange(rangeID, cc); retryErr != nil {
			h.mu.Unlock()
			return retryErr
		}
	}
	outbound, err := h.processReadyLocked()
	h.mu.Unlock()
	if err != nil {
		return err
	}
	return h.dispatchOutbound(ctx, outbound)
}

// ApplyReplicaChange persists one descriptor-level replica-set update.
func (h *Host) ApplyReplicaChange(ctx context.Context, rangeID uint64, metaLevel meta.Level, change replica.ReplicaChange) error {
	h.mu.Lock()
	desc, err := h.currentDescriptorLocked(rangeID)
	if err != nil {
		h.mu.Unlock()
		return err
	}
	payload, err := replica.Command{
		Version: 1,
		Type:    replica.CommandTypeChangeReplicas,
		Replica: &replica.ChangeReplicas{
			ExpectedGeneration: desc.Generation,
			MetaLevel:          metaLevel,
			Change:             change,
		},
	}.Marshal()
	if err != nil {
		h.mu.Unlock()
		return err
	}
	outbound, err := h.proposeWithLeaseholderRetryLocked(ctx, desc, payload)
	h.mu.Unlock()
	if err != nil {
		return err
	}
	return h.dispatchOutbound(ctx, outbound)
}

// UpdateLeaseholderHint moves the descriptor-designated leaseholder to another replica.
func (h *Host) UpdateLeaseholderHint(ctx context.Context, rangeID, targetReplicaID uint64) error {
	h.mu.Lock()
	desc, err := h.currentDescriptorLocked(rangeID)
	if err != nil {
		h.mu.Unlock()
		return err
	}
	if _, ok := nodeForReplica(desc, targetReplicaID); !ok {
		h.mu.Unlock()
		return fmt.Errorf("runtime: range %d target replica %d not found", rangeID, targetReplicaID)
	}
	if desc.LeaseholderReplicaID == targetReplicaID {
		h.mu.Unlock()
		return nil
	}
	next := desc
	next.Generation = desc.Generation + 1
	next.LeaseholderReplicaID = targetReplicaID
	payload, err := replica.Command{
		Version: 1,
		Type:    replica.CommandTypeUpdateDescriptor,
		Descriptor: &replica.UpdateDescriptor{
			ExpectedGeneration: desc.Generation,
			Descriptor:         next,
		},
	}.Marshal()
	if err != nil {
		h.mu.Unlock()
		return err
	}
	outbound, err := h.proposeWithLeaseholderRetryLocked(ctx, desc, payload)
	h.mu.Unlock()
	if err != nil {
		return err
	}
	return h.dispatchOutbound(ctx, outbound)
}

func (h *Host) currentDescriptorLocked(rangeID uint64) (meta.RangeDescriptor, error) {
	repl, err := h.scheduler.Replica(rangeID)
	if err == nil {
		desc, _ := h.descriptorForRangeLocked(rangeID, repl.Descriptor())
		if desc.RangeID != 0 {
			return desc, nil
		}
	}
	if desc, err := h.loadPersistedDescriptorLocked(rangeID); err == nil {
		return desc, nil
	} else if !errors.Is(err, ErrRangeNotHosted) {
		return meta.RangeDescriptor{}, err
	}
	return meta.RangeDescriptor{}, fmt.Errorf("runtime: range %d descriptor not found", rangeID)
}

func (h *Host) descriptorForRangeLocked(rangeID uint64, current meta.RangeDescriptor) (meta.RangeDescriptor, string) {
	if current.RangeID != 0 {
		return current, "applied"
	}
	if desc, ok := h.preparedDescriptors[rangeID]; ok {
		return cloneDescriptor(desc), "prepared"
	}
	desc, err := h.loadPersistedDescriptorLocked(rangeID)
	if err == nil {
		return desc, "persisted"
	}
	return current, "none"
}

func (h *Host) loadPersistedDescriptorLocked(rangeID uint64) (meta.RangeDescriptor, error) {
	payload, err := h.engine.GetRaw(context.Background(), storage.RangeDescriptorKey(rangeID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return meta.RangeDescriptor{}, ErrRangeNotHosted
		}
		return meta.RangeDescriptor{}, err
	}
	var desc meta.RangeDescriptor
	if err := desc.UnmarshalBinary(payload); err != nil {
		return meta.RangeDescriptor{}, err
	}
	return desc, nil
}

func cloneDescriptor(desc meta.RangeDescriptor) meta.RangeDescriptor {
	cloned := desc
	cloned.StartKey = append([]byte(nil), desc.StartKey...)
	cloned.EndKey = append([]byte(nil), desc.EndKey...)
	cloned.Replicas = append([]meta.ReplicaDescriptor(nil), desc.Replicas...)
	return cloned
}
