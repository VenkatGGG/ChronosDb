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
)

// ReplicaSnapshot is a runtime-portable snapshot image plus the raw data and
// raft state required to seed a new learner replica.
type ReplicaSnapshot struct {
	Image replica.SnapshotImage `json:"image"`
	Data  []storage.RawKV       `json:"data"`
	Raft  []storage.RawKV       `json:"raft"`
}

// CaptureReplicaSnapshot captures the current replica image, its user span, and
// the persisted raft state required to seed a new learner.
func (h *Host) CaptureReplicaSnapshot(rangeID uint64) (ReplicaSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	repl, err := h.scheduler.Replica(rangeID)
	if err != nil {
		return ReplicaSnapshot{}, err
	}
	desc := repl.Descriptor()
	if err := desc.Validate(); err != nil {
		return ReplicaSnapshot{}, err
	}

	image := replica.SnapshotImage{
		AppliedIndex:     repl.AppliedIndex(),
		Descriptor:       cloneDescriptor(desc),
		SafeReadFrontier: repl.SafeReadFrontier(),
	}
	if leaseRecord := repl.Lease(); leaseRecord.Sequence != 0 {
		image.Lease = leaseRecord
		image.HasLease = true
	}
	if publication, ok := repl.ClosedTimestamp(); ok {
		image.ClosedTimestamp = publication
		image.HasClosedTS = true
	}

	data, err := h.engine.ScanRawRange(desc.StartKey, desc.EndKey)
	if err != nil {
		return ReplicaSnapshot{}, err
	}
	raftKVs, err := h.engine.ScanRawRange(storage.RaftEntryPrefix(rangeID), storage.PrefixEnd(storage.RaftEntryPrefix(rangeID)))
	if err != nil {
		return ReplicaSnapshot{}, err
	}
	hardState, err := h.engine.GetRaw(context.Background(), storage.RaftHardStateKey(rangeID))
	switch {
	case err == nil:
		raftKVs = append(raftKVs, storage.RawKV{
			Key:   storage.RaftHardStateKey(rangeID),
			Value: hardState,
		})
	case errors.Is(err, pebble.ErrNotFound):
	default:
		return ReplicaSnapshot{}, err
	}

	return ReplicaSnapshot{
		Image: image,
		Data:  data,
		Raft:  raftKVs,
	}, nil
}

// InstallReplicaSnapshot seeds a new local replica with a pre-captured snapshot
// image, its raw data span, and the raft state needed to accept incremental log
// traffic from the leader.
func (h *Host) InstallReplicaSnapshot(req ReplicaInstallRequest) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if req.Snapshot.Image.Descriptor.RangeID != req.RangeID {
		return fmt.Errorf("runtime: snapshot range %d does not match install range %d", req.Snapshot.Image.Descriptor.RangeID, req.RangeID)
	}
	if err := req.Snapshot.Image.Descriptor.Validate(); err != nil {
		return err
	}
	if req.Snapshot.Image.AppliedIndex == 0 {
		return fmt.Errorf("runtime: snapshot applied index must be non-zero")
	}
	preparedDesc := cloneDescriptor(req.Snapshot.Image.Descriptor)
	if req.DescriptorHint != nil {
		if err := req.DescriptorHint.Validate(); err != nil {
			return err
		}
		if req.DescriptorHint.RangeID != req.RangeID {
			return fmt.Errorf("runtime: descriptor hint range %d does not match install range %d", req.DescriptorHint.RangeID, req.RangeID)
		}
		preparedDesc = cloneDescriptor(*req.DescriptorHint)
	}

	batch := h.engine.NewWriteBatch()
	defer batch.Close()

	descPayload, err := req.Snapshot.Image.Descriptor.MarshalBinary()
	if err != nil {
		return err
	}
	if err := batch.SetRaw(storage.RangeDescriptorKey(req.RangeID), descPayload); err != nil {
		return err
	}
	if err := batch.SetRangeAppliedIndex(req.RangeID, req.Snapshot.Image.AppliedIndex); err != nil {
		return err
	}
	if req.Snapshot.Image.HasLease {
		if err := batch.SetRangeLease(req.RangeID, req.Snapshot.Image.Lease); err != nil {
			return err
		}
	} else {
		if err := batch.DeleteRaw(storage.RangeLeaseKey(req.RangeID)); err != nil {
			return err
		}
	}
	if req.Snapshot.Image.HasClosedTS {
		if err := batch.SetRangeClosedTimestamp(req.RangeID, req.Snapshot.Image.ClosedTimestamp); err != nil {
			return err
		}
	} else {
		if err := batch.DeleteRaw(storage.RangeClosedTimestampKey(req.RangeID)); err != nil {
			return err
		}
	}
	for _, kv := range req.Snapshot.Data {
		if err := batch.SetRaw(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	for _, kv := range req.Snapshot.Raft {
		if err := batch.SetRaw(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	if err := batch.Commit(true); err != nil {
		return err
	}

	h.preparedDescriptors[req.RangeID] = preparedDesc
	targets := make(map[uint64]uint64, len(preparedDesc.Replicas))
	peers := make([]uint64, 0, len(preparedDesc.Replicas))
	for _, repl := range preparedDesc.Replicas {
		targets[repl.ReplicaID] = repl.NodeID
		if repl.Role == meta.ReplicaRoleVoter {
			peers = append(peers, repl.ReplicaID)
		}
	}
	h.pendingReplicaTargets[req.RangeID] = targets
	return h.scheduler.EnsureGroup(multiraft.GroupConfig{
		RangeID:   req.RangeID,
		ReplicaID: req.ReplicaID,
		Peers:     peers,
	})
}
