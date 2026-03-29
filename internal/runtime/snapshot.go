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
func (h *Host) CaptureReplicaSnapshot(_ context.Context, rangeID uint64) (ReplicaSnapshot, error) {
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
func (h *Host) InstallReplicaSnapshot(_ context.Context, rangeID, replicaID uint64, snapshot ReplicaSnapshot) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if snapshot.Image.Descriptor.RangeID != rangeID {
		return fmt.Errorf("runtime: snapshot range %d does not match install range %d", snapshot.Image.Descriptor.RangeID, rangeID)
	}
	if err := snapshot.Image.Descriptor.Validate(); err != nil {
		return err
	}
	if snapshot.Image.AppliedIndex == 0 {
		return fmt.Errorf("runtime: snapshot applied index must be non-zero")
	}

	batch := h.engine.NewWriteBatch()
	defer batch.Close()

	descPayload, err := snapshot.Image.Descriptor.MarshalBinary()
	if err != nil {
		return err
	}
	if err := batch.SetRaw(storage.RangeDescriptorKey(rangeID), descPayload); err != nil {
		return err
	}
	if err := batch.SetRangeAppliedIndex(rangeID, snapshot.Image.AppliedIndex); err != nil {
		return err
	}
	if snapshot.Image.HasLease {
		if err := batch.SetRangeLease(rangeID, snapshot.Image.Lease); err != nil {
			return err
		}
	} else {
		if err := batch.DeleteRaw(storage.RangeLeaseKey(rangeID)); err != nil {
			return err
		}
	}
	if snapshot.Image.HasClosedTS {
		if err := batch.SetRangeClosedTimestamp(rangeID, snapshot.Image.ClosedTimestamp); err != nil {
			return err
		}
	} else {
		if err := batch.DeleteRaw(storage.RangeClosedTimestampKey(rangeID)); err != nil {
			return err
		}
	}
	for _, kv := range snapshot.Data {
		if err := batch.SetRaw(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	for _, kv := range snapshot.Raft {
		if err := batch.SetRaw(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	if err := batch.Commit(true); err != nil {
		return err
	}

	h.preparedDescriptors[rangeID] = cloneDescriptor(snapshot.Image.Descriptor)
	targets := make(map[uint64]uint64, len(snapshot.Image.Descriptor.Replicas))
	peers := make([]uint64, 0, len(snapshot.Image.Descriptor.Replicas))
	for _, repl := range snapshot.Image.Descriptor.Replicas {
		targets[repl.ReplicaID] = repl.NodeID
		if repl.Role == meta.ReplicaRoleVoter {
			peers = append(peers, repl.ReplicaID)
		}
	}
	h.pendingReplicaTargets[rangeID] = targets
	return h.scheduler.EnsureGroup(multiraft.GroupConfig{
		RangeID:   rangeID,
		ReplicaID: replicaID,
		Peers:     peers,
	})
}
