package replica

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/cockroachdb/pebble"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// ErrDescriptorGenerationMismatch reports that a stale descriptor update was proposed.
var ErrDescriptorGenerationMismatch = errors.New("descriptor generation mismatch")

// ErrInvalidSplitTrigger reports that a split trigger violates the written replica-state contract.
var ErrInvalidSplitTrigger = errors.New("invalid split trigger")

// ErrInvalidReplicaChange reports that a rebalance step violates the membership rules.
var ErrInvalidReplicaChange = errors.New("invalid replica change")

// ErrStaleSnapshot reports that a snapshot image is older than the descriptor already applied locally.
var ErrStaleSnapshot = errors.New("stale snapshot image")

// ErrInvalidClosedTimestamp reports that a closed timestamp publication violates lease-bound rules.
var ErrInvalidClosedTimestamp = errors.New("invalid closed timestamp")

// StateMachine is the local replica apply state for one range on one node.
type StateMachine struct {
	rangeID          uint64
	replicaID        uint64
	engine           *storage.Engine
	appliedIndex     uint64
	lease            lease.Record
	closedTimestamp  closedts.Record
	hasClosedTS      bool
	descriptor       meta.RangeDescriptor
	safeReadFrontier hlc.Timestamp
}

// ApplyDelta contains the staged in-memory effects of committed log application.
type ApplyDelta struct {
	AppliedIndex     uint64
	Lease            lease.Record
	HasLease         bool
	ClosedTimestamp  closedts.Record
	HasClosedTS      bool
	Descriptor       meta.RangeDescriptor
	HasDescriptor    bool
	SafeReadFrontier hlc.Timestamp
}

// SnapshotImage is the self-consistent local replica image installed by snapshot application.
type SnapshotImage struct {
	AppliedIndex     uint64
	Descriptor       meta.RangeDescriptor
	Lease            lease.Record
	HasLease         bool
	ClosedTimestamp  closedts.Record
	HasClosedTS      bool
	SafeReadFrontier hlc.Timestamp
}

// OpenStateMachine loads persisted local apply state for one range replica.
func OpenStateMachine(rangeID, replicaID uint64, engine *storage.Engine) (*StateMachine, error) {
	appliedIndex, err := engine.LoadRangeAppliedIndex(rangeID)
	if err != nil {
		return nil, err
	}
	record, err := engine.LoadRangeLease(rangeID)
	if err != nil {
		return nil, err
	}
	closedRecord, err := engine.LoadRangeClosedTimestamp(rangeID)
	if err != nil {
		return nil, err
	}
	var desc meta.RangeDescriptor
	descPayload, err := engine.GetRaw(nil, storage.RangeDescriptorKey(rangeID))
	switch {
	case errors.Is(err, pebble.ErrNotFound):
	case err == nil:
		if decodeErr := desc.UnmarshalBinary(descPayload); decodeErr != nil {
			return nil, decodeErr
		}
	default:
		return nil, err
	}
	return &StateMachine{
		rangeID:         rangeID,
		replicaID:       replicaID,
		engine:          engine,
		appliedIndex:    appliedIndex,
		lease:           record,
		closedTimestamp: closedRecord,
		hasClosedTS:     closedRecord.RangeID != 0,
		descriptor:      desc,
	}, nil
}

// RangeID returns the owning range id.
func (s *StateMachine) RangeID() uint64 {
	return s.rangeID
}

// ReplicaID returns the local replica id.
func (s *StateMachine) ReplicaID() uint64 {
	return s.replicaID
}

// AppliedIndex returns the latest locally applied log index.
func (s *StateMachine) AppliedIndex() uint64 {
	return s.appliedIndex
}

// Lease returns the latest applied lease record.
func (s *StateMachine) Lease() lease.Record {
	return s.lease
}

// ClosedTimestamp returns the latest applied closed timestamp publication, if present.
func (s *StateMachine) ClosedTimestamp() (closedts.Record, bool) {
	return s.closedTimestamp, s.hasClosedTS
}

// Descriptor returns the latest applied range descriptor.
func (s *StateMachine) Descriptor() meta.RangeDescriptor {
	return s.descriptor
}

// SafeReadFrontier returns the highest exact committed timestamp proven locally.
func (s *StateMachine) SafeReadFrontier() hlc.Timestamp {
	return s.safeReadFrontier
}

// ReadExact reads one exact committed MVCC version without lease validation.
func (s *StateMachine) ReadExact(ctx context.Context, logicalKey []byte, ts hlc.Timestamp) ([]byte, error) {
	return s.engine.GetMVCCValue(ctx, logicalKey, ts)
}

// StageEntries validates and stages committed log entries into the provided batch.
func (s *StateMachine) StageEntries(batch *storage.WriteBatch, entries []raftpb.Entry) (ApplyDelta, error) {
	delta := ApplyDelta{
		AppliedIndex:     s.appliedIndex,
		SafeReadFrontier: s.safeReadFrontier,
	}
	for _, entry := range entries {
		delta.AppliedIndex = entry.Index
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				continue
			}
			cmd, err := UnmarshalCommand(entry.Data)
			if err != nil {
				return ApplyDelta{}, err
			}
			switch cmd.Type {
			case CommandTypePutValue:
				if err := batch.PutMVCCValue(cmd.Put.LogicalKey, cmd.Put.Timestamp, cmd.Put.Value); err != nil {
					return ApplyDelta{}, err
				}
				if cmd.Put.Timestamp.Compare(delta.SafeReadFrontier) > 0 {
					delta.SafeReadFrontier = cmd.Put.Timestamp
				}
			case CommandTypeSetLease:
				if err := batch.SetRangeLease(s.rangeID, cmd.Lease.Record); err != nil {
					return ApplyDelta{}, err
				}
				delta.Lease = cmd.Lease.Record
				delta.HasLease = true
			case CommandTypeSetClosedTS:
				currentLease := s.lease
				if delta.HasLease {
					currentLease = delta.Lease
				}
				currentClosedTS, hasCurrentClosedTS := s.closedTimestamp, s.hasClosedTS
				if delta.HasClosedTS {
					currentClosedTS, hasCurrentClosedTS = delta.ClosedTimestamp, true
				}
				if err := validateClosedTimestampUpdate(s.rangeID, currentLease, currentClosedTS, hasCurrentClosedTS, cmd.ClosedTS.Record); err != nil {
					return ApplyDelta{}, err
				}
				if err := batch.SetRangeClosedTimestamp(s.rangeID, cmd.ClosedTS.Record); err != nil {
					return ApplyDelta{}, err
				}
				delta.ClosedTimestamp = cmd.ClosedTS.Record
				delta.HasClosedTS = true
			case CommandTypeUpdateDescriptor:
				currentGeneration := s.descriptor.Generation
				if delta.HasDescriptor {
					currentGeneration = delta.Descriptor.Generation
				}
				if cmd.Descriptor.ExpectedGeneration != currentGeneration {
					return ApplyDelta{}, fmt.Errorf("%w: expected %d got %d", ErrDescriptorGenerationMismatch, cmd.Descriptor.ExpectedGeneration, currentGeneration)
				}
				payload, err := cmd.Descriptor.Descriptor.MarshalBinary()
				if err != nil {
					return ApplyDelta{}, err
				}
				if err := batch.SetRaw(storage.RangeDescriptorKey(s.rangeID), payload); err != nil {
					return ApplyDelta{}, err
				}
				delta.Descriptor = cmd.Descriptor.Descriptor
				delta.HasDescriptor = true
			case CommandTypeSplitRange:
				currentDescriptor := s.descriptor
				if delta.HasDescriptor {
					currentDescriptor = delta.Descriptor
				}
				if cmd.Split.ExpectedGeneration != currentDescriptor.Generation {
					return ApplyDelta{}, fmt.Errorf("%w: expected %d got %d", ErrDescriptorGenerationMismatch, cmd.Split.ExpectedGeneration, currentDescriptor.Generation)
				}
				if err := validateSplitTrigger(currentDescriptor, cmd.Split.Left, cmd.Split.Right); err != nil {
					return ApplyDelta{}, err
				}
				leftPayload, err := cmd.Split.Left.MarshalBinary()
				if err != nil {
					return ApplyDelta{}, err
				}
				if err := batch.SetRaw(storage.RangeDescriptorKey(cmd.Split.Left.RangeID), leftPayload); err != nil {
					return ApplyDelta{}, err
				}
				rightPayload, err := cmd.Split.Right.MarshalBinary()
				if err != nil {
					return ApplyDelta{}, err
				}
				if err := batch.SetRaw(storage.RangeDescriptorKey(cmd.Split.Right.RangeID), rightPayload); err != nil {
					return ApplyDelta{}, err
				}
				if err := batch.SetRangeAppliedIndex(cmd.Split.Right.RangeID, delta.AppliedIndex); err != nil {
					return ApplyDelta{}, err
				}
				if err := stageSplitMeta(batch, cmd.Split.MetaLevel, currentDescriptor.EndKey, cmd.Split.Left, cmd.Split.Right); err != nil {
					return ApplyDelta{}, err
				}
				delta.Descriptor = cmd.Split.Left
				delta.HasDescriptor = true
			case CommandTypeChangeReplicas:
				currentDescriptor := s.descriptor
				if delta.HasDescriptor {
					currentDescriptor = delta.Descriptor
				}
				if cmd.Replica.ExpectedGeneration != currentDescriptor.Generation {
					return ApplyDelta{}, fmt.Errorf("%w: expected %d got %d", ErrDescriptorGenerationMismatch, cmd.Replica.ExpectedGeneration, currentDescriptor.Generation)
				}
				nextDescriptor, err := applyReplicaChange(currentDescriptor, cmd.Replica.Change)
				if err != nil {
					return ApplyDelta{}, err
				}
				payload, err := nextDescriptor.MarshalBinary()
				if err != nil {
					return ApplyDelta{}, err
				}
				if err := batch.SetRaw(storage.RangeDescriptorKey(s.rangeID), payload); err != nil {
					return ApplyDelta{}, err
				}
				if err := batch.SetRaw(descriptorKeyForLevel(cmd.Replica.MetaLevel, nextDescriptor.EndKey), payload); err != nil {
					return ApplyDelta{}, err
				}
				delta.Descriptor = nextDescriptor
				delta.HasDescriptor = true
			default:
				return ApplyDelta{}, fmt.Errorf("stage entry: unhandled command %q", cmd.Type)
			}
		case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
			// Membership changes are handled in a later phase. The log still advances.
		default:
			return ApplyDelta{}, fmt.Errorf("stage entry: unsupported entry type %v", entry.Type)
		}
	}
	if delta.AppliedIndex > s.appliedIndex {
		if err := batch.SetRangeAppliedIndex(s.rangeID, delta.AppliedIndex); err != nil {
			return ApplyDelta{}, err
		}
	}
	return delta, nil
}

// CommitApply makes a staged apply delta visible in local in-memory state.
func (s *StateMachine) CommitApply(delta ApplyDelta) {
	if delta.AppliedIndex > s.appliedIndex {
		s.appliedIndex = delta.AppliedIndex
	}
	if delta.HasLease {
		s.lease = delta.Lease
	}
	if delta.HasClosedTS {
		s.closedTimestamp = delta.ClosedTimestamp
		s.hasClosedTS = true
	}
	if delta.HasDescriptor {
		s.descriptor = delta.Descriptor
	}
	if delta.SafeReadFrontier.Compare(s.safeReadFrontier) > 0 {
		s.safeReadFrontier = delta.SafeReadFrontier
	}
}

// FastGet serves one exact MVCC version if the local lease is valid.
func (s *StateMachine) FastGet(ctx context.Context, logicalKey []byte, readTS hlc.Timestamp, req lease.FastReadRequest) ([]byte, error) {
	req.ReplicaID = s.replicaID
	req.AppliedLeaseSequence = s.lease.Sequence
	req.SafeReadFrontier = s.safeReadFrontier
	if err := s.lease.CanServeFastRead(req); err != nil {
		return nil, err
	}
	value, err := s.engine.GetMVCCValue(ctx, logicalKey, readTS)
	if errors.Is(err, storage.ErrMVCCValueNotFound) {
		return nil, err
	}
	return value, err
}

// HistoricalGet serves one exact MVCC version from the follower-historical path if the closed timestamp permits it.
func (s *StateMachine) HistoricalGet(ctx context.Context, logicalKey []byte, readTS hlc.Timestamp) ([]byte, error) {
	if !s.hasClosedTS {
		return nil, closedts.ErrFollowerReadTooFresh
	}
	if err := s.closedTimestamp.CanServeFollowerRead(closedts.FollowerReadRequest{
		ReadTS:             readTS,
		AppliedThrough:     s.closedTimestamp.ClosedTS,
		KnownLeaseSequence: s.lease.Sequence,
	}); err != nil {
		return nil, err
	}
	value, err := s.engine.GetMVCCValue(ctx, logicalKey, readTS)
	if errors.Is(err, storage.ErrMVCCValueNotFound) {
		return nil, err
	}
	return value, err
}

// ApplySnapshot replaces the local replica image with a self-consistent snapshot.
func (s *StateMachine) ApplySnapshot(image SnapshotImage) error {
	if err := image.Descriptor.Validate(); err != nil {
		return err
	}
	if image.Descriptor.RangeID != s.rangeID {
		return fmt.Errorf("apply snapshot: descriptor range %d does not match state machine range %d", image.Descriptor.RangeID, s.rangeID)
	}
	if image.AppliedIndex == 0 {
		return fmt.Errorf("apply snapshot: applied index must be non-zero")
	}
	if image.HasLease {
		if err := image.Lease.Validate(); err != nil {
			return err
		}
	}
	if image.HasClosedTS {
		if err := validateClosedTimestampUpdate(s.rangeID, image.Lease, s.closedTimestamp, s.hasClosedTS, image.ClosedTimestamp); err != nil {
			return err
		}
	}
	if s.descriptor.Generation > 0 && image.Descriptor.Generation < s.descriptor.Generation {
		return fmt.Errorf("%w: current=%d incoming=%d", ErrStaleSnapshot, s.descriptor.Generation, image.Descriptor.Generation)
	}

	batch := s.engine.NewWriteBatch()
	defer batch.Close()

	payload, err := image.Descriptor.MarshalBinary()
	if err != nil {
		return err
	}
	if err := batch.SetRaw(storage.RangeDescriptorKey(s.rangeID), payload); err != nil {
		return err
	}
	if err := batch.SetRangeAppliedIndex(s.rangeID, image.AppliedIndex); err != nil {
		return err
	}
	if image.HasLease {
		if err := batch.SetRangeLease(s.rangeID, image.Lease); err != nil {
			return err
		}
	} else {
		if err := batch.DeleteRaw(storage.RangeLeaseKey(s.rangeID)); err != nil {
			return err
		}
	}
	if image.HasClosedTS {
		if err := batch.SetRangeClosedTimestamp(s.rangeID, image.ClosedTimestamp); err != nil {
			return err
		}
	} else {
		if err := batch.DeleteRaw(storage.RangeClosedTimestampKey(s.rangeID)); err != nil {
			return err
		}
	}
	if err := batch.Commit(true); err != nil {
		return err
	}

	s.appliedIndex = image.AppliedIndex
	s.descriptor = image.Descriptor
	if image.HasLease {
		s.lease = image.Lease
	} else {
		s.lease = lease.Record{}
	}
	if image.HasClosedTS {
		s.closedTimestamp = image.ClosedTimestamp
		s.hasClosedTS = true
	} else {
		s.closedTimestamp = closedts.Record{}
		s.hasClosedTS = false
	}
	s.safeReadFrontier = image.SafeReadFrontier
	return nil
}

func validateClosedTimestampUpdate(rangeID uint64, currentLease lease.Record, current closedts.Record, hasCurrent bool, next closedts.Record) error {
	if next.RangeID != rangeID {
		return fmt.Errorf("%w: record range %d does not match replica range %d", ErrInvalidClosedTimestamp, next.RangeID, rangeID)
	}
	if err := currentLease.Validate(); err != nil {
		return fmt.Errorf("%w: lease is required before publishing closed timestamps", ErrInvalidClosedTimestamp)
	}
	if next.LeaseSequence != currentLease.Sequence {
		return fmt.Errorf("%w: publication lease sequence %d does not match current lease sequence %d", ErrInvalidClosedTimestamp, next.LeaseSequence, currentLease.Sequence)
	}
	if hasCurrent {
		if next.LeaseSequence < current.LeaseSequence {
			return fmt.Errorf("%w: publication sequence regressed from %d to %d", ErrInvalidClosedTimestamp, current.LeaseSequence, next.LeaseSequence)
		}
		if next.LeaseSequence == current.LeaseSequence && next.ClosedTS.Compare(current.ClosedTS) < 0 {
			return fmt.Errorf("%w: publication timestamp regressed from %v to %v", ErrInvalidClosedTimestamp, current.ClosedTS, next.ClosedTS)
		}
	}
	return nil
}

func validateSplitTrigger(current, left, right meta.RangeDescriptor) error {
	if current.RangeID == 0 {
		return fmt.Errorf("%w: current descriptor is missing", ErrInvalidSplitTrigger)
	}
	if left.RangeID != current.RangeID {
		return fmt.Errorf("%w: left range %d does not match parent %d", ErrInvalidSplitTrigger, left.RangeID, current.RangeID)
	}
	if right.RangeID == current.RangeID {
		return fmt.Errorf("%w: right range must differ from parent", ErrInvalidSplitTrigger)
	}
	if !bytes.Equal(left.StartKey, current.StartKey) {
		return fmt.Errorf("%w: left start key must preserve parent start", ErrInvalidSplitTrigger)
	}
	if len(left.EndKey) == 0 {
		return fmt.Errorf("%w: split point must be finite", ErrInvalidSplitTrigger)
	}
	if !bytes.Equal(left.EndKey, right.StartKey) {
		return fmt.Errorf("%w: child spans must be contiguous", ErrInvalidSplitTrigger)
	}
	if !bytes.Equal(right.EndKey, current.EndKey) {
		return fmt.Errorf("%w: right end key must preserve parent end", ErrInvalidSplitTrigger)
	}
	if left.Generation <= current.Generation {
		return fmt.Errorf("%w: left generation must advance past parent generation", ErrInvalidSplitTrigger)
	}
	if !sameReplicaSet(left.Replicas, current.Replicas) {
		return fmt.Errorf("%w: left replica set must preserve parent replicas", ErrInvalidSplitTrigger)
	}
	if !sameReplicaSet(right.Replicas, current.Replicas) {
		return fmt.Errorf("%w: right replica set must preserve parent replicas", ErrInvalidSplitTrigger)
	}
	if left.LeaseholderReplicaID != current.LeaseholderReplicaID {
		return fmt.Errorf("%w: left leaseholder hint must preserve parent leaseholder", ErrInvalidSplitTrigger)
	}
	if right.LeaseholderReplicaID != current.LeaseholderReplicaID {
		return fmt.Errorf("%w: right leaseholder hint must preserve parent leaseholder", ErrInvalidSplitTrigger)
	}
	return nil
}

func applyReplicaChange(current meta.RangeDescriptor, change ReplicaChange) (meta.RangeDescriptor, error) {
	next := current
	next.Replicas = append([]meta.ReplicaDescriptor(nil), current.Replicas...)
	next.Generation = current.Generation + 1

	index := findReplicaIndex(current.Replicas, change.Replica.ReplicaID)
	switch change.Kind {
	case ReplicaChangeAddLearner:
		if index >= 0 {
			return meta.RangeDescriptor{}, fmt.Errorf("%w: learner replica %d already exists", ErrInvalidReplicaChange, change.Replica.ReplicaID)
		}
		next.Replicas = append(next.Replicas, change.Replica)
	case ReplicaChangePromote:
		if index < 0 {
			return meta.RangeDescriptor{}, fmt.Errorf("%w: learner replica %d not found", ErrInvalidReplicaChange, change.Replica.ReplicaID)
		}
		if current.Replicas[index].Role != meta.ReplicaRoleLearner {
			return meta.RangeDescriptor{}, fmt.Errorf("%w: replica %d is not a learner", ErrInvalidReplicaChange, change.Replica.ReplicaID)
		}
		next.Replicas[index] = change.Replica
	case ReplicaChangeRemove:
		if index < 0 {
			return meta.RangeDescriptor{}, fmt.Errorf("%w: replica %d not found", ErrInvalidReplicaChange, change.Replica.ReplicaID)
		}
		if current.LeaseholderReplicaID == change.Replica.ReplicaID {
			return meta.RangeDescriptor{}, fmt.Errorf("%w: transfer lease before removing leaseholder replica %d", ErrInvalidReplicaChange, change.Replica.ReplicaID)
		}
		next.Replicas = append(next.Replicas[:index], next.Replicas[index+1:]...)
		if countVoters(next.Replicas) == 0 {
			return meta.RangeDescriptor{}, fmt.Errorf("%w: removal would leave the range without voters", ErrInvalidReplicaChange)
		}
	default:
		return meta.RangeDescriptor{}, fmt.Errorf("%w: unknown replica change %q", ErrInvalidReplicaChange, change.Kind)
	}
	if err := next.Validate(); err != nil {
		return meta.RangeDescriptor{}, fmt.Errorf("%w: %v", ErrInvalidReplicaChange, err)
	}
	return next, nil
}

func findReplicaIndex(replicas []meta.ReplicaDescriptor, replicaID uint64) int {
	for i, replica := range replicas {
		if replica.ReplicaID == replicaID {
			return i
		}
	}
	return -1
}

func countVoters(replicas []meta.ReplicaDescriptor) int {
	count := 0
	for _, replica := range replicas {
		if replica.Role == meta.ReplicaRoleVoter {
			count++
		}
	}
	return count
}

func sameReplicaSet(left, right []meta.ReplicaDescriptor) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func stageSplitMeta(batch *storage.WriteBatch, level meta.Level, parentEndKey []byte, left, right meta.RangeDescriptor) error {
	if err := batch.DeleteRaw(descriptorKeyForLevel(level, parentEndKey)); err != nil {
		return err
	}
	leftPayload, err := left.MarshalBinary()
	if err != nil {
		return err
	}
	if err := batch.SetRaw(descriptorKeyForLevel(level, left.EndKey), leftPayload); err != nil {
		return err
	}
	rightPayload, err := right.MarshalBinary()
	if err != nil {
		return err
	}
	return batch.SetRaw(descriptorKeyForLevel(level, right.EndKey), rightPayload)
}

func descriptorKeyForLevel(level meta.Level, endKey []byte) []byte {
	switch level {
	case meta.LevelMeta1:
		return storage.Meta1DescriptorKey(endKey)
	case meta.LevelMeta2:
		return storage.Meta2DescriptorKey(endKey)
	default:
		panic(fmt.Sprintf("unknown meta level %d", level))
	}
}
