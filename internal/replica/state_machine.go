package replica

import (
	"bytes"
	"context"
	"errors"
	"fmt"

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

// StateMachine is the local replica apply state for one range on one node.
type StateMachine struct {
	rangeID          uint64
	replicaID        uint64
	engine           *storage.Engine
	appliedIndex     uint64
	lease            lease.Record
	descriptor       meta.RangeDescriptor
	safeReadFrontier hlc.Timestamp
}

// ApplyDelta contains the staged in-memory effects of committed log application.
type ApplyDelta struct {
	AppliedIndex     uint64
	Lease            lease.Record
	HasLease         bool
	Descriptor       meta.RangeDescriptor
	HasDescriptor    bool
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
		rangeID:      rangeID,
		replicaID:    replicaID,
		engine:       engine,
		appliedIndex: appliedIndex,
		lease:        record,
		descriptor:   desc,
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
