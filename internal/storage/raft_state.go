package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/cockroachdb/pebble"
	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// LoadRaftHardState loads one range's persisted HardState, if present.
func (e *Engine) LoadRaftHardState(rangeID uint64) (raftpb.HardState, error) {
	payload, err := e.GetRaw(nil, RaftHardStateKey(rangeID))
	if errors.Is(err, pebble.ErrNotFound) {
		return raftpb.HardState{}, nil
	}
	if err != nil {
		return raftpb.HardState{}, err
	}
	var st raftpb.HardState
	if err := st.Unmarshal(payload); err != nil {
		return raftpb.HardState{}, fmt.Errorf("decode hardstate: %w", err)
	}
	return st, nil
}

// LoadRaftEntries loads all persisted log entries for one range.
func (e *Engine) LoadRaftEntries(rangeID uint64) ([]raftpb.Entry, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: RaftEntryPrefix(rangeID),
		UpperBound: nextPrefix(RaftEntryPrefix(rangeID)),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var entries []raftpb.Entry
	for valid := iter.First(); valid; valid = iter.Next() {
		value := bytes.Clone(iter.Value())
		var entry raftpb.Entry
		if err := entry.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("decode raft entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, iter.Error()
}

// LoadRangeAppliedIndex loads the locally persisted applied index for a range.
func (e *Engine) LoadRangeAppliedIndex(rangeID uint64) (uint64, error) {
	payload, err := e.GetRaw(nil, RangeAppliedStateKey(rangeID))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(payload) != 8 {
		return 0, fmt.Errorf("decode applied index: want 8 bytes, got %d", len(payload))
	}
	return binary.BigEndian.Uint64(payload), nil
}

// LoadRangeLease loads the persisted lease record for a range.
func (e *Engine) LoadRangeLease(rangeID uint64) (lease.Record, error) {
	payload, err := e.GetRaw(nil, RangeLeaseKey(rangeID))
	if errors.Is(err, pebble.ErrNotFound) {
		return lease.Record{}, nil
	}
	if err != nil {
		return lease.Record{}, err
	}
	var record lease.Record
	if err := record.UnmarshalBinary(payload); err != nil {
		return lease.Record{}, fmt.Errorf("decode lease: %w", err)
	}
	return record, nil
}

// LoadRangeClosedTimestamp loads the persisted closed timestamp publication for a range.
func (e *Engine) LoadRangeClosedTimestamp(rangeID uint64) (closedts.Record, error) {
	payload, err := e.GetRaw(nil, RangeClosedTimestampKey(rangeID))
	if errors.Is(err, pebble.ErrNotFound) {
		return closedts.Record{}, nil
	}
	if err != nil {
		return closedts.Record{}, err
	}
	var record closedts.Record
	if err := record.UnmarshalBinary(payload); err != nil {
		return closedts.Record{}, fmt.Errorf("decode closed timestamp: %w", err)
	}
	return record, nil
}

// RehydrateRaftStorage rebuilds a MemoryStorage from durable range state.
func (e *Engine) RehydrateRaftStorage(rangeID uint64) (*raft.MemoryStorage, error) {
	mem := raft.NewMemoryStorage()
	st, err := e.LoadRaftHardState(rangeID)
	if err != nil {
		return nil, err
	}
	if !raft.IsEmptyHardState(st) {
		if err := mem.SetHardState(st); err != nil {
			return nil, err
		}
	}
	entries, err := e.LoadRaftEntries(rangeID)
	if err != nil {
		return nil, err
	}
	if len(entries) > 0 {
		if err := mem.Append(entries); err != nil {
			return nil, err
		}
	}
	return mem, nil
}

func nextPrefix(prefix []byte) []byte {
	out := bytes.Clone(prefix)
	return append(out, 0xff)
}
