package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/cockroachdb/pebble"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// WriteBatch is the durable unit used by the MultiRaft scheduler.
type WriteBatch struct {
	batch   *pebble.Batch
	opCount int
}

// NewWriteBatch returns a Pebble batch tracked for empty-commit elision.
func (e *Engine) NewWriteBatch() *WriteBatch {
	return &WriteBatch{batch: e.db.NewBatch()}
}

// Close releases the underlying Pebble batch.
func (b *WriteBatch) Close() error {
	if b == nil || b.batch == nil {
		return nil
	}
	return b.batch.Close()
}

// Empty reports whether the batch has queued any mutations.
func (b *WriteBatch) Empty() bool {
	return b == nil || b.opCount == 0
}

// SetRaw stages one raw key/value mutation.
func (b *WriteBatch) SetRaw(key, value []byte) error {
	if err := b.batch.Set(key, value, nil); err != nil {
		return err
	}
	b.opCount++
	return nil
}

// DeleteRaw stages one raw key deletion.
func (b *WriteBatch) DeleteRaw(key []byte) error {
	if err := b.batch.Delete(key, nil); err != nil {
		return err
	}
	b.opCount++
	return nil
}

// PutMVCCValue appends one committed MVCC value write to the batch.
func (b *WriteBatch) PutMVCCValue(logicalKey []byte, ts hlc.Timestamp, value []byte) error {
	encoded, err := EncodeMVCCVersionKey(logicalKey, ts)
	if err != nil {
		return err
	}
	payload, err := MVCCValue{Value: append([]byte(nil), value...)}.MarshalBinary()
	if err != nil {
		return err
	}
	return b.SetRaw(encoded, payload)
}

// PutMVCCTombstone appends one committed MVCC tombstone write to the batch.
func (b *WriteBatch) PutMVCCTombstone(logicalKey []byte, ts hlc.Timestamp) error {
	encoded, err := EncodeMVCCVersionKey(logicalKey, ts)
	if err != nil {
		return err
	}
	payload, err := MVCCValue{Tombstone: true}.MarshalBinary()
	if err != nil {
		return err
	}
	return b.SetRaw(encoded, payload)
}

// PutIntent appends an intent write to the batch.
func (b *WriteBatch) PutIntent(logicalKey []byte, intent Intent) error {
	key, err := EncodeMVCCMetadataKey(logicalKey)
	if err != nil {
		return err
	}
	value, err := intent.MarshalBinary()
	if err != nil {
		return err
	}
	return b.SetRaw(key, value)
}

// DeleteIntent removes the provisional intent at a logical key.
func (b *WriteBatch) DeleteIntent(logicalKey []byte) error {
	key, err := EncodeMVCCMetadataKey(logicalKey)
	if err != nil {
		return err
	}
	return b.DeleteRaw(key)
}

// SetTxnRecord stores the encoded transaction record under its global system key.
func (b *WriteBatch) SetTxnRecord(txnID TxnID, payload []byte) error {
	return b.SetRaw(GlobalTxnRecordKey(txnID), payload)
}

// SetRaftHardState appends a HardState update to the batch.
func (b *WriteBatch) SetRaftHardState(rangeID uint64, st raftpb.HardState) error {
	payload, err := st.Marshal()
	if err != nil {
		return fmt.Errorf("marshal hardstate: %w", err)
	}
	return b.SetRaw(RaftHardStateKey(rangeID), payload)
}

// AppendRaftEntries appends Raft log entries to the batch.
func (b *WriteBatch) AppendRaftEntries(rangeID uint64, entries []raftpb.Entry) error {
	for _, entry := range entries {
		payload, err := entry.Marshal()
		if err != nil {
			return fmt.Errorf("marshal raft entry %d: %w", entry.Index, err)
		}
		if err := b.SetRaw(RaftLogEntryKey(rangeID, entry.Index), payload); err != nil {
			return err
		}
	}
	return nil
}

// SetRangeAppliedIndex appends the latest applied index for a range.
func (b *WriteBatch) SetRangeAppliedIndex(rangeID, appliedIndex uint64) error {
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], appliedIndex)
	return b.SetRaw(RangeAppliedStateKey(rangeID), payload[:])
}

// SetRangeLease appends the latest range lease record.
func (b *WriteBatch) SetRangeLease(rangeID uint64, record lease.Record) error {
	payload, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return b.SetRaw(RangeLeaseKey(rangeID), payload)
}

// SetRangeClosedTimestamp appends the latest closed timestamp publication for a range.
func (b *WriteBatch) SetRangeClosedTimestamp(rangeID uint64, record closedts.Record) error {
	if record.RangeID != rangeID {
		return fmt.Errorf("set closed timestamp: record range %d does not match key range %d", record.RangeID, rangeID)
	}
	payload, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return b.SetRaw(RangeClosedTimestampKey(rangeID), payload)
}

// Commit writes the batch to disk, syncing if requested.
func (b *WriteBatch) Commit(sync bool) error {
	if b.Empty() {
		return nil
	}
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	return b.batch.Commit(opts)
}
