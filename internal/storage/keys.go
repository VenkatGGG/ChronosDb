package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

var (
	prefixRaft       = []byte("/raft/")
	prefixRaftRange  = []byte("/raft/range/")
	prefixMVCC       = []byte("/mvcc/")
	prefixMVCCGlobal = []byte("/mvcc/global/")
	prefixMVCCLocal  = []byte("/mvcc/local/")
	prefixMeta1      = []byte("/mvcc/global/meta1/")
	prefixMeta2      = []byte("/mvcc/global/meta2/")
	prefixSystem     = []byte("/mvcc/global/system/")
	prefixSystemTxn  = []byte("/mvcc/global/system/txn/")
	prefixTable      = []byte("/mvcc/global/table/")

	storeIdentKey   = []byte("/mvcc/local/store/ident")
	storeVersionKey = []byte("/mvcc/local/store/version")
)

const (
	mvccVersionMarker  byte = 0x00
	mvccMetadataMarker byte = 0x01
)

// MVCCKeyKind identifies whether an encoded MVCC key refers to a version or the
// metadata sentinel for a logical key.
type MVCCKeyKind uint8

const (
	MVCCKeyKindValue MVCCKeyKind = iota + 1
	MVCCKeyKindMetadata
)

// DecodedMVCCKey is the parsed form of an encoded MVCC key.
type DecodedMVCCKey struct {
	Kind       MVCCKeyKind
	LogicalKey []byte
	Timestamp  hlc.Timestamp
}

// StoreIdentKey returns the local key used to persist the store identity.
func StoreIdentKey() []byte {
	return bytes.Clone(storeIdentKey)
}

// StoreVersionKey returns the local key used to persist the store version.
func StoreVersionKey() []byte {
	return bytes.Clone(storeVersionKey)
}

// RaftHardStateKey returns the key for a range's HardState record.
func RaftHardStateKey(rangeID uint64) []byte {
	dst := append(bytes.Clone(prefixRaftRange), encodeUint64(rangeID)...)
	return append(dst, []byte("/hardstate")...)
}

// RaftEntryPrefix returns the prefix for a range's log entries.
func RaftEntryPrefix(rangeID uint64) []byte {
	dst := append(bytes.Clone(prefixRaftRange), encodeUint64(rangeID)...)
	return append(dst, []byte("/entry/")...)
}

// RaftLogEntryKey returns the key for a Raft log entry.
func RaftLogEntryKey(rangeID, logIndex uint64) []byte {
	dst := RaftEntryPrefix(rangeID)
	return append(dst, encodeUint64(logIndex)...)
}

// RangeAppliedStateKey returns the local range applied-state key.
func RangeAppliedStateKey(rangeID uint64) []byte {
	dst := rangeLocalPrefix(rangeID)
	return append(dst, []byte("/applied_state")...)
}

// RangeLeaseKey returns the local range lease key.
func RangeLeaseKey(rangeID uint64) []byte {
	dst := rangeLocalPrefix(rangeID)
	return append(dst, []byte("/lease")...)
}

// RangeClosedTimestampKey returns the persisted closed timestamp publication key for a range.
func RangeClosedTimestampKey(rangeID uint64) []byte {
	dst := append(bytes.Clone(prefixMVCCGlobal), []byte("system/closedts/")...)
	return append(dst, encodeUint64(rangeID)...)
}

// RangeDescriptorKey returns the local persisted descriptor key for a range.
func RangeDescriptorKey(rangeID uint64) []byte {
	dst := rangeLocalPrefix(rangeID)
	return append(dst, []byte("/descriptor")...)
}

// Meta1Prefix returns the authoritative meta1 namespace prefix.
func Meta1Prefix() []byte {
	return bytes.Clone(prefixMeta1)
}

// Meta2Prefix returns the authoritative meta2 namespace prefix.
func Meta2Prefix() []byte {
	return bytes.Clone(prefixMeta2)
}

// GlobalSystemPrefix returns the logical key prefix for replicated system records.
func GlobalSystemPrefix() []byte {
	return bytes.Clone(prefixSystem)
}

// GlobalSystemTxnPrefix returns the logical key prefix for transaction records.
func GlobalSystemTxnPrefix() []byte {
	return bytes.Clone(prefixSystemTxn)
}

// GlobalTablePrefix returns the logical key prefix shared by all user table/index data.
func GlobalTablePrefix() []byte {
	return bytes.Clone(prefixTable)
}

// Meta1DescriptorKey returns the meta1 key for the descriptor whose end key is endKey.
func Meta1DescriptorKey(endKey []byte) []byte {
	return metaKey(prefixMeta1, endKey)
}

// Meta2DescriptorKey returns the meta2 key for the descriptor whose end key is endKey.
func Meta2DescriptorKey(endKey []byte) []byte {
	return metaKey(prefixMeta2, endKey)
}

// Meta1LookupKey returns the seek key used to resolve a meta2 span through meta1.
func Meta1LookupKey(key []byte) []byte {
	return metaKey(prefixMeta1, key)
}

// Meta2LookupKey returns the seek key used to resolve a user/system span through meta2.
func Meta2LookupKey(key []byte) []byte {
	return metaKey(prefixMeta2, key)
}

// GlobalTablePrimaryPrefix returns the logical key prefix for a table's primary rows.
func GlobalTablePrimaryPrefix(tableID uint64) []byte {
	dst := append(bytes.Clone(prefixMVCCGlobal), []byte("table/")...)
	dst = append(dst, encodeUint64(tableID)...)
	return append(dst, []byte("/primary/")...)
}

// GlobalTablePrimaryKey returns the logical MVCC key for a table primary row.
func GlobalTablePrimaryKey(tableID uint64, encodedPrimaryKey []byte) []byte {
	return appendEscapedBytes(GlobalTablePrimaryPrefix(tableID), encodedPrimaryKey)
}

// GlobalTxnRecordKey returns the logical key for one durable transaction record.
func GlobalTxnRecordKey(txnID TxnID) []byte {
	return append(bytes.Clone(prefixSystemTxn), txnID[:]...)
}

// GlobalTableIndexKey returns the logical MVCC key for a table secondary index row.
func GlobalTableIndexKey(tableID, indexID uint64, encodedIndexKey []byte) []byte {
	dst := append(bytes.Clone(prefixMVCCGlobal), []byte("table/")...)
	dst = append(dst, encodeUint64(tableID)...)
	dst = append(dst, []byte("/index/")...)
	dst = append(dst, encodeUint64(indexID)...)
	dst = append(dst, '/')
	return appendEscapedBytes(dst, encodedIndexKey)
}

// EncodeMVCCVersionKey returns the on-disk key for a committed MVCC version.
func EncodeMVCCVersionKey(logicalKey []byte, ts hlc.Timestamp) ([]byte, error) {
	if len(logicalKey) == 0 {
		return nil, fmt.Errorf("encode mvcc version key: logical key required")
	}
	if ts.IsZero() {
		return nil, fmt.Errorf("encode mvcc version key: timestamp must be non-zero")
	}

	dst := make([]byte, 0, len(logicalKey)+1+hlc.EncodedSize)
	dst = append(dst, logicalKey...)
	dst = append(dst, mvccVersionMarker)
	dst = ts.AppendDescending(dst)
	return dst, nil
}

// EncodeMVCCMetadataKey returns the sentinel key stored adjacent to versions for a logical key.
func EncodeMVCCMetadataKey(logicalKey []byte) ([]byte, error) {
	if len(logicalKey) == 0 {
		return nil, fmt.Errorf("encode mvcc metadata key: logical key required")
	}
	dst := make([]byte, 0, len(logicalKey)+1)
	dst = append(dst, logicalKey...)
	dst = append(dst, mvccMetadataMarker)
	return dst, nil
}

// DecodeMVCCKey parses a Pebble MVCC key back into its logical parts.
func DecodeMVCCKey(encoded []byte) (DecodedMVCCKey, error) {
	if len(encoded) == 0 {
		return DecodedMVCCKey{}, fmt.Errorf("decode mvcc key: empty key")
	}
	if len(encoded) >= 1+hlc.EncodedSize && encoded[len(encoded)-1-hlc.EncodedSize] == mvccVersionMarker {
		logicalKey := bytes.Clone(encoded[:len(encoded)-1-hlc.EncodedSize])
		ts, rest, err := hlc.DecodeDescending(encoded[len(encoded)-hlc.EncodedSize:])
		if err != nil {
			return DecodedMVCCKey{}, err
		}
		if len(rest) != 0 {
			return DecodedMVCCKey{}, fmt.Errorf("decode mvcc key: trailing version payload")
		}
		return DecodedMVCCKey{
			Kind:       MVCCKeyKindValue,
			LogicalKey: logicalKey,
			Timestamp:  ts,
		}, nil
	}
	if encoded[len(encoded)-1] == mvccMetadataMarker {
		return DecodedMVCCKey{
			Kind:       MVCCKeyKindMetadata,
			LogicalKey: bytes.Clone(encoded[:len(encoded)-1]),
		}, nil
	}
	return DecodedMVCCKey{}, fmt.Errorf("decode mvcc key: unrecognized suffix")
}

// PrefixEnd returns an exclusive upper bound for all keys sharing prefix.
func PrefixEnd(prefix []byte) []byte {
	return append(bytes.Clone(prefix), 0xff)
}

func encodeUint64(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func rangeLocalPrefix(rangeID uint64) []byte {
	dst := append(bytes.Clone(prefixMVCCLocal), []byte("range/")...)
	return append(dst, encodeUint64(rangeID)...)
}

func metaKey(prefix, endKey []byte) []byte {
	dst := bytes.Clone(prefix)
	if len(endKey) == 0 {
		return append(dst, 0xff)
	}
	return appendEscapedBytes(dst, endKey)
}

func appendEscapedBytes(dst, raw []byte) []byte {
	for _, b := range raw {
		if b == 0x00 {
			dst = append(dst, 0x00, 0xff)
			continue
		}
		dst = append(dst, b)
	}
	return append(dst, 0x00, 0x01)
}
