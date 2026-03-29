package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

var (
	rangeLocalScanPrefix     = []byte("/mvcc/local/range/")
	rangeDescriptorKeySuffix = []byte("/descriptor")
)

// ScanRangeDescriptorIDs returns the locally persisted range ids sorted ascending.
func (e *Engine) ScanRangeDescriptorIDs() ([]uint64, error) {
	kvs, err := e.ScanRawRange(rangeLocalScanPrefix, PrefixEnd(rangeLocalScanPrefix))
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(kvs))
	for _, kv := range kvs {
		if !bytes.HasSuffix(kv.Key, rangeDescriptorKeySuffix) {
			continue
		}
		encodedID := kv.Key[len(rangeLocalScanPrefix) : len(kv.Key)-len(rangeDescriptorKeySuffix)]
		if len(encodedID) != 8 {
			return nil, fmt.Errorf("scan range descriptors: malformed descriptor key %q", kv.Key)
		}
		ids = append(ids, binary.BigEndian.Uint64(encodedID))
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, nil
}
