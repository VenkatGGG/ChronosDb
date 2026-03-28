package storage

import (
	"bytes"

	"github.com/cockroachdb/pebble"
)

// RawKV is one raw key/value pair returned from a bounded engine lookup.
type RawKV struct {
	Key   []byte
	Value []byte
}

// ScanRawRange returns all raw keys within [lowerBound, upperBound).
func (e *Engine) ScanRawRange(lowerBound, upperBound []byte) ([]RawKV, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var kvs []RawKV
	for iter.First(); iter.Valid(); iter.Next() {
		kvs = append(kvs, RawKV{
			Key:   bytes.Clone(iter.Key()),
			Value: bytes.Clone(iter.Value()),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return kvs, nil
}

// SeekRawGE seeks to the first raw key >= seekKey within [lowerBound, upperBound).
func (e *Engine) SeekRawGE(lowerBound, upperBound, seekKey []byte) (RawKV, bool, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return RawKV{}, false, err
	}
	defer iter.Close()

	if !iter.SeekGE(seekKey) {
		if err := iter.Error(); err != nil {
			return RawKV{}, false, err
		}
		return RawKV{}, false, nil
	}
	return RawKV{
		Key:   bytes.Clone(iter.Key()),
		Value: bytes.Clone(iter.Value()),
	}, true, iter.Error()
}
