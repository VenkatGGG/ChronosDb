package storage

import (
	"bytes"
	"context"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

// MVCCVersion is one latest committed logical row/version returned from a span scan.
type MVCCVersion struct {
	LogicalKey []byte
	Timestamp  hlc.Timestamp
	Value      []byte
}

// MVCCScanRequest names one logical MVCC scan span and its bound semantics.
type MVCCScanRequest struct {
	StartKey       []byte `json:"start_key"`
	EndKey         []byte `json:"end_key"`
	StartInclusive bool   `json:"start_inclusive"`
	EndInclusive   bool   `json:"end_inclusive"`
}

// ScanLatestMVCCRange returns the newest committed version for each logical key in the span.
func (e *Engine) ScanLatestMVCCRange(_ context.Context, req MVCCScanRequest) ([]MVCCVersion, error) {
	lowerBound := append([]byte(nil), req.StartKey...)
	if !req.StartInclusive {
		lowerBound = PrefixEnd(req.StartKey)
	}
	var upperBound []byte
	if len(req.EndKey) > 0 {
		upperBound = append([]byte(nil), req.EndKey...)
		if req.EndInclusive {
			upperBound = PrefixEnd(req.EndKey)
		}
	}

	kvs, err := e.ScanRawRange(lowerBound, upperBound)
	if err != nil {
		return nil, err
	}
	versions := make([]MVCCVersion, 0, len(kvs))
	var lastLogicalKey []byte
	for _, kv := range kvs {
		decoded, err := DecodeMVCCKey(kv.Key)
		if err != nil {
			return nil, err
		}
		if decoded.Kind != MVCCKeyKindValue {
			continue
		}
		if !containsLogicalKey(decoded.LogicalKey, req.StartKey, req.EndKey, req.StartInclusive, req.EndInclusive) {
			continue
		}
		if bytes.Equal(decoded.LogicalKey, lastLogicalKey) {
			continue
		}
		lastLogicalKey = append(lastLogicalKey[:0], decoded.LogicalKey...)
		value, err := UnmarshalMVCCValue(kv.Value)
		if err != nil {
			return nil, err
		}
		if value.Tombstone {
			continue
		}
		versions = append(versions, MVCCVersion{
			LogicalKey: decoded.LogicalKey,
			Timestamp:  decoded.Timestamp,
			Value:      append([]byte(nil), value.Value...),
		})
	}
	return versions, nil
}

func containsLogicalKey(key, startKey, endKey []byte, startInclusive, endInclusive bool) bool {
	if startInclusive {
		if bytes.Compare(key, startKey) < 0 {
			return false
		}
	} else {
		if bytes.Compare(key, startKey) <= 0 {
			return false
		}
	}
	if len(endKey) == 0 {
		return true
	}
	if endInclusive {
		return bytes.Compare(key, endKey) <= 0
	}
	return bytes.Compare(key, endKey) < 0
}
