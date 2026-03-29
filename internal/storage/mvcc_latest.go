package storage

import (
	"bytes"
	"context"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/cockroachdb/pebble"
)

// GetLatestMVCCValue returns the newest committed version for one logical key.
func (e *Engine) GetLatestMVCCValue(_ context.Context, logicalKey []byte) ([]byte, hlc.Timestamp, bool, error) {
	lowerBound := append(bytes.Clone(logicalKey), mvccVersionMarker)
	upperBound, err := EncodeMVCCMetadataKey(logicalKey)
	if err != nil {
		return nil, hlc.Timestamp{}, false, err
	}
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, hlc.Timestamp{}, false, err
	}
	defer iter.Close()

	if !iter.SeekGE(lowerBound) {
		if err := iter.Error(); err != nil {
			return nil, hlc.Timestamp{}, false, err
		}
		return nil, hlc.Timestamp{}, false, nil
	}
	decoded, err := DecodeMVCCKey(iter.Key())
	if err != nil {
		return nil, hlc.Timestamp{}, false, err
	}
	return bytes.Clone(iter.Value()), decoded.Timestamp, true, iter.Error()
}
