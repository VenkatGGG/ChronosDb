package storage

import (
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/cockroachdb/pebble"
)

// LoadRangeDescriptor loads the locally persisted descriptor for a range, if present.
func (e *Engine) LoadRangeDescriptor(rangeID uint64) (meta.RangeDescriptor, error) {
	payload, err := e.GetRaw(nil, RangeDescriptorKey(rangeID))
	if errors.Is(err, pebble.ErrNotFound) {
		return meta.RangeDescriptor{}, nil
	}
	if err != nil {
		return meta.RangeDescriptor{}, err
	}
	var desc meta.RangeDescriptor
	if err := desc.UnmarshalBinary(payload); err != nil {
		return meta.RangeDescriptor{}, fmt.Errorf("decode descriptor: %w", err)
	}
	return desc, nil
}

// SetRangeDescriptor appends a descriptor write to the batch.
func (b *WriteBatch) SetRangeDescriptor(rangeID uint64, desc meta.RangeDescriptor) error {
	payload, err := desc.MarshalBinary()
	if err != nil {
		return err
	}
	return b.SetRaw(RangeDescriptorKey(rangeID), payload)
}
