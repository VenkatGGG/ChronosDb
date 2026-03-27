package routing

import (
	"context"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

// DescriptorSource is the authoritative source used to refresh the range cache.
type DescriptorSource interface {
	LookupMeta2(context.Context, []byte) (meta.RangeDescriptor, error)
}

// Refresh resolves the descriptor from the authoritative source and installs it into the cache.
func Refresh(ctx context.Context, cache *RangeCache, source DescriptorSource, key []byte) (meta.RangeDescriptor, error) {
	desc, err := source.LookupMeta2(ctx, key)
	if err != nil {
		return meta.RangeDescriptor{}, err
	}
	if err := cache.Upsert(desc); err != nil {
		return meta.RangeDescriptor{}, err
	}
	return desc, nil
}
