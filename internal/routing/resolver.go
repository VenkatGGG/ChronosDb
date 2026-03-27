package routing

import (
	"context"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

// Resolver couples the range cache with the authoritative descriptor source.
type Resolver struct {
	cache  *RangeCache
	source DescriptorSource
}

// NewResolver constructs a routing resolver.
func NewResolver(cache *RangeCache, source DescriptorSource) *Resolver {
	return &Resolver{
		cache:  cache,
		source: source,
	}
}

// Resolve returns the descriptor for key, preferring the cache and refreshing on miss.
func (r *Resolver) Resolve(ctx context.Context, key []byte) (meta.RangeDescriptor, error) {
	if desc, ok := r.cache.Lookup(key); ok {
		return desc, nil
	}
	return Refresh(ctx, r.cache, r.source, key)
}

// ResolveAfterRoutingError invalidates the stale cache entry and refreshes it.
func (r *Resolver) ResolveAfterRoutingError(ctx context.Context, key []byte, routingErr RoutingError) (meta.RangeDescriptor, error) {
	if err := r.cache.HandleRoutingError(routingErr); err != nil {
		return meta.RangeDescriptor{}, err
	}
	return Refresh(ctx, r.cache, r.source, key)
}
