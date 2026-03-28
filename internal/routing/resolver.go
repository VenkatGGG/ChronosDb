package routing

import (
	"context"
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
func (r *Resolver) Resolve(ctx context.Context, key []byte) (ResolvedRange, error) {
	if desc, ok := r.cache.Lookup(key); ok {
		return buildResolvedRange(desc, ResolutionSourceCache)
	}
	return Refresh(ctx, r.cache, r.source, key)
}

// ResolveAfterRoutingError invalidates the stale cache entry and refreshes it.
func (r *Resolver) ResolveAfterRoutingError(ctx context.Context, key []byte, routingErr RoutingError) (ResolvedRange, error) {
	if err := r.cache.HandleRoutingError(routingErr); err != nil {
		return ResolvedRange{}, err
	}
	return Refresh(ctx, r.cache, r.source, key)
}
