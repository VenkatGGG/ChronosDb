package routing

import (
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
)

// ResolutionSource identifies whether a routing result came from cache or authoritative refresh.
type ResolutionSource string

const (
	ResolutionSourceCache   ResolutionSource = "cache"
	ResolutionSourceRefresh ResolutionSource = "refresh"
)

// ResolvedRange is the descriptor plus routing-visible locality policy hints.
type ResolvedRange struct {
	Descriptor      meta.RangeDescriptor
	Source          ResolutionSource
	PlacementMode   placement.Mode
	HomeRegion      string
	PreferredRegion string
}

func buildResolvedRange(desc meta.RangeDescriptor, source ResolutionSource) (ResolvedRange, error) {
	if err := desc.Validate(); err != nil {
		return ResolvedRange{}, err
	}
	resolved := ResolvedRange{
		Descriptor: desc,
		Source:     source,
	}
	if desc.PlacementPolicy == nil {
		return resolved, nil
	}
	normalized, err := desc.PlacementPolicy.Normalize()
	if err != nil {
		return ResolvedRange{}, err
	}
	resolved.PlacementMode = normalized.PlacementMode
	resolved.HomeRegion = normalized.HomeRegion
	if len(normalized.LeasePreferences) > 0 {
		resolved.PreferredRegion = normalized.LeasePreferences[0]
	} else if len(normalized.PreferredRegions) > 0 {
		resolved.PreferredRegion = normalized.PreferredRegions[0]
	}
	return resolved, nil
}
