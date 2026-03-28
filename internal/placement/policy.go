package placement

import (
	"fmt"
	"slices"
	"strings"
)

// Mode is the user-visible placement class.
type Mode string

const (
	ModeRegional   Mode = "REGIONAL"
	ModeHomeRegion Mode = "HOME_REGION"
	ModeGlobal     Mode = "GLOBAL"
)

// SurvivalGoal is the failure domain the placement must survive.
type SurvivalGoal string

const (
	SurvivalZoneFailure   SurvivalGoal = "ZONE_FAILURE"
	SurvivalRegionFailure SurvivalGoal = "REGION_FAILURE"
)

// ReplicaConstraint is one allocator-facing placement constraint.
type ReplicaConstraint struct {
	Region string
	Zone   string
}

// Policy is the descriptor-level placement configuration.
type Policy struct {
	PlacementMode      Mode
	HomeRegion         string
	SurvivalGoal       SurvivalGoal
	PreferredRegions   []string
	LeasePreferences   []string
	ReplicaConstraints []ReplicaConstraint
}

// CompiledPolicy is the allocator- and routing-facing form of a placement policy.
type CompiledPolicy struct {
	PlacementMode      Mode
	SurvivalGoal       SurvivalGoal
	ReplicaCount       int
	MinDistinctRegions int
	PreferredRegions   []string
	LeasePreferences   []string
}

// Normalize applies placement defaults while preserving the declared user intent.
func (p Policy) Normalize() (Policy, error) {
	p.HomeRegion = canonicalRegion(p.HomeRegion)
	p.PreferredRegions = canonicalRegions(p.PreferredRegions)
	p.LeasePreferences = canonicalRegions(p.LeasePreferences)
	for i, constraint := range p.ReplicaConstraints {
		p.ReplicaConstraints[i] = ReplicaConstraint{
			Region: canonicalRegion(constraint.Region),
			Zone:   strings.TrimSpace(constraint.Zone),
		}
	}
	switch p.PlacementMode {
	case ModeRegional:
		if p.SurvivalGoal == "" {
			p.SurvivalGoal = SurvivalZoneFailure
		}
		if len(p.PreferredRegions) == 0 {
			return Policy{}, fmt.Errorf("placement: REGIONAL policy requires exactly one preferred region")
		}
		if len(p.PreferredRegions) != 1 {
			return Policy{}, fmt.Errorf("placement: REGIONAL policy may only name one preferred region")
		}
		if p.HomeRegion != "" {
			return Policy{}, fmt.Errorf("placement: REGIONAL policy must not set home_region")
		}
		if len(p.LeasePreferences) == 0 {
			p.LeasePreferences = append([]string(nil), p.PreferredRegions...)
		}
	case ModeHomeRegion:
		if p.HomeRegion == "" {
			return Policy{}, fmt.Errorf("placement: HOME_REGION policy requires home_region")
		}
		if p.SurvivalGoal == "" {
			p.SurvivalGoal = SurvivalRegionFailure
		}
		if len(p.PreferredRegions) == 0 {
			p.PreferredRegions = []string{p.HomeRegion}
		}
		if !slices.Contains(p.PreferredRegions, p.HomeRegion) {
			p.PreferredRegions = append([]string{p.HomeRegion}, p.PreferredRegions...)
		}
		if len(p.LeasePreferences) == 0 {
			p.LeasePreferences = []string{p.HomeRegion}
		}
	case ModeGlobal:
		if p.SurvivalGoal == "" {
			p.SurvivalGoal = SurvivalRegionFailure
		}
		if len(p.PreferredRegions) == 0 {
			return Policy{}, fmt.Errorf("placement: GLOBAL policy requires preferred regions")
		}
	default:
		return Policy{}, fmt.Errorf("placement: unknown mode %q", p.PlacementMode)
	}
	if err := validateSurvival(p); err != nil {
		return Policy{}, err
	}
	return p, nil
}

// Compile validates the policy and derives allocator- and routing-facing requirements.
func Compile(policy Policy) (CompiledPolicy, error) {
	normalized, err := policy.Normalize()
	if err != nil {
		return CompiledPolicy{}, err
	}
	compiled := CompiledPolicy{
		PlacementMode:    normalized.PlacementMode,
		SurvivalGoal:     normalized.SurvivalGoal,
		PreferredRegions: append([]string(nil), normalized.PreferredRegions...),
		LeasePreferences: append([]string(nil), normalized.LeasePreferences...),
	}
	switch normalized.SurvivalGoal {
	case SurvivalZoneFailure:
		compiled.ReplicaCount = 3
		compiled.MinDistinctRegions = 1
	case SurvivalRegionFailure:
		compiled.ReplicaCount = 5
		compiled.MinDistinctRegions = 3
	default:
		return CompiledPolicy{}, fmt.Errorf("placement: unknown survival goal %q", normalized.SurvivalGoal)
	}
	return compiled, nil
}

func validateSurvival(policy Policy) error {
	switch policy.SurvivalGoal {
	case SurvivalZoneFailure:
		if policy.PlacementMode != ModeRegional {
			return fmt.Errorf("placement: ZONE_FAILURE is only valid for REGIONAL placement")
		}
		if len(policy.PreferredRegions) != 1 {
			return fmt.Errorf("placement: ZONE_FAILURE requires a single region")
		}
	case SurvivalRegionFailure:
		if policy.PlacementMode == ModeRegional {
			return fmt.Errorf("placement: REGIONAL placement may not request REGION_FAILURE survival")
		}
		if len(policy.PreferredRegions) < 3 {
			return fmt.Errorf("placement: REGION_FAILURE requires at least three preferred regions")
		}
	default:
		return fmt.Errorf("placement: unknown survival goal %q", policy.SurvivalGoal)
	}
	return nil
}

func canonicalRegions(regions []string) []string {
	if len(regions) == 0 {
		return nil
	}
	out := make([]string, 0, len(regions))
	seen := make(map[string]struct{}, len(regions))
	for _, region := range regions {
		region = canonicalRegion(region)
		if region == "" {
			continue
		}
		if _, ok := seen[region]; ok {
			continue
		}
		seen[region] = struct{}{}
		out = append(out, region)
	}
	return out
}

func canonicalRegion(region string) string {
	return strings.ToLower(strings.TrimSpace(region))
}
