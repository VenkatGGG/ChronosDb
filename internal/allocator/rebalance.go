package allocator

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
)

// NodeLoad is the allocator-visible load and locality state for one node.
type NodeLoad struct {
	NodeID    uint64
	Region    string
	Zone      string
	LoadScore float64
	Draining  bool
}

// RebalanceDecision replaces one existing replica with a replica on a new node.
type RebalanceDecision struct {
	SourceReplica   meta.ReplicaDescriptor
	SourceRegion    string
	TargetNode      NodeLoad
	PreferredRegion string
	TargetPreferred bool
	SourcePreferred bool
	Reason          string
}

// ChooseRebalance picks one placement-safe replica move that improves node-load distribution.
func ChooseRebalance(desc meta.RangeDescriptor, nodes []NodeLoad) (RebalanceDecision, error) {
	if err := desc.Validate(); err != nil {
		return RebalanceDecision{}, err
	}
	compiled, ok, err := desc.CompiledPlacement()
	if err != nil {
		return RebalanceDecision{}, err
	}
	if !ok {
		compiled = placement.CompiledPolicy{
			MinDistinctRegions: 1,
		}
	}

	loadByNode := make(map[uint64]NodeLoad, len(nodes))
	for _, node := range nodes {
		if node.NodeID == 0 {
			return RebalanceDecision{}, fmt.Errorf("allocator: node id must be non-zero")
		}
		node.Region = canonicalRegion(node.Region)
		loadByNode[node.NodeID] = node
	}

	var (
		bestDecision    RebalanceDecision
		bestImprovement float64
		found           bool
	)
	for _, replica := range desc.Replicas {
		sourceNode, ok := loadByNode[replica.NodeID]
		if !ok {
			continue
		}
		for _, target := range nodes {
			target.Region = canonicalRegion(target.Region)
			if target.NodeID == replica.NodeID || target.Draining || hasNode(desc.Replicas, target.NodeID) {
				continue
			}
			if !respectsPlacement(desc, compiled, loadByNode, replica, target) {
				continue
			}
			improvement := sourceNode.LoadScore - target.LoadScore
			if improvement <= 0 {
				continue
			}
			improvement += preferenceBonus(compiled, target.Region)
			improvement += localityTransitionBias(compiled, sourceNode.Region, target.Region)
			if replica.ReplicaID == desc.LeaseholderReplicaID {
				improvement -= 0.05
			}
			if !found || improvement > bestImprovement {
				found = true
				bestImprovement = improvement
				bestDecision = RebalanceDecision{
					SourceReplica:   replica,
					SourceRegion:    sourceNode.Region,
					TargetNode:      target,
					PreferredRegion: preferredRegion(compiled),
					TargetPreferred: regionPreferred(compiled, target.Region),
					SourcePreferred: regionPreferred(compiled, sourceNode.Region),
					Reason:          rebalanceReason(replica, sourceNode, target, compiled),
				}
			}
		}
	}
	if !found {
		return RebalanceDecision{}, fmt.Errorf("allocator: no placement-safe rebalance target")
	}
	return bestDecision, nil
}

func respectsPlacement(desc meta.RangeDescriptor, compiled placement.CompiledPolicy, loadByNode map[uint64]NodeLoad, source meta.ReplicaDescriptor, target NodeLoad) bool {
	regions := make([]string, 0, len(desc.Replicas))
	for _, replica := range desc.Replicas {
		node := loadByNode[replica.NodeID]
		region := node.Region
		if replica.ReplicaID == source.ReplicaID {
			region = target.Region
		}
		if replica.Role == meta.ReplicaRoleVoter {
			regions = append(regions, region)
		}
	}
	return distinctCount(regions) >= max(1, compiled.MinDistinctRegions)
}

func preferenceBonus(compiled placement.CompiledPolicy, region string) float64 {
	if len(compiled.PreferredRegions) == 0 {
		return 0
	}
	if regionPreferred(compiled, region) {
		return 0.03
	}
	if slices.Contains(compiled.PreferredRegions, region) {
		return 0.01
	}
	return 0
}

func hasNode(replicas []meta.ReplicaDescriptor, nodeID uint64) bool {
	for _, replica := range replicas {
		if replica.NodeID == nodeID {
			return true
		}
	}
	return false
}

func distinctCount(values []string) int {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		seen[value] = struct{}{}
	}
	return len(seen)
}

func canonicalRegion(region string) string {
	return strings.ToLower(strings.TrimSpace(region))
}

func max(a, b int) int {
	return int(math.Max(float64(a), float64(b)))
}

func localityTransitionBias(compiled placement.CompiledPolicy, sourceRegion, targetRegion string) float64 {
	sourcePreferred := regionPreferred(compiled, sourceRegion)
	targetPreferred := regionPreferred(compiled, targetRegion)
	switch {
	case !sourcePreferred && targetPreferred:
		return 0.02
	case sourcePreferred && !targetPreferred:
		return -0.04
	default:
		return 0
	}
}

func regionPreferred(compiled placement.CompiledPolicy, region string) bool {
	region = canonicalRegion(region)
	if region == "" {
		return false
	}
	if len(compiled.LeasePreferences) > 0 {
		return slices.Contains(compiled.LeasePreferences, region)
	}
	return slices.Contains(compiled.PreferredRegions, region)
}

func preferredRegion(compiled placement.CompiledPolicy) string {
	if len(compiled.LeasePreferences) > 0 {
		return compiled.LeasePreferences[0]
	}
	if len(compiled.PreferredRegions) > 0 {
		return compiled.PreferredRegions[0]
	}
	return ""
}

func rebalanceReason(source meta.ReplicaDescriptor, sourceNode, target NodeLoad, compiled placement.CompiledPolicy) string {
	reason := fmt.Sprintf(
		"rebalance from node %d (%s, %.2f) to node %d (%s, %.2f)",
		source.NodeID,
		sourceNode.Region,
		sourceNode.LoadScore,
		target.NodeID,
		target.Region,
		target.LoadScore,
	)
	if regionPreferred(compiled, target.Region) {
		reason += fmt.Sprintf(" while moving toward preferred region %q", preferredRegion(compiled))
	}
	return reason
}
