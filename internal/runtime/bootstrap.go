package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

const bootstrapManifestVersion = 1

// BootstrapNode identifies one store participating in the initial cluster image.
type BootstrapNode struct {
	NodeID  uint64 `json:"node_id"`
	StoreID uint64 `json:"store_id"`
}

// BootstrapManifest is the persisted on-disk cluster bootstrap contract.
type BootstrapManifest struct {
	Version   uint8                  `json:"version"`
	ClusterID string                 `json:"cluster_id"`
	Nodes     []BootstrapNode        `json:"nodes"`
	Meta1     []meta.RangeDescriptor `json:"meta1"`
	Meta2     []meta.RangeDescriptor `json:"meta2"`
}

// Validate checks the manifest for obvious bootstrap corruption.
func (m BootstrapManifest) Validate() error {
	if m.Version == 0 {
		m.Version = bootstrapManifestVersion
	}
	if m.Version != bootstrapManifestVersion {
		return fmt.Errorf("bootstrap manifest: unsupported version %d", m.Version)
	}
	if m.ClusterID == "" {
		return fmt.Errorf("bootstrap manifest: cluster id must not be empty")
	}
	if len(m.Nodes) == 0 {
		return fmt.Errorf("bootstrap manifest: nodes must not be empty")
	}
	if len(m.Meta1) == 0 {
		return fmt.Errorf("bootstrap manifest: meta1 layout must not be empty")
	}
	if len(m.Meta2) == 0 {
		return fmt.Errorf("bootstrap manifest: meta2 layout must not be empty")
	}
	seenNodes := make(map[uint64]struct{}, len(m.Nodes))
	for _, node := range m.Nodes {
		if node.NodeID == 0 || node.StoreID == 0 {
			return fmt.Errorf("bootstrap manifest: node/store ids must be non-zero")
		}
		if _, ok := seenNodes[node.NodeID]; ok {
			return fmt.Errorf("bootstrap manifest: duplicate node id %d", node.NodeID)
		}
		seenNodes[node.NodeID] = struct{}{}
	}
	for _, desc := range append(append([]meta.RangeDescriptor(nil), m.Meta1...), m.Meta2...) {
		if err := desc.Validate(); err != nil {
			return fmt.Errorf("bootstrap manifest: invalid range %d: %w", desc.RangeID, err)
		}
	}
	return nil
}

// Layout returns the catalog bootstrap layout encoded in the manifest.
func (m BootstrapManifest) Layout() meta.BootstrapLayout {
	return meta.BootstrapLayout{
		Meta1: append([]meta.RangeDescriptor(nil), m.Meta1...),
		Meta2: append([]meta.RangeDescriptor(nil), m.Meta2...),
	}
}

// AllRanges returns the full set of range descriptors referenced by the manifest.
func (m BootstrapManifest) AllRanges() []meta.RangeDescriptor {
	out := make([]meta.RangeDescriptor, 0, len(m.Meta1)+len(m.Meta2))
	out = append(out, m.Meta1...)
	out = append(out, m.Meta2...)
	return out
}

// InitialLease returns the bootstrap lease for a descriptor if one is defined.
func (m BootstrapManifest) InitialLease(desc meta.RangeDescriptor) (lease.Record, bool, error) {
	if desc.LeaseholderReplicaID == 0 {
		return lease.Record{}, false, nil
	}
	for _, replica := range desc.Replicas {
		if replica.ReplicaID != desc.LeaseholderReplicaID {
			continue
		}
		record, err := lease.NewRecord(
			desc.LeaseholderReplicaID,
			1,
			hlc.Timestamp{WallTime: 1},
			hlc.Timestamp{WallTime: 1 << 60},
			1,
		)
		if err != nil {
			return lease.Record{}, false, err
		}
		return record, true, nil
	}
	return lease.Record{}, false, fmt.Errorf("bootstrap manifest: leaseholder replica %d not found in range %d", desc.LeaseholderReplicaID, desc.RangeID)
}

// BuildBootstrapManifest constructs a deterministic bootstrap manifest from cluster nodes and initial user ranges.
func BuildBootstrapManifest(clusterID string, nodes []BootstrapNode, userRanges []meta.RangeDescriptor) (BootstrapManifest, error) {
	if clusterID == "" {
		return BootstrapManifest{}, fmt.Errorf("build bootstrap manifest: cluster id must not be empty")
	}
	if len(nodes) == 0 {
		return BootstrapManifest{}, fmt.Errorf("build bootstrap manifest: nodes must not be empty")
	}
	if len(userRanges) == 0 {
		return BootstrapManifest{}, fmt.Errorf("build bootstrap manifest: user ranges must not be empty")
	}

	orderedNodes := append([]BootstrapNode(nil), nodes...)
	slices.SortFunc(orderedNodes, func(left, right BootstrapNode) int {
		switch {
		case left.NodeID < right.NodeID:
			return -1
		case left.NodeID > right.NodeID:
			return 1
		default:
			return 0
		}
	})

	orderedRanges := append([]meta.RangeDescriptor(nil), userRanges...)
	slices.SortFunc(orderedRanges, func(left, right meta.RangeDescriptor) int {
		return bytes.Compare(left.StartKey, right.StartKey)
	})

	metaReplicas := make([]meta.ReplicaDescriptor, 0, len(orderedNodes))
	maxRangeID := uint64(0)
	for _, node := range orderedNodes {
		metaReplicas = append(metaReplicas, meta.ReplicaDescriptor{
			ReplicaID: node.StoreID,
			NodeID:    node.NodeID,
			Role:      meta.ReplicaRoleVoter,
		})
	}
	for _, desc := range orderedRanges {
		if err := desc.Validate(); err != nil {
			return BootstrapManifest{}, err
		}
		if desc.RangeID > maxRangeID {
			maxRangeID = desc.RangeID
		}
	}

	systemRangeID := maxRangeID + 1
	meta1RangeID := maxRangeID + 2
	systemRange := meta.RangeDescriptor{
		RangeID:              systemRangeID,
		Generation:           1,
		StartKey:             storage.GlobalSystemPrefix(),
		EndKey:               storage.GlobalTablePrefix(),
		Replicas:             metaReplicas,
		LeaseholderReplicaID: metaReplicas[0].ReplicaID,
	}
	meta2Ranges := append([]meta.RangeDescriptor{systemRange}, orderedRanges...)
	slices.SortFunc(meta2Ranges, func(left, right meta.RangeDescriptor) int {
		return bytes.Compare(left.StartKey, right.StartKey)
	})

	manifest := BootstrapManifest{
		Version:   bootstrapManifestVersion,
		ClusterID: clusterID,
		Nodes:     orderedNodes,
		Meta1: []meta.RangeDescriptor{
			{
				RangeID:              meta1RangeID,
				Generation:           1,
				StartKey:             storage.Meta2Prefix(),
				EndKey:               storage.PrefixEnd(storage.Meta2Prefix()),
				Replicas:             metaReplicas,
				LeaseholderReplicaID: metaReplicas[0].ReplicaID,
			},
		},
		Meta2: meta2Ranges,
	}
	return manifest, manifest.Validate()
}

// LoadBootstrapManifest reads and validates a manifest from disk.
func LoadBootstrapManifest(path string) (BootstrapManifest, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return BootstrapManifest{}, err
	}
	var manifest BootstrapManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return BootstrapManifest{}, fmt.Errorf("load bootstrap manifest: %w", err)
	}
	return manifest, manifest.Validate()
}

// WriteBootstrapManifest writes a validated manifest to disk.
func WriteBootstrapManifest(path string, manifest BootstrapManifest) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(payload, '\n'), 0o644)
}
