package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestBuildBootstrapManifestAndSeedHost(t *testing.T) {
	t.Parallel()

	manifest, err := BuildBootstrapManifest("cluster-bootstrap", []BootstrapNode{
		{NodeID: 1, StoreID: 1},
		{NodeID: 2, StoreID: 2},
		{NodeID: 3, StoreID: 3},
	}, []meta.RangeDescriptor{
		{
			RangeID:    10,
			Generation: 1,
			StartKey:   []byte("a"),
			EndKey:     []byte("m"),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 11, NodeID: 1, Role: meta.ReplicaRoleVoter},
				{ReplicaID: 12, NodeID: 2, Role: meta.ReplicaRoleVoter},
				{ReplicaID: 13, NodeID: 3, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 11,
		},
		{
			RangeID:    11,
			Generation: 1,
			StartKey:   []byte("m"),
			EndKey:     []byte("z"),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 21, NodeID: 1, Role: meta.ReplicaRoleVoter},
				{ReplicaID: 22, NodeID: 2, Role: meta.ReplicaRoleVoter},
				{ReplicaID: 23, NodeID: 3, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 22,
		},
	})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if len(manifest.Meta1) != 1 || len(manifest.Meta2) != 2 {
		t.Fatalf("manifest layout = %+v, want 1 meta1 and 2 meta2", manifest)
	}

	dataDir := t.TempDir()
	bootstrapPath := filepath.Join(dataDir, "bootstrap", "cluster.json")
	host, err := Open(context.Background(), Config{
		NodeID:            1,
		StoreID:           1,
		ClusterID:         manifest.ClusterID,
		DataDir:           dataDir,
		BootstrapPath:     bootstrapPath,
		BootstrapManifest: &manifest,
	})
	if err != nil {
		t.Fatalf("open host with bootstrap manifest: %v", err)
	}
	defer host.Close()

	if _, err := LoadBootstrapManifest(bootstrapPath); err != nil {
		t.Fatalf("load persisted bootstrap manifest: %v", err)
	}
	desc, err := host.catalog.LookupMeta2(context.Background(), []byte("b"))
	if err != nil {
		t.Fatalf("lookup meta2 user range: %v", err)
	}
	if desc.RangeID != 10 {
		t.Fatalf("meta2 user range = %d, want 10", desc.RangeID)
	}
	metaDesc, err := host.catalog.Lookup(context.Background(), storage.Meta2DescriptorKey([]byte("n")))
	if err != nil {
		t.Fatalf("lookup meta1 descriptor: %v", err)
	}
	if metaDesc.RangeID != manifest.Meta1[0].RangeID {
		t.Fatalf("meta1 range = %d, want %d", metaDesc.RangeID, manifest.Meta1[0].RangeID)
	}
	descs, err := host.HostedDescriptors()
	if err != nil {
		t.Fatalf("hosted descriptors: %v", err)
	}
	if len(descs) != 3 {
		t.Fatalf("hosted descriptors = %d, want 3 (meta1 + 2 local ranges)", len(descs))
	}
	leaseRecord, err := host.engine.LoadRangeLease(10)
	if err != nil {
		t.Fatalf("load bootstrap lease: %v", err)
	}
	if leaseRecord.HolderReplicaID != 11 || leaseRecord.Sequence != 1 {
		t.Fatalf("bootstrap lease = %+v, want holder 11 sequence 1", leaseRecord)
	}
}

func TestLoadBootstrapManifestMissingFile(t *testing.T) {
	t.Parallel()

	_, err := LoadBootstrapManifest(filepath.Join(t.TempDir(), "missing.json"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing bootstrap manifest error = %v, want os.ErrNotExist", err)
	}
}
