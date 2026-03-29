package runtime

import (
	"context"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

func TestHostBootstrapsStoreAndReopensSeededDescriptors(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	seeds := []meta.RangeDescriptor{
		{
			RangeID:    11,
			Generation: 1,
			StartKey:   []byte("a"),
			EndKey:     []byte("m"),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
				{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 1,
		},
		{
			RangeID:    12,
			Generation: 1,
			StartKey:   []byte("m"),
			EndKey:     []byte("z"),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 3, NodeID: 2, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 3,
		},
	}

	host, err := Open(context.Background(), Config{
		NodeID:     1,
		StoreID:    9,
		ClusterID:  "cluster-runtime",
		DataDir:    dataDir,
		SeedRanges: seeds,
	})
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	metadata := host.Metadata()
	if !metadata.Bootstrapped {
		t.Fatal("host metadata bootstrapped = false, want true")
	}
	if metadata.Ident.ClusterID != "cluster-runtime" || metadata.Ident.NodeID != 1 || metadata.Ident.StoreID != 9 {
		t.Fatalf("store ident = %+v, want cluster-runtime/1/9", metadata.Ident)
	}
	descs, err := host.HostedDescriptors()
	if err != nil {
		t.Fatalf("hosted descriptors: %v", err)
	}
	if len(descs) != 1 || descs[0].RangeID != 11 {
		t.Fatalf("hosted descriptors = %+v, want only range 11", descs)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("close host: %v", err)
	}

	reopened, err := Open(context.Background(), Config{
		NodeID:    1,
		StoreID:   9,
		ClusterID: "cluster-runtime",
		DataDir:   dataDir,
	})
	if err != nil {
		t.Fatalf("reopen host: %v", err)
	}
	defer reopened.Close()
	descs, err = reopened.HostedDescriptors()
	if err != nil {
		t.Fatalf("reopened hosted descriptors: %v", err)
	}
	if len(descs) != 1 || descs[0].RangeID != 11 {
		t.Fatalf("reopened hosted descriptors = %+v, want persisted range 11", descs)
	}
}
