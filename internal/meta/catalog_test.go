package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/cockroachdb/pebble/vfs"
)

func TestCatalogLookupMeta2(t *testing.T) {
	t.Parallel()

	engine, err := storage.Open(context.Background(), storage.Options{
		Dir: "meta-catalog-meta2",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(context.Background(), storage.StoreIdent{ClusterID: "cluster-a", NodeID: 1, StoreID: 1}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	catalog := NewCatalog(engine)
	left := RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("m"),
		Replicas: []ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: ReplicaRoleVoter},
		},
	}
	right := RangeDescriptor{
		RangeID:    2,
		Generation: 1,
		StartKey:   []byte("m"),
		EndKey:     nil,
		Replicas: []ReplicaDescriptor{
			{ReplicaID: 2, NodeID: 2, Role: ReplicaRoleVoter},
		},
	}
	if err := catalog.Upsert(context.Background(), LevelMeta2, left); err != nil {
		t.Fatalf("upsert left: %v", err)
	}
	if err := catalog.Upsert(context.Background(), LevelMeta2, right); err != nil {
		t.Fatalf("upsert right: %v", err)
	}

	got, err := catalog.LookupMeta2(context.Background(), []byte("b"))
	if err != nil {
		t.Fatalf("lookup left: %v", err)
	}
	if got.RangeID != left.RangeID {
		t.Fatalf("lookup left range = %d, want %d", got.RangeID, left.RangeID)
	}
	got, err = catalog.LookupMeta2(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("lookup right: %v", err)
	}
	if got.RangeID != right.RangeID {
		t.Fatalf("lookup right range = %d, want %d", got.RangeID, right.RangeID)
	}
}

func TestCatalogLookupMeta1(t *testing.T) {
	t.Parallel()

	engine, err := storage.Open(context.Background(), storage.Options{
		Dir: "meta-catalog-meta1",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(context.Background(), storage.StoreIdent{ClusterID: "cluster-a", NodeID: 1, StoreID: 1}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	catalog := NewCatalog(engine)
	metaRange := RangeDescriptor{
		RangeID:    10,
		Generation: 1,
		StartKey:   storage.Meta2DescriptorKey([]byte("a")),
		EndKey:     storage.Meta2DescriptorKey([]byte("z")),
		Replicas: []ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: ReplicaRoleVoter},
		},
	}
	if err := catalog.Upsert(context.Background(), LevelMeta1, metaRange); err != nil {
		t.Fatalf("upsert meta1 descriptor: %v", err)
	}

	got, err := catalog.LookupMeta1(context.Background(), storage.Meta2LookupKey([]byte("m")))
	if err != nil {
		t.Fatalf("lookup meta1: %v", err)
	}
	if got.RangeID != metaRange.RangeID {
		t.Fatalf("meta1 lookup range = %d, want %d", got.RangeID, metaRange.RangeID)
	}
}

func TestCatalogLookupNotFound(t *testing.T) {
	t.Parallel()

	engine, err := storage.Open(context.Background(), storage.Options{
		Dir: "meta-catalog-not-found",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(context.Background(), storage.StoreIdent{ClusterID: "cluster-a", NodeID: 1, StoreID: 1}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	catalog := NewCatalog(engine)
	_, err = catalog.LookupMeta2(context.Background(), []byte("missing"))
	if !errors.Is(err, ErrDescriptorNotFound) {
		t.Fatalf("lookup error = %v, want %v", err, ErrDescriptorNotFound)
	}
}

func TestCatalogBootstrapLayoutAndLookup(t *testing.T) {
	t.Parallel()

	engine, err := storage.Open(context.Background(), storage.Options{
		Dir: "meta-catalog-bootstrap-layout",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(context.Background(), storage.StoreIdent{ClusterID: "cluster-a", NodeID: 1, StoreID: 1}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	catalog := NewCatalog(engine)
	meta1Desc := RangeDescriptor{
		RangeID:    100,
		Generation: 1,
		StartKey:   storage.Meta2DescriptorKey([]byte("a")),
		EndKey:     storage.Meta2DescriptorKey([]byte("z")),
		Replicas: []ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: ReplicaRoleVoter},
		},
	}
	left := RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("m"),
		Replicas: []ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: ReplicaRoleVoter},
		},
	}
	right := RangeDescriptor{
		RangeID:    2,
		Generation: 1,
		StartKey:   []byte("m"),
		EndKey:     nil,
		Replicas: []ReplicaDescriptor{
			{ReplicaID: 2, NodeID: 2, Role: ReplicaRoleVoter},
		},
	}

	if err := catalog.BootstrapLayout(BootstrapLayout{
		Meta1: []RangeDescriptor{meta1Desc},
		Meta2: []RangeDescriptor{left, right},
	}); err != nil {
		t.Fatalf("bootstrap layout: %v", err)
	}

	gotUser, err := catalog.Lookup(context.Background(), []byte("b"))
	if err != nil {
		t.Fatalf("lookup user key: %v", err)
	}
	if gotUser.RangeID != left.RangeID {
		t.Fatalf("user lookup range = %d, want %d", gotUser.RangeID, left.RangeID)
	}

	gotMeta2, err := catalog.Lookup(context.Background(), storage.Meta2DescriptorKey([]byte("m")))
	if err != nil {
		t.Fatalf("lookup meta2 key: %v", err)
	}
	if gotMeta2.RangeID != meta1Desc.RangeID {
		t.Fatalf("meta2 lookup range = %d, want %d", gotMeta2.RangeID, meta1Desc.RangeID)
	}
}
