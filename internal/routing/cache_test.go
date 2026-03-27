package routing

import (
	"math/rand"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

func TestRangeCacheLookupInvalidateAndTTL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 15, 0, 0, 0, time.UTC)
	cache := NewRangeCache(Config{
		TTLSeconds:     5 * time.Minute,
		JitterFraction: 0,
		Now:            func() time.Time { return now },
		Rand:           rand.New(rand.NewSource(1)),
	})
	desc := meta.RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("m"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	if err := cache.Upsert(desc); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if got, ok := cache.Lookup([]byte("c")); !ok || got.RangeID != 1 {
		t.Fatalf("lookup returned %+v, %v; want range 1", got, ok)
	}
	if err := cache.HandleRoutingError(RoutingError{Code: ErrorCodeRangeSplit, RangeID: 1}); err != nil {
		t.Fatalf("handle routing error: %v", err)
	}
	if _, ok := cache.Lookup([]byte("c")); ok {
		t.Fatal("expected range to be invalidated")
	}

	if err := cache.Upsert(desc); err != nil {
		t.Fatalf("upsert second time: %v", err)
	}
	now = now.Add(6 * time.Minute)
	if _, ok := cache.Lookup([]byte("c")); ok {
		t.Fatal("expected range to expire by ttl")
	}
}

func TestRangeCacheReplacesDescriptorsAfterSplit(t *testing.T) {
	t.Parallel()

	cache := NewRangeCache(Config{TTLSeconds: 5 * time.Minute, JitterFraction: 0})
	left := meta.RangeDescriptor{
		RangeID:    10,
		Generation: 2,
		StartKey:   []byte("a"),
		EndKey:     []byte("m"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
		},
	}
	right := meta.RangeDescriptor{
		RangeID:    11,
		Generation: 1,
		StartKey:   []byte("m"),
		EndKey:     []byte("z"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
	}
	if err := cache.Upsert(left); err != nil {
		t.Fatalf("upsert left: %v", err)
	}
	if err := cache.Upsert(right); err != nil {
		t.Fatalf("upsert right: %v", err)
	}
	if got, ok := cache.Lookup([]byte("b")); !ok || got.RangeID != 10 {
		t.Fatalf("lookup b returned %+v, %v; want range 10", got, ok)
	}
	if got, ok := cache.Lookup([]byte("x")); !ok || got.RangeID != 11 {
		t.Fatalf("lookup x returned %+v, %v; want range 11", got, ok)
	}
}
