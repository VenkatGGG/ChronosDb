package routing

import (
	"context"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

type stubDescriptorSource struct {
	desc meta.RangeDescriptor
	err  error
}

func (s stubDescriptorSource) LookupMeta2(context.Context, []byte) (meta.RangeDescriptor, error) {
	return s.desc, s.err
}

func TestRefreshInstallsDescriptorIntoCache(t *testing.T) {
	t.Parallel()

	cache := NewRangeCache(Config{})
	desc := meta.RangeDescriptor{
		RangeID:    1,
		Generation: 7,
		StartKey:   []byte("a"),
		EndKey:     []byte("z"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
		},
	}
	got, err := Refresh(context.Background(), cache, stubDescriptorSource{desc: desc}, []byte("b"))
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if got.RangeID != desc.RangeID {
		t.Fatalf("descriptor range = %d, want %d", got.RangeID, desc.RangeID)
	}
	cached, ok := cache.Lookup([]byte("b"))
	if !ok {
		t.Fatal("expected descriptor to be cached")
	}
	if cached.RangeID != desc.RangeID {
		t.Fatalf("cached range = %d, want %d", cached.RangeID, desc.RangeID)
	}
}
