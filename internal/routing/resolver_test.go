package routing

import (
	"context"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

type sequenceDescriptorSource struct {
	descs []meta.RangeDescriptor
	calls int
}

func (s *sequenceDescriptorSource) LookupMeta2(context.Context, []byte) (meta.RangeDescriptor, error) {
	index := s.calls
	if index >= len(s.descs) {
		index = len(s.descs) - 1
	}
	s.calls++
	return s.descs[index], nil
}

func TestResolverUsesCacheOnSecondLookup(t *testing.T) {
	t.Parallel()

	source := &sequenceDescriptorSource{
		descs: []meta.RangeDescriptor{
			{
				RangeID:    1,
				Generation: 1,
				StartKey:   []byte("a"),
				EndKey:     []byte("z"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
				},
			},
		},
	}
	resolver := NewResolver(NewRangeCache(Config{}), source)

	desc, err := resolver.Resolve(context.Background(), []byte("b"))
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	if desc.RangeID != 1 {
		t.Fatalf("first resolve range = %d, want 1", desc.RangeID)
	}
	desc, err = resolver.Resolve(context.Background(), []byte("b"))
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if desc.RangeID != 1 {
		t.Fatalf("second resolve range = %d, want 1", desc.RangeID)
	}
	if source.calls != 1 {
		t.Fatalf("source calls = %d, want 1", source.calls)
	}
}

func TestResolverRefreshesAfterRoutingError(t *testing.T) {
	t.Parallel()

	source := &sequenceDescriptorSource{
		descs: []meta.RangeDescriptor{
			{
				RangeID:    1,
				Generation: 1,
				StartKey:   []byte("a"),
				EndKey:     []byte("m"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
				},
			},
			{
				RangeID:    2,
				Generation: 2,
				StartKey:   []byte("a"),
				EndKey:     []byte("m"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
				},
			},
		},
	}
	resolver := NewResolver(NewRangeCache(Config{}), source)

	first, err := resolver.Resolve(context.Background(), []byte("b"))
	if err != nil {
		t.Fatalf("initial resolve: %v", err)
	}
	if first.Generation != 1 {
		t.Fatalf("initial generation = %d, want 1", first.Generation)
	}
	second, err := resolver.ResolveAfterRoutingError(context.Background(), []byte("b"), RoutingError{
		Code:    ErrorCodeDescriptorStale,
		RangeID: first.RangeID,
	})
	if err != nil {
		t.Fatalf("resolve after routing error: %v", err)
	}
	if second.Generation != 2 || second.RangeID != 2 {
		t.Fatalf("refreshed descriptor = %+v, want generation 2 range 2", second)
	}
	if source.calls != 2 {
		t.Fatalf("source calls = %d, want 2", source.calls)
	}
}
