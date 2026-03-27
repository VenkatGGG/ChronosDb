package meta

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

func TestRangeDescriptorContainsKey(t *testing.T) {
	t.Parallel()

	desc := RangeDescriptor{
		RangeID:    1,
		Generation: 7,
		StartKey:   []byte("m"),
		EndKey:     []byte("z"),
		Replicas: []ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	if !desc.ContainsKey([]byte("m")) {
		t.Fatal("expected start key to be contained")
	}
	if !desc.ContainsKey([]byte("x")) {
		t.Fatal("expected interior key to be contained")
	}
	if desc.ContainsKey([]byte("z")) {
		t.Fatal("expected end key to be excluded")
	}
}

func TestDescriptorAndLivenessBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	desc := RangeDescriptor{
		RangeID:    1,
		Generation: 7,
		StartKey:   []byte("m"),
		EndKey:     []byte("z"),
		Replicas: []ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: ReplicaRoleLearner},
		},
		LeaseholderReplicaID: 1,
	}
	payload, err := desc.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	var decodedDesc RangeDescriptor
	if err := decodedDesc.UnmarshalBinary(payload); err != nil {
		t.Fatalf("unmarshal descriptor: %v", err)
	}
	if decodedDesc.RangeID != desc.RangeID || decodedDesc.Generation != desc.Generation {
		t.Fatalf("decoded descriptor = %+v, want %+v", decodedDesc, desc)
	}

	liveness := NodeLiveness{
		NodeID:    1,
		Epoch:     2,
		Draining:  true,
		UpdatedAt: hlc.Timestamp{WallTime: 100, Logical: 1},
	}
	payload, err = liveness.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal liveness: %v", err)
	}
	var decodedLiveness NodeLiveness
	if err := decodedLiveness.UnmarshalBinary(payload); err != nil {
		t.Fatalf("unmarshal liveness: %v", err)
	}
	if decodedLiveness != liveness {
		t.Fatalf("decoded liveness = %+v, want %+v", decodedLiveness, liveness)
	}
}
