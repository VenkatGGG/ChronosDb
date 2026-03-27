package replica

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/cockroachdb/pebble/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestStageEntriesAppliesWritesAndLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-stage-test",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(ctx, storage.StoreIdent{ClusterID: "cluster-a", NodeID: 1, StoreID: 1}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	stateMachine, err := OpenStateMachine(1, 1, engine)
	if err != nil {
		t.Fatalf("open state machine: %v", err)
	}
	record, err := lease.NewRecord(1, 1, hlc.Timestamp{WallTime: 100, Logical: 0}, hlc.Timestamp{WallTime: 200, Logical: 0}, 1)
	if err != nil {
		t.Fatalf("new lease: %v", err)
	}

	putPayload, err := Command{
		Version: 1,
		Type:    CommandTypePutValue,
		Put: &PutValue{
			LogicalKey: storage.GlobalTablePrimaryKey(7, []byte("alice")),
			Timestamp:  hlc.Timestamp{WallTime: 150, Logical: 1},
			Value:      []byte("value"),
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal put command: %v", err)
	}
	leasePayload, err := Command{
		Version: 1,
		Type:    CommandTypeSetLease,
		Lease:   &SetLease{Record: record},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal lease command: %v", err)
	}

	batch := engine.NewWriteBatch()
	defer batch.Close()
	delta, err := stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: 3, Type: raftpb.EntryNormal, Data: putPayload},
		{Index: 4, Type: raftpb.EntryNormal, Data: leasePayload},
	})
	if err != nil {
		t.Fatalf("stage entries: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit batch: %v", err)
	}
	stateMachine.CommitApply(delta)

	if stateMachine.AppliedIndex() != 4 {
		t.Fatalf("applied index = %d, want 4", stateMachine.AppliedIndex())
	}
	if stateMachine.Lease() != record {
		t.Fatalf("lease = %+v, want %+v", stateMachine.Lease(), record)
	}
	got, err := engine.GetMVCCValue(ctx, storage.GlobalTablePrimaryKey(7, []byte("alice")), hlc.Timestamp{WallTime: 150, Logical: 1})
	if err != nil {
		t.Fatalf("get mvcc value: %v", err)
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("value = %q, want %q", got, []byte("value"))
	}
}

func TestStageEntriesRejectsStaleDescriptorGeneration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-descriptor-test",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(ctx, storage.StoreIdent{ClusterID: "cluster-a", NodeID: 1, StoreID: 1}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	stateMachine, err := OpenStateMachine(1, 1, engine)
	if err != nil {
		t.Fatalf("open state machine: %v", err)
	}

	initialDesc := meta.RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("m"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	initialPayload, err := Command{
		Version: 1,
		Type:    CommandTypeUpdateDescriptor,
		Descriptor: &UpdateDescriptor{
			ExpectedGeneration: stateMachine.Descriptor().Generation,
			Descriptor:         initialDesc,
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal initial descriptor command: %v", err)
	}
	batch := engine.NewWriteBatch()
	defer batch.Close()
	delta, err := stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: 5, Type: raftpb.EntryNormal, Data: initialPayload},
	})
	if err != nil {
		t.Fatalf("stage initial descriptor: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit descriptor batch: %v", err)
	}
	stateMachine.CommitApply(delta)

	staleDesc := initialDesc
	staleDesc.Generation = 2
	stalePayload, err := Command{
		Version: 1,
		Type:    CommandTypeUpdateDescriptor,
		Descriptor: &UpdateDescriptor{
			ExpectedGeneration: 0,
			Descriptor:         staleDesc,
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal stale descriptor command: %v", err)
	}
	batch2 := engine.NewWriteBatch()
	defer batch2.Close()
	_, err = stateMachine.StageEntries(batch2, []raftpb.Entry{
		{Index: 6, Type: raftpb.EntryNormal, Data: stalePayload},
	})
	if !errors.Is(err, ErrDescriptorGenerationMismatch) {
		t.Fatalf("stale descriptor error = %v, want %v", err, ErrDescriptorGenerationMismatch)
	}
}
