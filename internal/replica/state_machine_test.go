package replica

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
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

func TestStageEntriesAppliesClosedTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-closedts-stage-test",
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
	record, err := lease.NewRecord(1, 1, hlc.Timestamp{WallTime: 100, Logical: 0}, hlc.Timestamp{WallTime: 200, Logical: 0}, 4)
	if err != nil {
		t.Fatalf("new lease: %v", err)
	}
	leasePayload, err := Command{
		Version: 1,
		Type:    CommandTypeSetLease,
		Lease:   &SetLease{Record: record},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal lease command: %v", err)
	}
	publication := closedts.Record{
		RangeID:       1,
		LeaseSequence: record.Sequence,
		ClosedTS:      hlc.Timestamp{WallTime: 150, Logical: 1},
		PublishedAt:   hlc.Timestamp{WallTime: 160, Logical: 0},
	}
	publicationPayload, err := Command{
		Version: 1,
		Type:    CommandTypeSetClosedTS,
		ClosedTS: &SetClosedTS{
			Record: publication,
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal closed timestamp command: %v", err)
	}

	batch := engine.NewWriteBatch()
	defer batch.Close()
	delta, err := stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: 4, Type: raftpb.EntryNormal, Data: leasePayload},
		{Index: 5, Type: raftpb.EntryNormal, Data: publicationPayload},
	})
	if err != nil {
		t.Fatalf("stage entries: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit batch: %v", err)
	}
	stateMachine.CommitApply(delta)

	got, ok := stateMachine.ClosedTimestamp()
	if !ok || got != publication {
		t.Fatalf("closed timestamp = %+v, ok=%v want %+v", got, ok, publication)
	}
	persisted, err := engine.LoadRangeClosedTimestamp(1)
	if err != nil {
		t.Fatalf("load closed timestamp: %v", err)
	}
	if persisted != publication {
		t.Fatalf("persisted closed timestamp = %+v, want %+v", persisted, publication)
	}
}

func TestStageEntriesRejectsClosedTimestampLeaseMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-closedts-mismatch-test",
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
	record, err := lease.NewRecord(1, 1, hlc.Timestamp{WallTime: 100, Logical: 0}, hlc.Timestamp{WallTime: 200, Logical: 0}, 4)
	if err != nil {
		t.Fatalf("new lease: %v", err)
	}
	if err := applyLeaseCommand(stateMachine, engine, 4, record); err != nil {
		t.Fatalf("apply lease command: %v", err)
	}

	payload, err := Command{
		Version: 1,
		Type:    CommandTypeSetClosedTS,
		ClosedTS: &SetClosedTS{
			Record: closedts.Record{
				RangeID:       1,
				LeaseSequence: record.Sequence - 1,
				ClosedTS:      hlc.Timestamp{WallTime: 150, Logical: 1},
				PublishedAt:   hlc.Timestamp{WallTime: 160, Logical: 0},
			},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal closed timestamp command: %v", err)
	}
	batch := engine.NewWriteBatch()
	defer batch.Close()
	_, err = stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: 5, Type: raftpb.EntryNormal, Data: payload},
	})
	if !errors.Is(err, ErrInvalidClosedTimestamp) {
		t.Fatalf("closed timestamp mismatch error = %v, want %v", err, ErrInvalidClosedTimestamp)
	}
}

func TestHistoricalGetUsesClosedTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-historical-get-test",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(ctx, storage.StoreIdent{ClusterID: "cluster-a", NodeID: 1, StoreID: 1}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	stateMachine, err := OpenStateMachine(1, 2, engine)
	if err != nil {
		t.Fatalf("open state machine: %v", err)
	}
	record, err := lease.NewRecord(1, 1, hlc.Timestamp{WallTime: 100, Logical: 0}, hlc.Timestamp{WallTime: 200, Logical: 0}, 4)
	if err != nil {
		t.Fatalf("new lease: %v", err)
	}
	if err := applyLeaseCommand(stateMachine, engine, 3, record); err != nil {
		t.Fatalf("apply lease command: %v", err)
	}

	logicalKey := storage.GlobalTablePrimaryKey(7, []byte("alice"))
	putPayload, err := Command{
		Version: 1,
		Type:    CommandTypePutValue,
		Put: &PutValue{
			LogicalKey: logicalKey,
			Timestamp:  hlc.Timestamp{WallTime: 150, Logical: 1},
			Value:      []byte("value"),
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal put command: %v", err)
	}
	publicationPayload, err := Command{
		Version: 1,
		Type:    CommandTypeSetClosedTS,
		ClosedTS: &SetClosedTS{
			Record: closedts.Record{
				RangeID:       1,
				LeaseSequence: record.Sequence,
				ClosedTS:      hlc.Timestamp{WallTime: 150, Logical: 1},
				PublishedAt:   hlc.Timestamp{WallTime: 160, Logical: 0},
			},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal closed timestamp command: %v", err)
	}
	batch := engine.NewWriteBatch()
	defer batch.Close()
	delta, err := stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: 4, Type: raftpb.EntryNormal, Data: putPayload},
		{Index: 5, Type: raftpb.EntryNormal, Data: publicationPayload},
	})
	if err != nil {
		t.Fatalf("stage entries: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit batch: %v", err)
	}
	stateMachine.CommitApply(delta)

	got, err := stateMachine.HistoricalGet(ctx, logicalKey, hlc.Timestamp{WallTime: 150, Logical: 1})
	if err != nil {
		t.Fatalf("historical get: %v", err)
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("historical value = %q, want %q", got, []byte("value"))
	}

	_, err = stateMachine.HistoricalGet(ctx, logicalKey, hlc.Timestamp{WallTime: 151, Logical: 0})
	if !errors.Is(err, closedts.ErrFollowerReadTooFresh) {
		t.Fatalf("historical read too fresh error = %v, want %v", err, closedts.ErrFollowerReadTooFresh)
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

func TestStageEntriesApplySplitTrigger(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-split-test",
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

	parent := meta.RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     nil,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 3, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	if err := installDescriptor(t, stateMachine, engine, 5, parent); err != nil {
		t.Fatalf("install parent descriptor: %v", err)
	}
	catalog := meta.NewCatalog(engine)
	if err := catalog.Upsert(ctx, meta.LevelMeta2, parent); err != nil {
		t.Fatalf("seed parent routing: %v", err)
	}

	left := meta.RangeDescriptor{
		RangeID:              1,
		Generation:           2,
		StartKey:             []byte("a"),
		EndKey:               []byte("m"),
		Replicas:             append([]meta.ReplicaDescriptor(nil), parent.Replicas...),
		LeaseholderReplicaID: 1,
	}
	right := meta.RangeDescriptor{
		RangeID:              2,
		Generation:           1,
		StartKey:             []byte("m"),
		EndKey:               nil,
		Replicas:             append([]meta.ReplicaDescriptor(nil), parent.Replicas...),
		LeaseholderReplicaID: 1,
	}
	payload, err := Command{
		Version: 1,
		Type:    CommandTypeSplitRange,
		Split: &SplitRange{
			ExpectedGeneration: parent.Generation,
			MetaLevel:          meta.LevelMeta2,
			Left:               left,
			Right:              right,
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal split command: %v", err)
	}

	batch := engine.NewWriteBatch()
	defer batch.Close()
	delta, err := stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: 6, Type: raftpb.EntryNormal, Data: payload},
	})
	if err != nil {
		t.Fatalf("stage split command: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit split batch: %v", err)
	}
	stateMachine.CommitApply(delta)

	if got := stateMachine.Descriptor(); got.RangeID != left.RangeID || !bytes.Equal(got.EndKey, left.EndKey) || got.Generation != left.Generation {
		t.Fatalf("left descriptor = %+v, want %+v", got, left)
	}
	if stateMachine.AppliedIndex() != 6 {
		t.Fatalf("left applied index = %d, want 6", stateMachine.AppliedIndex())
	}

	rightPayload, err := engine.GetRaw(ctx, storage.RangeDescriptorKey(right.RangeID))
	if err != nil {
		t.Fatalf("load right descriptor: %v", err)
	}
	var persistedRight meta.RangeDescriptor
	if err := persistedRight.UnmarshalBinary(rightPayload); err != nil {
		t.Fatalf("decode right descriptor: %v", err)
	}
	if persistedRight.RangeID != right.RangeID || !bytes.Equal(persistedRight.StartKey, right.StartKey) || !bytes.Equal(persistedRight.EndKey, right.EndKey) {
		t.Fatalf("persisted right descriptor = %+v, want %+v", persistedRight, right)
	}

	rightApplied, err := engine.LoadRangeAppliedIndex(right.RangeID)
	if err != nil {
		t.Fatalf("load right applied index: %v", err)
	}
	if rightApplied != 6 {
		t.Fatalf("right applied index = %d, want 6", rightApplied)
	}

	gotLeft, err := catalog.LookupMeta2(ctx, []byte("b"))
	if err != nil {
		t.Fatalf("lookup left child: %v", err)
	}
	if gotLeft.RangeID != left.RangeID {
		t.Fatalf("left routing range = %d, want %d", gotLeft.RangeID, left.RangeID)
	}
	gotRight, err := catalog.LookupMeta2(ctx, []byte("z"))
	if err != nil {
		t.Fatalf("lookup right child: %v", err)
	}
	if gotRight.RangeID != right.RangeID {
		t.Fatalf("right routing range = %d, want %d", gotRight.RangeID, right.RangeID)
	}
}

func TestStageEntriesRejectInvalidSplitTrigger(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-invalid-split-test",
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
	parent := meta.RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("z"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	if err := installDescriptor(t, stateMachine, engine, 2, parent); err != nil {
		t.Fatalf("install parent descriptor: %v", err)
	}

	payload, err := Command{
		Version: 1,
		Type:    CommandTypeSplitRange,
		Split: &SplitRange{
			ExpectedGeneration: parent.Generation,
			MetaLevel:          meta.LevelMeta2,
			Left: meta.RangeDescriptor{
				RangeID:              1,
				Generation:           2,
				StartKey:             []byte("a"),
				EndKey:               []byte("m"),
				Replicas:             append([]meta.ReplicaDescriptor(nil), parent.Replicas...),
				LeaseholderReplicaID: 1,
			},
			Right: meta.RangeDescriptor{
				RangeID:              2,
				Generation:           1,
				StartKey:             []byte("n"),
				EndKey:               []byte("z"),
				Replicas:             append([]meta.ReplicaDescriptor(nil), parent.Replicas...),
				LeaseholderReplicaID: 1,
			},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal split command: %v", err)
	}

	batch := engine.NewWriteBatch()
	defer batch.Close()
	_, err = stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: 3, Type: raftpb.EntryNormal, Data: payload},
	})
	if !errors.Is(err, ErrInvalidSplitTrigger) {
		t.Fatalf("invalid split error = %v, want %v", err, ErrInvalidSplitTrigger)
	}
}

func TestStageEntriesApplyReplicaRebalanceFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-rebalance-test",
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
	parent := meta.RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("z"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 3, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	if err := installDescriptor(t, stateMachine, engine, 3, parent); err != nil {
		t.Fatalf("install parent descriptor: %v", err)
	}

	catalog := meta.NewCatalog(engine)
	if err := catalog.Upsert(ctx, meta.LevelMeta2, parent); err != nil {
		t.Fatalf("seed meta descriptor: %v", err)
	}

	learner := meta.ReplicaDescriptor{ReplicaID: 4, NodeID: 4, Role: meta.ReplicaRoleLearner}
	if err := applyReplicaChangeCommand(stateMachine, engine, 4, ChangeReplicas{
		ExpectedGeneration: 1,
		MetaLevel:          meta.LevelMeta2,
		Change:             ReplicaChange{Kind: ReplicaChangeAddLearner, Replica: learner},
	}); err != nil {
		t.Fatalf("add learner: %v", err)
	}
	if got := stateMachine.Descriptor().Generation; got != 2 {
		t.Fatalf("generation after add learner = %d, want 2", got)
	}
	if role := stateMachine.Descriptor().Replicas[3].Role; role != meta.ReplicaRoleLearner {
		t.Fatalf("learner role = %q, want %q", role, meta.ReplicaRoleLearner)
	}

	stalePromotePayload, err := Command{
		Version: 1,
		Type:    CommandTypeChangeReplicas,
		Replica: &ChangeReplicas{
			ExpectedGeneration: 1,
			MetaLevel:          meta.LevelMeta2,
			Change: ReplicaChange{
				Kind:    ReplicaChangePromote,
				Replica: meta.ReplicaDescriptor{ReplicaID: 4, NodeID: 4, Role: meta.ReplicaRoleVoter},
			},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal stale promote: %v", err)
	}
	batch := engine.NewWriteBatch()
	defer batch.Close()
	_, err = stateMachine.StageEntries(batch, []raftpb.Entry{{Index: 5, Type: raftpb.EntryNormal, Data: stalePromotePayload}})
	if !errors.Is(err, ErrDescriptorGenerationMismatch) {
		t.Fatalf("stale promote error = %v, want %v", err, ErrDescriptorGenerationMismatch)
	}

	if err := applyReplicaChangeCommand(stateMachine, engine, 6, ChangeReplicas{
		ExpectedGeneration: 2,
		MetaLevel:          meta.LevelMeta2,
		Change: ReplicaChange{
			Kind:    ReplicaChangePromote,
			Replica: meta.ReplicaDescriptor{ReplicaID: 4, NodeID: 4, Role: meta.ReplicaRoleVoter},
		},
	}); err != nil {
		t.Fatalf("promote learner: %v", err)
	}
	if got := stateMachine.Descriptor().Generation; got != 3 {
		t.Fatalf("generation after promote = %d, want 3", got)
	}
	if role := stateMachine.Descriptor().Replicas[3].Role; role != meta.ReplicaRoleVoter {
		t.Fatalf("promoted role = %q, want %q", role, meta.ReplicaRoleVoter)
	}

	if err := applyReplicaChangeCommand(stateMachine, engine, 7, ChangeReplicas{
		ExpectedGeneration: 3,
		MetaLevel:          meta.LevelMeta2,
		Change: ReplicaChange{
			Kind:    ReplicaChangeRemove,
			Replica: meta.ReplicaDescriptor{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
	}); err != nil {
		t.Fatalf("remove voter: %v", err)
	}
	if got := stateMachine.Descriptor().Generation; got != 4 {
		t.Fatalf("generation after remove = %d, want 4", got)
	}
	if len(stateMachine.Descriptor().Replicas) != 3 {
		t.Fatalf("replica count after remove = %d, want 3", len(stateMachine.Descriptor().Replicas))
	}
	for _, replica := range stateMachine.Descriptor().Replicas {
		if replica.ReplicaID == 2 {
			t.Fatalf("removed replica 2 still present")
		}
	}

	routed, err := catalog.LookupMeta2(ctx, []byte("m"))
	if err != nil {
		t.Fatalf("routing lookup after rebalance: %v", err)
	}
	if routed.Generation != 4 {
		t.Fatalf("routed descriptor generation = %d, want 4", routed.Generation)
	}
	if len(routed.Replicas) != 3 {
		t.Fatalf("routed replica count = %d, want 3", len(routed.Replicas))
	}
}

func TestStageEntriesRejectLeaseholderRemoval(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-remove-leaseholder-test",
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
	parent := meta.RangeDescriptor{
		RangeID:    1,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("z"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	if err := installDescriptor(t, stateMachine, engine, 2, parent); err != nil {
		t.Fatalf("install parent descriptor: %v", err)
	}

	payload, err := Command{
		Version: 1,
		Type:    CommandTypeChangeReplicas,
		Replica: &ChangeReplicas{
			ExpectedGeneration: 1,
			MetaLevel:          meta.LevelMeta2,
			Change: ReplicaChange{
				Kind:    ReplicaChangeRemove,
				Replica: meta.ReplicaDescriptor{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("marshal removal: %v", err)
	}
	batch := engine.NewWriteBatch()
	defer batch.Close()
	_, err = stateMachine.StageEntries(batch, []raftpb.Entry{{Index: 3, Type: raftpb.EntryNormal, Data: payload}})
	if !errors.Is(err, ErrInvalidReplicaChange) {
		t.Fatalf("leaseholder removal error = %v, want %v", err, ErrInvalidReplicaChange)
	}
}

func TestApplySnapshotInstallsReplicaImage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-snapshot-install-test",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(ctx, storage.StoreIdent{ClusterID: "cluster-a", NodeID: 2, StoreID: 2}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	stateMachine, err := OpenStateMachine(2, 4, engine)
	if err != nil {
		t.Fatalf("open state machine: %v", err)
	}
	record, err := lease.NewRecord(4, 2, hlc.Timestamp{WallTime: 100, Logical: 0}, hlc.Timestamp{WallTime: 200, Logical: 0}, 1)
	if err != nil {
		t.Fatalf("new lease: %v", err)
	}
	image := SnapshotImage{
		AppliedIndex: 11,
		Descriptor: meta.RangeDescriptor{
			RangeID:    2,
			Generation: 3,
			StartKey:   []byte("m"),
			EndKey:     []byte("z"),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 3, NodeID: 3, Role: meta.ReplicaRoleVoter},
				{ReplicaID: 4, NodeID: 4, Role: meta.ReplicaRoleLearner},
			},
			LeaseholderReplicaID: 3,
		},
		Lease:    record,
		HasLease: true,
		ClosedTimestamp: closedts.Record{
			RangeID:       2,
			LeaseSequence: record.Sequence,
			ClosedTS:      hlc.Timestamp{WallTime: 140, Logical: 0},
			PublishedAt:   hlc.Timestamp{WallTime: 150, Logical: 1},
		},
		HasClosedTS:      true,
		SafeReadFrontier: hlc.Timestamp{WallTime: 150, Logical: 1},
	}

	if err := stateMachine.ApplySnapshot(image); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}
	if stateMachine.AppliedIndex() != 11 {
		t.Fatalf("applied index = %d, want 11", stateMachine.AppliedIndex())
	}
	if stateMachine.Lease() != record {
		t.Fatalf("lease = %+v, want %+v", stateMachine.Lease(), record)
	}
	if got, ok := stateMachine.ClosedTimestamp(); !ok || got != image.ClosedTimestamp {
		t.Fatalf("closed timestamp = %+v, ok=%v, want %+v", got, ok, image.ClosedTimestamp)
	}
	reopened, err := OpenStateMachine(2, 4, engine)
	if err != nil {
		t.Fatalf("reopen state machine: %v", err)
	}
	if reopened.AppliedIndex() != 11 {
		t.Fatalf("reopened applied index = %d, want 11", reopened.AppliedIndex())
	}
	if reopened.Descriptor().Generation != 3 {
		t.Fatalf("reopened generation = %d, want 3", reopened.Descriptor().Generation)
	}
	if reopened.Lease() != record {
		t.Fatalf("reopened lease = %+v, want %+v", reopened.Lease(), record)
	}
	if got, ok := reopened.ClosedTimestamp(); !ok || got != image.ClosedTimestamp {
		t.Fatalf("reopened closed timestamp = %+v, ok=%v, want %+v", got, ok, image.ClosedTimestamp)
	}
}

func TestApplySnapshotRejectsStaleGeneration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.Options{
		Dir: "replica-snapshot-stale-test",
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
	parent := meta.RangeDescriptor{
		RangeID:    1,
		Generation: 4,
		StartKey:   []byte("a"),
		EndKey:     []byte("z"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	if err := installDescriptor(t, stateMachine, engine, 5, parent); err != nil {
		t.Fatalf("install parent descriptor: %v", err)
	}

	err = stateMachine.ApplySnapshot(SnapshotImage{
		AppliedIndex: 6,
		Descriptor: meta.RangeDescriptor{
			RangeID:    1,
			Generation: 3,
			StartKey:   []byte("a"),
			EndKey:     []byte("z"),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 1,
		},
	})
	if !errors.Is(err, ErrStaleSnapshot) {
		t.Fatalf("stale snapshot error = %v, want %v", err, ErrStaleSnapshot)
	}
}

func installDescriptor(t *testing.T, stateMachine *StateMachine, engine *storage.Engine, index uint64, desc meta.RangeDescriptor) error {
	t.Helper()

	payload, err := Command{
		Version: 1,
		Type:    CommandTypeUpdateDescriptor,
		Descriptor: &UpdateDescriptor{
			ExpectedGeneration: stateMachine.Descriptor().Generation,
			Descriptor:         desc,
		},
	}.Marshal()
	if err != nil {
		return err
	}
	batch := engine.NewWriteBatch()
	defer batch.Close()
	delta, err := stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: index, Type: raftpb.EntryNormal, Data: payload},
	})
	if err != nil {
		return err
	}
	if err := batch.Commit(true); err != nil {
		return err
	}
	stateMachine.CommitApply(delta)
	return nil
}

func applyReplicaChangeCommand(stateMachine *StateMachine, engine *storage.Engine, index uint64, change ChangeReplicas) error {
	payload, err := Command{
		Version: 1,
		Type:    CommandTypeChangeReplicas,
		Replica: &change,
	}.Marshal()
	if err != nil {
		return err
	}
	batch := engine.NewWriteBatch()
	defer batch.Close()
	delta, err := stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: index, Type: raftpb.EntryNormal, Data: payload},
	})
	if err != nil {
		return err
	}
	if err := batch.Commit(true); err != nil {
		return err
	}
	stateMachine.CommitApply(delta)
	return nil
}

func applyLeaseCommand(stateMachine *StateMachine, engine *storage.Engine, index uint64, record lease.Record) error {
	payload, err := Command{
		Version: 1,
		Type:    CommandTypeSetLease,
		Lease:   &SetLease{Record: record},
	}.Marshal()
	if err != nil {
		return err
	}
	batch := engine.NewWriteBatch()
	defer batch.Close()
	delta, err := stateMachine.StageEntries(batch, []raftpb.Entry{
		{Index: index, Type: raftpb.EntryNormal, Data: payload},
	})
	if err != nil {
		return err
	}
	if err := batch.Commit(true); err != nil {
		return err
	}
	stateMachine.CommitApply(delta)
	return nil
}
