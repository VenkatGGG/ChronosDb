package sim

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/replica"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestRangeClusterReplicatesDescriptorLeaseWriteAndFollowerRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster, err := NewRangeCluster(5, []Member{
		{NodeID: 1, ReplicaID: 1},
		{NodeID: 2, ReplicaID: 2},
	})
	if err != nil {
		t.Fatalf("new range cluster: %v", err)
	}
	defer cluster.Close()

	rangeStart := storage.GlobalTablePrimaryPrefix(7)
	rangeEnd := storage.GlobalTablePrimaryPrefix(8)
	desc := meta.RangeDescriptor{
		RangeID:    5,
		Generation: 1,
		StartKey:   rangeStart,
		EndKey:     rangeEnd,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeUpdateDescriptor,
		Descriptor: &replica.UpdateDescriptor{
			ExpectedGeneration: 0,
			Descriptor:         desc,
		},
	})

	leaseRecord, err := lease.NewRecord(
		1,
		1,
		hlc.Timestamp{WallTime: 100, Logical: 0},
		hlc.Timestamp{WallTime: 300, Logical: 0},
		1,
	)
	if err != nil {
		t.Fatalf("new lease record: %v", err)
	}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetLease,
		Lease:   &replica.SetLease{Record: leaseRecord},
	})

	logicalKey := storage.GlobalTablePrimaryKey(7, []byte("alice"))
	valueTS := hlc.Timestamp{WallTime: 150, Logical: 1}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypePutValue,
		Put: &replica.PutValue{
			LogicalKey: logicalKey,
			Timestamp:  valueTS,
			Value:      []byte("value"),
		},
	})

	for _, replicaID := range []uint64{1, 2} {
		node, ok := cluster.Replica(replicaID)
		if !ok {
			t.Fatalf("replica %d missing", replicaID)
		}
		if !reflect.DeepEqual(node.State.Descriptor(), desc) {
			t.Fatalf("replica %d descriptor = %+v, want %+v", replicaID, node.State.Descriptor(), desc)
		}
		got, err := node.State.ReadExact(ctx, logicalKey, valueTS)
		if err != nil {
			t.Fatalf("replica %d read exact: %v", replicaID, err)
		}
		if !bytes.Equal(got, []byte("value")) {
			t.Fatalf("replica %d value = %q, want %q", replicaID, got, []byte("value"))
		}
	}

	publication := closedts.Record{
		RangeID:       5,
		LeaseSequence: leaseRecord.Sequence,
		ClosedTS:      valueTS,
		PublishedAt:   hlc.Timestamp{WallTime: 160, Logical: 0},
	}
	closedEntry := nextEntry(t, cluster, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetClosedTS,
		ClosedTS: &replica.SetClosedTS{
			Record: publication,
		},
	})
	if err := cluster.ApplyEntry([]uint64{1}, closedEntry); err != nil {
		t.Fatalf("apply closed timestamp to leader: %v", err)
	}

	follower, ok := cluster.Replica(2)
	if !ok {
		t.Fatalf("replica 2 missing")
	}
	_, err = follower.State.HistoricalGet(ctx, logicalKey, valueTS)
	if !errors.Is(err, closedts.ErrFollowerReadTooFresh) {
		t.Fatalf("historical read before publication = %v, want %v", err, closedts.ErrFollowerReadTooFresh)
	}

	if err := cluster.ApplyEntry([]uint64{2}, closedEntry); err != nil {
		t.Fatalf("apply closed timestamp to follower: %v", err)
	}
	got, err := follower.State.HistoricalGet(ctx, logicalKey, valueTS)
	if err != nil {
		t.Fatalf("historical get after publication: %v", err)
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("historical value = %q, want %q", got, []byte("value"))
	}
}

func TestRangeClusterLeaseTransferRequiresFreshClosedTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster, err := NewRangeCluster(9, []Member{
		{NodeID: 1, ReplicaID: 1},
		{NodeID: 2, ReplicaID: 2},
	})
	if err != nil {
		t.Fatalf("new range cluster: %v", err)
	}
	defer cluster.Close()

	rangeStart := storage.GlobalTablePrimaryPrefix(11)
	rangeEnd := storage.GlobalTablePrimaryPrefix(12)
	initialDesc := meta.RangeDescriptor{
		RangeID:    9,
		Generation: 1,
		StartKey:   rangeStart,
		EndKey:     rangeEnd,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeUpdateDescriptor,
		Descriptor: &replica.UpdateDescriptor{
			ExpectedGeneration: 0,
			Descriptor:         initialDesc,
		},
	})

	leaseOne, err := lease.NewRecord(
		1,
		1,
		hlc.Timestamp{WallTime: 100, Logical: 0},
		hlc.Timestamp{WallTime: 300, Logical: 0},
		1,
	)
	if err != nil {
		t.Fatalf("new lease record: %v", err)
	}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetLease,
		Lease:   &replica.SetLease{Record: leaseOne},
	})

	logicalKey := storage.GlobalTablePrimaryKey(11, []byte("bob"))
	valueTS := hlc.Timestamp{WallTime: 140, Logical: 2}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypePutValue,
		Put: &replica.PutValue{
			LogicalKey: logicalKey,
			Timestamp:  valueTS,
			Value:      []byte("balance"),
		},
	})
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetClosedTS,
		ClosedTS: &replica.SetClosedTS{
			Record: closedts.Record{
				RangeID:       9,
				LeaseSequence: leaseOne.Sequence,
				ClosedTS:      valueTS,
				PublishedAt:   hlc.Timestamp{WallTime: 150, Logical: 0},
			},
		},
	})

	follower, ok := cluster.Replica(2)
	if !ok {
		t.Fatalf("replica 2 missing")
	}
	if _, err := follower.State.HistoricalGet(ctx, logicalKey, valueTS); err != nil {
		t.Fatalf("historical get before transfer: %v", err)
	}

	leaseTwo, err := leaseOne.Transfer(2, 2, hlc.Timestamp{WallTime: 170, Logical: 0}, hlc.Timestamp{WallTime: 320, Logical: 0})
	if err != nil {
		t.Fatalf("transfer lease: %v", err)
	}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetLease,
		Lease:   &replica.SetLease{Record: leaseTwo},
	})

	nextDesc := initialDesc
	nextDesc.Generation = 2
	nextDesc.LeaseholderReplicaID = 2
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeUpdateDescriptor,
		Descriptor: &replica.UpdateDescriptor{
			ExpectedGeneration: 1,
			Descriptor:         nextDesc,
		},
	})

	_, err = follower.State.HistoricalGet(ctx, logicalKey, valueTS)
	if !errors.Is(err, closedts.ErrClosedTimestampLeaseMismatch) {
		t.Fatalf("historical read after transfer = %v, want %v", err, closedts.ErrClosedTimestampLeaseMismatch)
	}

	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetClosedTS,
		ClosedTS: &replica.SetClosedTS{
			Record: closedts.Record{
				RangeID:       9,
				LeaseSequence: leaseTwo.Sequence,
				ClosedTS:      hlc.Timestamp{WallTime: 170, Logical: 0},
				PublishedAt:   hlc.Timestamp{WallTime: 180, Logical: 0},
			},
		},
	})
	got, err := follower.State.HistoricalGet(ctx, logicalKey, valueTS)
	if err != nil {
		t.Fatalf("historical get after refreshed closed timestamp: %v", err)
	}
	if !bytes.Equal(got, []byte("balance")) {
		t.Fatalf("historical value after transfer = %q, want %q", got, []byte("balance"))
	}
}

func TestNewRangeClusterRejectsInvalidMembers(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		members []Member
		want    string
	}{
		{
			name:    "zero node id",
			members: []Member{{NodeID: 0, ReplicaID: 1}},
			want:    "node id",
		},
		{
			name:    "zero replica id",
			members: []Member{{NodeID: 1, ReplicaID: 0}},
			want:    "replica id",
		},
		{
			name: "duplicate replica id",
			members: []Member{
				{NodeID: 1, ReplicaID: 1},
				{NodeID: 2, ReplicaID: 1},
			},
			want: "duplicate replica id",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cluster, err := NewRangeCluster(3, tc.members)
			if err == nil {
				_ = cluster.Close()
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestRangeClusterLearnerSnapshotCatchUpAndPromotion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster, err := NewRangeCluster(12, []Member{
		{NodeID: 1, ReplicaID: 1},
		{NodeID: 2, ReplicaID: 2},
		{NodeID: 3, ReplicaID: 3},
	})
	if err != nil {
		t.Fatalf("new range cluster: %v", err)
	}
	defer cluster.Close()

	rangeStart := storage.GlobalTablePrimaryPrefix(21)
	rangeEnd := storage.GlobalTablePrimaryPrefix(22)
	desc := meta.RangeDescriptor{
		RangeID:    12,
		Generation: 1,
		StartKey:   rangeStart,
		EndKey:     rangeEnd,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 1,
	}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeUpdateDescriptor,
		Descriptor: &replica.UpdateDescriptor{
			ExpectedGeneration: 0,
			Descriptor:         desc,
		},
	})

	leaseRecord, err := lease.NewRecord(
		1,
		1,
		hlc.Timestamp{WallTime: 100, Logical: 0},
		hlc.Timestamp{WallTime: 400, Logical: 0},
		1,
	)
	if err != nil {
		t.Fatalf("new lease record: %v", err)
	}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetLease,
		Lease:   &replica.SetLease{Record: leaseRecord},
	})

	logicalKey := storage.GlobalTablePrimaryKey(21, []byte("warehouse"))
	valueTS := hlc.Timestamp{WallTime: 180, Logical: 3}
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypePutValue,
		Put: &replica.PutValue{
			LogicalKey: logicalKey,
			Timestamp:  valueTS,
			Value:      []byte("reserved"),
		},
	})
	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetClosedTS,
		ClosedTS: &replica.SetClosedTS{
			Record: closedts.Record{
				RangeID:       12,
				LeaseSequence: leaseRecord.Sequence,
				ClosedTS:      valueTS,
				PublishedAt:   hlc.Timestamp{WallTime: 190, Logical: 0},
			},
		},
	})

	applyReplicatedCommand(t, cluster, []uint64{1, 2}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeChangeReplicas,
		Replica: &replica.ChangeReplicas{
			ExpectedGeneration: 1,
			MetaLevel:          meta.LevelMeta2,
			Change: replica.ReplicaChange{
				Kind:    replica.ReplicaChangeAddLearner,
				Replica: meta.ReplicaDescriptor{ReplicaID: 3, NodeID: 3, Role: meta.ReplicaRoleLearner},
			},
		},
	})

	snapshot, err := cluster.CaptureSnapshot(1)
	if err != nil {
		t.Fatalf("capture snapshot: %v", err)
	}
	if err := cluster.InstallSnapshot(3, snapshot); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	learner, ok := cluster.Replica(3)
	if !ok {
		t.Fatalf("replica 3 missing")
	}
	if learner.State.Descriptor().Generation != 2 {
		t.Fatalf("learner generation after snapshot = %d, want 2", learner.State.Descriptor().Generation)
	}
	if role, ok := replicaRole(learner.State.Descriptor(), 3); !ok || role != meta.ReplicaRoleLearner {
		t.Fatalf("learner role after snapshot = %q, ok=%v, want %q", role, ok, meta.ReplicaRoleLearner)
	}
	got, err := learner.State.HistoricalGet(ctx, logicalKey, valueTS)
	if err != nil {
		t.Fatalf("learner historical get after snapshot: %v", err)
	}
	if !bytes.Equal(got, []byte("reserved")) {
		t.Fatalf("learner value after snapshot = %q, want %q", got, []byte("reserved"))
	}

	applyReplicatedCommand(t, cluster, []uint64{1, 2, 3}, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeChangeReplicas,
		Replica: &replica.ChangeReplicas{
			ExpectedGeneration: 2,
			MetaLevel:          meta.LevelMeta2,
			Change: replica.ReplicaChange{
				Kind:    replica.ReplicaChangePromote,
				Replica: meta.ReplicaDescriptor{ReplicaID: 3, NodeID: 3, Role: meta.ReplicaRoleVoter},
			},
		},
	})

	for _, replicaID := range []uint64{1, 2, 3} {
		node, ok := cluster.Replica(replicaID)
		if !ok {
			t.Fatalf("replica %d missing", replicaID)
		}
		if node.State.Descriptor().Generation != 3 {
			t.Fatalf("replica %d generation after promotion = %d, want 3", replicaID, node.State.Descriptor().Generation)
		}
		if role, ok := replicaRole(node.State.Descriptor(), 3); !ok || role != meta.ReplicaRoleVoter {
			t.Fatalf("replica %d promoted role = %q, ok=%v, want %q", replicaID, role, ok, meta.ReplicaRoleVoter)
		}
	}
}

func applyReplicatedCommand(t *testing.T, cluster *RangeCluster, replicaIDs []uint64, cmd replica.Command) {
	t.Helper()

	entry := nextEntry(t, cluster, cmd)
	if err := cluster.ApplyEntry(replicaIDs, entry); err != nil {
		t.Fatalf("apply entry: %v", err)
	}
}

func nextEntry(t *testing.T, cluster *RangeCluster, cmd replica.Command) raftpb.Entry {
	t.Helper()

	entry, err := cluster.NextEntry(cmd)
	if err != nil {
		t.Fatalf("next entry: %v", err)
	}
	return entry
}

func replicaRole(desc meta.RangeDescriptor, replicaID uint64) (meta.ReplicaRole, bool) {
	for _, replica := range desc.Replicas {
		if replica.ReplicaID == replicaID {
			return replica.Role, true
		}
	}
	return "", false
}
