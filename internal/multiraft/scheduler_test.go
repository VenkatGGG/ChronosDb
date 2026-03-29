package multiraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/VenkatGGG/ChronosDb/internal/replica"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/cockroachdb/pebble/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type testNode struct {
	id        uint64
	scheduler *Scheduler
	clock     *hlc.ManualClock
	liveness  uint64
}

type testCluster struct {
	nodes map[uint64]*testNode
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()

	cluster := &testCluster{
		nodes: make(map[uint64]*testNode),
	}
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		engine, err := storage.Open(context.Background(), storage.Options{
			Dir: fmt.Sprintf("node-%d", nodeID),
			FS:  vfs.NewMem(),
		})
		if err != nil {
			t.Fatalf("open engine %d: %v", nodeID, err)
		}
		if err := engine.Bootstrap(context.Background(), storage.StoreIdent{
			ClusterID: "cluster-a",
			NodeID:    nodeID,
			StoreID:   nodeID,
		}); err != nil {
			t.Fatalf("bootstrap engine %d: %v", nodeID, err)
		}
		scheduler := NewScheduler(engine)
		if err := scheduler.AddGroup(GroupConfig{
			RangeID:       1,
			ReplicaID:     nodeID,
			Peers:         []uint64{1, 2, 3},
			ElectionTick:  10,
			HeartbeatTick: 1,
		}); err != nil {
			t.Fatalf("add group %d: %v", nodeID, err)
		}
		cluster.nodes[nodeID] = &testNode{
			id:        nodeID,
			scheduler: scheduler,
			clock:     hlc.NewManualClock(hlc.Timestamp{WallTime: 100, Logical: 0}),
			liveness:  1,
		}
	}
	cluster.pumpUntil(t, 32, func(_ map[uint64]ProcessResult) bool {
		for _, node := range cluster.nodes {
			repl, err := node.scheduler.Replica(1)
			if err != nil {
				t.Fatalf("bootstrap replica lookup: %v", err)
			}
			if repl.AppliedIndex() < 3 {
				return false
			}
		}
		return true
	})
	return cluster
}

func (c *testCluster) campaign(t *testing.T, nodeID uint64) {
	t.Helper()
	if err := c.nodes[nodeID].scheduler.Campaign(1); err != nil {
		t.Fatalf("campaign from %d: %v", nodeID, err)
	}
	c.pumpUntil(t, 64, func(_ map[uint64]ProcessResult) bool {
		return c.leader(t) == nodeID
	})
}

func (c *testCluster) leader(t *testing.T) uint64 {
	t.Helper()
	for nodeID, node := range c.nodes {
		leader, err := node.scheduler.Leader(1)
		if err != nil {
			t.Fatalf("get leader from %d: %v", nodeID, err)
		}
		if leader != 0 {
			return leader
		}
	}
	return 0
}

func (c *testCluster) propose(t *testing.T, nodeID uint64, cmd replica.Command) {
	t.Helper()
	payload, err := cmd.Marshal()
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if err := c.nodes[nodeID].scheduler.Propose(1, payload); err != nil {
		t.Fatalf("propose on %d: %v", nodeID, err)
	}
	c.pumpUntil(t, 64, func(_ map[uint64]ProcessResult) bool {
		switch cmd.Type {
		case replica.CommandTypePutValue:
			for _, node := range c.nodes {
				repl, err := node.scheduler.Replica(1)
				if err != nil {
					t.Fatalf("get replica: %v", err)
				}
				got, err := repl.ReadExact(context.Background(), cmd.Put.LogicalKey, cmd.Put.Timestamp)
				if err != nil || !bytes.Equal(got, cmd.Put.Value) {
					return false
				}
			}
			return true
		case replica.CommandTypeSetLease:
			for _, node := range c.nodes {
				repl, err := node.scheduler.Replica(1)
				if err != nil {
					t.Fatalf("get replica: %v", err)
				}
				current := repl.Lease()
				if current.Sequence != cmd.Lease.Record.Sequence || current.HolderReplicaID != cmd.Lease.Record.HolderReplicaID {
					return false
				}
			}
			return true
		default:
			t.Fatalf("unhandled command type in test helper: %q", cmd.Type)
		}
		return false
	})
}

func (c *testCluster) transferLeader(t *testing.T, from, to uint64) {
	t.Helper()
	if err := c.nodes[from].scheduler.TransferLeader(1, to); err != nil {
		t.Fatalf("transfer leader %d->%d: %v", from, to, err)
	}
	c.pumpUntil(t, 64, func(_ map[uint64]ProcessResult) bool {
		return c.leader(t) == to
	})
}

func (c *testCluster) waitForReadState(t *testing.T, nodeID uint64, requestCtx string) {
	t.Helper()
	c.pumpUntil(t, 64, func(results map[uint64]ProcessResult) bool {
		states := results[nodeID].ReadStates[1]
		if len(states) == 0 {
			return false
		}
		repl, err := c.nodes[nodeID].scheduler.Replica(1)
		if err != nil {
			t.Fatalf("lookup replica: %v", err)
		}
		for _, state := range states {
			if string(state.RequestCtx) == requestCtx && repl.AppliedIndex() >= state.Index {
				return true
			}
		}
		return false
	})
}

func (c *testCluster) pumpUntil(t *testing.T, rounds int, done func(map[uint64]ProcessResult) bool) {
	t.Helper()
	for i := 0; i < rounds; i++ {
		results, progressed, err := c.pumpOnce()
		if err != nil {
			t.Fatalf("pump once: %v", err)
		}
		if done(results) {
			return
		}
		if !progressed {
			for _, node := range c.nodes {
				node.scheduler.Tick()
			}
		}
	}
	t.Fatal("cluster did not reach expected state")
}

func (c *testCluster) pumpOnce() (map[uint64]ProcessResult, bool, error) {
	progressed := false
	results := make(map[uint64]ProcessResult, len(c.nodes))
	var outbound []MessageEnvelope
	for nodeID, node := range c.nodes {
		result, err := node.scheduler.ProcessReady()
		if err != nil {
			return nil, false, err
		}
		results[nodeID] = result
		if len(result.Messages) > 0 || len(result.ReadStates) > 0 {
			progressed = true
		}
		outbound = append(outbound, result.Messages...)
	}
	for _, envelope := range outbound {
		progressed = true
		target := c.nodes[envelope.Message.To]
		if err := target.scheduler.Step(envelope.RangeID, envelope.Message); err != nil {
			return nil, false, err
		}
	}
	return results, progressed, nil
}

func TestSingleRangeReplicatesTransfersLeaseAndReadsSafely(t *testing.T) {
	t.Parallel()

	cluster := newTestCluster(t)
	cluster.campaign(t, 1)

	initialLease, err := lease.NewRecord(1, cluster.nodes[1].liveness, hlc.Timestamp{WallTime: 120, Logical: 0}, hlc.Timestamp{WallTime: 220, Logical: 0}, 1)
	if err != nil {
		t.Fatalf("new initial lease: %v", err)
	}
	valueKey := storage.GlobalTablePrimaryKey(7, []byte("alice"))
	valueTS := hlc.Timestamp{WallTime: 130, Logical: 1}
	cluster.propose(t, 1, replica.Command{
		Version: 1,
		Type:    replica.CommandTypePutValue,
		Put: &replica.PutValue{
			LogicalKey: valueKey,
			Timestamp:  valueTS,
			Value:      []byte("v1"),
		},
	})
	cluster.propose(t, 1, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetLease,
		Lease:   &replica.SetLease{Record: initialLease},
	})

	repl1, err := cluster.nodes[1].scheduler.Replica(1)
	if err != nil {
		t.Fatalf("replica 1: %v", err)
	}
	cluster.nodes[1].clock.Set(hlc.Timestamp{WallTime: 140, Logical: 0})
	got, err := repl1.FastGet(context.Background(), valueKey, valueTS, lease.FastReadRequest{
		CurrentLivenessEpoch: cluster.nodes[1].liveness,
		Now:                  cluster.nodes[1].clock.Now(),
		ReadTimestamp:        valueTS,
	})
	if err != nil {
		t.Fatalf("fast read on leaseholder: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("fast read value = %q, want %q", got, []byte("v1"))
	}

	cluster.transferLeader(t, 1, 2)

	transferredLease, err := initialLease.Transfer(2, cluster.nodes[2].liveness, hlc.Timestamp{WallTime: 150, Logical: 0}, hlc.Timestamp{WallTime: 260, Logical: 0})
	if err != nil {
		t.Fatalf("transfer lease: %v", err)
	}
	cluster.propose(t, 2, replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetLease,
		Lease:   &replica.SetLease{Record: transferredLease},
	})
	valueTS2 := hlc.Timestamp{WallTime: 160, Logical: 1}
	cluster.propose(t, 2, replica.Command{
		Version: 1,
		Type:    replica.CommandTypePutValue,
		Put: &replica.PutValue{
			LogicalKey: valueKey,
			Timestamp:  valueTS2,
			Value:      []byte("v2"),
		},
	})

	cluster.nodes[1].clock.Set(hlc.Timestamp{WallTime: 170, Logical: 0})
	_, err = repl1.FastGet(context.Background(), valueKey, valueTS2, lease.FastReadRequest{
		CurrentLivenessEpoch: cluster.nodes[1].liveness,
		Now:                  cluster.nodes[1].clock.Now(),
		ReadTimestamp:        valueTS2,
	})
	if !errors.Is(err, lease.ErrReadIndexRequired) {
		t.Fatalf("old holder read error = %v, want %v", err, lease.ErrReadIndexRequired)
	}

	repl2, err := cluster.nodes[2].scheduler.Replica(1)
	if err != nil {
		t.Fatalf("replica 2: %v", err)
	}
	cluster.nodes[2].clock.Set(hlc.Timestamp{WallTime: 170, Logical: 0})
	got, err = repl2.FastGet(context.Background(), valueKey, valueTS2, lease.FastReadRequest{
		CurrentLivenessEpoch: cluster.nodes[2].liveness,
		Now:                  cluster.nodes[2].clock.Now(),
		ReadTimestamp:        valueTS2,
	})
	if err != nil {
		t.Fatalf("new holder fast read: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("new holder value = %q, want %q", got, []byte("v2"))
	}

	cluster.nodes[2].liveness = 2
	_, err = repl2.FastGet(context.Background(), valueKey, valueTS2, lease.FastReadRequest{
		CurrentLivenessEpoch: cluster.nodes[2].liveness,
		Now:                  cluster.nodes[2].clock.Now(),
		ReadTimestamp:        valueTS2,
	})
	if !errors.Is(err, lease.ErrReadIndexRequired) {
		t.Fatalf("liveness-bump fast read error = %v, want %v", err, lease.ErrReadIndexRequired)
	}

	leader := cluster.leader(t)
	if leader != 2 {
		t.Fatalf("leader = %d, want 2", leader)
	}
	if err := cluster.nodes[leader].scheduler.ReadIndex(1, []byte("read-1")); err != nil {
		t.Fatalf("read index request: %v", err)
	}
	cluster.waitForReadState(t, leader, "read-1")

	got, err = repl2.ReadExact(context.Background(), valueKey, valueTS2)
	if err != nil {
		t.Fatalf("read value after ReadIndex: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("read index value = %q, want %q", got, []byte("v2"))
	}

	for nodeID, node := range cluster.nodes {
		repl, err := node.scheduler.Replica(1)
		if err != nil {
			t.Fatalf("replica lookup on %d: %v", nodeID, err)
		}
		got, err := repl.ReadExact(context.Background(), valueKey, valueTS2)
		if err != nil {
			t.Fatalf("replica %d exact read: %v", nodeID, err)
		}
		if !bytes.Equal(got, []byte("v2")) {
			t.Fatalf("replica %d value = %q, want %q", nodeID, got, []byte("v2"))
		}
	}
}

func TestConfChangeAddsLearnerAndReplicates(t *testing.T) {
	t.Parallel()

	cluster := newTestCluster(t)

	engine, err := storage.Open(context.Background(), storage.Options{
		Dir: "node-4",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open learner engine: %v", err)
	}
	if err := engine.Bootstrap(context.Background(), storage.StoreIdent{
		ClusterID: "cluster-a",
		NodeID:    4,
		StoreID:   4,
	}); err != nil {
		t.Fatalf("bootstrap learner engine: %v", err)
	}
	learner := NewScheduler(engine)
	if err := learner.AddGroup(GroupConfig{
		RangeID:       1,
		ReplicaID:     4,
		ElectionTick:  10,
		HeartbeatTick: 1,
	}); err != nil {
		t.Fatalf("add learner group: %v", err)
	}
	cluster.nodes[4] = &testNode{
		id:        4,
		scheduler: learner,
		clock:     hlc.NewManualClock(hlc.Timestamp{WallTime: 100, Logical: 0}),
		liveness:  1,
	}

	cluster.campaign(t, 1)
	if err := cluster.nodes[1].scheduler.ProposeConfChange(1, raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddLearnerNode,
		NodeID: 4,
	}); err != nil {
		t.Fatalf("propose add learner: %v", err)
	}
	cluster.pumpUntil(t, 64, func(_ map[uint64]ProcessResult) bool {
		repl, err := cluster.nodes[4].scheduler.Replica(1)
		if err != nil {
			t.Fatalf("lookup learner replica: %v", err)
		}
		return repl.AppliedIndex() > 0
	})

	valueKey := storage.GlobalTablePrimaryKey(7, []byte("learner"))
	valueTS := hlc.Timestamp{WallTime: 150, Logical: 1}
	cluster.propose(t, 1, replica.Command{
		Version: 1,
		Type:    replica.CommandTypePutValue,
		Put: &replica.PutValue{
			LogicalKey: valueKey,
			Timestamp:  valueTS,
			Value:      []byte("v1"),
		},
	})
}
