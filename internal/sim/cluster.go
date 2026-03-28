package sim

import (
	"context"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/replica"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/cockroachdb/pebble/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// Member identifies one replica in the deterministic simulation cluster.
type Member struct {
	NodeID    uint64
	ReplicaID uint64
}

// ReplicaNode is one deterministic replica host in the simulation.
type ReplicaNode struct {
	NodeID    uint64
	ReplicaID uint64
	Clock     *hlc.ManualClock
	Engine    *storage.Engine
	State     *replica.StateMachine
}

// RangeCluster is a deterministic multi-replica harness for one range.
type RangeCluster struct {
	rangeID   uint64
	nextIndex uint64
	replicas  map[uint64]*ReplicaNode
}

// NewRangeCluster constructs a deterministic replica harness for one range.
func NewRangeCluster(rangeID uint64, members []Member) (*RangeCluster, error) {
	if rangeID == 0 {
		return nil, fmt.Errorf("sim: range id must be non-zero")
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("sim: at least one member is required")
	}
	seenReplicas := make(map[uint64]struct{}, len(members))
	cluster := &RangeCluster{
		rangeID:   rangeID,
		nextIndex: 1,
		replicas:  make(map[uint64]*ReplicaNode, len(members)),
	}
	for _, member := range members {
		switch {
		case member.NodeID == 0:
			return nil, fmt.Errorf("sim: member node id must be non-zero")
		case member.ReplicaID == 0:
			return nil, fmt.Errorf("sim: member replica id must be non-zero")
		}
		if _, ok := seenReplicas[member.ReplicaID]; ok {
			return nil, fmt.Errorf("sim: duplicate replica id %d", member.ReplicaID)
		}
		seenReplicas[member.ReplicaID] = struct{}{}
		engine, err := storage.Open(context.Background(), storage.Options{
			Dir: fmt.Sprintf("sim-%d-%d", member.NodeID, member.ReplicaID),
			FS:  vfs.NewMem(),
		})
		if err != nil {
			cluster.Close()
			return nil, err
		}
		if err := engine.Bootstrap(context.Background(), storage.StoreIdent{
			ClusterID: "sim-cluster",
			NodeID:    member.NodeID,
			StoreID:   member.NodeID,
		}); err != nil {
			cluster.Close()
			return nil, err
		}
		state, err := replica.OpenStateMachine(rangeID, member.ReplicaID, engine)
		if err != nil {
			cluster.Close()
			return nil, err
		}
		cluster.replicas[member.ReplicaID] = &ReplicaNode{
			NodeID:    member.NodeID,
			ReplicaID: member.ReplicaID,
			Clock:     hlc.NewManualClock(hlc.Timestamp{WallTime: 1, Logical: 0}),
			Engine:    engine,
			State:     state,
		}
	}
	return cluster, nil
}

// Close releases the engines backing the simulation cluster.
func (c *RangeCluster) Close() error {
	var firstErr error
	for _, replicaNode := range c.replicas {
		if err := replicaNode.Engine.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Replica returns the deterministic replica state by replica id.
func (c *RangeCluster) Replica(replicaID uint64) (*ReplicaNode, bool) {
	node, ok := c.replicas[replicaID]
	return node, ok
}

// NextEntry allocates the next log entry index for a replicated command.
func (c *RangeCluster) NextEntry(cmd replica.Command) (raftpb.Entry, error) {
	payload, err := cmd.Marshal()
	if err != nil {
		return raftpb.Entry{}, err
	}
	entry := raftpb.Entry{
		Index: c.nextIndex,
		Type:  raftpb.EntryNormal,
		Data:  payload,
	}
	c.nextIndex++
	return entry, nil
}

// ApplyEntry applies one replicated entry to the selected replicas.
func (c *RangeCluster) ApplyEntry(replicaIDs []uint64, entry raftpb.Entry) error {
	for _, replicaID := range replicaIDs {
		node, ok := c.replicas[replicaID]
		if !ok {
			return fmt.Errorf("sim: replica %d not found", replicaID)
		}
		batch := node.Engine.NewWriteBatch()
		delta, err := node.State.StageEntries(batch, []raftpb.Entry{entry})
		if err != nil {
			_ = batch.Close()
			return err
		}
		if err := batch.Commit(true); err != nil {
			_ = batch.Close()
			return err
		}
		node.State.CommitApply(delta)
		if err := batch.Close(); err != nil {
			return err
		}
	}
	return nil
}
