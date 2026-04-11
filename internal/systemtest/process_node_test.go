package systemtest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestProcessNodeAdminEndpoints(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	node, err := NewProcessNode(ProcessNodeConfig{
		NodeID:            1,
		DataDir:           dataDir,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Ranges: []meta.RangeDescriptor{
			{
				RangeID:    11,
				Generation: 2,
				StartKey:   []byte("a"),
				EndKey:     []byte("m"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
					{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
				},
				LeaseholderReplicaID: 1,
			},
			{
				RangeID:    12,
				Generation: 5,
				StartKey:   []byte("m"),
				EndKey:     []byte("z"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 3, NodeID: 1, Role: meta.ReplicaRoleLearner},
					{ReplicaID: 4, NodeID: 3, Role: meta.ReplicaRoleVoter},
				},
				LeaseholderReplicaID: 4,
			},
			{
				RangeID:    13,
				Generation: 1,
				StartKey:   []byte("z"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 5, NodeID: 2, Role: meta.ReplicaRoleVoter},
				},
				LeaseholderReplicaID: 5,
			},
		},
		EventBufferSize: 8,
	})
	if err != nil {
		t.Fatalf("new process node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				t.Fatalf("node run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for process node shutdown")
		}
	})

	state := waitForProcessNodeState(t, filepath.Join(dataDir, "state.json"))
	client := &http.Client{Timeout: 2 * time.Second}

	var nodeView adminapi.NodeView
	getJSON(t, client, state.ObservabilityURL+"/admin/node", &nodeView)
	if nodeView.NodeID != 1 {
		t.Fatalf("node id = %d, want 1", nodeView.NodeID)
	}
	if nodeView.Status != "ok" {
		t.Fatalf("node status = %q, want ok", nodeView.Status)
	}
	if nodeView.ReplicaCount != 2 || nodeView.LeaseCount != 1 {
		t.Fatalf("node counts = replicas %d leases %d, want 2/1", nodeView.ReplicaCount, nodeView.LeaseCount)
	}

	var ranges []adminapi.RangeView
	getJSON(t, client, state.ObservabilityURL+"/admin/ranges", &ranges)
	if len(ranges) != 2 {
		t.Fatalf("local ranges = %+v, want 2 local descriptors", ranges)
	}
	if ranges[0].RangeID != 11 || ranges[1].RangeID != 12 {
		t.Fatalf("range ids = %+v, want 11 and 12", ranges)
	}

	postJSON(t, client, state.ControlURL+"/control/partition", partitionControlRequest{
		IsolatedFrom: []uint64{2, 3},
	})

	getJSON(t, client, state.ObservabilityURL+"/admin/node", &nodeView)
	if nodeView.Status != "degraded" {
		t.Fatalf("node status after partition = %q, want degraded", nodeView.Status)
	}
	if len(nodeView.PartitionedFrom) != 2 || nodeView.PartitionedFrom[0] != 2 || nodeView.PartitionedFrom[1] != 3 {
		t.Fatalf("partitioned_from = %v, want [2 3]", nodeView.PartitionedFrom)
	}

	var events []adminapi.ClusterEvent
	getJSON(t, client, state.ObservabilityURL+"/admin/events?limit=2", &events)
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2", len(events))
	}
	if events[1].Type != "partition_applied" {
		t.Fatalf("latest event = %+v, want partition_applied", events[1])
	}

	var snapshot adminapi.ClusterSnapshot
	getJSON(t, client, state.ObservabilityURL+"/admin/snapshot?event_limit=1", &snapshot)
	if len(snapshot.Nodes) != 1 || len(snapshot.Ranges) != 2 || len(snapshot.Events) != 1 {
		t.Fatalf("snapshot = %+v, want one node, two ranges, one event", snapshot)
	}
	if snapshot.Events[0].Type != "partition_applied" {
		t.Fatalf("snapshot latest event = %+v, want partition_applied", snapshot.Events[0])
	}
}

func TestProcessNodeProtectsAdminAndControlHTTP(t *testing.T) {
	t.Parallel()

	node, err := NewProcessNode(ProcessNodeConfig{
		NodeID:    1,
		StoreID:   1,
		ClusterID: "chronos-auth",
		DataDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new process node: %v", err)
	}
	defer node.host.Close()

	controlReq := httptest.NewRequest(http.MethodGet, "/control/state", nil)
	controlReq.RemoteAddr = "10.0.0.8:4567"
	controlRec := httptest.NewRecorder()
	node.controlMux().ServeHTTP(controlRec, controlReq)
	if controlRec.Code != http.StatusForbidden {
		t.Fatalf("control status = %d, want %d", controlRec.Code, http.StatusForbidden)
	}

	adminReq := httptest.NewRequest(http.MethodGet, "/admin/node", nil)
	adminReq.RemoteAddr = "10.0.0.8:4567"
	adminRec := httptest.NewRecorder()
	node.observabilityMux().ServeHTTP(adminRec, adminReq)
	if adminRec.Code != http.StatusForbidden {
		t.Fatalf("admin status = %d, want %d", adminRec.Code, http.StatusForbidden)
	}

	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthReq.RemoteAddr = "10.0.0.8:4567"
	healthRec := httptest.NewRecorder()
	node.observabilityMux().ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want %d", healthRec.Code, http.StatusOK)
	}
}

func TestProcessNodeAdminRangesIncludeRowStats(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	node, err := NewProcessNode(ProcessNodeConfig{
		NodeID:            1,
		DataDir:           dataDir,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Ranges: []meta.RangeDescriptor{
			{
				RangeID:    21,
				Generation: 1,
				StartKey:   storage.GlobalTablePrimaryPrefix(7),
				EndKey:     storage.GlobalTablePrimaryPrefix(8),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
				},
				LeaseholderReplicaID: 1,
			},
		},
	})
	if err != nil {
		t.Fatalf("new process node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				t.Fatalf("node run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for process node shutdown")
		}
	})

	waitForProcessNodeState(t, filepath.Join(dataDir, "state.json"))
	if err := node.host.Campaign(context.Background(), 21); err != nil {
		t.Fatalf("campaign users range: %v", err)
	}
	waitForRangeLeader(t, node.host, 21, 1)
	putTS := hlc.Timestamp{WallTime: 10}
	for _, id := range []int64{1, 2, 3} {
		if _, err := node.host.PutValueLocal(
			context.Background(),
			storage.GlobalTablePrimaryKey(7, encodedIntKeyForTest(id)),
			putTS,
			[]byte(fmt.Sprintf("user-%d", id)),
		); err != nil {
			t.Fatalf("put row %d: %v", id, err)
		}
		putTS.WallTime++
	}

	client := &http.Client{Timeout: 2 * time.Second}
	state := waitForProcessNodeState(t, filepath.Join(dataDir, "state.json"))
	var ranges []adminapi.RangeView
	getJSON(t, client, state.ObservabilityURL+"/admin/ranges", &ranges)
	if len(ranges) != 1 {
		t.Fatalf("ranges = %+v, want 1", ranges)
	}
	if ranges[0].ShardLabel != "users shard" || ranges[0].Keyspace != "table" {
		t.Fatalf("range label = %+v, want users table shard", ranges[0])
	}
	if ranges[0].RowCount != 3 {
		t.Fatalf("range row count = %d, want 3", ranges[0].RowCount)
	}
	if len(ranges[0].Tables) != 1 || ranges[0].Tables[0].TableName != "users" || ranges[0].Tables[0].RowCount != 3 {
		t.Fatalf("range tables = %+v, want users row count 3", ranges[0].Tables)
	}
}

func TestProcessNodePersistsHostedDescriptorsAcrossRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	seeded := ProcessNodeConfig{
		NodeID:            1,
		DataDir:           dataDir,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Ranges: []meta.RangeDescriptor{
			{
				RangeID:    21,
				Generation: 4,
				StartKey:   []byte("alpha"),
				EndKey:     []byte("omega"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 11, NodeID: 1, Role: meta.ReplicaRoleVoter},
					{ReplicaID: 12, NodeID: 2, Role: meta.ReplicaRoleVoter},
				},
				LeaseholderReplicaID: 11,
			},
		},
	}
	runNodeOnce(t, seeded)

	restarted := ProcessNodeConfig{
		NodeID:            1,
		DataDir:           dataDir,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	}
	if err := os.Remove(filepath.Join(restarted.DataDir, "state.json")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove stale state file: %v", err)
	}
	node, err := NewProcessNode(restarted)
	if err != nil {
		t.Fatalf("new restarted process node: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				t.Fatalf("restarted node run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for restarted process node shutdown")
		}
	})
	state := waitForProcessNodeState(t, filepath.Join(restarted.DataDir, "state.json"))

	client := &http.Client{Timeout: 2 * time.Second}
	var ranges []adminapi.RangeView
	getJSON(t, client, state.ObservabilityURL+"/admin/ranges", &ranges)
	if len(ranges) != 1 {
		t.Fatalf("persisted ranges = %+v, want 1", ranges)
	}
	if ranges[0].RangeID != 21 || ranges[0].Source != "runtime_store" {
		t.Fatalf("persisted range = %+v, want range 21 from runtime_store", ranges[0])
	}

	var nodeView adminapi.NodeView
	getJSON(t, client, state.ObservabilityURL+"/admin/node", &nodeView)
	if nodeView.ReplicaCount != 1 || nodeView.LeaseCount != 1 {
		t.Fatalf("persisted node counts = replicas %d leases %d, want 1/1", nodeView.ReplicaCount, nodeView.LeaseCount)
	}
}

func encodedIntKeyForTest(value int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value)^(uint64(1)<<63))
	return buf[:]
}

func TestProcessNodeRaftTransportElectsAcrossProcesses(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	desc := meta.RangeDescriptor{
		RangeID:    31,
		Generation: 1,
		StartKey:   []byte("a"),
		EndKey:     []byte("z"),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 11, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 12, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 11,
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            1,
		DataDir:           filepath.Join(rootDir, "node-1"),
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Ranges:            []meta.RangeDescriptor{desc},
	})
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "node1")
	})
	node2, cancel2, done2 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            2,
		DataDir:           filepath.Join(rootDir, "node-2"),
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Ranges:            []meta.RangeDescriptor{desc},
	})
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "node2")
	})

	if err := node1.host.Campaign(context.Background(), 31); err != nil {
		t.Fatalf("campaign node1 range 31: %v", err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		leader1, err1 := node1.host.Leader(31)
		leader2, err2 := node2.host.Leader(31)
		if err1 == nil && err2 == nil && leader1 == 11 && leader2 == 11 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	leader1, _ := node1.host.Leader(31)
	leader2, _ := node2.host.Leader(31)
	t.Fatalf("leaders after transport election = node1:%d node2:%d, want both 11", leader1, leader2)
}

func TestProcessNodePublishesPersistedNodeLiveness(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-process-liveness", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 11},
	}, []meta.RangeDescriptor{
		{
			RangeID:    41,
			Generation: 1,
			StartKey:   storage.GlobalTablePrimaryPrefix(7),
			EndKey:     storage.GlobalTablePrimaryPrefix(8),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 11, NodeID: 1, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 11,
		},
	})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	cfg := ProcessNodeConfig{
		NodeID:            1,
		DataDir:           filepath.Join(rootDir, "node-1"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		HeartbeatInterval: 50 * time.Millisecond,
	}
	client := &http.Client{Timeout: 2 * time.Second}
	node1, cancel1, done1 := startProcessNodeForTest(t, cfg)
	firstState := waitForProcessNodeState(t, filepath.Join(cfg.DataDir, "state.json"))
	var firstView adminapi.NodeView
	waitForNodeLivenessEpoch(t, client, firstState.ObservabilityURL+"/admin/node", 1)
	getJSON(t, client, firstState.ObservabilityURL+"/admin/node", &firstView)
	if firstView.LivenessEpoch != 1 {
		t.Fatalf("first liveness epoch = %d, want 1", firstView.LivenessEpoch)
	}
	if firstView.LivenessUpdatedAt.IsZero() {
		t.Fatal("first liveness updated_at is zero")
	}
	cancel1()
	waitProcessNodeDone(t, done1, "liveness-initial")
	if node1.host.LivenessEpoch() != 1 {
		t.Fatalf("initial host liveness epoch = %d, want 1", node1.host.LivenessEpoch())
	}

	restarted := ProcessNodeConfig{
		NodeID:            1,
		DataDir:           filepath.Join(rootDir, "node-1"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		HeartbeatInterval: 50 * time.Millisecond,
	}
	node, cancel, done := startProcessNodeForTest(t, restarted)
	t.Cleanup(func() {
		cancel()
		waitProcessNodeDone(t, done, "liveness-restart")
	})
	state := waitForProcessNodeState(t, filepath.Join(restarted.DataDir, "state.json"))

	waitForNodeLivenessEpoch(t, client, state.ObservabilityURL+"/admin/node", 2)
	if node.host.LivenessEpoch() != 2 {
		t.Fatalf("host liveness epoch = %d, want 2", node.host.LivenessEpoch())
	}
}

func TestProcessNodeEmitsRebalanceRecommendation(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-process-rebalance", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 21},
		{NodeID: 2, StoreID: 22},
		{NodeID: 3, StoreID: 23},
	}, []meta.RangeDescriptor{
		{
			RangeID:    51,
			Generation: 1,
			StartKey:   storage.GlobalTablePrimaryPrefix(7),
			EndKey:     storage.GlobalTablePrimaryPrefix(8),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 21, NodeID: 1, Role: meta.ReplicaRoleVoter},
				{ReplicaID: 22, NodeID: 2, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 21,
		},
	})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	startCfg := func(nodeID uint64, storeID uint64) ProcessNodeConfig {
		return ProcessNodeConfig{
			NodeID:            nodeID,
			StoreID:           storeID,
			DataDir:           filepath.Join(rootDir, fmt.Sprintf("node-%d", nodeID)),
			BootstrapPath:     bootstrapPath,
			PGListenAddr:      "127.0.0.1:0",
			ObservabilityAddr: "127.0.0.1:0",
			ControlAddr:       "127.0.0.1:0",
			HeartbeatInterval: 50 * time.Millisecond,
			AllocatorInterval: 50 * time.Millisecond,
		}
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, startCfg(1, 21))
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "rebalance-node1")
	})
	_, cancel2, done2 := startProcessNodeForTest(t, startCfg(2, 22))
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "rebalance-node2")
	})
	_, cancel3, done3 := startProcessNodeForTest(t, startCfg(3, 23))
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "rebalance-node3")
	})

	if err := node1.host.Campaign(context.Background(), 51); err != nil {
		t.Fatalf("campaign rebalance range: %v", err)
	}
	waitForRangeLeader(t, node1.host, 51, 21)
	if err := node1.recommendRebalances(context.Background()); err != nil {
		t.Fatalf("recommend rebalances: %v", err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	waitForEventType(t, client, node1.state.ObservabilityURL+"/admin/events?limit=32", "rebalance_recommended")
}

func TestProcessNodeAppliesLiveRebalance(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	rangeOne := meta.RangeDescriptor{
		RangeID:    61,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 31, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 32, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 31,
	}
	rangeTwo := meta.RangeDescriptor{
		RangeID:    62,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(9),
		EndKey:     storage.GlobalTablePrimaryPrefix(10),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 33, NodeID: 2, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 33,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-process-live-rebalance", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 31},
		{NodeID: 2, StoreID: 32},
		{NodeID: 3, StoreID: 33},
	}, []meta.RangeDescriptor{rangeOne, rangeTwo})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	startCfg := func(nodeID uint64, storeID uint64) ProcessNodeConfig {
		return ProcessNodeConfig{
			NodeID:            nodeID,
			StoreID:           storeID,
			DataDir:           filepath.Join(rootDir, fmt.Sprintf("node-%d", nodeID)),
			BootstrapPath:     bootstrapPath,
			PGListenAddr:      "127.0.0.1:0",
			ObservabilityAddr: "127.0.0.1:0",
			ControlAddr:       "127.0.0.1:0",
			HeartbeatInterval: 50 * time.Millisecond,
			AllocatorInterval: 50 * time.Millisecond,
		}
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, startCfg(1, 31))
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "live-rebalance-node1")
	})
	node2, cancel2, done2 := startProcessNodeForTest(t, startCfg(2, 32))
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "live-rebalance-node2")
	})
	node3, cancel3, done3 := startProcessNodeForTest(t, startCfg(3, 33))
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "live-rebalance-node3")
	})

	if err := node1.host.Campaign(context.Background(), rangeOne.RangeID); err != nil {
		t.Fatalf("campaign rebalance range: %v", err)
	}
	waitForRangeLeader(t, node1.host, rangeOne.RangeID, 31)

	client := &http.Client{Timeout: 2 * time.Second}
	waitForEventType(t, client, node1.state.ObservabilityURL+"/admin/events?limit=64", "rebalance_applied")

	waitForRangeReplicaRole(t, client, node3.state.ControlURL, rangeOne.RangeID, 33, meta.ReplicaRoleVoter)
	waitForRangeReplicaAbsent(t, client, node2.state.ControlURL, rangeOne.RangeID, 32)
}

func waitForProcessNodeState(t *testing.T, path string) ProcessNodeState {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state, err := ReadProcessNodeState(path)
		if err == nil {
			return state
		}
		if !os.IsNotExist(err) {
			t.Fatalf("read process node state: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for state file %s", path)
	return ProcessNodeState{}
}

func runNodeOnce(t *testing.T, cfg ProcessNodeConfig) ProcessNodeState {
	t.Helper()

	if err := os.Remove(filepath.Join(cfg.DataDir, "state.json")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove stale state file: %v", err)
	}
	node, err := NewProcessNode(cfg)
	if err != nil {
		t.Fatalf("new process node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx)
	}()
	state := waitForProcessNodeState(t, filepath.Join(cfg.DataDir, "state.json"))
	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("node run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for process node shutdown")
	}
	return state
}

func startProcessNodeForTest(t *testing.T, cfg ProcessNodeConfig) (*ProcessNode, context.CancelFunc, chan error) {
	t.Helper()

	if err := os.Remove(filepath.Join(cfg.DataDir, "state.json")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove stale state file: %v", err)
	}
	node, err := NewProcessNode(cfg)
	if err != nil {
		t.Fatalf("new process node: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx)
	}()
	_ = waitForProcessNodeState(t, filepath.Join(cfg.DataDir, "state.json"))
	return node, cancel, done
}

func waitProcessNodeDone(t *testing.T, done chan error, label string) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("%s run: %v", label, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s shutdown", label)
	}
}

func waitForNodeLivenessEpoch(t *testing.T, client *http.Client, url string, want uint64) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var view adminapi.NodeView
		getJSON(t, client, url, &view)
		if view.LivenessEpoch == want && !view.LivenessUpdatedAt.IsZero() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	var view adminapi.NodeView
	getJSON(t, client, url, &view)
	t.Fatalf("node liveness epoch = %d, want %d", view.LivenessEpoch, want)
}

func waitForEventType(t *testing.T, client *http.Client, url, want string) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var events []adminapi.ClusterEvent
		getJSON(t, client, url, &events)
		for _, event := range events {
			if event.Type == want {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	var events []adminapi.ClusterEvent
	getJSON(t, client, url, &events)
	t.Fatalf("event %q not found in %+v", want, events)
}

func waitForRangeReplicaRole(t *testing.T, client *http.Client, controlURL string, rangeID, replicaID uint64, role meta.ReplicaRole) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var status rangeStatusResponse
		postJSONExpectJSON(t, client, controlURL+"/control/range/status", chronosruntime.RangeStatusRequest{RangeID: rangeID}, &status)
		for _, repl := range status.Descriptor.Replicas {
			if repl.ReplicaID == replicaID && repl.Role == role {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	var status rangeStatusResponse
	postJSONExpectJSON(t, client, controlURL+"/control/range/status", chronosruntime.RangeStatusRequest{RangeID: rangeID}, &status)
	t.Fatalf("range %d replica %d role not %q: %+v", rangeID, replicaID, role, status)
}

func waitForRangeReplicaAbsent(t *testing.T, client *http.Client, controlURL string, rangeID, replicaID uint64) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var status rangeStatusResponse
		postJSONExpectJSON(t, client, controlURL+"/control/range/status", chronosruntime.RangeStatusRequest{RangeID: rangeID}, &status)
		found := false
		for _, repl := range status.Descriptor.Replicas {
			if repl.ReplicaID == replicaID {
				found = true
				break
			}
		}
		if !found {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	var status rangeStatusResponse
	postJSONExpectJSON(t, client, controlURL+"/control/range/status", chronosruntime.RangeStatusRequest{RangeID: rangeID}, &status)
	t.Fatalf("range %d still contains replica %d: %+v", rangeID, replicaID, status)
}

func postJSONExpectJSON(t *testing.T, client *http.Client, url string, in, out any) {
	t.Helper()

	var body io.Reader
	if in != nil {
		payload, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal request for %s: %v", url, err)
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("request %s status = %d body=%s", url, resp.StatusCode, payload)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode response from %s: %v", url, err)
		}
	}
}

func getJSON(t *testing.T, client *http.Client, url string, out any) {
	t.Helper()

	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get %s: status = %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

func postJSON(t *testing.T, client *http.Client, url string, payload any) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", url, err)
	}
	resp, err := client.Post(url, "application/json", bytesReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("post %s: status = %d, want %d", url, resp.StatusCode, http.StatusNoContent)
	}
}

func bytesReader(body []byte) io.Reader {
	return bytes.NewReader(body)
}
