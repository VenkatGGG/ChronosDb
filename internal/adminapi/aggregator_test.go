package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAggregatorSnapshotMergesNodesRangesAndEvents(t *testing.T) {
	t.Parallel()

	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/snapshot" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			GeneratedAt: time.Unix(10, 0).UTC(),
			Nodes: []NodeView{{
				NodeID:           1,
				ObservabilityURL: "http://node-1",
				Status:           "ok",
				ReplicaCount:     1,
				LeaseCount:       1,
			}},
			Ranges: []RangeView{{
				RangeID:              11,
				Generation:           4,
				StartKey:             "61",
				EndKey:               "6d",
				Replicas:             []ReplicaView{{ReplicaID: 1, NodeID: 1, Role: "voter"}},
				LeaseholderReplicaID: 1,
				LeaseholderNodeID:    1,
				Source:               "local_node_state",
			}},
			Events: []ClusterEvent{{
				Timestamp: time.Unix(11, 0).UTC(),
				Type:      "node_started",
				NodeID:    1,
				Message:   "node 1 started",
			}},
		})
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/snapshot" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			GeneratedAt: time.Unix(12, 0).UTC(),
			Nodes: []NodeView{{
				NodeID:           2,
				ObservabilityURL: "http://node-2",
				Status:           "degraded",
				ReplicaCount:     2,
				LeaseCount:       0,
			}},
			Ranges: []RangeView{
				{
					RangeID:              11,
					Generation:           4,
					StartKey:             "61",
					EndKey:               "6d",
					Replicas:             []ReplicaView{{ReplicaID: 2, NodeID: 2, Role: "voter"}},
					LeaseholderReplicaID: 1,
					LeaseholderNodeID:    1,
					Source:               "local_node_state",
				},
				{
					RangeID:    12,
					Generation: 1,
					StartKey:   "6d",
					EndKey:     "7a",
					Replicas:   []ReplicaView{{ReplicaID: 3, NodeID: 2, Role: "learner"}},
					Source:     "local_node_state",
				},
			},
			Events: []ClusterEvent{{
				Timestamp: time.Unix(13, 0).UTC(),
				Type:      "partition_applied",
				NodeID:    2,
				Message:   "partition applied",
			}},
		})
	}))
	defer server2.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{
			{NodeID: 1, BaseURL: server1.URL},
			{NodeID: 2, BaseURL: server2.URL},
		},
		Client:            &http.Client{Timeout: time.Second},
		EventLimitPerNode: 8,
		Now:               func() time.Time { return time.Unix(20, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}

	snapshot, err := aggregator.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot.Nodes) != 2 {
		t.Fatalf("node count = %d, want 2", len(snapshot.Nodes))
	}
	if len(snapshot.Ranges) != 2 {
		t.Fatalf("range count = %d, want 2", len(snapshot.Ranges))
	}
	if len(snapshot.Events) != 2 {
		t.Fatalf("event count = %d, want 2", len(snapshot.Events))
	}
	if snapshot.Ranges[0].RangeID != 11 || len(snapshot.Ranges[0].Replicas) != 2 {
		t.Fatalf("merged range[0] = %+v, want range 11 with 2 replicas", snapshot.Ranges[0])
	}
	if snapshot.Events[0].Type != "node_started" || snapshot.Events[1].Type != "partition_applied" {
		t.Fatalf("events = %+v, want ordered merged events", snapshot.Events)
	}
}

func TestAggregatorRejectsNodeIdentityMismatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			Nodes: []NodeView{{NodeID: 9, Status: "ok"}},
		})
	}))
	defer server.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{{NodeID: 1, BaseURL: server.URL}},
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}
	if _, err := aggregator.Snapshot(context.Background()); err == nil {
		t.Fatal("expected snapshot identity mismatch error")
	}
}

func TestAggregatorLocateKey(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			Nodes: []NodeView{{NodeID: 1, Status: "ok"}},
			Ranges: []RangeView{
				{
					RangeID:              11,
					Generation:           3,
					StartKey:             "61",
					EndKey:               "6d",
					Replicas:             []ReplicaView{{ReplicaID: 1, NodeID: 1, Role: "voter"}},
					LeaseholderReplicaID: 1,
					LeaseholderNodeID:    1,
				},
				{
					RangeID:              12,
					Generation:           4,
					StartKey:             "6d",
					EndKey:               "",
					Replicas:             []ReplicaView{{ReplicaID: 2, NodeID: 1, Role: "voter"}},
					LeaseholderReplicaID: 2,
					LeaseholderNodeID:    1,
				},
			},
		})
	}))
	defer server.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{{NodeID: 1, BaseURL: server.URL}},
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}

	location, err := aggregator.LocateKey(context.Background(), "b")
	if err != nil {
		t.Fatalf("locate utf8 key: %v", err)
	}
	if location.Range.RangeID != 11 || location.Encoding != "utf8" || location.Key != "62" {
		t.Fatalf("location = %+v, want range 11 utf8 key 62", location)
	}

	location, err = aggregator.LocateKey(context.Background(), "hex:7a")
	if err != nil {
		t.Fatalf("locate hex key: %v", err)
	}
	if location.Range.RangeID != 12 || location.Encoding != "hex" || location.Key != "7a" {
		t.Fatalf("location = %+v, want range 12 hex key 7a", location)
	}
}

func TestAggregatorLocateKeyErrors(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			Nodes: []NodeView{{NodeID: 1, Status: "ok"}},
			Ranges: []RangeView{{
				RangeID:    11,
				Generation: 1,
				StartKey:   "61",
				EndKey:     "62",
				Replicas:   []ReplicaView{{ReplicaID: 1, NodeID: 1, Role: "voter"}},
			}},
		})
	}))
	defer server.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{{NodeID: 1, BaseURL: server.URL}},
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}

	if _, err := aggregator.LocateKey(context.Background(), "hex:not-hex"); err == nil {
		t.Fatal("expected invalid hex lookup error")
	}
	if _, err := aggregator.LocateKey(context.Background(), "z"); !errors.Is(err, ErrKeyNotLocated) {
		t.Fatalf("err = %v, want ErrKeyNotLocated", err)
	}
}

func TestAggregatorTopologyAndDetails(t *testing.T) {
	t.Parallel()

	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			Nodes: []NodeView{{
				NodeID:       1,
				Status:       "ok",
				ReplicaCount: 2,
				LeaseCount:   1,
			}},
			Ranges: []RangeView{
				{
					RangeID:              11,
					Generation:           4,
					StartKey:             "61",
					EndKey:               "6d",
					Replicas:             []ReplicaView{{ReplicaID: 1, NodeID: 1, Role: "voter"}},
					LeaseholderReplicaID: 1,
					LeaseholderNodeID:    1,
					PlacementMode:        "REGIONAL",
				},
				{
					RangeID:              12,
					Generation:           2,
					StartKey:             "6d",
					EndKey:               "",
					Replicas:             []ReplicaView{{ReplicaID: 3, NodeID: 1, Role: "voter"}},
					LeaseholderReplicaID: 3,
					LeaseholderNodeID:    1,
				},
			},
			Events: []ClusterEvent{
				{
					Timestamp: time.Unix(100, 0).UTC(),
					Type:      "lease_transfer",
					NodeID:    1,
					RangeID:   11,
					Message:   "lease moved to node 1",
				},
				{
					Timestamp: time.Unix(101, 0).UTC(),
					Type:      "node_heartbeat",
					NodeID:    1,
					Message:   "node 1 healthy",
				},
			},
		})
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			Nodes: []NodeView{{
				NodeID:       2,
				Status:       "ok",
				ReplicaCount: 1,
				LeaseCount:   0,
			}},
			Ranges: []RangeView{{
				RangeID:              11,
				Generation:           4,
				StartKey:             "61",
				EndKey:               "6d",
				Replicas:             []ReplicaView{{ReplicaID: 2, NodeID: 2, Role: "voter"}},
				LeaseholderReplicaID: 1,
				LeaseholderNodeID:    1,
				PlacementMode:        "REGIONAL",
			}},
			Events: []ClusterEvent{{
				Timestamp: time.Unix(102, 0).UTC(),
				Type:      "replica_added",
				NodeID:    2,
				RangeID:   11,
				Message:   "replica added on node 2",
			}},
		})
	}))
	defer server2.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{
			{NodeID: 1, BaseURL: server1.URL},
			{NodeID: 2, BaseURL: server2.URL},
		},
		Now: func() time.Time { return time.Unix(200, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}

	topology, err := aggregator.Topology(context.Background())
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	if len(topology.Edges) != 3 {
		t.Fatalf("edge count = %d, want 3", len(topology.Edges))
	}
	if !topology.Edges[0].Leaseholder {
		t.Fatalf("first edge = %+v, want leaseholder edge", topology.Edges[0])
	}

	nodeDetail, err := aggregator.NodeDetail(context.Background(), 1)
	if err != nil {
		t.Fatalf("node detail: %v", err)
	}
	if len(nodeDetail.HostedRanges) != 2 {
		t.Fatalf("hosted range count = %d, want 2", len(nodeDetail.HostedRanges))
	}
	if len(nodeDetail.RecentEvents) != 3 {
		t.Fatalf("recent event count = %d, want 3", len(nodeDetail.RecentEvents))
	}
	if nodeDetail.RecentEvents[0].Timestamp.Before(nodeDetail.RecentEvents[1].Timestamp) {
		t.Fatalf("recent events not sorted newest-first: %+v", nodeDetail.RecentEvents)
	}

	rangeDetail, err := aggregator.RangeDetail(context.Background(), 11)
	if err != nil {
		t.Fatalf("range detail: %v", err)
	}
	if len(rangeDetail.ReplicaNodes) != 2 {
		t.Fatalf("replica node count = %d, want 2", len(rangeDetail.ReplicaNodes))
	}
	if rangeDetail.ReplicaNodes[0].Node == nil || rangeDetail.ReplicaNodes[1].Node == nil {
		t.Fatalf("replica nodes = %+v, want resolved node surfaces", rangeDetail.ReplicaNodes)
	}
	if len(rangeDetail.RecentEvents) != 2 {
		t.Fatalf("range event count = %d, want 2", len(rangeDetail.RecentEvents))
	}

	if _, err := aggregator.NodeDetail(context.Background(), 9); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("node detail err = %v, want ErrNodeNotFound", err)
	}
	if _, err := aggregator.RangeDetail(context.Background(), 99); !errors.Is(err, ErrRangeNotFound) {
		t.Fatalf("range detail err = %v, want ErrRangeNotFound", err)
	}
}

func TestAggregatorCorrelateScenarioDetail(t *testing.T) {
	t.Parallel()

	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			GeneratedAt: time.Unix(300, 0).UTC(),
			Nodes:       []NodeView{{NodeID: 1, Status: "ok", ReplicaCount: 2, LeaseCount: 1}},
			Ranges: []RangeView{{
				RangeID:              11,
				Generation:           4,
				StartKey:             "61",
				EndKey:               "6d",
				Replicas:             []ReplicaView{{ReplicaID: 1, NodeID: 1, Role: "voter"}},
				LeaseholderReplicaID: 1,
				LeaseholderNodeID:    1,
			}},
		})
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			GeneratedAt: time.Unix(300, 0).UTC(),
			Nodes:       []NodeView{{NodeID: 2, Status: "ok", ReplicaCount: 1, LeaseCount: 0}},
			Ranges: []RangeView{{
				RangeID:              12,
				Generation:           4,
				StartKey:             "6d",
				EndKey:               "",
				Replicas:             []ReplicaView{{ReplicaID: 2, NodeID: 2, Role: "voter"}},
				LeaseholderReplicaID: 2,
				LeaseholderNodeID:    2,
			}},
		})
	}))
	defer server2.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{
			{NodeID: 1, BaseURL: server1.URL},
			{NodeID: 2, BaseURL: server2.URL},
		},
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}

	correlation, err := aggregator.CorrelateScenarioDetail(context.Background(), ScenarioRunDetail{
		Manifest: ScenarioManifest{
			Scenario: "minority-partition",
			Nodes:    []uint64{1, 3},
		},
	})
	if err != nil {
		t.Fatalf("correlate scenario detail: %v", err)
	}
	if correlation.Source != "manifest_nodes_current_topology" {
		t.Fatalf("correlation source = %q", correlation.Source)
	}
	if len(correlation.Nodes) != 1 || correlation.Nodes[0].NodeID != 1 {
		t.Fatalf("correlation nodes = %+v, want node 1 only", correlation.Nodes)
	}
	if len(correlation.MissingNodeIDs) != 1 || correlation.MissingNodeIDs[0] != 3 {
		t.Fatalf("missing nodes = %+v, want [3]", correlation.MissingNodeIDs)
	}
	if len(correlation.Ranges) != 1 || correlation.Ranges[0].RangeID != 11 {
		t.Fatalf("correlation ranges = %+v, want range 11", correlation.Ranges)
	}
}
