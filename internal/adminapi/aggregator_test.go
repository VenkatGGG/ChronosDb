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
