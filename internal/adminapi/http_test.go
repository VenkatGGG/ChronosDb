package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPHandlerServesMergedClusterAPI(t *testing.T) {
	t.Parallel()

	nodeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/snapshot" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(ClusterSnapshot{
			GeneratedAt: time.Unix(50, 0).UTC(),
			Nodes: []NodeView{{
				NodeID:           1,
				ObservabilityURL: "http://node-1",
				Status:           "ok",
			}},
			Ranges: []RangeView{{
				RangeID:    11,
				Generation: 2,
				StartKey:   "61",
				EndKey:     "6d",
				Replicas:   []ReplicaView{{ReplicaID: 1, NodeID: 1, Role: "voter"}},
			}},
			Events: []ClusterEvent{{
				Timestamp: time.Unix(51, 0).UTC(),
				Type:      "node_started",
				NodeID:    1,
				Message:   "node started",
			}},
		})
	}))
	defer nodeServer.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{{NodeID: 1, BaseURL: nodeServer.URL}},
		Now:     func() time.Time { return time.Unix(60, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}
	handler := NewHTTPHandler(aggregator)

	t.Run("cluster", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("cluster status = %d", rec.Code)
		}
		var snapshot ClusterSnapshot
		if err := json.NewDecoder(rec.Body).Decode(&snapshot); err != nil {
			t.Fatalf("decode cluster: %v", err)
		}
		if len(snapshot.Nodes) != 1 || len(snapshot.Ranges) != 1 || len(snapshot.Events) != 1 {
			t.Fatalf("cluster snapshot = %+v", snapshot)
		}
	})

	t.Run("nodes", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/nodes", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("nodes status = %d", rec.Code)
		}
		var nodes []NodeView
		if err := json.NewDecoder(rec.Body).Decode(&nodes); err != nil {
			t.Fatalf("decode nodes: %v", err)
		}
		if len(nodes) != 1 || nodes[0].NodeID != 1 {
			t.Fatalf("nodes = %+v", nodes)
		}
	})

	t.Run("ranges", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/ranges", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("ranges status = %d", rec.Code)
		}
		var ranges []RangeView
		if err := json.NewDecoder(rec.Body).Decode(&ranges); err != nil {
			t.Fatalf("decode ranges: %v", err)
		}
		if len(ranges) != 1 || ranges[0].RangeID != 11 {
			t.Fatalf("ranges = %+v", ranges)
		}
	})

	t.Run("events", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("events status = %d", rec.Code)
		}
		var events []ClusterEvent
		if err := json.NewDecoder(rec.Body).Decode(&events); err != nil {
			t.Fatalf("decode events: %v", err)
		}
		if len(events) != 1 || events[0].Type != "node_started" {
			t.Fatalf("events = %+v", events)
		}
	})
}
