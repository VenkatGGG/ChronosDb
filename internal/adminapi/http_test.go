package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type stubScenarioReader struct {
	runs   []ScenarioRunView
	detail ScenarioRunDetail
	err    error
}

func (s stubScenarioReader) ListRuns() ([]ScenarioRunView, error) {
	return append([]ScenarioRunView(nil), s.runs...), nil
}

func (s stubScenarioReader) LoadRun(string) (ScenarioRunDetail, error) {
	if s.err != nil {
		return ScenarioRunDetail{}, s.err
	}
	return s.detail, nil
}

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
	handler := NewHTTPHandlerWithOptions(aggregator, HTTPHandlerOptions{Scenarios: stubScenarioReader{
		runs: []ScenarioRunView{{
			RunID:        "minority-partition",
			ScenarioName: "minority-partition",
			Status:       "pass",
			StartedAt:    time.Unix(10, 0).UTC(),
			FinishedAt:   time.Unix(11, 0).UTC(),
			StepCount:    3,
			NodeCount:    3,
			NodeLogCount: 4,
		}},
		detail: ScenarioRunDetail{
			Run: ScenarioRunView{
				RunID:        "minority-partition",
				ScenarioName: "minority-partition",
				Status:       "pass",
				StartedAt:    time.Unix(10, 0).UTC(),
				FinishedAt:   time.Unix(11, 0).UTC(),
				StepCount:    3,
				NodeCount:    3,
				NodeLogCount: 4,
			},
			Manifest: ScenarioManifest{
				Version:  "chronosdb.systemtest.v1",
				Scenario: "minority-partition",
				Nodes:    []uint64{1, 2, 3},
				Steps:    []ScenarioManifestStep{{Index: 1, Action: "wait", Duration: "10ms"}},
			},
			Handoff: &ScenarioHandoffBundle{
				Version: "chronosdb.systemtest.handoff.v1",
				Manifest: ScenarioManifest{
					Version:  "chronosdb.systemtest.v1",
					Scenario: "minority-partition",
					Nodes:    []uint64{1, 2, 3},
				},
			},
			Report: ScenarioRunReport{
				ScenarioName: "minority-partition",
				StartedAt:    time.Unix(10, 0).UTC(),
				FinishedAt:   time.Unix(11, 0).UTC(),
			},
			Summary: ScenarioRunSummary{
				Version:      "chronosdb.systemtest.artifacts.v1",
				ScenarioName: "minority-partition",
				Status:       "pass",
				StartedAt:    time.Unix(10, 0).UTC(),
				FinishedAt:   time.Unix(11, 0).UTC(),
				StepCount:    3,
				NodeCount:    3,
				NodeLogCount: 4,
			},
			NodeLogs: map[uint64][]ScenarioNodeLogEntry{
				1: {{Timestamp: time.Unix(10, 0).UTC(), Message: "node up"}},
			},
		},
	}})

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

	t.Run("node detail", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("node detail status = %d", rec.Code)
		}
		var detail NodeDetailView
		if err := json.NewDecoder(rec.Body).Decode(&detail); err != nil {
			t.Fatalf("decode node detail: %v", err)
		}
		if detail.Node.NodeID != 1 || len(detail.HostedRanges) != 1 {
			t.Fatalf("node detail = %+v", detail)
		}
	})

	t.Run("range detail", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/ranges/11", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("range detail status = %d", rec.Code)
		}
		var detail RangeDetailView
		if err := json.NewDecoder(rec.Body).Decode(&detail); err != nil {
			t.Fatalf("decode range detail: %v", err)
		}
		if detail.Range.RangeID != 11 || len(detail.ReplicaNodes) != 1 {
			t.Fatalf("range detail = %+v", detail)
		}
	})

	t.Run("topology", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("topology status = %d", rec.Code)
		}
		var topology ClusterTopologyView
		if err := json.NewDecoder(rec.Body).Decode(&topology); err != nil {
			t.Fatalf("decode topology: %v", err)
		}
		if len(topology.Nodes) != 1 || len(topology.Ranges) != 1 || len(topology.Edges) != 1 {
			t.Fatalf("topology = %+v", topology)
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

	t.Run("locate", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/locate?key=b", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("locate status = %d", rec.Code)
		}
		var location KeyLocationView
		if err := json.NewDecoder(rec.Body).Decode(&location); err != nil {
			t.Fatalf("decode locate: %v", err)
		}
		if location.Range.RangeID != 11 || location.Key != "62" || location.Encoding != "utf8" {
			t.Fatalf("location = %+v", location)
		}
	})

	t.Run("locate bad input", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/locate?key=hex:not-hex", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("locate bad input status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("node detail bad id", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/not-a-number", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("node detail bad id status = %d", rec.Code)
		}
	})

	t.Run("scenario list", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/scenarios", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("scenarios status = %d", rec.Code)
		}
		var runs []ScenarioRunView
		if err := json.NewDecoder(rec.Body).Decode(&runs); err != nil {
			t.Fatalf("decode scenarios: %v", err)
		}
		if len(runs) != 1 || runs[0].RunID != "minority-partition" {
			t.Fatalf("runs = %+v", runs)
		}
	})

	t.Run("scenario detail", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/scenarios/minority-partition", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("scenario detail status = %d", rec.Code)
		}
		var detail ScenarioRunDetail
		if err := json.NewDecoder(rec.Body).Decode(&detail); err != nil {
			t.Fatalf("decode scenario detail: %v", err)
		}
		if detail.Run.RunID != "minority-partition" || detail.Handoff == nil {
			t.Fatalf("detail = %+v", detail)
		}
		if detail.LiveCorrelation == nil || len(detail.LiveCorrelation.Nodes) != 1 || detail.LiveCorrelation.Nodes[0].NodeID != 1 {
			t.Fatalf("live correlation = %+v, want node 1 correlation", detail.LiveCorrelation)
		}
		if len(detail.LiveCorrelation.Ranges) != 1 || detail.LiveCorrelation.Ranges[0].RangeID != 11 {
			t.Fatalf("live correlation ranges = %+v, want range 11", detail.LiveCorrelation.Ranges)
		}
	})

	t.Run("scenario detail missing", func(t *testing.T) {
		missingHandler := NewHTTPHandlerWithOptions(aggregator, HTTPHandlerOptions{
			Scenarios: stubScenarioReader{err: ErrScenarioRunNotFound},
		})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/scenarios/missing-run", nil)
		missingHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("scenario detail missing status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}
