package observability

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlerServesMetricsHealthReadyOverviewAndPprof(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	metrics.ObserveRangeCacheLookup(false)
	metrics.ObserveSplit("success", 2*time.Second)
	metrics.SetSnapshotPressure("store-1", "send", 3)
	metrics.SetPebbleCompactionPressure("store-1", "critical", 0.9)
	metrics.SetAllocatorRebalanceScore("range-7", true, 0.84)
	metrics.ObserveAllocatorDecision("rebalance", true)

	handler := NewHandler(HandlerOptions{
		Metrics: metrics,
		Health: func(context.Context) error {
			return nil
		},
		Ready: func(context.Context) error {
			return nil
		},
		Overview: func(context.Context) (Overview, error) {
			return Overview{
				Status: "degraded",
				Components: map[string]string{
					"allocator": "ok",
					"storage":   "compaction_pressure",
				},
				Notes: []string{"background compaction elevated"},
			}, nil
		},
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	mustStatus(t, server.URL+"/healthz", http.StatusOK)
	mustStatus(t, server.URL+"/readyz", http.StatusOK)

	metricsBody := mustGetBody(t, server.URL+"/metrics")
	for _, want := range []string{
		"chronosdb_range_cache_lookups_total",
		"chronosdb_split_duration_seconds",
		"chronosdb_snapshot_pressure",
		"chronosdb_pebble_compaction_pressure",
		"chronosdb_allocator_rebalance_score",
		"chronosdb_allocator_decisions_total",
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics output missing %q", want)
		}
	}

	pprofBody := mustGetBody(t, server.URL+"/debug/pprof/")
	if !strings.Contains(pprofBody, "profile") {
		t.Fatalf("pprof output missing profile listing")
	}

	resp, err := http.Get(server.URL + "/debug/chronos/overview")
	if err != nil {
		t.Fatalf("get overview: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("overview status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var overview Overview
	if err := json.NewDecoder(resp.Body).Decode(&overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if overview.Status != "degraded" {
		t.Fatalf("overview status = %q, want %q", overview.Status, "degraded")
	}
	if got := overview.Components["storage"]; got != "compaction_pressure" {
		t.Fatalf("overview storage component = %q, want %q", got, "compaction_pressure")
	}
}

func TestHandlerPropagatesUnhealthyAndNotReady(t *testing.T) {
	t.Parallel()

	unhealthy := errors.New("node unhealthy")
	notReady := errors.New("warming snapshots")
	handler := NewHandler(HandlerOptions{
		Health: func(context.Context) error {
			return unhealthy
		},
		Ready: func(context.Context) error {
			return notReady
		},
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("get healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("healthz status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read healthz body: %v", err)
	}
	if !strings.Contains(string(body), unhealthy.Error()) {
		t.Fatalf("healthz body = %q, want substring %q", string(body), unhealthy.Error())
	}

	resp, err = http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatalf("get readyz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read readyz body: %v", err)
	}
	if !strings.Contains(string(body), notReady.Error()) {
		t.Fatalf("readyz body = %q, want substring %q", string(body), notReady.Error())
	}
}

func mustStatus(t *testing.T, url string, want int) {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("%s status = %d, want %d", url, resp.StatusCode, want)
	}
}

func mustGetBody(t *testing.T, url string) string {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status = %d, want %d", url, resp.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return string(body)
}
