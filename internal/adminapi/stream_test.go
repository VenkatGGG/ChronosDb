package adminapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEventStreamPollOnceDeduplicatesAndReplays(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.RWMutex
		snapshot = ClusterSnapshot{
			Nodes: []NodeView{{NodeID: 1, Status: "ok"}},
			Events: []ClusterEvent{{
				Timestamp: time.Unix(10, 0).UTC(),
				Type:      "node_started",
				NodeID:    1,
				Message:   "node started",
			}},
		}
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		_ = json.NewEncoder(w).Encode(snapshot)
	}))
	defer server.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{{NodeID: 1, BaseURL: server.URL}},
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}
	stream, err := NewEventStream(EventStreamConfig{
		Aggregator:      aggregator,
		ReplayLimit:     8,
		MaxSeenEventIDs: 32,
	})
	if err != nil {
		t.Fatalf("new event stream: %v", err)
	}

	if err := stream.PollOnce(context.Background()); err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if err := stream.PollOnce(context.Background()); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	recent := stream.Recent(0)
	if len(recent) != 1 {
		t.Fatalf("recent events after duplicate polls = %+v, want 1", recent)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replay, live := stream.Subscribe(ctx, 4)
	if len(replay) != 1 || replay[0].Type != "node_started" {
		t.Fatalf("replay = %+v, want single node_started event", replay)
	}

	mu.Lock()
	snapshot.Events = append(snapshot.Events, ClusterEvent{
		Timestamp: time.Unix(11, 0).UTC(),
		Type:      "partition_applied",
		NodeID:    1,
		Message:   "partition applied",
	})
	mu.Unlock()
	if err := stream.PollOnce(context.Background()); err != nil {
		t.Fatalf("third poll: %v", err)
	}

	select {
	case event := <-live:
		if event.Type != "partition_applied" {
			t.Fatalf("live event = %+v, want partition_applied", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live event")
	}
}

func TestStreamHTTPHandlerReplaysAndStreams(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.RWMutex
		snapshot = ClusterSnapshot{
			Nodes: []NodeView{{NodeID: 1, Status: "ok"}},
			Events: []ClusterEvent{{
				Timestamp: time.Unix(20, 0).UTC(),
				Type:      "node_started",
				NodeID:    1,
				Message:   "node started",
			}},
		}
	)
	nodeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		_ = json.NewEncoder(w).Encode(snapshot)
	}))
	defer nodeServer.Close()

	aggregator, err := NewAggregator(AggregatorConfig{
		Targets: []NodeTarget{{NodeID: 1, BaseURL: nodeServer.URL}},
	})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}
	stream, err := NewEventStream(EventStreamConfig{
		Aggregator:        aggregator,
		ReplayLimit:       8,
		MaxSeenEventIDs:   32,
		HeartbeatInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("new event stream: %v", err)
	}
	if err := stream.PollOnce(context.Background()); err != nil {
		t.Fatalf("initial poll: %v", err)
	}

	console := httptest.NewServer(NewHTTPHandlerWithOptions(aggregator, HTTPHandlerOptions{
		Stream: stream,
	}))
	defer console.Close()

	req, err := http.NewRequest(http.MethodGet, console.URL+"/api/v1/events/stream?replay=1", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open sse stream: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("content type = %q, want text/event-stream", got)
	}
	reader := bufio.NewReader(resp.Body)

	replayed := readSSEEvent(t, reader)
	if replayed.Type != "node_started" {
		t.Fatalf("replayed event = %+v, want node_started", replayed)
	}

	mu.Lock()
	snapshot.Events = append(snapshot.Events, ClusterEvent{
		Timestamp: time.Unix(21, 0).UTC(),
		Type:      "partition_healed",
		NodeID:    1,
		Message:   "partition healed",
	})
	mu.Unlock()
	if err := stream.PollOnce(context.Background()); err != nil {
		t.Fatalf("poll new event: %v", err)
	}

	live := readSSEEvent(t, reader)
	if live.Type != "partition_healed" {
		t.Fatalf("live event = %+v, want partition_healed", live)
	}
}

func readSSEEvent(t *testing.T, reader *bufio.Reader) ClusterEvent {
	t.Helper()

	var event ClusterEvent
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read sse line: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "id: "):
			event.ID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
				t.Fatalf("decode sse data: %v", err)
			}
		case line == "":
			if event.Type != "" || event.ID != "" {
				return event
			}
		}
		if strings.HasPrefix(line, "event: ") {
			event.Type = strings.TrimPrefix(line, "event: ")
		}
	}
	t.Fatal("timed out reading sse event")
	return ClusterEvent{}
}
