package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// EventStreamConfig configures the cluster event polling and replay pipeline.
type EventStreamConfig struct {
	Aggregator        *Aggregator
	PollInterval      time.Duration
	ReplayLimit       int
	MaxSeenEventIDs   int
	HeartbeatInterval time.Duration
	Now               func() time.Time
}

// EventStream polls cluster snapshots, deduplicates events, and serves replay/subscription state.
type EventStream struct {
	aggregator        *Aggregator
	pollInterval      time.Duration
	replayLimit       int
	maxSeenEventIDs   int
	heartbeatInterval time.Duration
	now               func() time.Time

	mu          sync.RWMutex
	recent      []ClusterEvent
	seen        map[string]struct{}
	seenOrder   []string
	subscribers map[uint64]chan ClusterEvent
	nextSubID   uint64
}

// NewEventStream constructs the event replay and subscription pipeline.
func NewEventStream(cfg EventStreamConfig) (*EventStream, error) {
	if cfg.Aggregator == nil {
		return nil, fmt.Errorf("adminapi: event stream requires an aggregator")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.ReplayLimit <= 0 {
		cfg.ReplayLimit = 256
	}
	if cfg.MaxSeenEventIDs <= 0 {
		cfg.MaxSeenEventIDs = cfg.ReplayLimit * 8
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 15 * time.Second
	}
	now := cfg.Now
	if now == nil {
		now = time.Now().UTC
	}
	return &EventStream{
		aggregator:        cfg.Aggregator,
		pollInterval:      cfg.PollInterval,
		replayLimit:       cfg.ReplayLimit,
		maxSeenEventIDs:   cfg.MaxSeenEventIDs,
		heartbeatInterval: cfg.HeartbeatInterval,
		now:               now,
		recent:            make([]ClusterEvent, 0, cfg.ReplayLimit),
		seen:              make(map[string]struct{}, cfg.MaxSeenEventIDs),
		seenOrder:         make([]string, 0, cfg.MaxSeenEventIDs),
		subscribers:       make(map[uint64]chan ClusterEvent),
	}, nil
}

// Start begins background polling until the context is canceled.
func (s *EventStream) Start(ctx context.Context) error {
	if err := s.PollOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.PollOnce(ctx); err != nil {
				return err
			}
		}
	}
}

// PollOnce refreshes the cluster snapshot and ingests any newly observed events.
func (s *EventStream) PollOnce(ctx context.Context) error {
	snapshot, err := s.aggregator.Snapshot(ctx)
	if err != nil {
		return err
	}
	s.ingest(snapshot.Events)
	return nil
}

// Recent returns the most recent replayable cluster events.
func (s *EventStream) Recent(limit int) []ClusterEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 || limit >= len(s.recent) {
		return append([]ClusterEvent(nil), s.recent...)
	}
	return append([]ClusterEvent(nil), s.recent[len(s.recent)-limit:]...)
}

// Subscribe registers one live subscriber and returns replayed events plus a live channel.
func (s *EventStream) Subscribe(ctx context.Context, replay int) ([]ClusterEvent, <-chan ClusterEvent) {
	recent := s.Recent(replay)

	s.mu.Lock()
	id := s.nextSubID
	s.nextSubID++
	ch := make(chan ClusterEvent, 64)
	s.subscribers[id] = ch
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if stream, ok := s.subscribers[id]; ok {
			delete(s.subscribers, id)
			close(stream)
		}
		s.mu.Unlock()
	}()
	return recent, ch
}

// HeartbeatInterval reports how often SSE streams should emit keep-alives.
func (s *EventStream) HeartbeatInterval() time.Duration {
	return s.heartbeatInterval
}

func (s *EventStream) ingest(events []ClusterEvent) {
	normalized := make([]ClusterEvent, 0, len(events))
	for _, event := range events {
		normalized = append(normalized, NormalizeEvent(event))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range normalized {
		if _, seen := s.seen[event.ID]; seen {
			continue
		}
		s.seen[event.ID] = struct{}{}
		s.seenOrder = append(s.seenOrder, event.ID)
		s.recent = append(s.recent, event)
		if len(s.recent) > s.replayLimit {
			s.recent = append([]ClusterEvent(nil), s.recent[len(s.recent)-s.replayLimit:]...)
		}
		if len(s.seenOrder) > s.maxSeenEventIDs {
			evict := s.seenOrder[0]
			s.seenOrder = s.seenOrder[1:]
			delete(s.seen, evict)
		}
		for _, subscriber := range s.subscribers {
			select {
			case subscriber <- event:
			default:
			}
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, event ClusterEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %s\n", event.ID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\n", event.Type); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return err
	}
	return nil
}

func writeSSEComment(w http.ResponseWriter, comment string) error {
	_, err := fmt.Fprintf(w, ": %s\n\n", comment)
	return err
}

// StreamHTTPHandler serves a live SSE feed backed by the event stream.
func StreamHTTPHandler(stream *EventStream) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		replay, ch := stream.Subscribe(r.Context(), parsePositiveInt(r.URL.Query().Get("replay")))
		for _, event := range replay {
			if err := writeSSEEvent(w, event); err != nil {
				return
			}
		}
		flusher.Flush()

		heartbeat := time.NewTicker(stream.HeartbeatInterval())
		defer heartbeat.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				if err := writeSSEEvent(w, event); err != nil {
					return
				}
				flusher.Flush()
			case <-heartbeat.C:
				if err := writeSSEComment(w, "keepalive"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

func parsePositiveInt(raw string) int {
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0
	}
	return value
}
