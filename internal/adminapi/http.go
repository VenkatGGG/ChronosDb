package adminapi

import (
	"encoding/json"
	"net/http"
)

// HTTPHandlerOptions wires optional live-stream behavior into the console API.
type HTTPHandlerOptions struct {
	Stream *EventStream
}

// NewHTTPHandler exposes a unified read-only cluster API for the console UI.
func NewHTTPHandler(aggregator *Aggregator) http.Handler {
	return NewHTTPHandlerWithOptions(aggregator, HTTPHandlerOptions{})
}

// NewHTTPHandlerWithOptions exposes the unified cluster API plus optional live endpoints.
func NewHTTPHandlerWithOptions(aggregator *Aggregator, opts HTTPHandlerOptions) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/cluster", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snapshot, err := aggregator.Snapshot(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSONResponse(w, snapshot)
	})
	mux.HandleFunc("/api/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snapshot, err := aggregator.Snapshot(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSONResponse(w, snapshot.Nodes)
	})
	mux.HandleFunc("/api/v1/ranges", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snapshot, err := aggregator.Snapshot(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSONResponse(w, snapshot.Ranges)
	})
	mux.HandleFunc("/api/v1/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snapshot, err := aggregator.Snapshot(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSONResponse(w, snapshot.Events)
	})
	if opts.Stream != nil {
		mux.Handle("/api/v1/events/stream", StreamHTTPHandler(opts.Stream))
	}
	return mux
}

func writeJSONResponse(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
