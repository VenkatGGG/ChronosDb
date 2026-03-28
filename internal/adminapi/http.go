package adminapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// HTTPHandlerOptions wires optional live-stream behavior into the console API.
type HTTPHandlerOptions struct {
	Stream    *EventStream
	Scenarios ScenarioReader
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
	mux.HandleFunc("/api/v1/locate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		location, err := aggregator.LocateKey(r.Context(), r.URL.Query().Get("key"))
		if err != nil {
			status := http.StatusBadGateway
			switch {
			case errors.Is(err, ErrKeyNotLocated):
				status = http.StatusNotFound
			default:
				if isLookupInputError(err) {
					status = http.StatusBadRequest
				}
			}
			http.Error(w, err.Error(), status)
			return
		}
		writeJSONResponse(w, location)
	})
	if opts.Stream != nil {
		mux.Handle("/api/v1/events/stream", StreamHTTPHandler(opts.Stream))
	}
	if opts.Scenarios != nil {
		mux.HandleFunc("/api/v1/scenarios", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			runs, err := opts.Scenarios.ListRuns()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			writeJSONResponse(w, runs)
		})
		mux.HandleFunc("/api/v1/scenarios/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			runID := strings.TrimPrefix(r.URL.Path, "/api/v1/scenarios/")
			if runID == "" || strings.Contains(runID, "/") {
				http.Error(w, "scenario run not found", http.StatusNotFound)
				return
			}
			detail, err := opts.Scenarios.LoadRun(runID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			writeJSONResponse(w, detail)
		})
	}
	return mux
}

func writeJSONResponse(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func isLookupInputError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.HasPrefix(message, "adminapi: lookup key") || strings.HasPrefix(message, "adminapi: invalid hex lookup key")
}
