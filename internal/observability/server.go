package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Probe checks one local liveness/readiness condition.
type Probe func(context.Context) error

// Overview summarizes operator-facing state for a single node or process.
type Overview struct {
	Status      string            `json:"status"`
	GeneratedAt time.Time         `json:"generated_at"`
	Components  map[string]string `json:"components,omitempty"`
	Notes       []string          `json:"notes,omitempty"`
}

// OverviewProvider returns a summarized operator view.
type OverviewProvider func(context.Context) (Overview, error)

// HandlerOptions controls the observability HTTP surface.
type HandlerOptions struct {
	Metrics  *Metrics
	Registry *prometheus.Registry
	Health   Probe
	Ready    Probe
	Overview OverviewProvider
}

// NewHandler builds the ChronosDB observability HTTP surface.
func NewHandler(opts HandlerOptions) http.Handler {
	registry := opts.Registry
	if opts.Metrics != nil && opts.Metrics.Registry() != nil {
		registry = opts.Metrics.Registry()
	}
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", probeHandler(opts.Health))
	mux.HandleFunc("/readyz", probeHandler(opts.Ready))
	mux.HandleFunc("/debug/chronos/overview", overviewHandler(opts.Overview))

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return mux
}

func probeHandler(probe Probe) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if probe == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		if err := probe(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

func overviewHandler(provider OverviewProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		overview := Overview{
			Status:      "ok",
			GeneratedAt: time.Now().UTC(),
		}
		if provider != nil {
			next, err := provider(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if next.GeneratedAt.IsZero() {
				next.GeneratedAt = time.Now().UTC()
			}
			overview = next
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(overview)
	}
}
