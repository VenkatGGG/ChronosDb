package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/httpauth"
)

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:8080", "console listen address")
	nodeURLs := flag.String("node-observability-urls", "", "comma-separated node observability base URLs")
	eventLimit := flag.Int("event-limit-per-node", 64, "recent events to request from each node")
	streamPollInterval := flag.Duration("event-poll-interval", time.Second, "cluster event polling interval")
	streamReplayLimit := flag.Int("event-replay-limit", 256, "recent events retained for SSE replay")
	uiDir := flag.String("ui-dir", "", "optional built UI directory to serve alongside the console API")
	artifactRoot := flag.String("artifact-root", "", "optional retained chaos artifact root to expose via the console API")
	apiBearerToken := flag.String("api-bearer-token", "", "optional bearer token required for console API access")
	flag.Parse()

	rawTargets := splitAndTrim(*nodeURLs)
	if len(rawTargets) == 0 {
		log.Fatal("at least one -node-observability-urls entry is required")
	}

	targets := make([]adminapi.NodeTarget, 0, len(rawTargets))
	for _, baseURL := range rawTargets {
		targets = append(targets, adminapi.NodeTarget{BaseURL: baseURL})
	}
	aggregator, err := adminapi.NewAggregator(adminapi.AggregatorConfig{
		Targets:           targets,
		EventLimitPerNode: *eventLimit,
	})
	if err != nil {
		log.Fatal(err)
	}
	stream, err := adminapi.NewEventStream(adminapi.EventStreamConfig{
		Aggregator:   aggregator,
		PollInterval: *streamPollInterval,
		ReplayLimit:  *streamReplayLimit,
	})
	if err != nil {
		log.Fatal(err)
	}
	var scenarios *scenarioStore
	if *artifactRoot != "" {
		scenarios, err = newScenarioStore(*artifactRoot)
		if err != nil {
			log.Fatal(err)
		}
	}

	apiHandler := adminapi.NewHTTPHandlerWithOptions(aggregator, adminapi.HTTPHandlerOptions{
		Stream:    stream,
		Scenarios: scenarios,
	})
	apiHandler = httpauth.Policy{
		BearerToken:  *apiBearerToken,
		LoopbackOnly: true,
		Realm:        "chronos-console-api",
	}.Wrap(apiHandler)
	handler, err := buildHandler(apiHandler, *uiDir)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	server := &http.Server{
		Addr:    *listenAddr,
		Handler: handler,
	}
	go func() {
		if err := stream.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("chronos console event stream stopped: %v", err)
		}
	}()
	log.Printf("chronos console listening on %s", *listenAddr)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func splitAndTrim(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func buildHandler(api http.Handler, uiDir string) (http.Handler, error) {
	if uiDir == "" {
		return api, nil
	}
	indexPath := filepath.Join(uiDir, "index.html")
	info, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, os.ErrNotExist
	}

	fileServer := http.FileServer(http.Dir(uiDir))
	mux := http.NewServeMux()
	mux.Handle("/api/", api)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			api.ServeHTTP(w, r)
			return
		}
		cleaned := filepath.Clean(strings.TrimPrefix(r.URL.Path, "/"))
		if cleaned == "." || cleaned == "/" {
			http.ServeFile(w, r, indexPath)
			return
		}
		if strings.HasPrefix(cleaned, "..") {
			http.ServeFile(w, r, indexPath)
			return
		}
		target := filepath.Join(uiDir, cleaned)
		if stat, err := os.Stat(target); err == nil && !stat.IsDir() {
			fileServer.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, indexPath)
	})
	return mux, nil
}
