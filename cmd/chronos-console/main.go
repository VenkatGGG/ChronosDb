package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
)

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:8080", "console listen address")
	nodeURLs := flag.String("node-observability-urls", "", "comma-separated node observability base URLs")
	eventLimit := flag.Int("event-limit-per-node", 64, "recent events to request from each node")
	streamPollInterval := flag.Duration("event-poll-interval", time.Second, "cluster event polling interval")
	streamReplayLimit := flag.Int("event-replay-limit", 256, "recent events retained for SSE replay")
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	server := &http.Server{
		Addr: *listenAddr,
		Handler: adminapi.NewHTTPHandlerWithOptions(aggregator, adminapi.HTTPHandlerOptions{
			Stream: stream,
		}),
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
