package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
)

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:8080", "console listen address")
	nodeURLs := flag.String("node-observability-urls", "", "comma-separated node observability base URLs")
	eventLimit := flag.Int("event-limit-per-node", 64, "recent events to request from each node")
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

	server := &http.Server{
		Addr:    *listenAddr,
		Handler: adminapi.NewHTTPHandler(aggregator),
	}
	log.Printf("chronos console listening on %s", *listenAddr)
	log.Fatal(server.ListenAndServe())
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
