package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/demo"
	"github.com/VenkatGGG/ChronosDb/internal/pgclient"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

func main() {
	var (
		dataRoot          string
		clusterID         string
		consoleListenAddr string
		uiDir             string
		runSmokeTest      bool
	)
	flag.StringVar(&dataRoot, "data-root", "", "demo data root (defaults to a fresh temp dir)")
	flag.StringVar(&clusterID, "cluster-id", demo.DefaultClusterID, "demo cluster identifier")
	flag.StringVar(&consoleListenAddr, "console-listen", demo.DefaultConsoleAddress, "console listen address")
	flag.StringVar(&uiDir, "ui-dir", "", "optional built UI directory to serve with the console")
	flag.BoolVar(&runSmokeTest, "smoke-test", true, "run the seeded insert/select smoke test after startup")
	flag.Parse()

	if dataRoot == "" {
		tempRoot, err := os.MkdirTemp("", "chronos-demo-*")
		if err != nil {
			log.Fatal(err)
		}
		dataRoot = tempRoot
	}
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		log.Fatal(err)
	}

	manifest, err := demo.DefaultBootstrapManifest(clusterID)
	if err != nil {
		log.Fatal(err)
	}
	bootstrapPath := demo.BootstrapPath(dataRoot)
	if _, err := os.Stat(bootstrapPath); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(bootstrapPath), 0o755); err != nil {
			log.Fatal(err)
		}
		if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}

	catalog, err := systemtest.DefaultCatalog()
	if err != nil {
		log.Fatal(err)
	}
	nodeConfigs := demo.DefaultNodeConfigs(dataRoot, clusterID)
	nodes := make([]*systemtest.ProcessNode, 0, len(nodeConfigs))
	doneChans := make([]chan error, 0, len(nodeConfigs))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for _, cfg := range nodeConfigs {
		cfg.Catalog = catalog
		node, done, err := startNode(ctx, cfg)
		if err != nil {
			log.Fatal(err)
		}
		nodes = append(nodes, node)
		doneChans = append(doneChans, done)
	}
	for _, cfg := range nodeConfigs {
		if err := waitForAdminSnapshot(ctx, "http://"+cfg.ObservabilityAddr); err != nil {
			log.Fatal(err)
		}
	}

	rawTargets := demo.DefaultObservabilityURLs()
	targets := make([]adminapi.NodeTarget, 0, len(rawTargets))
	for _, baseURL := range rawTargets {
		targets = append(targets, adminapi.NodeTarget{BaseURL: baseURL})
	}
	aggregator, err := adminapi.NewAggregator(adminapi.AggregatorConfig{
		Targets:           targets,
		EventLimitPerNode: 64,
	})
	if err != nil {
		log.Fatal(err)
	}
	stream, err := adminapi.NewEventStream(adminapi.EventStreamConfig{
		Aggregator:   aggregator,
		PollInterval: time.Second,
		ReplayLimit:  256,
	})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := stream.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("chronos demo event stream stopped: %v", err)
		}
	}()

	if uiDir == "" {
		candidate := filepath.Join(".", "ui", "dist")
		if _, err := os.Stat(filepath.Join(candidate, "index.html")); err == nil {
			uiDir = candidate
		}
	}
	handler, err := buildHandler(
		adminapi.NewHTTPHandlerWithOptions(aggregator, adminapi.HTTPHandlerOptions{Stream: stream}),
		uiDir,
	)
	if err != nil {
		log.Fatal(err)
	}
	console := &http.Server{
		Addr:    consoleListenAddr,
		Handler: handler,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = console.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := console.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("chronos demo console stopped: %v", err)
			stop()
		}
	}()

	if runSmokeTest {
		smokeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		if err := runSmoke(smokeCtx, nodeConfigs[0].PGListenAddr); err != nil {
			cancel()
			log.Fatal(err)
		}
		cancel()
	}

	log.Printf("chronos demo data root: %s", dataRoot)
	log.Printf("console: http://%s", consoleListenAddr)
	for _, cfg := range nodeConfigs {
		log.Printf("node %d pgwire=%s observability=http://%s control=http://%s", cfg.NodeID, cfg.PGListenAddr, cfg.ObservabilityAddr, cfg.ControlAddr)
	}
	log.Printf("psql: psql \"postgresql://chronos@%s/postgres?sslmode=disable\"", nodeConfigs[0].PGListenAddr)
	log.Printf("seeded smoke queries are loaded across split users/orders ranges")

	<-ctx.Done()

	for idx, done := range doneChans {
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("node %d stopped with error: %v", nodeConfigs[idx].NodeID, err)
		}
	}
}

func startNode(ctx context.Context, cfg systemtest.ProcessNodeConfig) (*systemtest.ProcessNode, chan error, error) {
	node, err := systemtest.NewProcessNode(cfg)
	if err != nil {
		return nil, nil, err
	}
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx)
	}()
	return node, done, nil
}

func waitForAdminSnapshot(ctx context.Context, baseURL string) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	url := strings.TrimRight(baseURL, "/") + "/admin/snapshot?event_limit=16"
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func runSmoke(ctx context.Context, addr string) error {
	client, err := pgclient.Dial(ctx, addr, "chronos")
	if err != nil {
		return err
	}
	defer client.Close()
	for _, sql := range demo.DefaultSmokeQueries() {
		result, err := retryQuery(ctx, client, sql)
		if err != nil {
			return err
		}
		log.Printf("smoke query ok: %s => %s rows=%d", sql, result.CommandTag, len(result.Rows))
	}
	return nil
}

func retryQuery(ctx context.Context, client *pgclient.Client, sql string) (pgclient.Result, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := client.SimpleQuery(ctx, sql)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return pgclient.Result{}, fmt.Errorf("smoke query %q: %w", sql, err)
		case <-ticker.C:
		}
	}
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
		if cleaned == "." || cleaned == "/" || strings.HasPrefix(cleaned, "..") {
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
