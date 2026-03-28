package systemtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/observability"
	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

// ProcessNodeConfig configures one externally launched ChronosDB node process.
type ProcessNodeConfig struct {
	NodeID            uint64
	DataDir           string
	PGListenAddr      string
	ObservabilityAddr string
	ControlAddr       string
	Catalog           *chronossql.Catalog
}

// ProcessNodeState is the externally readable address/state manifest for one launched node.
type ProcessNodeState struct {
	NodeID           uint64    `json:"node_id"`
	PGAddr           string    `json:"pg_addr"`
	ObservabilityURL string    `json:"observability_url"`
	ControlURL       string    `json:"control_url"`
	StartedAt        time.Time `json:"started_at"`
}

// ProcessNode is a single-node process runtime for the external chaos runner.
type ProcessNode struct {
	mu         sync.RWMutex
	cfg        ProcessNodeConfig
	metrics    *observability.Metrics
	handler    *faultInjectingHandler
	pgListener net.Listener
	obsServer  *http.Server
	obsListen  net.Listener
	ctrlServer *http.Server
	ctrlListen net.Listener
	lastError  string
	partitions map[uint64]struct{}
	state      ProcessNodeState
	logPath    string
	statePath  string
}

type partitionControlRequest struct {
	IsolatedFrom []uint64 `json:"isolated_from"`
}

type logControlRequest struct {
	Message string `json:"message"`
}

// NewProcessNode constructs a node runtime.
func NewProcessNode(cfg ProcessNodeConfig) (*ProcessNode, error) {
	if cfg.NodeID == 0 {
		return nil, fmt.Errorf("systemtest: process node id must be non-zero")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("systemtest: process node data dir must not be empty")
	}
	if cfg.PGListenAddr == "" {
		cfg.PGListenAddr = "127.0.0.1:0"
	}
	if cfg.ObservabilityAddr == "" {
		cfg.ObservabilityAddr = "127.0.0.1:0"
	}
	if cfg.ControlAddr == "" {
		cfg.ControlAddr = "127.0.0.1:0"
	}
	if cfg.Catalog == nil {
		var err error
		cfg.Catalog, err = defaultSystemTestCatalog()
		if err != nil {
			return nil, err
		}
	}
	node := &ProcessNode{
		cfg:        cfg,
		metrics:    observability.NewMetrics(),
		partitions: make(map[uint64]struct{}),
		logPath:    NodeLogPath(cfg.DataDir),
		statePath:  filepath.Join(cfg.DataDir, "state.json"),
	}
	node.handler = &faultInjectingHandler{
		delegate: pgwire.NewPlanningHandler(
			chronossql.NewPlanner(chronossql.NewParser(), cfg.Catalog),
			chronossql.NewFlowPlanner(),
		),
		record: func(message string) {
			_ = node.appendLog(message)
		},
	}
	return node, nil
}

// Run starts the node listeners and blocks until the context is canceled.
func (n *ProcessNode) Run(ctx context.Context) error {
	if err := os.MkdirAll(n.cfg.DataDir, 0o755); err != nil {
		return err
	}
	pgListener, err := net.Listen("tcp", n.cfg.PGListenAddr)
	if err != nil {
		return fmt.Errorf("systemtest: listen pgwire: %w", err)
	}
	obsListener, err := net.Listen("tcp", n.cfg.ObservabilityAddr)
	if err != nil {
		_ = pgListener.Close()
		return fmt.Errorf("systemtest: listen observability: %w", err)
	}
	ctrlListener, err := net.Listen("tcp", n.cfg.ControlAddr)
	if err != nil {
		_ = pgListener.Close()
		_ = obsListener.Close()
		return fmt.Errorf("systemtest: listen control: %w", err)
	}

	n.mu.Lock()
	n.pgListener = pgListener
	n.obsListen = obsListener
	n.ctrlListen = ctrlListener
	n.state = ProcessNodeState{
		NodeID:           n.cfg.NodeID,
		PGAddr:           pgListener.Addr().String(),
		ObservabilityURL: "http://" + obsListener.Addr().String(),
		ControlURL:       "http://" + ctrlListener.Addr().String(),
		StartedAt:        time.Now().UTC(),
	}
	n.obsServer = &http.Server{
		Handler: observability.NewHandler(observability.HandlerOptions{
			Metrics:  n.metrics,
			Health:   n.probe(),
			Ready:    n.probe(),
			Overview: n.overview(),
		}),
	}
	n.ctrlServer = &http.Server{Handler: n.controlMux()}
	n.mu.Unlock()

	if err := n.writeState(); err != nil {
		return err
	}
	if err := n.appendLog(fmt.Sprintf(
		"node started pgwire=%s observability=%s control=%s",
		n.state.PGAddr,
		n.state.ObservabilityURL,
		n.state.ControlURL,
	)); err != nil {
		return err
	}

	go n.servePG(ctx)
	go n.serveObservability(ctx)
	go n.serveControl(ctx)

	<-ctx.Done()
	return n.shutdown()
}

func (n *ProcessNode) servePG(ctx context.Context) {
	server := pgwire.NewServer(n.handler)
	if err := server.ServeListener(ctx, n.pgListener); err != nil && !errors.Is(err, context.Canceled) {
		n.recordServeError("pgwire", err)
	}
}

func (n *ProcessNode) serveObservability(ctx context.Context) {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = n.obsServer.Shutdown(shutdownCtx)
		cancel()
	}()
	if err := n.obsServer.Serve(n.obsListen); err != nil && !errors.Is(err, http.ErrServerClosed) {
		n.recordServeError("observability", err)
	}
}

func (n *ProcessNode) serveControl(ctx context.Context) {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = n.ctrlServer.Shutdown(shutdownCtx)
		cancel()
	}()
	if err := n.ctrlServer.Serve(n.ctrlListen); err != nil && !errors.Is(err, http.ErrServerClosed) {
		n.recordServeError("control", err)
	}
}

func (n *ProcessNode) shutdown() error {
	n.mu.RLock()
	pgListener := n.pgListener
	obsServer := n.obsServer
	obsListen := n.obsListen
	ctrlServer := n.ctrlServer
	ctrlListen := n.ctrlListen
	n.mu.RUnlock()

	_ = n.appendLog("node stopping")
	var errs []error
	if pgListener != nil {
		if err := pgListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	if obsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := obsServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, err)
		}
		cancel()
	}
	if obsListen != nil {
		_ = obsListen.Close()
	}
	if ctrlServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := ctrlServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, err)
		}
		cancel()
	}
	if ctrlListen != nil {
		_ = ctrlListen.Close()
	}
	return errors.Join(errs...)
}

func (n *ProcessNode) writeState() error {
	payload, err := json.MarshalIndent(n.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(n.statePath, append(payload, '\n'), 0o644)
}

func (n *ProcessNode) controlMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/control/state", n.handleState)
	mux.HandleFunc("/control/partition", n.handlePartition)
	mux.HandleFunc("/control/heal", n.handleHeal)
	mux.HandleFunc("/control/ambiguous-commit", n.handleAmbiguousCommit)
	mux.HandleFunc("/control/logs", n.handleLogs)
	mux.HandleFunc("/control/log", n.handleLog)
	return mux
}

func (n *ProcessNode) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(n.state)
}

func (n *ProcessNode) handlePartition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req partitionControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	clear(n.partitions)
	for _, nodeID := range req.IsolatedFrom {
		n.partitions[nodeID] = struct{}{}
	}
	_ = n.appendLog("partition applied against nodes " + formatNodeSlice(req.IsolatedFrom))
	w.WriteHeader(http.StatusNoContent)
}

func (n *ProcessNode) handleHeal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	n.mu.Lock()
	hadPartition := len(n.partitions) > 0
	clear(n.partitions)
	n.mu.Unlock()
	if hadPartition {
		_ = n.appendLog("partition healed")
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *ProcessNode) handleAmbiguousCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var spec AmbiguousCommitSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.handler.Install(spec)
	_ = n.appendLog(fmt.Sprintf("ambiguous commit fault armed for label %q", spec.TxnLabel))
	w.WriteHeader(http.StatusNoContent)
}

func (n *ProcessNode) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	logs, err := readNodeLogFile(n.logPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(logs)
}

func (n *ProcessNode) handleLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req logControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Message == "" {
		http.Error(w, "message must not be empty", http.StatusBadRequest)
		return
	}
	if err := n.appendLog(req.Message); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *ProcessNode) probe() observability.Probe {
	return func(context.Context) error {
		n.mu.RLock()
		defer n.mu.RUnlock()
		if n.lastError != "" {
			return errors.New(n.lastError)
		}
		return nil
	}
}

func (n *ProcessNode) overview() observability.OverviewProvider {
	return func(context.Context) (observability.Overview, error) {
		n.mu.RLock()
		defer n.mu.RUnlock()
		overview := observability.Overview{
			Status:      "ok",
			GeneratedAt: time.Now().UTC(),
			Components: map[string]string{
				"pgwire":        "up",
				"observability": "up",
				"control":       "up",
			},
		}
		if len(n.partitions) > 0 {
			overview.Status = "degraded"
			overview.Notes = append(overview.Notes, fmt.Sprintf("partitioned from nodes %s", formatNodeSet(n.partitions)))
		}
		if n.handler != nil && n.handler.HasFault() {
			overview.Status = "degraded"
			overview.Notes = append(overview.Notes, "ambiguous commit fault armed")
		}
		if n.lastError != "" {
			overview.Status = "degraded"
			overview.Notes = append(overview.Notes, n.lastError)
		}
		return overview, nil
	}
}

func (n *ProcessNode) appendLog(message string) error {
	return appendNodeLogMessage(n.logPath, message)
}

func (n *ProcessNode) recordServeError(component string, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lastError = fmt.Sprintf("%s: %v", component, err)
	_ = n.appendLog("serve error " + n.lastError)
}

// ReadProcessNodeState reads the process state manifest written by a launched node.
func ReadProcessNodeState(path string) (ProcessNodeState, error) {
	var state ProcessNodeState
	payload, err := os.ReadFile(path)
	if err != nil {
		return ProcessNodeState{}, err
	}
	if err := json.Unmarshal(payload, &state); err != nil {
		return ProcessNodeState{}, err
	}
	return state, nil
}
