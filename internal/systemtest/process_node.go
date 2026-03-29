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
	"strconv"
	"sync"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/observability"
	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

// ProcessNodeConfig configures one externally launched ChronosDB node process.
type ProcessNodeConfig struct {
	NodeID            uint64
	ClusterID         string
	StoreID           uint64
	BootstrapPath     string
	DataDir           string
	PGListenAddr      string
	ObservabilityAddr string
	ControlAddr       string
	Catalog           *chronossql.Catalog
	Ranges            []meta.RangeDescriptor
	EventBufferSize   int
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
	events     []adminapi.ClusterEvent
	state      ProcessNodeState
	logPath    string
	statePath  string
	host       *chronosruntime.Host
}

const defaultProcessNodeEventBufferSize = 256

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
		cfg.Catalog = nil
	}
	if cfg.EventBufferSize <= 0 {
		cfg.EventBufferSize = defaultProcessNodeEventBufferSize
	}
	if cfg.BootstrapPath == "" {
		cfg.BootstrapPath = filepath.Join(filepath.Dir(cfg.DataDir), "bootstrap.json")
	}
	if len(cfg.Ranges) > 0 {
		cfg.Ranges = append([]meta.RangeDescriptor(nil), cfg.Ranges...)
		for _, desc := range cfg.Ranges {
			if err := desc.Validate(); err != nil {
				return nil, fmt.Errorf("systemtest: invalid process node range descriptor: %w", err)
			}
		}
	}
	host, err := chronosruntime.Open(context.Background(), chronosruntime.Config{
		NodeID:        cfg.NodeID,
		StoreID:       cfg.StoreID,
		ClusterID:     cfg.ClusterID,
		DataDir:       filepath.Join(cfg.DataDir, "store"),
		SeedRanges:    cfg.Ranges,
		BootstrapPath: cfg.BootstrapPath,
		Transport:     newProcessNodeTransport(cfg.NodeID, filepath.Dir(cfg.DataDir)),
	})
	if err != nil {
		return nil, err
	}
	if cfg.Catalog == nil {
		defaultCatalog, err := defaultSystemTestCatalog()
		if err != nil {
			return nil, err
		}
		if err := host.SeedSQLCatalog(context.Background(), defaultCatalog); err != nil {
			return nil, err
		}
		cfg.Catalog, err = host.LoadSQLCatalog(context.Background())
		if err != nil {
			return nil, err
		}
	} else {
		if err := host.SeedSQLCatalog(context.Background(), cfg.Catalog); err != nil {
			return nil, err
		}
		loadedCatalog, err := host.LoadSQLCatalog(context.Background())
		if err != nil {
			return nil, err
		}
		cfg.Catalog = loadedCatalog
	}
	node := &ProcessNode{
		cfg:        cfg,
		metrics:    observability.NewMetrics(),
		partitions: make(map[uint64]struct{}),
		events:     make([]adminapi.ClusterEvent, 0, cfg.EventBufferSize),
		logPath:    NodeLogPath(cfg.DataDir),
		statePath:  filepath.Join(cfg.DataDir, "state.json"),
		host:       host,
	}
	planner := chronossql.NewPlanner(chronossql.NewParser(), cfg.Catalog)
	flowPlanner := chronossql.NewFlowPlanner()
	node.handler = &faultInjectingHandler{
		delegate: newRuntimeQueryHandler(planner, flowPlanner, newKVClient(cfg.NodeID, filepath.Dir(cfg.DataDir), host)),
		record: func(message string) {
			_ = node.recordEvent(adminapi.ClusterEvent{
				Type:     "node_activity",
				Severity: "info",
				Message:  message,
			})
		},
	}
	return node, nil
}

// State returns the last published process-node addresses and timestamps.
func (n *ProcessNode) State() ProcessNodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
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
		Handler: n.observabilityMux(),
	}
	n.ctrlServer = &http.Server{Handler: n.controlMux()}
	n.mu.Unlock()

	if err := n.writeState(); err != nil {
		return err
	}
	if err := n.recordEvent(adminapi.ClusterEvent{
		Type:     "node_started",
		Severity: "info",
		Message: fmt.Sprintf(
			"node started pgwire=%s observability=%s control=%s",
			n.state.PGAddr,
			n.state.ObservabilityURL,
			n.state.ControlURL,
		),
	}); err != nil {
		return err
	}

	go n.servePG(ctx)
	go n.serveObservability(ctx)
	go n.serveControl(ctx)
	go n.serveRuntime(ctx)

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

func (n *ProcessNode) serveRuntime(ctx context.Context) {
	if n.host == nil {
		return
	}
	if err := n.host.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		n.recordServeError("runtime", err)
	}
}

func (n *ProcessNode) shutdown() error {
	n.mu.RLock()
	pgListener := n.pgListener
	obsServer := n.obsServer
	obsListen := n.obsListen
	ctrlServer := n.ctrlServer
	ctrlListen := n.ctrlListen
	host := n.host
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
	if host != nil {
		if err := host.Close(); err != nil {
			errs = append(errs, err)
		}
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
	mux.HandleFunc("/control/raft/message", n.handleRaftMessage)
	mux.HandleFunc("/control/kv/get", n.handleKVGet)
	mux.HandleFunc("/control/kv/put", n.handleKVPut)
	mux.HandleFunc("/control/kv/scan", n.handleKVScan)
	mux.HandleFunc("/control/ambiguous-commit", n.handleAmbiguousCommit)
	mux.HandleFunc("/control/logs", n.handleLogs)
	mux.HandleFunc("/control/log", n.handleLog)
	return mux
}

func (n *ProcessNode) observabilityMux() http.Handler {
	base := observability.NewHandler(observability.HandlerOptions{
		Metrics:  n.metrics,
		Health:   n.probe(),
		Ready:    n.probe(),
		Overview: n.overview(),
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/node", n.handleAdminNode)
	mux.HandleFunc("/admin/ranges", n.handleAdminRanges)
	mux.HandleFunc("/admin/events", n.handleAdminEvents)
	mux.HandleFunc("/admin/snapshot", n.handleAdminSnapshot)
	mux.Handle("/", base)
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
	clear(n.partitions)
	for _, nodeID := range req.IsolatedFrom {
		n.partitions[nodeID] = struct{}{}
	}
	n.mu.Unlock()
	_ = n.recordEvent(adminapi.ClusterEvent{
		Type:     "partition_applied",
		Severity: "warning",
		Message:  "partition applied against nodes " + formatNodeSlice(req.IsolatedFrom),
		Fields: map[string]string{
			"isolated_from": formatNodeSlice(req.IsolatedFrom),
		},
	})
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
		_ = n.recordEvent(adminapi.ClusterEvent{
			Type:     "partition_healed",
			Severity: "info",
			Message:  "partition healed",
		})
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
	_ = n.recordEvent(adminapi.ClusterEvent{
		Type:     "ambiguous_commit_armed",
		Severity: "warning",
		Message:  fmt.Sprintf("ambiguous commit fault armed for label %q", spec.TxnLabel),
		Fields: map[string]string{
			"txn_label": spec.TxnLabel,
		},
	})
	w.WriteHeader(http.StatusNoContent)
}

func (n *ProcessNode) handleRaftMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req raftMessageBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, message := range req.Messages {
		decoded, err := unmarshalRaftMessage(message.Message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := n.host.Step(r.Context(), message.RangeID, decoded); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *ProcessNode) handleKVGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req kvGetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	value, _, found, err := n.host.ReadLatestLocal(r.Context(), req.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(w, kvGetResponse{
		Found: found,
		Value: value,
	})
}

func (n *ProcessNode) handleKVPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req kvPutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := n.host.PutValueLocal(r.Context(), req.Key, req.Timestamp, req.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *ProcessNode) handleKVScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req kvScanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rows, _, err := n.host.ScanRangeLocal(r.Context(), req.StartKey, req.EndKey, req.StartInclusive, req.EndInclusive)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := kvScanResponse{Rows: make([]kvScanRow, 0, len(rows))}
	for _, row := range rows {
		resp.Rows = append(resp.Rows, kvScanRow{
			LogicalKey: row.LogicalKey,
			Timestamp:  row.Timestamp,
			Value:      row.Value,
		})
	}
	writeJSONResponse(w, resp)
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
	if err := n.recordEvent(adminapi.ClusterEvent{
		Type:     "operator_log",
		Severity: "info",
		Message:  req.Message,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *ProcessNode) handleAdminNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	view, err := n.nodeView()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(w, view)
}

func (n *ProcessNode) handleAdminRanges(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	views, err := n.rangeViews()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(w, views)
}

func (n *ProcessNode) handleAdminEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSONResponse(w, n.recentEvents(parsePositiveInt(r.URL.Query().Get("limit"))))
}

func (n *ProcessNode) handleAdminSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snapshot, err := n.snapshot(parsePositiveInt(r.URL.Query().Get("event_limit")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(w, snapshot)
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
		status, notes := n.statusLocked()
		overview := observability.Overview{
			Status:      status,
			GeneratedAt: time.Now().UTC(),
			Components: map[string]string{
				"pgwire":        "up",
				"observability": "up",
				"control":       "up",
			},
			Notes: append([]string(nil), notes...),
		}
		return overview, nil
	}
}

func (n *ProcessNode) appendLog(message string) error {
	return appendNodeLogMessage(n.logPath, message)
}

func (n *ProcessNode) recordServeError(component string, err error) {
	n.mu.Lock()
	n.lastError = fmt.Sprintf("%s: %v", component, err)
	message := "serve error " + n.lastError
	n.mu.Unlock()
	_ = n.recordEvent(adminapi.ClusterEvent{
		Type:     "serve_error",
		Severity: "error",
		Message:  message,
		Fields: map[string]string{
			"component": component,
		},
	})
}

func (n *ProcessNode) nodeView() (adminapi.NodeView, error) {
	n.mu.RLock()
	status, notes := n.statusLocked()
	state := n.state
	partitions := sortedNodeSet(n.partitions)
	n.mu.RUnlock()

	descs, err := n.host.HostedDescriptors()
	if err != nil {
		return adminapi.NodeView{}, err
	}
	leaseCount := 0
	for _, desc := range descs {
		if leaseholderNodeID(desc) == n.cfg.NodeID {
			leaseCount++
		}
	}
	return adminapi.NodeView{
		NodeID:           state.NodeID,
		PGAddr:           state.PGAddr,
		ObservabilityURL: state.ObservabilityURL,
		ControlURL:       state.ControlURL,
		Status:           status,
		StartedAt:        state.StartedAt,
		PartitionedFrom:  partitions,
		Notes:            notes,
		ReplicaCount:     len(descs),
		LeaseCount:       leaseCount,
	}, nil
}

func (n *ProcessNode) rangeViews() ([]adminapi.RangeView, error) {
	descs, err := n.host.HostedDescriptors()
	if err != nil {
		return nil, err
	}
	views := make([]adminapi.RangeView, 0, len(descs))
	for _, desc := range descs {
		views = append(views, adminapi.RangeViewFromDescriptor(desc, "runtime_store"))
	}
	return views, nil
}

func (n *ProcessNode) recentEvents(limit int) []adminapi.ClusterEvent {
	n.mu.RLock()
	defer n.mu.RUnlock()

	events := append([]adminapi.ClusterEvent(nil), n.events...)
	if limit <= 0 || limit >= len(events) {
		return events
	}
	return append([]adminapi.ClusterEvent(nil), events[len(events)-limit:]...)
}

func (n *ProcessNode) snapshot(eventLimit int) (adminapi.ClusterSnapshot, error) {
	nodeView, err := n.nodeView()
	if err != nil {
		return adminapi.ClusterSnapshot{}, err
	}
	rangeViews, err := n.rangeViews()
	if err != nil {
		return adminapi.ClusterSnapshot{}, err
	}
	return adminapi.ClusterSnapshot{
		GeneratedAt: time.Now().UTC(),
		Nodes:       []adminapi.NodeView{nodeView},
		Ranges:      rangeViews,
		Events:      n.recentEvents(eventLimit),
	}, nil
}

func (n *ProcessNode) statusLocked() (string, []string) {
	status := "ok"
	notes := make([]string, 0, 3)
	if len(n.partitions) > 0 {
		status = "degraded"
		notes = append(notes, fmt.Sprintf("partitioned from nodes %s", formatNodeSet(n.partitions)))
	}
	if n.handler != nil && n.handler.HasFault() {
		status = "degraded"
		notes = append(notes, "ambiguous commit fault armed")
	}
	if n.lastError != "" {
		status = "degraded"
		notes = append(notes, n.lastError)
	}
	return status, notes
}

func (n *ProcessNode) recordEvent(event adminapi.ClusterEvent) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.NodeID == 0 {
		event.NodeID = n.cfg.NodeID
	}
	event = adminapi.NormalizeEvent(event)
	n.mu.Lock()
	n.events = append(n.events, event)
	if len(n.events) > n.cfg.EventBufferSize {
		n.events = append([]adminapi.ClusterEvent(nil), n.events[len(n.events)-n.cfg.EventBufferSize:]...)
	}
	n.mu.Unlock()
	if event.Message == "" {
		return nil
	}
	return n.appendLog(event.Message)
}

func descriptorReplicaOnNode(desc meta.RangeDescriptor, nodeID uint64) bool {
	for _, replica := range desc.Replicas {
		if replica.NodeID == nodeID {
			return true
		}
	}
	return false
}

func leaseholderNodeID(desc meta.RangeDescriptor) uint64 {
	for _, replica := range desc.Replicas {
		if replica.ReplicaID == desc.LeaseholderReplicaID {
			return replica.NodeID
		}
	}
	return 0
}

func sortedNodeSet(nodes map[uint64]struct{}) []uint64 {
	if len(nodes) == 0 {
		return nil
	}
	ordered := make([]uint64, 0, len(nodes))
	for nodeID := range nodes {
		ordered = append(ordered, nodeID)
	}
	for i := 1; i < len(ordered); i++ {
		for j := i; j > 0 && ordered[j-1] > ordered[j]; j-- {
			ordered[j-1], ordered[j] = ordered[j], ordered[j-1]
		}
	}
	return ordered
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

func writeJSONResponse(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
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
