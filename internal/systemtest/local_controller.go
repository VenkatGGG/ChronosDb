package systemtest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/observability"
	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

// LocalControllerConfig defines one in-process local cluster.
type LocalControllerConfig struct {
	Nodes   []uint64
	Catalog *chronossql.Catalog
}

// LocalController runs a local multi-node cluster with real pgwire and
// observability listeners for system-test execution.
type LocalController struct {
	mu      sync.RWMutex
	catalog *chronossql.Catalog
	nodes   map[uint64]*localNode
}

type localNode struct {
	id           uint64
	running      bool
	pgAddr       string
	obsURL       string
	pgListener   net.Listener
	obsListener  net.Listener
	obsServer    *http.Server
	cancel       context.CancelFunc
	metrics      *observability.Metrics
	handler      *faultInjectingHandler
	partitionSet map[uint64]struct{}
	lastError    string
	logs         []NodeLogEntry
}

type faultInjectingHandler struct {
	mu       sync.Mutex
	delegate pgwire.QueryHandler
	fault    *AmbiguousCommitSpec
	record   func(string)
}

type terminateSessionError struct {
	message string
}

var _ Controller = (*LocalController)(nil)

// NewLocalController starts an in-process local cluster backed by real
// listeners and the current pgwire/sql front door.
func NewLocalController(cfg LocalControllerConfig) (*LocalController, error) {
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("systemtest: local controller requires at least one node")
	}
	controller := &LocalController{
		catalog: cfg.Catalog,
		nodes:   make(map[uint64]*localNode, len(cfg.Nodes)),
	}
	if controller.catalog == nil {
		var err error
		controller.catalog, err = defaultSystemTestCatalog()
		if err != nil {
			return nil, err
		}
	}
	seen := make(map[uint64]struct{}, len(cfg.Nodes))
	for _, nodeID := range cfg.Nodes {
		if nodeID == 0 {
			return nil, fmt.Errorf("systemtest: local controller node ids must be non-zero")
		}
		if _, ok := seen[nodeID]; ok {
			return nil, fmt.Errorf("systemtest: duplicate local controller node %d", nodeID)
		}
		seen[nodeID] = struct{}{}
	}
	for _, nodeID := range cfg.Nodes {
		if err := controller.startNode(nodeID); err != nil {
			_ = controller.Close()
			return nil, err
		}
	}
	return controller, nil
}

// Close stops every local node and releases all listeners.
func (c *LocalController) Close() error {
	c.mu.Lock()
	nodes := make([]*localNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	c.mu.Unlock()

	var closeErr error
	for _, node := range nodes {
		if err := c.stopNode(node); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

// PGWireAddr returns the current pgwire listener address for the node.
func (c *LocalController) PGWireAddr(nodeID uint64) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return "", fmt.Errorf("systemtest: unknown node %d", nodeID)
	}
	if !node.running {
		return "", fmt.Errorf("systemtest: node %d is not running", nodeID)
	}
	return node.pgAddr, nil
}

// ObservabilityURL returns the current observability base URL for the node.
func (c *LocalController) ObservabilityURL(nodeID uint64) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return "", fmt.Errorf("systemtest: unknown node %d", nodeID)
	}
	if !node.running {
		return "", fmt.Errorf("systemtest: node %d is not running", nodeID)
	}
	return node.obsURL, nil
}

// Partition applies a bidirectional network partition in controller state.
func (c *LocalController) Partition(_ context.Context, spec PartitionSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.validatePartitionNodes(spec); err != nil {
		return err
	}
	for _, left := range spec.Left {
		node := c.nodes[left]
		if node.partitionSet == nil {
			node.partitionSet = make(map[uint64]struct{})
		}
		for _, right := range spec.Right {
			node.partitionSet[right] = struct{}{}
		}
		c.appendLogLocked(node, "partition applied against nodes "+formatNodeSlice(spec.Right))
	}
	for _, right := range spec.Right {
		node := c.nodes[right]
		if node.partitionSet == nil {
			node.partitionSet = make(map[uint64]struct{})
		}
		for _, left := range spec.Left {
			node.partitionSet[left] = struct{}{}
		}
		c.appendLogLocked(node, "partition applied against nodes "+formatNodeSlice(spec.Left))
	}
	return nil
}

// Heal removes all partition annotations from the local cluster state.
func (c *LocalController) Heal(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range c.nodes {
		if len(node.partitionSet) > 0 {
			c.appendLogLocked(node, "partition healed")
		}
		clear(node.partitionSet)
	}
	return nil
}

// CrashNode stops all listeners for one node.
func (c *LocalController) CrashNode(_ context.Context, nodeID uint64) error {
	c.mu.RLock()
	node, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("systemtest: unknown node %d", nodeID)
	}
	if !node.running {
		return fmt.Errorf("systemtest: node %d is already crashed", nodeID)
	}
	c.recordNodeEvent(nodeID, "crash requested")
	return c.stopNode(node)
}

// RestartNode restarts one stopped node with fresh listeners.
func (c *LocalController) RestartNode(_ context.Context, nodeID uint64) error {
	c.mu.RLock()
	node, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("systemtest: unknown node %d", nodeID)
	}
	if node.running {
		return fmt.Errorf("systemtest: node %d is already running", nodeID)
	}
	return c.startNode(nodeID)
}

// InjectAmbiguousCommit installs a one-shot ambiguous commit fault on the gateway node.
func (c *LocalController) InjectAmbiguousCommit(_ context.Context, spec AmbiguousCommitSpec) error {
	c.mu.RLock()
	node, ok := c.nodes[spec.GatewayNodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("systemtest: unknown gateway node %d", spec.GatewayNodeID)
	}
	if !node.running {
		return fmt.Errorf("systemtest: gateway node %d is not running", spec.GatewayNodeID)
	}
	node.handler.Install(spec)
	c.recordNodeEvent(spec.GatewayNodeID, fmt.Sprintf("ambiguous commit fault armed for label %q", spec.TxnLabel))
	return nil
}

// Wait blocks for the requested duration or until the context is canceled.
func (c *LocalController) Wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *LocalController) startNode(nodeID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node := c.nodes[nodeID]
	if node != nil && node.running {
		return fmt.Errorf("systemtest: node %d is already running", nodeID)
	}
	if node == nil {
		node = &localNode{id: nodeID}
		c.nodes[nodeID] = node
	}

	pgListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("systemtest: listen pgwire for node %d: %w", nodeID, err)
	}
	obsListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = pgListener.Close()
		return fmt.Errorf("systemtest: listen observability for node %d: %w", nodeID, err)
	}

	handler := &faultInjectingHandler{
		delegate: pgwire.NewPlanningHandler(
			chronossql.NewPlanner(chronossql.NewParser(), c.catalog),
			chronossql.NewFlowPlanner(),
		),
		record: func(message string) {
			c.recordNodeEvent(nodeID, message)
		},
	}
	metrics := observability.NewMetrics()
	nodeCtx, cancel := context.WithCancel(context.Background())
	obsServer := &http.Server{
		Handler: observability.NewHandler(observability.HandlerOptions{
			Metrics:  metrics,
			Health:   c.probe(nodeID),
			Ready:    c.probe(nodeID),
			Overview: c.overview(nodeID),
		}),
	}
	node.pgListener = pgListener
	node.obsListener = obsListener
	node.obsServer = obsServer
	node.cancel = cancel
	node.metrics = metrics
	node.handler = handler
	node.pgAddr = pgListener.Addr().String()
	node.obsURL = "http://" + obsListener.Addr().String()
	node.running = true
	node.lastError = ""
	c.appendLogLocked(node, fmt.Sprintf("node started pgwire=%s observability=%s", node.pgAddr, node.obsURL))

	go c.servePG(nodeCtx, nodeID, handler, pgListener)
	go c.serveObservability(nodeCtx, nodeID, obsServer, obsListener)
	return nil
}

func (c *LocalController) stopNode(node *localNode) error {
	c.mu.Lock()
	if current, ok := c.nodes[node.id]; !ok || current != node {
		c.mu.Unlock()
		return fmt.Errorf("systemtest: unknown node %d", node.id)
	}
	if !node.running {
		c.mu.Unlock()
		return nil
	}
	c.appendLogLocked(node, "node stopping")
	node.running = false
	cancel := node.cancel
	pgListener := node.pgListener
	obsServer := node.obsServer
	obsListener := node.obsListener
	node.pgListener = nil
	node.obsListener = nil
	node.obsServer = nil
	node.cancel = nil
	node.pgAddr = ""
	node.obsURL = ""
	node.handler = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	var stopErr error
	if pgListener != nil {
		if err := pgListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) && stopErr == nil {
			stopErr = err
		}
	}
	if obsServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		if err := obsServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) && stopErr == nil {
			stopErr = err
		}
		shutdownCancel()
	}
	if obsListener != nil {
		_ = obsListener.Close()
	}
	return stopErr
}

func (c *LocalController) servePG(ctx context.Context, nodeID uint64, handler pgwire.QueryHandler, listener net.Listener) {
	server := pgwire.NewServer(handler)
	if err := server.ServeListener(ctx, listener); err != nil && !errors.Is(err, context.Canceled) {
		c.recordServeError(nodeID, "pgwire", err)
	}
}

func (c *LocalController) serveObservability(ctx context.Context, nodeID uint64, server *http.Server, listener net.Listener) {
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		_ = server.Shutdown(shutdownCtx)
		shutdownCancel()
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		c.recordServeError(nodeID, "observability", err)
	}
}

func (c *LocalController) recordServeError(nodeID uint64, component string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return
	}
	node.lastError = fmt.Sprintf("%s: %v", component, err)
	c.appendLogLocked(node, "serve error "+node.lastError)
}

func (c *LocalController) probe(nodeID uint64) observability.Probe {
	return func(context.Context) error {
		c.mu.RLock()
		defer c.mu.RUnlock()

		node, ok := c.nodes[nodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", nodeID)
		}
		if !node.running {
			return fmt.Errorf("node %d is not running", nodeID)
		}
		if node.lastError != "" {
			return errors.New(node.lastError)
		}
		return nil
	}
}

func (c *LocalController) overview(nodeID uint64) observability.OverviewProvider {
	return func(context.Context) (observability.Overview, error) {
		c.mu.RLock()
		defer c.mu.RUnlock()

		node, ok := c.nodes[nodeID]
		if !ok {
			return observability.Overview{}, fmt.Errorf("unknown node %d", nodeID)
		}
		overview := observability.Overview{
			Status:      "ok",
			GeneratedAt: time.Now().UTC(),
			Components: map[string]string{
				"pgwire":        statusLabel(node.running),
				"observability": statusLabel(node.running),
			},
		}
		if len(node.partitionSet) > 0 {
			overview.Status = "degraded"
			overview.Notes = append(overview.Notes, fmt.Sprintf("partitioned from nodes %s", formatNodeSet(node.partitionSet)))
		}
		if node.handler != nil && node.handler.HasFault() {
			overview.Status = "degraded"
			overview.Notes = append(overview.Notes, "ambiguous commit fault armed")
		}
		if node.lastError != "" {
			overview.Status = "degraded"
			overview.Notes = append(overview.Notes, node.lastError)
		}
		return overview, nil
	}
}

func (c *LocalController) validatePartitionNodes(spec PartitionSpec) error {
	if len(spec.Left) == 0 || len(spec.Right) == 0 {
		return fmt.Errorf("systemtest: partition sides must not be empty")
	}
	for _, nodeID := range append(append([]uint64(nil), spec.Left...), spec.Right...) {
		if _, ok := c.nodes[nodeID]; !ok {
			return fmt.Errorf("systemtest: unknown partition node %d", nodeID)
		}
	}
	return nil
}

func (h *faultInjectingHandler) HandleSimpleQuery(ctx context.Context, query string) (pgwire.QueryResult, error) {
	spec, match := h.matchingFault(query)
	if match {
		if h.record != nil {
			h.record(fmt.Sprintf("ambiguous commit fault triggered for label %q", spec.TxnLabel))
		}
		timer := time.NewTimer(spec.AckDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return pgwire.QueryResult{}, ctx.Err()
		case <-timer.C:
		}
		if spec.DropResponse {
			if h.record != nil {
				h.record(fmt.Sprintf("ambiguous commit response dropped for label %q", spec.TxnLabel))
			}
			return pgwire.QueryResult{}, terminateSessionError{
				message: fmt.Sprintf("systemtest: dropped response for %q", spec.TxnLabel),
			}
		}
	}
	return h.delegate.HandleSimpleQuery(ctx, query)
}

func (h *faultInjectingHandler) Install(spec AmbiguousCommitSpec) {
	h.mu.Lock()
	defer h.mu.Unlock()
	copySpec := spec
	h.fault = &copySpec
}

func (h *faultInjectingHandler) HasFault() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fault != nil
}

func (h *faultInjectingHandler) matchingFault(query string) (AmbiguousCommitSpec, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.fault == nil || !strings.Contains(query, h.fault.TxnLabel) {
		return AmbiguousCommitSpec{}, false
	}
	spec := *h.fault
	h.fault = nil
	return spec, true
}

func (e terminateSessionError) Error() string {
	return e.message
}

func (e terminateSessionError) TerminateSession() bool {
	return true
}

func defaultSystemTestCatalog() (*chronossql.Catalog, error) {
	catalog := chronossql.NewCatalog()
	if err := catalog.AddTable(chronossql.TableDescriptor{
		ID:   7,
		Name: "users",
		Columns: []chronossql.ColumnDescriptor{
			{ID: 1, Name: "id", Type: chronossql.ColumnTypeInt},
			{ID: 2, Name: "name", Type: chronossql.ColumnTypeString},
			{ID: 3, Name: "email", Type: chronossql.ColumnTypeString, Nullable: true},
		},
		PrimaryKey: []string{"id"},
		Stats: chronossql.TableStats{
			EstimatedRows:   10000,
			AverageRowBytes: 192,
		},
	}); err != nil {
		return nil, err
	}
	if err := catalog.AddTable(chronossql.TableDescriptor{
		ID:   9,
		Name: "orders",
		Columns: []chronossql.ColumnDescriptor{
			{ID: 1, Name: "id", Type: chronossql.ColumnTypeInt},
			{ID: 2, Name: "user_id", Type: chronossql.ColumnTypeInt},
			{ID: 3, Name: "sales", Type: chronossql.ColumnTypeInt},
		},
		PrimaryKey: []string{"id"},
		Stats: chronossql.TableStats{
			EstimatedRows:   10000,
			AverageRowBytes: 96,
		},
		PlacementPolicy: &placement.Policy{
			PlacementMode:    placement.ModeRegional,
			PreferredRegions: []string{"us-east1"},
		},
	}); err != nil {
		return nil, err
	}
	return catalog, nil
}

func statusLabel(v bool) string {
	if v {
		return "up"
	}
	return "down"
}

func formatNodeSet(nodes map[uint64]struct{}) string {
	ordered := make([]uint64, 0, len(nodes))
	for nodeID := range nodes {
		ordered = append(ordered, nodeID)
	}
	if len(ordered) == 0 {
		return "[]"
	}
	for i := 1; i < len(ordered); i++ {
		for j := i; j > 0 && ordered[j-1] > ordered[j]; j-- {
			ordered[j-1], ordered[j] = ordered[j], ordered[j-1]
		}
	}
	parts := make([]string, 0, len(ordered))
	for _, nodeID := range ordered {
		parts = append(parts, fmt.Sprintf("%d", nodeID))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func formatNodeSlice(nodes []uint64) string {
	set := make(map[uint64]struct{}, len(nodes))
	for _, nodeID := range nodes {
		set[nodeID] = struct{}{}
	}
	return formatNodeSet(set)
}

// SnapshotNodeLogs returns a deep copy of the per-node event log stream.
func (c *LocalController) SnapshotNodeLogs() map[uint64][]NodeLogEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[uint64][]NodeLogEntry, len(c.nodes))
	for nodeID, node := range c.nodes {
		out[nodeID] = append([]NodeLogEntry(nil), node.logs...)
	}
	return out
}

// RecordNodeLog appends one explicit system-test observation to the node log.
func (c *LocalController) RecordNodeLog(nodeID uint64, message string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("systemtest: unknown node %d", nodeID)
	}
	c.appendLogLocked(node, message)
	return nil
}

func (c *LocalController) recordNodeEvent(nodeID uint64, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return
	}
	c.appendLogLocked(node, message)
}

func (c *LocalController) appendLogLocked(node *localNode, message string) {
	node.logs = append(node.logs, NodeLogEntry{
		Timestamp: time.Now().UTC(),
		Message:   message,
	})
}
