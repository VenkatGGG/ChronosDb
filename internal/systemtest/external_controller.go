package systemtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// NodeLaunchSpec captures the per-node addresses and data directory for one child process.
type NodeLaunchSpec struct {
	NodeID            uint64
	DataDir           string
	PGListenAddr      string
	ObservabilityAddr string
	ControlAddr       string
}

// ExternalProcessControllerConfig describes how the OS-process-backed cluster should be launched.
type ExternalProcessControllerConfig struct {
	Nodes        []uint64
	BinaryPath   string
	BaseDir      string
	StartTimeout time.Duration
	BinaryEnv    []string
	LaunchArgs   func(NodeLaunchSpec) []string
}

type externalNode struct {
	nodeID    uint64
	dataDir   string
	statePath string
	stdout    string
	stderr    string
	cmd       *exec.Cmd
	state     ProcessNodeState
	stdoutFD  *os.File
	stderrFD  *os.File
	waitDone  chan error
}

// ExternalProcessController drives faults against separate ChronosDB processes.
type ExternalProcessController struct {
	mu     sync.RWMutex
	cfg    ExternalProcessControllerConfig
	nodes  map[uint64]*externalNode
	client *http.Client
}

var _ ArtifactController = (*ExternalProcessController)(nil)

// NewExternalProcessController starts the configured child-process cluster.
func NewExternalProcessController(cfg ExternalProcessControllerConfig) (*ExternalProcessController, error) {
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("systemtest: external controller requires nodes")
	}
	if cfg.BinaryPath == "" {
		return nil, fmt.Errorf("systemtest: external controller requires a binary path")
	}
	if cfg.BaseDir == "" {
		return nil, fmt.Errorf("systemtest: external controller requires a base dir")
	}
	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = 5 * time.Second
	}
	controller := &ExternalProcessController{
		cfg:    cfg,
		nodes:  make(map[uint64]*externalNode, len(cfg.Nodes)),
		client: &http.Client{Timeout: 2 * time.Second},
	}
	if err := os.MkdirAll(cfg.BaseDir, 0o755); err != nil {
		return nil, err
	}
	for _, nodeID := range cfg.Nodes {
		if err := controller.startNode(nodeID); err != nil {
			_ = controller.Close()
			return nil, err
		}
	}
	return controller, nil
}

// Close stops all child processes and aggregates shutdown errors.
func (c *ExternalProcessController) Close() error {
	c.mu.Lock()
	nodes := make([]*externalNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	c.mu.Unlock()

	var errs []error
	for _, node := range nodes {
		if err := c.stopNode(node); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Partition applies partition hints to every running node in the affected sets.
func (c *ExternalProcessController) Partition(_ context.Context, spec PartitionSpec) error {
	for _, left := range spec.Left {
		if err := c.postJSON(left, "/control/partition", partitionControlRequest{IsolatedFrom: spec.Right}); err != nil {
			return err
		}
	}
	for _, right := range spec.Right {
		if err := c.postJSON(right, "/control/partition", partitionControlRequest{IsolatedFrom: spec.Left}); err != nil {
			return err
		}
	}
	return nil
}

// Heal clears all partition hints on running nodes.
func (c *ExternalProcessController) Heal(context.Context) error {
	c.mu.RLock()
	nodeIDs := make([]uint64, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	c.mu.RUnlock()
	var errs []error
	for _, nodeID := range nodeIDs {
		if err := c.postJSON(nodeID, "/control/heal", struct{}{}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// CrashNode terminates one child process.
func (c *ExternalProcessController) CrashNode(_ context.Context, nodeID uint64) error {
	c.mu.RLock()
	node, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("systemtest: unknown external node %d", nodeID)
	}
	if node.cmd == nil || node.cmd.Process == nil {
		return fmt.Errorf("systemtest: node %d is already crashed", nodeID)
	}
	return c.stopNode(node)
}

// RestartNode launches a fresh child process for the node.
func (c *ExternalProcessController) RestartNode(_ context.Context, nodeID uint64) error {
	c.mu.RLock()
	node, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("systemtest: unknown external node %d", nodeID)
	}
	if node.cmd != nil && node.cmd.Process != nil {
		return fmt.Errorf("systemtest: node %d is already running", nodeID)
	}
	return c.startNode(nodeID)
}

// InjectAmbiguousCommit forwards the fault spec to the running gateway node.
func (c *ExternalProcessController) InjectAmbiguousCommit(_ context.Context, spec AmbiguousCommitSpec) error {
	return c.postJSON(spec.GatewayNodeID, "/control/ambiguous-commit", spec)
}

// Wait delays scenario execution without mutating controller state.
func (c *ExternalProcessController) Wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// SnapshotNodeLogs reads the persisted JSONL node logs for every node.
func (c *ExternalProcessController) SnapshotNodeLogs() map[uint64][]NodeLogEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	logs := make(map[uint64][]NodeLogEntry, len(c.nodes))
	for nodeID, node := range c.nodes {
		entries, err := readNodeLogFile(NodeLogPath(node.dataDir))
		if err != nil {
			continue
		}
		logs[nodeID] = entries
	}
	return logs
}

// RecordNodeLog appends a marker or note to the node log stream.
func (c *ExternalProcessController) RecordNodeLog(nodeID uint64, message string) error {
	c.mu.RLock()
	node, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("systemtest: unknown external node %d", nodeID)
	}
	return appendNodeLogMessage(NodeLogPath(node.dataDir), message)
}

func (c *ExternalProcessController) startNode(nodeID uint64) error {
	c.mu.Lock()
	node := c.nodes[nodeID]
	if node == nil {
		dataDir := filepath.Join(c.cfg.BaseDir, fmt.Sprintf("node-%d", nodeID))
		node = &externalNode{
			nodeID:    nodeID,
			dataDir:   dataDir,
			statePath: filepath.Join(dataDir, "state.json"),
			stdout:    filepath.Join(dataDir, "stdout.log"),
			stderr:    filepath.Join(dataDir, "stderr.log"),
		}
		c.nodes[nodeID] = node
	}
	if err := os.MkdirAll(node.dataDir, 0o755); err != nil {
		c.mu.Unlock()
		return err
	}
	_ = os.Remove(node.statePath)
	if node.cmd != nil && node.cmd.Process != nil {
		c.mu.Unlock()
		return fmt.Errorf("systemtest: node %d is already running", nodeID)
	}
	spec := NodeLaunchSpec{
		NodeID:            nodeID,
		DataDir:           node.dataDir,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	}
	args := c.buildArgs(spec)
	cmd := exec.Command(c.cfg.BinaryPath, args...)
	cmd.Env = append(os.Environ(), c.cfg.BinaryEnv...)
	stdoutFile, err := os.OpenFile(node.stdout, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	stderrFile, err := os.OpenFile(node.stderr, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		_ = stdoutFile.Close()
		c.mu.Unlock()
		return err
	}
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile
	node.cmd = cmd
	node.stdoutFD = stdoutFile
	node.stderrFD = stderrFile
	node.waitDone = make(chan error, 1)
	c.mu.Unlock()

	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return err
	}
	go func(waitDone chan error, stdoutFD, stderrFD *os.File) {
		waitDone <- cmd.Wait()
		_ = stdoutFD.Close()
		_ = stderrFD.Close()
	}(node.waitDone, stdoutFile, stderrFile)
	state, err := c.waitForState(node.statePath)
	if err != nil {
		_ = c.stopNode(node)
		return err
	}
	if err := c.waitForControl(state.ControlURL); err != nil {
		_ = c.stopNode(node)
		return err
	}
	c.mu.Lock()
	node.state = state
	c.mu.Unlock()
	return nil
}

func (c *ExternalProcessController) stopNode(node *externalNode) error {
	c.mu.Lock()
	if node.cmd == nil || node.cmd.Process == nil {
		c.mu.Unlock()
		return nil
	}
	cmd := node.cmd
	waitDone := node.waitDone
	node.cmd = nil
	node.waitDone = nil
	node.stdoutFD = nil
	node.stderrFD = nil
	node.state = ProcessNodeState{}
	c.mu.Unlock()

	_ = appendNodeLogMessage(NodeLogPath(node.dataDir), "external controller stop requested")
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		if waitDone != nil {
			<-waitDone
		}
	case <-waitDone:
	}
	return nil
}

func (c *ExternalProcessController) buildArgs(spec NodeLaunchSpec) []string {
	if c.cfg.LaunchArgs != nil {
		return c.cfg.LaunchArgs(spec)
	}
	return []string{
		"-node-id", fmt.Sprintf("%d", spec.NodeID),
		"-data-dir", spec.DataDir,
		"-pg-addr", spec.PGListenAddr,
		"-obs-addr", spec.ObservabilityAddr,
		"-control-addr", spec.ControlAddr,
	}
}

func (c *ExternalProcessController) waitForState(path string) (ProcessNodeState, error) {
	deadline := time.Now().Add(c.cfg.StartTimeout)
	for time.Now().Before(deadline) {
		state, err := ReadProcessNodeState(path)
		if err == nil {
			return state, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return ProcessNodeState{}, fmt.Errorf("systemtest: timed out waiting for node state at %s", path)
}

func (c *ExternalProcessController) waitForControl(controlURL string) error {
	deadline := time.Now().Add(c.cfg.StartTimeout)
	for time.Now().Before(deadline) {
		resp, err := c.client.Get(controlURL + "/control/state")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("systemtest: timed out waiting for control endpoint %s", controlURL)
}

func (c *ExternalProcessController) postJSON(nodeID uint64, path string, payload any) error {
	c.mu.RLock()
	node, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("systemtest: unknown external node %d", nodeID)
	}
	if node.state.ControlURL == "" {
		return fmt.Errorf("systemtest: node %d is not running", nodeID)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := c.client.Post(node.state.ControlURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("systemtest: %s on node %d returned status %d", path, nodeID, resp.StatusCode)
	}
	return nil
}

// NewExternalControllerFactory returns a matrix-compatible controller factory.
func NewExternalControllerFactory(binaryPath, baseDir string, binaryEnv []string, launchArgs func(NodeLaunchSpec) []string) func([]uint64) (ArtifactController, error) {
	return func(nodes []uint64) (ArtifactController, error) {
		runDir := filepath.Join(baseDir, fmt.Sprintf("run-%d", time.Now().UTC().UnixNano()))
		return NewExternalProcessController(ExternalProcessControllerConfig{
			Nodes:        nodes,
			BinaryPath:   binaryPath,
			BaseDir:      runDir,
			StartTimeout: 5 * time.Second,
			BinaryEnv:    binaryEnv,
			LaunchArgs:   launchArgs,
		})
	}
}
