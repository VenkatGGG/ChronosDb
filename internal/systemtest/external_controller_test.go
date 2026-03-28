package systemtest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
)

func TestHelperProcessNode(t *testing.T) {
	if os.Getenv("CHRONOS_HELPER_PROCESS_NODE") != "1" {
		return
	}

	args := os.Args
	sep := -1
	for i, arg := range args {
		if arg == "--" {
			sep = i
			break
		}
	}
	if sep == -1 {
		fmt.Fprintln(os.Stderr, "missing helper separator")
		os.Exit(2)
	}

	fs := flag.NewFlagSet("chronos-helper-node", flag.ExitOnError)
	nodeID := fs.Uint64("node-id", 0, "")
	dataDir := fs.String("data-dir", "", "")
	pgAddr := fs.String("pg-addr", "127.0.0.1:0", "")
	obsAddr := fs.String("obs-addr", "127.0.0.1:0", "")
	controlAddr := fs.String("control-addr", "127.0.0.1:0", "")
	_ = fs.Parse(args[sep+1:])

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	node, err := NewProcessNode(ProcessNodeConfig{
		NodeID:            *nodeID,
		DataDir:           *dataDir,
		PGListenAddr:      *pgAddr,
		ObservabilityAddr: *obsAddr,
		ControlAddr:       *controlAddr,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := node.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func TestExecuteFaultMatrixWithExternalProcessController(t *testing.T) {
	root := t.TempDir()
	result, err := ExecuteFaultMatrix(context.Background(), FaultMatrixConfig{
		ArtifactRoot: filepath.Join(root, "artifacts"),
		ControllerFactory: NewExternalControllerFactory(
			os.Args[0],
			filepath.Join(root, "cluster"),
			[]string{"CHRONOS_HELPER_PROCESS_NODE=1"},
			func(spec NodeLaunchSpec) []string {
				return []string{
					"-test.run=TestHelperProcessNode",
					"--",
					"-node-id", fmt.Sprintf("%d", spec.NodeID),
					"-data-dir", spec.DataDir,
					"-pg-addr", spec.PGListenAddr,
					"-obs-addr", spec.ObservabilityAddr,
					"-control-addr", spec.ControlAddr,
				}
			},
		),
	})
	if err != nil {
		t.Fatalf("execute external fault matrix: %v", err)
	}
	if len(result.Entries) != 7 {
		t.Fatalf("matrix entries = %+v, want 7", result.Entries)
	}
	for _, entry := range result.Entries {
		if entry.Status != "pass" {
			t.Fatalf("entry = %+v, want pass", entry)
		}
		for _, path := range []string{
			filepath.Join(entry.ArtifactDir, "manifest.json"),
			filepath.Join(entry.ArtifactDir, "handoff.json"),
			filepath.Join(entry.ArtifactDir, "report.json"),
			filepath.Join(entry.ArtifactDir, "summary.json"),
		} {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("stat %s: %v", path, err)
			}
		}
	}
}
