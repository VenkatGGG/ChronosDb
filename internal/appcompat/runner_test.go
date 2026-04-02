package appcompat

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/demo"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

func TestRunAgainstSeededDemoCluster(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	manifest, err := demo.DefaultBootstrapManifest("appcompat-seeded-demo")
	if err != nil {
		t.Fatalf("default bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(demo.BootstrapPath(rootDir), manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}
	catalog, err := systemtest.DefaultCatalog()
	if err != nil {
		t.Fatalf("default catalog: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configs := demo.DefaultNodeConfigs(rootDir, manifest.ClusterID)
	doneChans := make([]chan error, 0, len(configs))
	nodes := make([]*systemtest.ProcessNode, 0, len(configs))
	for _, cfg := range configs {
		cfg.Catalog = catalog
		node, err := systemtest.NewProcessNode(cfg)
		if err != nil {
			t.Fatalf("new process node %d: %v", cfg.NodeID, err)
		}
		done := make(chan error, 1)
		go func(node *systemtest.ProcessNode) {
			done <- node.Run(ctx)
		}(node)
		doneChans = append(doneChans, done)
		nodes = append(nodes, node)
	}
	t.Cleanup(func() {
		cancel()
		for idx, done := range doneChans {
			select {
			case err := <-done:
				if err != nil && err != context.Canceled {
					t.Fatalf("node %d run: %v", idx+1, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("timed out waiting for node %d shutdown", idx+1)
			}
		}
	})

	if err := demo.WaitForSeededClusterReady(ctx, manifest, configs); err != nil {
		t.Fatalf("wait for seeded cluster readiness: %v", err)
	}
	state := nodes[0].State()
	runCtx, runCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer runCancel()
	report, err := Run(runCtx, Config{
		Addr:             state.PGAddr,
		Iterations:       2,
		StatementTimeout: 5 * time.Second,
		BaseUserID:       300000,
		BaseConflictID:   1300000,
	})
	if err != nil {
		t.Fatalf("run appcompat workload: %v", err)
	}
	if !report.Compatible {
		t.Fatalf("report incompatible: %s", SummaryString(report))
	}
	if report.TotalFailures != 0 {
		t.Fatalf("total failures = %d, want 0", report.TotalFailures)
	}
	if len(report.Operations) != len(operationOrder) {
		t.Fatalf("operation count = %d, want %d", len(report.Operations), len(operationOrder))
	}
	for _, summary := range report.Operations {
		if summary.Successes != 2 {
			t.Fatalf("operation %s successes = %d, want 2", summary.Name, summary.Successes)
		}
		if summary.Failures != 0 {
			t.Fatalf("operation %s failures = %d, want 0", summary.Name, summary.Failures)
		}
	}
}

func TestSummaryStringIncludesStatusAndFailures(t *testing.T) {
	report := Report{
		Addr:          "127.0.0.1:26257",
		Iterations:    1,
		Compatible:    false,
		TotalFailures: 2,
		MissingOperations: []string{
			"insert_user",
		},
		Operations: []OperationSummary{
			{Name: "insert_user", Count: 1, Successes: 0, Failures: 1, AverageMs: 1.5, P95Ms: 2.0, MaxMs: 2.0, ErrorClasses: map[string]int{"23": 1}},
		},
	}
	summary := SummaryString(report)
	if !containsAll(summary, "FAIL", "insert_user", "errors=map[23:1]", "missing-success coverage") {
		t.Fatalf("summary = %q", summary)
	}
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
