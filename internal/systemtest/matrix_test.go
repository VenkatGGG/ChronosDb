package systemtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestExecuteFaultMatrixWithLocalController(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	result, err := ExecuteFaultMatrix(context.Background(), FaultMatrixConfig{
		ArtifactRoot: root,
	})
	if err != nil {
		t.Fatalf("execute fault matrix: %v", err)
	}
	if len(result.Entries) != 7 {
		t.Fatalf("matrix entries = %+v, want 7 scenarios", result.Entries)
	}
	for _, entry := range result.Entries {
		if entry.Status != "pass" {
			t.Fatalf("matrix entry = %+v, want pass", entry)
		}
		for _, path := range []string{
			filepath.Join(entry.ArtifactDir, "manifest.json"),
			filepath.Join(entry.ArtifactDir, "report.json"),
			filepath.Join(entry.ArtifactDir, "summary.json"),
		} {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("stat %s: %v", path, err)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(root, "fault-matrix.json")); err != nil {
		t.Fatalf("stat fault matrix summary: %v", err)
	}
}
