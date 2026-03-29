package demo

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestDefaultBootstrapManifestProvidesVisiblePlacement(t *testing.T) {
	t.Parallel()

	manifest, err := DefaultBootstrapManifest(DefaultClusterID)
	if err != nil {
		t.Fatalf("default bootstrap manifest: %v", err)
	}
	if got, want := len(manifest.Nodes), 3; got != want {
		t.Fatalf("manifest nodes = %d, want %d", got, want)
	}
	if got, want := len(manifest.Meta2), 5; got != want {
		t.Fatalf("manifest meta2 ranges = %d, want %d", got, want)
	}
	if !bytes.Equal(manifest.Meta2[1].StartKey, storage.GlobalTablePrimaryPrefix(7)) {
		t.Fatalf("users low start = %q, want %q", manifest.Meta2[1].StartKey, storage.GlobalTablePrimaryPrefix(7))
	}
	if !bytes.Equal(manifest.Meta2[2].EndKey, storage.GlobalTablePrimaryPrefix(8)) {
		t.Fatalf("users high end = %q, want %q", manifest.Meta2[2].EndKey, storage.GlobalTablePrimaryPrefix(8))
	}
	if manifest.Meta2[1].LeaseholderReplicaID == manifest.Meta2[2].LeaseholderReplicaID {
		t.Fatal("users ranges share a leaseholder, want visible distribution")
	}
}

func TestDefaultNodeConfigsShareBootstrapPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configs := DefaultNodeConfigs(root, DefaultClusterID)
	if got, want := len(configs), 3; got != want {
		t.Fatalf("node configs = %d, want %d", got, want)
	}
	bootstrapPath := BootstrapPath(root)
	for _, cfg := range configs {
		if cfg.BootstrapPath != bootstrapPath {
			t.Fatalf("node %d bootstrap path = %q, want %q", cfg.NodeID, cfg.BootstrapPath, bootstrapPath)
		}
		if filepath.Dir(cfg.DataDir) != root {
			t.Fatalf("node %d data dir = %q, want under %q", cfg.NodeID, cfg.DataDir, root)
		}
	}
}
