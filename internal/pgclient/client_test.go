package pgclient_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/pgclient"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

func TestClientSimpleQueryAgainstProcessNode(t *testing.T) {
	t.Parallel()

	catalog, err := systemtest.DefaultCatalog()
	if err != nil {
		t.Fatalf("default catalog: %v", err)
	}
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	manifest, err := chronosruntime.BuildBootstrapManifest("pgclient-test", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 1},
	}, []meta.RangeDescriptor{
		{
			RangeID:    11,
			Generation: 1,
			StartKey:   storage.GlobalTablePrimaryPrefix(7),
			EndKey:     storage.GlobalTablePrimaryPrefix(8),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 1,
		},
	})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}
	node, err := systemtest.NewProcessNode(systemtest.ProcessNodeConfig{
		NodeID:            1,
		ClusterID:         manifest.ClusterID,
		StoreID:           1,
		BootstrapPath:     bootstrapPath,
		DataDir:           filepath.Join(rootDir, "node-1"),
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Catalog:           catalog,
	})
	if err != nil {
		t.Fatalf("new process node: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				t.Fatalf("node run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for node shutdown")
		}
	})

	state := waitForNodeState(t, node)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	client, err := pgclient.Dial(dialCtx, state.PGAddr, "chronos")
	if err != nil {
		t.Fatalf("dial pgclient: %v", err)
	}
	defer client.Close()

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer queryCancel()
	insertResult, err := client.SimpleQuery(queryCtx, "insert into users (id, name, email) values (7, 'alice', 'a@example.com')")
	if err != nil {
		t.Fatalf("insert query: %v", err)
	}
	if insertResult.CommandTag != "INSERT 0 1" {
		t.Fatalf("insert command tag = %q, want INSERT 0 1", insertResult.CommandTag)
	}

	selectResult, err := client.SimpleQuery(queryCtx, "select id, name from users where id = 7")
	if err != nil {
		t.Fatalf("select query: %v", err)
	}
	if len(selectResult.Rows) != 1 {
		t.Fatalf("select rows = %d, want 1", len(selectResult.Rows))
	}
	if got := selectResult.Rows[0]; len(got) != 2 || got[0] != "7" || got[1] != "alice" {
		t.Fatalf("select row = %#v, want [7 alice]", got)
	}
}

func TestClientExtendedQueryAgainstProcessNode(t *testing.T) {
	t.Parallel()

	catalog, err := systemtest.DefaultCatalog()
	if err != nil {
		t.Fatalf("default catalog: %v", err)
	}
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	manifest, err := chronosruntime.BuildBootstrapManifest("pgclient-extended-test", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 1},
	}, []meta.RangeDescriptor{
		{
			RangeID:    11,
			Generation: 1,
			StartKey:   storage.GlobalTablePrimaryPrefix(7),
			EndKey:     storage.GlobalTablePrimaryPrefix(8),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 1,
		},
	})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}
	node, err := systemtest.NewProcessNode(systemtest.ProcessNodeConfig{
		NodeID:            1,
		ClusterID:         manifest.ClusterID,
		StoreID:           1,
		BootstrapPath:     bootstrapPath,
		DataDir:           filepath.Join(rootDir, "node-1"),
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Catalog:           catalog,
	})
	if err != nil {
		t.Fatalf("new process node: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				t.Fatalf("node run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for node shutdown")
		}
	})

	state := waitForNodeState(t, node)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	client, err := pgclient.Dial(dialCtx, state.PGAddr, "chronos")
	if err != nil {
		t.Fatalf("dial pgclient: %v", err)
	}
	defer client.Close()

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer queryCancel()

	insertResult, err := client.ExtendedQuery(queryCtx, "insert into users (id, name, email) values ($1, $2, $3) returning id, name", int64(9), "bob", "b@example.com")
	if err != nil {
		t.Fatalf("extended insert: %v", err)
	}
	if insertResult.CommandTag != "INSERT 0 1" {
		t.Fatalf("insert command tag = %q, want INSERT 0 1", insertResult.CommandTag)
	}
	if len(insertResult.Rows) != 1 || len(insertResult.Rows[0]) != 2 || insertResult.Rows[0][0] != "9" || insertResult.Rows[0][1] != "bob" {
		t.Fatalf("insert returning rows = %#v", insertResult.Rows)
	}

	updateResult, err := client.ExtendedQuery(queryCtx, "update users set name = $1 where id = $2 returning id, name", "robert", int64(9))
	if err != nil {
		t.Fatalf("extended update: %v", err)
	}
	if updateResult.CommandTag != "UPDATE 1" {
		t.Fatalf("update command tag = %q, want UPDATE 1", updateResult.CommandTag)
	}
	if len(updateResult.Rows) != 1 || updateResult.Rows[0][1] != "robert" {
		t.Fatalf("update returning rows = %#v", updateResult.Rows)
	}

	selectResult, err := client.ExtendedQuery(queryCtx, "select id, name from users where id = $1", int64(9))
	if err != nil {
		t.Fatalf("extended select: %v", err)
	}
	if len(selectResult.Rows) != 1 || len(selectResult.Rows[0]) != 2 || selectResult.Rows[0][0] != "9" || selectResult.Rows[0][1] != "robert" {
		t.Fatalf("select rows = %#v, want [[9 robert]]", selectResult.Rows)
	}

	filteredResult, err := client.ExtendedQuery(queryCtx, "select id, name from users where name >= $1 order by name desc limit $2", "r", int64(1))
	if err != nil {
		t.Fatalf("extended filtered select: %v", err)
	}
	if len(filteredResult.Rows) != 1 || len(filteredResult.Rows[0]) != 2 || filteredResult.Rows[0][0] != "9" || filteredResult.Rows[0][1] != "robert" {
		t.Fatalf("filtered rows = %#v, want [[9 robert]]", filteredResult.Rows)
	}
}

func waitForNodeState(t *testing.T, node *systemtest.ProcessNode) systemtest.ProcessNodeState {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state := node.State()
		if state.PGAddr != "" {
			return state
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for process node state")
	return systemtest.ProcessNodeState{}
}
