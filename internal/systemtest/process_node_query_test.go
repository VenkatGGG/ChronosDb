package systemtest

import (
	"context"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestProcessNodeExecutesPointInsertAndSelectAcrossNodes(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    41,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 11, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 12, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 13, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 12,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-live-query", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 11},
		{NodeID: 2, StoreID: 12},
		{NodeID: 3, StoreID: 13},
	}, []meta.RangeDescriptor{usersRange})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            1,
		DataDir:           filepath.Join(rootDir, "node-1"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "node1")
	})
	node2, cancel2, done2 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            2,
		DataDir:           filepath.Join(rootDir, "node-2"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "node2")
	})
	node3, cancel3, done3 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            3,
		DataDir:           filepath.Join(rootDir, "node-3"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 12)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (7, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("insert frame tags = %q, want CZ", got)
	}

	key := usersPrimaryKey(7)
	waitForReplicatedUserRow(t, node3, key)

	selectConn := openPGConn(t, node3.state.PGAddr)
	defer selectConn.Close()
	if _, err := selectConn.Write(queryFrame("select id, name from users where id = 7")); err != nil {
		t.Fatalf("write select: %v", err)
	}
	frames := readFrames(t, selectConn, 4)
	if got := frameTags(frames); got != "TDCZ" {
		t.Fatalf("select frame tags = %q, want TDCZ", got)
	}
	values, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode data row: %v", err)
	}
	if len(values) != 2 || string(values[0]) != "7" || string(values[1]) != "alice" {
		t.Fatalf("selected values = %q/%q, want 7/alice", values[0], values[1])
	}
}

func TestProcessNodeExecutesRangeScanAcrossLeaseholders(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	splitKey := usersPrimaryKey(50)
	rangeOne := meta.RangeDescriptor{
		RangeID:    51,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     splitKey,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 21, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 22, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 23, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 21,
	}
	rangeTwo := meta.RangeDescriptor{
		RangeID:    52,
		Generation: 1,
		StartKey:   splitKey,
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 24, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 25, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 26, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 26,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-range-scan", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 21},
		{NodeID: 2, StoreID: 22},
		{NodeID: 3, StoreID: 23},
	}, []meta.RangeDescriptor{rangeOne, rangeTwo})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            1,
		DataDir:           filepath.Join(rootDir, "node-1"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "node1")
	})
	node2, cancel2, done2 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            2,
		DataDir:           filepath.Join(rootDir, "node-2"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "node2")
	})
	node3, cancel3, done3 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            3,
		DataDir:           filepath.Join(rootDir, "node-3"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "node3")
	})

	if err := node1.host.Campaign(context.Background(), rangeOne.RangeID); err != nil {
		t.Fatalf("campaign first range leaseholder: %v", err)
	}
	if err := node3.host.Campaign(context.Background(), rangeTwo.RangeID); err != nil {
		t.Fatalf("campaign second range leaseholder: %v", err)
	}
	waitForRangeLeader(t, node1.host, rangeOne.RangeID, 21)
	waitForRangeLeader(t, node3.host, rangeTwo.RangeID, 26)

	conn := openPGConn(t, node2.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (7, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write first insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("first insert frame tags = %q, want CZ", got)
	}
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (70, 'bob', 'b@example.com')")); err != nil {
		t.Fatalf("write second insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("second insert frame tags = %q, want CZ", got)
	}

	scanConn := openPGConn(t, node2.state.PGAddr)
	defer scanConn.Close()
	if _, err := scanConn.Write(queryFrame("select id, name from users where id >= 7 and id < 80")); err != nil {
		t.Fatalf("write range scan: %v", err)
	}
	frames := readFramesUntilReady(t, scanConn)
	if got := frameTags(frames); got != "TDDCZ" {
		t.Fatalf("range scan frame tags = %q, want TDDCZ", got)
	}
	firstRow, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode first data row: %v", err)
	}
	secondRow, err := decodeDataRowFrame(frames[2])
	if err != nil {
		t.Fatalf("decode second data row: %v", err)
	}
	if len(firstRow) != 2 || string(firstRow[0]) != "7" || string(firstRow[1]) != "alice" {
		t.Fatalf("first scanned row = %q/%q, want 7/alice", firstRow[0], firstRow[1])
	}
	if len(secondRow) != 2 || string(secondRow[0]) != "70" || string(secondRow[1]) != "bob" {
		t.Fatalf("second scanned row = %q/%q, want 70/bob", secondRow[0], secondRow[1])
	}
}

func TestProcessNodeExecutesAggregateAndJoinQueries(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    61,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 31, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 32, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 33, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 31,
	}
	ordersRange := meta.RangeDescriptor{
		RangeID:    62,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(9),
		EndKey:     storage.GlobalTablePrimaryPrefix(10),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 34, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 35, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 36, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 36,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-aggregate-join", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 31},
		{NodeID: 2, StoreID: 32},
		{NodeID: 3, StoreID: 33},
	}, []meta.RangeDescriptor{usersRange, ordersRange})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            1,
		DataDir:           filepath.Join(rootDir, "node-1"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "node1")
	})
	node2, cancel2, done2 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            2,
		DataDir:           filepath.Join(rootDir, "node-2"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "node2")
	})
	node3, cancel3, done3 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            3,
		DataDir:           filepath.Join(rootDir, "node-3"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "node3")
	})

	if err := node1.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign users range leaseholder: %v", err)
	}
	if err := node3.host.Campaign(context.Background(), ordersRange.RangeID); err != nil {
		t.Fatalf("campaign orders range leaseholder: %v", err)
	}
	waitForRangeLeader(t, node1.host, usersRange.RangeID, 31)
	waitForRangeLeader(t, node3.host, ordersRange.RangeID, 36)

	conn := openPGConn(t, node2.state.PGAddr)
	defer conn.Close()
	for _, query := range []string{
		"insert into users (id, name, email) values (7, 'alice', 'a@example.com')",
		"insert into users (id, name, email) values (8, 'bob', 'b@example.com')",
		"insert into orders (id, user_id, region, sales) values (101, 7, 'us-east', 50)",
		"insert into orders (id, user_id, region, sales) values (102, 7, 'us-east', 25)",
		"insert into orders (id, user_id, region, sales) values (103, 8, 'us-west', 80)",
	} {
		if _, err := conn.Write(queryFrame(query)); err != nil {
			t.Fatalf("write insert %q: %v", query, err)
		}
		if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
			t.Fatalf("insert frame tags for %q = %q, want CZ", query, got)
		}
	}

	aggregateConn := openPGConn(t, node2.state.PGAddr)
	defer aggregateConn.Close()
	if _, err := aggregateConn.Write(queryFrame("select region, sum(sales) from orders group by region")); err != nil {
		t.Fatalf("write aggregate query: %v", err)
	}
	aggregateFrames := readFramesUntilReady(t, aggregateConn)
	if got := frameTags(aggregateFrames); got != "TDDCZ" {
		t.Fatalf("aggregate frame tags = %q, want TDDCZ", got)
	}
	aggregateRow1, err := decodeDataRowFrame(aggregateFrames[1])
	if err != nil {
		t.Fatalf("decode aggregate row 1: %v", err)
	}
	aggregateRow2, err := decodeDataRowFrame(aggregateFrames[2])
	if err != nil {
		t.Fatalf("decode aggregate row 2: %v", err)
	}
	if len(aggregateRow1) != 2 || string(aggregateRow1[0]) != "us-east" || string(aggregateRow1[1]) != "75" {
		t.Fatalf("aggregate row 1 = %q/%q, want us-east/75", aggregateRow1[0], aggregateRow1[1])
	}
	if len(aggregateRow2) != 2 || string(aggregateRow2[0]) != "us-west" || string(aggregateRow2[1]) != "80" {
		t.Fatalf("aggregate row 2 = %q/%q, want us-west/80", aggregateRow2[0], aggregateRow2[1])
	}

	joinConn := openPGConn(t, node2.state.PGAddr)
	defer joinConn.Close()
	if _, err := joinConn.Write(queryFrame("select u.name, o.sales from users u join orders o on u.id = o.user_id")); err != nil {
		t.Fatalf("write join query: %v", err)
	}
	joinFrames := readFramesUntilReady(t, joinConn)
	if got := frameTags(joinFrames); got != "TDDDCZ" {
		t.Fatalf("join frame tags = %q, want TDDDCZ", got)
	}
	joinRow1, err := decodeDataRowFrame(joinFrames[1])
	if err != nil {
		t.Fatalf("decode join row 1: %v", err)
	}
	joinRow2, err := decodeDataRowFrame(joinFrames[2])
	if err != nil {
		t.Fatalf("decode join row 2: %v", err)
	}
	joinRow3, err := decodeDataRowFrame(joinFrames[3])
	if err != nil {
		t.Fatalf("decode join row 3: %v", err)
	}
	if len(joinRow1) != 2 || string(joinRow1[0]) != "alice" || string(joinRow1[1]) != "50" {
		t.Fatalf("join row 1 = %q/%q, want alice/50", joinRow1[0], joinRow1[1])
	}
	if len(joinRow2) != 2 || string(joinRow2[0]) != "alice" || string(joinRow2[1]) != "25" {
		t.Fatalf("join row 2 = %q/%q, want alice/25", joinRow2[0], joinRow2[1])
	}
	if len(joinRow3) != 2 || string(joinRow3[0]) != "bob" || string(joinRow3[1]) != "80" {
		t.Fatalf("join row 3 = %q/%q, want bob/80", joinRow3[0], joinRow3[1])
	}
}

func TestProcessNodeLoadsPersistedCustomCatalogAcrossRestart(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	widgetsRange := meta.RangeDescriptor{
		RangeID:    71,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(42),
		EndKey:     storage.GlobalTablePrimaryPrefix(43),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 41, NodeID: 1, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 41,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-custom-catalog", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 41},
	}, []meta.RangeDescriptor{widgetsRange})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	customCatalog := chronossql.NewCatalog()
	if err := customCatalog.AddTable(chronossql.TableDescriptor{
		ID:   42,
		Name: "widgets",
		Columns: []chronossql.ColumnDescriptor{
			{ID: 1, Name: "id", Type: chronossql.ColumnTypeInt},
			{ID: 2, Name: "name", Type: chronossql.ColumnTypeString},
		},
		PrimaryKey: []string{"id"},
	}); err != nil {
		t.Fatalf("add widgets table: %v", err)
	}

	dataDir := filepath.Join(rootDir, "node-1")
	node, cancel, done := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            1,
		DataDir:           dataDir,
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Catalog:           customCatalog,
	})
	if err := node.host.Campaign(context.Background(), widgetsRange.RangeID); err != nil {
		t.Fatalf("campaign widgets range: %v", err)
	}
	waitForRangeLeader(t, node.host, widgetsRange.RangeID, 41)

	conn := openPGConn(t, node.state.PGAddr)
	if _, err := conn.Write(queryFrame("insert into widgets (id, name) values (1, 'gizmo')")); err != nil {
		t.Fatalf("write widgets insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("widgets insert frame tags = %q, want CZ", got)
	}
	conn.Close()
	cancel()
	waitProcessNodeDone(t, done, "custom-catalog-node-1")

	if err := os.Remove(filepath.Join(dataDir, "state.json")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove state file: %v", err)
	}

	restarted, cancelRestart, doneRestart := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            1,
		DataDir:           dataDir,
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancelRestart()
		waitProcessNodeDone(t, doneRestart, "restarted-custom-catalog-node-1")
	})
	waitForRangeLeader(t, restarted.host, widgetsRange.RangeID, 41)

	selectConn := openPGConn(t, restarted.state.PGAddr)
	defer selectConn.Close()
	if _, err := selectConn.Write(queryFrame("select id, name from widgets where id = 1")); err != nil {
		t.Fatalf("write widgets select: %v", err)
	}
	frames := readFrames(t, selectConn, 4)
	if got := frameTags(frames); got != "TDCZ" {
		t.Fatalf("widgets select frame tags = %q, want TDCZ", got)
	}
	values, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode widgets data row: %v", err)
	}
	if len(values) != 2 || string(values[0]) != "1" || string(values[1]) != "gizmo" {
		t.Fatalf("widgets values = %q/%q, want 1/gizmo", values[0], values[1])
	}
}

func TestProcessNodeRestartsAndResumesServingReplicatedRows(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    81,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 51, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 52, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 53, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 52,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-restart-recovery", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 51},
		{NodeID: 2, StoreID: 52},
		{NodeID: 3, StoreID: 53},
	}, []meta.RangeDescriptor{usersRange})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            1,
		DataDir:           filepath.Join(rootDir, "node-1"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "restart-node1")
	})
	node2DataDir := filepath.Join(rootDir, "node-2")
	node2, cancel2, done2 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            2,
		DataDir:           node2DataDir,
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	node3, cancel3, done3 := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            3,
		DataDir:           filepath.Join(rootDir, "node-3"),
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "restart-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign restart leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 52)

	insertConn := openPGConn(t, node1.state.PGAddr)
	if _, err := insertConn.Write(queryFrame("insert into users (id, name, email) values (7, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write restart insert: %v", err)
	}
	if got := frameTags(readFrames(t, insertConn, 2)); got != "CZ" {
		t.Fatalf("restart insert frame tags = %q, want CZ", got)
	}
	insertConn.Close()

	key := usersPrimaryKey(7)
	waitForReplicatedUserRow(t, node3, key)

	cancel2()
	waitProcessNodeDone(t, done2, "restart-node2-initial")

	restarted, cancelRestart, doneRestart := startProcessNodeForTest(t, ProcessNodeConfig{
		NodeID:            2,
		DataDir:           node2DataDir,
		BootstrapPath:     bootstrapPath,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
	})
	t.Cleanup(func() {
		cancelRestart()
		waitProcessNodeDone(t, doneRestart, "restart-node2-restarted")
	})

	waitForReplicatedUserRow(t, restarted, key)

	selectConn := openPGConn(t, restarted.state.PGAddr)
	defer selectConn.Close()
	if _, err := selectConn.Write(queryFrame("select id, name from users where id = 7")); err != nil {
		t.Fatalf("write restart select: %v", err)
	}
	frames := readFrames(t, selectConn, 4)
	if got := frameTags(frames); got != "TDCZ" {
		t.Fatalf("restart select frame tags = %q, want TDCZ", got)
	}
	values, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode restart data row: %v", err)
	}
	if len(values) != 2 || string(values[0]) != "7" || string(values[1]) != "alice" {
		t.Fatalf("restart selected values = %q/%q, want 7/alice", values[0], values[1])
	}
}

func waitForRangeLeader(t *testing.T, host *chronosruntime.Host, rangeID, leaderReplicaID uint64) {
	t.Helper()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		leader, err := host.Leader(rangeID)
		if err == nil && leader == leaderReplicaID {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	leader, _ := host.Leader(rangeID)
	t.Fatalf("range %d leader = %d, want %d", rangeID, leader, leaderReplicaID)
}

func waitForReplicatedUserRow(t *testing.T, node *ProcessNode, key []byte) {
	t.Helper()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		value, _, found, err := node.host.ReadLatestLocal(context.Background(), key)
		if err == nil && found && len(value) > 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for replicated row %q on node %d", key, node.cfg.NodeID)
}

func usersPrimaryKey(id int64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(id)^(uint64(1)<<63))
	return storage.GlobalTablePrimaryKey(7, encoded[:])
}

func decodeDataRowFrame(frame []byte) ([][]byte, error) {
	payload := frame[5:]
	columnCount := int(binary.BigEndian.Uint16(payload[:2]))
	payload = payload[2:]
	values := make([][]byte, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		size := int(int32(binary.BigEndian.Uint32(payload[:4])))
		payload = payload[4:]
		if size < 0 {
			values = append(values, nil)
			continue
		}
		values = append(values, append([]byte(nil), payload[:size]...))
		payload = payload[size:]
	}
	return values, nil
}

func readFramesUntilReady(t *testing.T, conn net.Conn) [][]byte {
	t.Helper()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	frames := make([][]byte, 0, 8)
	for {
		frame, err := readBackendFrame(conn)
		if err != nil {
			t.Fatalf("read backend frame: %v", err)
		}
		frames = append(frames, frame)
		if frame[0] == 'Z' {
			return frames
		}
	}
}
