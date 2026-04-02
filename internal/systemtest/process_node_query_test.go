package systemtest

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/txn"
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

func TestProcessNodeExecutesFilteredOrderedLimitedSelectAcrossLeaseholders(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	splitKey := usersPrimaryKey(50)
	rangeOne := meta.RangeDescriptor{
		RangeID:    53,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     splitKey,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 27, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 28, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 29, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 27,
	}
	rangeTwo := meta.RangeDescriptor{
		RangeID:    54,
		Generation: 1,
		StartKey:   splitKey,
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 30, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 31, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 32, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 32,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-filtered-select", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 27},
		{NodeID: 2, StoreID: 28},
		{NodeID: 3, StoreID: 29},
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
		waitProcessNodeDone(t, done1, "filtered-select-node1")
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
		waitProcessNodeDone(t, done2, "filtered-select-node2")
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
		waitProcessNodeDone(t, done3, "filtered-select-node3")
	})

	if err := node1.host.Campaign(context.Background(), rangeOne.RangeID); err != nil {
		t.Fatalf("campaign first filtered range leaseholder: %v", err)
	}
	if err := node3.host.Campaign(context.Background(), rangeTwo.RangeID); err != nil {
		t.Fatalf("campaign second filtered range leaseholder: %v", err)
	}
	waitForRangeLeader(t, node1.host, rangeOne.RangeID, 27)
	waitForRangeLeader(t, node3.host, rangeTwo.RangeID, 32)

	conn := openPGConn(t, node2.state.PGAddr)
	defer conn.Close()
	for _, query := range []string{
		"insert into users (id, name, email) values (7, 'alice', 'a@example.com')",
		"insert into users (id, name, email) values (70, 'bob', 'b@example.com')",
		"insert into users (id, name, email) values (90, 'carol', 'c@example.com')",
	} {
		if _, err := conn.Write(queryFrame(query)); err != nil {
			t.Fatalf("write insert %q: %v", query, err)
		}
		if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
			t.Fatalf("insert frame tags for %q = %q, want CZ", query, got)
		}
	}

	selectConn := openPGConn(t, node2.state.PGAddr)
	defer selectConn.Close()
	if _, err := selectConn.Write(queryFrame("select id, name from users where name >= 'bob' order by name desc limit 2")); err != nil {
		t.Fatalf("write filtered select: %v", err)
	}
	frames := readFramesUntilReady(t, selectConn)
	if got := frameTags(frames); got != "TDDCZ" {
		t.Fatalf("filtered select frame tags = %q, want TDDCZ", got)
	}
	firstRow, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode first filtered row: %v", err)
	}
	secondRow, err := decodeDataRowFrame(frames[2])
	if err != nil {
		t.Fatalf("decode second filtered row: %v", err)
	}
	if len(firstRow) != 2 || string(firstRow[0]) != "90" || string(firstRow[1]) != "carol" {
		t.Fatalf("first filtered row = %q/%q, want 90/carol", firstRow[0], firstRow[1])
	}
	if len(secondRow) != 2 || string(secondRow[0]) != "70" || string(secondRow[1]) != "bob" {
		t.Fatalf("second filtered row = %q/%q, want 70/bob", secondRow[0], secondRow[1])
	}
}

func TestProcessNodeExecutesPointDeleteAcrossNodes(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    53,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 27, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 28, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 29, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 28,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-point-delete", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 27},
		{NodeID: 2, StoreID: 28},
		{NodeID: 3, StoreID: 29},
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
		waitProcessNodeDone(t, done1, "point-delete-node1")
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
		waitProcessNodeDone(t, done2, "point-delete-node2")
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
		waitProcessNodeDone(t, done3, "point-delete-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign delete leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 28)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (17, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("insert frame tags = %q, want CZ", got)
	}
	if _, err := conn.Write(queryFrame("delete from users where id = 17")); err != nil {
		t.Fatalf("write delete: %v", err)
	}
	deleteFrames := readFrames(t, conn, 2)
	if got := frameTags(deleteFrames); got != "CZ" {
		t.Fatalf("delete frame tags = %q, want CZ", got)
	}
	if tag := decodeCommandCompleteFrame(deleteFrames[0]); tag != "DELETE 1" {
		t.Fatalf("delete command tag = %q, want DELETE 1", tag)
	}

	selectConn := openPGConn(t, node3.state.PGAddr)
	defer selectConn.Close()
	if _, err := selectConn.Write(queryFrame("select id, name from users where id = 17")); err != nil {
		t.Fatalf("write delete check select: %v", err)
	}
	selectFrames := readFrames(t, selectConn, 3)
	if got := frameTags(selectFrames); got != "TCZ" {
		t.Fatalf("delete check frames = %q, want TCZ", got)
	}
}

func TestProcessNodeExecutesBoundedRangeDeleteAcrossLeaseholders(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	splitKey := usersPrimaryKey(50)
	rangeOne := meta.RangeDescriptor{
		RangeID:    54,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     splitKey,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 30, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 31, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 32, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 30,
	}
	rangeTwo := meta.RangeDescriptor{
		RangeID:    55,
		Generation: 1,
		StartKey:   splitKey,
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 33, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 34, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 35, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 35,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-range-delete", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 30},
		{NodeID: 2, StoreID: 31},
		{NodeID: 3, StoreID: 32},
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
		waitProcessNodeDone(t, done1, "range-delete-node1")
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
		waitProcessNodeDone(t, done2, "range-delete-node2")
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
		waitProcessNodeDone(t, done3, "range-delete-node3")
	})

	if err := node1.host.Campaign(context.Background(), rangeOne.RangeID); err != nil {
		t.Fatalf("campaign first delete leaseholder: %v", err)
	}
	if err := node3.host.Campaign(context.Background(), rangeTwo.RangeID); err != nil {
		t.Fatalf("campaign second delete leaseholder: %v", err)
	}
	waitForRangeLeader(t, node1.host, rangeOne.RangeID, 30)
	waitForRangeLeader(t, node3.host, rangeTwo.RangeID, 35)

	conn := openPGConn(t, node2.state.PGAddr)
	defer conn.Close()
	for _, query := range []string{
		"insert into users (id, name, email) values (7, 'alice', 'a@example.com')",
		"insert into users (id, name, email) values (70, 'bob', 'b@example.com')",
		"insert into users (id, name, email) values (90, 'carol', 'c@example.com')",
	} {
		if _, err := conn.Write(queryFrame(query)); err != nil {
			t.Fatalf("write insert %q: %v", query, err)
		}
		if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
			t.Fatalf("insert frame tags for %q = %q, want CZ", query, got)
		}
	}
	if _, err := conn.Write(queryFrame("delete from users where id >= 7 and id < 80")); err != nil {
		t.Fatalf("write range delete: %v", err)
	}
	deleteFrames := readFrames(t, conn, 2)
	if got := frameTags(deleteFrames); got != "CZ" {
		t.Fatalf("range delete frame tags = %q, want CZ", got)
	}
	if tag := decodeCommandCompleteFrame(deleteFrames[0]); tag != "DELETE 2" {
		t.Fatalf("range delete command tag = %q, want DELETE 2", tag)
	}

	scanConn := openPGConn(t, node2.state.PGAddr)
	defer scanConn.Close()
	if _, err := scanConn.Write(queryFrame("select id, name from users where id >= 1 and id < 100")); err != nil {
		t.Fatalf("write post-delete scan: %v", err)
	}
	frames := readFramesUntilReady(t, scanConn)
	if got := frameTags(frames); got != "TDCZ" {
		t.Fatalf("post-delete scan frames = %q, want TDCZ", got)
	}
	row, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode post-delete row: %v", err)
	}
	if len(row) != 2 || string(row[0]) != "90" || string(row[1]) != "carol" {
		t.Fatalf("remaining row = %q/%q, want 90/carol", row[0], row[1])
	}
}

func TestProcessNodeExecutesPointUpdateAcrossNodes(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    56,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 36, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 37, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 38, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 37,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-point-update", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 36},
		{NodeID: 2, StoreID: 37},
		{NodeID: 3, StoreID: 38},
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
		waitProcessNodeDone(t, done1, "point-update-node1")
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
		waitProcessNodeDone(t, done2, "point-update-node2")
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
		waitProcessNodeDone(t, done3, "point-update-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign update leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 37)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (17, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("insert frame tags = %q, want CZ", got)
	}
	if _, err := conn.Write(queryFrame("update users set name = 'ally', email = 'ally@example.com' where id = 17")); err != nil {
		t.Fatalf("write update: %v", err)
	}
	updateFrames := readFrames(t, conn, 2)
	if got := frameTags(updateFrames); got != "CZ" {
		t.Fatalf("update frame tags = %q, want CZ", got)
	}
	if tag := decodeCommandCompleteFrame(updateFrames[0]); tag != "UPDATE 1" {
		t.Fatalf("update command tag = %q, want UPDATE 1", tag)
	}

	selectConn := openPGConn(t, node3.state.PGAddr)
	defer selectConn.Close()
	if _, err := selectConn.Write(queryFrame("select id, name, email from users where id = 17")); err != nil {
		t.Fatalf("write update check select: %v", err)
	}
	frames := readFrames(t, selectConn, 4)
	if got := frameTags(frames); got != "TDCZ" {
		t.Fatalf("update check frames = %q, want TDCZ", got)
	}
	row, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode updated row: %v", err)
	}
	if len(row) != 3 || string(row[0]) != "17" || string(row[1]) != "ally" || string(row[2]) != "ally@example.com" {
		t.Fatalf("updated row = %q/%q/%q, want 17/ally/ally@example.com", row[0], row[1], row[2])
	}
}

func TestProcessNodeExecutesBoundedRangeUpdateAcrossLeaseholders(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	splitKey := usersPrimaryKey(50)
	rangeOne := meta.RangeDescriptor{
		RangeID:    57,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     splitKey,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 39, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 40, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 41, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 39,
	}
	rangeTwo := meta.RangeDescriptor{
		RangeID:    58,
		Generation: 1,
		StartKey:   splitKey,
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 42, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 43, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 44, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 44,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-range-update", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 39},
		{NodeID: 2, StoreID: 40},
		{NodeID: 3, StoreID: 41},
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
		waitProcessNodeDone(t, done1, "range-update-node1")
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
		waitProcessNodeDone(t, done2, "range-update-node2")
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
		waitProcessNodeDone(t, done3, "range-update-node3")
	})

	if err := node1.host.Campaign(context.Background(), rangeOne.RangeID); err != nil {
		t.Fatalf("campaign first update leaseholder: %v", err)
	}
	if err := node3.host.Campaign(context.Background(), rangeTwo.RangeID); err != nil {
		t.Fatalf("campaign second update leaseholder: %v", err)
	}
	waitForRangeLeader(t, node1.host, rangeOne.RangeID, 39)
	waitForRangeLeader(t, node3.host, rangeTwo.RangeID, 44)

	conn := openPGConn(t, node2.state.PGAddr)
	defer conn.Close()
	for _, query := range []string{
		"insert into users (id, name, email) values (7, 'alice', 'a@example.com')",
		"insert into users (id, name, email) values (70, 'bob', 'b@example.com')",
		"insert into users (id, name, email) values (90, 'carol', 'c@example.com')",
	} {
		if _, err := conn.Write(queryFrame(query)); err != nil {
			t.Fatalf("write insert %q: %v", query, err)
		}
		if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
			t.Fatalf("insert frame tags for %q = %q, want CZ", query, got)
		}
	}
	if _, err := conn.Write(queryFrame("update users set name = 'grouped' where id >= 7 and id < 80")); err != nil {
		t.Fatalf("write range update: %v", err)
	}
	updateFrames := readFrames(t, conn, 2)
	if got := frameTags(updateFrames); got != "CZ" {
		t.Fatalf("range update frame tags = %q, want CZ", got)
	}
	if tag := decodeCommandCompleteFrame(updateFrames[0]); tag != "UPDATE 2" {
		t.Fatalf("range update command tag = %q, want UPDATE 2", tag)
	}

	scanConn := openPGConn(t, node2.state.PGAddr)
	defer scanConn.Close()
	if _, err := scanConn.Write(queryFrame("select id, name, email from users where id >= 1 and id < 100")); err != nil {
		t.Fatalf("write post-update scan: %v", err)
	}
	frames := readFramesUntilReady(t, scanConn)
	if got := frameTags(frames); got != "TDDDCZ" {
		t.Fatalf("post-update scan frames = %q, want TDDDCZ", got)
	}
	row1, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode updated row 1: %v", err)
	}
	row2, err := decodeDataRowFrame(frames[2])
	if err != nil {
		t.Fatalf("decode updated row 2: %v", err)
	}
	row3, err := decodeDataRowFrame(frames[3])
	if err != nil {
		t.Fatalf("decode updated row 3: %v", err)
	}
	if len(row1) != 3 || string(row1[0]) != "7" || string(row1[1]) != "grouped" || string(row1[2]) != "a@example.com" {
		t.Fatalf("updated row 1 = %q/%q/%q, want 7/grouped/a@example.com", row1[0], row1[1], row1[2])
	}
	if len(row2) != 3 || string(row2[0]) != "70" || string(row2[1]) != "grouped" || string(row2[2]) != "b@example.com" {
		t.Fatalf("updated row 2 = %q/%q/%q, want 70/grouped/b@example.com", row2[0], row2[1], row2[2])
	}
	if len(row3) != 3 || string(row3[0]) != "90" || string(row3[1]) != "carol" || string(row3[2]) != "c@example.com" {
		t.Fatalf("updated row 3 = %q/%q/%q, want 90/carol/c@example.com", row3[0], row3[1], row3[2])
	}
}

func TestProcessNodeExecutesPrimaryKeyUpsertAcrossNodes(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    59,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 45, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 46, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 47, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 46,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-point-upsert", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 45},
		{NodeID: 2, StoreID: 46},
		{NodeID: 3, StoreID: 47},
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
		waitProcessNodeDone(t, done1, "upsert-node1")
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
		waitProcessNodeDone(t, done2, "upsert-node2")
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
		waitProcessNodeDone(t, done3, "upsert-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign upsert leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 46)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (31, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("insert frame tags = %q, want CZ", got)
	}
	if _, err := conn.Write(queryFrame("upsert into users (id, name, email) values (31, 'ally', 'ally@example.com')")); err != nil {
		t.Fatalf("write upsert: %v", err)
	}
	upsertFrames := readFrames(t, conn, 2)
	if got := frameTags(upsertFrames); got != "CZ" {
		t.Fatalf("upsert frame tags = %q, want CZ", got)
	}
	if tag := decodeCommandCompleteFrame(upsertFrames[0]); tag != "UPSERT 1" {
		t.Fatalf("upsert command tag = %q, want UPSERT 1", tag)
	}

	selectConn := openPGConn(t, node3.state.PGAddr)
	defer selectConn.Close()
	if _, err := selectConn.Write(queryFrame("select id, name, email from users where id = 31")); err != nil {
		t.Fatalf("write upsert check select: %v", err)
	}
	frames := readFrames(t, selectConn, 4)
	if got := frameTags(frames); got != "TDCZ" {
		t.Fatalf("upsert check frames = %q, want TDCZ", got)
	}
	row, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode upserted row: %v", err)
	}
	if len(row) != 3 || string(row[0]) != "31" || string(row[1]) != "ally" || string(row[2]) != "ally@example.com" {
		t.Fatalf("upserted row = %q/%q/%q, want 31/ally/ally@example.com", row[0], row[1], row[2])
	}
}

func TestProcessNodeRejectsDuplicatePrimaryKeyInsert(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    60,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 48, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 49, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 50, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 49,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-duplicate-insert", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 48},
		{NodeID: 2, StoreID: 49},
		{NodeID: 3, StoreID: 50},
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
		waitProcessNodeDone(t, done1, "dup-node1")
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
		waitProcessNodeDone(t, done2, "dup-node2")
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
		waitProcessNodeDone(t, done3, "dup-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign duplicate insert leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 49)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (41, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write first insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("first insert frame tags = %q, want CZ", got)
	}
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (41, 'ally', 'ally@example.com')")); err != nil {
		t.Fatalf("write duplicate insert: %v", err)
	}
	dupFrames := readFrames(t, conn, 2)
	if got := frameTags(dupFrames); got != "EZ" {
		t.Fatalf("duplicate insert frame tags = %q, want EZ", got)
	}
	if message := decodeErrorMessageFrame(dupFrames[0]); !strings.Contains(message, "duplicate key value") {
		t.Fatalf("duplicate insert error = %q, want duplicate key message", message)
	}

	checkConn := openPGConn(t, node3.state.PGAddr)
	defer checkConn.Close()
	if _, err := checkConn.Write(queryFrame("select id, name, email from users where id = 41")); err != nil {
		t.Fatalf("write duplicate check select: %v", err)
	}
	checkFrames := readFrames(t, checkConn, 4)
	if got := frameTags(checkFrames); got != "TDCZ" {
		t.Fatalf("duplicate check frames = %q, want TDCZ", got)
	}
	row, err := decodeDataRowFrame(checkFrames[1])
	if err != nil {
		t.Fatalf("decode duplicate check row: %v", err)
	}
	if len(row) != 3 || string(row[0]) != "41" || string(row[1]) != "alice" || string(row[2]) != "a@example.com" {
		t.Fatalf("duplicate check row = %q/%q/%q, want 41/alice/a@example.com", row[0], row[1], row[2])
	}
}

func TestProcessNodeRejectsDuplicateUniqueIndexInsert(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    70,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 71, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 72, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 73, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 72,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-duplicate-unique-insert", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 71},
		{NodeID: 2, StoreID: 72},
		{NodeID: 3, StoreID: 73},
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
		waitProcessNodeDone(t, done1, "dup-unique-node1")
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
		waitProcessNodeDone(t, done2, "dup-unique-node2")
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
		waitProcessNodeDone(t, done3, "dup-unique-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign duplicate unique insert leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 72)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (51, 'alice', 'shared@example.com')")); err != nil {
		t.Fatalf("write first unique insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("first unique insert frame tags = %q, want CZ", got)
	}
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (52, 'ally', 'shared@example.com')")); err != nil {
		t.Fatalf("write duplicate unique insert: %v", err)
	}
	dupFrames := readFrames(t, conn, 2)
	if got := frameTags(dupFrames); got != "EZ" {
		t.Fatalf("duplicate unique insert frame tags = %q, want EZ", got)
	}
	if message := decodeErrorMessageFrame(dupFrames[0]); !strings.Contains(message, "users_email_key") {
		t.Fatalf("duplicate unique insert error = %q, want users_email_key", message)
	}

	table := usersTableDescriptor(t)
	waitForDecodedUserRow(t, node3.kv, table, usersPrimaryKey(51), "51", "alice")
	waitForKeyAbsent(t, node3.kv, usersPrimaryKey(52))
}

func TestProcessNodeRejectsUniqueIndexConflictOnUpdate(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    74,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 75, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 76, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 77, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 76,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-duplicate-unique-update", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 75},
		{NodeID: 2, StoreID: 76},
		{NodeID: 3, StoreID: 77},
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
		waitProcessNodeDone(t, done1, "dup-update-node1")
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
		waitProcessNodeDone(t, done2, "dup-update-node2")
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
		waitProcessNodeDone(t, done3, "dup-update-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign duplicate update leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 76)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	for _, query := range []string{
		"insert into users (id, name, email) values (61, 'alice', 'alice@example.com')",
		"insert into users (id, name, email) values (62, 'ally', 'ally@example.com')",
	} {
		if _, err := conn.Write(queryFrame(query)); err != nil {
			t.Fatalf("write seed query %q: %v", query, err)
		}
		if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
			t.Fatalf("seed query frame tags = %q, want CZ", got)
		}
	}

	if _, err := conn.Write(queryFrame("update users set email = 'alice@example.com' where id = 62")); err != nil {
		t.Fatalf("write duplicate update: %v", err)
	}
	dupFrames := readFrames(t, conn, 2)
	if got := frameTags(dupFrames); got != "EZ" {
		t.Fatalf("duplicate update frame tags = %q, want EZ", got)
	}
	if message := decodeErrorMessageFrame(dupFrames[0]); !strings.Contains(message, "users_email_key") {
		t.Fatalf("duplicate update error = %q, want users_email_key", message)
	}

	table := usersTableDescriptor(t)
	waitForDecodedUserRow(t, node3.kv, table, usersPrimaryKey(62), "62", "ally")
}

func TestProcessNodeMaintainsSecondaryIndexesAcrossUpdateAndDelete(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    78,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 79, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 80, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 81, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 80,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-secondary-index-maintenance", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 79},
		{NodeID: 2, StoreID: 80},
		{NodeID: 3, StoreID: 81},
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
		waitProcessNodeDone(t, done1, "index-maintenance-node1")
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
		waitProcessNodeDone(t, done2, "index-maintenance-node2")
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
		waitProcessNodeDone(t, done3, "index-maintenance-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign index maintenance leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 80)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (71, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write maintenance insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("maintenance insert frame tags = %q, want CZ", got)
	}

	oldNameKey, oldEmailKey, oldPK := usersIndexKeys(t, 71, "alice", "a@example.com")
	waitForKeyValue(t, node3.kv, oldNameKey, oldPK)
	waitForKeyValue(t, node3.kv, oldEmailKey, oldPK)

	if _, err := conn.Write(queryFrame("update users set name = 'ally', email = 'ally@example.com' where id = 71")); err != nil {
		t.Fatalf("write maintenance update: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("maintenance update frame tags = %q, want CZ", got)
	}

	newNameKey, newEmailKey, newPK := usersIndexKeys(t, 71, "ally", "ally@example.com")
	waitForKeyAbsent(t, node3.kv, oldNameKey)
	waitForKeyAbsent(t, node3.kv, oldEmailKey)
	waitForKeyValue(t, node3.kv, newNameKey, newPK)
	waitForKeyValue(t, node3.kv, newEmailKey, newPK)

	if _, err := conn.Write(queryFrame("delete from users where id = 71")); err != nil {
		t.Fatalf("write maintenance delete: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("maintenance delete frame tags = %q, want CZ", got)
	}

	waitForKeyAbsent(t, node3.kv, usersPrimaryKey(71))
	waitForKeyAbsent(t, node3.kv, newNameKey)
	waitForKeyAbsent(t, node3.kv, newEmailKey)
}

func TestProcessNodeExecutesDMLReturning(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    81,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 83, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 84, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 85, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 84,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-dml-returning", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 83},
		{NodeID: 2, StoreID: 84},
		{NodeID: 3, StoreID: 85},
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
		waitProcessNodeDone(t, done1, "returning-node1")
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
		waitProcessNodeDone(t, done2, "returning-node2")
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
		waitProcessNodeDone(t, done3, "returning-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign returning leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 84)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()

	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (51, 'alice', 'a@example.com') returning id, name")); err != nil {
		t.Fatalf("write insert returning: %v", err)
	}
	insertFrames := readFrames(t, conn, 4)
	if got := frameTags(insertFrames); got != "TDCZ" {
		t.Fatalf("insert returning frames = %q, want TDCZ", got)
	}
	insertRow, err := decodeDataRowFrame(insertFrames[1])
	if err != nil {
		t.Fatalf("decode insert returning row: %v", err)
	}
	if len(insertRow) != 2 || string(insertRow[0]) != "51" || string(insertRow[1]) != "alice" {
		t.Fatalf("insert returning row = %q/%q, want 51/alice", insertRow[0], insertRow[1])
	}

	if _, err := conn.Write(queryFrame("update users set email = 'ally@example.com' where id = 51 returning id, email")); err != nil {
		t.Fatalf("write update returning: %v", err)
	}
	updateFrames := readFrames(t, conn, 4)
	if got := frameTags(updateFrames); got != "TDCZ" {
		t.Fatalf("update returning frames = %q, want TDCZ", got)
	}
	updateRow, err := decodeDataRowFrame(updateFrames[1])
	if err != nil {
		t.Fatalf("decode update returning row: %v", err)
	}
	if len(updateRow) != 2 || string(updateRow[0]) != "51" || string(updateRow[1]) != "ally@example.com" {
		t.Fatalf("update returning row = %q/%q, want 51/ally@example.com", updateRow[0], updateRow[1])
	}

	if _, err := conn.Write(queryFrame("upsert into users (id, name, email) values (51, 'ally', 'ally@example.com') returning id, name, email")); err != nil {
		t.Fatalf("write upsert returning: %v", err)
	}
	upsertFrames := readFrames(t, conn, 4)
	if got := frameTags(upsertFrames); got != "TDCZ" {
		t.Fatalf("upsert returning frames = %q, want TDCZ", got)
	}
	upsertRow, err := decodeDataRowFrame(upsertFrames[1])
	if err != nil {
		t.Fatalf("decode upsert returning row: %v", err)
	}
	if len(upsertRow) != 3 || string(upsertRow[0]) != "51" || string(upsertRow[1]) != "ally" || string(upsertRow[2]) != "ally@example.com" {
		t.Fatalf("upsert returning row = %q/%q/%q, want 51/ally/ally@example.com", upsertRow[0], upsertRow[1], upsertRow[2])
	}

	if _, err := conn.Write(queryFrame("delete from users where id = 51 returning id, name")); err != nil {
		t.Fatalf("write delete returning: %v", err)
	}
	deleteFrames := readFrames(t, conn, 4)
	if got := frameTags(deleteFrames); got != "TDCZ" {
		t.Fatalf("delete returning frames = %q, want TDCZ", got)
	}
	deleteRow, err := decodeDataRowFrame(deleteFrames[1])
	if err != nil {
		t.Fatalf("decode delete returning row: %v", err)
	}
	if len(deleteRow) != 2 || string(deleteRow[0]) != "51" || string(deleteRow[1]) != "ally" {
		t.Fatalf("delete returning row = %q/%q, want 51/ally", deleteRow[0], deleteRow[1])
	}

	checkConn := openPGConn(t, node3.state.PGAddr)
	defer checkConn.Close()
	if _, err := checkConn.Write(queryFrame("select id, name from users where id = 51")); err != nil {
		t.Fatalf("write final select: %v", err)
	}
	checkFrames := readFrames(t, checkConn, 3)
	if got := frameTags(checkFrames); got != "TCZ" {
		t.Fatalf("final returning check frames = %q, want TCZ", got)
	}
}

func TestProcessNodeExecutesOnConflictDoNothing(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    86,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 87, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 88, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 89, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 88,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-on-conflict-do-nothing", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 87},
		{NodeID: 2, StoreID: 88},
		{NodeID: 3, StoreID: 89},
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
		waitProcessNodeDone(t, done1, "on-conflict-nothing-node1")
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
		waitProcessNodeDone(t, done2, "on-conflict-nothing-node2")
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
		waitProcessNodeDone(t, done3, "on-conflict-nothing-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign on conflict do nothing leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 88)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (91, 'alice', 'shared@example.com')")); err != nil {
		t.Fatalf("write seed insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("seed insert frame tags = %q, want CZ", got)
	}

	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (92, 'ally', 'shared@example.com') on conflict (email) do nothing returning id, name")); err != nil {
		t.Fatalf("write on conflict do nothing: %v", err)
	}
	frames := readFrames(t, conn, 3)
	if got := frameTags(frames); got != "TCZ" {
		t.Fatalf("on conflict do nothing frames = %q, want TCZ", got)
	}
	if tag := decodeCommandCompleteFrame(frames[1]); tag != "INSERT 0 0" {
		t.Fatalf("on conflict do nothing tag = %q, want INSERT 0 0", tag)
	}

	table := usersTableDescriptor(t)
	waitForDecodedUserRow(t, node3.kv, table, usersPrimaryKey(91), "91", "alice")
	waitForKeyAbsent(t, node3.kv, usersPrimaryKey(92))
}

func TestProcessNodeExecutesOnConflictDoUpdateReturning(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    90,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 91, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 92, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 93, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 92,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-on-conflict-do-update", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 91},
		{NodeID: 2, StoreID: 92},
		{NodeID: 3, StoreID: 93},
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
		waitProcessNodeDone(t, done1, "on-conflict-update-node1")
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
		waitProcessNodeDone(t, done2, "on-conflict-update-node2")
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
		waitProcessNodeDone(t, done3, "on-conflict-update-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign on conflict do update leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 92)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (101, 'alice', 'shared@example.com')")); err != nil {
		t.Fatalf("write seed insert: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("seed insert frame tags = %q, want CZ", got)
	}

	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (102, 'ally', 'shared@example.com') on conflict (email) do update set name = excluded.name returning id, name, email")); err != nil {
		t.Fatalf("write on conflict do update: %v", err)
	}
	frames := readFrames(t, conn, 4)
	if got := frameTags(frames); got != "TDCZ" {
		t.Fatalf("on conflict do update frames = %q, want TDCZ", got)
	}
	row, err := decodeDataRowFrame(frames[1])
	if err != nil {
		t.Fatalf("decode on conflict do update row: %v", err)
	}
	if len(row) != 3 || string(row[0]) != "101" || string(row[1]) != "ally" || string(row[2]) != "shared@example.com" {
		t.Fatalf("on conflict do update row = %q/%q/%q, want 101/ally/shared@example.com", row[0], row[1], row[2])
	}

	table := usersTableDescriptor(t)
	waitForDecodedUserRow(t, node3.kv, table, usersPrimaryKey(101), "101", "ally")
	waitForKeyAbsent(t, node3.kv, usersPrimaryKey(102))
}

func TestProcessNodeExecutesOnConflictDoUpdateInExplicitTransaction(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    94,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 95, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 96, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 97, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 96,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-on-conflict-explicit", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 95},
		{NodeID: 2, StoreID: 96},
		{NodeID: 3, StoreID: 97},
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
		waitProcessNodeDone(t, done1, "on-conflict-explicit-node1")
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
		waitProcessNodeDone(t, done2, "on-conflict-explicit-node2")
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
		waitProcessNodeDone(t, done3, "on-conflict-explicit-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign on conflict explicit leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 96)

	seedConn := openPGConn(t, node1.state.PGAddr)
	defer seedConn.Close()
	if _, err := seedConn.Write(queryFrame("insert into users (id, name, email) values (111, 'alice', 'shared@example.com')")); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	if got := frameTags(readFrames(t, seedConn, 2)); got != "CZ" {
		t.Fatalf("seed insert frames = %q, want CZ", got)
	}

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("begin")); err != nil {
		t.Fatalf("write begin: %v", err)
	}
	beginFrames := readFrames(t, conn, 2)
	if got := frameTags(beginFrames); got != "CZ" || readyStatus(beginFrames[1]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("begin frames = %q / %d, want CZ / in-transaction", got, readyStatus(beginFrames[1]))
	}

	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (112, 'ally', 'shared@example.com') on conflict (email) do update set name = excluded.name returning id, name")); err != nil {
		t.Fatalf("write on conflict do update in txn: %v", err)
	}
	updateFrames := readFrames(t, conn, 4)
	if got := frameTags(updateFrames); got != "TDCZ" || readyStatus(updateFrames[3]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("on conflict in-txn frames = %q / %d, want TDCZ / in-transaction", got, readyStatus(updateFrames[3]))
	}
	updateRow, err := decodeDataRowFrame(updateFrames[1])
	if err != nil {
		t.Fatalf("decode on conflict in-txn row: %v", err)
	}
	if len(updateRow) != 2 || string(updateRow[0]) != "111" || string(updateRow[1]) != "ally" {
		t.Fatalf("on conflict in-txn row = %q/%q, want 111/ally", updateRow[0], updateRow[1])
	}

	if _, err := conn.Write(queryFrame("select id, name from users where id = 111")); err != nil {
		t.Fatalf("write in-txn select: %v", err)
	}
	inTxnFrames := readFrames(t, conn, 4)
	if got := frameTags(inTxnFrames); got != "TDCZ" || readyStatus(inTxnFrames[3]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("in-txn select frames = %q / %d, want TDCZ / in-transaction", got, readyStatus(inTxnFrames[3]))
	}
	inTxnRow, err := decodeDataRowFrame(inTxnFrames[1])
	if err != nil {
		t.Fatalf("decode in-txn select row: %v", err)
	}
	if len(inTxnRow) != 2 || string(inTxnRow[0]) != "111" || string(inTxnRow[1]) != "ally" {
		t.Fatalf("in-txn row = %q/%q, want 111/ally", inTxnRow[0], inTxnRow[1])
	}

	otherConn := openPGConn(t, node3.state.PGAddr)
	defer otherConn.Close()
	if _, err := otherConn.Write(queryFrame("select id, name from users where id = 111")); err != nil {
		t.Fatalf("write external select: %v", err)
	}
	otherFrames := readFrames(t, otherConn, 4)
	if got := frameTags(otherFrames); got != "TDCZ" {
		t.Fatalf("external select frames = %q, want TDCZ", got)
	}
	otherRow, err := decodeDataRowFrame(otherFrames[1])
	if err != nil {
		t.Fatalf("decode external row: %v", err)
	}
	if len(otherRow) != 2 || string(otherRow[0]) != "111" || string(otherRow[1]) != "alice" {
		t.Fatalf("external row = %q/%q, want 111/alice", otherRow[0], otherRow[1])
	}

	if _, err := conn.Write(queryFrame("commit")); err != nil {
		t.Fatalf("write commit: %v", err)
	}
	commitFrames := readFrames(t, conn, 2)
	if got := frameTags(commitFrames); got != "CZ" || readyStatus(commitFrames[1]) != byte(pgwire.TxIdle) {
		t.Fatalf("commit frames = %q / %d, want CZ / idle", got, readyStatus(commitFrames[1]))
	}

	finalConn := openPGConn(t, node2.state.PGAddr)
	defer finalConn.Close()
	if _, err := finalConn.Write(queryFrame("select id, name from users where id = 111")); err != nil {
		t.Fatalf("write final select: %v", err)
	}
	finalFrames := readFrames(t, finalConn, 4)
	if got := frameTags(finalFrames); got != "TDCZ" {
		t.Fatalf("final select frames = %q, want TDCZ", got)
	}
	finalRow, err := decodeDataRowFrame(finalFrames[1])
	if err != nil {
		t.Fatalf("decode final row: %v", err)
	}
	if len(finalRow) != 2 || string(finalRow[0]) != "111" || string(finalRow[1]) != "ally" {
		t.Fatalf("final row = %q/%q, want 111/ally", finalRow[0], finalRow[1])
	}

	waitForKeyAbsent(t, node3.kv, usersPrimaryKey(112))
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

func TestProcessNodeExecutesExplicitTransactionCommit(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    63,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 41, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 42, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 43, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 42,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-explicit-txn", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 41},
		{NodeID: 2, StoreID: 42},
		{NodeID: 3, StoreID: 43},
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
		waitProcessNodeDone(t, done1, "txn-node1")
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
		waitProcessNodeDone(t, done2, "txn-node2")
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
		waitProcessNodeDone(t, done3, "txn-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 42)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("begin")); err != nil {
		t.Fatalf("write begin: %v", err)
	}
	beginFrames := readFrames(t, conn, 2)
	if got := frameTags(beginFrames); got != "CZ" || readyStatus(beginFrames[1]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("begin frames = %q/%q, want CZ/T", got, readyStatus(beginFrames[1]))
	}
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (7, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write insert in txn: %v", err)
	}
	insertFrames := readFrames(t, conn, 2)
	if got := frameTags(insertFrames); got != "CZ" || readyStatus(insertFrames[1]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("insert frames = %q/%q, want CZ/T", got, readyStatus(insertFrames[1]))
	}
	if _, err := conn.Write(queryFrame("select id, name from users where id = 7")); err != nil {
		t.Fatalf("write select in txn: %v", err)
	}
	selectFrames := readFrames(t, conn, 4)
	if got := frameTags(selectFrames); got != "TDCZ" || readyStatus(selectFrames[3]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("select frames = %q/%q, want TDCZ/T", got, readyStatus(selectFrames[3]))
	}

	otherConn := openPGConn(t, node3.state.PGAddr)
	defer otherConn.Close()
	if _, err := otherConn.Write(queryFrame("select id, name from users where id = 7")); err != nil {
		t.Fatalf("write external select before commit: %v", err)
	}
	otherFrames := readFrames(t, otherConn, 3)
	if got := frameTags(otherFrames); got != "TCZ" {
		t.Fatalf("pre-commit external select frames = %q, want TCZ", got)
	}

	if _, err := conn.Write(queryFrame("commit")); err != nil {
		t.Fatalf("write commit: %v", err)
	}
	commitFrames := readFrames(t, conn, 2)
	if got := frameTags(commitFrames); got != "CZ" || readyStatus(commitFrames[1]) != byte(pgwire.TxIdle) {
		t.Fatalf("commit frames = %q/%q, want CZ/I", got, readyStatus(commitFrames[1]))
	}

	finalConn := openPGConn(t, node3.state.PGAddr)
	defer finalConn.Close()
	if _, err := finalConn.Write(queryFrame("select id, name from users where id = 7")); err != nil {
		t.Fatalf("write external select after commit: %v", err)
	}
	finalFrames := readFrames(t, finalConn, 4)
	if got := frameTags(finalFrames); got != "TDCZ" {
		t.Fatalf("post-commit external select frames = %q, want TDCZ", got)
	}
}

func TestProcessNodeExecutesExplicitTransactionRollback(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    64,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 44, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 45, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 46, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 45,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-explicit-rollback", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 44},
		{NodeID: 2, StoreID: 45},
		{NodeID: 3, StoreID: 46},
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
		waitProcessNodeDone(t, done1, "rollback-node1")
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
		waitProcessNodeDone(t, done2, "rollback-node2")
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
		waitProcessNodeDone(t, done3, "rollback-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign rollback leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 45)

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	for _, query := range []string{
		"begin",
		"insert into users (id, name, email) values (9, 'bob', 'b@example.com')",
		"rollback",
	} {
		if _, err := conn.Write(queryFrame(query)); err != nil {
			t.Fatalf("write %q: %v", query, err)
		}
		frames := readFrames(t, conn, 2)
		if got := frameTags(frames); got != "CZ" {
			t.Fatalf("%q frame tags = %q, want CZ", query, got)
		}
	}

	checkConn := openPGConn(t, node3.state.PGAddr)
	defer checkConn.Close()
	if _, err := checkConn.Write(queryFrame("select id, name from users where id = 9")); err != nil {
		t.Fatalf("write rollback check select: %v", err)
	}
	checkFrames := readFrames(t, checkConn, 3)
	if got := frameTags(checkFrames); got != "TCZ" {
		t.Fatalf("rollback check frames = %q, want TCZ", got)
	}
}

func TestProcessNodeExecutesExplicitTransactionDeleteCommit(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    64,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 47, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 48, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 49, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 48,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-explicit-delete", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 47},
		{NodeID: 2, StoreID: 48},
		{NodeID: 3, StoreID: 49},
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
		waitProcessNodeDone(t, done1, "explicit-delete-node1")
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
		waitProcessNodeDone(t, done2, "explicit-delete-node2")
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
		waitProcessNodeDone(t, done3, "explicit-delete-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign explicit delete leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 48)

	seedConn := openPGConn(t, node1.state.PGAddr)
	defer seedConn.Close()
	if _, err := seedConn.Write(queryFrame("insert into users (id, name, email) values (29, 'dana', 'd@example.com')")); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	if got := frameTags(readFrames(t, seedConn, 2)); got != "CZ" {
		t.Fatalf("seed insert frames = %q, want CZ", got)
	}

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	for _, query := range []string{"begin", "delete from users where id = 29"} {
		if _, err := conn.Write(queryFrame(query)); err != nil {
			t.Fatalf("write %q: %v", query, err)
		}
		frames := readFrames(t, conn, 2)
		if got := frameTags(frames); got != "CZ" {
			t.Fatalf("%q frame tags = %q, want CZ", query, got)
		}
	}

	if _, err := conn.Write(queryFrame("select id, name from users where id = 29")); err != nil {
		t.Fatalf("write in-txn select: %v", err)
	}
	inTxnFrames := readFrames(t, conn, 3)
	if got := frameTags(inTxnFrames); got != "TCZ" || readyStatus(inTxnFrames[2]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("in-txn delete select frames = %q/%q, want TCZ/T", got, readyStatus(inTxnFrames[2]))
	}

	otherConn := openPGConn(t, node3.state.PGAddr)
	defer otherConn.Close()
	if _, err := otherConn.Write(queryFrame("select id, name from users where id = 29")); err != nil {
		t.Fatalf("write external pre-commit select: %v", err)
	}
	otherFrames := readFrames(t, otherConn, 4)
	if got := frameTags(otherFrames); got != "TDCZ" {
		t.Fatalf("pre-commit external select frames = %q, want TDCZ", got)
	}

	if _, err := conn.Write(queryFrame("commit")); err != nil {
		t.Fatalf("write commit: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("commit frames = %q, want CZ", got)
	}

	finalConn := openPGConn(t, node3.state.PGAddr)
	defer finalConn.Close()
	if _, err := finalConn.Write(queryFrame("select id, name from users where id = 29")); err != nil {
		t.Fatalf("write external post-commit select: %v", err)
	}
	finalFrames := readFrames(t, finalConn, 3)
	if got := frameTags(finalFrames); got != "TCZ" {
		t.Fatalf("post-commit external select frames = %q, want TCZ", got)
	}
}

func TestProcessNodeExecutesExplicitTransactionUpdateCommit(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    80,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 80, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 81, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 82, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 81,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-explicit-update", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 80},
		{NodeID: 2, StoreID: 81},
		{NodeID: 3, StoreID: 82},
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
		waitProcessNodeDone(t, done1, "explicit-update-node1")
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
		waitProcessNodeDone(t, done2, "explicit-update-node2")
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
		waitProcessNodeDone(t, done3, "explicit-update-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign explicit update leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 81)

	seedConn := openPGConn(t, node1.state.PGAddr)
	defer seedConn.Close()
	if _, err := seedConn.Write(queryFrame("insert into users (id, name, email) values (29, 'dana', 'd@example.com')")); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	if got := frameTags(readFrames(t, seedConn, 2)); got != "CZ" {
		t.Fatalf("seed insert frames = %q, want CZ", got)
	}

	conn := openPGConn(t, node1.state.PGAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("begin")); err != nil {
		t.Fatalf("write begin: %v", err)
	}
	beginFrames := readFrames(t, conn, 2)
	if got := frameTags(beginFrames); got != "CZ" || readyStatus(beginFrames[1]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("begin frames = %q/%q, want CZ/T", got, readyStatus(beginFrames[1]))
	}
	if _, err := conn.Write(queryFrame("update users set name = 'ally' where id = 29")); err != nil {
		t.Fatalf("write update in txn: %v", err)
	}
	updateFrames := readFrames(t, conn, 2)
	if got := frameTags(updateFrames); got != "CZ" || readyStatus(updateFrames[1]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("update frames = %q/%q, want CZ/T", got, readyStatus(updateFrames[1]))
	}
	if tag := decodeCommandCompleteFrame(updateFrames[0]); tag != "UPDATE 1" {
		t.Fatalf("update command tag = %q, want UPDATE 1", tag)
	}
	if _, err := conn.Write(queryFrame("select id, name from users where id = 29")); err != nil {
		t.Fatalf("write in-txn select: %v", err)
	}
	inTxnFrames := readFrames(t, conn, 4)
	if got := frameTags(inTxnFrames); got != "TDCZ" || readyStatus(inTxnFrames[3]) != byte(pgwire.TxInTransaction) {
		t.Fatalf("in-txn select frames = %q/%q, want TDCZ/T", got, readyStatus(inTxnFrames[3]))
	}
	inTxnRow, err := decodeDataRowFrame(inTxnFrames[1])
	if err != nil {
		t.Fatalf("decode in-txn row: %v", err)
	}
	if len(inTxnRow) != 2 || string(inTxnRow[0]) != "29" || string(inTxnRow[1]) != "ally" {
		t.Fatalf("in-txn row = %q/%q, want 29/ally", inTxnRow[0], inTxnRow[1])
	}

	otherConn := openPGConn(t, node3.state.PGAddr)
	defer otherConn.Close()
	if _, err := otherConn.Write(queryFrame("select id, name from users where id = 29")); err != nil {
		t.Fatalf("write external pre-commit select: %v", err)
	}
	otherFrames := readFrames(t, otherConn, 4)
	if got := frameTags(otherFrames); got != "TDCZ" {
		t.Fatalf("pre-commit external select frames = %q, want TDCZ", got)
	}
	otherRow, err := decodeDataRowFrame(otherFrames[1])
	if err != nil {
		t.Fatalf("decode pre-commit external row: %v", err)
	}
	if len(otherRow) != 2 || string(otherRow[0]) != "29" || string(otherRow[1]) != "dana" {
		t.Fatalf("pre-commit external row = %q/%q, want 29/dana", otherRow[0], otherRow[1])
	}

	if _, err := conn.Write(queryFrame("commit")); err != nil {
		t.Fatalf("write commit: %v", err)
	}
	commitFrames := readFrames(t, conn, 2)
	if got := frameTags(commitFrames); got != "CZ" || readyStatus(commitFrames[1]) != byte(pgwire.TxIdle) {
		t.Fatalf("commit frames = %q/%q, want CZ/I", got, readyStatus(commitFrames[1]))
	}

	finalConn := openPGConn(t, node3.state.PGAddr)
	defer finalConn.Close()
	if _, err := finalConn.Write(queryFrame("select id, name from users where id = 29")); err != nil {
		t.Fatalf("write external post-commit select: %v", err)
	}
	finalFrames := readFrames(t, finalConn, 4)
	if got := frameTags(finalFrames); got != "TDCZ" {
		t.Fatalf("post-commit external select frames = %q, want TDCZ", got)
	}
	finalRow, err := decodeDataRowFrame(finalFrames[1])
	if err != nil {
		t.Fatalf("decode post-commit external row: %v", err)
	}
	if len(finalRow) != 2 || string(finalRow[0]) != "29" || string(finalRow[1]) != "ally" {
		t.Fatalf("post-commit external row = %q/%q, want 29/ally", finalRow[0], finalRow[1])
	}
}

func TestProcessNodeExecutesExplicitMultiRangeTransactionCommit(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	splitKey := usersPrimaryKey(50)
	rangeOne := meta.RangeDescriptor{
		RangeID:    65,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     splitKey,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 47, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 48, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 49, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 47,
	}
	rangeTwo := meta.RangeDescriptor{
		RangeID:    66,
		Generation: 1,
		StartKey:   splitKey,
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 50, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 51, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 52, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 52,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-explicit-multirange", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 47},
		{NodeID: 2, StoreID: 48},
		{NodeID: 3, StoreID: 49},
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
		waitProcessNodeDone(t, done1, "multi-node1")
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
		waitProcessNodeDone(t, done2, "multi-node2")
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
		waitProcessNodeDone(t, done3, "multi-node3")
	})

	if err := node1.host.Campaign(context.Background(), rangeOne.RangeID); err != nil {
		t.Fatalf("campaign first multirange leaseholder: %v", err)
	}
	if err := node3.host.Campaign(context.Background(), rangeTwo.RangeID); err != nil {
		t.Fatalf("campaign second multirange leaseholder: %v", err)
	}
	waitForRangeLeader(t, node1.host, rangeOne.RangeID, 47)
	waitForRangeLeader(t, node3.host, rangeTwo.RangeID, 52)

	conn := openPGConn(t, node2.state.PGAddr)
	defer conn.Close()
	for _, query := range []string{
		"begin",
		"insert into users (id, name, email) values (7, 'alice', 'a@example.com')",
		"insert into users (id, name, email) values (70, 'bob', 'b@example.com')",
		"commit",
	} {
		if _, err := conn.Write(queryFrame(query)); err != nil {
			t.Fatalf("write %q: %v", query, err)
		}
		frames := readFrames(t, conn, 2)
		if got := frameTags(frames); got != "CZ" {
			t.Fatalf("%q frame tags = %q, want CZ", query, got)
		}
	}

	scanConn := openPGConn(t, node2.state.PGAddr)
	defer scanConn.Close()
	if _, err := scanConn.Write(queryFrame("select id, name from users where id >= 7 and id < 80")); err != nil {
		t.Fatalf("write multirange select: %v", err)
	}
	frames := readFramesUntilReady(t, scanConn)
	if got := frameTags(frames); got != "TDDCZ" {
		t.Fatalf("multirange select frames = %q, want TDDCZ", got)
	}
}

func TestProcessNodeAbortsExplicitTransactionOnDisconnect(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    67,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 53, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 54, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 55, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 54,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-explicit-disconnect", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 53},
		{NodeID: 2, StoreID: 54},
		{NodeID: 3, StoreID: 55},
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
		waitProcessNodeDone(t, done1, "disconnect-node1")
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
		waitProcessNodeDone(t, done2, "disconnect-node2")
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
		waitProcessNodeDone(t, done3, "disconnect-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign disconnect leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 54)

	conn := openPGConn(t, node1.state.PGAddr)
	if _, err := conn.Write(queryFrame("begin")); err != nil {
		t.Fatalf("write begin: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("begin frame tags = %q, want CZ", got)
	}
	if _, err := conn.Write(queryFrame("insert into users (id, name, email) values (11, 'carol', 'c@example.com')")); err != nil {
		t.Fatalf("write insert before disconnect: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 2)); got != "CZ" {
		t.Fatalf("insert frame tags = %q, want CZ", got)
	}
	_ = conn.Close()

	time.Sleep(200 * time.Millisecond)

	checkConn := openPGConn(t, node3.state.PGAddr)
	defer checkConn.Close()
	if _, err := checkConn.Write(queryFrame("select id, name from users where id = 11")); err != nil {
		t.Fatalf("write select after disconnect: %v", err)
	}
	checkFrames := readFrames(t, checkConn, 3)
	if got := frameTags(checkFrames); got != "TCZ" {
		t.Fatalf("disconnect check frames = %q, want TCZ", got)
	}
}

func TestProcessNodeRejectsExplicitTransactionContention(t *testing.T) {
	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    68,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 56, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 57, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 58, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 57,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-explicit-contention", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 56},
		{NodeID: 2, StoreID: 57},
		{NodeID: 3, StoreID: 58},
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
		waitProcessNodeDone(t, done1, "contention-node1")
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
		waitProcessNodeDone(t, done2, "contention-node2")
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
		waitProcessNodeDone(t, done3, "contention-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign contention leaseholder: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 57)

	conn1 := openPGConn(t, node1.state.PGAddr)
	defer conn1.Close()
	if _, err := conn1.Write(queryFrame("begin")); err != nil {
		t.Fatalf("write begin on txn1: %v", err)
	}
	if got := frameTags(readFrames(t, conn1, 2)); got != "CZ" {
		t.Fatalf("txn1 begin frame tags = %q, want CZ", got)
	}
	if _, err := conn1.Write(queryFrame("insert into users (id, name, email) values (21, 'alice', 'a@example.com')")); err != nil {
		t.Fatalf("write insert on txn1: %v", err)
	}
	if got := frameTags(readFrames(t, conn1, 2)); got != "CZ" {
		t.Fatalf("txn1 insert frame tags = %q, want CZ", got)
	}

	conn2 := openPGConn(t, node3.state.PGAddr)
	defer conn2.Close()
	if _, err := conn2.Write(queryFrame("begin")); err != nil {
		t.Fatalf("write begin on txn2: %v", err)
	}
	if got := frameTags(readFrames(t, conn2, 2)); got != "CZ" {
		t.Fatalf("txn2 begin frame tags = %q, want CZ", got)
	}
	if _, err := conn2.Write(queryFrame("insert into users (id, name, email) values (21, 'bob', 'b@example.com')")); err != nil {
		t.Fatalf("write contended insert on txn2: %v", err)
	}
	contentionFrames := readFrames(t, conn2, 2)
	if got := frameTags(contentionFrames); got != "EZ" || readyStatus(contentionFrames[1]) != byte(pgwire.TxFailedTransaction) {
		t.Fatalf("contended insert frames = %q/%q, want EZ/E", got, readyStatus(contentionFrames[1]))
	}
	if _, err := conn2.Write(queryFrame("rollback")); err != nil {
		t.Fatalf("write rollback on txn2: %v", err)
	}
	if got := frameTags(readFrames(t, conn2, 2)); got != "CZ" {
		t.Fatalf("txn2 rollback frame tags = %q, want CZ", got)
	}

	if _, err := conn1.Write(queryFrame("commit")); err != nil {
		t.Fatalf("write commit on txn1: %v", err)
	}
	if got := frameTags(readFrames(t, conn1, 2)); got != "CZ" {
		t.Fatalf("txn1 commit frame tags = %q, want CZ", got)
	}

	checkConn := openPGConn(t, node2.state.PGAddr)
	defer checkConn.Close()
	if _, err := checkConn.Write(queryFrame("select id, name from users where id = 21")); err != nil {
		t.Fatalf("write final select: %v", err)
	}
	checkFrames := readFrames(t, checkConn, 4)
	if got := frameTags(checkFrames); got != "TDCZ" {
		t.Fatalf("final contention check frames = %q, want TDCZ", got)
	}
	values, err := decodeDataRowFrame(checkFrames[1])
	if err != nil {
		t.Fatalf("decode final contention row: %v", err)
	}
	if len(values) != 2 || string(values[0]) != "21" || string(values[1]) != "alice" {
		t.Fatalf("final row = %q/%q, want 21/alice", values[0], values[1])
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

func TestProcessNodeRecoversStagingTxnInBackground(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	splitKey := usersPrimaryKey(50)
	rangeOne := meta.RangeDescriptor{
		RangeID:    71,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     splitKey,
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 61, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 62, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 63, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 61,
	}
	rangeTwo := meta.RangeDescriptor{
		RangeID:    72,
		Generation: 1,
		StartKey:   splitKey,
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 64, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 65, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 66, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 66,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-background-staging-recovery", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 61},
		{NodeID: 2, StoreID: 62},
		{NodeID: 3, StoreID: 63},
	}, []meta.RangeDescriptor{rangeOne, rangeTwo})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	startCfg := func(nodeID, storeID uint64) ProcessNodeConfig {
		return ProcessNodeConfig{
			NodeID:              nodeID,
			StoreID:             storeID,
			DataDir:             filepath.Join(rootDir, fmt.Sprintf("node-%d", nodeID)),
			BootstrapPath:       bootstrapPath,
			PGListenAddr:        "127.0.0.1:0",
			ObservabilityAddr:   "127.0.0.1:0",
			ControlAddr:         "127.0.0.1:0",
			HeartbeatInterval:   50 * time.Millisecond,
			TxnRecoveryInterval: 50 * time.Millisecond,
		}
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, startCfg(1, 61))
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "staging-recovery-node1")
	})
	node2, cancel2, done2 := startProcessNodeForTest(t, startCfg(2, 62))
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "staging-recovery-node2")
	})
	node3, cancel3, done3 := startProcessNodeForTest(t, startCfg(3, 63))
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "staging-recovery-node3")
	})

	if err := node1.host.Campaign(context.Background(), rangeOne.RangeID); err != nil {
		t.Fatalf("campaign first range: %v", err)
	}
	if err := node3.host.Campaign(context.Background(), rangeTwo.RangeID); err != nil {
		t.Fatalf("campaign second range: %v", err)
	}
	waitForRangeLeader(t, node1.host, rangeOne.RangeID, 61)
	waitForRangeLeader(t, node3.host, rangeTwo.RangeID, 66)

	catalog, err := defaultSystemTestCatalog()
	if err != nil {
		t.Fatalf("default catalog: %v", err)
	}
	users, err := catalog.ResolveTable("users")
	if err != nil {
		t.Fatalf("resolve users table: %v", err)
	}
	planner := chronossql.NewPlanner(chronossql.NewParser(), catalog)
	insertAlice := mustInsertPlan(t, planner, "insert into users (id, name, email) values (7, 'alice', 'a@example.com')")
	insertBob := mustInsertPlan(t, planner, "insert into users (id, name, email) values (70, 'bob', 'b@example.com')")

	now := hlc.Timestamp{WallTime: uint64(time.Now().UTC().UnixNano())}
	record := txn.Record{
		ID:         newTxnID(),
		Status:     txn.StatusPending,
		ReadTS:     now,
		WriteTS:    now,
		Priority:   1,
		DeadlineTS: hlc.Timestamp{WallTime: now.WallTime + uint64(30*time.Second)},
	}
	record, err = record.Anchor(rangeOne.RangeID, now)
	if err != nil {
		t.Fatalf("anchor record: %v", err)
	}
	record, err = record.TrackIntent(txn.IntentRef{RangeID: rangeOne.RangeID, Key: insertAlice.Key, Strength: storage.IntentStrengthExclusive})
	if err != nil {
		t.Fatalf("track alice intent: %v", err)
	}
	record, err = record.TrackIntent(txn.IntentRef{RangeID: rangeTwo.RangeID, Key: insertBob.Key, Strength: storage.IntentStrengthExclusive})
	if err != nil {
		t.Fatalf("track bob intent: %v", err)
	}

	if err := node1.kv.PutIntent(context.Background(), insertAlice.Key, storage.Intent{
		TxnID:          record.ID,
		Epoch:          record.Epoch,
		WriteTimestamp: record.WriteTS,
		Strength:       storage.IntentStrengthExclusive,
		Value:          insertAlice.Value,
	}); err != nil {
		t.Fatalf("put alice intent: %v", err)
	}
	if err := node1.kv.PutIntent(context.Background(), insertBob.Key, storage.Intent{
		TxnID:          record.ID,
		Epoch:          record.Epoch,
		WriteTimestamp: record.WriteTS,
		Strength:       storage.IntentStrengthExclusive,
		Value:          insertBob.Value,
	}); err != nil {
		t.Fatalf("put bob intent: %v", err)
	}
	required, err := intentSetFromRecord(record)
	if err != nil {
		t.Fatalf("intent set from record: %v", err)
	}
	staged, err := record.StageForParallelCommit(required)
	if err != nil {
		t.Fatalf("stage record: %v", err)
	}
	if err := node1.kv.PutTxnRecord(context.Background(), staged); err != nil {
		t.Fatalf("put staged txn record: %v", err)
	}

	waitForTxnRecordStatus(t, node1.kv, staged.ID, txn.StatusCommitted)
	waitForDecodedUserRow(t, node2.kv, users, insertAlice.Key, "7", "alice")
	waitForDecodedUserRow(t, node2.kv, users, insertBob.Key, "70", "bob")
	waitForIntentMissing(t, node1.kv, insertAlice.Key)
	waitForIntentMissing(t, node1.kv, insertBob.Key)
}

func TestProcessNodeAbortsExpiredPendingTxnInBackground(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    73,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 67, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 68, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 69, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 68,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-background-pending-abort", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 67},
		{NodeID: 2, StoreID: 68},
		{NodeID: 3, StoreID: 69},
	}, []meta.RangeDescriptor{usersRange})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	startCfg := func(nodeID, storeID uint64) ProcessNodeConfig {
		return ProcessNodeConfig{
			NodeID:              nodeID,
			StoreID:             storeID,
			DataDir:             filepath.Join(rootDir, fmt.Sprintf("pending-node-%d", nodeID)),
			BootstrapPath:       bootstrapPath,
			PGListenAddr:        "127.0.0.1:0",
			ObservabilityAddr:   "127.0.0.1:0",
			ControlAddr:         "127.0.0.1:0",
			HeartbeatInterval:   50 * time.Millisecond,
			TxnRecoveryInterval: 50 * time.Millisecond,
		}
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, startCfg(1, 67))
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "pending-recovery-node1")
	})
	node2, cancel2, done2 := startProcessNodeForTest(t, startCfg(2, 68))
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "pending-recovery-node2")
	})
	_, cancel3, done3 := startProcessNodeForTest(t, startCfg(3, 69))
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "pending-recovery-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign pending range: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 68)

	catalog, err := defaultSystemTestCatalog()
	if err != nil {
		t.Fatalf("default catalog: %v", err)
	}
	planner := chronossql.NewPlanner(chronossql.NewParser(), catalog)
	insertCarol := mustInsertPlan(t, planner, "insert into users (id, name, email) values (11, 'carol', 'c@example.com')")

	now := hlc.Timestamp{WallTime: uint64(time.Now().UTC().UnixNano())}
	record := txn.Record{
		ID:         newTxnID(),
		Status:     txn.StatusPending,
		ReadTS:     now,
		WriteTS:    now,
		Priority:   1,
		DeadlineTS: hlc.Timestamp{WallTime: now.WallTime - uint64(time.Second)},
	}
	record, err = record.Anchor(usersRange.RangeID, now)
	if err != nil {
		t.Fatalf("anchor pending record: %v", err)
	}
	record, err = record.TrackIntent(txn.IntentRef{RangeID: usersRange.RangeID, Key: insertCarol.Key, Strength: storage.IntentStrengthExclusive})
	if err != nil {
		t.Fatalf("track pending intent: %v", err)
	}
	if err := node1.kv.PutIntent(context.Background(), insertCarol.Key, storage.Intent{
		TxnID:          record.ID,
		Epoch:          record.Epoch,
		WriteTimestamp: record.WriteTS,
		Strength:       storage.IntentStrengthExclusive,
		Value:          insertCarol.Value,
	}); err != nil {
		t.Fatalf("put pending intent: %v", err)
	}
	if err := node1.kv.PutTxnRecord(context.Background(), record); err != nil {
		t.Fatalf("put pending txn record: %v", err)
	}

	waitForTxnRecordStatus(t, node1.kv, record.ID, txn.StatusAborted)
	waitForIntentMissing(t, node1.kv, insertCarol.Key)
	waitForKeyAbsent(t, node1.kv, insertCarol.Key)
}

func TestProcessNodeCommitsDeleteTombstoneInBackground(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	bootstrapPath := filepath.Join(rootDir, "bootstrap.json")
	usersRange := meta.RangeDescriptor{
		RangeID:    74,
		Generation: 1,
		StartKey:   storage.GlobalTablePrimaryPrefix(7),
		EndKey:     storage.GlobalTablePrimaryPrefix(8),
		Replicas: []meta.ReplicaDescriptor{
			{ReplicaID: 77, NodeID: 1, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 78, NodeID: 2, Role: meta.ReplicaRoleVoter},
			{ReplicaID: 79, NodeID: 3, Role: meta.ReplicaRoleVoter},
		},
		LeaseholderReplicaID: 78,
	}
	manifest, err := chronosruntime.BuildBootstrapManifest("cluster-background-delete-commit", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 77},
		{NodeID: 2, StoreID: 78},
		{NodeID: 3, StoreID: 79},
	}, []meta.RangeDescriptor{usersRange})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}
	if err := chronosruntime.WriteBootstrapManifest(bootstrapPath, manifest); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}

	startCfg := func(nodeID, storeID uint64) ProcessNodeConfig {
		return ProcessNodeConfig{
			NodeID:              nodeID,
			StoreID:             storeID,
			DataDir:             filepath.Join(rootDir, fmt.Sprintf("delete-node-%d", nodeID)),
			BootstrapPath:       bootstrapPath,
			PGListenAddr:        "127.0.0.1:0",
			ObservabilityAddr:   "127.0.0.1:0",
			ControlAddr:         "127.0.0.1:0",
			HeartbeatInterval:   50 * time.Millisecond,
			TxnRecoveryInterval: 50 * time.Millisecond,
		}
	}

	node1, cancel1, done1 := startProcessNodeForTest(t, startCfg(1, 77))
	t.Cleanup(func() {
		cancel1()
		waitProcessNodeDone(t, done1, "delete-recovery-node1")
	})
	node2, cancel2, done2 := startProcessNodeForTest(t, startCfg(2, 78))
	t.Cleanup(func() {
		cancel2()
		waitProcessNodeDone(t, done2, "delete-recovery-node2")
	})
	_, cancel3, done3 := startProcessNodeForTest(t, startCfg(3, 79))
	t.Cleanup(func() {
		cancel3()
		waitProcessNodeDone(t, done3, "delete-recovery-node3")
	})

	if err := node2.host.Campaign(context.Background(), usersRange.RangeID); err != nil {
		t.Fatalf("campaign delete range: %v", err)
	}
	waitForRangeLeader(t, node2.host, usersRange.RangeID, 78)

	catalog, err := defaultSystemTestCatalog()
	if err != nil {
		t.Fatalf("default catalog: %v", err)
	}
	planner := chronossql.NewPlanner(chronossql.NewParser(), catalog)
	insertAlice := mustInsertPlan(t, planner, "insert into users (id, name, email) values (15, 'alice', 'a@example.com')")

	baseTS := hlc.Timestamp{WallTime: uint64(time.Now().UTC().UnixNano())}
	if err := node1.kv.PutAt(context.Background(), insertAlice.Key, baseTS, insertAlice.Value); err != nil {
		t.Fatalf("seed committed row: %v", err)
	}
	waitForDecodedUserRow(t, node2.kv, mustResolveUsersTable(t, catalog), insertAlice.Key, "15", "alice")

	deleteTS := hlc.Timestamp{WallTime: baseTS.WallTime + uint64(time.Second)}
	record := txn.Record{
		ID:         newTxnID(),
		Status:     txn.StatusCommitted,
		ReadTS:     deleteTS,
		WriteTS:    deleteTS,
		Priority:   1,
		DeadlineTS: hlc.Timestamp{WallTime: deleteTS.WallTime + uint64(30*time.Second)},
	}
	record, err = record.Anchor(usersRange.RangeID, deleteTS)
	if err != nil {
		t.Fatalf("anchor delete record: %v", err)
	}
	record, err = record.TrackIntent(txn.IntentRef{RangeID: usersRange.RangeID, Key: insertAlice.Key, Strength: storage.IntentStrengthExclusive})
	if err != nil {
		t.Fatalf("track delete intent: %v", err)
	}
	if err := node1.kv.PutIntent(context.Background(), insertAlice.Key, storage.Intent{
		TxnID:          record.ID,
		Epoch:          record.Epoch,
		WriteTimestamp: record.WriteTS,
		Strength:       storage.IntentStrengthExclusive,
		Tombstone:      true,
	}); err != nil {
		t.Fatalf("put delete intent: %v", err)
	}
	if err := node1.kv.PutTxnRecord(context.Background(), record); err != nil {
		t.Fatalf("put committed delete txn record: %v", err)
	}

	waitForIntentMissing(t, node1.kv, insertAlice.Key)
	waitForKeyAbsent(t, node1.kv, insertAlice.Key)
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

func mustResolveUsersTable(t *testing.T, catalog *chronossql.Catalog) chronossql.TableDescriptor {
	t.Helper()
	users, err := catalog.ResolveTable("users")
	if err != nil {
		t.Fatalf("resolve users table: %v", err)
	}
	return users
}

func waitForTxnRecordStatus(t *testing.T, kv *kvClient, txnID storage.TxnID, want txn.Status) {
	t.Helper()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		record, found, err := kv.GetTxnRecord(context.Background(), txnID)
		if err == nil && found && record.Status == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	record, _, _ := kv.GetTxnRecord(context.Background(), txnID)
	t.Fatalf("txn %x status = %s, want %s", txnID[:], record.Status, want)
}

func waitForIntentMissing(t *testing.T, kv *kvClient, key []byte) {
	t.Helper()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		_, found, err := kv.GetIntent(context.Background(), key)
		if err == nil && !found {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("intent for key %q still present", key)
}

func waitForKeyAbsent(t *testing.T, kv *kvClient, key []byte) {
	t.Helper()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		_, found, err := kv.GetLatest(context.Background(), key)
		if err == nil && !found {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("key %q is still present", key)
}

func waitForKeyValue(t *testing.T, kv *kvClient, key, want []byte) {
	t.Helper()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		value, found, err := kv.GetLatest(context.Background(), key)
		if err == nil && found && bytes.Equal(value, want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for key %q to equal %v", key, want)
}

func waitForDecodedUserRow(t *testing.T, kv *kvClient, table chronossql.TableDescriptor, key []byte, wantID, wantName string) {
	t.Helper()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		value, found, err := kv.GetLatest(context.Background(), key)
		if err == nil && found {
			row, decodeErr := chronossql.DecodeRowValue(table, value)
			if decodeErr == nil {
				id, idErr := chronossql.FormatValueText(row["id"])
				name, nameErr := chronossql.FormatValueText(row["name"])
				if idErr == nil && nameErr == nil && string(id) == wantID && string(name) == wantName {
					return
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for decoded row %q/%q", wantID, wantName)
}

func mustInsertPlan(t *testing.T, planner *chronossql.Planner, query string) chronossql.InsertPlan {
	t.Helper()

	plan, err := planner.Plan(query)
	if err != nil {
		t.Fatalf("plan insert %q: %v", query, err)
	}
	insert, ok := plan.(chronossql.InsertPlan)
	if !ok {
		t.Fatalf("plan type = %T, want InsertPlan", plan)
	}
	return insert
}

func usersTableDescriptor(t *testing.T) chronossql.TableDescriptor {
	t.Helper()

	catalog, err := DefaultCatalog()
	if err != nil {
		t.Fatalf("build default catalog: %v", err)
	}
	table, err := catalog.ResolveTable("users")
	if err != nil {
		t.Fatalf("resolve users table: %v", err)
	}
	return table
}

func usersIndexKeys(t *testing.T, id int64, name, email string) ([]byte, []byte, []byte) {
	t.Helper()

	table := usersTableDescriptor(t)
	row := map[string]chronossql.Value{
		"id":    {Type: chronossql.ColumnTypeInt, Int64: id},
		"name":  {Type: chronossql.ColumnTypeString, String: name},
		"email": {Type: chronossql.ColumnTypeString, String: email},
	}
	entries, err := chronossql.BuildIndexEntries(table, row)
	if err != nil {
		t.Fatalf("build index entries: %v", err)
	}
	encodedPK, err := chronossql.EncodePrimaryKeyBytes(table, row)
	if err != nil {
		t.Fatalf("encode primary key bytes: %v", err)
	}
	var nameKey, emailKey []byte
	for _, entry := range entries {
		switch entry.Index.Name {
		case "users_name_idx":
			nameKey = append([]byte(nil), entry.Key...)
		case "users_email_key":
			emailKey = append([]byte(nil), entry.Key...)
		}
	}
	if len(nameKey) == 0 || len(emailKey) == 0 {
		t.Fatalf("missing expected users secondary index keys: name=%q email=%q", nameKey, emailKey)
	}
	return nameKey, emailKey, encodedPK
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

func decodeCommandCompleteFrame(frame []byte) string {
	if len(frame) < 6 || frame[0] != 'C' {
		return ""
	}
	payload := frame[5:]
	if len(payload) == 0 {
		return ""
	}
	return string(payload[:len(payload)-1])
}

func decodeErrorMessageFrame(frame []byte) string {
	if len(frame) < 6 || frame[0] != 'E' {
		return ""
	}
	payload := frame[5:]
	for len(payload) > 1 && payload[0] != 0 {
		fieldType := payload[0]
		payload = payload[1:]
		idx := -1
		for i, b := range payload {
			if b == 0 {
				idx = i
				break
			}
		}
		if idx < 0 {
			return ""
		}
		value := string(payload[:idx])
		payload = payload[idx+1:]
		if fieldType == 'M' {
			return value
		}
	}
	return ""
}

func readyStatus(frame []byte) byte {
	if len(frame) < 6 || frame[0] != 'Z' {
		return 0
	}
	return frame[5]
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
