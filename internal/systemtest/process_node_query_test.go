package systemtest

import (
	"context"
	"encoding/binary"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
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

func readAllFrames(conn io.Reader, count int) ([][]byte, error) {
	frames := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		frame, err := readBackendFrame(conn)
		if err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}
	return frames, nil
}
