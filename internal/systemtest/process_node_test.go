package systemtest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

func TestProcessNodeAdminEndpoints(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	node, err := NewProcessNode(ProcessNodeConfig{
		NodeID:            1,
		DataDir:           dataDir,
		PGListenAddr:      "127.0.0.1:0",
		ObservabilityAddr: "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Ranges: []meta.RangeDescriptor{
			{
				RangeID:    11,
				Generation: 2,
				StartKey:   []byte("a"),
				EndKey:     []byte("m"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
					{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
				},
				LeaseholderReplicaID: 1,
			},
			{
				RangeID:    12,
				Generation: 5,
				StartKey:   []byte("m"),
				EndKey:     []byte("z"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 3, NodeID: 1, Role: meta.ReplicaRoleLearner},
					{ReplicaID: 4, NodeID: 3, Role: meta.ReplicaRoleVoter},
				},
				LeaseholderReplicaID: 4,
			},
			{
				RangeID:    13,
				Generation: 1,
				StartKey:   []byte("z"),
				Replicas: []meta.ReplicaDescriptor{
					{ReplicaID: 5, NodeID: 2, Role: meta.ReplicaRoleVoter},
				},
				LeaseholderReplicaID: 5,
			},
		},
		EventBufferSize: 8,
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
			t.Fatal("timed out waiting for process node shutdown")
		}
	})

	state := waitForProcessNodeState(t, filepath.Join(dataDir, "state.json"))
	client := &http.Client{Timeout: 2 * time.Second}

	var nodeView adminapi.NodeView
	getJSON(t, client, state.ObservabilityURL+"/admin/node", &nodeView)
	if nodeView.NodeID != 1 {
		t.Fatalf("node id = %d, want 1", nodeView.NodeID)
	}
	if nodeView.Status != "ok" {
		t.Fatalf("node status = %q, want ok", nodeView.Status)
	}
	if nodeView.ReplicaCount != 2 || nodeView.LeaseCount != 1 {
		t.Fatalf("node counts = replicas %d leases %d, want 2/1", nodeView.ReplicaCount, nodeView.LeaseCount)
	}

	var ranges []adminapi.RangeView
	getJSON(t, client, state.ObservabilityURL+"/admin/ranges", &ranges)
	if len(ranges) != 2 {
		t.Fatalf("local ranges = %+v, want 2 local descriptors", ranges)
	}
	if ranges[0].RangeID != 11 || ranges[1].RangeID != 12 {
		t.Fatalf("range ids = %+v, want 11 and 12", ranges)
	}

	postJSON(t, client, state.ControlURL+"/control/partition", partitionControlRequest{
		IsolatedFrom: []uint64{2, 3},
	})

	getJSON(t, client, state.ObservabilityURL+"/admin/node", &nodeView)
	if nodeView.Status != "degraded" {
		t.Fatalf("node status after partition = %q, want degraded", nodeView.Status)
	}
	if len(nodeView.PartitionedFrom) != 2 || nodeView.PartitionedFrom[0] != 2 || nodeView.PartitionedFrom[1] != 3 {
		t.Fatalf("partitioned_from = %v, want [2 3]", nodeView.PartitionedFrom)
	}

	var events []adminapi.ClusterEvent
	getJSON(t, client, state.ObservabilityURL+"/admin/events?limit=2", &events)
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2", len(events))
	}
	if events[1].Type != "partition_applied" {
		t.Fatalf("latest event = %+v, want partition_applied", events[1])
	}

	var snapshot adminapi.ClusterSnapshot
	getJSON(t, client, state.ObservabilityURL+"/admin/snapshot?event_limit=1", &snapshot)
	if len(snapshot.Nodes) != 1 || len(snapshot.Ranges) != 2 || len(snapshot.Events) != 1 {
		t.Fatalf("snapshot = %+v, want one node, two ranges, one event", snapshot)
	}
	if snapshot.Events[0].Type != "partition_applied" {
		t.Fatalf("snapshot latest event = %+v, want partition_applied", snapshot.Events[0])
	}
}

func waitForProcessNodeState(t *testing.T, path string) ProcessNodeState {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, err := ReadProcessNodeState(path)
		if err == nil {
			return state
		}
		if !os.IsNotExist(err) {
			t.Fatalf("read process node state: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for state file %s", path)
	return ProcessNodeState{}
}

func getJSON(t *testing.T, client *http.Client, url string, out any) {
	t.Helper()

	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get %s: status = %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

func postJSON(t *testing.T, client *http.Client, url string, payload any) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", url, err)
	}
	resp, err := client.Post(url, "application/json", bytesReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("post %s: status = %d, want %d", url, resp.StatusCode, http.StatusNoContent)
	}
}

func bytesReader(body []byte) io.Reader {
	return bytes.NewReader(body)
}
