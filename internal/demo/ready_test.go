package demo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

func TestWaitForSeededClusterReadyWaitsForLeaseholderLeaders(t *testing.T) {
	t.Parallel()

	manifest, err := chronosruntime.BuildBootstrapManifest("demo-ready-test", []chronosruntime.BootstrapNode{
		{NodeID: 1, StoreID: 11},
		{NodeID: 2, StoreID: 12},
	}, []meta.RangeDescriptor{
		{
			RangeID:              41,
			Generation:           1,
			StartKey:             storage.GlobalTablePrimaryPrefix(7),
			EndKey:               storage.GlobalTablePrimaryPrefix(8),
			Replicas:             voters(21, 1, 22, 2, 23, 1),
			LeaseholderReplicaID: 22,
		},
	})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}

	type statusKey struct {
		nodeID  uint64
		rangeID uint64
	}

	var (
		mu        sync.Mutex
		attempts  int
		statusMap = map[statusKey]rangeStatusResponse{
			{nodeID: 1, rangeID: manifest.Meta1[0].RangeID}: {Hosted: true, LeaderReplicaID: manifest.Meta1[0].LeaseholderReplicaID},
			{nodeID: 1, rangeID: manifest.Meta2[0].RangeID}: {Hosted: true, LeaderReplicaID: manifest.Meta2[0].LeaseholderReplicaID},
			{nodeID: 2, rangeID: 41}:                          {Hosted: true, LeaderReplicaID: 0},
		}
	)

	newNodeServer := func(nodeID uint64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/admin/snapshot":
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"nodes":[],"ranges":[]}`))
			case r.Method == http.MethodPost && r.URL.Path == "/control/range/status":
				var req rangeStatusRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				if nodeID == 2 && req.RangeID == 41 {
					attempts++
					if attempts >= 3 {
						statusMap[statusKey{nodeID: 2, rangeID: 41}] = rangeStatusResponse{
							Hosted:          true,
							LeaderReplicaID: 22,
						}
					}
				}
				status, ok := statusMap[statusKey{nodeID: nodeID, rangeID: req.RangeID}]
				if !ok {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(status)
			default:
				http.NotFound(w, r)
			}
		}))
	}

	node1 := newNodeServer(1)
	defer node1.Close()
	node2 := newNodeServer(2)
	defer node2.Close()

	configs := []systemtest.ProcessNodeConfig{
		{
			NodeID:            1,
			ObservabilityAddr: strings.TrimPrefix(node1.URL, "http://"),
			ControlAddr:       strings.TrimPrefix(node1.URL, "http://"),
		},
		{
			NodeID:            2,
			ObservabilityAddr: strings.TrimPrefix(node2.URL, "http://"),
			ControlAddr:       strings.TrimPrefix(node2.URL, "http://"),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := WaitForSeededClusterReady(ctx, manifest, configs); err != nil {
		t.Fatalf("wait for seeded cluster ready: %v", err)
	}

	mu.Lock()
	gotAttempts := attempts
	mu.Unlock()
	if gotAttempts < 3 {
		t.Fatalf("leaseholder status polls = %d, want at least 3", gotAttempts)
	}
}
