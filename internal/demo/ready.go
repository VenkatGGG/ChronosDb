package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/node"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
)

type rangeStatusResponse struct {
	Hosted          bool   `json:"hosted"`
	LeaderReplicaID uint64 `json:"leader_replica_id,omitempty"`
}

// WaitForSeededClusterReady waits for all configured node admin surfaces to be
// reachable and for each bootstrap-designated leaseholder range to have an
// elected leader on the expected node before callers start issuing workloads.
func WaitForSeededClusterReady(ctx context.Context, manifest chronosruntime.BootstrapManifest, configs []node.Config) error {
	controlURLs := make(map[uint64]string, len(configs))
	for _, cfg := range configs {
		if cfg.NodeID == 0 {
			return fmt.Errorf("demo readiness: node id must not be zero")
		}
		if cfg.ObservabilityAddr == "" || cfg.ControlAddr == "" {
			return fmt.Errorf("demo readiness: node %d must expose observability and control addresses", cfg.NodeID)
		}
		if err := waitForAdminSnapshot(ctx, "http://"+cfg.ObservabilityAddr); err != nil {
			return err
		}
		controlURLs[cfg.NodeID] = "http://" + cfg.ControlAddr
	}
	client := &http.Client{Timeout: 500 * time.Millisecond}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := bootstrapLeaseholdersReady(ctx, client, manifest, controlURLs)
		if err == nil && ready {
			return nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("demo readiness: %w", err)
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func bootstrapLeaseholdersReady(ctx context.Context, client *http.Client, manifest chronosruntime.BootstrapManifest, controlURLs map[uint64]string) (bool, error) {
	for _, desc := range manifest.AllRanges() {
		targetNodeID, ok := leaseholderNodeID(desc)
		if !ok {
			return false, fmt.Errorf("range %d leaseholder replica %d not found", desc.RangeID, desc.LeaseholderReplicaID)
		}
		controlURL, ok := controlURLs[targetNodeID]
		if !ok {
			return false, fmt.Errorf("range %d leaseholder node %d has no control url", desc.RangeID, targetNodeID)
		}
		status, err := fetchRangeStatus(ctx, client, controlURL, desc.RangeID)
		if err != nil {
			return false, err
		}
		if !status.Hosted || status.LeaderReplicaID != desc.LeaseholderReplicaID {
			return false, nil
		}
	}
	return true, nil
}

func waitForAdminSnapshot(ctx context.Context, baseURL string) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	url := strings.TrimRight(baseURL, "/") + "/admin/snapshot?event_limit=16"
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func fetchRangeStatus(ctx context.Context, client *http.Client, controlURL string, rangeID uint64) (rangeStatusResponse, error) {
	body, err := json.Marshal(chronosruntime.RangeStatusRequest{RangeID: rangeID})
	if err != nil {
		return rangeStatusResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(controlURL, "/")+"/control/range/status", bytes.NewReader(body))
	if err != nil {
		return rangeStatusResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return rangeStatusResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return rangeStatusResponse{}, fmt.Errorf("range status %d returned status %d", rangeID, resp.StatusCode)
	}
	var status rangeStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return rangeStatusResponse{}, err
	}
	return status, nil
}

func leaseholderNodeID(desc meta.RangeDescriptor) (uint64, bool) {
	for _, replica := range desc.Replicas {
		if replica.ReplicaID == desc.LeaseholderReplicaID {
			return replica.NodeID, true
		}
	}
	return 0, false
}
