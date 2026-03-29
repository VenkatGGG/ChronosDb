package systemtest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/allocator"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

const (
	defaultHeartbeatInterval = time.Second
	defaultAllocatorInterval = 2 * time.Second
)

func (n *ProcessNode) serveBackground(ctx context.Context) {
	_ = n.heartbeatLiveness(ctx)
	heartbeatInterval := n.cfg.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	allocatorInterval := n.cfg.AllocatorInterval
	if allocatorInterval <= 0 {
		allocatorInterval = defaultAllocatorInterval
	}
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()
	allocatorTicker := time.NewTicker(allocatorInterval)
	defer allocatorTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			_ = n.heartbeatLiveness(ctx)
		case <-allocatorTicker.C:
			_ = n.recommendRebalances(ctx)
		}
	}
}

func (n *ProcessNode) heartbeatLiveness(ctx context.Context) error {
	if n.kv == nil || n.host == nil {
		return nil
	}
	epoch, err := n.currentLivenessEpoch(ctx)
	if err != nil {
		return err
	}
	now := wallClockHLC()
	record := meta.NodeLiveness{
		NodeID:    n.cfg.NodeID,
		Epoch:     epoch,
		UpdatedAt: now,
	}
	payload, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	if err := n.kv.PutAt(ctx, storage.GlobalNodeLivenessKey(n.cfg.NodeID), now, payload); err != nil {
		return err
	}
	n.host.SetLivenessEpoch(epoch)

	n.mu.Lock()
	changedEpoch := !n.hasLiveness || n.liveness.Epoch != epoch
	n.liveness = record
	n.hasLiveness = true
	n.mu.Unlock()

	if changedEpoch {
		_ = n.recordEvent(adminapi.ClusterEvent{
			Type:     "node_liveness_epoch_bumped",
			Severity: "info",
			Message:  fmt.Sprintf("node liveness epoch is now %d", epoch),
			Fields: map[string]string{
				"epoch": fmt.Sprintf("%d", epoch),
			},
		})
	}
	return nil
}

func (n *ProcessNode) currentLivenessEpoch(ctx context.Context) (uint64, error) {
	n.mu.RLock()
	if n.hasLiveness {
		epoch := n.liveness.Epoch
		n.mu.RUnlock()
		return epoch, nil
	}
	n.mu.RUnlock()

	value, found, err := n.kv.GetLatest(ctx, storage.GlobalNodeLivenessKey(n.cfg.NodeID))
	if err != nil {
		return 0, err
	}
	if !found {
		return 1, nil
	}
	var record meta.NodeLiveness
	if err := record.UnmarshalBinary(value); err != nil {
		return 0, err
	}
	return record.Epoch + 1, nil
}

func wallClockHLC() hlc.Timestamp {
	return hlc.Timestamp{WallTime: uint64(time.Now().UTC().UnixNano())}
}

func hlcToTime(ts hlc.Timestamp) time.Time {
	if ts.IsZero() {
		return time.Time{}
	}
	return time.Unix(0, int64(ts.WallTime)).UTC()
}

func (n *ProcessNode) recommendRebalances(ctx context.Context) error {
	if n.host == nil {
		return nil
	}
	descs, err := n.host.HostedDescriptors()
	if err != nil {
		return err
	}
	loads, err := n.liveNodeLoads(ctx)
	if err != nil {
		return err
	}
	nextRecommendations := make(map[uint64]string)
	for _, desc := range descs {
		_, ok := localLeaseholderReplicaID(desc, n.cfg.NodeID)
		if !ok {
			continue
		}
		decision, err := allocator.ChooseRebalance(desc, loads)
		if err != nil {
			continue
		}
		signature := fmt.Sprintf("%d->%d:%s", decision.SourceReplica.NodeID, decision.TargetNode.NodeID, decision.Reason)
		nextRecommendations[desc.RangeID] = signature

		n.mu.RLock()
		previous := n.rebalanceRecommendations[desc.RangeID]
		n.mu.RUnlock()
		if previous == signature {
			continue
		}
		_ = n.recordEvent(adminapi.ClusterEvent{
			Type:     "rebalance_recommended",
			Severity: "info",
			RangeID:  desc.RangeID,
			Message:  decision.Reason,
			Fields: map[string]string{
				"source_node_id": fmt.Sprintf("%d", decision.SourceReplica.NodeID),
				"target_node_id": fmt.Sprintf("%d", decision.TargetNode.NodeID),
			},
		})
	}
	n.mu.Lock()
	n.rebalanceRecommendations = nextRecommendations
	n.mu.Unlock()
	return nil
}

func (n *ProcessNode) liveNodeLoads(ctx context.Context) ([]allocator.NodeLoad, error) {
	rootDir := filepath.Dir(n.cfg.DataDir)
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 500 * time.Millisecond}
	loads := make([]allocator.NodeLoad, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		state, err := ReadProcessNodeState(filepath.Join(rootDir, entry.Name(), "state.json"))
		if err != nil || state.ObservabilityURL == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, state.ObservabilityURL+"/admin/node", nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var view adminapi.NodeView
		decodeErr := json.NewDecoder(resp.Body).Decode(&view)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || decodeErr != nil {
			continue
		}
		loads = append(loads, allocator.NodeLoad{
			NodeID:    view.NodeID,
			LoadScore: float64(view.ReplicaCount) + float64(view.LeaseCount)*0.25,
			Draining:  view.Status != "ok",
		})
	}
	return loads, nil
}

func localLeaseholderReplicaID(desc meta.RangeDescriptor, nodeID uint64) (uint64, bool) {
	for _, replica := range desc.Replicas {
		if replica.NodeID == nodeID && replica.ReplicaID == desc.LeaseholderReplicaID {
			return replica.ReplicaID, true
		}
	}
	return 0, false
}
