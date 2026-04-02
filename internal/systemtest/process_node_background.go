package systemtest

import (
	"bytes"
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
	"github.com/VenkatGGG/ChronosDb/internal/replica"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/txn"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	defaultHeartbeatInterval   = time.Second
	defaultAllocatorInterval   = 2 * time.Second
	defaultTxnRecoveryInterval = time.Second
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
	txnRecoveryInterval := n.cfg.TxnRecoveryInterval
	if txnRecoveryInterval <= 0 {
		txnRecoveryInterval = defaultTxnRecoveryInterval
	}
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()
	allocatorTicker := time.NewTicker(allocatorInterval)
	defer allocatorTicker.Stop()
	recoveryTicker := time.NewTicker(txnRecoveryInterval)
	defer recoveryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			_ = n.heartbeatLiveness(ctx)
		case <-allocatorTicker.C:
			_ = n.recommendRebalances(ctx)
		case <-recoveryTicker.C:
			_ = n.recoverTransactions(ctx)
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
		if !bytes.HasPrefix(desc.StartKey, storage.GlobalTablePrefix()) {
			continue
		}
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
		if decision.SourceReplica.ReplicaID == desc.LeaseholderReplicaID {
			continue
		}
		if err := n.executeRebalance(ctx, desc, decision); err != nil {
			_ = n.recordEvent(adminapi.ClusterEvent{
				Type:     "rebalance_failed",
				Severity: "error",
				RangeID:  desc.RangeID,
				Message:  err.Error(),
				Fields: map[string]string{
					"source_node_id": fmt.Sprintf("%d", decision.SourceReplica.NodeID),
					"target_node_id": fmt.Sprintf("%d", decision.TargetNode.NodeID),
				},
			})
			continue
		}
		_ = n.recordEvent(adminapi.ClusterEvent{
			Type:     "rebalance_applied",
			Severity: "info",
			RangeID:  desc.RangeID,
			Message:  decision.Reason,
			Fields: map[string]string{
				"source_node_id":  fmt.Sprintf("%d", decision.SourceReplica.NodeID),
				"target_node_id":  fmt.Sprintf("%d", decision.TargetNode.NodeID),
				"target_replica":  fmt.Sprintf("%d", nextReplicaID(desc)),
				"removed_replica": fmt.Sprintf("%d", decision.SourceReplica.ReplicaID),
			},
		})
		break
	}
	n.mu.Lock()
	n.rebalanceRecommendations = nextRecommendations
	n.mu.Unlock()
	return nil
}

func (n *ProcessNode) recoverTransactions(ctx context.Context) error {
	if n.kv == nil {
		return nil
	}
	records, err := n.kv.ScanTxnRecords(ctx)
	if err != nil {
		return err
	}
	now := wallClockHLC()
	for _, record := range records {
		recovered, changed, err := n.recoverTransactionRecord(ctx, now, record)
		if err != nil {
			_ = n.recordEvent(adminapi.ClusterEvent{
				Type:     "txn_recovery_failed",
				Severity: "error",
				Message:  err.Error(),
				Fields: map[string]string{
					"txn_id": fmt.Sprintf("%x", record.ID[:]),
					"status": string(record.Status),
				},
			})
			continue
		}
		if !changed {
			continue
		}
		_ = n.recordEvent(adminapi.ClusterEvent{
			Type:     "txn_recovered",
			Severity: "info",
			Message:  fmt.Sprintf("transaction %x moved to %s", record.ID[:], recovered.Status),
			Fields: map[string]string{
				"txn_id":      fmt.Sprintf("%x", record.ID[:]),
				"from_status": string(record.Status),
				"to_status":   string(recovered.Status),
			},
		})
	}
	return nil
}

func (n *ProcessNode) recoverTransactionRecord(ctx context.Context, now hlc.Timestamp, record txn.Record) (txn.Record, bool, error) {
	switch record.Status {
	case txn.StatusPending:
		if record.CanRetry(now) {
			return record, false, nil
		}
		record.Status = txn.StatusAborted
		if err := n.kv.PutTxnRecord(ctx, record); err != nil {
			return txn.Record{}, false, err
		}
		if err := n.kv.ResolveTxnRecord(ctx, record); err != nil {
			return txn.Record{}, false, err
		}
		return record, true, nil
	case txn.StatusStaging:
		required, err := intentSetFromRecord(record)
		if err != nil {
			return txn.Record{}, false, err
		}
		observed, err := n.kv.ObserveIntents(ctx, record.RequiredIntents)
		if err != nil {
			return txn.Record{}, false, err
		}
		recovered, _, err := txn.RecoverAfterCoordinatorFailure(record, required, observed)
		if err != nil {
			return txn.Record{}, false, err
		}
		if err := n.kv.PutTxnRecord(ctx, recovered); err != nil {
			return txn.Record{}, false, err
		}
		if err := n.kv.ResolveTxnRecord(ctx, recovered); err != nil {
			return txn.Record{}, false, err
		}
		return recovered, true, nil
	case txn.StatusCommitted, txn.StatusAborted:
		if len(record.RequiredIntents) == 0 {
			return record, false, nil
		}
		observed, err := n.kv.ObserveIntents(ctx, record.RequiredIntents)
		if err != nil {
			return txn.Record{}, false, err
		}
		needsCleanup := false
		for _, intent := range observed {
			if intent.Present {
				needsCleanup = true
				break
			}
		}
		if !needsCleanup {
			return record, false, nil
		}
		if err := n.kv.ResolveTxnRecord(ctx, record); err != nil {
			return txn.Record{}, false, err
		}
		return record, true, nil
	default:
		return record, false, nil
	}
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

func (n *ProcessNode) executeRebalance(ctx context.Context, desc meta.RangeDescriptor, decision allocator.RebalanceDecision) error {
	metaLevel := descriptorMetaLevel(desc)
	targetReplica := meta.ReplicaDescriptor{
		ReplicaID: nextReplicaID(desc),
		NodeID:    decision.TargetNode.NodeID,
		Role:      meta.ReplicaRoleLearner,
	}
	recordStage := func(stage string, fields map[string]string) {
		if fields == nil {
			fields = make(map[string]string)
		}
		fields["stage"] = stage
		fields["target_replica_id"] = fmt.Sprintf("%d", targetReplica.ReplicaID)
		_ = n.recordEvent(adminapi.ClusterEvent{
			Type:     "rebalance_stage",
			Severity: "info",
			RangeID:  desc.RangeID,
			Message:  stage,
			Fields:   fields,
		})
	}

	recordStage("prepare_target_replica", map[string]string{
		"target_node_id": fmt.Sprintf("%d", targetReplica.NodeID),
	})
	snapshot, err := n.host.CaptureReplicaSnapshot(desc.RangeID)
	if err != nil {
		return err
	}
	if err := n.kv.PrepareReplica(ctx, targetReplica.NodeID, chronosruntime.ReplicaInstallRequest{
		RangeID:        desc.RangeID,
		ReplicaID:      targetReplica.ReplicaID,
		DescriptorHint: ptr(preparedReplicaDescriptor(desc, targetReplica)),
		Snapshot:       snapshot,
	}); err != nil {
		return err
	}
	if err := n.waitForReplicaApplied(ctx, targetReplica.NodeID, desc.RangeID, 0); err != nil {
		return err
	}

	n.host.SetPendingReplicaTarget(desc.RangeID, targetReplica.ReplicaID, targetReplica.NodeID)
	defer n.host.ClearPendingReplicaTarget(desc.RangeID, targetReplica.ReplicaID)
	recordStage("add_learner_conf_change", map[string]string{
		"target_node_id": fmt.Sprintf("%d", targetReplica.NodeID),
	})
	if err := n.host.ProposeConfChange(ctx, desc.RangeID, raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddLearnerNode,
		NodeID: targetReplica.ReplicaID,
	}, targetReplica.NodeID); err != nil {
		return err
	}
	leaderApplied, err := n.localRangeAppliedIndex(desc.RangeID)
	if err != nil {
		return err
	}
	recordStage("wait_for_learner_conf_apply", nil)
	if err := n.waitForReplicaApplied(ctx, targetReplica.NodeID, desc.RangeID, leaderApplied); err != nil {
		return err
	}
	recordStage("descriptor_add_learner", nil)
	if err := n.host.ApplyReplicaChange(ctx, desc.RangeID, metaLevel, replica.ReplicaChange{
		Kind:    replica.ReplicaChangeAddLearner,
		Replica: targetReplica,
	}); err != nil {
		return err
	}
	leaderApplied, err = n.localRangeAppliedIndex(desc.RangeID)
	if err != nil {
		return err
	}
	recordStage("wait_for_learner_descriptor_apply", nil)
	if err := n.waitForReplicaApplied(ctx, targetReplica.NodeID, desc.RangeID, leaderApplied); err != nil {
		return err
	}
	recordStage("wait_for_learner_role", nil)
	if err := n.waitForReplicaRole(ctx, targetReplica.NodeID, desc.RangeID, targetReplica.ReplicaID, meta.ReplicaRoleLearner); err != nil {
		return err
	}

	recordStage("promote_conf_change", nil)
	if err := n.host.ProposeConfChange(ctx, desc.RangeID, raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: targetReplica.ReplicaID,
	}, 0); err != nil {
		return err
	}
	leaderApplied, err = n.localRangeAppliedIndex(desc.RangeID)
	if err != nil {
		return err
	}
	recordStage("wait_for_promote_conf_apply", nil)
	if err := n.waitForReplicaApplied(ctx, targetReplica.NodeID, desc.RangeID, leaderApplied); err != nil {
		return err
	}
	promoted := targetReplica
	promoted.Role = meta.ReplicaRoleVoter
	recordStage("descriptor_promote_voter", nil)
	if err := n.host.ApplyReplicaChange(ctx, desc.RangeID, metaLevel, replica.ReplicaChange{
		Kind:    replica.ReplicaChangePromote,
		Replica: promoted,
	}); err != nil {
		return err
	}
	leaderApplied, err = n.localRangeAppliedIndex(desc.RangeID)
	if err != nil {
		return err
	}
	recordStage("wait_for_promote_descriptor_apply", nil)
	if err := n.waitForReplicaApplied(ctx, targetReplica.NodeID, desc.RangeID, leaderApplied); err != nil {
		return err
	}
	recordStage("wait_for_voter_role", nil)
	if err := n.waitForReplicaRole(ctx, targetReplica.NodeID, desc.RangeID, promoted.ReplicaID, meta.ReplicaRoleVoter); err != nil {
		return err
	}

	n.host.SetPendingReplicaTarget(desc.RangeID, decision.SourceReplica.ReplicaID, decision.SourceReplica.NodeID)
	defer n.host.ClearPendingReplicaTarget(desc.RangeID, decision.SourceReplica.ReplicaID)
	recordStage("descriptor_remove_source", map[string]string{
		"source_replica_id": fmt.Sprintf("%d", decision.SourceReplica.ReplicaID),
	})
	if err := n.host.ApplyReplicaChange(ctx, desc.RangeID, metaLevel, replica.ReplicaChange{
		Kind:    replica.ReplicaChangeRemove,
		Replica: decision.SourceReplica,
	}); err != nil {
		return err
	}
	leaderApplied, err = n.localRangeAppliedIndex(desc.RangeID)
	if err != nil {
		return err
	}
	recordStage("wait_for_remove_descriptor_apply", nil)
	if err := n.waitForReplicaApplied(ctx, decision.SourceReplica.NodeID, desc.RangeID, leaderApplied); err != nil {
		return err
	}
	recordStage("remove_conf_change", nil)
	if err := n.host.ProposeConfChange(ctx, desc.RangeID, raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: decision.SourceReplica.ReplicaID,
	}, 0); err != nil {
		return err
	}
	recordStage("wait_for_source_removal", nil)
	if err := n.waitForReplicaRemoved(ctx, decision.SourceReplica.NodeID, desc.RangeID, decision.SourceReplica.ReplicaID); err != nil {
		return err
	}
	recordStage("complete", nil)
	return nil
}

func (n *ProcessNode) waitForReplicaApplied(ctx context.Context, nodeID, rangeID, minApplied uint64) error {
	deadline := time.Now().Add(3 * time.Second)
	var (
		lastStatus rangeStatusResponse
		lastErr    error
	)
	for time.Now().Before(deadline) {
		status, err := n.kv.RangeStatus(ctx, nodeID, chronosruntime.RangeStatusRequest{RangeID: rangeID})
		lastStatus = status
		lastErr = err
		if err == nil && status.Hosted && status.AppliedIndex >= minApplied {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("range %d on node %d did not apply through %d: last_status=%+v last_err=%v", rangeID, nodeID, minApplied, lastStatus, lastErr)
}

func (n *ProcessNode) waitForReplicaRole(ctx context.Context, nodeID, rangeID, replicaID uint64, role meta.ReplicaRole) error {
	deadline := time.Now().Add(3 * time.Second)
	var (
		lastStatus rangeStatusResponse
		lastErr    error
	)
	for time.Now().Before(deadline) {
		status, err := n.kv.RangeStatus(ctx, nodeID, chronosruntime.RangeStatusRequest{RangeID: rangeID})
		lastStatus = status
		lastErr = err
		if err == nil && status.DescriptorSource != "prepared" && descriptorReplicaRole(status.Descriptor, replicaID) == role {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("range %d on node %d did not reach replica %d role %q: last_status=%+v last_err=%v", rangeID, nodeID, replicaID, role, lastStatus, lastErr)
}

func (n *ProcessNode) waitForReplicaRemoved(ctx context.Context, nodeID, rangeID, replicaID uint64) error {
	deadline := time.Now().Add(3 * time.Second)
	var (
		lastStatus rangeStatusResponse
		lastErr    error
	)
	for time.Now().Before(deadline) {
		status, err := n.kv.RangeStatus(ctx, nodeID, chronosruntime.RangeStatusRequest{RangeID: rangeID})
		lastStatus = status
		lastErr = err
		if err == nil && descriptorReplicaRole(status.Descriptor, replicaID) == "" {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("range %d on node %d did not remove replica %d: last_status=%+v last_err=%v", rangeID, nodeID, replicaID, lastStatus, lastErr)
}

func descriptorReplicaRole(desc meta.RangeDescriptor, replicaID uint64) meta.ReplicaRole {
	for _, repl := range desc.Replicas {
		if repl.ReplicaID == replicaID {
			return repl.Role
		}
	}
	return ""
}

func descriptorMetaLevel(desc meta.RangeDescriptor) meta.Level {
	if bytes.HasPrefix(desc.StartKey, storage.Meta2Prefix()) {
		return meta.LevelMeta1
	}
	return meta.LevelMeta2
}

func nextReplicaID(desc meta.RangeDescriptor) uint64 {
	var maxID uint64
	for _, repl := range desc.Replicas {
		if repl.ReplicaID > maxID {
			maxID = repl.ReplicaID
		}
	}
	return maxID + 1
}

func (n *ProcessNode) localRangeAppliedIndex(rangeID uint64) (uint64, error) {
	status, err := n.host.RangeStatus(chronosruntime.RangeStatusRequest{RangeID: rangeID})
	if err != nil {
		return 0, err
	}
	return status.AppliedIndex, nil
}

func preparedReplicaDescriptor(desc meta.RangeDescriptor, target meta.ReplicaDescriptor) meta.RangeDescriptor {
	hint := desc
	hint.StartKey = append([]byte(nil), desc.StartKey...)
	hint.EndKey = append([]byte(nil), desc.EndKey...)
	hint.Replicas = append([]meta.ReplicaDescriptor(nil), desc.Replicas...)
	if descriptorReplicaRole(hint, target.ReplicaID) == "" {
		hint.Replicas = append(hint.Replicas, target)
	}
	return hint
}

func ptr[T any](value T) *T {
	return &value
}
