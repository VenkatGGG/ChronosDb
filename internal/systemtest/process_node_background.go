package systemtest

import (
	"context"
	"fmt"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/adminapi"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

const defaultHeartbeatInterval = time.Second

func (n *ProcessNode) serveBackground(ctx context.Context) {
	interval := n.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	_ = n.heartbeatLiveness(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = n.heartbeatLiveness(ctx)
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
