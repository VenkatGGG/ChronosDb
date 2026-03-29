package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/multiraft"
	"github.com/VenkatGGG/ChronosDb/internal/replica"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	raft "go.etcd.io/raft/v3"
)

const (
	defaultLeaseTTL                 = 5 * time.Second
	defaultClosedTSLag              = 250 * time.Millisecond
	defaultLeaseMaintenanceInterval = 500 * time.Millisecond
	defaultClosedTSPublishInterval  = 250 * time.Millisecond
	defaultSplitCheckInterval       = time.Second
	defaultAutoSplitRows            = 64
)

func (h *Host) applyBackgroundDefaults() {
	if h.leaseTTL <= 0 {
		h.leaseTTL = defaultLeaseTTL
	}
	if h.closedTSLag <= 0 {
		h.closedTSLag = defaultClosedTSLag
	}
	if h.autoSplitRows <= 0 {
		h.autoSplitRows = defaultAutoSplitRows
	}
	if h.livenessEpoch == 0 {
		h.livenessEpoch = 1
	}
}

// SetLivenessEpoch updates the local liveness epoch used when maintaining leases.
func (h *Host) SetLivenessEpoch(epoch uint64) {
	if epoch == 0 {
		return
	}
	h.mu.Lock()
	if epoch > h.livenessEpoch {
		h.livenessEpoch = epoch
	}
	h.mu.Unlock()
}

// LivenessEpoch returns the current local liveness epoch.
func (h *Host) LivenessEpoch() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.livenessEpoch
}

func (h *Host) runBackgroundTickLocked(ctx context.Context, now time.Time) ([]OutboundMessage, error) {
	if err := h.ensureHostedGroupsLocked(); err != nil {
		return nil, err
	}
	var outbound []OutboundMessage
	appendOutbound := func(next []OutboundMessage) {
		if len(next) == 0 {
			return
		}
		outbound = append(outbound, next...)
	}

	next, err := h.maintainLeasesLocked(ctx, now)
	if err != nil {
		return nil, err
	}
	appendOutbound(next)

	next, err = h.publishClosedTimestampsLocked(ctx, now)
	if err != nil {
		return nil, err
	}
	appendOutbound(next)

	next, err = h.applySplitTriggersLocked(ctx, now)
	if err != nil {
		return nil, err
	}
	appendOutbound(next)
	return outbound, nil
}

func (h *Host) ensureHostedGroupsLocked() error {
	descs, err := h.HostedDescriptors()
	if err != nil {
		return err
	}
	for _, desc := range descs {
		if !descriptorReplicaOnNode(desc, h.ident.NodeID) {
			continue
		}
		if _, err := h.scheduler.Replica(desc.RangeID); err == nil {
			continue
		}
		replicaID, ok := localReplicaID(desc, h.ident.NodeID)
		if !ok {
			continue
		}
		peers := make([]uint64, 0, len(desc.Replicas))
		for _, repl := range desc.Replicas {
			peers = append(peers, repl.ReplicaID)
		}
		if err := h.scheduler.AddGroup(multiraft.GroupConfig{
			RangeID:   desc.RangeID,
			ReplicaID: replicaID,
			Peers:     peers,
		}); err != nil {
			return fmt.Errorf("runtime: add dynamic hosted group %d: %w", desc.RangeID, err)
		}
	}
	return nil
}

func (h *Host) maintainLeasesLocked(ctx context.Context, now time.Time) ([]OutboundMessage, error) {
	if !h.lastLeaseTick.IsZero() && now.Sub(h.lastLeaseTick) < defaultLeaseMaintenanceInterval {
		return nil, nil
	}
	h.lastLeaseTick = now

	descs, err := h.HostedDescriptors()
	if err != nil {
		return nil, err
	}
	nowTS := wallClockTimestamp(now)
	var outbound []OutboundMessage
	for _, desc := range descs {
		localReplica, ok := localReplicaID(desc, h.ident.NodeID)
		if !ok || localReplica != desc.LeaseholderReplicaID {
			continue
		}
		leader, err := h.scheduler.Leader(desc.RangeID)
		if err != nil {
			return nil, err
		}
		if leader == 0 {
			if err := h.scheduler.Campaign(desc.RangeID); err != nil {
				return nil, err
			}
			next, err := h.processReadyLocked()
			if err != nil {
				return nil, err
			}
			outbound = append(outbound, next...)
			leader = localReplica
		}
		if leader != localReplica {
			continue
		}
		replicaState, err := h.scheduler.Replica(desc.RangeID)
		if err != nil {
			return nil, err
		}
		record, needUpdate, err := desiredLeaseRecord(replicaState.Lease(), localReplica, h.livenessEpoch, nowTS, h.leaseTTL)
		if err != nil {
			return nil, err
		}
		if !needUpdate {
			continue
		}
		payload, err := replica.Command{
			Version: 1,
			Type:    replica.CommandTypeSetLease,
			Lease:   &replica.SetLease{Record: record},
		}.Marshal()
		if err != nil {
			return nil, err
		}
		next, err := h.proposeWithLeaseholderRetryLocked(ctx, desc, payload)
		if err != nil {
			if errors.Is(err, raft.ErrProposalDropped) {
				continue
			}
			return nil, err
		}
		outbound = append(outbound, next...)
	}
	return outbound, nil
}

func (h *Host) publishClosedTimestampsLocked(ctx context.Context, now time.Time) ([]OutboundMessage, error) {
	if !h.lastClosedTSTick.IsZero() && now.Sub(h.lastClosedTSTick) < defaultClosedTSPublishInterval {
		return nil, nil
	}
	h.lastClosedTSTick = now

	descs, err := h.HostedDescriptors()
	if err != nil {
		return nil, err
	}
	nowTS := wallClockTimestamp(now)
	publisher := closedts.NewPublisher()
	var outbound []OutboundMessage
	for _, desc := range descs {
		localReplica, ok := localReplicaID(desc, h.ident.NodeID)
		if !ok || localReplica != desc.LeaseholderReplicaID {
			continue
		}
		leader, err := h.scheduler.Leader(desc.RangeID)
		if err != nil {
			return nil, err
		}
		if leader != localReplica {
			continue
		}
		replicaState, err := h.scheduler.Replica(desc.RangeID)
		if err != nil {
			return nil, err
		}
		currentLease := replicaState.Lease()
		if err := currentLease.Validate(); err != nil {
			continue
		}
		lastApplied := replicaState.SafeReadFrontier()
		if lastApplied.IsZero() {
			lastApplied = currentLease.StartTS
		}
		var previous *closedts.Record
		if current, ok := replicaState.ClosedTimestamp(); ok {
			copyRecord := current
			previous = &copyRecord
		}
		oldestIntentTS, err := h.oldestUnresolvedIntentTimestamp(desc)
		if err != nil {
			return nil, err
		}
		record, advanced, err := publisher.Publish(previous, closedts.PublishInput{
			RangeID:                  desc.RangeID,
			Lease:                    currentLease,
			LastApplied:              lastApplied,
			Now:                      nowTS,
			MaxOffset:                uint64(h.closedTSLag),
			OldestUnresolvedIntentTS: oldestIntentTS,
		})
		if err != nil || !advanced {
			if err != nil {
				return nil, err
			}
			continue
		}
		payload, err := replica.Command{
			Version: 1,
			Type:    replica.CommandTypeSetClosedTS,
			ClosedTS: &replica.SetClosedTS{
				Record: record,
			},
		}.Marshal()
		if err != nil {
			return nil, err
		}
		next, err := h.proposeWithLeaseholderRetryLocked(ctx, desc, payload)
		if err != nil {
			if errors.Is(err, raft.ErrProposalDropped) {
				continue
			}
			return nil, err
		}
		outbound = append(outbound, next...)
	}
	return outbound, nil
}

func (h *Host) applySplitTriggersLocked(ctx context.Context, now time.Time) ([]OutboundMessage, error) {
	if h.autoSplitRows <= 1 {
		return nil, nil
	}
	if !h.lastSplitTick.IsZero() && now.Sub(h.lastSplitTick) < defaultSplitCheckInterval {
		return nil, nil
	}
	h.lastSplitTick = now

	descs, err := h.HostedDescriptors()
	if err != nil {
		return nil, err
	}
	var outbound []OutboundMessage
	for _, desc := range descs {
		if !bytes.HasPrefix(desc.StartKey, storage.GlobalTablePrefix()) || len(desc.EndKey) == 0 {
			continue
		}
		localReplica, ok := localReplicaID(desc, h.ident.NodeID)
		if !ok || localReplica != desc.LeaseholderReplicaID {
			continue
		}
		leader, err := h.scheduler.Leader(desc.RangeID)
		if err != nil {
			return nil, err
		}
		if leader != localReplica {
			continue
		}
		versions, err := h.engine.ScanLatestMVCCRange(ctx, desc.StartKey, desc.EndKey, true, false)
		if err != nil {
			return nil, err
		}
		if len(versions) <= h.autoSplitRows {
			continue
		}
		splitKey, ok := chooseSplitKey(desc, versions)
		if !ok {
			continue
		}
		nextRangeID, err := h.nextRangeIDLocked()
		if err != nil {
			return nil, err
		}
		left := desc
		left.Generation = desc.Generation + 1
		left.EndKey = append([]byte(nil), splitKey...)
		right := desc
		right.RangeID = nextRangeID
		right.Generation = desc.Generation + 1
		right.StartKey = append([]byte(nil), splitKey...)
		payload, err := replica.Command{
			Version: 1,
			Type:    replica.CommandTypeSplitRange,
			Split: &replica.SplitRange{
				ExpectedGeneration: desc.Generation,
				MetaLevel:          meta.LevelMeta2,
				Left:               left,
				Right:              right,
			},
		}.Marshal()
		if err != nil {
			return nil, err
		}
		next, err := h.proposeWithLeaseholderRetryLocked(ctx, desc, payload)
		if err != nil {
			if errors.Is(err, raft.ErrProposalDropped) {
				continue
			}
			return nil, err
		}
		outbound = append(outbound, next...)
		break
	}
	return outbound, nil
}

func desiredLeaseRecord(current lease.Record, replicaID, livenessEpoch uint64, now hlc.Timestamp, ttl time.Duration) (lease.Record, bool, error) {
	expiration := addDuration(now, ttl)
	switch {
	case current.Sequence == 0:
		record, err := lease.NewRecord(replicaID, livenessEpoch, now, expiration, 1)
		return record, err == nil, err
	case current.HolderReplicaID != replicaID || current.HolderLivenessEpoch != livenessEpoch:
		start := maxTimestamp(current.StartTS, now)
		record, err := lease.NewRecord(replicaID, livenessEpoch, start, expiration, current.Sequence+1)
		return record, err == nil, err
	case current.ExpirationTS.Compare(addDuration(now, ttl/2)) > 0:
		return current, false, nil
	default:
		start := maxTimestamp(current.StartTS, now)
		record, err := current.Renew(start, expiration)
		return record, err == nil, err
	}
}

func chooseSplitKey(desc meta.RangeDescriptor, versions []storage.MVCCVersion) ([]byte, bool) {
	if len(versions) < 2 {
		return nil, false
	}
	index := len(versions) / 2
	for index < len(versions) {
		key := versions[index].LogicalKey
		if bytes.Compare(key, desc.StartKey) > 0 && (len(desc.EndKey) == 0 || bytes.Compare(key, desc.EndKey) < 0) {
			return append([]byte(nil), key...), true
		}
		index++
	}
	return nil, false
}

func (h *Host) oldestUnresolvedIntentTimestamp(desc meta.RangeDescriptor) (*hlc.Timestamp, error) {
	kvs, err := h.engine.ScanRawRange(desc.StartKey, desc.EndKey)
	if err != nil {
		return nil, err
	}
	var oldest *hlc.Timestamp
	for _, kv := range kvs {
		decoded, err := storage.DecodeMVCCKey(kv.Key)
		if err != nil || decoded.Kind != storage.MVCCKeyKindMetadata {
			continue
		}
		var intent storage.Intent
		if err := intent.UnmarshalBinary(kv.Value); err != nil {
			continue
		}
		if oldest == nil || intent.WriteTimestamp.Compare(*oldest) < 0 {
			copyTS := intent.WriteTimestamp
			oldest = &copyTS
		}
	}
	return oldest, nil
}

func (h *Host) nextRangeIDLocked() (uint64, error) {
	ids, err := h.engine.ScanRangeDescriptorIDs()
	if err != nil {
		return 0, err
	}
	var maxID uint64
	for _, id := range ids {
		if id > maxID {
			maxID = id
		}
	}
	return maxID + 1, nil
}

func wallClockTimestamp(now time.Time) hlc.Timestamp {
	return hlc.Timestamp{WallTime: uint64(now.UTC().UnixNano())}
}

func addDuration(ts hlc.Timestamp, d time.Duration) hlc.Timestamp {
	if d <= 0 {
		return ts
	}
	return hlc.Timestamp{
		WallTime: ts.WallTime + uint64(d),
		Logical:  ts.Logical,
	}
}

func maxTimestamp(left, right hlc.Timestamp) hlc.Timestamp {
	if left.Compare(right) >= 0 {
		return left
	}
	return right
}
