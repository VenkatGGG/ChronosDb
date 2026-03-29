package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/multiraft"
	"github.com/VenkatGGG/ChronosDb/internal/replica"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/txn"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	defaultClusterID    = "chronos-local"
	defaultTickInterval = 100 * time.Millisecond
)

var (
	// ErrRangeNotHosted reports that a request targeted a range not present on the local store.
	ErrRangeNotHosted = errors.New("runtime: range not hosted on local store")
)

// OutboundMessage is one transport-ready Raft message with its resolved target node.
type OutboundMessage struct {
	RangeID      uint64
	TargetNodeID uint64
	Message      raftpb.Message
}

// Transport delivers outbound Raft messages for hosted groups.
type Transport interface {
	Send(context.Context, []OutboundMessage) error
}

// Config configures one live node runtime host.
type Config struct {
	NodeID            uint64
	StoreID           uint64
	ClusterID         string
	DataDir           string
	SeedRanges        []meta.RangeDescriptor
	BootstrapPath     string
	BootstrapManifest *BootstrapManifest
	TickInterval      time.Duration
	Transport         Transport
	FS                vfs.FS
}

// Host is the live runtime assembly behind one ChronosDB node process.
type Host struct {
	mu           sync.Mutex
	engine       *storage.Engine
	catalog      *meta.Catalog
	scheduler    *multiraft.Scheduler
	ident        storage.StoreIdent
	transport    Transport
	tickInterval time.Duration
	bootstrapped bool
}

// KVRow is one live logical row returned from a local runtime scan.
type KVRow struct {
	LogicalKey []byte
	Timestamp  hlc.Timestamp
	Value      []byte
}

type noopTransport struct{}

func (noopTransport) Send(context.Context, []OutboundMessage) error {
	return nil
}

// Open constructs a runtime host backed by Pebble and the shared MultiRaft scheduler.
func Open(ctx context.Context, cfg Config) (*Host, error) {
	if cfg.NodeID == 0 {
		return nil, fmt.Errorf("runtime: node id must be non-zero")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("runtime: data dir must not be empty")
	}
	if cfg.StoreID == 0 {
		cfg.StoreID = cfg.NodeID
	}
	if cfg.ClusterID == "" {
		cfg.ClusterID = defaultClusterID
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.Transport == nil {
		cfg.Transport = noopTransport{}
	}
	manifest, err := resolveBootstrapManifest(cfg)
	if err != nil {
		return nil, err
	}
	if manifest != nil && cfg.ClusterID == defaultClusterID {
		cfg.ClusterID = manifest.ClusterID
	}

	engine, err := storage.Open(ctx, storage.Options{
		Dir: cfg.DataDir,
		FS:  cfg.FS,
	})
	if err != nil {
		return nil, err
	}
	if !engine.Metadata().Bootstrapped {
		if err := engine.Bootstrap(ctx, storage.StoreIdent{
			ClusterID: cfg.ClusterID,
			NodeID:    cfg.NodeID,
			StoreID:   cfg.StoreID,
		}); err != nil {
			_ = engine.Close()
			return nil, err
		}
	}

	host := &Host{
		engine:       engine,
		catalog:      meta.NewCatalog(engine),
		scheduler:    multiraft.NewScheduler(engine),
		ident:        engine.Metadata().Ident,
		transport:    cfg.Transport,
		tickInterval: cfg.TickInterval,
		bootstrapped: manifest != nil || len(cfg.SeedRanges) > 0,
	}
	if manifest != nil {
		if err := host.seedBootstrapManifest(ctx, *manifest); err != nil {
			_ = engine.Close()
			return nil, err
		}
	} else {
		if err := host.seedInitialDescriptors(ctx, cfg.SeedRanges, nil); err != nil {
			_ = engine.Close()
			return nil, err
		}
	}
	if err := host.loadHostedGroups(); err != nil {
		_ = engine.Close()
		return nil, err
	}
	return host, nil
}

// Metadata returns the persisted local store bootstrap metadata.
func (h *Host) Metadata() storage.Metadata {
	return h.engine.Metadata()
}

// HostedDescriptors returns the locally persisted descriptors hosted by this store.
func (h *Host) HostedDescriptors() ([]meta.RangeDescriptor, error) {
	ids, err := h.engine.ScanRangeDescriptorIDs()
	if err != nil {
		return nil, err
	}
	descs := make([]meta.RangeDescriptor, 0, len(ids))
	for _, rangeID := range ids {
		payload, err := h.engine.GetRaw(context.Background(), storage.RangeDescriptorKey(rangeID))
		if err != nil {
			return nil, err
		}
		var desc meta.RangeDescriptor
		if err := desc.UnmarshalBinary(payload); err != nil {
			return nil, err
		}
		descs = append(descs, desc)
	}
	return descs, nil
}

// NodeID returns the local node identifier.
func (h *Host) NodeID() uint64 {
	return h.ident.NodeID
}

// LookupRange resolves the authoritative descriptor for one logical key.
func (h *Host) LookupRange(ctx context.Context, key []byte) (meta.RangeDescriptor, error) {
	return h.catalog.Lookup(ctx, key)
}

// ReadLatestLocal returns the latest committed value for a key hosted on the local store.
func (h *Host) ReadLatestLocal(ctx context.Context, key []byte) ([]byte, meta.RangeDescriptor, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	desc, err := h.catalog.Lookup(ctx, key)
	if err != nil {
		return nil, meta.RangeDescriptor{}, false, err
	}
	if !descriptorReplicaOnNode(desc, h.ident.NodeID) {
		return nil, meta.RangeDescriptor{}, false, ErrRangeNotHosted
	}
	replicaState, err := h.scheduler.Replica(desc.RangeID)
	if err != nil {
		return nil, meta.RangeDescriptor{}, false, err
	}
	value, ts, found, err := h.engine.GetLatestMVCCValue(ctx, key)
	if err != nil || !found {
		return value, desc, found, err
	}
	if readValue, readErr := readLeaseholderValue(ctx, replicaState, key, ts); readErr == nil {
		return readValue, desc, true, nil
	}
	value, err = replicaState.ReadExact(ctx, key, ts)
	if err != nil {
		return nil, meta.RangeDescriptor{}, false, err
	}
	return value, desc, true, nil
}

// ScanRangeLocal returns the latest committed row version for each key in the span hosted locally.
func (h *Host) ScanRangeLocal(ctx context.Context, startKey, endKey []byte, startInclusive, endInclusive bool) ([]KVRow, meta.RangeDescriptor, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	desc, err := h.catalog.Lookup(ctx, startKey)
	if err != nil {
		return nil, meta.RangeDescriptor{}, err
	}
	if !descriptorReplicaOnNode(desc, h.ident.NodeID) {
		return nil, meta.RangeDescriptor{}, ErrRangeNotHosted
	}
	versions, err := h.engine.ScanLatestMVCCRange(ctx, startKey, endKey, startInclusive, endInclusive)
	if err != nil {
		return nil, meta.RangeDescriptor{}, err
	}
	rows := make([]KVRow, 0, len(versions))
	for _, version := range versions {
		rows = append(rows, KVRow{
			LogicalKey: append([]byte(nil), version.LogicalKey...),
			Timestamp:  version.Timestamp,
			Value:      append([]byte(nil), version.Value...),
		})
	}
	return rows, desc, nil
}

// PutValueLocal proposes one committed value on a locally hosted range and waits
// until the version is durably visible on the local store.
func (h *Host) PutValueLocal(ctx context.Context, key []byte, ts hlc.Timestamp, value []byte) (meta.RangeDescriptor, error) {
	h.mu.Lock()
	desc, err := h.catalog.Lookup(ctx, key)
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	if !descriptorReplicaOnNode(desc, h.ident.NodeID) {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, ErrRangeNotHosted
	}
	payload, err := replica.Command{
		Version: 1,
		Type:    replica.CommandTypePutValue,
		Put: &replica.PutValue{
			LogicalKey: append([]byte(nil), key...),
			Timestamp:  ts,
			Value:      append([]byte(nil), value...),
		},
	}.Marshal()
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	if err := h.scheduler.Propose(desc.RangeID, payload); err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	outbound, err := h.processReadyLocked()
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	h.mu.Unlock()
	if err := h.dispatchOutbound(ctx, outbound); err != nil {
		return meta.RangeDescriptor{}, err
	}

	if err := h.waitForMVCCValue(ctx, key, ts); err != nil {
		return meta.RangeDescriptor{}, err
	}
	return desc, nil
}

// PutIntentLocal proposes one provisional intent on a locally hosted range.
func (h *Host) PutIntentLocal(ctx context.Context, key []byte, intent storage.Intent) (meta.RangeDescriptor, error) {
	h.mu.Lock()
	desc, err := h.catalog.Lookup(ctx, key)
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	if !descriptorReplicaOnNode(desc, h.ident.NodeID) {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, ErrRangeNotHosted
	}
	payload, err := replica.Command{
		Version: 1,
		Type:    replica.CommandTypePutIntent,
		Intent: &replica.PutIntent{
			LogicalKey: append([]byte(nil), key...),
			Intent:     intent,
		},
	}.Marshal()
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	if err := h.scheduler.Propose(desc.RangeID, payload); err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	outbound, err := h.processReadyLocked()
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	h.mu.Unlock()
	if err := h.dispatchOutbound(ctx, outbound); err != nil {
		return meta.RangeDescriptor{}, err
	}
	if err := h.waitForIntent(ctx, key, intent); err != nil {
		return meta.RangeDescriptor{}, err
	}
	return desc, nil
}

// DeleteIntentLocal removes one provisional intent from a locally hosted range.
func (h *Host) DeleteIntentLocal(ctx context.Context, key []byte) (meta.RangeDescriptor, error) {
	h.mu.Lock()
	desc, err := h.catalog.Lookup(ctx, key)
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	if !descriptorReplicaOnNode(desc, h.ident.NodeID) {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, ErrRangeNotHosted
	}
	payload, err := replica.Command{
		Version: 1,
		Type:    replica.CommandTypeDeleteIntent,
		ClearIntent: &replica.DeleteIntent{
			LogicalKey: append([]byte(nil), key...),
		},
	}.Marshal()
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	if err := h.scheduler.Propose(desc.RangeID, payload); err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	outbound, err := h.processReadyLocked()
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	h.mu.Unlock()
	if err := h.dispatchOutbound(ctx, outbound); err != nil {
		return meta.RangeDescriptor{}, err
	}
	if err := h.waitForIntentGone(ctx, key); err != nil {
		return meta.RangeDescriptor{}, err
	}
	return desc, nil
}

// PutTxnRecordLocal stores one durable transaction record on its system-span anchor range.
func (h *Host) PutTxnRecordLocal(ctx context.Context, record txn.Record) (meta.RangeDescriptor, error) {
	if err := record.Validate(); err != nil {
		return meta.RangeDescriptor{}, err
	}
	key := storage.GlobalTxnRecordKey(record.ID)

	h.mu.Lock()
	desc, err := h.catalog.Lookup(ctx, key)
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	if !descriptorReplicaOnNode(desc, h.ident.NodeID) {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, ErrRangeNotHosted
	}
	payload, err := replica.Command{
		Version: 1,
		Type:    replica.CommandTypeSetTxnRecord,
		TxnRecord: &replica.SetTxnRecord{
			Record: record,
		},
	}.Marshal()
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	if err := h.scheduler.Propose(desc.RangeID, payload); err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	outbound, err := h.processReadyLocked()
	if err != nil {
		h.mu.Unlock()
		return meta.RangeDescriptor{}, err
	}
	h.mu.Unlock()
	if err := h.dispatchOutbound(ctx, outbound); err != nil {
		return meta.RangeDescriptor{}, err
	}
	if err := h.waitForTxnRecord(ctx, record.ID, record); err != nil {
		return meta.RangeDescriptor{}, err
	}
	return desc, nil
}

// GetTxnRecordLocal loads one durable transaction record from the local store.
func (h *Host) GetTxnRecordLocal(ctx context.Context, txnID storage.TxnID) (txn.Record, error) {
	payload, err := h.engine.GetRaw(ctx, storage.GlobalTxnRecordKey(txnID))
	if err != nil {
		return txn.Record{}, err
	}
	var record txn.Record
	if err := record.UnmarshalBinary(payload); err != nil {
		return txn.Record{}, err
	}
	return record, nil
}

// Campaign forces the local replica to begin an election for the specified range.
func (h *Host) Campaign(ctx context.Context, rangeID uint64) error {
	h.mu.Lock()
	if err := h.scheduler.Campaign(rangeID); err != nil {
		h.mu.Unlock()
		return err
	}
	outbound, err := h.processReadyLocked()
	h.mu.Unlock()
	if err != nil {
		return err
	}
	return h.dispatchOutbound(ctx, outbound)
}

// Leader returns the current known leader replica id for the range.
func (h *Host) Leader(rangeID uint64) (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.scheduler.Leader(rangeID)
}

// Step delivers one inbound Raft message and persists any resulting Ready state.
func (h *Host) Step(ctx context.Context, rangeID uint64, msg raftpb.Message) error {
	h.mu.Lock()
	if err := h.scheduler.Step(rangeID, msg); err != nil {
		h.mu.Unlock()
		return err
	}
	outbound, err := h.processReadyLocked()
	h.mu.Unlock()
	if err != nil {
		return err
	}
	return h.dispatchOutbound(ctx, outbound)
}

// Run drives the background MultiRaft tick and ready-processing loop.
func (h *Host) Run(ctx context.Context) error {
	h.mu.Lock()
	outbound, err := h.processReadyLocked()
	if err != nil {
		h.mu.Unlock()
		return err
	}
	if h.bootstrapped {
		bootstrapOutbound, err := h.campaignBootstrapLeaseholdersLocked()
		if err != nil {
			h.mu.Unlock()
			return err
		}
		outbound = append(outbound, bootstrapOutbound...)
	}
	h.mu.Unlock()
	if err := h.dispatchOutbound(ctx, outbound); err != nil {
		return err
	}
	ticker := time.NewTicker(h.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			h.mu.Lock()
			h.scheduler.Tick()
			outbound, err := h.processReadyLocked()
			if err != nil {
				h.mu.Unlock()
				return err
			}
			h.mu.Unlock()
			if err := h.dispatchOutbound(ctx, outbound); err != nil {
				return err
			}
		}
	}
}

// Close releases the underlying storage engine.
func (h *Host) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.engine.Close()
}

func (h *Host) seedBootstrapManifest(ctx context.Context, manifest BootstrapManifest) error {
	hasMeta, err := h.hasMetaLayout()
	if err != nil {
		return err
	}
	if !hasMeta {
		if err := h.catalog.BootstrapLayout(manifest.Layout()); err != nil {
			return err
		}
	}
	return h.seedInitialDescriptors(ctx, manifest.AllRanges(), &manifest)
}

func (h *Host) seedInitialDescriptors(ctx context.Context, descs []meta.RangeDescriptor, manifest *BootstrapManifest) error {
	if len(descs) == 0 {
		return nil
	}
	existing, err := h.engine.ScanRangeDescriptorIDs()
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		return nil
	}

	batch := h.engine.NewWriteBatch()
	defer batch.Close()
	for _, desc := range descs {
		if err := desc.Validate(); err != nil {
			return fmt.Errorf("runtime: invalid seed descriptor %d: %w", desc.RangeID, err)
		}
		if !descriptorReplicaOnNode(desc, h.ident.NodeID) {
			continue
		}
		payload, err := desc.MarshalBinary()
		if err != nil {
			return err
		}
		if err := batch.SetRaw(storage.RangeDescriptorKey(desc.RangeID), payload); err != nil {
			return err
		}
		if manifest != nil {
			record, ok, err := manifest.InitialLease(desc)
			if err != nil {
				return err
			}
			if ok {
				if err := batch.SetRangeLease(desc.RangeID, record); err != nil {
					return err
				}
			}
		}
	}
	if err := batch.Commit(true); err != nil {
		return err
	}
	if manifest != nil {
		return nil
	}
	for _, desc := range descs {
		if err := h.catalog.Upsert(ctx, meta.LevelMeta2, desc); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) loadHostedGroups() error {
	descs, err := h.HostedDescriptors()
	if err != nil {
		return err
	}
	for _, desc := range descs {
		replicaID, ok := localReplicaID(desc, h.ident.NodeID)
		if !ok {
			continue
		}
		peers := make([]uint64, 0, len(desc.Replicas))
		for _, replica := range desc.Replicas {
			peers = append(peers, replica.ReplicaID)
		}
		if err := h.scheduler.AddGroup(multiraft.GroupConfig{
			RangeID:   desc.RangeID,
			ReplicaID: replicaID,
			Peers:     peers,
		}); err != nil {
			return fmt.Errorf("runtime: add hosted group %d: %w", desc.RangeID, err)
		}
	}
	return nil
}

func (h *Host) processReadyLocked() ([]OutboundMessage, error) {
	result, err := h.scheduler.ProcessReady()
	if err != nil {
		return nil, err
	}
	if len(result.Messages) == 0 {
		return nil, nil
	}
	outbound, err := h.resolveOutbound(result.Messages)
	if err != nil {
		return nil, err
	}
	return outbound, nil
}

func descriptorReplicaOnNode(desc meta.RangeDescriptor, nodeID uint64) bool {
	_, ok := localReplicaID(desc, nodeID)
	return ok
}

func (h *Host) campaignBootstrapLeaseholdersLocked() ([]OutboundMessage, error) {
	descs, err := h.HostedDescriptors()
	if err != nil {
		return nil, err
	}
	var outbound []OutboundMessage
	for _, desc := range descs {
		replicaID, ok := localReplicaID(desc, h.ident.NodeID)
		if !ok || replicaID != desc.LeaseholderReplicaID {
			continue
		}
		leader, err := h.scheduler.Leader(desc.RangeID)
		if err == nil && leader == replicaID {
			continue
		}
		if err := h.scheduler.Campaign(desc.RangeID); err != nil {
			return nil, err
		}
		readyOutbound, err := h.processReadyLocked()
		if err != nil {
			return nil, err
		}
		outbound = append(outbound, readyOutbound...)
	}
	h.bootstrapped = false
	return outbound, nil
}

func localReplicaID(desc meta.RangeDescriptor, nodeID uint64) (uint64, bool) {
	for _, replica := range desc.Replicas {
		if replica.NodeID == nodeID {
			return replica.ReplicaID, true
		}
	}
	return 0, false
}

func resolveBootstrapManifest(cfg Config) (*BootstrapManifest, error) {
	if cfg.BootstrapPath == "" {
		if cfg.BootstrapManifest == nil {
			return nil, nil
		}
		copyManifest := *cfg.BootstrapManifest
		return &copyManifest, copyManifest.Validate()
	}

	manifest, err := LoadBootstrapManifest(cfg.BootstrapPath)
	if err == nil {
		return &manifest, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if cfg.BootstrapManifest == nil {
		return nil, nil
	}
	copyManifest := *cfg.BootstrapManifest
	if err := WriteBootstrapManifest(cfg.BootstrapPath, copyManifest); err != nil {
		return nil, err
	}
	return &copyManifest, nil
}

func (h *Host) resolveOutbound(messages []multiraft.MessageEnvelope) ([]OutboundMessage, error) {
	outbound := make([]OutboundMessage, 0, len(messages))
	for _, envelope := range messages {
		if envelope.Message.To == 0 {
			continue
		}
		replicaState, err := h.scheduler.Replica(envelope.RangeID)
		if err != nil {
			return nil, err
		}
		targetNodeID, ok := nodeForReplica(replicaState.Descriptor(), envelope.Message.To)
		if !ok {
			return nil, fmt.Errorf("runtime: range %d target replica %d not found in descriptor", envelope.RangeID, envelope.Message.To)
		}
		outbound = append(outbound, OutboundMessage{
			RangeID:      envelope.RangeID,
			TargetNodeID: targetNodeID,
			Message:      envelope.Message,
		})
	}
	return outbound, nil
}

func (h *Host) hasMetaLayout() (bool, error) {
	meta1, err := h.engine.ScanRawRange(storage.Meta1Prefix(), storage.PrefixEnd(storage.Meta1Prefix()))
	if err != nil {
		return false, err
	}
	meta2, err := h.engine.ScanRawRange(storage.Meta2Prefix(), storage.PrefixEnd(storage.Meta2Prefix()))
	if err != nil {
		return false, err
	}
	return len(meta1) > 0 && len(meta2) > 0, nil
}

func nodeForReplica(desc meta.RangeDescriptor, replicaID uint64) (uint64, bool) {
	for _, replica := range desc.Replicas {
		if replica.ReplicaID == replicaID {
			return replica.NodeID, true
		}
	}
	return 0, false
}

func (h *Host) dispatchOutbound(ctx context.Context, outbound []OutboundMessage) error {
	if len(outbound) == 0 {
		return nil
	}
	if err := h.transport.Send(ctx, outbound); err != nil {
		// Transient transport failures are retried on later Raft ticks.
		return nil
	}
	return nil
}

func (h *Host) waitForMVCCValue(ctx context.Context, key []byte, ts hlc.Timestamp) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := h.engine.GetMVCCValue(ctx, key, ts)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, storage.ErrMVCCValueNotFound):
		default:
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *Host) waitForIntent(ctx context.Context, key []byte, expected storage.Intent) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		intent, err := h.engine.GetIntent(ctx, key)
		switch {
		case err == nil:
			if intent.TxnID == expected.TxnID &&
				intent.Epoch == expected.Epoch &&
				intent.WriteTimestamp == expected.WriteTimestamp &&
				intent.Strength == expected.Strength &&
				bytes.Equal(intent.Value, expected.Value) {
				return nil
			}
		case errors.Is(err, storage.ErrIntentNotFound):
		default:
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *Host) waitForIntentGone(ctx context.Context, key []byte) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := h.engine.GetIntent(ctx, key)
		switch {
		case errors.Is(err, storage.ErrIntentNotFound):
			return nil
		case err != nil:
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *Host) waitForTxnRecord(ctx context.Context, txnID storage.TxnID, expected txn.Record) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, err := h.GetTxnRecordLocal(ctx, txnID)
		switch {
		case err == nil:
			if recordsEqual(record, expected) {
				return nil
			}
		default:
			if errors.Is(err, pebble.ErrNotFound) {
				// Wait for the replicated system key to appear.
			} else {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func recordsEqual(left, right txn.Record) bool {
	if left.ID != right.ID ||
		left.Status != right.Status ||
		left.ReadTS != right.ReadTS ||
		left.WriteTS != right.WriteTS ||
		left.MinCommitTS != right.MinCommitTS ||
		left.Epoch != right.Epoch ||
		left.Priority != right.Priority ||
		left.AnchorRangeID != right.AnchorRangeID ||
		left.LastHeartbeatTS != right.LastHeartbeatTS ||
		left.DeadlineTS != right.DeadlineTS {
		return false
	}
	if len(left.TouchedRanges) != len(right.TouchedRanges) {
		return false
	}
	for i := range left.TouchedRanges {
		if left.TouchedRanges[i] != right.TouchedRanges[i] {
			return false
		}
	}
	return true
}

func readLeaseholderValue(ctx context.Context, replicaState *replica.StateMachine, key []byte, ts hlc.Timestamp) ([]byte, error) {
	record := replicaState.Lease()
	return replicaState.FastGet(ctx, key, ts, lease.FastReadRequest{
		CurrentLivenessEpoch: record.HolderLivenessEpoch,
		Now:                  record.StartTS,
		ReadTimestamp:        ts,
	})
}
