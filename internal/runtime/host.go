package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/multiraft"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/cockroachdb/pebble/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	defaultClusterID    = "chronos-local"
	defaultTickInterval = 100 * time.Millisecond
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
	engine       *storage.Engine
	catalog      *meta.Catalog
	scheduler    *multiraft.Scheduler
	ident        storage.StoreIdent
	transport    Transport
	tickInterval time.Duration
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

// Campaign forces the local replica to begin an election for the specified range.
func (h *Host) Campaign(ctx context.Context, rangeID uint64) error {
	if err := h.scheduler.Campaign(rangeID); err != nil {
		return err
	}
	return h.processReady(ctx)
}

// Leader returns the current known leader replica id for the range.
func (h *Host) Leader(rangeID uint64) (uint64, error) {
	return h.scheduler.Leader(rangeID)
}

// Step delivers one inbound Raft message and persists any resulting Ready state.
func (h *Host) Step(ctx context.Context, rangeID uint64, msg raftpb.Message) error {
	if err := h.scheduler.Step(rangeID, msg); err != nil {
		return err
	}
	return h.processReady(ctx)
}

// Run drives the background MultiRaft tick and ready-processing loop.
func (h *Host) Run(ctx context.Context) error {
	if err := h.processReady(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(h.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			h.scheduler.Tick()
			if err := h.processReady(ctx); err != nil {
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

func (h *Host) processReady(ctx context.Context) error {
	result, err := h.scheduler.ProcessReady()
	if err != nil {
		return err
	}
	if len(result.Messages) == 0 {
		return nil
	}
	outbound, err := h.resolveOutbound(result.Messages)
	if err != nil {
		return err
	}
	if len(outbound) == 0 {
		return nil
	}
	if err := h.transport.Send(ctx, outbound); err != nil {
		// Transient transport failures are retried on later Raft ticks.
		return nil
	}
	return nil
}

func descriptorReplicaOnNode(desc meta.RangeDescriptor, nodeID uint64) bool {
	_, ok := localReplicaID(desc, nodeID)
	return ok
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
