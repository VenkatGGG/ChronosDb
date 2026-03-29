package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/multiraft"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	defaultClusterID    = "chronos-local"
	defaultTickInterval = 100 * time.Millisecond
)

// Transport delivers outbound Raft messages for hosted groups.
type Transport interface {
	Send(context.Context, []multiraft.MessageEnvelope) error
}

// Config configures one live node runtime host.
type Config struct {
	NodeID       uint64
	StoreID      uint64
	ClusterID    string
	DataDir      string
	SeedRanges   []meta.RangeDescriptor
	TickInterval time.Duration
	Transport    Transport
	FS           vfs.FS
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

func (noopTransport) Send(context.Context, []multiraft.MessageEnvelope) error {
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
	if err := host.seedInitialDescriptors(ctx, cfg.SeedRanges); err != nil {
		_ = engine.Close()
		return nil, err
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

func (h *Host) seedInitialDescriptors(ctx context.Context, descs []meta.RangeDescriptor) error {
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
	}
	if err := batch.Commit(true); err != nil {
		return err
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
	return h.transport.Send(ctx, result.Messages)
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
