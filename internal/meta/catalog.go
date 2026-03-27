package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

// ErrDescriptorNotFound reports that no authoritative descriptor matched a lookup key.
var ErrDescriptorNotFound = errors.New("descriptor not found")

// Level identifies which meta namespace a descriptor record belongs to.
type Level uint8

const (
	LevelMeta1 Level = iota + 1
	LevelMeta2
)

// Catalog is the authoritative meta descriptor store backed by Pebble.
type Catalog struct {
	engine *storage.Engine
}

// BootstrapLayout is the authoritative descriptor image installed during cluster bootstrap.
type BootstrapLayout struct {
	Meta1 []RangeDescriptor
	Meta2 []RangeDescriptor
}

// NewCatalog constructs a catalog backed by the given engine.
func NewCatalog(engine *storage.Engine) *Catalog {
	return &Catalog{engine: engine}
}

// BootstrapLayout atomically installs the initial meta1/meta2 descriptor image.
func (c *Catalog) BootstrapLayout(layout BootstrapLayout) error {
	if len(layout.Meta1) == 0 {
		return fmt.Errorf("bootstrap layout: at least one meta1 descriptor is required")
	}
	if len(layout.Meta2) == 0 {
		return fmt.Errorf("bootstrap layout: at least one meta2 descriptor is required")
	}
	batch := c.engine.NewWriteBatch()
	defer batch.Close()
	for _, desc := range layout.Meta1 {
		if err := stageDescriptor(batch, LevelMeta1, desc); err != nil {
			return err
		}
	}
	for _, desc := range layout.Meta2 {
		if err := stageDescriptor(batch, LevelMeta2, desc); err != nil {
			return err
		}
	}
	return batch.Commit(true)
}

// Upsert stores one descriptor in the selected meta level.
func (c *Catalog) Upsert(ctx context.Context, level Level, desc RangeDescriptor) error {
	batch := c.engine.NewWriteBatch()
	defer batch.Close()
	if err := stageDescriptor(batch, level, desc); err != nil {
		return err
	}
	return batch.Commit(true)
}

// LookupMeta2 resolves a user/system key through the authoritative meta2 span map.
func (c *Catalog) LookupMeta2(_ context.Context, key []byte) (RangeDescriptor, error) {
	return c.lookup(LevelMeta2, key)
}

// LookupMeta1 resolves a meta2 key through the authoritative meta1 span map.
func (c *Catalog) LookupMeta1(_ context.Context, meta2Key []byte) (RangeDescriptor, error) {
	return c.lookup(LevelMeta1, meta2Key)
}

// Lookup resolves either a user/system key through meta2 or a meta2 key through meta1.
func (c *Catalog) Lookup(ctx context.Context, key []byte) (RangeDescriptor, error) {
	if bytes.HasPrefix(key, storage.Meta2Prefix()) {
		return c.LookupMeta1(ctx, key)
	}
	return c.LookupMeta2(ctx, key)
}

func (c *Catalog) lookup(level Level, key []byte) (RangeDescriptor, error) {
	prefix := descriptorPrefix(level)
	item, ok, err := c.engine.SeekRawGE(prefix, prefixUpperBound(level), lookupKey(level, key))
	if err != nil {
		return RangeDescriptor{}, err
	}
	if !ok {
		return RangeDescriptor{}, ErrDescriptorNotFound
	}
	var desc RangeDescriptor
	if err := desc.UnmarshalBinary(item.Value); err != nil {
		return RangeDescriptor{}, err
	}
	if !desc.ContainsKey(key) {
		return RangeDescriptor{}, ErrDescriptorNotFound
	}
	return desc, nil
}

func stageDescriptor(batch *storage.WriteBatch, level Level, desc RangeDescriptor) error {
	if err := desc.Validate(); err != nil {
		return err
	}
	if len(desc.EndKey) == 0 && level == LevelMeta1 {
		return fmt.Errorf("meta1 descriptor: end key must be finite")
	}
	payload, err := desc.MarshalBinary()
	if err != nil {
		return err
	}
	return batch.SetRaw(descriptorKey(level, desc.EndKey), payload)
}

func descriptorKey(level Level, endKey []byte) []byte {
	switch level {
	case LevelMeta1:
		return storage.Meta1DescriptorKey(endKey)
	case LevelMeta2:
		return storage.Meta2DescriptorKey(endKey)
	default:
		panic(fmt.Sprintf("unknown meta level %d", level))
	}
}

func lookupKey(level Level, key []byte) []byte {
	switch level {
	case LevelMeta1:
		return storage.Meta1LookupKey(key)
	case LevelMeta2:
		return storage.Meta2LookupKey(key)
	default:
		panic(fmt.Sprintf("unknown meta level %d", level))
	}
}

func descriptorPrefix(level Level) []byte {
	switch level {
	case LevelMeta1:
		return storage.Meta1Prefix()
	case LevelMeta2:
		return storage.Meta2Prefix()
	default:
		panic(fmt.Sprintf("unknown meta level %d", level))
	}
}

func prefixUpperBound(level Level) []byte {
	prefix := descriptorPrefix(level)
	return append(prefix, 0xff, 0xff)
}
