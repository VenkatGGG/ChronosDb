package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Options configures a storage engine.
type Options struct {
	Dir string
	FS  vfs.FS
}

// Metadata describes the local bootstrap state of an engine.
type Metadata struct {
	Bootstrapped bool
	Ident        StoreIdent
	Version      StoreVersion
}

// Engine is the Phase 1 single-node storage wrapper around Pebble.
type Engine struct {
	db       *pebble.DB
	dir      string
	fs       vfs.FS
	metadata Metadata
}

// Open opens the Pebble engine and loads local bootstrap metadata if present.
func Open(_ context.Context, opts Options) (*Engine, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("open engine: directory is required")
	}
	if opts.FS == nil {
		opts.FS = vfs.Default
	}

	db, err := pebble.Open(opts.Dir, &pebble.Options{FS: opts.FS})
	if err != nil {
		return nil, fmt.Errorf("open engine: %w", err)
	}

	engine := &Engine{
		db:  db,
		dir: opts.Dir,
		fs:  opts.FS,
	}
	metadata, err := engine.loadMetadata()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	engine.metadata = metadata
	return engine, nil
}

// Close closes the underlying Pebble engine.
func (e *Engine) Close() error {
	if e == nil || e.db == nil {
		return nil
	}
	return e.db.Close()
}

// Metadata returns the current local bootstrap metadata.
func (e *Engine) Metadata() Metadata {
	return e.metadata
}

// Bootstrap writes the store identity and current store version into an empty store.
func (e *Engine) Bootstrap(_ context.Context, ident StoreIdent) error {
	if err := ident.Validate(); err != nil {
		return err
	}
	if e.metadata.Bootstrapped {
		return ErrAlreadyBootstrapped
	}

	identPayload, err := ident.marshalBinary()
	if err != nil {
		return err
	}
	versionPayload := CurrentStoreVersion.marshalBinary()

	batch := e.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(storeIdentKey, identPayload, nil); err != nil {
		return fmt.Errorf("bootstrap store ident: %w", err)
	}
	if err := batch.Set(storeVersionKey, versionPayload, nil); err != nil {
		return fmt.Errorf("bootstrap store version: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("bootstrap commit: %w", err)
	}

	e.metadata = Metadata{
		Bootstrapped: true,
		Ident:        ident,
		Version:      CurrentStoreVersion,
	}
	return nil
}

// PutRaw writes a raw key/value pair through the engine.
func (e *Engine) PutRaw(_ context.Context, key, value []byte) error {
	return e.db.Set(key, value, pebble.Sync)
}

// GetRaw returns a copy of the value stored for key.
func (e *Engine) GetRaw(_ context.Context, key []byte) ([]byte, error) {
	value, closer, err := e.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, pebble.ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return bytes.Clone(value), nil
}

// PutMVCCValue writes one committed MVCC version.
func (e *Engine) PutMVCCValue(ctx context.Context, logicalKey []byte, ts hlc.Timestamp, value []byte) error {
	encoded, err := EncodeMVCCVersionKey(logicalKey, ts)
	if err != nil {
		return err
	}
	return e.PutRaw(ctx, encoded, value)
}

// GetMVCCValue returns one committed MVCC version by exact timestamp.
func (e *Engine) GetMVCCValue(ctx context.Context, logicalKey []byte, ts hlc.Timestamp) ([]byte, error) {
	encoded, err := EncodeMVCCVersionKey(logicalKey, ts)
	if err != nil {
		return nil, err
	}
	value, err := e.GetRaw(ctx, encoded)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrMVCCValueNotFound
	}
	return value, err
}

// PutIntent writes the provisional intent at the logical key's metadata key.
func (e *Engine) PutIntent(ctx context.Context, logicalKey []byte, intent Intent) error {
	key, err := EncodeMVCCMetadataKey(logicalKey)
	if err != nil {
		return err
	}
	value, err := intent.MarshalBinary()
	if err != nil {
		return err
	}
	return e.PutRaw(ctx, key, value)
}

// GetIntent loads the provisional intent at a logical key.
func (e *Engine) GetIntent(ctx context.Context, logicalKey []byte) (Intent, error) {
	key, err := EncodeMVCCMetadataKey(logicalKey)
	if err != nil {
		return Intent{}, err
	}
	value, err := e.GetRaw(ctx, key)
	if errors.Is(err, pebble.ErrNotFound) {
		return Intent{}, ErrIntentNotFound
	}
	if err != nil {
		return Intent{}, err
	}
	var intent Intent
	if err := intent.UnmarshalBinary(value); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

// NewSnapshot creates a read-only point-in-time snapshot.
func (e *Engine) NewSnapshot() *Snapshot {
	return &Snapshot{snap: e.db.NewSnapshot()}
}

func (e *Engine) loadMetadata() (Metadata, error) {
	identPayload, identErr := e.GetRaw(context.Background(), storeIdentKey)
	versionPayload, versionErr := e.GetRaw(context.Background(), storeVersionKey)

	identExists := identErr == nil
	versionExists := versionErr == nil
	if !identExists && errors.Is(identErr, pebble.ErrNotFound) && !versionExists && errors.Is(versionErr, pebble.ErrNotFound) {
		return Metadata{}, nil
	}
	if identErr != nil && !errors.Is(identErr, pebble.ErrNotFound) {
		return Metadata{}, fmt.Errorf("load metadata ident: %w", identErr)
	}
	if versionErr != nil && !errors.Is(versionErr, pebble.ErrNotFound) {
		return Metadata{}, fmt.Errorf("load metadata version: %w", versionErr)
	}
	if identExists != versionExists {
		return Metadata{}, ErrCorruptMetadata
	}

	ident, err := unmarshalStoreIdent(identPayload)
	if err != nil {
		return Metadata{}, fmt.Errorf("%w: %v", ErrCorruptMetadata, err)
	}
	version, err := unmarshalStoreVersion(versionPayload)
	if err != nil {
		return Metadata{}, fmt.Errorf("%w: %v", ErrCorruptMetadata, err)
	}
	if !CurrentStoreVersion.Compatible(version) {
		return Metadata{}, fmt.Errorf("%w: disk=%+v binary=%+v", ErrIncompatibleStoreVersion, version, CurrentStoreVersion)
	}

	return Metadata{
		Bootstrapped: true,
		Ident:        ident,
		Version:      version,
	}, nil
}

// Snapshot is a point-in-time read-only view of Pebble state.
type Snapshot struct {
	snap *pebble.Snapshot
}

// Close releases the underlying snapshot handle.
func (s *Snapshot) Close() error {
	if s == nil || s.snap == nil {
		return nil
	}
	return s.snap.Close()
}

// GetRaw returns a copy of the raw value visible to the snapshot.
func (s *Snapshot) GetRaw(_ context.Context, key []byte) ([]byte, error) {
	value, closer, err := s.snap.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, pebble.ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return bytes.Clone(value), nil
}

// GetMVCCValue returns one committed MVCC version from the snapshot.
func (s *Snapshot) GetMVCCValue(ctx context.Context, logicalKey []byte, ts hlc.Timestamp) ([]byte, error) {
	encoded, err := EncodeMVCCVersionKey(logicalKey, ts)
	if err != nil {
		return nil, err
	}
	value, err := s.GetRaw(ctx, encoded)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrMVCCValueNotFound
	}
	return value, err
}

// GetIntent returns the intent visible to the snapshot.
func (s *Snapshot) GetIntent(ctx context.Context, logicalKey []byte) (Intent, error) {
	key, err := EncodeMVCCMetadataKey(logicalKey)
	if err != nil {
		return Intent{}, err
	}
	value, err := s.GetRaw(ctx, key)
	if errors.Is(err, pebble.ErrNotFound) {
		return Intent{}, ErrIntentNotFound
	}
	if err != nil {
		return Intent{}, err
	}
	var intent Intent
	if err := intent.UnmarshalBinary(value); err != nil {
		return Intent{}, err
	}
	return intent, nil
}
