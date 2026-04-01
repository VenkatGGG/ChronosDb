package storage

import "errors"

var (
	// ErrAlreadyBootstrapped reports that a store already has bootstrap metadata.
	ErrAlreadyBootstrapped = errors.New("store already bootstrapped")
	// ErrNotBootstrapped reports that a store has not yet been initialized.
	ErrNotBootstrapped = errors.New("store not bootstrapped")
	// ErrIncompatibleStoreVersion reports an unsupported on-disk store version.
	ErrIncompatibleStoreVersion = errors.New("incompatible store version")
	// ErrCorruptMetadata reports inconsistent local metadata.
	ErrCorruptMetadata = errors.New("corrupt store metadata")
	// ErrMVCCValueNotFound reports that a specific MVCC version does not exist.
	ErrMVCCValueNotFound = errors.New("mvcc value not found")
	// ErrMVCCValueDeleted reports that a specific MVCC version is a tombstone.
	ErrMVCCValueDeleted = errors.New("mvcc value deleted")
	// ErrIntentNotFound reports that no intent exists for a logical key.
	ErrIntentNotFound = errors.New("intent not found")
	// ErrClosedTimestampNotFound reports that no closed timestamp publication exists for a range.
	ErrClosedTimestampNotFound = errors.New("closed timestamp not found")
)
