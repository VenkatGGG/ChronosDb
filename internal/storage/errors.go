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
	// ErrIntentNotFound reports that no intent exists for a logical key.
	ErrIntentNotFound = errors.New("intent not found")
)
