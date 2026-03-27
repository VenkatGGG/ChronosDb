package txn

import (
	"bytes"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

// Status is the durable transaction state.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusStaging   Status = "STAGING"
	StatusCommitted Status = "COMMITTED"
	StatusAborted   Status = "ABORTED"
)

// Record is the canonical in-memory transaction record used by the transaction layer.
type Record struct {
	ID          storage.TxnID
	Status      Status
	ReadTS      hlc.Timestamp
	WriteTS     hlc.Timestamp
	MinCommitTS hlc.Timestamp
	Epoch       uint32
	Priority    uint32
	DeadlineTS  hlc.Timestamp
}

// Validate checks that the transaction record is internally consistent.
func (r Record) Validate() error {
	if isZeroTxnID(r.ID) {
		return fmt.Errorf("txn: id must be non-zero")
	}
	switch r.Status {
	case StatusPending, StatusStaging, StatusCommitted, StatusAborted:
	default:
		return fmt.Errorf("txn: unknown status %q", r.Status)
	}
	if r.ReadTS.IsZero() {
		return fmt.Errorf("txn: read timestamp must be non-zero")
	}
	if r.WriteTS.IsZero() {
		return fmt.Errorf("txn: write timestamp must be non-zero")
	}
	if r.WriteTS.Compare(r.ReadTS) < 0 {
		return fmt.Errorf("txn: write timestamp must not move behind read timestamp")
	}
	if !r.MinCommitTS.IsZero() && r.MinCommitTS.Compare(r.WriteTS) > 0 && r.Status != StatusPending && r.Status != StatusStaging {
		return fmt.Errorf("txn: terminal write timestamp must not trail min commit timestamp")
	}
	return nil
}

// IsTerminal reports whether the record has reached an immutable outcome.
func (r Record) IsTerminal() bool {
	return r.Status == StatusCommitted || r.Status == StatusAborted
}

// CanRetry reports whether the transaction may restart at the given timestamp.
func (r Record) CanRetry(now hlc.Timestamp) bool {
	if r.IsTerminal() {
		return false
	}
	if !r.DeadlineTS.IsZero() && now.Compare(r.DeadlineTS) >= 0 {
		return false
	}
	return true
}

// Restart increments the epoch and advances timestamps according to the retry contract.
func (r Record) Restart(nextReadTS hlc.Timestamp) (Record, error) {
	if err := r.Validate(); err != nil {
		return Record{}, err
	}
	if r.IsTerminal() {
		return Record{}, fmt.Errorf("txn: terminal transaction may not restart")
	}
	readTS := maxTimestamp(r.ReadTS, nextReadTS)
	writeTS := maxTimestamp(r.WriteTS, readTS)
	if !r.MinCommitTS.IsZero() {
		writeTS = maxTimestamp(writeTS, r.MinCommitTS)
	}
	r.Status = StatusPending
	r.Epoch++
	r.ReadTS = readTS
	r.WriteTS = writeTS
	return r, r.Validate()
}

// OlderThan reports whether r should win wound-wait age comparisons against other.
func (r Record) OlderThan(other Record) bool {
	if cmp := r.ReadTS.Compare(other.ReadTS); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(r.ID[:], other.ID[:]) < 0
}

func maxTimestamp(a, b hlc.Timestamp) hlc.Timestamp {
	if a.Compare(b) >= 0 {
		return a
	}
	return b
}

func isZeroTxnID(id storage.TxnID) bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return true
}
