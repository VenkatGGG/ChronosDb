package txn

import (
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

// ErrOnePhaseCommitIneligible reports that the request must fall back to a non-1PC path.
var ErrOnePhaseCommitIneligible = fmt.Errorf("one-phase commit ineligible")

// PendingWrite is one write candidate considered by the 1PC path.
type PendingWrite struct {
	Key      []byte
	RangeID  uint64
	Strength storage.IntentStrength
	Value    []byte
}

// OnePhaseCommitRequest is the canonical input to the single-range fast path.
type OnePhaseCommitRequest struct {
	Txn            Record
	Writes         []PendingWrite
	RefreshSpans   *RefreshSet
	ObservedWrites []ObservedWrite
}

// OnePhaseCommitResult is the committed outcome of a successful 1PC path.
type OnePhaseCommitResult struct {
	Txn      Record
	CommitTS hlc.Timestamp
	RangeID  uint64
	Writes   []PendingWrite
}

// OnePhaseCommit attempts the single-range commit fast path and falls back cleanly on ineligible inputs.
func OnePhaseCommit(req OnePhaseCommitRequest) (OnePhaseCommitResult, error) {
	if err := req.Txn.Validate(); err != nil {
		return OnePhaseCommitResult{}, err
	}
	if req.Txn.Status != StatusPending {
		return OnePhaseCommitResult{}, fmt.Errorf("%w: transaction status must be PENDING", ErrOnePhaseCommitIneligible)
	}
	if len(req.Writes) == 0 {
		return OnePhaseCommitResult{}, fmt.Errorf("%w: at least one write is required", ErrOnePhaseCommitIneligible)
	}

	rangeID := req.Writes[0].RangeID
	if rangeID == 0 {
		return OnePhaseCommitResult{}, fmt.Errorf("%w: write range id must be non-zero", ErrOnePhaseCommitIneligible)
	}
	writes := make([]PendingWrite, len(req.Writes))
	for i, write := range req.Writes {
		if write.RangeID != rangeID {
			return OnePhaseCommitResult{}, fmt.Errorf("%w: writes span multiple ranges", ErrOnePhaseCommitIneligible)
		}
		if len(write.Key) == 0 {
			return OnePhaseCommitResult{}, fmt.Errorf("%w: write key must be non-empty", ErrOnePhaseCommitIneligible)
		}
		if write.Strength == storage.IntentStrengthUnknown {
			return OnePhaseCommitResult{}, fmt.Errorf("%w: write strength must be set", ErrOnePhaseCommitIneligible)
		}
		writes[i] = PendingWrite{
			Key:      append([]byte(nil), write.Key...),
			RangeID:  write.RangeID,
			Strength: write.Strength,
			Value:    append([]byte(nil), write.Value...),
		}
	}

	commitTS := req.Txn.WriteTS
	if !req.Txn.MinCommitTS.IsZero() && req.Txn.MinCommitTS.Compare(commitTS) > 0 {
		commitTS = req.Txn.MinCommitTS
	}
	if req.RefreshSpans != nil {
		if err := req.RefreshSpans.ValidateRefresh(req.Txn.ReadTS, commitTS, req.ObservedWrites); err != nil {
			var conflict *RefreshConflictError
			if errors.As(err, &conflict) {
				return OnePhaseCommitResult{}, NewSerializationRetryError(req.Txn, conflict.CommitTS.Next(), "refresh span conflict during one-phase commit")
			}
			return OnePhaseCommitResult{}, err
		}
	}

	committed := req.Txn
	committed.Status = StatusCommitted
	committed.WriteTS = commitTS
	if err := committed.Validate(); err != nil {
		return OnePhaseCommitResult{}, err
	}
	return OnePhaseCommitResult{
		Txn:      committed,
		CommitTS: commitTS,
		RangeID:  rangeID,
		Writes:   writes,
	}, nil
}
