package txn

import (
	"errors"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestOnePhaseCommitSucceedsSingleRange(t *testing.T) {
	t.Parallel()

	result, err := OnePhaseCommit(OnePhaseCommitRequest{
		Txn: testRecord(1, 10),
		Writes: []PendingWrite{
			{Key: []byte("accounts/alice"), RangeID: 7, Strength: storage.IntentStrengthExclusive, Value: []byte("50")},
			{Key: []byte("accounts/bob"), RangeID: 7, Strength: storage.IntentStrengthExclusive, Value: []byte("75")},
		},
	})
	if err != nil {
		t.Fatalf("one-phase commit: %v", err)
	}
	if result.Txn.Status != StatusCommitted {
		t.Fatalf("txn status = %q, want %q", result.Txn.Status, StatusCommitted)
	}
	if result.RangeID != 7 {
		t.Fatalf("range id = %d, want 7", result.RangeID)
	}
}

func TestOnePhaseCommitUsesRefreshSpans(t *testing.T) {
	t.Parallel()

	var refreshes RefreshSet
	if err := refreshes.Add(Span{StartKey: []byte("accounts/a"), EndKey: []byte("accounts/z")}); err != nil {
		t.Fatalf("add refresh span: %v", err)
	}
	record := testRecord(1, 10)
	record.MinCommitTS = hlc.Timestamp{WallTime: 12, Logical: 0}

	result, err := OnePhaseCommit(OnePhaseCommitRequest{
		Txn:          record,
		RefreshSpans: &refreshes,
		ObservedWrites: []ObservedWrite{
			{Span: Span{StartKey: []byte("inventory/a")}, CommitTS: hlc.Timestamp{WallTime: 11, Logical: 0}},
		},
		Writes: []PendingWrite{
			{Key: []byte("accounts/alice"), RangeID: 7, Strength: storage.IntentStrengthExclusive, Value: []byte("50")},
		},
	})
	if err != nil {
		t.Fatalf("one-phase commit with refresh: %v", err)
	}
	if result.CommitTS.Compare(hlc.Timestamp{WallTime: 12, Logical: 0}) != 0 {
		t.Fatalf("commit ts = %s, want 12.0", result.CommitTS)
	}
}

func TestOnePhaseCommitRejectsMultiRange(t *testing.T) {
	t.Parallel()

	_, err := OnePhaseCommit(OnePhaseCommitRequest{
		Txn: testRecord(1, 10),
		Writes: []PendingWrite{
			{Key: []byte("accounts/alice"), RangeID: 7, Strength: storage.IntentStrengthExclusive},
			{Key: []byte("accounts/bob"), RangeID: 8, Strength: storage.IntentStrengthExclusive},
		},
	})
	if !errors.Is(err, ErrOnePhaseCommitIneligible) {
		t.Fatalf("one-phase commit error = %v, want %v", err, ErrOnePhaseCommitIneligible)
	}
}

func TestOnePhaseCommitReturnsRetryErrorOnRefreshConflict(t *testing.T) {
	t.Parallel()

	var refreshes RefreshSet
	if err := refreshes.Add(Span{StartKey: []byte("accounts/a"), EndKey: []byte("accounts/z")}); err != nil {
		t.Fatalf("add refresh span: %v", err)
	}
	record := testRecord(1, 10)
	record.MinCommitTS = hlc.Timestamp{WallTime: 12, Logical: 0}

	_, err := OnePhaseCommit(OnePhaseCommitRequest{
		Txn:          record,
		RefreshSpans: &refreshes,
		ObservedWrites: []ObservedWrite{
			{Span: Span{StartKey: []byte("accounts/b"), EndKey: []byte("accounts/c")}, CommitTS: hlc.Timestamp{WallTime: 11, Logical: 0}},
		},
		Writes: []PendingWrite{
			{Key: []byte("accounts/alice"), RangeID: 7, Strength: storage.IntentStrengthExclusive},
		},
	})
	var txnErr *Error
	if !errors.As(err, &txnErr) {
		t.Fatalf("one-phase commit error = %v, want txn error", err)
	}
	if txnErr.Code != CodeTxnRetrySerialization {
		t.Fatalf("txn error code = %q, want %q", txnErr.Code, CodeTxnRetrySerialization)
	}
	if !txnErr.Retryable() {
		t.Fatalf("refresh conflict must be retryable")
	}
	if txnErr.RestartTimestamp.Compare(hlc.Timestamp{WallTime: 11, Logical: 1}) != 0 {
		t.Fatalf("restart ts = %s, want 11.1", txnErr.RestartTimestamp)
	}
}
