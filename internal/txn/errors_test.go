package txn

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestContentionErrorFromDecision(t *testing.T) {
	t.Parallel()

	record := testRecord(1, 10)
	blocker := testRecord(2, 5)
	err, mapErr := ContentionErrorFromDecision(record, LockDecision{
		Kind: LockDecisionWait,
		WaitFor: []LockHolder{
			{Txn: blocker, Strength: storage.IntentStrengthExclusive},
		},
	}, 25)
	if mapErr != nil {
		t.Fatalf("map contention error: %v", mapErr)
	}
	if err.Code != CodeContentionBackoff {
		t.Fatalf("code = %q, want %q", err.Code, CodeContentionBackoff)
	}
	if !err.Retryable() {
		t.Fatalf("contention error should be retryable")
	}
	if err.RetryDelayMillis != 25 {
		t.Fatalf("retry delay = %d, want 25", err.RetryDelayMillis)
	}
}

func TestRetryErrorsCarryRestartTimestamp(t *testing.T) {
	t.Parallel()

	record := testRecord(1, 10)
	restartTS := hlc.Timestamp{WallTime: 20, Logical: 1}
	err := NewSerializationRetryError(record, restartTS, "")
	if err.Code != CodeTxnRetrySerialization {
		t.Fatalf("code = %q, want %q", err.Code, CodeTxnRetrySerialization)
	}
	if !err.Retryable() {
		t.Fatalf("serialization retry should be retryable")
	}
	if err.RestartTimestamp.Compare(restartTS) != 0 {
		t.Fatalf("restart ts = %s, want %s", err.RestartTimestamp, restartTS)
	}
}

func TestAbortedErrorIsNotRetryable(t *testing.T) {
	t.Parallel()

	record := testRecord(1, 10)
	err := NewAbortedError(record, "")
	if err.Code != CodeTxnAborted {
		t.Fatalf("code = %q, want %q", err.Code, CodeTxnAborted)
	}
	if err.Retryable() {
		t.Fatalf("aborted error must not be retryable")
	}
	if err.Class != ErrorClassClientSemantic {
		t.Fatalf("class = %q, want %q", err.Class, ErrorClassClientSemantic)
	}
}
