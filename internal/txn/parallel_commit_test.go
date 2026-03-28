package txn

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestStageForParallelCommitTouchesParticipantRanges(t *testing.T) {
	t.Parallel()

	record := testRecord(1, 10)
	record, err := record.Anchor(7, hlc.Timestamp{WallTime: 11, Logical: 0})
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	required := mustIntentSet(t,
		IntentRef{RangeID: 7, Key: []byte("accounts/alice"), Strength: storage.IntentStrengthExclusive},
		IntentRef{RangeID: 9, Key: []byte("accounts/bob"), Strength: storage.IntentStrengthExclusive},
	)

	staged, err := record.StageForParallelCommit(required)
	if err != nil {
		t.Fatalf("stage for parallel commit: %v", err)
	}
	if staged.Status != StatusStaging {
		t.Fatalf("status = %q, want %q", staged.Status, StatusStaging)
	}
	if len(staged.TouchedRanges) != 2 {
		t.Fatalf("touched ranges = %+v, want two participant ranges", staged.TouchedRanges)
	}
}

func TestRecoverStagingCommitsWhenRequiredIntentsPresent(t *testing.T) {
	t.Parallel()

	record := stagedRecord(t, 1)
	required := mustIntentSet(t,
		IntentRef{RangeID: 7, Key: []byte("accounts/alice"), Strength: storage.IntentStrengthExclusive},
		IntentRef{RangeID: 9, Key: []byte("accounts/bob"), Strength: storage.IntentStrengthExclusive},
	)

	recovered, decision, err := RecoverStaging(record, required, []ObservedIntent{
		{
			Ref: required.Refs()[0],
			Intent: storage.Intent{
				TxnID:          record.ID,
				Epoch:          record.Epoch,
				WriteTimestamp: record.WriteTS,
				Strength:       storage.IntentStrengthExclusive,
				Value:          []byte("50"),
			},
			Present: true,
		},
		{
			Ref: required.Refs()[1],
			Intent: storage.Intent{
				TxnID:          record.ID,
				Epoch:          record.Epoch,
				WriteTimestamp: record.WriteTS,
				Strength:       storage.IntentStrengthExclusive,
				Value:          []byte("75"),
			},
			Present: true,
		},
	})
	if err != nil {
		t.Fatalf("recover staging: %v", err)
	}
	if recovered.Status != StatusCommitted {
		t.Fatalf("status = %q, want %q", recovered.Status, StatusCommitted)
	}
	if decision.Outcome != StatusCommitted {
		t.Fatalf("decision outcome = %q, want %q", decision.Outcome, StatusCommitted)
	}
	if len(decision.Resolve) != 2 || decision.Resolve[0].Kind != ResolveActionCommit {
		t.Fatalf("resolve actions = %+v, want commit actions", decision.Resolve)
	}
}

func TestRecoverStagingAbortsOnMissingIntent(t *testing.T) {
	t.Parallel()

	record := stagedRecord(t, 2)
	required := mustIntentSet(t,
		IntentRef{RangeID: 7, Key: []byte("accounts/alice"), Strength: storage.IntentStrengthExclusive},
		IntentRef{RangeID: 9, Key: []byte("accounts/bob"), Strength: storage.IntentStrengthExclusive},
	)

	recovered, decision, err := RecoverStaging(record, required, []ObservedIntent{
		{
			Ref: required.Refs()[0],
			Intent: storage.Intent{
				TxnID:          record.ID,
				Epoch:          record.Epoch,
				WriteTimestamp: record.WriteTS,
				Strength:       storage.IntentStrengthExclusive,
				Value:          []byte("50"),
			},
			Present: true,
		},
	})
	if err != nil {
		t.Fatalf("recover staging: %v", err)
	}
	if recovered.Status != StatusAborted {
		t.Fatalf("status = %q, want %q", recovered.Status, StatusAborted)
	}
	if len(decision.Missing) != 1 {
		t.Fatalf("missing intents = %+v, want one missing intent", decision.Missing)
	}
	if len(decision.Resolve) != 2 || decision.Resolve[0].Kind != ResolveActionAbort {
		t.Fatalf("resolve actions = %+v, want abort actions", decision.Resolve)
	}
}

func TestRecoverAfterCoordinatorFailureUsesDurableState(t *testing.T) {
	t.Parallel()

	record := stagedRecord(t, 3)
	required := mustIntentSet(t,
		IntentRef{RangeID: 7, Key: []byte("accounts/alice"), Strength: storage.IntentStrengthExclusive},
	)
	recovered, decision, err := RecoverAfterCoordinatorFailure(record, required, []ObservedIntent{
		{
			Ref: required.Refs()[0],
			Intent: storage.Intent{
				TxnID:          record.ID,
				Epoch:          record.Epoch,
				WriteTimestamp: record.WriteTS,
				Strength:       storage.IntentStrengthExclusive,
				Value:          []byte("50"),
			},
			Present: true,
		},
	})
	if err != nil {
		t.Fatalf("recover after coordinator failure: %v", err)
	}
	if recovered.Status != StatusCommitted || decision.Outcome != StatusCommitted {
		t.Fatalf("recovered = %+v decision = %+v, want committed outcome", recovered, decision)
	}
}

func TestBuildAsyncResolutionForTerminalOutcome(t *testing.T) {
	t.Parallel()

	record := testRecord(4, 10)
	record.Status = StatusCommitted
	required := mustIntentSet(t,
		IntentRef{RangeID: 7, Key: []byte("accounts/alice"), Strength: storage.IntentStrengthExclusive},
	)
	resolve, err := BuildAsyncResolution(record, required)
	if err != nil {
		t.Fatalf("build async resolution: %v", err)
	}
	if len(resolve) != 1 || resolve[0].Kind != ResolveActionCommit {
		t.Fatalf("resolve actions = %+v, want one commit action", resolve)
	}
}

func mustIntentSet(t *testing.T, refs ...IntentRef) IntentSet {
	t.Helper()

	var set IntentSet
	for _, ref := range refs {
		if err := set.Add(ref); err != nil {
			t.Fatalf("add intent ref: %v", err)
		}
	}
	return set
}

func stagedRecord(t *testing.T, seed byte) Record {
	t.Helper()

	record := testRecord(seed, 10)
	var err error
	record, err = record.Anchor(7, hlc.Timestamp{WallTime: 11, Logical: 0})
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	required := mustIntentSet(t,
		IntentRef{RangeID: 7, Key: []byte("accounts/alice"), Strength: storage.IntentStrengthExclusive},
		IntentRef{RangeID: 9, Key: []byte("accounts/bob"), Strength: storage.IntentStrengthExclusive},
	)
	record, err = record.StageForParallelCommit(required)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	return record
}
