package sim

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/txn"
)

func TestRecoverTxnAfterCoordinatorFailureCommitsAcrossParticipants(t *testing.T) {
	t.Parallel()

	record, required := stagedRecoveryRecord(t, 1)
	recovered, decision, err := RecoverTxnAfterCoordinatorFailure(record, required, []TxnParticipantObservation{
		{
			RangeID: 7,
			Observed: []txn.ObservedIntent{
				presentObservedIntent(record, required.Refs()[0], []byte("50")),
			},
		},
		{
			RangeID: 9,
			Observed: []txn.ObservedIntent{
				presentObservedIntent(record, required.Refs()[1], []byte("75")),
			},
		},
	})
	if err != nil {
		t.Fatalf("recover txn after coordinator failure: %v", err)
	}
	if recovered.Status != txn.StatusCommitted || decision.Outcome != txn.StatusCommitted {
		t.Fatalf("recovered = %+v decision = %+v, want committed outcome", recovered, decision)
	}
	if len(decision.Resolve) != 2 || decision.Resolve[0].Kind != txn.ResolveActionCommit {
		t.Fatalf("resolve actions = %+v, want commit resolution", decision.Resolve)
	}
}

func TestRecoverTxnAfterCoordinatorFailureAbortsOnContestedParticipant(t *testing.T) {
	t.Parallel()

	record, required := stagedRecoveryRecord(t, 2)
	recovered, decision, err := RecoverTxnAfterCoordinatorFailure(record, required, []TxnParticipantObservation{
		{
			RangeID: 7,
			Observed: []txn.ObservedIntent{
				presentObservedIntent(record, required.Refs()[0], []byte("50")),
			},
		},
		{
			RangeID: 9,
			Observed: []txn.ObservedIntent{
				{
					Ref:       required.Refs()[1],
					Present:   true,
					Contested: true,
					Intent: storage.Intent{
						TxnID:          record.ID,
						Epoch:          record.Epoch,
						WriteTimestamp: record.WriteTS,
						Strength:       storage.IntentStrengthExclusive,
						Value:          []byte("75"),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("recover txn after coordinator failure: %v", err)
	}
	if recovered.Status != txn.StatusAborted || decision.Outcome != txn.StatusAborted {
		t.Fatalf("recovered = %+v decision = %+v, want aborted outcome", recovered, decision)
	}
	if len(decision.Contended) != 1 || decision.Contended[0].RangeID != 9 {
		t.Fatalf("contended = %+v, want one contended range 9 intent", decision.Contended)
	}
	if len(decision.Resolve) != 2 || decision.Resolve[0].Kind != txn.ResolveActionAbort {
		t.Fatalf("resolve actions = %+v, want abort resolution", decision.Resolve)
	}
}

func TestRecoverTxnAfterCoordinatorFailureRejectsCrossRangeObservation(t *testing.T) {
	t.Parallel()

	record, required := stagedRecoveryRecord(t, 3)
	_, _, err := RecoverTxnAfterCoordinatorFailure(record, required, []TxnParticipantObservation{
		{
			RangeID: 7,
			Observed: []txn.ObservedIntent{
				presentObservedIntent(record, required.Refs()[1], []byte("75")),
			},
		},
	})
	if err == nil {
		t.Fatal("expected cross-range participant observation to fail")
	}
}

func stagedRecoveryRecord(t *testing.T, seed byte) (txn.Record, txn.IntentSet) {
	t.Helper()

	record := txn.Record{
		ID:       txnID(seed),
		Status:   txn.StatusPending,
		ReadTS:   hlc.Timestamp{WallTime: 10, Logical: 0},
		WriteTS:  hlc.Timestamp{WallTime: 10, Logical: 0},
		Priority: 1,
	}
	var err error
	record, err = record.Anchor(7, hlc.Timestamp{WallTime: 11, Logical: 0})
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	var required txn.IntentSet
	for _, ref := range []txn.IntentRef{
		{RangeID: 7, Key: []byte("accounts/alice"), Strength: storage.IntentStrengthExclusive},
		{RangeID: 9, Key: []byte("accounts/bob"), Strength: storage.IntentStrengthExclusive},
	} {
		if err := required.Add(ref); err != nil {
			t.Fatalf("add intent ref: %v", err)
		}
	}
	record, err = record.StageForParallelCommit(required)
	if err != nil {
		t.Fatalf("stage for parallel commit: %v", err)
	}
	return record, required
}

func presentObservedIntent(record txn.Record, ref txn.IntentRef, value []byte) txn.ObservedIntent {
	return txn.ObservedIntent{
		Ref: ref,
		Intent: storage.Intent{
			TxnID:          record.ID,
			Epoch:          record.Epoch,
			WriteTimestamp: record.WriteTS,
			Strength:       ref.Strength,
			Value:          append([]byte(nil), value...),
		},
		Present: true,
	}
}

func txnID(seed byte) storage.TxnID {
	var id storage.TxnID
	id[0] = seed
	return id
}
