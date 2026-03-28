package sim

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/txn"
)

// TxnParticipantObservation is one deterministic participant snapshot used during coordinator recovery.
type TxnParticipantObservation struct {
	RangeID  uint64
	Observed []txn.ObservedIntent
}

// RecoverTxnAfterCoordinatorFailure deterministically aggregates participant observations and runs recovery.
func RecoverTxnAfterCoordinatorFailure(
	record txn.Record,
	required txn.IntentSet,
	participants []TxnParticipantObservation,
) (txn.Record, txn.RecoveryDecision, error) {
	aggregated, err := aggregateParticipantObservations(participants)
	if err != nil {
		return txn.Record{}, txn.RecoveryDecision{}, err
	}
	return txn.RecoverAfterCoordinatorFailure(record, required, aggregated)
}

func aggregateParticipantObservations(participants []TxnParticipantObservation) ([]txn.ObservedIntent, error) {
	if len(participants) == 0 {
		return nil, fmt.Errorf("sim: participant observations must not be empty")
	}
	ordered := append([]TxnParticipantObservation(nil), participants...)
	slices.SortFunc(ordered, func(left, right TxnParticipantObservation) int {
		switch {
		case left.RangeID < right.RangeID:
			return -1
		case left.RangeID > right.RangeID:
			return 1
		default:
			return 0
		}
	})

	var aggregated []txn.ObservedIntent
	seen := make(map[string]struct{})
	for _, participant := range ordered {
		if participant.RangeID == 0 {
			return nil, fmt.Errorf("sim: participant range id must be non-zero")
		}
		observed := cloneObservedIntents(participant.Observed)
		slices.SortFunc(observed, func(left, right txn.ObservedIntent) int {
			if left.Ref.RangeID != right.Ref.RangeID {
				if left.Ref.RangeID < right.Ref.RangeID {
					return -1
				}
				return 1
			}
			return bytes.Compare(left.Ref.Key, right.Ref.Key)
		})
		for _, intent := range observed {
			if intent.Ref.RangeID != participant.RangeID {
				return nil, fmt.Errorf(
					"sim: participant range %d reported observation for range %d",
					participant.RangeID,
					intent.Ref.RangeID,
				)
			}
			key := observationKey(intent.Ref)
			if _, ok := seen[key]; ok {
				return nil, fmt.Errorf("sim: duplicate participant observation for range %d key %q", intent.Ref.RangeID, intent.Ref.Key)
			}
			seen[key] = struct{}{}
			aggregated = append(aggregated, intent)
		}
	}
	return aggregated, nil
}

func observationKey(ref txn.IntentRef) string {
	return fmt.Sprintf("%d:%s", ref.RangeID, ref.Key)
}

func cloneObservedIntents(observed []txn.ObservedIntent) []txn.ObservedIntent {
	if len(observed) == 0 {
		return nil
	}
	out := make([]txn.ObservedIntent, len(observed))
	for i, intent := range observed {
		out[i] = txn.ObservedIntent{
			Ref: txn.IntentRef{
				RangeID:  intent.Ref.RangeID,
				Key:      bytes.Clone(intent.Ref.Key),
				Strength: intent.Ref.Strength,
			},
			Intent: storage.Intent{
				TxnID:          intent.Intent.TxnID,
				Epoch:          intent.Intent.Epoch,
				WriteTimestamp: intent.Intent.WriteTimestamp,
				Strength:       intent.Intent.Strength,
				Value:          append([]byte(nil), intent.Intent.Value...),
			},
			Present:   intent.Present,
			Contested: intent.Contested,
		}
	}
	return out
}
