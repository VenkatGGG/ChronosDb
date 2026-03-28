package txn

import (
	"bytes"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

// IntentRef identifies one required intent in a multi-range transaction.
type IntentRef struct {
	RangeID  uint64
	Key      []byte
	Strength storage.IntentStrength
}

// ObservedIntent is one intent observation used by recovery.
type ObservedIntent struct {
	Ref       IntentRef
	Intent     storage.Intent
	Present    bool
	Contested  bool
}

// IntentSet is the canonical set of required intents for parallel commit.
type IntentSet struct {
	refs []IntentRef
}

// ResolveActionKind identifies how one terminal transaction should clean up an intent.
type ResolveActionKind string

const (
	ResolveActionCommit ResolveActionKind = "commit_intent"
	ResolveActionAbort  ResolveActionKind = "abort_intent"
)

// ResolveAction is one async intent-resolution step.
type ResolveAction struct {
	Ref  IntentRef
	Kind ResolveActionKind
}

// RecoveryDecision is the deterministic result of staging recovery.
type RecoveryDecision struct {
	Outcome   Status
	Missing   []IntentRef
	Contended []IntentRef
	Resolve   []ResolveAction
}

// Add registers one required intent.
func (s *IntentSet) Add(ref IntentRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	for i, existing := range s.refs {
		if !sameIntentIdentity(existing, ref) {
			continue
		}
		if existing.Strength != ref.Strength {
			return fmt.Errorf("intent set: duplicate intent %q on range %d changed strength", ref.Key, ref.RangeID)
		}
		s.refs[i] = cloneIntentRef(ref)
		return nil
	}
	s.refs = append(s.refs, cloneIntentRef(ref))
	return nil
}

// Len returns the number of required intents.
func (s IntentSet) Len() int {
	return len(s.refs)
}

// Refs returns a copy of the required-intent set.
func (s IntentSet) Refs() []IntentRef {
	if len(s.refs) == 0 {
		return nil
	}
	out := make([]IntentRef, len(s.refs))
	for i, ref := range s.refs {
		out[i] = cloneIntentRef(ref)
	}
	return out
}

// Validate checks the intent reference for obvious corruption.
func (r IntentRef) Validate() error {
	if r.RangeID == 0 {
		return fmt.Errorf("intent ref: range id must be non-zero")
	}
	if len(r.Key) == 0 {
		return fmt.Errorf("intent ref: key must be non-empty")
	}
	if r.Strength == storage.IntentStrengthUnknown {
		return fmt.Errorf("intent ref: strength must be set")
	}
	return nil
}

// StageForParallelCommit advances a live transaction into STAGING after required intents exist.
func (r Record) StageForParallelCommit(required IntentSet) (Record, error) {
	if err := r.Validate(); err != nil {
		return Record{}, err
	}
	if r.Status != StatusPending {
		return Record{}, fmt.Errorf("txn: only PENDING transactions may enter STAGING")
	}
	if r.AnchorRangeID == 0 {
		return Record{}, fmt.Errorf("txn: transaction must be anchored before parallel commit")
	}
	if required.Len() == 0 {
		return Record{}, fmt.Errorf("txn: required intents must be present before STAGING")
	}
	for _, ref := range required.Refs() {
		var err error
		r, err = r.TouchRange(ref.RangeID)
		if err != nil {
			return Record{}, err
		}
	}
	r.Status = StatusStaging
	return r, r.Validate()
}

// RecoverStaging derives the durable outcome from the TxnRecord plus required intents.
func RecoverStaging(record Record, required IntentSet, observed []ObservedIntent) (Record, RecoveryDecision, error) {
	if err := record.Validate(); err != nil {
		return Record{}, RecoveryDecision{}, err
	}
	if record.Status != StatusStaging {
		return Record{}, RecoveryDecision{}, fmt.Errorf("txn: only STAGING records may be recovered")
	}
	if required.Len() == 0 {
		return Record{}, RecoveryDecision{}, fmt.Errorf("txn: STAGING recovery requires required intents")
	}

	observedByKey := make(map[string]ObservedIntent, len(observed))
	for _, intent := range observed {
		if err := intent.Ref.Validate(); err != nil {
			return Record{}, RecoveryDecision{}, err
		}
		if intent.Present {
			if err := intent.Intent.Validate(); err != nil {
				return Record{}, RecoveryDecision{}, err
			}
		}
		observedByKey[intentIdentity(intent.Ref)] = cloneObservedIntent(intent)
	}

	decision := RecoveryDecision{}
	for _, ref := range required.Refs() {
		observedIntent, ok := observedByKey[intentIdentity(ref)]
		switch {
		case !ok || !observedIntent.Present:
			decision.Missing = append(decision.Missing, ref)
		case observedIntent.Contested:
			decision.Contended = append(decision.Contended, ref)
		case observedIntent.Intent.TxnID != record.ID:
			decision.Contended = append(decision.Contended, ref)
		case observedIntent.Intent.Epoch != record.Epoch:
			decision.Contended = append(decision.Contended, ref)
		case observedIntent.Intent.Strength != ref.Strength:
			decision.Contended = append(decision.Contended, ref)
		}
	}

	next := record
	if len(decision.Missing) == 0 && len(decision.Contended) == 0 {
		next.Status = StatusCommitted
	} else {
		next.Status = StatusAborted
	}
	resolve, err := BuildAsyncResolution(next, required)
	if err != nil {
		return Record{}, RecoveryDecision{}, err
	}
	decision.Outcome = next.Status
	decision.Resolve = resolve
	return next, decision, next.Validate()
}

// RecoverAfterCoordinatorFailure derives the post-failure outcome from the durable record and intents.
func RecoverAfterCoordinatorFailure(record Record, required IntentSet, observed []ObservedIntent) (Record, RecoveryDecision, error) {
	switch record.Status {
	case StatusStaging:
		return RecoverStaging(record, required, observed)
	case StatusCommitted, StatusAborted:
		resolve, err := BuildAsyncResolution(record, required)
		if err != nil {
			return Record{}, RecoveryDecision{}, err
		}
		return record, RecoveryDecision{
			Outcome: record.Status,
			Resolve: resolve,
		}, nil
	default:
		return Record{}, RecoveryDecision{}, fmt.Errorf("txn: coordinator recovery requires STAGING or terminal record")
	}
}

// BuildAsyncResolution turns a terminal transaction outcome into explicit intent cleanup work.
func BuildAsyncResolution(record Record, required IntentSet) ([]ResolveAction, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	if !record.IsTerminal() {
		return nil, fmt.Errorf("txn: async intent resolution requires terminal status")
	}
	kind := ResolveActionAbort
	if record.Status == StatusCommitted {
		kind = ResolveActionCommit
	}
	refs := required.Refs()
	actions := make([]ResolveAction, len(refs))
	for i, ref := range refs {
		actions[i] = ResolveAction{Ref: ref, Kind: kind}
	}
	return actions, nil
}

func sameIntentIdentity(left, right IntentRef) bool {
	return left.RangeID == right.RangeID && bytes.Equal(left.Key, right.Key)
}

func intentIdentity(ref IntentRef) string {
	return fmt.Sprintf("%d:%s", ref.RangeID, ref.Key)
}

func cloneIntentRef(ref IntentRef) IntentRef {
	return IntentRef{
		RangeID:  ref.RangeID,
		Key:      bytes.Clone(ref.Key),
		Strength: ref.Strength,
	}
}

func cloneObservedIntent(intent ObservedIntent) ObservedIntent {
	intent.Ref = cloneIntentRef(intent.Ref)
	intent.Intent.Value = append([]byte(nil), intent.Intent.Value...)
	return intent
}
