package txn

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

// LockDecisionKind identifies the outcome of one lock-table acquisition attempt.
type LockDecisionKind string

const (
	LockDecisionAcquired LockDecisionKind = "acquired"
	LockDecisionWait     LockDecisionKind = "wait"
	LockDecisionWound    LockDecisionKind = "wound"
)

// LockHolder is one current holder or waiter in the lock table.
type LockHolder struct {
	Txn      Record
	Strength storage.IntentStrength
}

// LockDecision describes the canonical result of one acquisition attempt.
type LockDecision struct {
	Kind    LockDecisionKind
	WaitFor []LockHolder
	Victims []LockHolder
}

type lockState struct {
	holders []LockHolder
	waiters []LockHolder
}

// LockStateSnapshot is the observable state of one lock-table entry.
type LockStateSnapshot struct {
	Holders []LockHolder
	Waiters []LockHolder
}

// LockTable centralizes contention decisions and wound-wait behavior.
type LockTable struct {
	mu    sync.Mutex
	locks map[string]*lockState
}

// NewLockTable constructs an empty lock table.
func NewLockTable() *LockTable {
	return &LockTable{locks: make(map[string]*lockState)}
}

// Acquire tries to acquire one lock and returns the wound-wait decision.
func (t *LockTable) Acquire(key []byte, txn Record, strength storage.IntentStrength) (LockDecision, error) {
	if err := txn.Validate(); err != nil {
		return LockDecision{}, err
	}
	if strength == storage.IntentStrengthUnknown {
		return LockDecision{}, fmt.Errorf("lock table: lock strength must be set")
	}
	if txn.Status != StatusPending && txn.Status != StatusStaging {
		return LockDecision{}, fmt.Errorf("lock table: txn status %q may not acquire locks", txn.Status)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.stateForKey(key)
	state.holders = pruneStaleEpochs(state.holders, txn)
	state.waiters = pruneStaleEpochs(state.waiters, txn)

	blockers := conflictingHolders(state.holders, txn, strength)
	if len(blockers) == 0 {
		state.holders = addOrUpdateHolder(state.holders, LockHolder{Txn: txn, Strength: strength})
		state.waiters = removeTxn(state.waiters, txn.ID)
		return LockDecision{Kind: LockDecisionAcquired}, nil
	}
	if olderThanAll(txn, blockers) {
		state.holders = removeVictims(state.holders, blockers)
		state.waiters = removeVictims(state.waiters, blockers)
		state.holders = addOrUpdateHolder(state.holders, LockHolder{Txn: txn, Strength: strength})
		return LockDecision{Kind: LockDecisionWound, Victims: cloneHolders(blockers)}, nil
	}

	state.waiters = addOrUpdateWaiter(state.waiters, LockHolder{Txn: txn, Strength: strength})
	return LockDecision{Kind: LockDecisionWait, WaitFor: cloneHolders(blockers)}, nil
}

// Release removes one holder and grants newly unblocked waiters in age order.
func (t *LockTable) Release(key []byte, txnID storage.TxnID) []LockHolder {
	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.locks[string(key)]
	if state == nil {
		return nil
	}
	state.holders = removeTxn(state.holders, txnID)

	var granted []LockHolder
	for {
		waiter, ok := pickGrantableWaiter(state.holders, state.waiters)
		if !ok {
			break
		}
		state.waiters = removeTxn(state.waiters, waiter.Txn.ID)
		state.holders = addOrUpdateHolder(state.holders, waiter)
		granted = append(granted, waiter)
	}
	if len(state.holders) == 0 && len(state.waiters) == 0 {
		delete(t.locks, string(key))
	}
	return granted
}

// Snapshot returns the observable state for one key.
func (t *LockTable) Snapshot(key []byte) LockStateSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.locks[string(key)]
	if state == nil {
		return LockStateSnapshot{}
	}
	return LockStateSnapshot{
		Holders: cloneHolders(state.holders),
		Waiters: cloneHolders(state.waiters),
	}
}

func (t *LockTable) stateForKey(key []byte) *lockState {
	k := string(key)
	state := t.locks[k]
	if state == nil {
		state = &lockState{}
		t.locks[k] = state
	}
	return state
}

func conflictingHolders(holders []LockHolder, claimant Record, strength storage.IntentStrength) []LockHolder {
	var blockers []LockHolder
	for _, holder := range holders {
		if bytes.Equal(holder.Txn.ID[:], claimant.ID[:]) {
			continue
		}
		if compatible(holder.Strength, strength) {
			continue
		}
		blockers = append(blockers, holder)
	}
	return blockers
}

func compatible(left, right storage.IntentStrength) bool {
	return left == storage.IntentStrengthShared && right == storage.IntentStrengthShared
}

func olderThanAll(claimant Record, blockers []LockHolder) bool {
	for _, blocker := range blockers {
		if !claimant.OlderThan(blocker.Txn) {
			return false
		}
	}
	return true
}

func addOrUpdateHolder(holders []LockHolder, next LockHolder) []LockHolder {
	for i, holder := range holders {
		if !bytes.Equal(holder.Txn.ID[:], next.Txn.ID[:]) {
			continue
		}
		holders[i] = next
		return holders
	}
	return append(holders, next)
}

func addOrUpdateWaiter(waiters []LockHolder, next LockHolder) []LockHolder {
	for i, waiter := range waiters {
		if !bytes.Equal(waiter.Txn.ID[:], next.Txn.ID[:]) {
			continue
		}
		waiters[i] = next
		return sortByAge(waiters)
	}
	waiters = append(waiters, next)
	return sortByAge(waiters)
}

func pruneStaleEpochs(entries []LockHolder, claimant Record) []LockHolder {
	out := entries[:0]
	for _, entry := range entries {
		if bytes.Equal(entry.Txn.ID[:], claimant.ID[:]) && entry.Txn.Epoch < claimant.Epoch {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func removeVictims(entries []LockHolder, victims []LockHolder) []LockHolder {
	out := entries[:0]
	for _, entry := range entries {
		if containsTxn(victims, entry.Txn.ID) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func removeTxn(entries []LockHolder, txnID storage.TxnID) []LockHolder {
	out := entries[:0]
	for _, entry := range entries {
		if bytes.Equal(entry.Txn.ID[:], txnID[:]) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func containsTxn(entries []LockHolder, txnID storage.TxnID) bool {
	for _, entry := range entries {
		if bytes.Equal(entry.Txn.ID[:], txnID[:]) {
			return true
		}
	}
	return false
}

func sortByAge(entries []LockHolder) []LockHolder {
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[j].Txn.OlderThan(entries[i].Txn) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
	return entries
}

func pickGrantableWaiter(holders, waiters []LockHolder) (LockHolder, bool) {
	for _, waiter := range waiters {
		if len(conflictingHolders(holders, waiter.Txn, waiter.Strength)) == 0 {
			return waiter, true
		}
	}
	return LockHolder{}, false
}

func cloneHolders(entries []LockHolder) []LockHolder {
	if len(entries) == 0 {
		return nil
	}
	out := make([]LockHolder, len(entries))
	copy(out, entries)
	return out
}
