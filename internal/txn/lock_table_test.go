package txn

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestLockTableAcquireUncontended(t *testing.T) {
	t.Parallel()

	table := NewLockTable()
	decision, err := table.Acquire([]byte("accounts/alice"), testRecord(1, 10), storage.IntentStrengthExclusive)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if decision.Kind != LockDecisionAcquired {
		t.Fatalf("decision = %q, want %q", decision.Kind, LockDecisionAcquired)
	}
	snapshot := table.Snapshot([]byte("accounts/alice"))
	if len(snapshot.Holders) != 1 {
		t.Fatalf("holder count = %d, want 1", len(snapshot.Holders))
	}
}

func TestLockTableYoungerTxnWaits(t *testing.T) {
	t.Parallel()

	table := NewLockTable()
	older := testRecord(1, 10)
	younger := testRecord(2, 20)
	if _, err := table.Acquire([]byte("accounts/alice"), older, storage.IntentStrengthExclusive); err != nil {
		t.Fatalf("older acquire: %v", err)
	}
	decision, err := table.Acquire([]byte("accounts/alice"), younger, storage.IntentStrengthExclusive)
	if err != nil {
		t.Fatalf("younger acquire: %v", err)
	}
	if decision.Kind != LockDecisionWait {
		t.Fatalf("decision = %q, want %q", decision.Kind, LockDecisionWait)
	}
	if len(decision.WaitFor) != 1 || decision.WaitFor[0].Txn.ID != older.ID {
		t.Fatalf("wait-for = %+v, want older holder", decision.WaitFor)
	}
	snapshot := table.Snapshot([]byte("accounts/alice"))
	if len(snapshot.Waiters) != 1 || snapshot.Waiters[0].Txn.ID != younger.ID {
		t.Fatalf("waiters = %+v, want younger waiter", snapshot.Waiters)
	}
}

func TestLockTableOlderTxnWoundsYounger(t *testing.T) {
	t.Parallel()

	table := NewLockTable()
	younger := testRecord(2, 20)
	older := testRecord(1, 10)
	if _, err := table.Acquire([]byte("accounts/alice"), younger, storage.IntentStrengthExclusive); err != nil {
		t.Fatalf("younger acquire: %v", err)
	}
	decision, err := table.Acquire([]byte("accounts/alice"), older, storage.IntentStrengthExclusive)
	if err != nil {
		t.Fatalf("older acquire: %v", err)
	}
	if decision.Kind != LockDecisionWound {
		t.Fatalf("decision = %q, want %q", decision.Kind, LockDecisionWound)
	}
	if len(decision.Victims) != 1 || decision.Victims[0].Txn.ID != younger.ID {
		t.Fatalf("victims = %+v, want younger victim", decision.Victims)
	}
	snapshot := table.Snapshot([]byte("accounts/alice"))
	if len(snapshot.Holders) != 1 || snapshot.Holders[0].Txn.ID != older.ID {
		t.Fatalf("holders = %+v, want older holder", snapshot.Holders)
	}
}

func TestLockTableEpochBumpReplacesStaleSelf(t *testing.T) {
	t.Parallel()

	table := NewLockTable()
	original := testRecord(1, 10)
	if _, err := table.Acquire([]byte("accounts/alice"), original, storage.IntentStrengthExclusive); err != nil {
		t.Fatalf("original acquire: %v", err)
	}
	restarted, err := original.Restart(hlc.Timestamp{WallTime: 11, Logical: 0})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	decision, err := table.Acquire([]byte("accounts/alice"), restarted, storage.IntentStrengthExclusive)
	if err != nil {
		t.Fatalf("restarted acquire: %v", err)
	}
	if decision.Kind != LockDecisionAcquired {
		t.Fatalf("decision = %q, want %q", decision.Kind, LockDecisionAcquired)
	}
	snapshot := table.Snapshot([]byte("accounts/alice"))
	if len(snapshot.Holders) != 1 || snapshot.Holders[0].Txn.Epoch != 1 {
		t.Fatalf("holders = %+v, want restarted epoch holder", snapshot.Holders)
	}
}

func TestLockTableReleaseGrantsWaitingTxn(t *testing.T) {
	t.Parallel()

	table := NewLockTable()
	older := testRecord(1, 10)
	younger := testRecord(2, 20)
	if _, err := table.Acquire([]byte("accounts/alice"), older, storage.IntentStrengthExclusive); err != nil {
		t.Fatalf("older acquire: %v", err)
	}
	if _, err := table.Acquire([]byte("accounts/alice"), younger, storage.IntentStrengthExclusive); err != nil {
		t.Fatalf("younger acquire: %v", err)
	}
	granted := table.Release([]byte("accounts/alice"), older.ID)
	if len(granted) != 1 || granted[0].Txn.ID != younger.ID {
		t.Fatalf("granted = %+v, want younger waiter", granted)
	}
	snapshot := table.Snapshot([]byte("accounts/alice"))
	if len(snapshot.Holders) != 1 || snapshot.Holders[0].Txn.ID != younger.ID {
		t.Fatalf("holders = %+v, want younger holder", snapshot.Holders)
	}
}

func testRecord(seed byte, wall uint64) Record {
	return Record{
		ID:       txnID(seed),
		Status:   StatusPending,
		ReadTS:   hlc.Timestamp{WallTime: wall, Logical: 0},
		WriteTS:  hlc.Timestamp{WallTime: wall, Logical: 1},
		Priority: 1,
	}
}

func txnID(seed byte) storage.TxnID {
	var id storage.TxnID
	id[len(id)-1] = seed
	return id
}
