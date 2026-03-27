package hlc

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommitWaitBlocksUntilTarget(t *testing.T) {
	t.Parallel()

	clock := NewManualClock(Timestamp{WallTime: 100, Logical: 1})
	target := Timestamp{WallTime: 100, Logical: 3}

	done := make(chan error, 1)
	go func() {
		done <- CommitWait(context.Background(), clock, target)
	}()

	select {
	case err := <-done:
		t.Fatalf("commit wait returned too early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	clock.Tick()
	select {
	case err := <-done:
		t.Fatalf("commit wait returned after one tick: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	clock.Tick()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("commit wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("commit wait did not complete after target time")
	}
}

func TestCommitWaitFailsClosedOnOffsetViolation(t *testing.T) {
	t.Parallel()

	clock := NewManualClock(Timestamp{WallTime: 100, Logical: 1})
	done := make(chan error, 1)
	go func() {
		done <- CommitWait(context.Background(), clock, Timestamp{WallTime: 200, Logical: 0})
	}()

	clock.SetOffsetExceeded(true)
	select {
	case err := <-done:
		if !errors.Is(err, ErrClockOffsetExceeded) {
			t.Fatalf("commit wait error = %v, want %v", err, ErrClockOffsetExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("commit wait did not fail on offset violation")
	}
}
