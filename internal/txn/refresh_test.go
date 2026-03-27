package txn

import (
	"errors"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

func TestRefreshSetTracksUniqueSpans(t *testing.T) {
	t.Parallel()

	var set RefreshSet
	if err := set.Add(Span{StartKey: []byte("a"), EndKey: []byte("m")}); err != nil {
		t.Fatalf("add span: %v", err)
	}
	if err := set.Add(Span{StartKey: []byte("a"), EndKey: []byte("m")}); err != nil {
		t.Fatalf("add duplicate span: %v", err)
	}
	if got := len(set.Spans()); got != 1 {
		t.Fatalf("span count = %d, want 1", got)
	}
}

func TestRefreshSetAcceptsNonConflictingWrites(t *testing.T) {
	t.Parallel()

	var set RefreshSet
	if err := set.Add(Span{StartKey: []byte("a"), EndKey: []byte("m")}); err != nil {
		t.Fatalf("add span: %v", err)
	}
	err := set.ValidateRefresh(
		hlc.Timestamp{WallTime: 10, Logical: 0},
		hlc.Timestamp{WallTime: 20, Logical: 0},
		[]ObservedWrite{
			{Span: Span{StartKey: []byte("z")}, CommitTS: hlc.Timestamp{WallTime: 15, Logical: 0}},
			{Span: Span{StartKey: []byte("b"), EndKey: []byte("c")}, CommitTS: hlc.Timestamp{WallTime: 9, Logical: 0}},
		},
	)
	if err != nil {
		t.Fatalf("validate refresh: %v", err)
	}
}

func TestRefreshSetRejectsConflictingWrite(t *testing.T) {
	t.Parallel()

	var set RefreshSet
	if err := set.Add(Span{StartKey: []byte("a"), EndKey: []byte("m")}); err != nil {
		t.Fatalf("add span: %v", err)
	}
	err := set.ValidateRefresh(
		hlc.Timestamp{WallTime: 10, Logical: 0},
		hlc.Timestamp{WallTime: 20, Logical: 0},
		[]ObservedWrite{
			{Span: Span{StartKey: []byte("b"), EndKey: []byte("c")}, CommitTS: hlc.Timestamp{WallTime: 15, Logical: 0}},
		},
	)
	if !errors.Is(err, ErrRefreshConflict) {
		t.Fatalf("refresh error = %v, want %v", err, ErrRefreshConflict)
	}
}

func TestRefreshSetPointSpanConflicts(t *testing.T) {
	t.Parallel()

	var set RefreshSet
	if err := set.Add(Span{StartKey: []byte("k")}); err != nil {
		t.Fatalf("add point span: %v", err)
	}
	err := set.ValidateRefresh(
		hlc.Timestamp{WallTime: 10, Logical: 0},
		hlc.Timestamp{WallTime: 20, Logical: 0},
		[]ObservedWrite{
			{Span: Span{StartKey: []byte("j"), EndKey: []byte("z")}, CommitTS: hlc.Timestamp{WallTime: 11, Logical: 0}},
		},
	)
	if !errors.Is(err, ErrRefreshConflict) {
		t.Fatalf("point refresh error = %v, want %v", err, ErrRefreshConflict)
	}
}
