package txn

import (
	"bytes"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

// ErrRefreshConflict reports that a tracked read span observed a conflicting committed write.
var ErrRefreshConflict = fmt.Errorf("refresh span conflict")

// Span is a half-open key span. An empty EndKey denotes a point span.
type Span struct {
	StartKey []byte
	EndKey   []byte
}

// ObservedWrite is one committed write considered during refresh validation.
type ObservedWrite struct {
	Span     Span
	CommitTS hlc.Timestamp
}

// RefreshSet tracks read spans that must refresh before timestamp advancement.
type RefreshSet struct {
	spans []Span
}

// Add registers one tracked read span.
func (r *RefreshSet) Add(span Span) error {
	if err := span.Validate(); err != nil {
		return err
	}
	for _, existing := range r.spans {
		if equalSpan(existing, span) {
			return nil
		}
	}
	r.spans = append(r.spans, cloneSpan(span))
	return nil
}

// Spans returns a copy of the tracked spans.
func (r *RefreshSet) Spans() []Span {
	if len(r.spans) == 0 {
		return nil
	}
	out := make([]Span, len(r.spans))
	for i, span := range r.spans {
		out[i] = cloneSpan(span)
	}
	return out
}

// ValidateRefresh ensures no observed write conflicts with tracked reads between readTS and refreshTS.
func (r *RefreshSet) ValidateRefresh(readTS, refreshTS hlc.Timestamp, observed []ObservedWrite) error {
	if readTS.IsZero() {
		return fmt.Errorf("refresh spans: read timestamp must be non-zero")
	}
	if refreshTS.IsZero() {
		return fmt.Errorf("refresh spans: refresh timestamp must be non-zero")
	}
	if refreshTS.Compare(readTS) <= 0 {
		return nil
	}
	for _, write := range observed {
		if err := write.Span.Validate(); err != nil {
			return err
		}
		if write.CommitTS.IsZero() {
			return fmt.Errorf("refresh spans: observed write timestamp must be non-zero")
		}
		if write.CommitTS.Compare(readTS) <= 0 || write.CommitTS.Compare(refreshTS) > 0 {
			continue
		}
		for _, span := range r.spans {
			if spansOverlap(span, write.Span) {
				return fmt.Errorf("%w: observed write at %s overlaps tracked span", ErrRefreshConflict, write.CommitTS)
			}
		}
	}
	return nil
}

// Validate checks that the span is structurally valid.
func (s Span) Validate() error {
	if len(s.StartKey) == 0 {
		return fmt.Errorf("span: start key must be non-empty")
	}
	if len(s.EndKey) > 0 && bytes.Compare(s.StartKey, s.EndKey) >= 0 {
		return fmt.Errorf("span: start key must sort before end key")
	}
	return nil
}

// ContainsKey reports whether key falls inside the span.
func (s Span) ContainsKey(key []byte) bool {
	if len(s.EndKey) == 0 {
		return bytes.Equal(s.StartKey, key)
	}
	return bytes.Compare(key, s.StartKey) >= 0 && bytes.Compare(key, s.EndKey) < 0
}

func spansOverlap(left, right Span) bool {
	if len(left.EndKey) == 0 {
		return right.ContainsKey(left.StartKey)
	}
	if len(right.EndKey) == 0 {
		return left.ContainsKey(right.StartKey)
	}
	return bytes.Compare(left.StartKey, right.EndKey) < 0 && bytes.Compare(right.StartKey, left.EndKey) < 0
}

func cloneSpan(span Span) Span {
	return Span{
		StartKey: bytes.Clone(span.StartKey),
		EndKey:   bytes.Clone(span.EndKey),
	}
}

func equalSpan(left, right Span) bool {
	return bytes.Equal(left.StartKey, right.StartKey) && bytes.Equal(left.EndKey, right.EndKey)
}
