package closedts

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
)

var (
	// ErrFollowerReadTooFresh reports that a follower read is above the closed frontier.
	ErrFollowerReadTooFresh = errors.New("follower read too fresh")
	// ErrClosedTimestampNotApplied reports that the follower has not yet applied the publication.
	ErrClosedTimestampNotApplied = errors.New("closed timestamp not fully applied")
	// ErrClosedTimestampLeaseMismatch reports that the publication belongs to a stale lease sequence.
	ErrClosedTimestampLeaseMismatch = errors.New("closed timestamp lease sequence mismatch")
)

// Record is one closed-timestamp publication for a specific range and lease epoch.
type Record struct {
	RangeID       uint64
	LeaseSequence uint64
	ClosedTS      hlc.Timestamp
	PublishedAt   hlc.Timestamp
}

// PublishInput is the local leaseholder state needed to publish a closed timestamp.
type PublishInput struct {
	RangeID                  uint64
	Lease                    lease.Record
	LastApplied              hlc.Timestamp
	Now                      hlc.Timestamp
	MaxOffset                uint64
	ClockOffsetExceeded      bool
	OldestUnresolvedIntentTS *hlc.Timestamp
}

// FollowerReadRequest is the state a follower must prove before serving a historical read.
type FollowerReadRequest struct {
	ReadTS             hlc.Timestamp
	AppliedThrough     hlc.Timestamp
	KnownLeaseSequence uint64
}

// Publisher computes monotonic closed timestamp publications.
type Publisher struct{}

// NewPublisher constructs a closed timestamp publisher.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// Validate checks that the record is minimally well-formed.
func (r Record) Validate() error {
	switch {
	case r.RangeID == 0:
		return fmt.Errorf("closedts: range id must be non-zero")
	case r.LeaseSequence == 0:
		return fmt.Errorf("closedts: lease sequence must be non-zero")
	case r.ClosedTS.IsZero():
		return fmt.Errorf("closedts: closed timestamp must be non-zero")
	case r.PublishedAt.IsZero():
		return fmt.Errorf("closedts: published-at timestamp must be non-zero")
	case r.PublishedAt.Compare(r.ClosedTS) < 0:
		return fmt.Errorf("closedts: published-at must not be behind closed timestamp")
	default:
		return nil
	}
}

// MarshalBinary encodes the closed timestamp record in a stable binary form.
func (r Record) MarshalBinary() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1+8+8+hlc.EncodedSize+hlc.EncodedSize)
	buf = append(buf, 1)
	buf = appendUint64(buf, r.RangeID)
	buf = appendUint64(buf, r.LeaseSequence)
	buf = r.ClosedTS.AppendAscending(buf)
	buf = r.PublishedAt.AppendAscending(buf)
	return buf, nil
}

// UnmarshalBinary decodes the binary form produced by MarshalBinary.
func (r *Record) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("closedts: empty payload")
	}
	if data[0] != 1 {
		return fmt.Errorf("closedts: unknown version %d", data[0])
	}
	data = data[1:]
	if len(data) < 8+8+hlc.EncodedSize+hlc.EncodedSize {
		return fmt.Errorf("closedts: truncated payload")
	}
	r.RangeID = binary.BigEndian.Uint64(data[:8])
	data = data[8:]
	r.LeaseSequence = binary.BigEndian.Uint64(data[:8])
	data = data[8:]
	closedTS, rest, err := hlc.DecodeAscending(data)
	if err != nil {
		return err
	}
	r.ClosedTS = closedTS
	publishedAt, rest, err := hlc.DecodeAscending(rest)
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return fmt.Errorf("closedts: trailing payload")
	}
	r.PublishedAt = publishedAt
	return r.Validate()
}

// Publish computes the next closed timestamp publication for the leaseholder.
func (p *Publisher) Publish(previous *Record, input PublishInput) (Record, bool, error) {
	if err := input.Lease.Validate(); err != nil {
		return Record{}, false, err
	}
	switch {
	case input.RangeID == 0:
		return Record{}, false, fmt.Errorf("closedts: range id must be non-zero")
	case input.LastApplied.IsZero():
		return Record{}, false, fmt.Errorf("closedts: last applied timestamp must be non-zero")
	case input.Now.IsZero():
		return Record{}, false, fmt.Errorf("closedts: now timestamp must be non-zero")
	case input.ClockOffsetExceeded:
		if previous != nil {
			return *previous, false, nil
		}
		return Record{}, false, nil
	}

	candidate := minTimestamp(input.LastApplied, subtractWallTime(input.Now, input.MaxOffset))
	if input.OldestUnresolvedIntentTS != nil && !input.OldestUnresolvedIntentTS.IsZero() {
		candidate = minTimestamp(candidate, previousTick(*input.OldestUnresolvedIntentTS))
	}
	if candidate.IsZero() {
		if previous != nil {
			return *previous, false, nil
		}
		return Record{}, false, nil
	}

	if previous != nil && previous.RangeID == input.RangeID && previous.LeaseSequence == input.Lease.Sequence {
		if candidate.Compare(previous.ClosedTS) <= 0 {
			return *previous, false, nil
		}
	}

	record := Record{
		RangeID:       input.RangeID,
		LeaseSequence: input.Lease.Sequence,
		ClosedTS:      candidate,
		PublishedAt:   input.Now,
	}
	if err := record.Validate(); err != nil {
		return Record{}, false, err
	}
	return record, true, nil
}

// CanServeFollowerRead validates whether a follower may serve a historical read.
func (r Record) CanServeFollowerRead(req FollowerReadRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}
	switch {
	case req.KnownLeaseSequence != r.LeaseSequence:
		return ErrClosedTimestampLeaseMismatch
	case req.AppliedThrough.Compare(r.ClosedTS) < 0:
		return ErrClosedTimestampNotApplied
	case req.ReadTS.Compare(r.ClosedTS) > 0:
		return ErrFollowerReadTooFresh
	default:
		return nil
	}
}

func minTimestamp(a, b hlc.Timestamp) hlc.Timestamp {
	if a.Compare(b) <= 0 {
		return a
	}
	return b
}

func subtractWallTime(ts hlc.Timestamp, delta uint64) hlc.Timestamp {
	if delta == 0 {
		return ts
	}
	if ts.WallTime <= delta {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{
		WallTime: ts.WallTime - delta,
		Logical:  ts.Logical,
	}
}

func previousTick(ts hlc.Timestamp) hlc.Timestamp {
	switch {
	case ts.IsZero():
		return hlc.Timestamp{}
	case ts.Logical > 0:
		return hlc.Timestamp{WallTime: ts.WallTime, Logical: ts.Logical - 1}
	case ts.WallTime > 0:
		return hlc.Timestamp{WallTime: ts.WallTime - 1, Logical: math.MaxUint32}
	default:
		return hlc.Timestamp{}
	}
}

func appendUint64(dst []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return append(dst, buf[:]...)
}
