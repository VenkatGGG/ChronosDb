package lease

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

// ErrReadIndexRequired reports that the caller may not serve a fast local read.
var ErrReadIndexRequired = errors.New("read index required")

// Record is the replicated lease record for one range.
type Record struct {
	HolderReplicaID     uint64
	StartTS             hlc.Timestamp
	ExpirationTS        hlc.Timestamp
	Sequence            uint64
	HolderLivenessEpoch uint64
}

// FastReadRequest contains the local facts required to validate a fast read.
type FastReadRequest struct {
	ReplicaID            uint64
	AppliedLeaseSequence uint64
	CurrentLivenessEpoch uint64
	Now                  hlc.Timestamp
	ReadTimestamp        hlc.Timestamp
	SafeReadFrontier     hlc.Timestamp
	ClockOffsetExceeded  bool
}

// NewRecord constructs a validated lease record.
func NewRecord(holderReplicaID, holderLivenessEpoch uint64, startTS, expirationTS hlc.Timestamp, sequence uint64) (Record, error) {
	record := Record{
		HolderReplicaID:     holderReplicaID,
		StartTS:             startTS,
		ExpirationTS:        expirationTS,
		Sequence:            sequence,
		HolderLivenessEpoch: holderLivenessEpoch,
	}
	return record, record.Validate()
}

// Validate checks that the lease record is minimally well-formed.
func (r Record) Validate() error {
	switch {
	case r.HolderReplicaID == 0:
		return fmt.Errorf("lease: holder replica must be non-zero")
	case r.HolderLivenessEpoch == 0:
		return fmt.Errorf("lease: holder liveness epoch must be non-zero")
	case r.Sequence == 0:
		return fmt.Errorf("lease: sequence must be non-zero")
	case r.StartTS.IsZero():
		return fmt.Errorf("lease: start timestamp must be non-zero")
	case r.ExpirationTS.IsZero():
		return fmt.Errorf("lease: expiration timestamp must be non-zero")
	case r.ExpirationTS.Compare(r.StartTS) <= 0:
		return fmt.Errorf("lease: expiration must be greater than start")
	default:
		return nil
	}
}

// Renew extends the existing holder lease without changing the epoch sequence.
func (r Record) Renew(startTS, expirationTS hlc.Timestamp) (Record, error) {
	if err := r.Validate(); err != nil {
		return Record{}, err
	}
	if startTS.Compare(r.StartTS) < 0 {
		return Record{}, fmt.Errorf("lease renewal: start must not move backward")
	}
	return NewRecord(r.HolderReplicaID, r.HolderLivenessEpoch, startTS, expirationTS, r.Sequence)
}

// Transfer moves lease ownership to a new replica and bumps the sequence barrier.
func (r Record) Transfer(targetReplicaID, targetLivenessEpoch uint64, now, expirationTS hlc.Timestamp) (Record, error) {
	if err := r.Validate(); err != nil {
		return Record{}, err
	}
	startTS := now
	if r.StartTS.Compare(startTS) > 0 {
		startTS = r.StartTS
	}
	return NewRecord(targetReplicaID, targetLivenessEpoch, startTS, expirationTS, r.Sequence+1)
}

// CanServeFastRead validates whether the local replica may serve a leaseholder-local read.
func (r Record) CanServeFastRead(req FastReadRequest) error {
	if err := r.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrReadIndexRequired, err)
	}
	switch {
	case req.ClockOffsetExceeded:
		return ErrReadIndexRequired
	case req.ReplicaID != r.HolderReplicaID:
		return ErrReadIndexRequired
	case req.AppliedLeaseSequence != r.Sequence:
		return ErrReadIndexRequired
	case req.CurrentLivenessEpoch != r.HolderLivenessEpoch:
		return ErrReadIndexRequired
	case req.Now.Compare(r.StartTS) < 0:
		return ErrReadIndexRequired
	case req.Now.Compare(r.ExpirationTS) >= 0:
		return ErrReadIndexRequired
	case req.ReadTimestamp.Compare(req.SafeReadFrontier) > 0:
		return ErrReadIndexRequired
	default:
		return nil
	}
}

// MarshalBinary encodes the lease record in a stable binary form.
func (r Record) MarshalBinary() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1+8+hlc.EncodedSize+hlc.EncodedSize+8+8)
	buf = append(buf, 1)
	buf = appendUint64(buf, r.HolderReplicaID)
	buf = r.StartTS.AppendAscending(buf)
	buf = r.ExpirationTS.AppendAscending(buf)
	buf = appendUint64(buf, r.Sequence)
	buf = appendUint64(buf, r.HolderLivenessEpoch)
	return buf, nil
}

// UnmarshalBinary decodes a lease record written by MarshalBinary.
func (r *Record) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("decode lease: empty payload")
	}
	if data[0] != 1 {
		return fmt.Errorf("decode lease: unknown version %d", data[0])
	}
	data = data[1:]
	if len(data) < 8+hlc.EncodedSize+hlc.EncodedSize+8+8 {
		return fmt.Errorf("decode lease: truncated payload")
	}
	r.HolderReplicaID = binary.BigEndian.Uint64(data[:8])
	data = data[8:]
	startTS, rest, err := hlc.DecodeAscending(data)
	if err != nil {
		return err
	}
	r.StartTS = startTS
	data = rest
	expirationTS, rest, err := hlc.DecodeAscending(data)
	if err != nil {
		return err
	}
	r.ExpirationTS = expirationTS
	data = rest
	r.Sequence = binary.BigEndian.Uint64(data[:8])
	data = data[8:]
	r.HolderLivenessEpoch = binary.BigEndian.Uint64(data[:8])
	return r.Validate()
}

func appendUint64(dst []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return append(dst, buf[:]...)
}
