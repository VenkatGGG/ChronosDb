package hlc

import (
	"encoding/binary"
	"fmt"
)

const (
	// EncodedSize is the fixed-width binary size of a Timestamp.
	EncodedSize = 12
)

// Timestamp is a hybrid logical clock timestamp.
type Timestamp struct {
	WallTime uint64
	Logical  uint32
}

// IsZero reports whether the timestamp is the zero value.
func (t Timestamp) IsZero() bool {
	return t.WallTime == 0 && t.Logical == 0
}

// Compare returns -1 if t < other, 0 if t == other, and 1 if t > other.
func (t Timestamp) Compare(other Timestamp) int {
	switch {
	case t.WallTime < other.WallTime:
		return -1
	case t.WallTime > other.WallTime:
		return 1
	case t.Logical < other.Logical:
		return -1
	case t.Logical > other.Logical:
		return 1
	default:
		return 0
	}
}

// String renders the timestamp in a stable human-readable form.
func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%d", t.WallTime, t.Logical)
}

// Next returns the next logical tick at the same wall time.
func (t Timestamp) Next() Timestamp {
	return Timestamp{
		WallTime: t.WallTime,
		Logical:  t.Logical + 1,
	}
}

// AppendAscending appends the timestamp using fixed-width ascending encoding.
func (t Timestamp) AppendAscending(dst []byte) []byte {
	var buf [EncodedSize]byte
	binary.BigEndian.PutUint64(buf[:8], t.WallTime)
	binary.BigEndian.PutUint32(buf[8:], t.Logical)
	return append(dst, buf[:]...)
}

// AppendDescending appends the timestamp in descending sort order.
func (t Timestamp) AppendDescending(dst []byte) []byte {
	var buf [EncodedSize]byte
	binary.BigEndian.PutUint64(buf[:8], t.WallTime)
	binary.BigEndian.PutUint32(buf[8:], t.Logical)
	for i := range buf {
		buf[i] = ^buf[i]
	}
	return append(dst, buf[:]...)
}

// DecodeAscending decodes a fixed-width ascending timestamp.
func DecodeAscending(src []byte) (Timestamp, []byte, error) {
	if len(src) < EncodedSize {
		return Timestamp{}, nil, fmt.Errorf("decode ascending timestamp: need %d bytes, got %d", EncodedSize, len(src))
	}
	ts := Timestamp{
		WallTime: binary.BigEndian.Uint64(src[:8]),
		Logical:  binary.BigEndian.Uint32(src[8:EncodedSize]),
	}
	return ts, src[EncodedSize:], nil
}

// DecodeDescending decodes a fixed-width descending timestamp.
func DecodeDescending(src []byte) (Timestamp, []byte, error) {
	if len(src) < EncodedSize {
		return Timestamp{}, nil, fmt.Errorf("decode descending timestamp: need %d bytes, got %d", EncodedSize, len(src))
	}
	var buf [EncodedSize]byte
	copy(buf[:], src[:EncodedSize])
	for i := range buf {
		buf[i] = ^buf[i]
	}
	ts := Timestamp{
		WallTime: binary.BigEndian.Uint64(buf[:8]),
		Logical:  binary.BigEndian.Uint32(buf[8:]),
	}
	return ts, src[EncodedSize:], nil
}
