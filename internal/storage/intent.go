package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

// IntentStrength describes the locking strength of an intent.
type IntentStrength uint8

const (
	IntentStrengthUnknown IntentStrength = iota
	IntentStrengthShared
	IntentStrengthExclusive
)

// TxnID is the fixed-width transaction identifier stored in intents.
type TxnID [16]byte

// Intent is the on-disk provisional write record stored at the metadata key.
type Intent struct {
	TxnID          TxnID
	Epoch          uint32
	WriteTimestamp hlc.Timestamp
	Strength       IntentStrength
	Value          []byte
}

// Validate checks that the intent is minimally well-formed.
func (i Intent) Validate() error {
	if i.WriteTimestamp.IsZero() {
		return fmt.Errorf("intent: write timestamp must be non-zero")
	}
	if i.Strength == IntentStrengthUnknown {
		return fmt.Errorf("intent: strength must be set")
	}
	return nil
}

// MarshalBinary encodes the intent into a stable binary form.
func (i Intent) MarshalBinary() ([]byte, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 1+len(i.TxnID)+4+1+hlc.EncodedSize+binary.MaxVarintLen64+len(i.Value))
	buf = append(buf, 1)
	buf = append(buf, i.TxnID[:]...)
	var fixed [4]byte
	binary.BigEndian.PutUint32(fixed[:], i.Epoch)
	buf = append(buf, fixed[:]...)
	buf = append(buf, byte(i.Strength))
	buf = i.WriteTimestamp.AppendAscending(buf)
	buf = binary.AppendUvarint(buf, uint64(len(i.Value)))
	buf = append(buf, i.Value...)
	return buf, nil
}

// UnmarshalBinary decodes the intent from the binary form written by MarshalBinary.
func (i *Intent) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("decode intent: empty payload")
	}
	if data[0] != 1 {
		return fmt.Errorf("decode intent: unknown version %d", data[0])
	}
	data = data[1:]
	if len(data) < len(i.TxnID)+4+1+hlc.EncodedSize {
		return fmt.Errorf("decode intent: truncated payload")
	}

	copy(i.TxnID[:], data[:len(i.TxnID)])
	data = data[len(i.TxnID):]
	i.Epoch = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	i.Strength = IntentStrength(data[0])
	data = data[1:]

	ts, rest, err := hlc.DecodeAscending(data)
	if err != nil {
		return err
	}
	i.WriteTimestamp = ts
	data = rest

	valueLen, n := binary.Uvarint(data)
	if n <= 0 {
		return fmt.Errorf("decode intent: invalid value length")
	}
	data = data[n:]
	if uint64(len(data)) < valueLen {
		return fmt.Errorf("decode intent: truncated value")
	}
	i.Value = append(i.Value[:0], data[:valueLen]...)
	return i.Validate()
}
