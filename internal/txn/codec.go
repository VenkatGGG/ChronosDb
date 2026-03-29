package txn

import (
	"encoding/binary"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

const recordEncodingVersion byte = 2

// MarshalBinary encodes the transaction record into a stable binary form.
func (r Record) MarshalBinary() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 1+16+1+(5*hlc.EncodedSize)+(4*6)+(binary.MaxVarintLen64*(1+len(r.TouchedRanges))))
	buf = append(buf, recordEncodingVersion)
	buf = append(buf, r.ID[:]...)
	buf = append(buf, encodeStatus(r.Status))
	buf = r.ReadTS.AppendAscending(buf)
	buf = r.WriteTS.AppendAscending(buf)
	buf = r.MinCommitTS.AppendAscending(buf)
	buf = appendUint32(buf, r.Epoch)
	buf = appendUint32(buf, r.Priority)
	buf = appendUint64(buf, r.AnchorRangeID)
	buf = appendUint32(buf, uint32(len(r.TouchedRanges)))
	for _, rangeID := range r.TouchedRanges {
		buf = appendUint64(buf, rangeID)
	}
	buf = r.LastHeartbeatTS.AppendAscending(buf)
	buf = r.DeadlineTS.AppendAscending(buf)
	buf = appendUint32(buf, uint32(len(r.RequiredIntents)))
	for _, intent := range r.RequiredIntents {
		buf = appendUint64(buf, intent.RangeID)
		buf = append(buf, byte(intent.Strength))
		buf = appendUint32(buf, uint32(len(intent.Key)))
		buf = append(buf, intent.Key...)
	}
	return buf, nil
}

// UnmarshalBinary decodes the transaction record from MarshalBinary.
func (r *Record) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("txn: empty record payload")
	}
	version := data[0]
	if version != 1 && version != recordEncodingVersion {
		return fmt.Errorf("txn: unsupported record encoding version %d", data[0])
	}
	data = data[1:]
	if len(data) < len(r.ID)+1 {
		return fmt.Errorf("txn: truncated record payload")
	}

	copy(r.ID[:], data[:len(r.ID)])
	data = data[len(r.ID):]
	status, err := decodeStatus(data[0])
	if err != nil {
		return err
	}
	r.Status = status
	data = data[1:]

	if r.ReadTS, data, err = hlc.DecodeAscending(data); err != nil {
		return err
	}
	if r.WriteTS, data, err = hlc.DecodeAscending(data); err != nil {
		return err
	}
	if r.MinCommitTS, data, err = hlc.DecodeAscending(data); err != nil {
		return err
	}
	if r.Epoch, data, err = consumeUint32(data); err != nil {
		return err
	}
	if r.Priority, data, err = consumeUint32(data); err != nil {
		return err
	}
	if r.AnchorRangeID, data, err = consumeUint64(data); err != nil {
		return err
	}

	var touched uint32
	if touched, data, err = consumeUint32(data); err != nil {
		return err
	}
	r.TouchedRanges = make([]uint64, 0, touched)
	for i := uint32(0); i < touched; i++ {
		rangeID, rest, consumeErr := consumeUint64(data)
		if consumeErr != nil {
			return consumeErr
		}
		r.TouchedRanges = append(r.TouchedRanges, rangeID)
		data = rest
	}

	if r.LastHeartbeatTS, data, err = hlc.DecodeAscending(data); err != nil {
		return err
	}
	if r.DeadlineTS, data, err = hlc.DecodeAscending(data); err != nil {
		return err
	}
	r.RequiredIntents = nil
	if version >= 2 {
		var requiredCount uint32
		if requiredCount, data, err = consumeUint32(data); err != nil {
			return err
		}
		r.RequiredIntents = make([]IntentRef, 0, requiredCount)
		for i := uint32(0); i < requiredCount; i++ {
			var rangeID uint64
			if rangeID, data, err = consumeUint64(data); err != nil {
				return err
			}
			if len(data) == 0 {
				return fmt.Errorf("txn: truncated intent strength")
			}
			strength := storage.IntentStrength(data[0])
			data = data[1:]
			keyLen, rest, consumeErr := consumeUint32(data)
			if consumeErr != nil {
				return consumeErr
			}
			data = rest
			if len(data) < int(keyLen) {
				return fmt.Errorf("txn: truncated intent key payload")
			}
			ref := IntentRef{
				RangeID:  rangeID,
				Key:      append([]byte(nil), data[:keyLen]...),
				Strength: strength,
			}
			if err := ref.Validate(); err != nil {
				return err
			}
			r.RequiredIntents = append(r.RequiredIntents, ref)
			data = data[keyLen:]
		}
	}
	if len(data) != 0 {
		return fmt.Errorf("txn: trailing record payload")
	}
	return r.Validate()
}

func encodeStatus(status Status) byte {
	switch status {
	case StatusPending:
		return 1
	case StatusStaging:
		return 2
	case StatusCommitted:
		return 3
	case StatusAborted:
		return 4
	default:
		return 0
	}
}

func decodeStatus(encoded byte) (Status, error) {
	switch encoded {
	case 1:
		return StatusPending, nil
	case 2:
		return StatusStaging, nil
	case 3:
		return StatusCommitted, nil
	case 4:
		return StatusAborted, nil
	default:
		return "", fmt.Errorf("txn: unknown status encoding %d", encoded)
	}
}

func appendUint32(dst []byte, value uint32) []byte {
	var fixed [4]byte
	binary.BigEndian.PutUint32(fixed[:], value)
	return append(dst, fixed[:]...)
}

func appendUint64(dst []byte, value uint64) []byte {
	var fixed [8]byte
	binary.BigEndian.PutUint64(fixed[:], value)
	return append(dst, fixed[:]...)
}

func consumeUint32(data []byte) (uint32, []byte, error) {
	if len(data) < 4 {
		return 0, nil, fmt.Errorf("txn: truncated uint32 field")
	}
	return binary.BigEndian.Uint32(data[:4]), data[4:], nil
}

func consumeUint64(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, fmt.Errorf("txn: truncated uint64 field")
	}
	return binary.BigEndian.Uint64(data[:8]), data[8:], nil
}
