package storage

import (
	"bytes"
	"fmt"
)

var mvccValueMagic = []byte{0x00, 0xff, 'C', 'D', 'B', 'M', 'V', 'C', 'C', 0x01}

const (
	mvccValueKindLive      byte = 0x01
	mvccValueKindTombstone byte = 0x02
)

// MVCCValue is one committed MVCC payload stored under a versioned MVCC key.
// Newer payloads carry an explicit value kind so tombstones can be represented
// without overloading empty byte slices. Older payloads are treated as live
// values for backward compatibility.
type MVCCValue struct {
	Tombstone bool
	Value     []byte
}

// MarshalBinary encodes the MVCC value into a stable on-disk representation.
func (v MVCCValue) MarshalBinary() ([]byte, error) {
	if v.Tombstone && len(v.Value) > 0 {
		return nil, fmt.Errorf("mvcc value: tombstone must not carry a payload")
	}
	buf := make([]byte, 0, len(mvccValueMagic)+1+len(v.Value))
	buf = append(buf, mvccValueMagic...)
	if v.Tombstone {
		buf = append(buf, mvccValueKindTombstone)
		return buf, nil
	}
	buf = append(buf, mvccValueKindLive)
	buf = append(buf, v.Value...)
	return buf, nil
}

// UnmarshalMVCCValue decodes one committed MVCC payload. Legacy unwrapped
// payloads are interpreted as live values.
func UnmarshalMVCCValue(data []byte) (MVCCValue, error) {
	if !bytes.HasPrefix(data, mvccValueMagic) {
		return MVCCValue{Value: bytes.Clone(data)}, nil
	}
	payload := data[len(mvccValueMagic):]
	if len(payload) == 0 {
		return MVCCValue{}, fmt.Errorf("mvcc value: missing value kind")
	}
	switch payload[0] {
	case mvccValueKindLive:
		return MVCCValue{Value: bytes.Clone(payload[1:])}, nil
	case mvccValueKindTombstone:
		if len(payload) != 1 {
			return MVCCValue{}, fmt.Errorf("mvcc value: tombstone must not carry a payload")
		}
		return MVCCValue{Tombstone: true}, nil
	default:
		return MVCCValue{}, fmt.Errorf("mvcc value: unknown value kind %d", payload[0])
	}
}

