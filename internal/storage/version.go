package storage

import (
	"encoding/binary"
	"fmt"
)

// StoreVersion is the local on-disk format marker.
type StoreVersion struct {
	Major uint16
	Minor uint16
}

// CurrentStoreVersion is the version written by this binary.
var CurrentStoreVersion = StoreVersion{
	Major: 1,
	Minor: 0,
}

// Compatible reports whether the engine can safely read data at version other.
func (v StoreVersion) Compatible(other StoreVersion) bool {
	return v.Major == other.Major && v.Minor >= other.Minor
}

func (v StoreVersion) marshalBinary() []byte {
	var buf [4]byte
	binary.BigEndian.PutUint16(buf[:2], v.Major)
	binary.BigEndian.PutUint16(buf[2:], v.Minor)
	return buf[:]
}

func unmarshalStoreVersion(data []byte) (StoreVersion, error) {
	if len(data) != 4 {
		return StoreVersion{}, fmt.Errorf("decode store version: want 4 bytes, got %d", len(data))
	}
	return StoreVersion{
		Major: binary.BigEndian.Uint16(data[:2]),
		Minor: binary.BigEndian.Uint16(data[2:]),
	}, nil
}
