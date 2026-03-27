package storage

import (
	"encoding/binary"
	"fmt"
)

// StoreIdent is the local identity record for a bootstrapped store.
type StoreIdent struct {
	ClusterID string
	NodeID    uint64
	StoreID   uint64
}

// Validate checks that the identity has the fields needed to safely bootstrap.
func (s StoreIdent) Validate() error {
	switch {
	case s.ClusterID == "":
		return fmt.Errorf("store ident: cluster id must be set")
	case s.NodeID == 0:
		return fmt.Errorf("store ident: node id must be non-zero")
	case s.StoreID == 0:
		return fmt.Errorf("store ident: store id must be non-zero")
	default:
		return nil
	}
}

func (s StoreIdent) marshalBinary() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(s.ClusterID)+binary.MaxVarintLen64*2)
	buf = append(buf, 1)
	buf = binary.AppendUvarint(buf, uint64(len(s.ClusterID)))
	buf = append(buf, s.ClusterID...)
	buf = binary.AppendUvarint(buf, s.NodeID)
	buf = binary.AppendUvarint(buf, s.StoreID)
	return buf, nil
}

func unmarshalStoreIdent(data []byte) (StoreIdent, error) {
	if len(data) == 0 {
		return StoreIdent{}, fmt.Errorf("decode store ident: empty payload")
	}
	if data[0] != 1 {
		return StoreIdent{}, fmt.Errorf("decode store ident: unknown version %d", data[0])
	}
	data = data[1:]

	clusterLen, n := binary.Uvarint(data)
	if n <= 0 {
		return StoreIdent{}, fmt.Errorf("decode store ident: invalid cluster id length")
	}
	data = data[n:]
	if uint64(len(data)) < clusterLen {
		return StoreIdent{}, fmt.Errorf("decode store ident: truncated cluster id")
	}
	ident := StoreIdent{
		ClusterID: string(data[:clusterLen]),
	}
	data = data[clusterLen:]

	nodeID, n := binary.Uvarint(data)
	if n <= 0 {
		return StoreIdent{}, fmt.Errorf("decode store ident: invalid node id")
	}
	data = data[n:]
	storeID, n := binary.Uvarint(data)
	if n <= 0 {
		return StoreIdent{}, fmt.Errorf("decode store ident: invalid store id")
	}
	ident.NodeID = nodeID
	ident.StoreID = storeID
	return ident, ident.Validate()
}
