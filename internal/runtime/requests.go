package runtime

import "github.com/VenkatGGG/ChronosDb/internal/meta"

// RangeStatusRequest identifies one hosted range whose local state should be reported.
type RangeStatusRequest struct {
	RangeID uint64 `json:"range_id"`
}

// ReplicaInstallRequest seeds one local replica from a snapshot image and optional descriptor hint.
type ReplicaInstallRequest struct {
	RangeID        uint64                `json:"range_id"`
	ReplicaID      uint64                `json:"replica_id"`
	DescriptorHint *meta.RangeDescriptor `json:"descriptor_hint,omitempty"`
	Snapshot       ReplicaSnapshot       `json:"snapshot"`
}
