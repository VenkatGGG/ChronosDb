package routing

import (
	"errors"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

// ReadRouteMode is the selected read path for one replica-local request.
type ReadRouteMode string

const (
	ReadRouteLeaseholder        ReadRouteMode = "leaseholder"
	ReadRouteFollowerHistorical ReadRouteMode = "follower_historical"
)

// ReadDecision is the routing result for one read request.
type ReadDecision struct {
	Mode            ReadRouteMode
	TargetReplicaID uint64
	Reason          string
}

// HistoricalReadRequest is the state needed to choose between follower and leaseholder read paths.
type HistoricalReadRequest struct {
	Descriptor         meta.RangeDescriptor
	LocalReplicaID     uint64
	ReadTS             hlc.Timestamp
	AppliedThrough     hlc.Timestamp
	KnownLeaseSequence uint64
	ClosedTimestamp    *closedts.Record
}

// DecideHistoricalRead chooses the local historical follower path when safe, else routes to the leaseholder.
func DecideHistoricalRead(req HistoricalReadRequest) (ReadDecision, error) {
	if err := req.Descriptor.Validate(); err != nil {
		return ReadDecision{}, err
	}
	if req.LocalReplicaID == 0 {
		return ReadDecision{}, fmt.Errorf("routing: local replica id must be non-zero")
	}
	if req.ReadTS.IsZero() {
		return ReadDecision{}, fmt.Errorf("routing: read timestamp must be non-zero")
	}
	if req.LocalReplicaID == req.Descriptor.LeaseholderReplicaID {
		return ReadDecision{
			Mode:            ReadRouteLeaseholder,
			TargetReplicaID: req.Descriptor.LeaseholderReplicaID,
			Reason:          "local replica is current leaseholder",
		}, nil
	}
	if !descriptorHasReplica(req.Descriptor, req.LocalReplicaID) {
		return ReadDecision{}, fmt.Errorf("routing: local replica %d is not in descriptor", req.LocalReplicaID)
	}
	if req.ClosedTimestamp != nil {
		err := req.ClosedTimestamp.CanServeFollowerRead(closedts.FollowerReadRequest{
			ReadTS:             req.ReadTS,
			AppliedThrough:     req.AppliedThrough,
			KnownLeaseSequence: req.KnownLeaseSequence,
		})
		if err == nil {
			return ReadDecision{
				Mode:            ReadRouteFollowerHistorical,
				TargetReplicaID: req.LocalReplicaID,
				Reason:          "closed timestamp permits local historical read",
			}, nil
		}
		if !errors.Is(err, closedts.ErrFollowerReadTooFresh) &&
			!errors.Is(err, closedts.ErrClosedTimestampNotApplied) &&
			!errors.Is(err, closedts.ErrClosedTimestampLeaseMismatch) {
			return ReadDecision{}, err
		}
	}
	return ReadDecision{
		Mode:            ReadRouteLeaseholder,
		TargetReplicaID: req.Descriptor.LeaseholderReplicaID,
		Reason:          "historical follower read is not yet safe",
	}, nil
}

func descriptorHasReplica(desc meta.RangeDescriptor, replicaID uint64) bool {
	for _, replica := range desc.Replicas {
		if replica.ReplicaID == replicaID {
			return true
		}
	}
	return false
}
