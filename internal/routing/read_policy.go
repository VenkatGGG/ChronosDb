package routing

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
)

// ReadRouteMode is the selected read path for one replica-local request.
type ReadRouteMode string

const (
	ReadRouteLeaseholder        ReadRouteMode = "leaseholder"
	ReadRouteFollowerHistorical ReadRouteMode = "follower_historical"
)

// ReplicaLocality is the routing-visible locality for one replica.
type ReplicaLocality struct {
	ReplicaID uint64
	Region    string
	Zone      string
}

// TimestampGap is the routing-visible distance between two timestamps.
type TimestampGap struct {
	WallTime uint64
	Logical  uint32
}

// IsZero reports whether the gap is empty.
func (g TimestampGap) IsZero() bool {
	return g.WallTime == 0 && g.Logical == 0
}

// ReadDecision is the routing result for one read request.
type ReadDecision struct {
	Mode               ReadRouteMode
	TargetReplicaID    uint64
	Reason             string
	LocalReplicaRegion string
	LeaseholderRegion  string
	TargetRegion       string
	PreferredRegion    string
	LocalityPreferred  bool
	ClosedThrough      hlc.Timestamp
	AppliedThrough     hlc.Timestamp
	FreshnessGap       TimestampGap
}

// HistoricalReadRequest is the state needed to choose between follower and leaseholder read paths.
type HistoricalReadRequest struct {
	Descriptor         meta.RangeDescriptor
	LocalReplicaID     uint64
	ReadTS             hlc.Timestamp
	AppliedThrough     hlc.Timestamp
	KnownLeaseSequence uint64
	ClosedTimestamp    *closedts.Record
	ReplicaLocalities  []ReplicaLocality
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
	if !descriptorHasReplica(req.Descriptor, req.LocalReplicaID) {
		return ReadDecision{}, fmt.Errorf("routing: local replica %d is not in descriptor", req.LocalReplicaID)
	}
	localityInfo, err := buildReadLocalityInfo(req)
	if err != nil {
		return ReadDecision{}, err
	}
	decision := ReadDecision{
		LocalReplicaRegion: localityInfo.localReplicaRegion,
		LeaseholderRegion:  localityInfo.leaseholderRegion,
		PreferredRegion:    localityInfo.preferredRegion,
		AppliedThrough:     req.AppliedThrough,
	}
	if req.ClosedTimestamp != nil {
		decision.ClosedThrough = req.ClosedTimestamp.ClosedTS
		err := req.ClosedTimestamp.CanServeFollowerRead(closedts.FollowerReadRequest{
			ReadTS:             req.ReadTS,
			AppliedThrough:     req.AppliedThrough,
			KnownLeaseSequence: req.KnownLeaseSequence,
		})
		if err == nil {
			if localityInfo.shouldPreferLeaseholder() {
				decision.Mode = ReadRouteLeaseholder
				decision.TargetReplicaID = req.Descriptor.LeaseholderReplicaID
				decision.TargetRegion = localityInfo.leaseholderRegion
				decision.Reason = fmt.Sprintf(
					"historical follower read is safe locally, but placement prefers leaseholder region %q over local region %q",
					localityInfo.leaseholderRegion,
					localityInfo.localReplicaRegion,
				)
				return decision, nil
			}
			decision.Mode = ReadRouteFollowerHistorical
			decision.TargetReplicaID = req.LocalReplicaID
			decision.TargetRegion = localityInfo.localReplicaRegion
			decision.LocalityPreferred = localityInfo.localRegionPreferred
			decision.Reason = localityInfo.followerReason()
			return decision, nil
		}
		if !errors.Is(err, closedts.ErrFollowerReadTooFresh) &&
			!errors.Is(err, closedts.ErrClosedTimestampNotApplied) &&
			!errors.Is(err, closedts.ErrClosedTimestampLeaseMismatch) {
			return ReadDecision{}, err
		}
		decision.FreshnessGap = followerFreshnessGap(req.ReadTS, req.AppliedThrough, req.ClosedTimestamp)
		if errors.Is(err, closedts.ErrClosedTimestampLeaseMismatch) {
			decision.Reason = "closed timestamp belongs to a stale lease sequence"
		} else if errors.Is(err, closedts.ErrClosedTimestampNotApplied) {
			decision.Reason = "closed timestamp has not been applied on the local replica"
		}
	}
	if req.LocalReplicaID == req.Descriptor.LeaseholderReplicaID {
		decision.Mode = ReadRouteLeaseholder
		decision.TargetReplicaID = req.Descriptor.LeaseholderReplicaID
		decision.TargetRegion = localityInfo.leaseholderRegion
		decision.LocalityPreferred = localityInfo.leaseholderPreferred
		decision.Reason = localityInfo.leaseholderReason()
		return decision, nil
	}
	decision.Mode = ReadRouteLeaseholder
	decision.TargetReplicaID = req.Descriptor.LeaseholderReplicaID
	decision.TargetRegion = localityInfo.leaseholderRegion
	decision.LocalityPreferred = localityInfo.leaseholderPreferred
	if decision.Reason == "" {
		decision.Reason = localityInfo.leaseholderFallbackReason(decision.FreshnessGap)
	}
	return decision, nil
}

func descriptorHasReplica(desc meta.RangeDescriptor, replicaID uint64) bool {
	for _, replica := range desc.Replicas {
		if replica.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

type readLocalityInfo struct {
	preferredRegion      string
	localReplicaRegion   string
	leaseholderRegion    string
	localRegionPreferred bool
	leaseholderPreferred bool
}

func buildReadLocalityInfo(req HistoricalReadRequest) (readLocalityInfo, error) {
	regionByReplica := make(map[uint64]string, len(req.ReplicaLocalities))
	for _, locality := range req.ReplicaLocalities {
		if locality.ReplicaID == 0 {
			return readLocalityInfo{}, fmt.Errorf("routing: locality replica id must be non-zero")
		}
		regionByReplica[locality.ReplicaID] = canonicalRegion(locality.Region)
	}
	compiled, ok, err := req.Descriptor.CompiledPlacement()
	if err != nil {
		return readLocalityInfo{}, err
	}
	info := readLocalityInfo{
		localReplicaRegion: canonicalRegion(regionByReplica[req.LocalReplicaID]),
		leaseholderRegion:  canonicalRegion(regionByReplica[req.Descriptor.LeaseholderReplicaID]),
	}
	if !ok {
		return info, nil
	}
	if len(compiled.LeasePreferences) > 0 {
		info.preferredRegion = compiled.LeasePreferences[0]
	} else if len(compiled.PreferredRegions) > 0 {
		info.preferredRegion = compiled.PreferredRegions[0]
	}
	info.localRegionPreferred = regionPreferred(info.localReplicaRegion, compiled)
	info.leaseholderPreferred = regionPreferred(info.leaseholderRegion, compiled)
	return info, nil
}

func (i readLocalityInfo) shouldPreferLeaseholder() bool {
	if i.preferredRegion == "" {
		return false
	}
	if i.localReplicaRegion == "" || i.leaseholderRegion == "" {
		return false
	}
	if i.localReplicaRegion == i.leaseholderRegion {
		return false
	}
	return !i.localRegionPreferred && i.leaseholderPreferred
}

func (i readLocalityInfo) followerReason() string {
	switch {
	case i.preferredRegion != "" && i.localRegionPreferred:
		return fmt.Sprintf("closed timestamp permits a local historical read in preferred region %q", i.localReplicaRegion)
	case i.localReplicaRegion != "":
		return fmt.Sprintf("closed timestamp permits a local historical read in region %q", i.localReplicaRegion)
	default:
		return "closed timestamp permits local historical read"
	}
}

func (i readLocalityInfo) leaseholderReason() string {
	switch {
	case i.preferredRegion != "" && i.leaseholderPreferred:
		return fmt.Sprintf("local replica is current leaseholder in preferred region %q", i.leaseholderRegion)
	case i.leaseholderRegion != "":
		return fmt.Sprintf("local replica is current leaseholder in region %q", i.leaseholderRegion)
	default:
		return "local replica is current leaseholder"
	}
}

func (i readLocalityInfo) leaseholderFallbackReason(gap TimestampGap) string {
	switch {
	case !gap.IsZero() && i.preferredRegion != "" && i.leaseholderPreferred:
		return fmt.Sprintf(
			"historical follower read is not yet safe; route to preferred-region leaseholder %q with freshness gap %s",
			i.leaseholderRegion,
			gap.String(),
		)
	case !gap.IsZero():
		return fmt.Sprintf("historical follower read is not yet safe; freshness gap %s", gap.String())
	case i.preferredRegion != "" && i.leaseholderPreferred:
		return fmt.Sprintf("historical follower read is not yet safe; route to preferred-region leaseholder %q", i.leaseholderRegion)
	default:
		return "historical follower read is not yet safe"
	}
}

func (g TimestampGap) String() string {
	return fmt.Sprintf("%d.%d", g.WallTime, g.Logical)
}

func followerFreshnessGap(readTS, appliedThrough hlc.Timestamp, record *closedts.Record) TimestampGap {
	if record == nil {
		return TimestampGap{}
	}
	barrier := record.ClosedTS
	if appliedThrough.Compare(barrier) < 0 {
		barrier = appliedThrough
	}
	return timestampGap(readTS, barrier)
}

func timestampGap(target, barrier hlc.Timestamp) TimestampGap {
	if target.Compare(barrier) <= 0 {
		return TimestampGap{}
	}
	if target.WallTime == barrier.WallTime {
		return TimestampGap{
			Logical: target.Logical - barrier.Logical,
		}
	}
	return TimestampGap{
		WallTime: target.WallTime - barrier.WallTime,
	}
}

func regionPreferred(region string, compiled placement.CompiledPolicy) bool {
	region = canonicalRegion(region)
	if region == "" {
		return false
	}
	if len(compiled.LeasePreferences) > 0 {
		return slices.Contains(compiled.LeasePreferences, region)
	}
	return slices.Contains(compiled.PreferredRegions, region)
}

func canonicalRegion(region string) string {
	return strings.ToLower(strings.TrimSpace(region))
}
