package placement

import "fmt"

// ReplicaCandidate is one leaseholder-eligible replica annotated with locality.
type ReplicaCandidate struct {
	ReplicaID uint64
	Region    string
	Zone      string
	Voter     bool
}

// SelectLeaseholder chooses the best voter replica according to compiled lease preferences.
func SelectLeaseholder(policy CompiledPolicy, replicas []ReplicaCandidate) (ReplicaCandidate, error) {
	if len(replicas) == 0 {
		return ReplicaCandidate{}, fmt.Errorf("placement: no replicas available for leaseholder selection")
	}
	for _, preferredRegion := range policy.LeasePreferences {
		for _, replica := range replicas {
			if !replica.Voter {
				continue
			}
			if canonicalRegion(replica.Region) == preferredRegion {
				return normalizedReplica(replica), nil
			}
		}
	}
	for _, replica := range replicas {
		if replica.Voter {
			return normalizedReplica(replica), nil
		}
	}
	return ReplicaCandidate{}, fmt.Errorf("placement: no voter replicas available for leaseholder selection")
}

func normalizedReplica(replica ReplicaCandidate) ReplicaCandidate {
	replica.Region = canonicalRegion(replica.Region)
	return replica
}
