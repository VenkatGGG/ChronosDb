package systemtest

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	logWriteAck             = "write_ack"
	logWriteVisible         = "write_visible"
	logFollowerReadCheck    = "follower_read_check"
	logStagingObserved      = "staging_observed"
	logStagingOutcome       = "staging_outcome"
	logLeaseSequence        = "lease_sequence"
	logDescriptorGeneration = "descriptor_generation"
)

// ArtifactAssertion validates a persisted run artifact bundle.
type ArtifactAssertion interface {
	Name() string
	Assert(RunArtifacts) error
}

// AssertionMarker helpers standardize external-runner log lines so the built-in
// artifact assertions can reason over node logs deterministically.
func AssertionMarkerWriteAck(token string) string {
	return fmt.Sprintf("%s token=%s", logWriteAck, token)
}

func AssertionMarkerWriteVisible(token string) string {
	return fmt.Sprintf("%s token=%s", logWriteVisible, token)
}

func AssertionMarkerFollowerReadCheck(status string, readTS, closedTS uint64) string {
	return fmt.Sprintf("%s status=%s read_ts=%d closed_ts=%d", logFollowerReadCheck, status, readTS, closedTS)
}

func AssertionMarkerStagingObserved(txn string) string {
	return fmt.Sprintf("%s txn=%s", logStagingObserved, txn)
}

func AssertionMarkerStagingOutcome(txn, result string) string {
	return fmt.Sprintf("%s txn=%s result=%s", logStagingOutcome, txn, result)
}

func AssertionMarkerLeaseSequence(rangeID, seq uint64) string {
	return fmt.Sprintf("%s range=%d seq=%d", logLeaseSequence, rangeID, seq)
}

func AssertionMarkerDescriptorGeneration(rangeID, generation uint64) string {
	return fmt.Sprintf("%s range=%d gen=%d", logDescriptorGeneration, rangeID, generation)
}

// DefaultCorrectnessAssertions returns the artifact validators required by the
// Phase 8 closure plan.
func DefaultCorrectnessAssertions() []ArtifactAssertion {
	return []ArtifactAssertion{
		noAcknowledgedWriteLossAssertion{},
		noStaleFollowerReadAssertion{},
		deterministicStagingRecoveryAssertion{},
		monotonicMetadataAssertion{},
	}
}

// ValidateArtifactAssertions executes the supplied assertion pack against one artifact bundle.
func ValidateArtifactAssertions(artifacts RunArtifacts, assertions ...ArtifactAssertion) error {
	var errs []error
	for _, assertion := range assertions {
		if assertion == nil {
			continue
		}
		if err := assertion.Assert(artifacts); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", assertion.Name(), err))
		}
	}
	return errors.Join(errs...)
}

type noAcknowledgedWriteLossAssertion struct{}

func (noAcknowledgedWriteLossAssertion) Name() string {
	return "no_acknowledged_write_loss"
}

func (noAcknowledgedWriteLossAssertion) Assert(artifacts RunArtifacts) error {
	acked := make(map[string]struct{})
	visible := make(map[string]struct{})
	for _, entry := range flattenLogs(artifacts.NodeLogs) {
		marker, ok := parseMarker(entry.Message)
		if !ok {
			continue
		}
		switch marker.kind {
		case logWriteAck:
			if token := marker.fields["token"]; token != "" {
				acked[token] = struct{}{}
			}
		case logWriteVisible:
			if token := marker.fields["token"]; token != "" {
				visible[token] = struct{}{}
			}
		}
	}
	for token := range acked {
		if _, ok := visible[token]; !ok {
			return fmt.Errorf("acknowledged write %q never became visible", token)
		}
	}
	return nil
}

type noStaleFollowerReadAssertion struct{}

func (noStaleFollowerReadAssertion) Name() string {
	return "no_stale_follower_read"
}

func (noStaleFollowerReadAssertion) Assert(artifacts RunArtifacts) error {
	for _, entry := range flattenLogs(artifacts.NodeLogs) {
		marker, ok := parseMarker(entry.Message)
		if !ok || marker.kind != logFollowerReadCheck {
			continue
		}
		readTS, err := parseUintField(marker.fields, "read_ts")
		if err != nil {
			return err
		}
		closedTS, err := parseUintField(marker.fields, "closed_ts")
		if err != nil {
			return err
		}
		status := marker.fields["status"]
		if status == "violation" || readTS > closedTS {
			return fmt.Errorf("follower read exceeded closed timestamp: read_ts=%d closed_ts=%d", readTS, closedTS)
		}
	}
	return nil
}

type deterministicStagingRecoveryAssertion struct{}

func (deterministicStagingRecoveryAssertion) Name() string {
	return "deterministic_staging_recovery"
}

func (deterministicStagingRecoveryAssertion) Assert(artifacts RunArtifacts) error {
	observed := make(map[string]struct{})
	outcomes := make(map[string]string)
	for _, entry := range flattenLogs(artifacts.NodeLogs) {
		marker, ok := parseMarker(entry.Message)
		if !ok {
			continue
		}
		switch marker.kind {
		case logStagingObserved:
			if txn := marker.fields["txn"]; txn != "" {
				observed[txn] = struct{}{}
			}
		case logStagingOutcome:
			txn := marker.fields["txn"]
			result := marker.fields["result"]
			if txn == "" || result == "" {
				return fmt.Errorf("staging outcome marker requires txn and result")
			}
			if prior, ok := outcomes[txn]; ok && prior != result {
				return fmt.Errorf("transaction %q observed conflicting staging outcomes %q and %q", txn, prior, result)
			}
			outcomes[txn] = result
		}
	}
	for txn := range observed {
		if _, ok := outcomes[txn]; !ok {
			return fmt.Errorf("transaction %q entered staging without a terminal outcome", txn)
		}
	}
	return nil
}

type monotonicMetadataAssertion struct{}

func (monotonicMetadataAssertion) Name() string {
	return "monotonic_metadata_generations"
}

func (monotonicMetadataAssertion) Assert(artifacts RunArtifacts) error {
	leaseByRange := make(map[uint64]uint64)
	generationByRange := make(map[uint64]uint64)
	for _, entry := range flattenLogs(artifacts.NodeLogs) {
		marker, ok := parseMarker(entry.Message)
		if !ok {
			continue
		}
		switch marker.kind {
		case logLeaseSequence:
			rangeID, err := parseUintField(marker.fields, "range")
			if err != nil {
				return err
			}
			seq, err := parseUintField(marker.fields, "seq")
			if err != nil {
				return err
			}
			if prior, ok := leaseByRange[rangeID]; ok && seq < prior {
				return fmt.Errorf("lease sequence regressed for range %d: %d -> %d", rangeID, prior, seq)
			}
			leaseByRange[rangeID] = seq
		case logDescriptorGeneration:
			rangeID, err := parseUintField(marker.fields, "range")
			if err != nil {
				return err
			}
			generation, err := parseUintField(marker.fields, "gen")
			if err != nil {
				return err
			}
			if prior, ok := generationByRange[rangeID]; ok && generation < prior {
				return fmt.Errorf("descriptor generation regressed for range %d: %d -> %d", rangeID, prior, generation)
			}
			generationByRange[rangeID] = generation
		}
	}
	return nil
}

type marker struct {
	kind   string
	fields map[string]string
}

type logObservation struct {
	NodeID    uint64
	Timestamp time.Time
	Message   string
}

func flattenLogs(nodeLogs map[uint64][]NodeLogEntry) []logObservation {
	flattened := make([]logObservation, 0)
	for nodeID, entries := range nodeLogs {
		for _, entry := range entries {
			flattened = append(flattened, logObservation{
				NodeID:    nodeID,
				Timestamp: entry.Timestamp,
				Message:   entry.Message,
			})
		}
	}
	sort.SliceStable(flattened, func(i, j int) bool {
		if flattened[i].Timestamp.Equal(flattened[j].Timestamp) {
			return flattened[i].NodeID < flattened[j].NodeID
		}
		return flattened[i].Timestamp.Before(flattened[j].Timestamp)
	})
	return flattened
}

func parseMarker(message string) (marker, bool) {
	parts := strings.Fields(message)
	if len(parts) == 0 {
		return marker{}, false
	}
	kind := parts[0]
	if !slices.Contains([]string{
		logWriteAck,
		logWriteVisible,
		logFollowerReadCheck,
		logStagingObserved,
		logStagingOutcome,
		logLeaseSequence,
		logDescriptorGeneration,
	}, kind) {
		return marker{}, false
	}
	fields := make(map[string]string, len(parts)-1)
	for _, part := range parts[1:] {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		fields[key] = value
	}
	return marker{kind: kind, fields: fields}, true
}

func parseUintField(fields map[string]string, key string) (uint64, error) {
	value := fields[key]
	if value == "" {
		return 0, fmt.Errorf("missing %s field", key)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s=%q: %w", key, value, err)
	}
	return parsed, nil
}
