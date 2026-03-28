package systemtest

import (
	"testing"
	"time"
)

func TestValidateArtifactAssertionsPassesForConsistentMarkers(t *testing.T) {
	t.Parallel()

	artifacts := RunArtifacts{
		NodeLogs: map[uint64][]NodeLogEntry{
			1: {
				{Timestamp: time.Unix(1, 0).UTC(), Message: AssertionMarkerWriteAck("tx-1")},
				{Timestamp: time.Unix(2, 0).UTC(), Message: AssertionMarkerStagingObserved("tx-1")},
				{Timestamp: time.Unix(3, 0).UTC(), Message: AssertionMarkerLeaseSequence(7, 1)},
			},
			2: {
				{Timestamp: time.Unix(4, 0).UTC(), Message: AssertionMarkerWriteVisible("tx-1")},
				{Timestamp: time.Unix(5, 0).UTC(), Message: AssertionMarkerFollowerReadCheck("ok", 80, 90)},
				{Timestamp: time.Unix(6, 0).UTC(), Message: AssertionMarkerStagingOutcome("tx-1", "committed")},
				{Timestamp: time.Unix(7, 0).UTC(), Message: AssertionMarkerDescriptorGeneration(7, 2)},
				{Timestamp: time.Unix(8, 0).UTC(), Message: AssertionMarkerLeaseSequence(7, 2)},
				{Timestamp: time.Unix(9, 0).UTC(), Message: AssertionMarkerDescriptorGeneration(7, 3)},
			},
		},
	}
	if err := ValidateArtifactAssertions(artifacts, DefaultCorrectnessAssertions()...); err != nil {
		t.Fatalf("validate artifact assertions: %v", err)
	}
}

func TestValidateArtifactAssertionsDetectsWriteLoss(t *testing.T) {
	t.Parallel()

	artifacts := RunArtifacts{
		NodeLogs: map[uint64][]NodeLogEntry{
			1: {{Timestamp: time.Unix(1, 0).UTC(), Message: AssertionMarkerWriteAck("tx-99")}},
		},
	}
	if err := ValidateArtifactAssertions(artifacts, DefaultCorrectnessAssertions()...); err == nil {
		t.Fatal("expected write-loss assertion to fail")
	}
}

func TestValidateArtifactAssertionsDetectsStagingAndMetadataRegression(t *testing.T) {
	t.Parallel()

	artifacts := RunArtifacts{
		NodeLogs: map[uint64][]NodeLogEntry{
			1: {
				{Timestamp: time.Unix(1, 0).UTC(), Message: AssertionMarkerStagingObserved("tx-7")},
				{Timestamp: time.Unix(2, 0).UTC(), Message: AssertionMarkerStagingOutcome("tx-7", "committed")},
				{Timestamp: time.Unix(3, 0).UTC(), Message: AssertionMarkerStagingOutcome("tx-7", "aborted")},
				{Timestamp: time.Unix(4, 0).UTC(), Message: AssertionMarkerFollowerReadCheck("violation", 120, 110)},
				{Timestamp: time.Unix(5, 0).UTC(), Message: AssertionMarkerLeaseSequence(5, 4)},
				{Timestamp: time.Unix(6, 0).UTC(), Message: AssertionMarkerLeaseSequence(5, 3)},
			},
		},
	}
	if err := ValidateArtifactAssertions(artifacts, DefaultCorrectnessAssertions()...); err == nil {
		t.Fatal("expected assertion pack to fail")
	}
}
