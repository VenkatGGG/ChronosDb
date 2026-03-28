package storage

import (
	"bytes"
	"strings"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

func TestMVCCKeyOrderingAndDecoding(t *testing.T) {
	t.Parallel()

	logical := GlobalTablePrimaryKey(42, []byte("alice\x00primary"))
	newer := hlc.Timestamp{WallTime: 20, Logical: 1}
	older := hlc.Timestamp{WallTime: 10, Logical: 7}

	newerKey, err := EncodeMVCCVersionKey(logical, newer)
	if err != nil {
		t.Fatalf("encode newer key: %v", err)
	}
	olderKey, err := EncodeMVCCVersionKey(logical, older)
	if err != nil {
		t.Fatalf("encode older key: %v", err)
	}
	metadataKey, err := EncodeMVCCMetadataKey(logical)
	if err != nil {
		t.Fatalf("encode metadata key: %v", err)
	}

	if cmp := bytes.Compare(newerKey, olderKey); cmp >= 0 {
		t.Fatalf("expected newer version to sort before older version, cmp=%d", cmp)
	}
	if cmp := bytes.Compare(olderKey, metadataKey); cmp >= 0 {
		t.Fatalf("expected metadata key to sort after all versions, cmp=%d", cmp)
	}

	decodedVersion, err := DecodeMVCCKey(newerKey)
	if err != nil {
		t.Fatalf("decode version key: %v", err)
	}
	if decodedVersion.Kind != MVCCKeyKindValue {
		t.Fatalf("unexpected version key kind: %v", decodedVersion.Kind)
	}
	if !bytes.Equal(decodedVersion.LogicalKey, logical) {
		t.Fatalf("logical key mismatch: got %q want %q", decodedVersion.LogicalKey, logical)
	}
	if decodedVersion.Timestamp.Compare(newer) != 0 {
		t.Fatalf("timestamp mismatch: got %v want %v", decodedVersion.Timestamp, newer)
	}

	decodedMetadata, err := DecodeMVCCKey(metadataKey)
	if err != nil {
		t.Fatalf("decode metadata key: %v", err)
	}
	if decodedMetadata.Kind != MVCCKeyKindMetadata {
		t.Fatalf("unexpected metadata key kind: %v", decodedMetadata.Kind)
	}
	if !bytes.Equal(decodedMetadata.LogicalKey, logical) {
		t.Fatalf("metadata logical key mismatch: got %q want %q", decodedMetadata.LogicalKey, logical)
	}
}

func TestNamespaceBuilders(t *testing.T) {
	t.Parallel()

	if key := string(StoreIdentKey()); key != "/mvcc/local/store/ident" {
		t.Fatalf("unexpected store ident key: %q", key)
	}
	if key := string(StoreVersionKey()); key != "/mvcc/local/store/version" {
		t.Fatalf("unexpected store version key: %q", key)
	}
	if key := string(RaftHardStateKey(7)); !strings.HasPrefix(key, "/raft/range/") {
		t.Fatalf("hardstate key should live under raft namespace: %q", key)
	}
	if key := string(RangeAppliedStateKey(7)); !strings.HasPrefix(key, "/mvcc/local/range/") {
		t.Fatalf("applied state key should live under mvcc local namespace: %q", key)
	}
	if prefix := string(GlobalTablePrimaryPrefix(9)); prefix != "/mvcc/global/table/\x00\x00\x00\x00\x00\x00\x00\t/primary/" {
		t.Fatalf("unexpected table primary prefix: %q", prefix)
	}
	if end := PrefixEnd([]byte("/demo/")); !bytes.Equal(end, []byte("/demo/\xff")) {
		t.Fatalf("unexpected prefix end: %q", end)
	}
}
