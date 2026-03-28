package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/closedts"
	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/cockroachdb/pebble/vfs"
)

func TestBootstrapSnapshotAndRecovery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fs := vfs.NewMem()
	engine, err := Open(ctx, Options{
		Dir: "chronosdb-test",
		FS:  fs,
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	if engine.Metadata().Bootstrapped {
		t.Fatalf("fresh engine should not be bootstrapped")
	}

	ident := StoreIdent{
		ClusterID: "cluster-a",
		NodeID:    11,
		StoreID:   29,
	}
	if err := engine.Bootstrap(ctx, ident); err != nil {
		t.Fatalf("bootstrap engine: %v", err)
	}

	logicalKey := GlobalTablePrimaryKey(7, []byte("alice"))
	ts1 := hlc.Timestamp{WallTime: 100, Logical: 1}
	ts2 := hlc.Timestamp{WallTime: 200, Logical: 1}
	if err := engine.PutMVCCValue(ctx, logicalKey, ts1, []byte("v1")); err != nil {
		t.Fatalf("put mvcc value: %v", err)
	}
	intent := Intent{
		TxnID:          TxnID{9, 9, 9},
		Epoch:          1,
		WriteTimestamp: hlc.Timestamp{WallTime: 150, Logical: 4},
		Strength:       IntentStrengthExclusive,
		Value:          []byte("pending"),
	}
	if err := engine.PutIntent(ctx, logicalKey, intent); err != nil {
		t.Fatalf("put intent: %v", err)
	}
	publication := closedts.Record{
		RangeID:       7,
		LeaseSequence: 3,
		ClosedTS:      hlc.Timestamp{WallTime: 180, Logical: 0},
		PublishedAt:   hlc.Timestamp{WallTime: 190, Logical: 0},
	}
	if err := engine.PutClosedTimestamp(ctx, publication); err != nil {
		t.Fatalf("put closed timestamp: %v", err)
	}

	snap := engine.NewSnapshot()

	if err := engine.PutMVCCValue(ctx, logicalKey, ts2, []byte("v2")); err != nil {
		t.Fatalf("put post-snapshot value: %v", err)
	}

	snapValue, err := snap.GetMVCCValue(ctx, logicalKey, ts2)
	if err == nil {
		t.Fatalf("expected snapshot to not see post-snapshot value, got %q", snapValue)
	}
	if got, err := snap.GetMVCCValue(ctx, logicalKey, ts1); err != nil || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("snapshot read old value: got %q err=%v", got, err)
	}
	if got, err := snap.GetIntent(ctx, logicalKey); err != nil || !bytes.Equal(got.Value, intent.Value) {
		t.Fatalf("snapshot read intent: got %+v err=%v", got, err)
	}

	if got, err := engine.GetMVCCValue(ctx, logicalKey, ts2); err != nil || !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("engine read new value: got %q err=%v", got, err)
	}
	if got, err := engine.GetClosedTimestamp(ctx, publication.RangeID); err != nil || got != publication {
		t.Fatalf("engine read closed timestamp: got %+v err=%v", got, err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	reopened, err := Open(ctx, Options{
		Dir: "chronosdb-test",
		FS:  fs,
	})
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer reopened.Close()

	meta := reopened.Metadata()
	if !meta.Bootstrapped {
		t.Fatalf("reopened engine should be bootstrapped")
	}
	if meta.Ident != ident {
		t.Fatalf("ident mismatch: got %+v want %+v", meta.Ident, ident)
	}
	if meta.Version != CurrentStoreVersion {
		t.Fatalf("version mismatch: got %+v want %+v", meta.Version, CurrentStoreVersion)
	}
	if got, err := reopened.GetMVCCValue(ctx, logicalKey, ts1); err != nil || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("recovered old value: got %q err=%v", got, err)
	}
	if got, err := reopened.GetMVCCValue(ctx, logicalKey, ts2); err != nil || !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("recovered new value: got %q err=%v", got, err)
	}
	if got, err := reopened.GetIntent(ctx, logicalKey); err != nil || !bytes.Equal(got.Value, intent.Value) {
		t.Fatalf("recovered intent: got %+v err=%v", got, err)
	}
	if got, err := reopened.GetClosedTimestamp(ctx, publication.RangeID); err != nil || got != publication {
		t.Fatalf("recovered closed timestamp: got %+v err=%v", got, err)
	}
}

func TestBootstrapRejectsSecondInitialization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := Open(ctx, Options{
		Dir: "already-bootstrapped",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	ident := StoreIdent{
		ClusterID: "cluster-a",
		NodeID:    1,
		StoreID:   2,
	}
	if err := engine.Bootstrap(ctx, ident); err != nil {
		t.Fatalf("first bootstrap: %v", err)
	}
	if err := engine.Bootstrap(ctx, ident); err != ErrAlreadyBootstrapped {
		t.Fatalf("second bootstrap error = %v, want %v", err, ErrAlreadyBootstrapped)
	}
}

func TestScanRawRangeReturnsBoundedMVCCSpan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine, err := Open(ctx, Options{
		Dir: "scan-raw-range",
		FS:  vfs.NewMem(),
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if err := engine.Bootstrap(ctx, StoreIdent{ClusterID: "cluster-a", NodeID: 1, StoreID: 1}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	inRangeKey := GlobalTablePrimaryKey(7, []byte("alice"))
	outOfRangeKey := GlobalTablePrimaryKey(9, []byte("bob"))
	if err := engine.PutMVCCValue(ctx, inRangeKey, hlc.Timestamp{WallTime: 100, Logical: 1}, []byte("in")); err != nil {
		t.Fatalf("put in-range value: %v", err)
	}
	if err := engine.PutMVCCValue(ctx, outOfRangeKey, hlc.Timestamp{WallTime: 100, Logical: 1}, []byte("out")); err != nil {
		t.Fatalf("put out-of-range value: %v", err)
	}

	kvs, err := engine.ScanRawRange(GlobalTablePrimaryPrefix(7), GlobalTablePrimaryPrefix(8))
	if err != nil {
		t.Fatalf("scan raw range: %v", err)
	}
	if len(kvs) != 1 {
		t.Fatalf("raw kv count = %d, want 1", len(kvs))
	}
	for _, kv := range kvs {
		decoded, err := DecodeMVCCKey(kv.Key)
		if err != nil {
			t.Fatalf("decode mvcc key: %v", err)
		}
		if !bytes.Equal(decoded.LogicalKey, inRangeKey) {
			t.Fatalf("scanned logical key = %q, want %q", decoded.LogicalKey, inRangeKey)
		}
		if decoded.Timestamp != (hlc.Timestamp{WallTime: 100, Logical: 1}) {
			t.Fatalf("scanned timestamp = %+v, want %+v", decoded.Timestamp, hlc.Timestamp{WallTime: 100, Logical: 1})
		}
	}
}
