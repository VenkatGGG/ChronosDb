package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/txn"
)

func TestHostBootstrapsStoreAndReopensSeededDescriptors(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	seeds := []meta.RangeDescriptor{
		{
			RangeID:    11,
			Generation: 1,
			StartKey:   []byte("a"),
			EndKey:     []byte("m"),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
				{ReplicaID: 2, NodeID: 2, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 1,
		},
		{
			RangeID:    12,
			Generation: 1,
			StartKey:   []byte("m"),
			EndKey:     []byte("z"),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 3, NodeID: 2, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 3,
		},
	}

	host, err := Open(context.Background(), Config{
		NodeID:     1,
		StoreID:    9,
		ClusterID:  "cluster-runtime",
		DataDir:    dataDir,
		SeedRanges: seeds,
	})
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	metadata := host.Metadata()
	if !metadata.Bootstrapped {
		t.Fatal("host metadata bootstrapped = false, want true")
	}
	if metadata.Ident.ClusterID != "cluster-runtime" || metadata.Ident.NodeID != 1 || metadata.Ident.StoreID != 9 {
		t.Fatalf("store ident = %+v, want cluster-runtime/1/9", metadata.Ident)
	}
	descs, err := host.HostedDescriptors()
	if err != nil {
		t.Fatalf("hosted descriptors: %v", err)
	}
	if len(descs) != 1 || descs[0].RangeID != 11 {
		t.Fatalf("hosted descriptors = %+v, want only range 11", descs)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("close host: %v", err)
	}

	reopened, err := Open(context.Background(), Config{
		NodeID:    1,
		StoreID:   9,
		ClusterID: "cluster-runtime",
		DataDir:   dataDir,
	})
	if err != nil {
		t.Fatalf("reopen host: %v", err)
	}
	defer reopened.Close()
	descs, err = reopened.HostedDescriptors()
	if err != nil {
		t.Fatalf("reopened hosted descriptors: %v", err)
	}
	if len(descs) != 1 || descs[0].RangeID != 11 {
		t.Fatalf("reopened hosted descriptors = %+v, want persisted range 11", descs)
	}
}

func TestHostReplicatesTxnRecordsAndIntents(t *testing.T) {
	t.Parallel()

	manifest, err := BuildBootstrapManifest("cluster-runtime-txn", []BootstrapNode{
		{NodeID: 1, StoreID: 1},
	}, []meta.RangeDescriptor{
		{
			RangeID:    11,
			Generation: 1,
			StartKey:   storage.GlobalTablePrimaryPrefix(7),
			EndKey:     storage.GlobalTablePrimaryPrefix(8),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 1,
		},
	})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}

	host, err := Open(context.Background(), Config{
		NodeID:            1,
		StoreID:           1,
		ClusterID:         manifest.ClusterID,
		DataDir:           t.TempDir(),
		BootstrapManifest: &manifest,
	})
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- host.Run(runCtx)
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				t.Fatalf("run host: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out stopping host")
		}
	}()

	systemRange := manifest.Meta2[0]
	waitForLeader(t, host, systemRange.RangeID, 1)
	waitForLeader(t, host, 11, 1)

	record := txn.Record{
		ID:              storage.TxnID{4, 5, 6},
		Status:          txn.StatusPending,
		ReadTS:          hlc.Timestamp{WallTime: 10, Logical: 0},
		WriteTS:         hlc.Timestamp{WallTime: 11, Logical: 0},
		AnchorRangeID:   systemRange.RangeID,
		TouchedRanges:   []uint64{systemRange.RangeID, 11},
		LastHeartbeatTS: hlc.Timestamp{WallTime: 12, Logical: 0},
	}
	if _, err := host.PutTxnRecordLocal(context.Background(), record); err != nil {
		t.Fatalf("put txn record local: %v", err)
	}
	gotRecord, err := host.GetTxnRecordLocal(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("get txn record local: %v", err)
	}
	if gotRecord.Status != record.Status || gotRecord.AnchorRangeID != record.AnchorRangeID {
		t.Fatalf("txn record = %+v, want %+v", gotRecord, record)
	}

	key := storage.GlobalTablePrimaryKey(7, []byte("alice"))
	intent := storage.Intent{
		TxnID:          record.ID,
		Epoch:          record.Epoch,
		WriteTimestamp: record.WriteTS,
		Strength:       storage.IntentStrengthExclusive,
		Value:          []byte("value"),
	}
	if _, err := host.PutIntentLocal(context.Background(), key, intent); err != nil {
		t.Fatalf("put intent local: %v", err)
	}
	gotIntent, err := host.engine.GetIntent(context.Background(), key)
	if err != nil {
		t.Fatalf("get intent: %v", err)
	}
	if gotIntent.TxnID != intent.TxnID || string(gotIntent.Value) != string(intent.Value) {
		t.Fatalf("intent = %+v, want %+v", gotIntent, intent)
	}
	if _, err := host.DeleteIntentLocal(context.Background(), key); err != nil {
		t.Fatalf("delete intent local: %v", err)
	}
	if _, err := host.engine.GetIntent(context.Background(), key); err != storage.ErrIntentNotFound {
		t.Fatalf("post-delete intent err = %v, want %v", err, storage.ErrIntentNotFound)
	}
}

func TestHostSeedsAndLoadsSQLCatalog(t *testing.T) {
	t.Parallel()

	manifest, err := BuildBootstrapManifest("cluster-runtime-catalog", []BootstrapNode{
		{NodeID: 1, StoreID: 1},
	}, []meta.RangeDescriptor{
		{
			RangeID:    11,
			Generation: 1,
			StartKey:   storage.GlobalTablePrimaryPrefix(42),
			EndKey:     storage.GlobalTablePrimaryPrefix(43),
			Replicas: []meta.ReplicaDescriptor{
				{ReplicaID: 1, NodeID: 1, Role: meta.ReplicaRoleVoter},
			},
			LeaseholderReplicaID: 1,
		},
	})
	if err != nil {
		t.Fatalf("build bootstrap manifest: %v", err)
	}

	host, err := Open(context.Background(), Config{
		NodeID:            1,
		StoreID:           1,
		ClusterID:         manifest.ClusterID,
		DataDir:           t.TempDir(),
		BootstrapManifest: &manifest,
	})
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	catalog := chronossql.NewCatalog()
	if err := catalog.AddTable(chronossql.TableDescriptor{
		ID:   42,
		Name: "widgets",
		Columns: []chronossql.ColumnDescriptor{
			{ID: 1, Name: "id", Type: chronossql.ColumnTypeInt},
			{ID: 2, Name: "name", Type: chronossql.ColumnTypeString},
		},
		PrimaryKey: []string{"id"},
	}); err != nil {
		t.Fatalf("add widgets table: %v", err)
	}
	if err := host.SeedSQLCatalog(context.Background(), catalog); err != nil {
		t.Fatalf("seed sql catalog: %v", err)
	}
	loaded, err := host.LoadSQLCatalog(context.Background())
	if err != nil {
		t.Fatalf("load sql catalog: %v", err)
	}
	if _, err := loaded.ResolveTable("widgets"); err != nil {
		t.Fatalf("resolve persisted widgets table: %v", err)
	}
}

func waitForLeader(t *testing.T, host *Host, rangeID, replicaID uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leader, err := host.Leader(rangeID)
		if err == nil && leader == replicaID {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	leader, _ := host.Leader(rangeID)
	t.Fatalf("range %d leader = %d, want %d", rangeID, leader, replicaID)
}
