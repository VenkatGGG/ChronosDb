package sql

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestPlannerPointLookupSelect(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select id, name from users where id = 7")
	if err != nil {
		t.Fatalf("plan select: %v", err)
	}
	lookup, ok := plan.(PointLookupPlan)
	if !ok {
		t.Fatalf("plan type = %T, want PointLookupPlan", plan)
	}
	if len(lookup.Projection) != 2 {
		t.Fatalf("projection count = %d, want 2", len(lookup.Projection))
	}
	wantKey := storage.GlobalTablePrimaryKey(7, encodedIntKey(7))
	if !bytes.Equal(lookup.Key, wantKey) {
		t.Fatalf("lookup key = %q, want %q", lookup.Key, wantKey)
	}
}

func TestPlannerRangeScanSelect(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select * from users where id >= 10 and id < 20")
	if err != nil {
		t.Fatalf("plan select: %v", err)
	}
	scan, ok := plan.(RangeScanPlan)
	if !ok {
		t.Fatalf("plan type = %T, want RangeScanPlan", plan)
	}
	if !scan.StartInclusive || scan.EndInclusive {
		t.Fatalf("scan inclusivity = (%v,%v), want (true,false)", scan.StartInclusive, scan.EndInclusive)
	}
	if !bytes.Equal(scan.StartKey, storage.GlobalTablePrimaryKey(7, encodedIntKey(10))) {
		t.Fatalf("unexpected start key: %q", scan.StartKey)
	}
	if !bytes.Equal(scan.EndKey, storage.GlobalTablePrimaryKey(7, encodedIntKey(20))) {
		t.Fatalf("unexpected end key: %q", scan.EndKey)
	}
}

func TestPlannerInsertMapsToKV(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("insert into users (id, name, email) values (1, 'alice', 'a@example.com')")
	if err != nil {
		t.Fatalf("plan insert: %v", err)
	}
	insert, ok := plan.(InsertPlan)
	if !ok {
		t.Fatalf("plan type = %T, want InsertPlan", plan)
	}
	if !bytes.Equal(insert.Key, storage.GlobalTablePrimaryKey(7, encodedIntKey(1))) {
		t.Fatalf("insert key = %q, want %q", insert.Key, storage.GlobalTablePrimaryKey(7, encodedIntKey(1)))
	}
	payload := string(insert.Value)
	if !strings.Contains(payload, "\"name\"") || !strings.Contains(payload, "\"alice\"") {
		t.Fatalf("unexpected insert payload: %s", payload)
	}
}

func TestPlannerRejectsNonPrimaryKeyPredicate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	if _, err := planner.Plan("select id from users where name = 'alice'"); err == nil {
		t.Fatalf("expected planner error for non-primary-key predicate")
	}
}

func testPlanner(t *testing.T) *Planner {
	t.Helper()

	catalog := NewCatalog()
	if err := catalog.AddTable(TableDescriptor{
		ID:   7,
		Name: "users",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: ColumnTypeInt},
			{ID: 2, Name: "name", Type: ColumnTypeString},
			{ID: 3, Name: "email", Type: ColumnTypeString, Nullable: true},
		},
		PrimaryKey: []string{"id"},
	}); err != nil {
		t.Fatalf("add users descriptor: %v", err)
	}
	return NewPlanner(NewParser(), catalog)
}

func encodedIntKey(value int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value)^(uint64(1)<<63))
	return buf[:]
}
