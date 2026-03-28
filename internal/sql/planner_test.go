package sql

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/placement"
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

func TestPlannerAggregateSelect(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select name, count(*) from users group by name")
	if err != nil {
		t.Fatalf("plan aggregate select: %v", err)
	}
	aggregate, ok := plan.(AggregatePlan)
	if !ok {
		t.Fatalf("plan type = %T, want AggregatePlan", plan)
	}
	if len(aggregate.GroupBy) != 1 || aggregate.GroupBy[0].Name != "name" {
		t.Fatalf("group by = %+v, want [name]", aggregate.GroupBy)
	}
	if len(aggregate.Aggregates) != 1 || aggregate.Aggregates[0].Func != AggregateFuncCount {
		t.Fatalf("aggregates = %+v, want one count aggregate", aggregate.Aggregates)
	}
	if aggregate.Aggregates[0].Input != nil {
		t.Fatalf("count(*) input = %+v, want nil", aggregate.Aggregates[0].Input)
	}
	if len(aggregate.Input.Projection) != 1 || aggregate.Input.Projection[0].Name != "name" {
		t.Fatalf("input projection = %+v, want [name]", aggregate.Input.Projection)
	}
	if !bytes.Equal(aggregate.Input.StartKey, storage.GlobalTablePrimaryPrefix(7)) {
		t.Fatalf("aggregate start key = %q, want table prefix", aggregate.Input.StartKey)
	}
}

func TestPlannerAggregateSumSelect(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select region, sum(sales) from orders group by region")
	if err != nil {
		t.Fatalf("plan aggregate sum select: %v", err)
	}
	aggregate, ok := plan.(AggregatePlan)
	if !ok {
		t.Fatalf("plan type = %T, want AggregatePlan", plan)
	}
	if len(aggregate.GroupBy) != 1 || aggregate.GroupBy[0].Name != "region" {
		t.Fatalf("group by = %+v, want [region]", aggregate.GroupBy)
	}
	if len(aggregate.Aggregates) != 1 || aggregate.Aggregates[0].Func != AggregateFuncSum {
		t.Fatalf("aggregates = %+v, want one sum aggregate", aggregate.Aggregates)
	}
	if aggregate.Aggregates[0].Input == nil || aggregate.Aggregates[0].Input.Name != "sales" {
		t.Fatalf("sum input = %+v, want sales column", aggregate.Aggregates[0].Input)
	}
	if len(aggregate.Input.Projection) != 2 {
		t.Fatalf("input projection count = %d, want 2", len(aggregate.Input.Projection))
	}
}

func TestPlannerRejectsNonPrimaryKeyPredicate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	if _, err := planner.Plan("select id from users where name = 'alice'"); err == nil {
		t.Fatalf("expected planner error for non-primary-key predicate")
	}
}

func TestPlannerRejectsUngroupedAggregateProjection(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	if _, err := planner.Plan("select email, count(*) from users group by name"); err == nil {
		t.Fatalf("expected planner error for ungrouped aggregate projection")
	}
}

func TestPlannerOptimizePrefersPointLookupForEquality(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	optimized, err := planner.Optimize("select id, name from users where id = 7")
	if err != nil {
		t.Fatalf("optimize select: %v", err)
	}
	if len(optimized.Candidates) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(optimized.Candidates))
	}
	if optimized.Selected.Name != "point_lookup" {
		t.Fatalf("selected candidate = %q, want point_lookup", optimized.Selected.Name)
	}
	if _, ok := optimized.Selected.Plan.(PointLookupPlan); !ok {
		t.Fatalf("selected plan type = %T, want PointLookupPlan", optimized.Selected.Plan)
	}
	if optimized.Candidates[0].Cost.Score >= optimized.Candidates[1].Cost.Score {
		t.Fatalf("expected strictly cheaper winner, got %v >= %v", optimized.Candidates[0].Cost.Score, optimized.Candidates[1].Cost.Score)
	}
}

func TestPlannerOptimizeOpenEndedSelectUsesRangeScan(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	optimized, err := planner.Optimize("select * from users")
	if err != nil {
		t.Fatalf("optimize select: %v", err)
	}
	if len(optimized.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(optimized.Candidates))
	}
	if optimized.Selected.Name != "range_scan" {
		t.Fatalf("selected candidate = %q, want range_scan", optimized.Selected.Name)
	}
	scan, ok := optimized.Selected.Plan.(RangeScanPlan)
	if !ok {
		t.Fatalf("selected plan type = %T, want RangeScanPlan", optimized.Selected.Plan)
	}
	if len(scan.StartKey) == 0 || len(scan.EndKey) == 0 {
		t.Fatalf("range scan should carry a bounded key span")
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
		Stats: TableStats{
			EstimatedRows:   10000,
			AverageRowBytes: 192,
		},
		PlacementPolicy: &placement.Policy{
			PlacementMode:    placement.ModeHomeRegion,
			HomeRegion:       "us-east1",
			PreferredRegions: []string{"us-east1", "us-west1", "europe-west1"},
		},
	}); err != nil {
		t.Fatalf("add users descriptor: %v", err)
	}
	if err := catalog.AddTable(TableDescriptor{
		ID:   9,
		Name: "orders",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: ColumnTypeInt},
			{ID: 2, Name: "region", Type: ColumnTypeString},
			{ID: 3, Name: "sales", Type: ColumnTypeInt},
		},
		PrimaryKey: []string{"id"},
		Stats: TableStats{
			EstimatedRows:   250000,
			AverageRowBytes: 96,
		},
		PlacementPolicy: &placement.Policy{
			PlacementMode:    placement.ModeRegional,
			PreferredRegions: []string{"us-east1"},
		},
	}); err != nil {
		t.Fatalf("add orders descriptor: %v", err)
	}
	return NewPlanner(NewParser(), catalog)
}

func encodedIntKey(value int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value)^(uint64(1)<<63))
	return buf[:]
}
