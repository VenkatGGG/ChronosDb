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

func TestPlannerBroadSelectSupportsFilterOrderByAndLimit(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select id, name from users where name >= 'bob' order by name desc limit 2")
	if err != nil {
		t.Fatalf("plan broad select: %v", err)
	}
	scan, ok := plan.(RangeScanPlan)
	if !ok {
		t.Fatalf("plan type = %T, want RangeScanPlan", plan)
	}
	if len(scan.Filters) != 1 || scan.Filters[0].Column.Name != "name" || scan.Filters[0].Operator != ComparisonGreaterEqual {
		t.Fatalf("filters = %#v, want name >= ...", scan.Filters)
	}
	if len(scan.OrderBy) != 1 || scan.OrderBy[0].Column.Name != "name" || !scan.OrderBy[0].Descending {
		t.Fatalf("order by = %#v, want name desc", scan.OrderBy)
	}
	if scan.Limit == nil || *scan.Limit != 2 {
		t.Fatalf("limit = %#v, want 2", scan.Limit)
	}
	if !bytes.Equal(scan.StartKey, storage.GlobalTablePrimaryPrefix(7)) {
		t.Fatalf("unexpected broad-scan start key: %q", scan.StartKey)
	}
	if !bytes.Equal(scan.EndKey, storage.PrefixEnd(storage.GlobalTablePrimaryPrefix(7))) {
		t.Fatalf("unexpected broad-scan end key: %q", scan.EndKey)
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
	if got := insert.RowCount(); got != 1 {
		t.Fatalf("insert row count = %d, want 1", got)
	}
	payload := string(insert.Value)
	if !strings.Contains(payload, "\"name\"") || !strings.Contains(payload, "\"alice\"") {
		t.Fatalf("unexpected insert payload: %s", payload)
	}
}

func TestPlannerMultiRowInsertMapsToMultipleKVWrites(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("insert into users (id, name, email) values (1, 'alice', 'a@example.com'), (70, 'bob', 'b@example.com')")
	if err != nil {
		t.Fatalf("plan multi-row insert: %v", err)
	}
	insert, ok := plan.(InsertPlan)
	if !ok {
		t.Fatalf("plan type = %T, want InsertPlan", plan)
	}
	if got := insert.RowCount(); got != 2 {
		t.Fatalf("insert row count = %d, want 2", got)
	}
	rows := insert.Mutations()
	if len(rows) != 2 {
		t.Fatalf("mutation count = %d, want 2", len(rows))
	}
	if !bytes.Equal(rows[0].Key, storage.GlobalTablePrimaryKey(7, encodedIntKey(1))) {
		t.Fatalf("first insert key = %q, want %q", rows[0].Key, storage.GlobalTablePrimaryKey(7, encodedIntKey(1)))
	}
	if !bytes.Equal(rows[1].Key, storage.GlobalTablePrimaryKey(7, encodedIntKey(70))) {
		t.Fatalf("second insert key = %q, want %q", rows[1].Key, storage.GlobalTablePrimaryKey(7, encodedIntKey(70)))
	}
}

func TestPlannerUpsertMapsToKV(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("upsert into users (id, name, email) values (1, 'alice', 'a@example.com')")
	if err != nil {
		t.Fatalf("plan upsert: %v", err)
	}
	upsert, ok := plan.(UpsertPlan)
	if !ok {
		t.Fatalf("plan type = %T, want UpsertPlan", plan)
	}
	if !bytes.Equal(upsert.Key, storage.GlobalTablePrimaryKey(7, encodedIntKey(1))) {
		t.Fatalf("upsert key = %q, want %q", upsert.Key, storage.GlobalTablePrimaryKey(7, encodedIntKey(1)))
	}
	payload := string(upsert.Value)
	if !strings.Contains(payload, "\"name\"") || !strings.Contains(payload, "\"alice\"") {
		t.Fatalf("unexpected upsert payload: %s", payload)
	}
}

func TestPlannerInsertOnConflictDoNothing(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("insert into users (id, name, email) values (1, 'alice', 'a@example.com') on conflict (email) do nothing")
	if err != nil {
		t.Fatalf("plan insert on conflict do nothing: %v", err)
	}
	conflict, ok := plan.(OnConflictPlan)
	if !ok {
		t.Fatalf("plan type = %T, want OnConflictPlan", plan)
	}
	if conflict.Action != OnConflictDoNothing {
		t.Fatalf("conflict action = %q, want %q", conflict.Action, OnConflictDoNothing)
	}
	if conflict.ConflictTarget.Name != "users_email_key" {
		t.Fatalf("conflict target = %q, want users_email_key", conflict.ConflictTarget.Name)
	}
	if len(conflict.ConflictKey) == 0 {
		t.Fatalf("conflict key should be populated for unique index target")
	}
}

func TestPlannerInsertOnConflictDoUpdateBindsExcludedAssignments(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("insert into users (id, name, email) values (1, 'alice', 'a@example.com') on conflict (email) do update set name = excluded.name, email = excluded.email returning id, name")
	if err != nil {
		t.Fatalf("plan insert on conflict do update: %v", err)
	}
	conflict, ok := plan.(OnConflictPlan)
	if !ok {
		t.Fatalf("plan type = %T, want OnConflictPlan", plan)
	}
	if conflict.Action != OnConflictDoUpdate {
		t.Fatalf("conflict action = %q, want %q", conflict.Action, OnConflictDoUpdate)
	}
	if len(conflict.UpdateAssignments) != 2 {
		t.Fatalf("assignment count = %d, want 2", len(conflict.UpdateAssignments))
	}
	if conflict.UpdateAssignments[0].Column.Name != "name" || conflict.UpdateAssignments[0].Value.String != "alice" {
		t.Fatalf("first assignment = %+v, want name=alice", conflict.UpdateAssignments[0])
	}
	if len(conflict.Returning) != 2 || conflict.Returning[0].Name != "id" || conflict.Returning[1].Name != "name" {
		t.Fatalf("returning = %+v, want [id name]", conflict.Returning)
	}
}

func TestPlannerInsertReturningProjection(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("insert into users (id, name, email) values (1, 'alice', 'a@example.com') returning id, name")
	if err != nil {
		t.Fatalf("plan insert returning: %v", err)
	}
	insert, ok := plan.(InsertPlan)
	if !ok {
		t.Fatalf("plan type = %T, want InsertPlan", plan)
	}
	if len(insert.Returning) != 2 || insert.Returning[0].Name != "id" || insert.Returning[1].Name != "name" {
		t.Fatalf("returning = %+v, want [id name]", insert.Returning)
	}
}

func TestPlannerDeleteReturningProjection(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("delete from users where id = 7 returning *")
	if err != nil {
		t.Fatalf("plan delete returning: %v", err)
	}
	deletePlan, ok := plan.(DeletePlan)
	if !ok {
		t.Fatalf("plan type = %T, want DeletePlan", plan)
	}
	if len(deletePlan.Returning) != 3 {
		t.Fatalf("returning count = %d, want 3", len(deletePlan.Returning))
	}
}

func TestPlannerUpdateReturningProjection(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("update users set name = 'ally' where id = 7 returning id, email")
	if err != nil {
		t.Fatalf("plan update returning: %v", err)
	}
	updatePlan, ok := plan.(UpdatePlan)
	if !ok {
		t.Fatalf("plan type = %T, want UpdatePlan", plan)
	}
	if len(updatePlan.Returning) != 2 || updatePlan.Returning[0].Name != "id" || updatePlan.Returning[1].Name != "email" {
		t.Fatalf("returning = %+v, want [id email]", updatePlan.Returning)
	}
}

func TestPlannerPointDelete(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("delete from users where id = 7")
	if err != nil {
		t.Fatalf("plan delete: %v", err)
	}
	deletePlan, ok := plan.(DeletePlan)
	if !ok {
		t.Fatalf("plan type = %T, want DeletePlan", plan)
	}
	if !deletePlan.Singleton {
		t.Fatalf("delete singleton = false, want true")
	}
	if !bytes.Equal(deletePlan.Input.StartKey, storage.GlobalTablePrimaryKey(7, encodedIntKey(7))) {
		t.Fatalf("delete start key = %q, want point key", deletePlan.Input.StartKey)
	}
	if !bytes.Equal(deletePlan.Input.EndKey, storage.PrefixEnd(storage.GlobalTablePrimaryKey(7, encodedIntKey(7)))) {
		t.Fatalf("delete end key = %q, want point prefix end", deletePlan.Input.EndKey)
	}
}

func TestPlannerBoundedRangeDelete(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("delete from users where id >= 10 and id < 20")
	if err != nil {
		t.Fatalf("plan delete: %v", err)
	}
	deletePlan, ok := plan.(DeletePlan)
	if !ok {
		t.Fatalf("plan type = %T, want DeletePlan", plan)
	}
	if deletePlan.Singleton {
		t.Fatalf("delete singleton = true, want false")
	}
	if !bytes.Equal(deletePlan.Input.StartKey, storage.GlobalTablePrimaryKey(7, encodedIntKey(10))) {
		t.Fatalf("delete start key = %q, want bounded start", deletePlan.Input.StartKey)
	}
	if !bytes.Equal(deletePlan.Input.EndKey, storage.GlobalTablePrimaryKey(7, encodedIntKey(20))) {
		t.Fatalf("delete end key = %q, want bounded end", deletePlan.Input.EndKey)
	}
}

func TestPlannerPointUpdate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("update users set name = 'ally', email = 'ally@example.com' where id = 7")
	if err != nil {
		t.Fatalf("plan update: %v", err)
	}
	updatePlan, ok := plan.(UpdatePlan)
	if !ok {
		t.Fatalf("plan type = %T, want UpdatePlan", plan)
	}
	if !updatePlan.Singleton {
		t.Fatalf("update singleton = false, want true")
	}
	if len(updatePlan.Assignments) != 2 {
		t.Fatalf("assignment count = %d, want 2", len(updatePlan.Assignments))
	}
	if !bytes.Equal(updatePlan.Input.StartKey, storage.GlobalTablePrimaryKey(7, encodedIntKey(7))) {
		t.Fatalf("update start key = %q, want point key", updatePlan.Input.StartKey)
	}
}

func TestPlannerBoundedRangeUpdate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("update users set email = 'group@example.com' where id >= 10 and id < 20")
	if err != nil {
		t.Fatalf("plan update: %v", err)
	}
	updatePlan, ok := plan.(UpdatePlan)
	if !ok {
		t.Fatalf("plan type = %T, want UpdatePlan", plan)
	}
	if updatePlan.Singleton {
		t.Fatalf("update singleton = true, want false")
	}
	if len(updatePlan.Assignments) != 1 || updatePlan.Assignments[0].Column.Name != "email" {
		t.Fatalf("assignments = %+v, want one email assignment", updatePlan.Assignments)
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

func TestPlannerHashJoinSelect(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select u.name, o.sales from users u join orders o on u.id = o.user_id")
	if err != nil {
		t.Fatalf("plan join select: %v", err)
	}
	join, ok := plan.(HashJoinPlan)
	if !ok {
		t.Fatalf("plan type = %T, want HashJoinPlan", plan)
	}
	if join.Left.Alias != "u" || join.Right.Alias != "o" {
		t.Fatalf("join aliases = (%q,%q), want (u,o)", join.Left.Alias, join.Right.Alias)
	}
	if join.Join.Type != "inner" {
		t.Fatalf("join type = %q, want inner", join.Join.Type)
	}
	if len(join.Join.LeftKeys) != 1 || join.Join.LeftKeys[0] != "u.id" {
		t.Fatalf("left join keys = %+v, want [u.id]", join.Join.LeftKeys)
	}
	if len(join.Join.RightKeys) != 1 || join.Join.RightKeys[0] != "o.user_id" {
		t.Fatalf("right join keys = %+v, want [o.user_id]", join.Join.RightKeys)
	}
	if len(join.LeftScan.Projection) != 2 {
		t.Fatalf("left projection count = %d, want 2", len(join.LeftScan.Projection))
	}
	if len(join.RightScan.Projection) != 2 {
		t.Fatalf("right projection count = %d, want 2", len(join.RightScan.Projection))
	}
	if len(join.Projection) != 2 {
		t.Fatalf("join projection count = %d, want 2", len(join.Projection))
	}
	if join.Projection[0].Output.Name != "name" || join.Projection[1].Output.Name != "sales" {
		t.Fatalf("join outputs = %+v, want [name sales]", join.Projection)
	}
}

func TestPlannerRejectsNonPrimaryKeyPredicateForMutableStatements(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select id from users where name = 'alice'")
	if err != nil {
		t.Fatalf("select with non-primary-key predicate should plan: %v", err)
	}
	if _, ok := plan.(RangeScanPlan); !ok {
		t.Fatalf("select plan type = %T, want RangeScanPlan", plan)
	}
	if _, err := planner.Plan("delete from users where name = 'alice'"); err == nil {
		t.Fatalf("expected planner error for non-primary-key delete predicate")
	}
	if _, err := planner.Plan("update users set email = 'alice@example.com' where name = 'alice'"); err == nil {
		t.Fatalf("expected planner error for non-primary-key update predicate")
	}
}

func TestPlannerRejectsUnboundedDelete(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	if _, err := planner.Plan("delete from users"); err == nil {
		t.Fatalf("expected planner error for unbounded delete")
	}
	if _, err := planner.Plan("delete from users where id >= 10"); err == nil {
		t.Fatalf("expected planner error for half-bounded delete")
	}
}

func TestPlannerRejectsUnsupportedUpdate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	if _, err := planner.Plan("update users set id = 8 where id = 7"); err == nil {
		t.Fatalf("expected planner error for primary-key update")
	}
	if _, err := planner.Plan("update users set name = name where id = 7"); err == nil {
		t.Fatalf("expected planner error for non-literal update value")
	}
	if _, err := planner.Plan("update users set name = 'ally'"); err == nil {
		t.Fatalf("expected planner error for unbounded update")
	}
}

func TestPlannerRejectsUngroupedAggregateProjection(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	if _, err := planner.Plan("select email, count(*) from users group by name"); err == nil {
		t.Fatalf("expected planner error for ungrouped aggregate projection")
	}
}

func TestPlannerRejectsUnsupportedJoinPredicate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	if _, err := planner.Plan("select u.name, o.sales from users u join orders o on u.id > o.user_id"); err == nil {
		t.Fatalf("expected planner error for non-equi join")
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

func TestPlannerOptimizePointDeleteUsesPointCandidate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	optimized, err := planner.Optimize("delete from users where id = 7")
	if err != nil {
		t.Fatalf("optimize delete: %v", err)
	}
	if len(optimized.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(optimized.Candidates))
	}
	if optimized.Selected.Name != "point_delete" {
		t.Fatalf("selected candidate = %q, want point_delete", optimized.Selected.Name)
	}
	if _, ok := optimized.Selected.Plan.(DeletePlan); !ok {
		t.Fatalf("selected plan type = %T, want DeletePlan", optimized.Selected.Plan)
	}
}

func TestPlannerOptimizePointUpdateUsesPointCandidate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	optimized, err := planner.Optimize("update users set name = 'ally' where id = 7")
	if err != nil {
		t.Fatalf("optimize update: %v", err)
	}
	if len(optimized.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(optimized.Candidates))
	}
	if optimized.Selected.Name != "point_update" {
		t.Fatalf("selected candidate = %q, want point_update", optimized.Selected.Name)
	}
	if _, ok := optimized.Selected.Plan.(UpdatePlan); !ok {
		t.Fatalf("selected plan type = %T, want UpdatePlan", optimized.Selected.Plan)
	}
}

func TestPlannerOptimizeUpsertUsesUpsertCandidate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	optimized, err := planner.Optimize("upsert into users (id, name, email) values (1, 'alice', 'a@example.com')")
	if err != nil {
		t.Fatalf("optimize upsert: %v", err)
	}
	if len(optimized.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(optimized.Candidates))
	}
	if optimized.Selected.Name != "upsert_put" {
		t.Fatalf("selected candidate = %q, want upsert_put", optimized.Selected.Name)
	}
	if _, ok := optimized.Selected.Plan.(UpsertPlan); !ok {
		t.Fatalf("selected plan type = %T, want UpsertPlan", optimized.Selected.Plan)
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
		Indexes: []IndexDescriptor{
			{ID: 1, Name: "users_name_idx", Columns: []string{"name"}},
			{ID: 2, Name: "users_email_key", Columns: []string{"email"}, Unique: true},
		},
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
			{ID: 2, Name: "user_id", Type: ColumnTypeInt},
			{ID: 3, Name: "region", Type: ColumnTypeString},
			{ID: 4, Name: "sales", Type: ColumnTypeInt},
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
