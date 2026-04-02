package pgwire

import (
	"context"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/placement"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

func TestPlanningHandlerDescribeSelect(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	optimized, flow, result, err := handler.DescribeQuery("select id, name from users where id = 7")
	if err != nil {
		t.Fatalf("describe query: %v", err)
	}
	if optimized.Selected.Name != "point_lookup" {
		t.Fatalf("selected candidate = %q, want point_lookup", optimized.Selected.Name)
	}
	if flow.RootStageID != 1 {
		t.Fatalf("flow root = %d, want 1", flow.RootStageID)
	}
	if flow.RootFragmentID != 1 || len(flow.ResultSchema) != 2 {
		t.Fatalf("flow fragments/schema = root %d schema %+v", flow.RootFragmentID, flow.ResultSchema)
	}
	if len(result.Fields) != 2 {
		t.Fatalf("field count = %d, want 2", len(result.Fields))
	}
	if result.CommandTag != "SELECT 0" {
		t.Fatalf("command tag = %q, want SELECT 0", result.CommandTag)
	}
}

func TestPlanningHandlerDescribeInsert(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	result, err := handler.HandleSimpleQuery(context.Background(), NewSession(handler), "insert into users (id, name, email) values (1, 'alice', 'a@example.com')")
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if result.CommandTag != "INSERT 0 1" {
		t.Fatalf("command tag = %q, want INSERT 0 1", result.CommandTag)
	}
	if len(result.Fields) != 0 {
		t.Fatalf("insert should not expose row-description fields")
	}
}

func TestPlanningHandlerDescribeUpsert(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	result, err := handler.HandleSimpleQuery(context.Background(), NewSession(handler), "upsert into users (id, name, email) values (1, 'alice', 'a@example.com')")
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if result.CommandTag != "UPSERT 1" {
		t.Fatalf("command tag = %q, want UPSERT 1", result.CommandTag)
	}
	if len(result.Fields) != 0 {
		t.Fatalf("upsert should not expose row-description fields")
	}
}

func TestPlanningHandlerDescribeInsertOnConflictReturning(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	result, err := handler.HandleSimpleQuery(context.Background(), NewSession(handler), "insert into users (id, name, email) values (1, 'alice', 'a@example.com') on conflict (email) do update set name = excluded.name returning id, name")
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if result.CommandTag != "INSERT 0 1" {
		t.Fatalf("command tag = %q, want INSERT 0 1", result.CommandTag)
	}
	if len(result.Fields) != 2 || result.Fields[0].Name != "id" || result.Fields[1].Name != "name" {
		t.Fatalf("returning fields = %+v, want [id name]", result.Fields)
	}
}

func TestPlanningHandlerDescribeInsertReturning(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	result, err := handler.HandleSimpleQuery(context.Background(), NewSession(handler), "insert into users (id, name, email) values (1, 'alice', 'a@example.com') returning id, name")
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if result.CommandTag != "INSERT 0 1" {
		t.Fatalf("command tag = %q, want INSERT 0 1", result.CommandTag)
	}
	if len(result.Fields) != 2 || result.Fields[0].Name != "id" || result.Fields[1].Name != "name" {
		t.Fatalf("returning fields = %+v, want [id name]", result.Fields)
	}
}

func TestPlanningHandlerDescribeUpdateReturning(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	result, err := handler.HandleSimpleQuery(context.Background(), NewSession(handler), "update users set name = 'ally' where id = 7 returning id, name")
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if result.CommandTag != "UPDATE 0" {
		t.Fatalf("command tag = %q, want UPDATE 0", result.CommandTag)
	}
	if len(result.Fields) != 2 || result.Fields[0].Name != "id" || result.Fields[1].Name != "name" {
		t.Fatalf("returning fields = %+v, want [id name]", result.Fields)
	}
}

func TestPlanningHandlerDescribeDelete(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	result, err := handler.HandleSimpleQuery(context.Background(), NewSession(handler), "delete from users where id = 7")
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if result.CommandTag != "DELETE 0" {
		t.Fatalf("command tag = %q, want DELETE 0", result.CommandTag)
	}
	if len(result.Fields) != 0 {
		t.Fatalf("delete should not expose row-description fields")
	}
}

func TestPlanningHandlerDescribeUpdate(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	result, err := handler.HandleSimpleQuery(context.Background(), NewSession(handler), "update users set name = 'ally' where id = 7")
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if result.CommandTag != "UPDATE 0" {
		t.Fatalf("command tag = %q, want UPDATE 0", result.CommandTag)
	}
	if len(result.Fields) != 0 {
		t.Fatalf("update should not expose row-description fields")
	}
}

func TestPlanningHandlerDescribeAggregate(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	_, flow, result, err := handler.DescribeQuery("select name, count(*) from users group by name")
	if err != nil {
		t.Fatalf("describe aggregate query: %v", err)
	}
	if flow.RootFragmentID != 2 || len(flow.ResultSchema) != 2 {
		t.Fatalf("aggregate flow metadata = root %d schema %+v", flow.RootFragmentID, flow.ResultSchema)
	}
	if len(result.Fields) != 2 || result.Fields[0].Name != "name" || result.Fields[1].Name != "count" {
		t.Fatalf("aggregate fields = %+v, want [name count]", result.Fields)
	}
}

func TestPlanningHandlerDescribeJoin(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	optimized, flow, result, err := handler.DescribeQuery("select u.name, o.sales from users u join orders o on u.id = o.user_id")
	if err != nil {
		t.Fatalf("describe join query: %v", err)
	}
	if optimized.Selected.Name != "hash_join" {
		t.Fatalf("selected candidate = %q, want hash_join", optimized.Selected.Name)
	}
	if flow.RootFragmentID != 3 || len(flow.Fragments) != 3 {
		t.Fatalf("join flow metadata = root %d fragments %+v", flow.RootFragmentID, flow.Fragments)
	}
	if len(result.Fields) != 2 || result.Fields[0].Name != "name" || result.Fields[1].Name != "sales" {
		t.Fatalf("join fields = %+v, want [name sales]", result.Fields)
	}
}

func TestPlanningHandlerMapsPlannerErrors(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	_, err := handler.HandleSimpleQuery(context.Background(), NewSession(handler), "select distinct id from users")
	if err == nil {
		t.Fatalf("expected planner error")
	}
	wireErr, ok := err.(Error)
	if !ok {
		t.Fatalf("error type = %T, want pgwire.Error", err)
	}
	if wireErr.Code != "0A000" {
		t.Fatalf("error code = %q, want 0A000", wireErr.Code)
	}
}

func newPlanningHandler(t *testing.T) *PlanningHandler {
	t.Helper()

	catalog := chronossql.NewCatalog()
	if err := catalog.AddTable(chronossql.TableDescriptor{
		ID:   7,
		Name: "users",
		Columns: []chronossql.ColumnDescriptor{
			{ID: 1, Name: "id", Type: chronossql.ColumnTypeInt},
			{ID: 2, Name: "name", Type: chronossql.ColumnTypeString},
			{ID: 3, Name: "email", Type: chronossql.ColumnTypeString, Nullable: true},
		},
		PrimaryKey: []string{"id"},
		Indexes: []chronossql.IndexDescriptor{
			{ID: 1, Name: "users_name_idx", Columns: []string{"name"}},
			{ID: 2, Name: "users_email_key", Columns: []string{"email"}, Unique: true},
		},
		Stats: chronossql.TableStats{
			EstimatedRows:   10000,
			AverageRowBytes: 192,
		},
	}); err != nil {
		t.Fatalf("add users descriptor: %v", err)
	}
	if err := catalog.AddTable(chronossql.TableDescriptor{
		ID:   9,
		Name: "orders",
		Columns: []chronossql.ColumnDescriptor{
			{ID: 1, Name: "id", Type: chronossql.ColumnTypeInt},
			{ID: 2, Name: "user_id", Type: chronossql.ColumnTypeInt},
			{ID: 3, Name: "sales", Type: chronossql.ColumnTypeInt},
		},
		PrimaryKey: []string{"id"},
		Stats: chronossql.TableStats{
			EstimatedRows:   10000,
			AverageRowBytes: 96,
		},
		PlacementPolicy: &placement.Policy{
			PlacementMode:    placement.ModeRegional,
			PreferredRegions: []string{"us-east1"},
		},
	}); err != nil {
		t.Fatalf("add orders descriptor: %v", err)
	}

	return NewPlanningHandler(
		chronossql.NewPlanner(chronossql.NewParser(), catalog),
		chronossql.NewFlowPlanner(),
	)
}
