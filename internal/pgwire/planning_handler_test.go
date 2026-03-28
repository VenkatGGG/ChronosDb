package pgwire

import (
	"context"
	"testing"

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
	result, err := handler.HandleSimpleQuery(context.Background(), "insert into users (id, name, email) values (1, 'alice', 'a@example.com')")
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

func TestPlanningHandlerMapsPlannerErrors(t *testing.T) {
	t.Parallel()

	handler := newPlanningHandler(t)
	_, err := handler.HandleSimpleQuery(context.Background(), "select id from users where name = 'alice'")
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
		Stats: chronossql.TableStats{
			EstimatedRows:   10000,
			AverageRowBytes: 192,
		},
	}); err != nil {
		t.Fatalf("add users descriptor: %v", err)
	}

	return NewPlanningHandler(
		chronossql.NewPlanner(chronossql.NewParser(), catalog),
		chronossql.NewFlowPlanner(),
	)
}
