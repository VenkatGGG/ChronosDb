package sql

import "testing"

func TestPlannerPrepareInfersParameterTypes(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	prepared, err := planner.Prepare("update users set name = $1, email = $2 where id = $3 returning id, name")
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	if len(prepared.ParameterTypes) != 3 {
		t.Fatalf("parameter count = %d, want 3", len(prepared.ParameterTypes))
	}
	if prepared.ParameterTypes[0] != ColumnTypeString || prepared.ParameterTypes[1] != ColumnTypeString || prepared.ParameterTypes[2] != ColumnTypeInt {
		t.Fatalf("parameter types = %#v, want [STRING STRING INT]", prepared.ParameterTypes)
	}
}

func TestPlannerPrepareInfersSelectFilterAndLimitParameters(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	prepared, err := planner.Prepare("select id, name from users where name >= $1 order by name desc limit $2")
	if err != nil {
		t.Fatalf("prepare select query: %v", err)
	}
	if len(prepared.ParameterTypes) != 2 {
		t.Fatalf("parameter count = %d, want 2", len(prepared.ParameterTypes))
	}
	if prepared.ParameterTypes[0] != ColumnTypeString || prepared.ParameterTypes[1] != ColumnTypeInt {
		t.Fatalf("parameter types = %#v, want [STRING INT]", prepared.ParameterTypes)
	}
}

func TestPlannerPrepareInfersOnConflictParameters(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	prepared, err := planner.Prepare("insert into users (id, name, email) values ($1, $2, $3) on conflict (email) do update set name = $4, email = excluded.email returning id")
	if err != nil {
		t.Fatalf("prepare on conflict query: %v", err)
	}
	if len(prepared.ParameterTypes) != 4 {
		t.Fatalf("parameter count = %d, want 4", len(prepared.ParameterTypes))
	}
	if prepared.ParameterTypes[0] != ColumnTypeInt || prepared.ParameterTypes[1] != ColumnTypeString || prepared.ParameterTypes[2] != ColumnTypeString || prepared.ParameterTypes[3] != ColumnTypeString {
		t.Fatalf("parameter types = %#v, want [INT STRING STRING STRING]", prepared.ParameterTypes)
	}
}

func TestRenderPreparedQueryReplacesRepeatedParameters(t *testing.T) {
	t.Parallel()

	query, err := RenderPreparedQuery(
		"select id, name from users where id = $1 or id = $1",
		[]Value{{Type: ColumnTypeInt, Int64: 7}},
	)
	if err != nil {
		t.Fatalf("render prepared query: %v", err)
	}
	if query != "select id, name from users where id = 7 or id = 7" {
		t.Fatalf("rendered query = %q", query)
	}
}

func TestRenderPreparedQueryEscapesStrings(t *testing.T) {
	t.Parallel()

	query, err := RenderPreparedQuery(
		"insert into users (id, name, email) values ($1, $2, $3)",
		[]Value{
			{Type: ColumnTypeInt, Int64: 7},
			{Type: ColumnTypeString, String: "ali'ce"},
			{Type: ColumnTypeString, String: "a@example.com"},
		},
	)
	if err != nil {
		t.Fatalf("render prepared query: %v", err)
	}
	want := "insert into users (id, name, email) values (7, 'ali''ce', 'a@example.com')"
	if query != want {
		t.Fatalf("rendered query = %q, want %q", query, want)
	}
}
