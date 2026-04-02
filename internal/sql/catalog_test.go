package sql

import (
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/placement"
)

func TestCatalogAddResolveAndPrimaryKeyColumn(t *testing.T) {
	t.Parallel()

	catalog := NewCatalog()
	table := TableDescriptor{
		ID:   7,
		Name: "users",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: ColumnTypeInt},
			{ID: 2, Name: "name", Type: ColumnTypeString},
		},
		PrimaryKey: []string{"id"},
	}
	if err := catalog.AddTable(table); err != nil {
		t.Fatalf("add table: %v", err)
	}
	resolved, err := catalog.ResolveTable("Users")
	if err != nil {
		t.Fatalf("resolve table: %v", err)
	}
	if resolved.ID != table.ID {
		t.Fatalf("resolved table id = %d, want %d", resolved.ID, table.ID)
	}
	pk, err := resolved.PrimaryKeyColumn()
	if err != nil {
		t.Fatalf("primary key column: %v", err)
	}
	if pk.Name != "id" {
		t.Fatalf("primary key column = %q, want id", pk.Name)
	}
}

func TestCatalogValidatesPlacementPolicy(t *testing.T) {
	t.Parallel()

	catalog := NewCatalog()
	if err := catalog.AddTable(TableDescriptor{
		ID:   9,
		Name: "orders",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: ColumnTypeInt},
		},
		PrimaryKey: []string{"id"},
		PlacementPolicy: &placement.Policy{
			PlacementMode:    placement.ModeHomeRegion,
			HomeRegion:       "us-east1",
			PreferredRegions: []string{"us-east1", "us-west1", "europe-west1"},
		},
	}); err != nil {
		t.Fatalf("add table with placement: %v", err)
	}

	if err := catalog.AddTable(TableDescriptor{
		ID:   10,
		Name: "broken_orders",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: ColumnTypeInt},
		},
		PrimaryKey: []string{"id"},
		PlacementPolicy: &placement.Policy{
			PlacementMode:    placement.ModeGlobal,
			PreferredRegions: []string{"us-east1", "us-west1"},
		},
	}); err == nil {
		t.Fatalf("expected invalid placement policy to be rejected")
	}
}

func TestCatalogValidatesSecondaryIndexes(t *testing.T) {
	t.Parallel()

	catalog := NewCatalog()
	table := TableDescriptor{
		ID:   11,
		Name: "users",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: ColumnTypeInt},
			{ID: 2, Name: "name", Type: ColumnTypeString},
			{ID: 3, Name: "email", Type: ColumnTypeString},
		},
		PrimaryKey: []string{"id"},
		Indexes: []IndexDescriptor{
			{ID: 1, Name: "users_name_idx", Columns: []string{"name"}},
			{ID: 2, Name: "users_email_key", Columns: []string{"email"}, Unique: true},
		},
	}
	if err := catalog.AddTable(table); err != nil {
		t.Fatalf("add indexed table: %v", err)
	}
	resolved, err := catalog.ResolveTable("users")
	if err != nil {
		t.Fatalf("resolve indexed table: %v", err)
	}
	index, ok := resolved.IndexByName("users_email_key")
	if !ok || !index.Unique {
		t.Fatalf("email index = %+v, want unique users_email_key", index)
	}
	if _, ok := resolved.IndexByColumns([]string{"name"}); !ok {
		t.Fatalf("expected name index to resolve by columns")
	}
	if err := catalog.AddTable(TableDescriptor{
		ID:   12,
		Name: "broken_users",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: ColumnTypeInt},
			{ID: 2, Name: "email", Type: ColumnTypeString},
		},
		PrimaryKey: []string{"id"},
		Indexes: []IndexDescriptor{
			{ID: 1, Name: "broken_email_idx", Columns: []string{"missing"}},
		},
	}); err == nil {
		t.Fatalf("expected missing index column to be rejected")
	}
}
