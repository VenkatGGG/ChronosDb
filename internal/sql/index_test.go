package sql

import (
	"bytes"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

func TestBuildIndexEntriesBuildsUniqueAndNonUniqueKeys(t *testing.T) {
	t.Parallel()

	table := TableDescriptor{
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
	}
	row := map[string]Value{
		"id":    {Type: ColumnTypeInt, Int64: 7},
		"name":  {Type: ColumnTypeString, String: "alice"},
		"email": {Type: ColumnTypeString, String: "a@example.com"},
	}

	entries, err := BuildIndexEntries(table, row)
	if err != nil {
		t.Fatalf("build index entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entry count = %d, want 2", len(entries))
	}

	encodedPK, err := EncodePrimaryKeyBytes(table, row)
	if err != nil {
		t.Fatalf("encode primary key: %v", err)
	}
	byName := make(map[string]IndexEntry, len(entries))
	for _, entry := range entries {
		byName[entry.Index.Name] = entry
		if !bytes.Equal(entry.Value, encodedPK) {
			t.Fatalf("entry %q value = %v, want encoded primary key %v", entry.Index.Name, entry.Value, encodedPK)
		}
	}
	if got := byName["users_name_idx"].Key; !bytes.HasPrefix(got, storage.GlobalTableIndexPrefix(table.ID, 1)) {
		t.Fatalf("name index key = %q, want prefix %q", got, storage.GlobalTableIndexPrefix(table.ID, 1))
	}
	if got := byName["users_email_key"].Key; !bytes.HasPrefix(got, storage.GlobalTableIndexPrefix(table.ID, 2)) {
		t.Fatalf("email index key = %q, want prefix %q", got, storage.GlobalTableIndexPrefix(table.ID, 2))
	}
}

func TestBuildIndexEntriesSkipsMissingNullableColumn(t *testing.T) {
	t.Parallel()

	table := TableDescriptor{
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
	}
	row := map[string]Value{
		"id":   {Type: ColumnTypeInt, Int64: 7},
		"name": {Type: ColumnTypeString, String: "alice"},
	}

	entries, err := BuildIndexEntries(table, row)
	if err != nil {
		t.Fatalf("build index entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(entries))
	}
	if entries[0].Index.Name != "users_name_idx" {
		t.Fatalf("entry index = %q, want users_name_idx", entries[0].Index.Name)
	}
}
