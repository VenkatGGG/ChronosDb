package sql

import (
	"bytes"
	"testing"
)

func TestDecodeRowValueAndProjectText(t *testing.T) {
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
	}
	payload, err := encodeRowValue(table, map[string]Value{
		"id":    {Type: ColumnTypeInt, Int64: 7},
		"name":  {Type: ColumnTypeString, String: "alice"},
		"email": {Type: ColumnTypeString, String: "a@example.com"},
	})
	if err != nil {
		t.Fatalf("encode row value: %v", err)
	}

	row, err := DecodeRowValue(table, payload)
	if err != nil {
		t.Fatalf("decode row value: %v", err)
	}
	projected, err := ProjectRowText([]ColumnDescriptor{
		{ID: 1, Name: "id", Type: ColumnTypeInt},
		{ID: 2, Name: "name", Type: ColumnTypeString},
	}, row)
	if err != nil {
		t.Fatalf("project row text: %v", err)
	}
	if len(projected) != 2 {
		t.Fatalf("projected columns = %d, want 2", len(projected))
	}
	if !bytes.Equal(projected[0], []byte("7")) {
		t.Fatalf("projected id = %q, want %q", projected[0], []byte("7"))
	}
	if !bytes.Equal(projected[1], []byte("alice")) {
		t.Fatalf("projected name = %q, want %q", projected[1], []byte("alice"))
	}
}

func TestProjectRowTextPreservesNulls(t *testing.T) {
	t.Parallel()

	projected, err := ProjectRowText([]ColumnDescriptor{
		{ID: 1, Name: "email", Type: ColumnTypeString, Nullable: true},
	}, map[string]Value{})
	if err != nil {
		t.Fatalf("project row text: %v", err)
	}
	if len(projected) != 1 || projected[0] != nil {
		t.Fatalf("projected = %#v, want [nil]", projected)
	}
}
