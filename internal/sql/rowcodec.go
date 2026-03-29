package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type encodedRowValue struct {
	Column string     `json:"column"`
	Type   ColumnType `json:"type"`
	Int64  *int64     `json:"int64,omitempty"`
	String *string    `json:"string,omitempty"`
	Bytes  []byte     `json:"bytes,omitempty"`
}

// DecodeRowValue decodes one stored row payload into typed SQL values.
func DecodeRowValue(table TableDescriptor, payload []byte) (map[string]Value, error) {
	var encoded []encodedRowValue
	if err := json.Unmarshal(payload, &encoded); err != nil {
		return nil, fmt.Errorf("sql row decode: %w", err)
	}
	row := make(map[string]Value, len(encoded))
	for _, item := range encoded {
		column, ok := table.ColumnByName(item.Column)
		if !ok {
			return nil, fmt.Errorf("sql row decode: unknown column %q for table %q", item.Column, table.Name)
		}
		value, err := decodeRowItem(column, item)
		if err != nil {
			return nil, err
		}
		row[canonicalName(column.Name)] = value
	}
	return row, nil
}

// ProjectRowText projects a typed row onto a wire/text result set.
func ProjectRowText(projection []ColumnDescriptor, row map[string]Value) ([][]byte, error) {
	out := make([][]byte, 0, len(projection))
	for _, column := range projection {
		value, ok := row[canonicalName(column.Name)]
		if !ok {
			if column.Nullable {
				out = append(out, nil)
				continue
			}
			return nil, fmt.Errorf("sql row project: missing non-nullable column %q", column.Name)
		}
		formatted, err := FormatValueText(value)
		if err != nil {
			return nil, err
		}
		out = append(out, formatted)
	}
	return out, nil
}

// FormatValueText renders one SQL value into PostgreSQL text format bytes.
func FormatValueText(value Value) ([]byte, error) {
	switch value.Type {
	case ColumnTypeInt:
		return []byte(strconv.FormatInt(value.Int64, 10)), nil
	case ColumnTypeString:
		return []byte(value.String), nil
	case ColumnTypeBytes:
		return append([]byte(nil), value.Bytes...), nil
	default:
		return nil, fmt.Errorf("sql row format: unsupported column type %q", value.Type)
	}
}

func decodeRowItem(column ColumnDescriptor, item encodedRowValue) (Value, error) {
	if item.Type != column.Type {
		return Value{}, fmt.Errorf("sql row decode: column %q encoded as %q, want %q", column.Name, item.Type, column.Type)
	}
	switch item.Type {
	case ColumnTypeInt:
		if item.Int64 == nil {
			return Value{}, fmt.Errorf("sql row decode: missing int payload for column %q", column.Name)
		}
		return Value{Type: ColumnTypeInt, Int64: *item.Int64}, nil
	case ColumnTypeString:
		if item.String == nil {
			return Value{}, fmt.Errorf("sql row decode: missing string payload for column %q", column.Name)
		}
		return Value{Type: ColumnTypeString, String: *item.String}, nil
	case ColumnTypeBytes:
		return Value{Type: ColumnTypeBytes, Bytes: append([]byte(nil), item.Bytes...)}, nil
	default:
		return Value{}, fmt.Errorf("sql row decode: unsupported column type %q", item.Type)
	}
}
