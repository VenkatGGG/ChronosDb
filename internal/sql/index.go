package sql

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

// IndexEntry describes one concrete secondary-index KV entry derived from a row.
type IndexEntry struct {
	Index IndexDescriptor
	Key   []byte
	Value []byte
}

// EncodePrimaryKeyBytes returns the encoded primary-key bytes used in logical
// primary-row and secondary-index keys.
func EncodePrimaryKeyBytes(table TableDescriptor, row map[string]Value) ([]byte, error) {
	pkColumn, err := table.PrimaryKeyColumn()
	if err != nil {
		return nil, err
	}
	pkValue, ok := row[canonicalName(pkColumn.Name)]
	if !ok {
		return nil, fmt.Errorf("sql index: primary key column %q is missing", pkColumn.Name)
	}
	return encodePrimaryKeyValue(pkValue)
}

// BuildIndexEntries derives all secondary-index entries currently visible for a
// row. Any index whose columns include a missing/NULL value is omitted.
func BuildIndexEntries(table TableDescriptor, row map[string]Value) ([]IndexEntry, error) {
	if len(table.Indexes) == 0 || row == nil {
		return nil, nil
	}
	encodedPrimaryKey, err := EncodePrimaryKeyBytes(table, row)
	if err != nil {
		return nil, err
	}
	entries := make([]IndexEntry, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		encodedIndexKey, present, err := encodeIndexKey(table, index, row)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		entry := IndexEntry{
			Index: index,
			Value: append([]byte(nil), encodedPrimaryKey...),
		}
		if index.Unique {
			entry.Key = storage.GlobalTableUniqueIndexKey(table.ID, index.ID, encodedIndexKey)
		} else {
			entry.Key = storage.GlobalTableSecondaryIndexKey(table.ID, index.ID, encodedIndexKey, encodedPrimaryKey)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func encodeIndexKey(table TableDescriptor, index IndexDescriptor, row map[string]Value) ([]byte, bool, error) {
	var encoded []byte
	for _, columnName := range index.Columns {
		column, ok := table.ColumnByName(columnName)
		if !ok {
			return nil, false, fmt.Errorf("sql index: unknown column %q for index %q", columnName, index.Name)
		}
		value, ok := row[canonicalName(column.Name)]
		if !ok {
			return nil, false, nil
		}
		scalar, err := encodeIndexScalar(column, value)
		if err != nil {
			return nil, false, err
		}
		encoded = append(encoded, indexTypeMarker(column.Type))
		encoded = appendIndexEscapedBytes(encoded, scalar)
	}
	return encoded, true, nil
}

func encodeIndexScalar(column ColumnDescriptor, value Value) ([]byte, error) {
	if value.Type != column.Type {
		return nil, fmt.Errorf("sql index: column %q encoded as %q, want %q", column.Name, value.Type, column.Type)
	}
	switch value.Type {
	case ColumnTypeInt:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(value.Int64)^(uint64(1)<<63))
		return buf[:], nil
	case ColumnTypeString:
		return []byte(value.String), nil
	case ColumnTypeBytes:
		return bytes.Clone(value.Bytes), nil
	default:
		return nil, fmt.Errorf("sql index: unsupported column type %q", value.Type)
	}
}

func indexTypeMarker(columnType ColumnType) byte {
	switch columnType {
	case ColumnTypeInt:
		return 0x01
	case ColumnTypeString:
		return 0x02
	case ColumnTypeBytes:
		return 0x03
	default:
		return 0xff
	}
}

func appendIndexEscapedBytes(dst, raw []byte) []byte {
	for _, b := range raw {
		if b == 0x00 {
			dst = append(dst, 0x00, 0xff)
			continue
		}
		dst = append(dst, b)
	}
	return append(dst, 0x00, 0x01)
}
