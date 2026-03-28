package sql

import (
	"fmt"
	"strings"
)

// ColumnType is the logical SQL type supported by the current front-door slice.
type ColumnType string

const (
	ColumnTypeInt    ColumnType = "INT"
	ColumnTypeString ColumnType = "STRING"
	ColumnTypeBytes  ColumnType = "BYTES"
)

// ColumnDescriptor describes one SQL column.
type ColumnDescriptor struct {
	ID       uint32
	Name     string
	Type     ColumnType
	Nullable bool
}

// TableStats carries coarse planning statistics for the optimizer.
type TableStats struct {
	EstimatedRows   uint64
	AverageRowBytes uint64
}

// TableDescriptor describes one SQL table descriptor.
type TableDescriptor struct {
	ID         uint64
	Name       string
	Columns    []ColumnDescriptor
	PrimaryKey []string
	Stats      TableStats
}

// Catalog stores SQL table descriptors for the binder and planner.
type Catalog struct {
	tables map[string]TableDescriptor
}

// NewCatalog constructs an empty descriptor catalog.
func NewCatalog() *Catalog {
	return &Catalog{tables: make(map[string]TableDescriptor)}
}

// AddTable installs one validated descriptor.
func (c *Catalog) AddTable(desc TableDescriptor) error {
	if err := desc.Validate(); err != nil {
		return err
	}
	name := canonicalName(desc.Name)
	if _, exists := c.tables[name]; exists {
		return fmt.Errorf("sql catalog: table %q already exists", desc.Name)
	}
	c.tables[name] = desc
	return nil
}

// ResolveTable resolves one table descriptor by name.
func (c *Catalog) ResolveTable(name string) (TableDescriptor, error) {
	desc, ok := c.tables[canonicalName(name)]
	if !ok {
		return TableDescriptor{}, fmt.Errorf("sql catalog: unknown table %q", name)
	}
	return desc, nil
}

// Validate checks the table descriptor for obvious corruption.
func (t TableDescriptor) Validate() error {
	if t.ID == 0 {
		return fmt.Errorf("table descriptor: table id must be non-zero")
	}
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("table descriptor: table name must be non-empty")
	}
	if len(t.Columns) == 0 {
		return fmt.Errorf("table descriptor: columns must not be empty")
	}
	seen := make(map[string]struct{}, len(t.Columns))
	for _, column := range t.Columns {
		if column.ID == 0 {
			return fmt.Errorf("table descriptor: column id must be non-zero")
		}
		if strings.TrimSpace(column.Name) == "" {
			return fmt.Errorf("table descriptor: column name must be non-empty")
		}
		switch column.Type {
		case ColumnTypeInt, ColumnTypeString, ColumnTypeBytes:
		default:
			return fmt.Errorf("table descriptor: unsupported column type %q", column.Type)
		}
		name := canonicalName(column.Name)
		if _, exists := seen[name]; exists {
			return fmt.Errorf("table descriptor: duplicate column %q", column.Name)
		}
		seen[name] = struct{}{}
	}
	if len(t.PrimaryKey) == 0 {
		return fmt.Errorf("table descriptor: primary key must not be empty")
	}
	for _, pk := range t.PrimaryKey {
		if _, ok := seen[canonicalName(pk)]; !ok {
			return fmt.Errorf("table descriptor: primary key column %q not found", pk)
		}
	}
	return nil
}

// ColumnByName resolves one named column.
func (t TableDescriptor) ColumnByName(name string) (ColumnDescriptor, bool) {
	for _, column := range t.Columns {
		if canonicalName(column.Name) == canonicalName(name) {
			return column, true
		}
	}
	return ColumnDescriptor{}, false
}

// PrimaryKeyColumn resolves the supported single-column primary key descriptor.
func (t TableDescriptor) PrimaryKeyColumn() (ColumnDescriptor, error) {
	if len(t.PrimaryKey) != 1 {
		return ColumnDescriptor{}, fmt.Errorf("table descriptor: only single-column primary keys are supported in the current SQL slice")
	}
	column, ok := t.ColumnByName(t.PrimaryKey[0])
	if !ok {
		return ColumnDescriptor{}, fmt.Errorf("table descriptor: primary key column %q not found", t.PrimaryKey[0])
	}
	return column, nil
}

// StatsOrDefaults returns usable planning stats even before the catalog is fully populated.
func (t TableDescriptor) StatsOrDefaults() TableStats {
	stats := t.Stats
	if stats.EstimatedRows == 0 {
		stats.EstimatedRows = 1000
	}
	if stats.AverageRowBytes == 0 {
		stats.AverageRowBytes = 256
	}
	return stats
}

func canonicalName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
