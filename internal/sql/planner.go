package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
	vsqlparser "vitess.io/vitess/go/vt/sqlparser"
)

// Planner performs parser-backed SQL binding and logical-to-KV mapping.
type Planner struct {
	parser    *Parser
	catalog   *Catalog
	optimizer *Optimizer
}

// NewPlanner constructs a planner backed by the parser and catalog.
func NewPlanner(parser *Parser, catalog *Catalog) *Planner {
	return &Planner{
		parser:    parser,
		catalog:   catalog,
		optimizer: NewOptimizer(),
	}
}

// Plan parses, binds, and maps one SQL statement onto the KV substrate.
func (p *Planner) Plan(query string) (Plan, error) {
	optimized, err := p.Optimize(query)
	if err != nil {
		return nil, err
	}
	return optimized.Selected.Plan, nil
}

// Optimize parses, binds, and cost-ranks one SQL statement.
func (p *Planner) Optimize(query string) (OptimizedPlan, error) {
	stmt, err := p.parser.Parse(query)
	if err != nil {
		return OptimizedPlan{}, err
	}
	switch typed := stmt.(type) {
	case *vsqlparser.Select:
		return p.optimizeSelect(typed)
	case *vsqlparser.Insert:
		return p.optimizeInsert(typed)
	default:
		return OptimizedPlan{}, fmt.Errorf("sql planner: unsupported statement type %T", stmt)
	}
}

// Plan is one logical SQL plan mapped onto the KV substrate.
type Plan interface {
	isPlan()
}

// PointLookupPlan maps a primary-key point read to one KV key.
type PointLookupPlan struct {
	Table      TableDescriptor
	Projection []ColumnDescriptor
	Key        []byte
}

// RangeScanPlan maps a primary-key scan to one KV span.
type RangeScanPlan struct {
	Table          TableDescriptor
	Projection     []ColumnDescriptor
	StartKey       []byte
	EndKey         []byte
	StartInclusive bool
	EndInclusive   bool
}

// InsertPlan maps one SQL insert to one KV put.
type InsertPlan struct {
	Table TableDescriptor
	Key   []byte
	Value []byte
}

func (PointLookupPlan) isPlan() {}
func (RangeScanPlan) isPlan()   {}
func (InsertPlan) isPlan()      {}

type boundPredicate struct {
	equality *Value
	lower    *predicateBound
	upper    *predicateBound
}

type predicateBound struct {
	value     Value
	inclusive bool
}

// Value is a typed literal bound against a column descriptor.
type Value struct {
	Type   ColumnType
	Int64  int64
	String string
	Bytes  []byte
}

func (p *Planner) optimizeSelect(stmt *vsqlparser.Select) (OptimizedPlan, error) {
	if stmt.Distinct || stmt.GroupBy != nil || stmt.Having != nil || stmt.Limit != nil || len(stmt.OrderBy) > 0 {
		return OptimizedPlan{}, fmt.Errorf("sql planner: unsupported SELECT shape")
	}
	table, err := p.resolveSingleTable(stmt.From)
	if err != nil {
		return OptimizedPlan{}, err
	}
	projection, err := bindProjection(table, stmt.SelectExprs)
	if err != nil {
		return OptimizedPlan{}, err
	}
	predicate, err := bindPrimaryKeyPredicate(table, stmt.Where)
	if err != nil {
		return OptimizedPlan{}, err
	}
	candidates, err := makeSelectCandidates(p.optimizer, table, projection, predicate)
	if err != nil {
		return OptimizedPlan{}, err
	}
	return p.optimizer.Choose(candidates)
}

func (p *Planner) optimizeInsert(stmt *vsqlparser.Insert) (OptimizedPlan, error) {
	if stmt.Ignore || len(stmt.OnDup) > 0 || stmt.Rows == nil {
		return OptimizedPlan{}, fmt.Errorf("sql planner: unsupported INSERT shape")
	}
	tableName, err := stmt.Table.TableName()
	if err != nil {
		return OptimizedPlan{}, err
	}
	table, err := p.catalog.ResolveTable(tableName.Name.String())
	if err != nil {
		return OptimizedPlan{}, err
	}
	values, ok := stmt.Rows.(vsqlparser.Values)
	if !ok {
		return OptimizedPlan{}, fmt.Errorf("sql planner: only VALUES inserts are supported")
	}
	if len(values) != 1 {
		return OptimizedPlan{}, fmt.Errorf("sql planner: only single-row inserts are supported")
	}
	row, err := bindInsertRow(table, stmt.Columns, values[0])
	if err != nil {
		return OptimizedPlan{}, err
	}
	key, value, err := encodeInsert(table, row)
	if err != nil {
		return OptimizedPlan{}, err
	}
	plan := InsertPlan{
		Table: table,
		Key:   key,
		Value: value,
	}
	return p.optimizer.Choose([]PlanCandidate{makeInsertCandidate(p.optimizer, table, plan)})
}

func (p *Planner) resolveSingleTable(from []vsqlparser.TableExpr) (TableDescriptor, error) {
	if len(from) != 1 {
		return TableDescriptor{}, fmt.Errorf("sql planner: only single-table statements are supported")
	}
	aliased, ok := from[0].(*vsqlparser.AliasedTableExpr)
	if !ok {
		return TableDescriptor{}, fmt.Errorf("sql planner: unsupported table expression %T", from[0])
	}
	tableName, err := aliased.TableName()
	if err != nil {
		return TableDescriptor{}, err
	}
	return p.catalog.ResolveTable(tableName.Name.String())
}

func bindProjection(table TableDescriptor, exprs *vsqlparser.SelectExprs) ([]ColumnDescriptor, error) {
	if exprs == nil || len(exprs.Exprs) == 0 {
		return nil, fmt.Errorf("sql planner: SELECT list must not be empty")
	}
	if len(exprs.Exprs) == 1 {
		if _, ok := exprs.Exprs[0].(*vsqlparser.StarExpr); ok {
			return append([]ColumnDescriptor(nil), table.Columns...), nil
		}
	}
	projection := make([]ColumnDescriptor, 0, len(exprs.Exprs))
	for _, expr := range exprs.Exprs {
		aliased, ok := expr.(*vsqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("sql planner: unsupported projection expression %T", expr)
		}
		col, ok := aliased.Expr.(*vsqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("sql planner: only column projections are supported")
		}
		descriptor, exists := table.ColumnByName(col.Name.String())
		if !exists {
			return nil, fmt.Errorf("sql planner: unknown column %q", col.Name.String())
		}
		projection = append(projection, descriptor)
	}
	return projection, nil
}

func bindPrimaryKeyPredicate(table TableDescriptor, where *vsqlparser.Where) (boundPredicate, error) {
	if where == nil || where.Expr == nil {
		return boundPredicate{}, nil
	}
	pkColumn, err := table.PrimaryKeyColumn()
	if err != nil {
		return boundPredicate{}, err
	}
	comparisons, err := flattenComparisons(where.Expr)
	if err != nil {
		return boundPredicate{}, err
	}
	predicate := boundPredicate{}
	for _, cmp := range comparisons {
		normalized, value, err := normalizePrimaryKeyComparison(pkColumn, cmp)
		if err != nil {
			return boundPredicate{}, err
		}
		switch normalized {
		case vsqlparser.EqualOp:
			if predicate.equality != nil {
				return boundPredicate{}, fmt.Errorf("sql planner: duplicate primary-key equality predicate")
			}
			copyValue := value
			predicate.equality = &copyValue
		case vsqlparser.GreaterThanOp:
			bound := predicateBound{value: value, inclusive: false}
			predicate.lower = &bound
		case vsqlparser.GreaterEqualOp:
			bound := predicateBound{value: value, inclusive: true}
			predicate.lower = &bound
		case vsqlparser.LessThanOp:
			bound := predicateBound{value: value, inclusive: false}
			predicate.upper = &bound
		case vsqlparser.LessEqualOp:
			bound := predicateBound{value: value, inclusive: true}
			predicate.upper = &bound
		default:
			return boundPredicate{}, fmt.Errorf("sql planner: unsupported comparison operator %s", normalized.ToString())
		}
	}
	return predicate, nil
}

func flattenComparisons(expr vsqlparser.Expr) ([]*vsqlparser.ComparisonExpr, error) {
	switch typed := expr.(type) {
	case *vsqlparser.AndExpr:
		left, err := flattenComparisons(typed.Left)
		if err != nil {
			return nil, err
		}
		right, err := flattenComparisons(typed.Right)
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil
	case *vsqlparser.ComparisonExpr:
		return []*vsqlparser.ComparisonExpr{typed}, nil
	default:
		return nil, fmt.Errorf("sql planner: unsupported WHERE expression %T", expr)
	}
}

func normalizePrimaryKeyComparison(pk ColumnDescriptor, cmp *vsqlparser.ComparisonExpr) (vsqlparser.ComparisonExprOperator, Value, error) {
	leftCol, leftIsCol := cmp.Left.(*vsqlparser.ColName)
	rightCol, rightIsCol := cmp.Right.(*vsqlparser.ColName)
	leftLiteral, leftIsLiteral := cmp.Left.(*vsqlparser.Literal)
	rightLiteral, rightIsLiteral := cmp.Right.(*vsqlparser.Literal)

	switch {
	case leftIsCol && rightIsLiteral:
		if canonicalName(leftCol.Name.String()) != canonicalName(pk.Name) {
			return 0, Value{}, fmt.Errorf("sql planner: WHERE predicates must target primary key %q", pk.Name)
		}
		value, err := bindLiteral(pk.Type, rightLiteral)
		return cmp.Operator, value, err
	case rightIsCol && leftIsLiteral:
		if canonicalName(rightCol.Name.String()) != canonicalName(pk.Name) {
			return 0, Value{}, fmt.Errorf("sql planner: WHERE predicates must target primary key %q", pk.Name)
		}
		operator, ok := cmp.Operator.SwitchSides()
		if !ok {
			return 0, Value{}, fmt.Errorf("sql planner: unsupported comparison operator %s", cmp.Operator.ToString())
		}
		value, err := bindLiteral(pk.Type, leftLiteral)
		return operator, value, err
	default:
		return 0, Value{}, fmt.Errorf("sql planner: WHERE predicates must compare the primary key against a literal")
	}
}

func bindInsertRow(table TableDescriptor, columns vsqlparser.Columns, tuple vsqlparser.ValTuple) (map[string]Value, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("sql planner: INSERT column list is required")
	}
	if len(columns) != len(tuple) {
		return nil, fmt.Errorf("sql planner: INSERT column/value count mismatch")
	}
	row := make(map[string]Value, len(columns))
	for i, identifier := range columns {
		column, ok := table.ColumnByName(identifier.String())
		if !ok {
			return nil, fmt.Errorf("sql planner: unknown column %q", identifier.String())
		}
		literal, ok := tuple[i].(*vsqlparser.Literal)
		if !ok {
			return nil, fmt.Errorf("sql planner: only literal INSERT values are supported")
		}
		value, err := bindLiteral(column.Type, literal)
		if err != nil {
			return nil, err
		}
		row[canonicalName(column.Name)] = value
	}
	pkColumn, err := table.PrimaryKeyColumn()
	if err != nil {
		return nil, err
	}
	if _, ok := row[canonicalName(pkColumn.Name)]; !ok {
		return nil, fmt.Errorf("sql planner: primary key column %q is required", pkColumn.Name)
	}
	return row, nil
}

func encodeInsert(table TableDescriptor, row map[string]Value) ([]byte, []byte, error) {
	pkColumn, err := table.PrimaryKeyColumn()
	if err != nil {
		return nil, nil, err
	}
	pkValue := row[canonicalName(pkColumn.Name)]
	encodedPK, err := encodePrimaryKeyValue(pkValue)
	if err != nil {
		return nil, nil, err
	}
	payload, err := encodeRowValue(table, row)
	if err != nil {
		return nil, nil, err
	}
	return storage.GlobalTablePrimaryKey(table.ID, encodedPK), payload, nil
}

func bindLiteral(columnType ColumnType, literal *vsqlparser.Literal) (Value, error) {
	switch columnType {
	case ColumnTypeInt:
		if literal.Type != vsqlparser.IntVal {
			return Value{}, fmt.Errorf("sql planner: expected INT literal, got %q", literal.Val)
		}
		value, err := strconv.ParseInt(literal.Val, 10, 64)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: ColumnTypeInt, Int64: value}, nil
	case ColumnTypeString:
		if literal.Type != vsqlparser.StrVal {
			return Value{}, fmt.Errorf("sql planner: expected STRING literal, got %q", literal.Val)
		}
		return Value{Type: ColumnTypeString, String: literal.Val}, nil
	case ColumnTypeBytes:
		switch literal.Type {
		case vsqlparser.StrVal:
			return Value{Type: ColumnTypeBytes, Bytes: []byte(literal.Val)}, nil
		case vsqlparser.HexVal:
			decoded, err := literal.HexDecode()
			if err != nil {
				return Value{}, err
			}
			return Value{Type: ColumnTypeBytes, Bytes: decoded}, nil
		default:
			return Value{}, fmt.Errorf("sql planner: expected BYTES literal, got %q", literal.Val)
		}
	default:
		return Value{}, fmt.Errorf("sql planner: unsupported column type %q", columnType)
	}
}

func encodePrimaryKeyValue(value Value) ([]byte, error) {
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
		return nil, fmt.Errorf("sql planner: unsupported primary key type %q", value.Type)
	}
}

func encodeRowValue(table TableDescriptor, row map[string]Value) ([]byte, error) {
	type encodedValue struct {
		Column string     `json:"column"`
		Type   ColumnType `json:"type"`
		Int64  *int64     `json:"int64,omitempty"`
		String *string    `json:"string,omitempty"`
		Bytes  []byte     `json:"bytes,omitempty"`
	}
	payload := make([]encodedValue, 0, len(table.Columns))
	for _, column := range table.Columns {
		value, ok := row[canonicalName(column.Name)]
		if !ok {
			continue
		}
		item := encodedValue{
			Column: column.Name,
			Type:   value.Type,
		}
		switch value.Type {
		case ColumnTypeInt:
			item.Int64 = &value.Int64
		case ColumnTypeString:
			item.String = &value.String
		case ColumnTypeBytes:
			item.Bytes = append([]byte(nil), value.Bytes...)
		}
		payload = append(payload, item)
	}
	return json.Marshal(payload)
}
