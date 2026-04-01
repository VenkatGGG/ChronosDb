package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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
	case *vsqlparser.Delete:
		return p.optimizeDelete(typed)
	case *vsqlparser.Update:
		return p.optimizeUpdate(typed)
	default:
		return OptimizedPlan{}, fmt.Errorf("sql planner: unsupported statement type %T", stmt)
	}
}

// Plan is one logical SQL plan mapped onto the KV substrate.
type Plan interface {
	isPlan()
}

// AggregateFunc names one supported SQL aggregation function.
type AggregateFunc string

const (
	AggregateFuncCount AggregateFunc = "count"
	AggregateFuncSum   AggregateFunc = "sum"
)

// AggregateExpr is one validated SQL aggregate bound against a table descriptor.
type AggregateExpr struct {
	Func  AggregateFunc
	Input *ColumnDescriptor
	Alias string
}

// String returns the normalized SQL label for the aggregate expression.
func (a AggregateExpr) String() string {
	switch {
	case a.Input == nil:
		return fmt.Sprintf("%s(*)", a.Func)
	default:
		return fmt.Sprintf("%s(%s)", a.Func, canonicalName(a.Input.Name))
	}
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

// DeletePlan maps one SQL delete onto one KV span of tombstone writes.
type DeletePlan struct {
	Table     TableDescriptor
	Input     RangeScanPlan
	Singleton bool
}

// UpdateAssignment binds one SET target onto one typed literal value.
type UpdateAssignment struct {
	Column ColumnDescriptor
	Value  Value
}

// UpdatePlan maps one SQL update onto a scan plus row rewrites.
type UpdatePlan struct {
	Table       TableDescriptor
	Input       RangeScanPlan
	Assignments []UpdateAssignment
	Singleton   bool
}

// AggregatePlan maps a scan onto distributed grouping/aggregation stages.
type AggregatePlan struct {
	Input      RangeScanPlan
	GroupBy    []ColumnDescriptor
	Aggregates []AggregateExpr
}

// BoundTable binds one SQL table descriptor to the alias visible in the statement.
type BoundTable struct {
	Alias string
	Table TableDescriptor
}

// JoinProjection describes one output column sourced from one side of the join.
type JoinProjection struct {
	Output       ColumnDescriptor
	SourceAlias  string
	SourceColumn ColumnDescriptor
}

// HashJoinPlan maps two KV scans onto an equi-join boundary.
type HashJoinPlan struct {
	Left       BoundTable
	LeftScan   RangeScanPlan
	Right      BoundTable
	RightScan  RangeScanPlan
	Join       JoinSpec
	Projection []JoinProjection
}

func (PointLookupPlan) isPlan() {}
func (RangeScanPlan) isPlan()   {}
func (InsertPlan) isPlan()      {}
func (DeletePlan) isPlan()      {}
func (UpdatePlan) isPlan()      {}
func (AggregatePlan) isPlan()   {}
func (HashJoinPlan) isPlan()    {}

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
	if stmt.Distinct || stmt.Having != nil || stmt.Limit != nil || len(stmt.OrderBy) > 0 {
		return OptimizedPlan{}, fmt.Errorf("sql planner: unsupported SELECT shape")
	}
	if isJoinSelect(stmt.From) {
		return p.optimizeJoinSelect(stmt)
	}
	if stmt.GroupBy != nil || hasAggregateExprs(stmt.SelectExprs) {
		return p.optimizeAggregateSelect(stmt)
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

func (p *Planner) optimizeJoinSelect(stmt *vsqlparser.Select) (OptimizedPlan, error) {
	if stmt.GroupBy != nil || hasAggregateExprs(stmt.SelectExprs) {
		return OptimizedPlan{}, fmt.Errorf("sql planner: aggregate joins are unsupported")
	}
	left, right, joinExpr, err := p.resolveJoinTables(stmt.From)
	if err != nil {
		return OptimizedPlan{}, err
	}
	projection, leftProjection, rightProjection, err := bindJoinProjection(left, right, stmt.SelectExprs)
	if err != nil {
		return OptimizedPlan{}, err
	}
	joinSpec, leftJoinColumn, rightJoinColumn, err := bindEquiJoin(left, right, joinExpr)
	if err != nil {
		return OptimizedPlan{}, err
	}
	leftProjection = ensureColumnProjection(leftProjection, leftJoinColumn)
	rightProjection = ensureColumnProjection(rightProjection, rightJoinColumn)
	leftScan, _, err := buildRangeScanPlan(left.Table, leftProjection, boundPredicate{})
	if err != nil {
		return OptimizedPlan{}, err
	}
	rightScan, _, err := buildRangeScanPlan(right.Table, rightProjection, boundPredicate{})
	if err != nil {
		return OptimizedPlan{}, err
	}
	candidates := []PlanCandidate{
		makeHashJoinCandidate(p.optimizer, left, leftScan, right, rightScan, joinSpec, projection),
	}
	return p.optimizer.Choose(candidates)
}

func (p *Planner) optimizeAggregateSelect(stmt *vsqlparser.Select) (OptimizedPlan, error) {
	table, err := p.resolveSingleTable(stmt.From)
	if err != nil {
		return OptimizedPlan{}, err
	}
	groupBy, err := bindGroupBy(table, stmt.GroupBy)
	if err != nil {
		return OptimizedPlan{}, err
	}
	inputProjection, aggregates, err := bindAggregateProjection(table, stmt.SelectExprs, groupBy)
	if err != nil {
		return OptimizedPlan{}, err
	}
	predicate, err := bindPrimaryKeyPredicate(table, stmt.Where)
	if err != nil {
		return OptimizedPlan{}, err
	}
	candidates, err := makeAggregateCandidates(p.optimizer, table, inputProjection, predicate, groupBy, aggregates)
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

func (p *Planner) optimizeDelete(stmt *vsqlparser.Delete) (OptimizedPlan, error) {
	if stmt.With != nil || stmt.Ignore || len(stmt.Targets) > 0 || len(stmt.Partitions) > 0 || len(stmt.OrderBy) > 0 || stmt.Limit != nil {
		return OptimizedPlan{}, fmt.Errorf("sql planner: unsupported DELETE shape")
	}
	table, err := p.resolveSingleTable(stmt.TableExprs)
	if err != nil {
		return OptimizedPlan{}, err
	}
	predicate, err := bindPrimaryKeyPredicate(table, stmt.Where)
	if err != nil {
		return OptimizedPlan{}, err
	}
	switch {
	case predicate.equality != nil:
	case predicate.lower != nil && predicate.upper != nil:
	default:
		return OptimizedPlan{}, fmt.Errorf("sql planner: DELETE requires a primary-key point predicate or bounded primary-key range")
	}
	scanPlan, singleton, err := buildRangeScanPlan(table, table.Columns, predicate)
	if err != nil {
		return OptimizedPlan{}, err
	}
	return p.optimizer.Choose([]PlanCandidate{makeDeleteCandidate(p.optimizer, table, predicate, singleton, DeletePlan{
		Table:     table,
		Input:     scanPlan,
		Singleton: singleton,
	})})
}

func (p *Planner) optimizeUpdate(stmt *vsqlparser.Update) (OptimizedPlan, error) {
	if stmt.With != nil || stmt.Ignore || len(stmt.OrderBy) > 0 || stmt.Limit != nil {
		return OptimizedPlan{}, fmt.Errorf("sql planner: unsupported UPDATE shape")
	}
	table, err := p.resolveSingleTable(stmt.TableExprs)
	if err != nil {
		return OptimizedPlan{}, err
	}
	assignments, err := bindUpdateAssignments(table, stmt.Exprs)
	if err != nil {
		return OptimizedPlan{}, err
	}
	predicate, err := bindPrimaryKeyPredicate(table, stmt.Where)
	if err != nil {
		return OptimizedPlan{}, err
	}
	switch {
	case predicate.equality != nil:
	case predicate.lower != nil && predicate.upper != nil:
	default:
		return OptimizedPlan{}, fmt.Errorf("sql planner: UPDATE requires a primary-key point predicate or bounded primary-key range")
	}
	scanPlan, singleton, err := buildRangeScanPlan(table, table.Columns, predicate)
	if err != nil {
		return OptimizedPlan{}, err
	}
	return p.optimizer.Choose([]PlanCandidate{makeUpdateCandidate(p.optimizer, table, predicate, singleton, UpdatePlan{
		Table:       table,
		Input:       scanPlan,
		Assignments: assignments,
		Singleton:   singleton,
	})})
}

func (p *Planner) resolveSingleTable(from []vsqlparser.TableExpr) (TableDescriptor, error) {
	if len(from) != 1 {
		return TableDescriptor{}, fmt.Errorf("sql planner: only single-table statements are supported")
	}
	aliased, ok := from[0].(*vsqlparser.AliasedTableExpr)
	if !ok {
		return TableDescriptor{}, fmt.Errorf("sql planner: unsupported table expression %T", from[0])
	}
	tableName, err := baseTableName(aliased)
	if err != nil {
		return TableDescriptor{}, err
	}
	return p.catalog.ResolveTable(tableName)
}

func (p *Planner) resolveJoinTables(from []vsqlparser.TableExpr) (BoundTable, BoundTable, *vsqlparser.JoinTableExpr, error) {
	if len(from) != 1 {
		return BoundTable{}, BoundTable{}, nil, fmt.Errorf("sql planner: only one top-level join expression is supported")
	}
	joinExpr, ok := from[0].(*vsqlparser.JoinTableExpr)
	if !ok {
		return BoundTable{}, BoundTable{}, nil, fmt.Errorf("sql planner: expected join expression, got %T", from[0])
	}
	if !joinExpr.Join.IsInner() {
		return BoundTable{}, BoundTable{}, nil, fmt.Errorf("sql planner: only inner joins are supported")
	}
	if joinExpr.Condition == nil {
		return BoundTable{}, BoundTable{}, nil, fmt.Errorf("sql planner: join condition is required")
	}
	left, err := p.resolveBoundTable(joinExpr.LeftExpr)
	if err != nil {
		return BoundTable{}, BoundTable{}, nil, err
	}
	right, err := p.resolveBoundTable(joinExpr.RightExpr)
	if err != nil {
		return BoundTable{}, BoundTable{}, nil, err
	}
	if canonicalName(left.Alias) == canonicalName(right.Alias) {
		return BoundTable{}, BoundTable{}, nil, fmt.Errorf("sql planner: duplicate join alias %q", left.Alias)
	}
	return left, right, joinExpr, nil
}

func (p *Planner) resolveBoundTable(expr vsqlparser.TableExpr) (BoundTable, error) {
	aliased, ok := expr.(*vsqlparser.AliasedTableExpr)
	if !ok {
		return BoundTable{}, fmt.Errorf("sql planner: unsupported joined table expression %T", expr)
	}
	tableName, err := baseTableName(aliased)
	if err != nil {
		return BoundTable{}, err
	}
	table, err := p.catalog.ResolveTable(tableName)
	if err != nil {
		return BoundTable{}, err
	}
	alias := aliased.As.String()
	if strings.TrimSpace(alias) == "" {
		alias = table.Name
	}
	return BoundTable{
		Alias: alias,
		Table: table,
	}, nil
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

func bindJoinProjection(left, right BoundTable, exprs *vsqlparser.SelectExprs) ([]JoinProjection, []ColumnDescriptor, []ColumnDescriptor, error) {
	if exprs == nil || len(exprs.Exprs) == 0 {
		return nil, nil, nil, fmt.Errorf("sql planner: SELECT list must not be empty")
	}
	outputs := make([]JoinProjection, 0, len(exprs.Exprs))
	leftProjection := make([]ColumnDescriptor, 0, len(exprs.Exprs))
	rightProjection := make([]ColumnDescriptor, 0, len(exprs.Exprs))
	seenOutputs := make(map[string]struct{}, len(exprs.Exprs))

	for _, expr := range exprs.Exprs {
		aliased, ok := expr.(*vsqlparser.AliasedExpr)
		if !ok {
			return nil, nil, nil, fmt.Errorf("sql planner: unsupported join projection %T", expr)
		}
		col, ok := aliased.Expr.(*vsqlparser.ColName)
		if !ok {
			return nil, nil, nil, fmt.Errorf("sql planner: join projections must be columns")
		}
		sourceAlias, sourceColumn, err := resolveJoinColumn(left, right, col)
		if err != nil {
			return nil, nil, nil, err
		}
		outputName := aliased.ColumnName()
		if strings.TrimSpace(outputName) == "" {
			outputName = sourceColumn.Name
		}
		key := canonicalName(outputName)
		if _, exists := seenOutputs[key]; exists {
			return nil, nil, nil, fmt.Errorf("sql planner: duplicate output column %q", outputName)
		}
		seenOutputs[key] = struct{}{}

		output := sourceColumn
		output.Name = outputName
		outputs = append(outputs, JoinProjection{
			Output:       output,
			SourceAlias:  sourceAlias,
			SourceColumn: sourceColumn,
		})
		if canonicalName(sourceAlias) == canonicalName(left.Alias) {
			leftProjection = ensureColumnProjection(leftProjection, sourceColumn)
		} else {
			rightProjection = ensureColumnProjection(rightProjection, sourceColumn)
		}
	}

	return outputs, leftProjection, rightProjection, nil
}

func bindGroupBy(table TableDescriptor, groupBy *vsqlparser.GroupBy) ([]ColumnDescriptor, error) {
	if groupBy == nil || len(groupBy.Exprs) == 0 {
		return nil, nil
	}
	if groupBy.WithRollup {
		return nil, fmt.Errorf("sql planner: GROUP BY WITH ROLLUP is unsupported")
	}
	seen := make(map[string]struct{}, len(groupBy.Exprs))
	columns := make([]ColumnDescriptor, 0, len(groupBy.Exprs))
	for _, expr := range groupBy.Exprs {
		col, ok := expr.(*vsqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("sql planner: GROUP BY expressions must be columns")
		}
		descriptor, exists := table.ColumnByName(col.Name.String())
		if !exists {
			return nil, fmt.Errorf("sql planner: unknown GROUP BY column %q", col.Name.String())
		}
		name := canonicalName(descriptor.Name)
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("sql planner: duplicate GROUP BY column %q", descriptor.Name)
		}
		seen[name] = struct{}{}
		columns = append(columns, descriptor)
	}
	return columns, nil
}

func bindAggregateProjection(table TableDescriptor, exprs *vsqlparser.SelectExprs, groupBy []ColumnDescriptor) ([]ColumnDescriptor, []AggregateExpr, error) {
	if exprs == nil || len(exprs.Exprs) == 0 {
		return nil, nil, fmt.Errorf("sql planner: SELECT list must not be empty")
	}
	groupSet := make(map[string]ColumnDescriptor, len(groupBy))
	projectionSet := make(map[string]struct{}, len(groupBy))
	inputProjection := make([]ColumnDescriptor, 0, len(groupBy))
	addProjection := func(column ColumnDescriptor) {
		name := canonicalName(column.Name)
		if _, exists := projectionSet[name]; exists {
			return
		}
		projectionSet[name] = struct{}{}
		inputProjection = append(inputProjection, column)
	}
	for _, column := range groupBy {
		groupSet[canonicalName(column.Name)] = column
		addProjection(column)
	}

	aggregates := make([]AggregateExpr, 0, len(exprs.Exprs))
	for _, expr := range exprs.Exprs {
		aliased, ok := expr.(*vsqlparser.AliasedExpr)
		if !ok {
			return nil, nil, fmt.Errorf("sql planner: unsupported aggregate projection %T", expr)
		}
		switch typed := aliased.Expr.(type) {
		case *vsqlparser.ColName:
			column, exists := table.ColumnByName(typed.Name.String())
			if !exists {
				return nil, nil, fmt.Errorf("sql planner: unknown column %q", typed.Name.String())
			}
			if _, grouped := groupSet[canonicalName(column.Name)]; !grouped {
				return nil, nil, fmt.Errorf("sql planner: selected column %q must appear in GROUP BY or be aggregated", column.Name)
			}
			addProjection(column)
		case vsqlparser.AggrFunc:
			aggregate, err := bindAggregateExpr(table, aliased, typed)
			if err != nil {
				return nil, nil, err
			}
			aggregates = append(aggregates, aggregate)
			if aggregate.Input != nil {
				addProjection(*aggregate.Input)
			}
		default:
			return nil, nil, fmt.Errorf("sql planner: unsupported aggregate expression %T", aliased.Expr)
		}
	}
	if len(aggregates) == 0 && len(groupBy) == 0 {
		return nil, nil, fmt.Errorf("sql planner: aggregate planning requires GROUP BY columns or aggregate functions")
	}
	return inputProjection, aggregates, nil
}

func bindAggregateExpr(table TableDescriptor, aliased *vsqlparser.AliasedExpr, expr vsqlparser.AggrFunc) (AggregateExpr, error) {
	alias := aliased.As.String()
	switch typed := expr.(type) {
	case *vsqlparser.CountStar:
		return AggregateExpr{
			Func:  AggregateFuncCount,
			Alias: aggregateAlias(alias, "count"),
		}, nil
	case *vsqlparser.Count:
		if typed.Distinct {
			return AggregateExpr{}, fmt.Errorf("sql planner: DISTINCT aggregates are unsupported")
		}
		column, err := resolveAggregateColumn(table, typed.GetArg())
		if err != nil {
			return AggregateExpr{}, err
		}
		return AggregateExpr{
			Func:  AggregateFuncCount,
			Input: &column,
			Alias: aggregateAlias(alias, "count"),
		}, nil
	case *vsqlparser.Sum:
		if typed.Distinct {
			return AggregateExpr{}, fmt.Errorf("sql planner: DISTINCT aggregates are unsupported")
		}
		column, err := resolveAggregateColumn(table, typed.GetArg())
		if err != nil {
			return AggregateExpr{}, err
		}
		if column.Type != ColumnTypeInt {
			return AggregateExpr{}, fmt.Errorf("sql planner: SUM requires an INT column, got %s", column.Type)
		}
		return AggregateExpr{
			Func:  AggregateFuncSum,
			Input: &column,
			Alias: aggregateAlias(alias, "sum"),
		}, nil
	default:
		return AggregateExpr{}, fmt.Errorf("sql planner: unsupported aggregate function %q", expr.AggrName())
	}
}

func bindEquiJoin(left, right BoundTable, joinExpr *vsqlparser.JoinTableExpr) (JoinSpec, ColumnDescriptor, ColumnDescriptor, error) {
	if len(joinExpr.Condition.Using) > 0 {
		if len(joinExpr.Condition.Using) != 1 {
			return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: only single-column USING joins are supported")
		}
		columnName := joinExpr.Condition.Using[0].String()
		leftColumn, ok := left.Table.ColumnByName(columnName)
		if !ok {
			return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: left table %q has no join column %q", left.Table.Name, columnName)
		}
		rightColumn, ok := right.Table.ColumnByName(columnName)
		if !ok {
			return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: right table %q has no join column %q", right.Table.Name, columnName)
		}
		return JoinSpec{
			Type:      "inner",
			LeftKeys:  []string{qualifiedColumnName(left.Alias, leftColumn.Name)},
			RightKeys: []string{qualifiedColumnName(right.Alias, rightColumn.Name)},
		}, leftColumn, rightColumn, nil
	}
	if joinExpr.Condition.On == nil {
		return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: join ON condition is required")
	}
	cmp, ok := joinExpr.Condition.On.(*vsqlparser.ComparisonExpr)
	if !ok || cmp.Operator != vsqlparser.EqualOp {
		return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: only equi-join ON conditions are supported")
	}
	leftColExpr, ok := cmp.Left.(*vsqlparser.ColName)
	if !ok {
		return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: join left condition must be a column")
	}
	rightColExpr, ok := cmp.Right.(*vsqlparser.ColName)
	if !ok {
		return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: join right condition must be a column")
	}
	leftAlias, leftColumn, err := resolveJoinColumn(left, right, leftColExpr)
	if err != nil {
		return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, err
	}
	rightAlias, rightColumn, err := resolveJoinColumn(left, right, rightColExpr)
	if err != nil {
		return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, err
	}
	if canonicalName(leftAlias) == canonicalName(rightAlias) {
		return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: join columns must come from opposite sides")
	}
	if canonicalName(leftAlias) == canonicalName(right.Alias) {
		leftAlias, rightAlias = rightAlias, leftAlias
		leftColumn, rightColumn = rightColumn, leftColumn
	}
	if canonicalName(leftAlias) != canonicalName(left.Alias) || canonicalName(rightAlias) != canonicalName(right.Alias) {
		return JoinSpec{}, ColumnDescriptor{}, ColumnDescriptor{}, fmt.Errorf("sql planner: join columns must come from the left and right tables")
	}
	return JoinSpec{
		Type:      "inner",
		LeftKeys:  []string{qualifiedColumnName(left.Alias, leftColumn.Name)},
		RightKeys: []string{qualifiedColumnName(right.Alias, rightColumn.Name)},
	}, leftColumn, rightColumn, nil
}

func resolveAggregateColumn(table TableDescriptor, expr vsqlparser.Expr) (ColumnDescriptor, error) {
	col, ok := expr.(*vsqlparser.ColName)
	if !ok {
		return ColumnDescriptor{}, fmt.Errorf("sql planner: aggregate arguments must be columns")
	}
	column, exists := table.ColumnByName(col.Name.String())
	if !exists {
		return ColumnDescriptor{}, fmt.Errorf("sql planner: unknown aggregate column %q", col.Name.String())
	}
	return column, nil
}

func aggregateAlias(alias, fallback string) string {
	if strings.TrimSpace(alias) != "" {
		return alias
	}
	return fallback
}

func resolveJoinColumn(left, right BoundTable, col *vsqlparser.ColName) (string, ColumnDescriptor, error) {
	qualifier := canonicalName(col.Qualifier.Name.String())
	columnName := col.Name.String()
	if qualifier != "" {
		switch qualifier {
		case canonicalName(left.Alias), canonicalName(left.Table.Name):
			column, ok := left.Table.ColumnByName(columnName)
			if !ok {
				return "", ColumnDescriptor{}, fmt.Errorf("sql planner: unknown column %q on %q", columnName, left.Alias)
			}
			return left.Alias, column, nil
		case canonicalName(right.Alias), canonicalName(right.Table.Name):
			column, ok := right.Table.ColumnByName(columnName)
			if !ok {
				return "", ColumnDescriptor{}, fmt.Errorf("sql planner: unknown column %q on %q", columnName, right.Alias)
			}
			return right.Alias, column, nil
		default:
			return "", ColumnDescriptor{}, fmt.Errorf("sql planner: unknown table qualifier %q", col.Qualifier.Name.String())
		}
	}

	leftColumn, leftOK := left.Table.ColumnByName(columnName)
	rightColumn, rightOK := right.Table.ColumnByName(columnName)
	switch {
	case leftOK && rightOK:
		return "", ColumnDescriptor{}, fmt.Errorf("sql planner: ambiguous column %q", columnName)
	case leftOK:
		return left.Alias, leftColumn, nil
	case rightOK:
		return right.Alias, rightColumn, nil
	default:
		return "", ColumnDescriptor{}, fmt.Errorf("sql planner: unknown join column %q", columnName)
	}
}

func ensureColumnProjection(columns []ColumnDescriptor, column ColumnDescriptor) []ColumnDescriptor {
	for _, existing := range columns {
		if canonicalName(existing.Name) == canonicalName(column.Name) {
			return columns
		}
	}
	return append(columns, column)
}

func isJoinSelect(from []vsqlparser.TableExpr) bool {
	return len(from) == 1 && isJoinExpr(from[0])
}

func isJoinExpr(expr vsqlparser.TableExpr) bool {
	_, ok := expr.(*vsqlparser.JoinTableExpr)
	return ok
}

func qualifiedColumnName(alias, column string) string {
	return canonicalName(alias) + "." + canonicalName(column)
}

func baseTableName(aliased *vsqlparser.AliasedTableExpr) (string, error) {
	switch typed := aliased.Expr.(type) {
	case vsqlparser.TableName:
		return typed.Name.String(), nil
	case *vsqlparser.TableName:
		return typed.Name.String(), nil
	default:
		return "", fmt.Errorf("sql planner: unsupported table source %T", aliased.Expr)
	}
}

func hasAggregateExprs(exprs *vsqlparser.SelectExprs) bool {
	if exprs == nil {
		return false
	}
	for _, expr := range exprs.Exprs {
		aliased, ok := expr.(*vsqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if _, ok := aliased.Expr.(vsqlparser.AggrFunc); ok {
			return true
		}
	}
	return false
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

func bindUpdateAssignments(table TableDescriptor, exprs vsqlparser.UpdateExprs) ([]UpdateAssignment, error) {
	if len(exprs) == 0 {
		return nil, fmt.Errorf("sql planner: UPDATE requires at least one SET clause")
	}
	pkColumn, err := table.PrimaryKeyColumn()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(exprs))
	assignments := make([]UpdateAssignment, 0, len(exprs))
	for _, expr := range exprs {
		if expr == nil || expr.Name == nil {
			return nil, fmt.Errorf("sql planner: malformed UPDATE expression")
		}
		qualifier := strings.TrimSpace(expr.Name.Qualifier.Name.String())
		if qualifier != "" && canonicalName(qualifier) != canonicalName(table.Name) {
			return nil, fmt.Errorf("sql planner: unknown UPDATE qualifier %q", qualifier)
		}
		column, ok := table.ColumnByName(expr.Name.Name.String())
		if !ok {
			return nil, fmt.Errorf("sql planner: unknown UPDATE column %q", expr.Name.Name.String())
		}
		if canonicalName(column.Name) == canonicalName(pkColumn.Name) {
			return nil, fmt.Errorf("sql planner: updating primary key column %q is unsupported", pkColumn.Name)
		}
		name := canonicalName(column.Name)
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("sql planner: duplicate UPDATE assignment for column %q", column.Name)
		}
		seen[name] = struct{}{}
		literal, ok := expr.Expr.(*vsqlparser.Literal)
		if !ok {
			return nil, fmt.Errorf("sql planner: only literal UPDATE values are supported")
		}
		value, err := bindLiteral(column.Type, literal)
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, UpdateAssignment{
			Column: column,
			Value:  value,
		})
	}
	return assignments, nil
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
