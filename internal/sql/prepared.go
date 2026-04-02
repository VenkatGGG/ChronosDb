package sql

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	vsqlparser "vitess.io/vitess/go/vt/sqlparser"
)

// PreparedQuery captures the parameter contract for one extended-protocol SQL
// statement before any concrete bind values are supplied.
type PreparedQuery struct {
	OriginalQuery  string
	RewrittenQuery string
	ParameterTypes []ColumnType
}

// Prepare validates one PostgreSQL-style positional-parameter query and
// returns the inferred parameter types required to bind it later.
func (p *Planner) Prepare(query string) (PreparedQuery, error) {
	rewritten, maxIndex, err := rewritePositionalParameters(query)
	if err != nil {
		return PreparedQuery{}, err
	}
	features, err := extractQueryFeatures(rewritten)
	if err != nil {
		return PreparedQuery{}, err
	}
	stmt, bindVars, err := p.parser.Parse2(features.BaseQuery)
	if err != nil {
		return PreparedQuery{}, err
	}
	paramTypes := make([]ColumnType, maxIndex)
	switch typed := stmt.(type) {
	case *vsqlparser.Select:
		if err := p.inferSelectParameters(typed, paramTypes); err != nil {
			return PreparedQuery{}, err
		}
	case *vsqlparser.Insert:
		if err := p.inferInsertParameters(typed, paramTypes); err != nil {
			return PreparedQuery{}, err
		}
		if features.OnConflict != nil {
			tableName, tableErr := typed.Table.TableName()
			if tableErr != nil {
				return PreparedQuery{}, tableErr
			}
			table, tableErr := p.catalog.ResolveTable(tableName.Name.String())
			if tableErr != nil {
				return PreparedQuery{}, tableErr
			}
			if err := p.inferOnConflictParameters(table, *features.OnConflict, paramTypes); err != nil {
				return PreparedQuery{}, err
			}
		}
	case *vsqlparser.Delete:
		if err := p.inferDeleteParameters(typed, paramTypes); err != nil {
			return PreparedQuery{}, err
		}
	case *vsqlparser.Update:
		if err := p.inferUpdateParameters(typed, paramTypes); err != nil {
			return PreparedQuery{}, err
		}
	default:
		return PreparedQuery{}, fmt.Errorf("sql planner: unsupported prepared statement type %T", stmt)
	}
	if err := validatePreparedParameterCoverage(bindVars, paramTypes); err != nil {
		return PreparedQuery{}, err
	}
	return PreparedQuery{
		OriginalQuery:  query,
		RewrittenQuery: rewritten,
		ParameterTypes: paramTypes,
	}, nil
}

// SamplePreparedValues returns stable typed literals suitable for describing a
// prepared statement before real bind values arrive.
func SamplePreparedValues(types []ColumnType) []Value {
	if len(types) == 0 {
		return nil
	}
	values := make([]Value, len(types))
	for i, columnType := range types {
		switch columnType {
		case ColumnTypeInt:
			values[i] = Value{Type: ColumnTypeInt, Int64: 0}
		case ColumnTypeBytes:
			values[i] = Value{Type: ColumnTypeBytes, Bytes: []byte("sample")}
		case ColumnTypeString:
			fallthrough
		default:
			values[i] = Value{Type: ColumnTypeString, String: "sample"}
		}
	}
	return values
}

// RenderPreparedQuery substitutes PostgreSQL-style positional parameters with
// concrete typed SQL literals.
func RenderPreparedQuery(query string, values []Value) (string, error) {
	var out strings.Builder
	out.Grow(len(query) + len(values)*8)
	inSingle := false
	inDouble := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch ch {
		case '\'':
			out.WriteByte(ch)
			if !inDouble {
				if inSingle && i+1 < len(query) && query[i+1] == '\'' {
					out.WriteByte(query[i+1])
					i++
					continue
				}
				inSingle = !inSingle
			}
			continue
		case '"':
			out.WriteByte(ch)
			if !inSingle {
				inDouble = !inDouble
			}
			continue
		case '$':
			if inSingle || inDouble {
				out.WriteByte(ch)
				continue
			}
			j := i + 1
			for j < len(query) && unicode.IsDigit(rune(query[j])) {
				j++
			}
			if j == i+1 {
				out.WriteByte(ch)
				continue
			}
			index, err := strconv.Atoi(query[i+1 : j])
			if err != nil || index <= 0 || index > len(values) {
				return "", fmt.Errorf("sql planner: invalid prepared parameter reference $%s", query[i+1:j])
			}
			out.WriteString(encodePreparedLiteral(values[index-1]))
			i = j - 1
			continue
		default:
			out.WriteByte(ch)
		}
	}
	return out.String(), nil
}

func (p *Planner) inferSelectParameters(stmt *vsqlparser.Select, paramTypes []ColumnType) error {
	if isJoinSelect(stmt.From) {
		if len(paramTypes) > 0 {
			return fmt.Errorf("sql planner: prepared parameters on joins are unsupported")
		}
		return nil
	}
	table, err := p.resolveSingleTable(stmt.From)
	if err != nil {
		return err
	}
	if err := inferPredicateParameters(table, stmt.Where, paramTypes); err != nil {
		return err
	}
	return inferLimitParameters(stmt.Limit, paramTypes)
}

func (p *Planner) inferInsertParameters(stmt *vsqlparser.Insert, paramTypes []ColumnType) error {
	tableName, err := stmt.Table.TableName()
	if err != nil {
		return err
	}
	table, err := p.catalog.ResolveTable(tableName.Name.String())
	if err != nil {
		return err
	}
	values, ok := stmt.Rows.(vsqlparser.Values)
	if !ok {
		return fmt.Errorf("sql planner: only VALUES inserts are supported")
	}
	if len(values) != 1 {
		return fmt.Errorf("sql planner: only single-row inserts are supported")
	}
	if len(stmt.Columns) != len(values[0]) {
		return fmt.Errorf("sql planner: INSERT column/value count mismatch")
	}
	for i, identifier := range stmt.Columns {
		column, ok := table.ColumnByName(identifier.String())
		if !ok {
			return fmt.Errorf("sql planner: unknown column %q", identifier.String())
		}
		switch typed := values[0][i].(type) {
		case *vsqlparser.Argument:
			if err := assignArgumentType(paramTypes, typed, column.Type); err != nil {
				return err
			}
		case *vsqlparser.Literal:
		default:
			return fmt.Errorf("sql planner: only literal or parameter INSERT values are supported")
		}
	}
	return nil
}

func (p *Planner) inferDeleteParameters(stmt *vsqlparser.Delete, paramTypes []ColumnType) error {
	table, err := p.resolveSingleTable(stmt.TableExprs)
	if err != nil {
		return err
	}
	return inferPredicateParameters(table, stmt.Where, paramTypes)
}

func (p *Planner) inferUpdateParameters(stmt *vsqlparser.Update, paramTypes []ColumnType) error {
	table, err := p.resolveSingleTable(stmt.TableExprs)
	if err != nil {
		return err
	}
	for _, expr := range stmt.Exprs {
		if expr == nil || expr.Name == nil {
			return fmt.Errorf("sql planner: malformed UPDATE expression")
		}
		column, ok := table.ColumnByName(expr.Name.Name.String())
		if !ok {
			return fmt.Errorf("sql planner: unknown UPDATE column %q", expr.Name.Name.String())
		}
		switch typed := expr.Expr.(type) {
		case *vsqlparser.Argument:
			if err := assignArgumentType(paramTypes, typed, column.Type); err != nil {
				return err
			}
		case *vsqlparser.Literal:
		default:
			return fmt.Errorf("sql planner: only literal or parameter UPDATE values are supported")
		}
	}
	return inferPredicateParameters(table, stmt.Where, paramTypes)
}

func inferPredicateParameters(table TableDescriptor, where *vsqlparser.Where, paramTypes []ColumnType) error {
	if where == nil || where.Expr == nil {
		return nil
	}
	comparisons, err := flattenComparisons(where.Expr)
	if err != nil {
		return err
	}
	for _, cmp := range comparisons {
		if err := inferComparisonParameters(table, cmp, paramTypes); err != nil {
			return err
		}
	}
	return nil
}

func inferComparisonParameters(table TableDescriptor, cmp *vsqlparser.ComparisonExpr, paramTypes []ColumnType) error {
	leftCol, leftIsCol := cmp.Left.(*vsqlparser.ColName)
	rightCol, rightIsCol := cmp.Right.(*vsqlparser.ColName)
	leftArg, leftIsArg := cmp.Left.(*vsqlparser.Argument)
	rightArg, rightIsArg := cmp.Right.(*vsqlparser.Argument)
	switch {
	case leftIsCol && rightIsArg:
		column, ok := table.ColumnByName(leftCol.Name.String())
		if !ok {
			return fmt.Errorf("sql planner: unknown WHERE column %q", leftCol.Name.String())
		}
		return assignArgumentType(paramTypes, rightArg, column.Type)
	case rightIsCol && leftIsArg:
		column, ok := table.ColumnByName(rightCol.Name.String())
		if !ok {
			return fmt.Errorf("sql planner: unknown WHERE column %q", rightCol.Name.String())
		}
		return assignArgumentType(paramTypes, leftArg, column.Type)
	case leftIsCol || rightIsCol:
		return nil
	default:
		return fmt.Errorf("sql planner: WHERE predicates must compare a column against a literal or parameter")
	}
}

func inferLimitParameters(limit *vsqlparser.Limit, paramTypes []ColumnType) error {
	if limit == nil {
		return nil
	}
	if arg, ok := limit.Rowcount.(*vsqlparser.Argument); ok {
		return assignArgumentType(paramTypes, arg, ColumnTypeInt)
	}
	return nil
}

func validatePreparedParameterCoverage(bindVars vsqlparser.BindVars, paramTypes []ColumnType) error {
	for i, columnType := range paramTypes {
		if columnType == "" {
			return fmt.Errorf("sql planner: could not infer parameter type for $%d", i+1)
		}
	}
	for name := range bindVars {
		index, err := parsePreparedArgumentName(name)
		if err != nil {
			return err
		}
		if index <= 0 || index > len(paramTypes) {
			return fmt.Errorf("sql planner: unexpected prepared parameter :%s", name)
		}
	}
	return nil
}

func assignArgumentType(paramTypes []ColumnType, arg *vsqlparser.Argument, columnType ColumnType) error {
	index, err := parsePreparedArgumentName(arg.Name)
	if err != nil {
		return err
	}
	if index <= 0 || index > len(paramTypes) {
		return fmt.Errorf("sql planner: unexpected prepared parameter :%s", arg.Name)
	}
	current := paramTypes[index-1]
	switch {
	case current == "":
		paramTypes[index-1] = columnType
	case current != columnType:
		return fmt.Errorf("sql planner: parameter $%d is used with conflicting types %s and %s", index, current, columnType)
	}
	return nil
}

func parsePreparedArgumentName(name string) (int, error) {
	name = strings.TrimSpace(name)
	if !strings.HasPrefix(name, "v") {
		return 0, fmt.Errorf("sql planner: unsupported bind variable :%s", name)
	}
	index, err := strconv.Atoi(name[1:])
	if err != nil || index <= 0 {
		return 0, fmt.Errorf("sql planner: invalid bind variable :%s", name)
	}
	return index, nil
}

func rewritePositionalParameters(query string) (string, int, error) {
	var out strings.Builder
	out.Grow(len(query) + 8)
	inSingle := false
	inDouble := false
	maxIndex := 0
	seen := make(map[int]struct{})
	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch ch {
		case '\'':
			out.WriteByte(ch)
			if !inDouble {
				if inSingle && i+1 < len(query) && query[i+1] == '\'' {
					out.WriteByte(query[i+1])
					i++
					continue
				}
				inSingle = !inSingle
			}
			continue
		case '"':
			out.WriteByte(ch)
			if !inSingle {
				inDouble = !inDouble
			}
			continue
		case '$':
			if inSingle || inDouble {
				out.WriteByte(ch)
				continue
			}
			j := i + 1
			for j < len(query) && unicode.IsDigit(rune(query[j])) {
				j++
			}
			if j == i+1 {
				out.WriteByte(ch)
				continue
			}
			index, err := strconv.Atoi(query[i+1 : j])
			if err != nil || index <= 0 {
				return "", 0, fmt.Errorf("sql planner: invalid prepared parameter $%s", query[i+1:j])
			}
			seen[index] = struct{}{}
			if index > maxIndex {
				maxIndex = index
			}
			out.WriteString(":v")
			out.WriteString(strconv.Itoa(index))
			i = j - 1
			continue
		default:
			out.WriteByte(ch)
		}
	}
	for i := 1; i <= maxIndex; i++ {
		if _, ok := seen[i]; !ok {
			return "", 0, fmt.Errorf("sql planner: prepared parameters must be dense from $1 through $%d", maxIndex)
		}
	}
	return out.String(), maxIndex, nil
}

func encodePreparedLiteral(value Value) string {
	switch value.Type {
	case ColumnTypeInt:
		return strconv.FormatInt(value.Int64, 10)
	case ColumnTypeBytes:
		return "x'" + strings.ToUpper(hex.EncodeToString(value.Bytes)) + "'"
	case ColumnTypeString:
		fallthrough
	default:
		return "'" + strings.ReplaceAll(value.String, "'", "''") + "'"
	}
}
