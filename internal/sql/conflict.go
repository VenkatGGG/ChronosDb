package sql

import (
	"bytes"
	"fmt"
	"strings"

	vsqlparser "vitess.io/vitess/go/vt/sqlparser"
)

type conflictFeatures struct {
	TargetColumns []string
	Action        OnConflictAction
	UpdateClause  string
}

func splitOnConflictClause(query string) (string, *conflictFeatures, error) {
	trimmed := strings.TrimSpace(query)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "insert ") {
		return query, nil, nil
	}
	idx := findKeywordOutsideQuotes(query, "on conflict")
	if idx < 0 {
		return query, nil, nil
	}
	base := strings.TrimSpace(query[:idx])
	conflict, err := parseConflictClause(strings.TrimSpace(query[idx+len("on conflict"):]))
	if err != nil {
		return query, nil, err
	}
	return base, conflict, nil
}

func parseConflictClause(clause string) (*conflictFeatures, error) {
	target, rest, err := consumeParenthesizedSegment(clause)
	if err != nil {
		return nil, err
	}
	columns := parseConflictTargetColumns(target)
	if len(columns) == 0 {
		return nil, fmt.Errorf("sql planner: ON CONFLICT target must reference at least one column")
	}
	rest = strings.TrimSpace(rest)
	lower := strings.ToLower(rest)
	switch {
	case strings.HasPrefix(lower, "do nothing"):
		if strings.TrimSpace(rest[len("do nothing"):]) != "" {
			return nil, fmt.Errorf("sql planner: unsupported ON CONFLICT DO NOTHING clause")
		}
		return &conflictFeatures{
			TargetColumns: columns,
			Action:        OnConflictDoNothing,
		}, nil
	case strings.HasPrefix(lower, "do update set"):
		updateClause := strings.TrimSpace(rest[len("do update set"):])
		if updateClause == "" {
			return nil, fmt.Errorf("sql planner: ON CONFLICT DO UPDATE requires SET assignments")
		}
		return &conflictFeatures{
			TargetColumns: columns,
			Action:        OnConflictDoUpdate,
			UpdateClause:  updateClause,
		}, nil
	default:
		return nil, fmt.Errorf("sql planner: unsupported ON CONFLICT action")
	}
}

func consumeParenthesizedSegment(input string) (string, string, error) {
	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(trimmed, "(") {
		return "", "", fmt.Errorf("sql planner: ON CONFLICT target must start with '('")
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(trimmed); i++ {
		switch trimmed[i] {
		case '\'':
			if !inDouble {
				if inSingle && i+1 < len(trimmed) && trimmed[i+1] == '\'' {
					i++
					continue
				}
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble {
				depth--
				if depth == 0 {
					return trimmed[1:i], trimmed[i+1:], nil
				}
			}
		}
	}
	return "", "", fmt.Errorf("sql planner: unterminated ON CONFLICT target")
}

func parseConflictTargetColumns(segment string) []string {
	parts := strings.Split(segment, ",")
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		name = strings.Trim(name, "\"")
		if name == "" {
			continue
		}
		columns = append(columns, name)
	}
	return columns
}

func resolveConflictTarget(table TableDescriptor, columns []string) (ConflictTarget, error) {
	if sameColumnSequence(table.PrimaryKey, columns) {
		return ConflictTarget{
			Name:       strings.ToLower(strings.TrimSpace(table.Name)) + "_pkey",
			Columns:    append([]string(nil), table.PrimaryKey...),
			PrimaryKey: true,
		}, nil
	}
	index, ok := table.IndexByColumns(columns)
	if !ok || !index.Unique {
		return ConflictTarget{}, fmt.Errorf("sql planner: ON CONFLICT target (%s) must reference a unique index", strings.Join(columns, ", "))
	}
	return ConflictTarget{
		Name:    index.Name,
		Columns: append([]string(nil), index.Columns...),
		Index:   index,
	}, nil
}

func buildConflictKey(table TableDescriptor, target ConflictTarget, row map[string]Value, insertKey []byte) ([]byte, error) {
	if target.PrimaryKey {
		return append([]byte(nil), insertKey...), nil
	}
	entries, err := BuildIndexEntries(table, row)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Index.ID == target.Index.ID {
			return append([]byte(nil), entry.Key...), nil
		}
	}
	return nil, nil
}

func (p *Planner) bindOnConflictAssignments(table TableDescriptor, insertRow map[string]Value, conflict conflictFeatures) ([]UpdateAssignment, error) {
	if conflict.Action != OnConflictDoUpdate {
		return nil, nil
	}
	stmt, err := p.parser.Parse("update " + table.Name + " set " + conflict.UpdateClause)
	if err != nil {
		return nil, fmt.Errorf("sql planner: parse ON CONFLICT DO UPDATE assignments: %w", err)
	}
	update, ok := stmt.(*vsqlparser.Update)
	if !ok {
		return nil, fmt.Errorf("sql planner: parse ON CONFLICT DO UPDATE assignments: unexpected statement type %T", stmt)
	}
	if update.Where != nil {
		return nil, fmt.Errorf("sql planner: ON CONFLICT DO UPDATE ... WHERE is unsupported")
	}
	return bindConflictUpdateAssignments(table, insertRow, update.Exprs)
}

func bindConflictUpdateAssignments(table TableDescriptor, insertRow map[string]Value, exprs vsqlparser.UpdateExprs) ([]UpdateAssignment, error) {
	if len(exprs) == 0 {
		return nil, fmt.Errorf("sql planner: ON CONFLICT DO UPDATE requires at least one SET clause")
	}
	pkColumn, err := table.PrimaryKeyColumn()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(exprs))
	assignments := make([]UpdateAssignment, 0, len(exprs))
	for _, expr := range exprs {
		if expr == nil || expr.Name == nil {
			return nil, fmt.Errorf("sql planner: malformed ON CONFLICT assignment")
		}
		qualifier := strings.TrimSpace(expr.Name.Qualifier.Name.String())
		if qualifier != "" && canonicalName(qualifier) != canonicalName(table.Name) {
			return nil, fmt.Errorf("sql planner: unknown ON CONFLICT qualifier %q", qualifier)
		}
		column, ok := table.ColumnByName(expr.Name.Name.String())
		if !ok {
			return nil, fmt.Errorf("sql planner: unknown ON CONFLICT column %q", expr.Name.Name.String())
		}
		if canonicalName(column.Name) == canonicalName(pkColumn.Name) {
			return nil, fmt.Errorf("sql planner: updating primary key column %q is unsupported", pkColumn.Name)
		}
		name := canonicalName(column.Name)
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("sql planner: duplicate ON CONFLICT assignment for column %q", column.Name)
		}
		seen[name] = struct{}{}
		value, err := bindConflictAssignmentValue(table, insertRow, column, expr.Expr)
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

func bindConflictAssignmentValue(table TableDescriptor, insertRow map[string]Value, column ColumnDescriptor, expr vsqlparser.Expr) (Value, error) {
	switch typed := expr.(type) {
	case *vsqlparser.Literal:
		return bindLiteral(column.Type, typed)
	case *vsqlparser.ColName:
		qualifier := canonicalName(strings.TrimSpace(typed.Qualifier.Name.String()))
		if qualifier != "excluded" {
			return Value{}, fmt.Errorf("sql planner: ON CONFLICT assignments only support literals or EXCLUDED column references")
		}
		sourceColumn, ok := table.ColumnByName(typed.Name.String())
		if !ok {
			return Value{}, fmt.Errorf("sql planner: unknown EXCLUDED column %q", typed.Name.String())
		}
		value, ok := insertRow[canonicalName(sourceColumn.Name)]
		if !ok {
			return Value{}, fmt.Errorf("sql planner: EXCLUDED column %q is missing from inserted row", sourceColumn.Name)
		}
		return clonePreparedValue(value), nil
	default:
		return Value{}, fmt.Errorf("sql planner: ON CONFLICT assignments only support literals or EXCLUDED column references")
	}
}

func (p *Planner) inferOnConflictParameters(table TableDescriptor, conflict conflictFeatures, paramTypes []ColumnType) error {
	if conflict.Action != OnConflictDoUpdate {
		return nil
	}
	stmt, bindVars, err := p.parser.Parse2("update " + table.Name + " set " + conflict.UpdateClause)
	if err != nil {
		return fmt.Errorf("sql planner: parse ON CONFLICT DO UPDATE assignments: %w", err)
	}
	update, ok := stmt.(*vsqlparser.Update)
	if !ok {
		return fmt.Errorf("sql planner: parse ON CONFLICT DO UPDATE assignments: unexpected statement type %T", stmt)
	}
	if update.Where != nil {
		return fmt.Errorf("sql planner: ON CONFLICT DO UPDATE ... WHERE is unsupported")
	}
	for _, expr := range update.Exprs {
		if expr == nil || expr.Name == nil {
			return fmt.Errorf("sql planner: malformed ON CONFLICT assignment")
		}
		column, ok := table.ColumnByName(expr.Name.Name.String())
		if !ok {
			return fmt.Errorf("sql planner: unknown ON CONFLICT column %q", expr.Name.Name.String())
		}
		switch typed := expr.Expr.(type) {
		case *vsqlparser.Argument:
			if err := assignArgumentType(paramTypes, typed, column.Type); err != nil {
				return err
			}
		case *vsqlparser.ColName:
			if canonicalName(strings.TrimSpace(typed.Qualifier.Name.String())) != "excluded" {
				return fmt.Errorf("sql planner: ON CONFLICT assignments only support literals, parameters, or EXCLUDED column references")
			}
		case *vsqlparser.Literal:
		default:
			return fmt.Errorf("sql planner: ON CONFLICT assignments only support literals, parameters, or EXCLUDED column references")
		}
	}
	return validatePreparedParameterCoverage(bindVars, paramTypes)
}

func sameColumnSequence(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if canonicalName(left[i]) != canonicalName(right[i]) {
			return false
		}
	}
	return true
}

func clonePreparedValue(value Value) Value {
	cloned := value
	if value.Type == ColumnTypeBytes && value.Bytes != nil {
		cloned.Bytes = bytes.Clone(value.Bytes)
	}
	return cloned
}
