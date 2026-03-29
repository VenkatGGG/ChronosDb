package systemtest

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

type runtimeQueryHandler struct {
	planning *pgwire.PlanningHandler
	kv       *kvClient
}

func newRuntimeQueryHandler(planner *chronossql.Planner, flowPlanner *chronossql.FlowPlanner, kv *kvClient) *runtimeQueryHandler {
	return &runtimeQueryHandler{
		planning: pgwire.NewPlanningHandler(planner, flowPlanner),
		kv:       kv,
	}
}

func (h *runtimeQueryHandler) HandleSimpleQuery(ctx context.Context, query string) (pgwire.QueryResult, error) {
	optimized, _, result, err := h.planning.DescribeQuery(query)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	switch plan := optimized.Selected.Plan.(type) {
	case chronossql.PointLookupPlan:
		value, found, err := h.kv.GetLatest(ctx, plan.Key)
		if err != nil {
			return pgwire.QueryResult{}, err
		}
		if !found {
			return result, nil
		}
		row, err := chronossql.DecodeRowValue(plan.Table, value)
		if err != nil {
			return pgwire.QueryResult{}, pgwire.Error{
				Severity: "ERROR",
				Code:     "XX000",
				Message:  fmt.Sprintf("decode row payload: %v", err),
			}
		}
		projected, err := chronossql.ProjectRowText(plan.Projection, row)
		if err != nil {
			return pgwire.QueryResult{}, pgwire.Error{
				Severity: "ERROR",
				Code:     "XX000",
				Message:  fmt.Sprintf("project row payload: %v", err),
			}
		}
		result.Rows = [][][]byte{projected}
		result.CommandTag = "SELECT 1"
		return result, nil
	case chronossql.RangeScanPlan:
		rows, err := h.kv.ScanRange(ctx, plan.StartKey, plan.EndKey, plan.StartInclusive, plan.EndInclusive)
		if err != nil {
			return pgwire.QueryResult{}, err
		}
		result.Rows = make([][][]byte, 0, len(rows))
		for _, scanned := range rows {
			row, err := chronossql.DecodeRowValue(plan.Table, scanned.Value)
			if err != nil {
				return pgwire.QueryResult{}, pgwire.Error{
					Severity: "ERROR",
					Code:     "XX000",
					Message:  fmt.Sprintf("decode row payload: %v", err),
				}
			}
			projected, err := chronossql.ProjectRowText(plan.Projection, row)
			if err != nil {
				return pgwire.QueryResult{}, pgwire.Error{
					Severity: "ERROR",
					Code:     "XX000",
					Message:  fmt.Sprintf("project row payload: %v", err),
				}
			}
			result.Rows = append(result.Rows, projected)
		}
		result.CommandTag = fmt.Sprintf("SELECT %d", len(result.Rows))
		return result, nil
	case chronossql.InsertPlan:
		if err := h.kv.OnePhasePut(ctx, plan.Key, plan.Value); err != nil {
			return pgwire.QueryResult{}, pgwire.Error{
				Severity: "ERROR",
				Code:     "XX000",
				Message:  err.Error(),
			}
		}
		return result, nil
	case chronossql.AggregatePlan:
		rows, err := h.executeAggregate(ctx, plan)
		if err != nil {
			return pgwire.QueryResult{}, err
		}
		result.Rows = rows
		result.CommandTag = fmt.Sprintf("SELECT %d", len(rows))
		return result, nil
	case chronossql.HashJoinPlan:
		rows, err := h.executeHashJoin(ctx, plan)
		if err != nil {
			return pgwire.QueryResult{}, err
		}
		result.Rows = rows
		result.CommandTag = fmt.Sprintf("SELECT %d", len(rows))
		return result, nil
	default:
		return result, nil
	}
}

func (h *runtimeQueryHandler) executeAggregate(ctx context.Context, plan chronossql.AggregatePlan) ([][][]byte, error) {
	rows, err := h.scanRows(ctx, plan.Input)
	if err != nil {
		return nil, err
	}
	type aggregateState struct {
		groupValues []chronossql.Value
		counts      []int64
		sums        []int64
		sumSeen     []bool
	}
	groups := make(map[string]*aggregateState)
	order := make([]string, 0)

	ensureState := func(groupKey string, row map[string]chronossql.Value) *aggregateState {
		state, ok := groups[groupKey]
		if ok {
			return state
		}
		state = &aggregateState{
			groupValues: make([]chronossql.Value, len(plan.GroupBy)),
			counts:      make([]int64, len(plan.Aggregates)),
			sums:        make([]int64, len(plan.Aggregates)),
			sumSeen:     make([]bool, len(plan.Aggregates)),
		}
		for i, column := range plan.GroupBy {
			value, _ := lookupValue(row, column.Name)
			state.groupValues[i] = value
		}
		groups[groupKey] = state
		order = append(order, groupKey)
		return state
	}

	if len(rows) == 0 && len(plan.GroupBy) == 0 {
		ensureState("", nil)
	}

	for _, row := range rows {
		groupKey := aggregateGroupKey(plan.GroupBy, row)
		state := ensureState(groupKey, row)
		for i, aggregate := range plan.Aggregates {
			switch aggregate.Func {
			case chronossql.AggregateFuncCount:
				if aggregate.Input == nil {
					state.counts[i]++
					continue
				}
				if _, ok := lookupValue(row, aggregate.Input.Name); ok {
					state.counts[i]++
				}
			case chronossql.AggregateFuncSum:
				if aggregate.Input == nil {
					continue
				}
				value, ok := lookupValue(row, aggregate.Input.Name)
				if !ok {
					continue
				}
				state.sums[i] += value.Int64
				state.sumSeen[i] = true
			}
		}
	}

	sort.Strings(order)
	out := make([][][]byte, 0, len(order))
	for _, key := range order {
		state := groups[key]
		row := make([][]byte, 0, len(plan.GroupBy)+len(plan.Aggregates))
		for _, value := range state.groupValues {
			formatted, err := chronossql.FormatValueText(value)
			if err != nil {
				return nil, wrapExecutionError("format aggregate group value", err)
			}
			row = append(row, formatted)
		}
		for i, aggregate := range plan.Aggregates {
			switch aggregate.Func {
			case chronossql.AggregateFuncCount:
				formatted, err := chronossql.FormatValueText(chronossql.Value{
					Type:  chronossql.ColumnTypeInt,
					Int64: state.counts[i],
				})
				if err != nil {
					return nil, wrapExecutionError("format aggregate count", err)
				}
				row = append(row, formatted)
			case chronossql.AggregateFuncSum:
				if !state.sumSeen[i] {
					row = append(row, nil)
					continue
				}
				formatted, err := chronossql.FormatValueText(chronossql.Value{
					Type:  chronossql.ColumnTypeInt,
					Int64: state.sums[i],
				})
				if err != nil {
					return nil, wrapExecutionError("format aggregate sum", err)
				}
				row = append(row, formatted)
			default:
				return nil, wrapExecutionError("aggregate", fmt.Errorf("unsupported aggregate func %q", aggregate.Func))
			}
		}
		out = append(out, row)
	}
	return out, nil
}

func (h *runtimeQueryHandler) executeHashJoin(ctx context.Context, plan chronossql.HashJoinPlan) ([][][]byte, error) {
	leftRows, err := h.scanRows(ctx, plan.LeftScan)
	if err != nil {
		return nil, err
	}
	rightRows, err := h.scanRows(ctx, plan.RightScan)
	if err != nil {
		return nil, err
	}
	rightBuckets := make(map[string][]map[string]chronossql.Value, len(rightRows))
	for _, row := range rightRows {
		key, ok := joinKey(plan.Join.RightKeys, row)
		if !ok {
			continue
		}
		rightBuckets[key] = append(rightBuckets[key], row)
	}

	out := make([][][]byte, 0)
	for _, leftRow := range leftRows {
		key, ok := joinKey(plan.Join.LeftKeys, leftRow)
		if !ok {
			continue
		}
		matches := rightBuckets[key]
		for _, rightRow := range matches {
			projected, err := projectJoinRow(plan, leftRow, rightRow)
			if err != nil {
				return nil, err
			}
			out = append(out, projected)
		}
	}
	return out, nil
}

func (h *runtimeQueryHandler) scanRows(ctx context.Context, plan chronossql.RangeScanPlan) ([]map[string]chronossql.Value, error) {
	scanned, err := h.kv.ScanRange(ctx, plan.StartKey, plan.EndKey, plan.StartInclusive, plan.EndInclusive)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]chronossql.Value, 0, len(scanned))
	for _, item := range scanned {
		row, err := chronossql.DecodeRowValue(plan.Table, item.Value)
		if err != nil {
			return nil, wrapExecutionError("decode scanned row", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func aggregateGroupKey(columns []chronossql.ColumnDescriptor, row map[string]chronossql.Value) string {
	if len(columns) == 0 {
		return ""
	}
	var builder strings.Builder
	for i, column := range columns {
		if i > 0 {
			builder.WriteString("|")
		}
		value, _ := lookupValue(row, column.Name)
		builder.WriteString(column.Name)
		builder.WriteString("=")
		builder.WriteString(valueKey(value))
	}
	return builder.String()
}

func joinKey(qualifiedColumns []string, row map[string]chronossql.Value) (string, bool) {
	if len(qualifiedColumns) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(qualifiedColumns))
	for _, qualified := range qualifiedColumns {
		column := qualified
		if idx := strings.IndexByte(qualified, '.'); idx >= 0 {
			column = qualified[idx+1:]
		}
		value, ok := lookupValue(row, column)
		if !ok {
			return "", false
		}
		parts = append(parts, valueKey(value))
	}
	return strings.Join(parts, "|"), true
}

func projectJoinRow(plan chronossql.HashJoinPlan, leftRow, rightRow map[string]chronossql.Value) ([][]byte, error) {
	out := make([][]byte, 0, len(plan.Projection))
	for _, projection := range plan.Projection {
		var (
			row map[string]chronossql.Value
			ok  bool
			val chronossql.Value
		)
		switch strings.ToLower(strings.TrimSpace(projection.SourceAlias)) {
		case strings.ToLower(strings.TrimSpace(plan.Left.Alias)):
			row = leftRow
		case strings.ToLower(strings.TrimSpace(plan.Right.Alias)):
			row = rightRow
		default:
			return nil, wrapExecutionError("join projection", fmt.Errorf("unknown join source alias %q", projection.SourceAlias))
		}
		val, ok = lookupValue(row, projection.SourceColumn.Name)
		if !ok {
			if projection.Output.Nullable {
				out = append(out, nil)
				continue
			}
			return nil, wrapExecutionError("join projection", fmt.Errorf("missing join column %q", projection.SourceColumn.Name))
		}
		formatted, err := chronossql.FormatValueText(val)
		if err != nil {
			return nil, wrapExecutionError("format join value", err)
		}
		out = append(out, formatted)
	}
	return out, nil
}

func lookupValue(row map[string]chronossql.Value, column string) (chronossql.Value, bool) {
	if row == nil {
		return chronossql.Value{}, false
	}
	value, ok := row[strings.ToLower(strings.TrimSpace(column))]
	return value, ok
}

func valueKey(value chronossql.Value) string {
	switch value.Type {
	case chronossql.ColumnTypeInt:
		return fmt.Sprintf("i:%d", value.Int64)
	case chronossql.ColumnTypeBytes:
		return "b:" + string(value.Bytes)
	case chronossql.ColumnTypeString:
		fallthrough
	default:
		return "s:" + value.String
	}
}

func wrapExecutionError(scope string, err error) pgwire.Error {
	return pgwire.Error{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  fmt.Sprintf("%s: %v", scope, err),
	}
}
