package pgwire

import (
	"context"
	"fmt"

	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

// PlanningHandler adapts the SQL planner and flow planner onto the simple-query wire path.
type PlanningHandler struct {
	planner     *chronossql.Planner
	flowPlanner *chronossql.FlowPlanner
}

// PreparedQueryDescription captures the wire-visible metadata for one prepared
// statement before a Bind/Execute cycle supplies concrete values.
type PreparedQueryDescription struct {
	Query             string
	ParameterTypes    []chronossql.ColumnType
	ParameterTypeOIDs []uint32
	Result            QueryResult
}

// NewPlanningHandler constructs a pgwire simple-query handler backed by the SQL layer.
func NewPlanningHandler(planner *chronossql.Planner, flowPlanner *chronossql.FlowPlanner) *PlanningHandler {
	return &PlanningHandler{
		planner:     planner,
		flowPlanner: flowPlanner,
	}
}

// DescribeQuery validates the SQL statement and returns its wire-level description.
func (h *PlanningHandler) DescribeQuery(query string) (chronossql.OptimizedPlan, chronossql.FlowPlan, QueryResult, error) {
	if h == nil || h.planner == nil || h.flowPlanner == nil {
		return chronossql.OptimizedPlan{}, chronossql.FlowPlan{}, QueryResult{}, Error{
			Severity: "ERROR",
			Code:     "08006",
			Message:  "planning handler is not configured",
		}
	}
	optimized, err := h.planner.Optimize(query)
	if err != nil {
		return chronossql.OptimizedPlan{}, chronossql.FlowPlan{}, QueryResult{}, wrapPlannerError(err)
	}
	flow, err := h.flowPlanner.Build(optimized.Selected.Plan)
	if err != nil {
		return chronossql.OptimizedPlan{}, chronossql.FlowPlan{}, QueryResult{}, wrapPlannerError(err)
	}
	result, err := describeResult(optimized.Selected.Plan, flow)
	if err != nil {
		return chronossql.OptimizedPlan{}, chronossql.FlowPlan{}, QueryResult{}, wrapPlannerError(err)
	}
	return optimized, flow, result, nil
}

// DescribePreparedQuery validates one positional-parameter SQL statement and
// returns the planner-owned parameter contract plus the row/command metadata
// clients need during Parse and Describe.
func (h *PlanningHandler) DescribePreparedQuery(query string) (PreparedQueryDescription, error) {
	if h == nil || h.planner == nil || h.flowPlanner == nil {
		return PreparedQueryDescription{}, Error{
			Severity: "ERROR",
			Code:     "08006",
			Message:  "planning handler is not configured",
		}
	}
	prepared, err := h.planner.Prepare(query)
	if err != nil {
		return PreparedQueryDescription{}, wrapPlannerError(err)
	}
	sampleQuery, err := chronossql.RenderPreparedQuery(query, chronossql.SamplePreparedValues(prepared.ParameterTypes))
	if err != nil {
		return PreparedQueryDescription{}, wrapPlannerError(err)
	}
	_, _, result, err := h.DescribeQuery(sampleQuery)
	if err != nil {
		return PreparedQueryDescription{}, err
	}
	oids := make([]uint32, 0, len(prepared.ParameterTypes))
	for _, columnType := range prepared.ParameterTypes {
		oid, _ := postgresType(columnType)
		oids = append(oids, oid)
	}
	return PreparedQueryDescription{
		Query:             prepared.OriginalQuery,
		ParameterTypes:    prepared.ParameterTypes,
		ParameterTypeOIDs: oids,
		Result:            result,
	}, nil
}

// HandleSimpleQuery validates the SQL statement, derives its flow plan, and
// returns row metadata plus a command tag. Execution is still delegated to later phases.
func (h *PlanningHandler) HandleSimpleQuery(ctx context.Context, session *Session, query string) (QueryResult, error) {
	_, _, result, err := h.DescribeQuery(query)
	return result, err
}

func describeResult(plan chronossql.Plan, flow chronossql.FlowPlan) (QueryResult, error) {
	fields := describeResultSchema(flow.ResultSchema)
	switch plan.(type) {
	case chronossql.PointLookupPlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "SELECT 0",
		}, nil
	case chronossql.RangeScanPlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "SELECT 0",
		}, nil
	case chronossql.AggregatePlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "SELECT 0",
		}, nil
	case chronossql.HashJoinPlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "SELECT 0",
		}, nil
	case chronossql.InsertPlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "INSERT 0 1",
		}, nil
	case chronossql.UpsertPlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "UPSERT 1",
		}, nil
	case chronossql.OnConflictPlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "INSERT 0 1",
		}, nil
	case chronossql.DeletePlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "DELETE 0",
		}, nil
	case chronossql.UpdatePlan:
		return QueryResult{
			Fields:     fields,
			CommandTag: "UPDATE 0",
		}, nil
	default:
		return QueryResult{}, fmt.Errorf("unsupported sql plan type %T", plan)
	}
}

func describeResultSchema(columns []chronossql.ResultColumn) []FieldDescription {
	fields := make([]FieldDescription, 0, len(columns))
	for _, column := range columns {
		oid, size := postgresType(column.Type)
		fields = append(fields, FieldDescription{
			Name:        column.Name,
			DataTypeOID: oid,
			TypeSize:    size,
		})
	}
	return fields
}

func postgresType(columnType chronossql.ColumnType) (uint32, int16) {
	switch columnType {
	case chronossql.ColumnTypeInt:
		return 20, 8
	case chronossql.ColumnTypeBytes:
		return 17, -1
	case chronossql.ColumnTypeString:
		fallthrough
	default:
		return 25, -1
	}
}

func wrapPlannerError(err error) Error {
	return Error{
		Severity: "ERROR",
		Code:     "0A000",
		Message:  err.Error(),
	}
}
