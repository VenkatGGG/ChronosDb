package systemtest

import (
	"context"
	"fmt"

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
	case chronossql.InsertPlan:
		if err := h.kv.Put(ctx, plan.Key, plan.Value); err != nil {
			return pgwire.QueryResult{}, pgwire.Error{
				Severity: "ERROR",
				Code:     "XX000",
				Message:  err.Error(),
			}
		}
		return result, nil
	default:
		return result, nil
	}
}
