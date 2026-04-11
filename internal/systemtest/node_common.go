package systemtest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
	"github.com/VenkatGGG/ChronosDb/internal/placement"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

type faultInjectingHandler struct {
	mu       sync.Mutex
	delegate pgwire.QueryHandler
	fault    *AmbiguousCommitSpec
	record   func(string)
}

const (
	DefaultPGWireUser     = "chronos"
	DefaultPGWirePassword = "chronos"
)

type terminateSessionError struct {
	message string
}

func (h *faultInjectingHandler) HandleSimpleQuery(ctx context.Context, session *pgwire.Session, query string) (pgwire.QueryResult, error) {
	spec, match := h.matchingFault(query)
	if match {
		if h.record != nil {
			h.record(fmt.Sprintf("ambiguous commit fault triggered for label %q", spec.TxnLabel))
		}
		timer := time.NewTimer(spec.AckDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return pgwire.QueryResult{}, ctx.Err()
		case <-timer.C:
		}
		if spec.DropResponse {
			if h.record != nil {
				h.record(fmt.Sprintf("ambiguous commit response dropped for label %q", spec.TxnLabel))
			}
			return pgwire.QueryResult{}, terminateSessionError{
				message: fmt.Sprintf("systemtest: dropped response for %q", spec.TxnLabel),
			}
		}
	}
	return h.delegate.HandleSimpleQuery(ctx, session, query)
}

func (h *faultInjectingHandler) PrepareQuery(ctx context.Context, session *pgwire.Session, query string, parameterTypeOIDs []uint32) (pgwire.PreparedQueryDescription, error) {
	extended, ok := h.delegate.(pgwire.PreparedQueryHandler)
	if !ok {
		return pgwire.PreparedQueryDescription{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "0A000",
			Message:  "extended query protocol is not configured",
		}
	}
	return extended.PrepareQuery(ctx, session, query, parameterTypeOIDs)
}

func (h *faultInjectingHandler) ExecutePreparedQuery(ctx context.Context, session *pgwire.Session, prepared pgwire.PreparedQueryDescription, params []pgwire.BoundParameter) (pgwire.QueryResult, error) {
	extended, ok := h.delegate.(pgwire.PreparedQueryHandler)
	if !ok {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "0A000",
			Message:  "extended query protocol is not configured",
		}
	}
	query, err := chronossql.RenderPreparedQuery(prepared.Query, sampleOrActualPreparedValues(prepared.ParameterTypes, params))
	if err == nil {
		if spec, match := h.matchingFault(query); match {
			if h.record != nil {
				h.record(fmt.Sprintf("ambiguous commit fault triggered for label %q", spec.TxnLabel))
			}
			timer := time.NewTimer(spec.AckDelay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return pgwire.QueryResult{}, ctx.Err()
			case <-timer.C:
			}
			if spec.DropResponse {
				if h.record != nil {
					h.record(fmt.Sprintf("ambiguous commit response dropped for label %q", spec.TxnLabel))
				}
				return pgwire.QueryResult{}, terminateSessionError{
					message: fmt.Sprintf("systemtest: dropped response for %q", spec.TxnLabel),
				}
			}
		}
	}
	return extended.ExecutePreparedQuery(ctx, session, prepared, params)
}

func (h *faultInjectingHandler) Install(spec AmbiguousCommitSpec) {
	h.mu.Lock()
	defer h.mu.Unlock()
	copySpec := spec
	h.fault = &copySpec
}

func (h *faultInjectingHandler) HasFault() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fault != nil
}

func (h *faultInjectingHandler) matchingFault(query string) (AmbiguousCommitSpec, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.fault == nil || !strings.Contains(query, h.fault.TxnLabel) {
		return AmbiguousCommitSpec{}, false
	}
	spec := *h.fault
	h.fault = nil
	return spec, true
}

func (e terminateSessionError) Error() string {
	return e.message
}

func (e terminateSessionError) TerminateSession() bool {
	return true
}

func (h *faultInjectingHandler) CloseSession(ctx context.Context, session *pgwire.Session) error {
	closer, ok := h.delegate.(pgwire.SessionCloseHandler)
	if !ok {
		return nil
	}
	return closer.CloseSession(ctx, session)
}

func defaultSystemTestCatalog() (*chronossql.Catalog, error) {
	catalog := chronossql.NewCatalog()
	if err := catalog.AddTable(chronossql.TableDescriptor{
		ID:   7,
		Name: "users",
		Columns: []chronossql.ColumnDescriptor{
			{ID: 1, Name: "id", Type: chronossql.ColumnTypeInt},
			{ID: 2, Name: "name", Type: chronossql.ColumnTypeString},
			{ID: 3, Name: "email", Type: chronossql.ColumnTypeString, Nullable: true},
		},
		PrimaryKey: []string{"id"},
		Indexes: []chronossql.IndexDescriptor{
			{ID: 1, Name: "users_name_idx", Columns: []string{"name"}},
			{ID: 2, Name: "users_email_key", Columns: []string{"email"}, Unique: true},
		},
		Stats: chronossql.TableStats{
			EstimatedRows:   10000,
			AverageRowBytes: 192,
		},
	}); err != nil {
		return nil, err
	}
	if err := catalog.AddTable(chronossql.TableDescriptor{
		ID:   9,
		Name: "orders",
		Columns: []chronossql.ColumnDescriptor{
			{ID: 1, Name: "id", Type: chronossql.ColumnTypeInt},
			{ID: 2, Name: "user_id", Type: chronossql.ColumnTypeInt},
			{ID: 3, Name: "region", Type: chronossql.ColumnTypeString},
			{ID: 4, Name: "sales", Type: chronossql.ColumnTypeInt},
		},
		PrimaryKey: []string{"id"},
		Stats: chronossql.TableStats{
			EstimatedRows:   10000,
			AverageRowBytes: 96,
		},
		PlacementPolicy: &placement.Policy{
			PlacementMode:    placement.ModeRegional,
			PreferredRegions: []string{"us-east1"},
		},
	}); err != nil {
		return nil, err
	}
	return catalog, nil
}

// DefaultCatalog returns the built-in demo catalog used by the process-node and
// local-controller runtimes.
func DefaultCatalog() (*chronossql.Catalog, error) {
	return defaultSystemTestCatalog()
}

func sampleOrActualPreparedValues(types []chronossql.ColumnType, params []pgwire.BoundParameter) []chronossql.Value {
	values := chronossql.SamplePreparedValues(types)
	if len(values) != len(params) {
		return values
	}
	for i, param := range params {
		if param.IsNull {
			continue
		}
		switch types[i] {
		case chronossql.ColumnTypeInt:
			if parsed, err := strconv.ParseInt(string(param.Value), 10, 64); err == nil {
				values[i] = chronossql.Value{Type: chronossql.ColumnTypeInt, Int64: parsed}
			}
		case chronossql.ColumnTypeBytes:
			values[i] = chronossql.Value{Type: chronossql.ColumnTypeBytes, Bytes: append([]byte(nil), param.Value...)}
		case chronossql.ColumnTypeString:
			fallthrough
		default:
			values[i] = chronossql.Value{Type: chronossql.ColumnTypeString, String: string(param.Value)}
		}
	}
	return values
}
