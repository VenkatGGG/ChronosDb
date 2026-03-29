package systemtest

import (
	"context"
	"fmt"
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
