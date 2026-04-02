package pgwire

import (
	"context"
	"testing"

	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
)

func TestSessionHandleStartup(t *testing.T) {
	t.Parallel()

	session := NewSession(nil)
	frames, err := session.HandleStartup(StartupMessage{
		ProtocolVersion: ProtocolVersion30,
		Parameters: map[string]string{
			"user": "chronos",
		},
	})
	if err != nil {
		t.Fatalf("handle startup: %v", err)
	}
	if len(frames) < 3 {
		t.Fatalf("startup frame count = %d, want at least 3", len(frames))
	}
	if got := frames[0][0]; got != 'R' {
		t.Fatalf("first frame tag = %q, want 'R'", got)
	}
	if got := frames[len(frames)-1][0]; got != 'Z' {
		t.Fatalf("last frame tag = %q, want 'Z'", got)
	}
}

func TestSessionHandleQuery(t *testing.T) {
	t.Parallel()

	session := NewSession(staticHandler{
		result: QueryResult{
			Fields: []FieldDescription{
				{Name: "id", DataTypeOID: 23, TypeSize: 4},
			},
			Rows: [][][]byte{
				{[]byte("1")},
				{[]byte("2")},
			},
			CommandTag: "SELECT 2",
		},
	})
	frames, close, err := session.HandleFrontend(context.Background(), Query{SQL: "select id from users"})
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if close {
		t.Fatalf("query should not close session")
	}
	if len(frames) != 5 {
		t.Fatalf("frame count = %d, want 5", len(frames))
	}
	if frames[0][0] != 'T' || frames[1][0] != 'D' || frames[3][0] != 'C' || frames[4][0] != 'Z' {
		t.Fatalf("unexpected frame tag sequence")
	}
}

func TestSessionCarriesTransactionStatus(t *testing.T) {
	t.Parallel()

	session := NewSession(staticHandler{
		result: QueryResult{CommandTag: "BEGIN"},
		setTx:  TxInTransaction,
	})
	frames, close, err := session.HandleFrontend(context.Background(), Query{SQL: "begin"})
	if err != nil {
		t.Fatalf("handle begin: %v", err)
	}
	if close {
		t.Fatalf("begin should not close session")
	}
	if len(frames) != 2 || frames[1][0] != 'Z' || TxStatus(frames[1][5]) != TxInTransaction {
		t.Fatalf("ready status = %q, want %q", frames[1][5], TxInTransaction)
	}
}

func TestSessionHandleHandlerError(t *testing.T) {
	t.Parallel()

	session := NewSession(staticHandler{
		err: Error{
			Severity: "ERROR",
			Code:     "42P01",
			Message:  "relation does not exist",
		},
	})
	frames, close, err := session.HandleFrontend(context.Background(), Query{SQL: "select * from missing"})
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if close {
		t.Fatalf("query error should not close session")
	}
	if len(frames) != 2 {
		t.Fatalf("frame count = %d, want 2", len(frames))
	}
	if frames[0][0] != 'E' || frames[1][0] != 'Z' {
		t.Fatalf("unexpected error frame sequence")
	}
}

func TestSessionHandleTerminate(t *testing.T) {
	t.Parallel()

	session := NewSession(nil)
	frames, close, err := session.HandleFrontend(context.Background(), Terminate{})
	if err != nil {
		t.Fatalf("handle terminate: %v", err)
	}
	if len(frames) != 0 || !close {
		t.Fatalf("terminate should close session without frames")
	}
}

func TestSessionHandleExtendedQueryCycle(t *testing.T) {
	t.Parallel()

	handler := staticHandler{
		prepared: PreparedQueryDescription{
			Query:             "select id, name from users where id = $1",
			ParameterTypes:    []chronossql.ColumnType{chronossql.ColumnTypeInt},
			ParameterTypeOIDs: []uint32{20},
			Result: QueryResult{
				Fields: []FieldDescription{
					{Name: "id", DataTypeOID: 20, TypeSize: 8},
					{Name: "name", DataTypeOID: 25, TypeSize: -1},
				},
			},
		},
		executed: QueryResult{
			Rows:       [][][]byte{{[]byte("7"), []byte("alice")}},
			CommandTag: "SELECT 1",
		},
	}
	session := NewSession(handler)

	frames, close, err := session.HandleFrontend(context.Background(), Parse{
		Name:  "stmt1",
		Query: "select id, name from users where id = $1",
	})
	if err != nil || close {
		t.Fatalf("parse: err=%v close=%v", err, close)
	}
	if len(frames) != 1 || frames[0][0] != '1' {
		t.Fatalf("unexpected parse frames")
	}

	frames, close, err = session.HandleFrontend(context.Background(), Bind{
		StatementName: "stmt1",
		Parameters:    []BoundParameter{{FormatCode: 0, Value: []byte("7")}},
	})
	if err != nil || close {
		t.Fatalf("bind: err=%v close=%v", err, close)
	}
	if len(frames) != 1 || frames[0][0] != '2' {
		t.Fatalf("unexpected bind frames")
	}

	frames, close, err = session.HandleFrontend(context.Background(), Describe{ObjectType: 'S', Name: "stmt1"})
	if err != nil || close {
		t.Fatalf("describe statement: err=%v close=%v", err, close)
	}
	if len(frames) != 2 || frames[0][0] != 't' || frames[1][0] != 'T' {
		t.Fatalf("unexpected statement describe frames")
	}

	frames, close, err = session.HandleFrontend(context.Background(), Execute{})
	if err != nil || close {
		t.Fatalf("execute: err=%v close=%v", err, close)
	}
	if len(frames) != 2 || frames[0][0] != 'D' || frames[1][0] != 'C' {
		t.Fatalf("unexpected execute frames")
	}

	frames, close, err = session.HandleFrontend(context.Background(), Sync{})
	if err != nil || close {
		t.Fatalf("sync: err=%v close=%v", err, close)
	}
	if len(frames) != 1 || frames[0][0] != 'Z' {
		t.Fatalf("unexpected sync frames")
	}
}

type staticHandler struct {
	result   QueryResult
	err      error
	setTx    TxStatus
	prepared PreparedQueryDescription
	executed QueryResult
}

func (h staticHandler) HandleSimpleQuery(_ context.Context, session *Session, _ string) (QueryResult, error) {
	if h.setTx != 0 {
		session.SetTxStatus(h.setTx)
	}
	if h.err != nil {
		return QueryResult{}, h.err
	}
	return h.result, nil
}

func (h staticHandler) PrepareQuery(_ context.Context, _ *Session, _ string, _ []uint32) (PreparedQueryDescription, error) {
	if h.err != nil {
		return PreparedQueryDescription{}, h.err
	}
	return h.prepared, nil
}

func (h staticHandler) ExecutePreparedQuery(_ context.Context, _ *Session, _ PreparedQueryDescription, _ []BoundParameter) (QueryResult, error) {
	if h.err != nil {
		return QueryResult{}, h.err
	}
	return h.executed, nil
}
