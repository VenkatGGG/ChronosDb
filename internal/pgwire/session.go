package pgwire

import (
	"context"
	"errors"
	"fmt"
)

// QueryResult is one simple-query response set.
type QueryResult struct {
	Fields     []FieldDescription
	Rows       [][][]byte
	CommandTag string
}

// QueryHandler executes one simple query string.
type QueryHandler interface {
	HandleSimpleQuery(ctx context.Context, session *Session, query string) (QueryResult, error)
}

// PreparedQueryHandler extends the query handler with the Parse/Bind/Execute
// flow needed by PostgreSQL clients that use the extended protocol.
type PreparedQueryHandler interface {
	PrepareQuery(ctx context.Context, session *Session, query string, parameterTypeOIDs []uint32) (PreparedQueryDescription, error)
	ExecutePreparedQuery(ctx context.Context, session *Session, prepared PreparedQueryDescription, params []BoundParameter) (QueryResult, error)
}

// SessionCloseHandler allows handlers to clean up per-session resources when a
// connection terminates.
type SessionCloseHandler interface {
	CloseSession(ctx context.Context, session *Session) error
}

// Error exposes explicit backend error metadata when a handler wants control over the wire code.
type Error struct {
	Severity string
	Code     string
	Message  string
}

func (e Error) Error() string {
	return e.Message
}

// SessionTerminationError allows a query handler to terminate the current
// session without sending an error response frame.
type SessionTerminationError interface {
	error
	TerminateSession() bool
}

// Session is the protocol-level PostgreSQL session skeleton.
type Session struct {
	serverParameters map[string]string
	handler          QueryHandler
	txStatus         TxStatus
	prepared         map[string]PreparedQueryDescription
	portals          map[string]boundPortal
	discardUntilSync bool
}

type boundPortal struct {
	prepared         PreparedQueryDescription
	parameters       []BoundParameter
	resultFormatCode []uint16
}

// NewSession constructs a session with default PostgreSQL bootstrap parameters.
func NewSession(handler QueryHandler) *Session {
	return &Session{
		handler:  handler,
		txStatus: TxIdle,
		prepared: make(map[string]PreparedQueryDescription),
		portals:  make(map[string]boundPortal),
		serverParameters: map[string]string{
			"client_encoding": "UTF8",
			"server_version":  "ChronosDB dev",
		},
	}
}

// HandleStartup validates the startup frame and returns bootstrap responses.
func (s *Session) HandleStartup(msg StartupMessage) ([][]byte, error) {
	if msg.ProtocolVersion != ProtocolVersion30 {
		return nil, fmt.Errorf("pgwire: unsupported protocol version %d", msg.ProtocolVersion)
	}
	frames := make([][]byte, 0, len(s.serverParameters)+2)
	frames = append(frames, EncodeAuthenticationOK())
	frames = append(frames, StartupParameterFrames(s.serverParameters)...)
	frames = append(frames, EncodeReadyForQuery(s.txStatus))
	return frames, nil
}

// TxStatus returns the current ReadyForQuery transaction state for the session.
func (s *Session) TxStatus() TxStatus {
	return s.txStatus
}

// SetTxStatus updates the ReadyForQuery transaction state for the session.
func (s *Session) SetTxStatus(status TxStatus) {
	s.txStatus = status
}

// Close releases any handler-managed session resources.
func (s *Session) Close(ctx context.Context) error {
	if s == nil || s.handler == nil {
		return nil
	}
	closer, ok := s.handler.(SessionCloseHandler)
	if !ok {
		return nil
	}
	return closer.CloseSession(ctx, s)
}

// HandleFrontend processes one frontend message into backend frames.
func (s *Session) HandleFrontend(ctx context.Context, msg FrontendMessage) (frames [][]byte, close bool, err error) {
	if s.discardUntilSync {
		switch msg.(type) {
		case Sync:
			s.discardUntilSync = false
			return [][]byte{EncodeReadyForQuery(s.txStatus)}, false, nil
		case Terminate:
			return nil, true, nil
		default:
			return nil, false, nil
		}
	}
	switch typed := msg.(type) {
	case Query:
		if s.handler == nil {
			return errorFrames(Error{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  "simple-query handler is not configured",
			}, s.txStatus), false, nil
		}
		result, handlerErr := s.handler.HandleSimpleQuery(ctx, s, typed.SQL)
		if handlerErr != nil {
			var terminationErr SessionTerminationError
			if errors.As(handlerErr, &terminationErr) && terminationErr.TerminateSession() {
				return nil, true, nil
			}
			return errorFrames(asWireError(handlerErr), s.txStatus), false, nil
		}
		frames = make([][]byte, 0, len(result.Rows)+3)
		if len(result.Fields) > 0 {
			frames = append(frames, EncodeRowDescription(result.Fields))
			for _, row := range result.Rows {
				frames = append(frames, EncodeDataRow(row))
			}
		}
		frames = append(frames, EncodeCommandComplete(result.CommandTag))
		frames = append(frames, EncodeReadyForQuery(s.txStatus))
		return frames, false, nil
	case Parse:
		handler, ok := s.handler.(PreparedQueryHandler)
		if !ok {
			return s.extendedError(Error{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  "extended query protocol is not configured",
			}), false, nil
		}
		prepared, handlerErr := handler.PrepareQuery(ctx, s, typed.Query, typed.ParameterTypeOIDs)
		if handlerErr != nil {
			return s.extendedError(asWireError(handlerErr)), false, nil
		}
		s.prepared[typed.Name] = prepared
		return [][]byte{EncodeParseComplete()}, false, nil
	case Bind:
		prepared, ok := s.prepared[typed.StatementName]
		if !ok {
			return s.extendedError(Error{
				Severity: "ERROR",
				Code:     "26000",
				Message:  "prepared statement does not exist",
			}), false, nil
		}
		if len(prepared.ParameterTypeOIDs) != len(typed.Parameters) {
			return s.extendedError(Error{
				Severity: "ERROR",
				Code:     "08P01",
				Message:  "bind parameter count does not match prepared statement",
			}), false, nil
		}
		for _, code := range typed.ResultFormatCodes {
			if code != 0 {
				return s.extendedError(Error{
					Severity: "ERROR",
					Code:     "0A000",
					Message:  "binary result formats are unsupported",
				}), false, nil
			}
		}
		s.portals[typed.PortalName] = boundPortal{
			prepared:         prepared,
			parameters:       append([]BoundParameter(nil), typed.Parameters...),
			resultFormatCode: append([]uint16(nil), typed.ResultFormatCodes...),
		}
		return [][]byte{EncodeBindComplete()}, false, nil
	case Describe:
		switch typed.ObjectType {
		case 'S':
			prepared, ok := s.prepared[typed.Name]
			if !ok {
				return s.extendedError(Error{
					Severity: "ERROR",
					Code:     "26000",
					Message:  "prepared statement does not exist",
				}), false, nil
			}
			frames := [][]byte{EncodeParameterDescription(prepared.ParameterTypeOIDs)}
			if len(prepared.Result.Fields) == 0 {
				frames = append(frames, EncodeNoData())
			} else {
				frames = append(frames, EncodeRowDescription(prepared.Result.Fields))
			}
			return frames, false, nil
		case 'P':
			portal, ok := s.portals[typed.Name]
			if !ok {
				return s.extendedError(Error{
					Severity: "ERROR",
					Code:     "34000",
					Message:  "portal does not exist",
				}), false, nil
			}
			if len(portal.prepared.Result.Fields) == 0 {
				return [][]byte{EncodeNoData()}, false, nil
			}
			return [][]byte{EncodeRowDescription(portal.prepared.Result.Fields)}, false, nil
		default:
			return s.extendedError(Error{
				Severity: "ERROR",
				Code:     "08P01",
				Message:  "Describe target must be a statement or portal",
			}), false, nil
		}
	case Execute:
		if typed.MaxRows != 0 {
			return s.extendedError(Error{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  "partial portal execution is unsupported",
			}), false, nil
		}
		portal, ok := s.portals[typed.PortalName]
		if !ok {
			return s.extendedError(Error{
				Severity: "ERROR",
				Code:     "34000",
				Message:  "portal does not exist",
			}), false, nil
		}
		handler, ok := s.handler.(PreparedQueryHandler)
		if !ok {
			return s.extendedError(Error{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  "extended query protocol is not configured",
			}), false, nil
		}
		result, handlerErr := handler.ExecutePreparedQuery(ctx, s, portal.prepared, portal.parameters)
		if handlerErr != nil {
			return s.extendedError(asWireError(handlerErr)), false, nil
		}
		frames = make([][]byte, 0, len(result.Rows)+1)
		for _, row := range result.Rows {
			frames = append(frames, EncodeDataRow(row))
		}
		frames = append(frames, EncodeCommandComplete(result.CommandTag))
		return frames, false, nil
	case Close:
		switch typed.ObjectType {
		case 'S':
			delete(s.prepared, typed.Name)
		case 'P':
			delete(s.portals, typed.Name)
		default:
			return s.extendedError(Error{
				Severity: "ERROR",
				Code:     "08P01",
				Message:  "Close target must be a statement or portal",
			}), false, nil
		}
		return [][]byte{EncodeCloseComplete()}, false, nil
	case Sync:
		return [][]byte{EncodeReadyForQuery(s.txStatus)}, false, nil
	case Flush:
		return nil, false, nil
	case Terminate:
		return nil, true, nil
	default:
		return nil, false, fmt.Errorf("pgwire: unsupported frontend message %T", msg)
	}
}

func (s *Session) extendedError(err Error) [][]byte {
	s.discardUntilSync = true
	return [][]byte{EncodeErrorResponse(err.Severity, err.Code, err.Message)}
}

func errorFrames(err Error, status TxStatus) [][]byte {
	return [][]byte{
		EncodeErrorResponse(err.Severity, err.Code, err.Message),
		EncodeReadyForQuery(status),
	}
}

func asWireError(err error) Error {
	var wireErr Error
	if ok := errors.As(err, &wireErr); ok {
		if wireErr.Severity == "" {
			wireErr.Severity = "ERROR"
		}
		if wireErr.Code == "" {
			wireErr.Code = "XX000"
		}
		return wireErr
	}
	return Error{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  err.Error(),
	}
}
