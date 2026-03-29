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
}

// NewSession constructs a session with default PostgreSQL bootstrap parameters.
func NewSession(handler QueryHandler) *Session {
	return &Session{
		handler: handler,
		txStatus: TxIdle,
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
	case Terminate:
		return nil, true, nil
	default:
		return nil, false, fmt.Errorf("pgwire: unsupported frontend message %T", msg)
	}
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
