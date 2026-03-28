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
	HandleSimpleQuery(ctx context.Context, query string) (QueryResult, error)
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

// Session is the protocol-level PostgreSQL session skeleton.
type Session struct {
	serverParameters map[string]string
	handler          QueryHandler
}

// NewSession constructs a session with default PostgreSQL bootstrap parameters.
func NewSession(handler QueryHandler) *Session {
	return &Session{
		handler: handler,
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
	frames = append(frames, EncodeReadyForQuery(TxIdle))
	return frames, nil
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
			}), false, nil
		}
		result, handlerErr := s.handler.HandleSimpleQuery(ctx, typed.SQL)
		if handlerErr != nil {
			return errorFrames(asWireError(handlerErr)), false, nil
		}
		frames = make([][]byte, 0, len(result.Rows)+3)
		if len(result.Fields) > 0 {
			frames = append(frames, EncodeRowDescription(result.Fields))
			for _, row := range result.Rows {
				frames = append(frames, EncodeDataRow(row))
			}
		}
		frames = append(frames, EncodeCommandComplete(result.CommandTag))
		frames = append(frames, EncodeReadyForQuery(TxIdle))
		return frames, false, nil
	case Terminate:
		return nil, true, nil
	default:
		return nil, false, fmt.Errorf("pgwire: unsupported frontend message %T", msg)
	}
}

func errorFrames(err Error) [][]byte {
	return [][]byte{
		EncodeErrorResponse(err.Severity, err.Code, err.Message),
		EncodeReadyForQuery(TxIdle),
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
