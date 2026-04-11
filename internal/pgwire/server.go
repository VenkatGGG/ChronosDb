package pgwire

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// Server serves PostgreSQL wire-protocol sessions over network connections.
type Server struct {
	handler       QueryHandler
	authenticator Authenticator
}

// NewServer constructs a pgwire server backed by the given query handler.
func NewServer(handler QueryHandler, authenticator Authenticator) *Server {
	return &Server{handler: handler, authenticator: authenticator}
}

// ServeListener accepts and serves connections until the listener fails or the context is canceled.
func (s *Server) ServeListener(ctx context.Context, listener net.Listener) error {
	var once sync.Once
	go func() {
		<-ctx.Done()
		once.Do(func() { _ = listener.Close() })
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Temporary() {
				continue
			}
			return err
		}
		go func() {
			_ = s.ServeConn(ctx, conn)
		}()
	}
}

// ServeConn runs one startup handshake plus the simple-query message loop.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	defer conn.Close()

	startup, err := decodeStartupNegotiation(conn)
	if err != nil {
		return err
	}
	if startup.ProtocolVersion != ProtocolVersion30 {
		wireErr := Error{
			Severity: "FATAL",
			Code:     "08P01",
			Message:  "unsupported protocol version",
		}
		_ = writeFrames(conn, [][]byte{EncodeErrorResponse(wireErr.Severity, wireErr.Code, wireErr.Message)})
		return wireErr
	}
	principal, err := s.authenticate(ctx, conn, startup)
	if err != nil {
		if wireErr, ok := asWireAuthError(err); ok {
			_ = writeFrames(conn, [][]byte{EncodeErrorResponse(wireErr.Severity, wireErr.Code, wireErr.Message)})
		}
		return err
	}
	session := NewSession(s.handler, principal)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = session.Close(closeCtx)
		cancel()
	}()
	frames, err := session.HandleStartup(startup)
	if err != nil {
		if wireErr, ok := asWireAuthError(err); ok {
			_ = writeFrames(conn, [][]byte{EncodeErrorResponse(wireErr.Severity, wireErr.Code, wireErr.Message)})
		}
		return err
	}
	if err := writeFrames(conn, frames); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		message, err := DecodeFrontendMessage(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		frames, closeSession, err := session.HandleFrontend(ctx, message)
		if err != nil {
			return err
		}
		if err := writeFrames(conn, frames); err != nil {
			return err
		}
		if closeSession {
			return nil
		}
	}
}

func (s *Server) authenticate(ctx context.Context, conn net.Conn, startup StartupMessage) (Principal, error) {
	if s == nil || s.authenticator == nil {
		return Principal{}, Error{
			Severity: "FATAL",
			Code:     "28000",
			Message:  "pgwire authentication is not configured",
		}
	}
	return s.authenticator.Authenticate(ctx, conn, startup, conn.RemoteAddr())
}

func asWireAuthError(err error) (Error, bool) {
	var wireErr Error
	if errors.As(err, &wireErr) {
		if wireErr.Severity == "" {
			wireErr.Severity = "FATAL"
		}
		if wireErr.Code == "" {
			wireErr.Code = "28000"
		}
		return wireErr, true
	}
	return Error{}, false
}

func decodeStartupNegotiation(rw io.ReadWriter) (StartupMessage, error) {
	for {
		msg, err := DecodeStartup(rw)
		if err != nil {
			return StartupMessage{}, err
		}
		switch msg.ProtocolVersion {
		case SSLRequestCode, GSSENCRequestCode:
			if _, err := rw.Write([]byte{'N'}); err != nil {
				return StartupMessage{}, err
			}
			continue
		default:
			return msg, nil
		}
	}
}

func writeFrames(w io.Writer, frames [][]byte) error {
	for _, frame := range frames {
		if len(frame) == 0 {
			continue
		}
		if _, err := w.Write(frame); err != nil {
			return err
		}
	}
	return nil
}
