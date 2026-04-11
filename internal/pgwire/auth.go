package pgwire

import (
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"net"
	"strings"
)

// Principal is the authenticated identity bound to one pgwire session.
type Principal struct {
	User string
}

// Authenticator owns the startup authentication handshake for one session.
type Authenticator interface {
	Authenticate(ctx context.Context, rw io.ReadWriter, startup StartupMessage, remoteAddr net.Addr) (Principal, error)
}

// StaticPasswordAuthenticator authenticates users against an in-memory user/password map.
type StaticPasswordAuthenticator struct {
	passwords map[string]string
}

// NewStaticPasswordAuthenticator constructs an authenticator backed by fixed credentials.
func NewStaticPasswordAuthenticator(passwords map[string]string) (*StaticPasswordAuthenticator, error) {
	if len(passwords) == 0 {
		return nil, fmt.Errorf("pgwire: static password authenticator requires at least one user")
	}
	cloned := make(map[string]string, len(passwords))
	for user, password := range passwords {
		user = strings.TrimSpace(user)
		if user == "" {
			return nil, fmt.Errorf("pgwire: static password authenticator user must not be empty")
		}
		cloned[user] = password
	}
	return &StaticPasswordAuthenticator{passwords: cloned}, nil
}

// Authenticate challenges the client for a cleartext password and verifies it.
func (a *StaticPasswordAuthenticator) Authenticate(_ context.Context, rw io.ReadWriter, startup StartupMessage, _ net.Addr) (Principal, error) {
	if a == nil {
		return Principal{}, Error{
			Severity: "FATAL",
			Code:     "28000",
			Message:  "pgwire authentication is not configured",
		}
	}
	user := strings.TrimSpace(startup.Parameters["user"])
	if user == "" {
		return Principal{}, Error{
			Severity: "FATAL",
			Code:     "28000",
			Message:  "startup user is required",
		}
	}
	expectedPassword, ok := a.passwords[user]
	if !ok {
		return Principal{}, Error{
			Severity: "FATAL",
			Code:     "28000",
			Message:  fmt.Sprintf("unknown user %q", user),
		}
	}
	if _, err := rw.Write(EncodeAuthenticationCleartextPassword()); err != nil {
		return Principal{}, err
	}
	msg, err := DecodeFrontendMessage(rw)
	if err != nil {
		return Principal{}, err
	}
	passwordMsg, ok := msg.(PasswordMessage)
	if !ok {
		return Principal{}, Error{
			Severity: "FATAL",
			Code:     "08P01",
			Message:  "password response required during authentication",
		}
	}
	if subtle.ConstantTimeCompare([]byte(passwordMsg.Password), []byte(expectedPassword)) != 1 {
		return Principal{}, Error{
			Severity: "FATAL",
			Code:     "28P01",
			Message:  fmt.Sprintf("password authentication failed for user %q", user),
		}
	}
	if _, err := rw.Write(EncodeAuthenticationOK()); err != nil {
		return Principal{}, err
	}
	return Principal{User: user}, nil
}
