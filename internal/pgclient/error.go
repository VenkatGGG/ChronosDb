package pgclient

import "fmt"

// ServerError exposes structured backend error metadata decoded from pgwire.
type ServerError struct {
	Severity string
	Code     string
	Message  string
}

func (e ServerError) Error() string {
	message := e.Message
	if message == "" {
		message = "unknown pgwire error"
	}
	if e.Code != "" {
		return fmt.Sprintf("%s (%s)", message, e.Code)
	}
	return message
}

// Class returns the SQLSTATE class prefix when one is available.
func (e ServerError) Class() string {
	if len(e.Code) >= 2 {
		return e.Code[:2]
	}
	return ""
}
