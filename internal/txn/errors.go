package txn

import (
	"encoding/hex"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

// ErrorClass is the top-level error taxonomy for transaction-facing failures.
type ErrorClass string

const (
	ErrorClassRetriable      ErrorClass = "Retriable"
	ErrorClassClientSemantic ErrorClass = "Client/Semantic"
	ErrorClassResourcePolicy ErrorClass = "Resource/Policy"
	ErrorClassFatal          ErrorClass = "Fatal"
)

// Code is the canonical machine-readable error code.
type Code string

const (
	CodeTxnRetrySerialization Code = "TXN_RETRY_SERIALIZATION"
	CodeTxnRetryWriteTooOld   Code = "TXN_RETRY_WRITE_TOO_OLD"
	CodeContentionBackoff     Code = "CONTENTION_BACKOFF"
	CodeTxnAborted            Code = "TXN_ABORTED"
)

// Error is the client-visible transaction error payload.
type Error struct {
	Code             Code
	Class            ErrorClass
	Message          string
	CorrelationID    string
	RestartTimestamp hlc.Timestamp
	RetryDelayMillis uint64
}

// Error implements error.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s (%s): %s", e.Code, e.Class, e.Message)
}

// Retryable reports whether the caller may safely retry the request.
func (e *Error) Retryable() bool {
	return e != nil && e.Class == ErrorClassRetriable
}

// NewSerializationRetryError reports a serialization restart with a restart timestamp.
func NewSerializationRetryError(record Record, restartTS hlc.Timestamp, message string) *Error {
	return &Error{
		Code:             CodeTxnRetrySerialization,
		Class:            ErrorClassRetriable,
		Message:          withDefaultMessage(message, "transaction retry required due to serialization"),
		CorrelationID:    correlationID(record),
		RestartTimestamp: restartTS,
	}
}

// NewWriteTooOldError reports a retryable write-too-old restart.
func NewWriteTooOldError(record Record, restartTS hlc.Timestamp, message string) *Error {
	return &Error{
		Code:             CodeTxnRetryWriteTooOld,
		Class:            ErrorClassRetriable,
		Message:          withDefaultMessage(message, "transaction write timestamp is too old"),
		CorrelationID:    correlationID(record),
		RestartTimestamp: restartTS,
	}
}

// NewAbortedError reports a terminal abort to the client.
func NewAbortedError(record Record, message string) *Error {
	return &Error{
		Code:          CodeTxnAborted,
		Class:         ErrorClassClientSemantic,
		Message:       withDefaultMessage(message, "transaction aborted"),
		CorrelationID: correlationID(record),
	}
}

// ContentionErrorFromDecision maps a lock-table wait decision into a retry-safe error.
func ContentionErrorFromDecision(record Record, decision LockDecision, retryDelayMillis uint64) (*Error, error) {
	if decision.Kind != LockDecisionWait {
		return nil, fmt.Errorf("txn error mapping: expected wait decision, got %q", decision.Kind)
	}
	message := "transaction is waiting on conflicting lock holder"
	if len(decision.WaitFor) > 0 {
		message = fmt.Sprintf("transaction is waiting on conflicting lock holder %s", correlationID(decision.WaitFor[0].Txn))
	}
	return &Error{
		Code:             CodeContentionBackoff,
		Class:            ErrorClassRetriable,
		Message:          message,
		CorrelationID:    correlationID(record),
		RetryDelayMillis: retryDelayMillis,
	}, nil
}

func correlationID(record Record) string {
	return hex.EncodeToString(record.ID[:])
}

func withDefaultMessage(message, fallback string) string {
	if message != "" {
		return message
	}
	return fallback
}
