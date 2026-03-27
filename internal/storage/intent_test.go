package storage

import (
	"bytes"
	"testing"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
)

func TestIntentBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	intent := Intent{
		TxnID:          TxnID{1, 2, 3, 4, 5, 6, 7, 8},
		Epoch:          3,
		WriteTimestamp: hlc.Timestamp{WallTime: 123, Logical: 9},
		Strength:       IntentStrengthExclusive,
		Value:          []byte("provisional-value"),
	}

	payload, err := intent.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal intent: %v", err)
	}

	var decoded Intent
	if err := decoded.UnmarshalBinary(payload); err != nil {
		t.Fatalf("unmarshal intent: %v", err)
	}

	if decoded.TxnID != intent.TxnID {
		t.Fatalf("txn id mismatch: got %v want %v", decoded.TxnID, intent.TxnID)
	}
	if decoded.Epoch != intent.Epoch {
		t.Fatalf("epoch mismatch: got %d want %d", decoded.Epoch, intent.Epoch)
	}
	if decoded.Strength != intent.Strength {
		t.Fatalf("strength mismatch: got %d want %d", decoded.Strength, intent.Strength)
	}
	if decoded.WriteTimestamp.Compare(intent.WriteTimestamp) != 0 {
		t.Fatalf("timestamp mismatch: got %v want %v", decoded.WriteTimestamp, intent.WriteTimestamp)
	}
	if !bytes.Equal(decoded.Value, intent.Value) {
		t.Fatalf("value mismatch: got %q want %q", decoded.Value, intent.Value)
	}
}
