package pgwire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDecodeStartup(t *testing.T) {
	t.Parallel()

	var frame bytes.Buffer
	writeInt32(&frame, int32(4+4+len("user")+1+len("chronos")+1+1))
	writeUint32(&frame, ProtocolVersion30)
	writeCString(&frame, "user")
	writeCString(&frame, "chronos")
	frame.WriteByte(0)

	msg, err := DecodeStartup(bytes.NewReader(frame.Bytes()))
	if err != nil {
		t.Fatalf("decode startup: %v", err)
	}
	if msg.ProtocolVersion != ProtocolVersion30 {
		t.Fatalf("protocol = %d, want %d", msg.ProtocolVersion, ProtocolVersion30)
	}
	if got := msg.Parameters["user"]; got != "chronos" {
		t.Fatalf("startup parameter user = %q, want chronos", got)
	}
}

func TestDecodeFrontendQuery(t *testing.T) {
	t.Parallel()

	frame := make([]byte, 0, 64)
	frame = append(frame, 'Q')
	frame = append(frame, encodeLengthPrefixedPayload(append([]byte("select 1"), 0))...)

	msg, err := DecodeFrontendMessage(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode query: %v", err)
	}
	query, ok := msg.(Query)
	if !ok {
		t.Fatalf("frontend message type = %T, want Query", msg)
	}
	if query.SQL != "select 1" {
		t.Fatalf("query sql = %q, want %q", query.SQL, "select 1")
	}
}

func TestEncodeDataRow(t *testing.T) {
	t.Parallel()

	frame := EncodeDataRow([][]byte{[]byte("alice"), nil})
	if got := frame[0]; got != 'D' {
		t.Fatalf("tag = %q, want 'D'", got)
	}
	if length := binary.BigEndian.Uint32(frame[1:5]); length == 0 {
		t.Fatalf("row frame length must be non-zero")
	}
}

func encodeLengthPrefixedPayload(payload []byte) []byte {
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)+4))
	copy(buf[4:], payload)
	return buf
}
