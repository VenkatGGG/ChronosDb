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

func TestDecodeFrontendParse(t *testing.T) {
	t.Parallel()

	var payload bytes.Buffer
	writeCString(&payload, "stmt1")
	writeCString(&payload, "select id from users where id = $1")
	writeUint16(&payload, 1)
	writeUint32(&payload, 20)

	frame := append([]byte{'P'}, encodeLengthPrefixedPayload(payload.Bytes())...)
	msg, err := DecodeFrontendMessage(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode parse: %v", err)
	}
	parse, ok := msg.(Parse)
	if !ok {
		t.Fatalf("frontend message type = %T, want Parse", msg)
	}
	if parse.Name != "stmt1" || parse.Query != "select id from users where id = $1" {
		t.Fatalf("unexpected parse payload: %+v", parse)
	}
	if len(parse.ParameterTypeOIDs) != 1 || parse.ParameterTypeOIDs[0] != 20 {
		t.Fatalf("parameter oids = %#v, want [20]", parse.ParameterTypeOIDs)
	}
}

func TestDecodeFrontendBind(t *testing.T) {
	t.Parallel()

	var payload bytes.Buffer
	writeCString(&payload, "")
	writeCString(&payload, "stmt1")
	writeUint16(&payload, 1)
	writeUint16(&payload, 0)
	writeUint16(&payload, 2)
	writeInt32(&payload, 1)
	payload.WriteByte('7')
	writeInt32(&payload, 5)
	payload.WriteString("alice")
	writeUint16(&payload, 1)
	writeUint16(&payload, 0)

	frame := append([]byte{'B'}, encodeLengthPrefixedPayload(payload.Bytes())...)
	msg, err := DecodeFrontendMessage(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode bind: %v", err)
	}
	bind, ok := msg.(Bind)
	if !ok {
		t.Fatalf("frontend message type = %T, want Bind", msg)
	}
	if bind.StatementName != "stmt1" {
		t.Fatalf("statement name = %q, want stmt1", bind.StatementName)
	}
	if len(bind.Parameters) != 2 || string(bind.Parameters[0].Value) != "7" || string(bind.Parameters[1].Value) != "alice" {
		t.Fatalf("parameters = %#v", bind.Parameters)
	}
	if len(bind.ResultFormatCodes) != 1 || bind.ResultFormatCodes[0] != 0 {
		t.Fatalf("result formats = %#v, want [0]", bind.ResultFormatCodes)
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
