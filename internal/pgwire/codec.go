package pgwire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const (
	// ProtocolVersion30 is PostgreSQL protocol version 3.0.
	ProtocolVersion30 uint32 = 196608
	// SSLRequestCode is the startup code for SSL negotiation.
	SSLRequestCode uint32 = 80877103
	// GSSENCRequestCode is the startup code for GSS encryption negotiation.
	GSSENCRequestCode uint32 = 80877104
)

// TxStatus is the ready-for-query transaction state byte.
type TxStatus byte

const (
	TxIdle              TxStatus = 'I'
	TxInTransaction     TxStatus = 'T'
	TxFailedTransaction TxStatus = 'E'
)

// StartupMessage is the untagged PostgreSQL startup frame.
type StartupMessage struct {
	ProtocolVersion uint32
	Parameters      map[string]string
}

// FrontendMessage is one tagged frontend message.
type FrontendMessage interface {
	isFrontendMessage()
}

// Query carries one simple-query string.
type Query struct {
	SQL string
}

// Terminate requests session shutdown.
type Terminate struct{}

func (Query) isFrontendMessage()     {}
func (Terminate) isFrontendMessage() {}

// FieldDescription is the row-description metadata for one output column.
type FieldDescription struct {
	Name         string
	TableOID     uint32
	ColumnAttr   uint16
	DataTypeOID  uint32
	TypeSize     int16
	TypeModifier int32
	FormatCode   uint16
}

// DecodeStartup reads and parses one PostgreSQL startup message.
func DecodeStartup(r io.Reader) (StartupMessage, error) {
	payload, err := readPayload(r)
	if err != nil {
		return StartupMessage{}, err
	}
	if len(payload) < 4 {
		return StartupMessage{}, fmt.Errorf("pgwire: startup payload too short")
	}
	msg := StartupMessage{
		ProtocolVersion: binary.BigEndian.Uint32(payload[:4]),
		Parameters:      make(map[string]string),
	}
	if len(payload) == 4 {
		return msg, nil
	}
	items, err := parseCStringFields(payload[4:])
	if err != nil {
		return StartupMessage{}, err
	}
	if len(items)%2 != 0 {
		return StartupMessage{}, fmt.Errorf("pgwire: startup parameters must be key/value pairs")
	}
	for i := 0; i < len(items); i += 2 {
		msg.Parameters[items[i]] = items[i+1]
	}
	return msg, nil
}

// DecodeFrontendMessage reads and parses one tagged frontend message.
func DecodeFrontendMessage(r io.Reader) (FrontendMessage, error) {
	var tag [1]byte
	if _, err := io.ReadFull(r, tag[:]); err != nil {
		return nil, err
	}
	payload, err := readPayload(r)
	if err != nil {
		return nil, err
	}
	switch tag[0] {
	case 'Q':
		sql, rest, ok := bytes.Cut(payload, []byte{0})
		if !ok || len(rest) != 0 {
			return nil, fmt.Errorf("pgwire: malformed Query message")
		}
		return Query{SQL: string(sql)}, nil
	case 'X':
		if len(payload) != 0 {
			return nil, fmt.Errorf("pgwire: Terminate message must be empty")
		}
		return Terminate{}, nil
	default:
		return nil, fmt.Errorf("pgwire: unsupported frontend message tag %q", tag[0])
	}
}

// EncodeAuthenticationOK encodes the server AuthenticationOk response.
func EncodeAuthenticationOK() []byte {
	var payload bytes.Buffer
	writeInt32(&payload, 0)
	return encodeTagged('R', payload.Bytes())
}

// EncodeParameterStatus announces one server parameter.
func EncodeParameterStatus(name, value string) []byte {
	var payload bytes.Buffer
	writeCString(&payload, name)
	writeCString(&payload, value)
	return encodeTagged('S', payload.Bytes())
}

// EncodeReadyForQuery encodes the server transaction status byte.
func EncodeReadyForQuery(status TxStatus) []byte {
	return encodeTagged('Z', []byte{byte(status)})
}

// EncodeCommandComplete encodes one command tag.
func EncodeCommandComplete(tag string) []byte {
	var payload bytes.Buffer
	writeCString(&payload, tag)
	return encodeTagged('C', payload.Bytes())
}

// EncodeErrorResponse encodes one backend error response.
func EncodeErrorResponse(severity, code, message string) []byte {
	var payload bytes.Buffer
	writeField(&payload, 'S', severity)
	writeField(&payload, 'C', code)
	writeField(&payload, 'M', message)
	payload.WriteByte(0)
	return encodeTagged('E', payload.Bytes())
}

// EncodeRowDescription encodes output-column metadata.
func EncodeRowDescription(fields []FieldDescription) []byte {
	var payload bytes.Buffer
	writeUint16(&payload, uint16(len(fields)))
	for _, field := range fields {
		writeCString(&payload, field.Name)
		writeUint32(&payload, field.TableOID)
		writeUint16(&payload, field.ColumnAttr)
		writeUint32(&payload, field.DataTypeOID)
		writeInt16(&payload, field.TypeSize)
		writeInt32(&payload, field.TypeModifier)
		writeUint16(&payload, field.FormatCode)
	}
	return encodeTagged('T', payload.Bytes())
}

// EncodeDataRow encodes one result row.
func EncodeDataRow(values [][]byte) []byte {
	var payload bytes.Buffer
	writeUint16(&payload, uint16(len(values)))
	for _, value := range values {
		if value == nil {
			writeInt32(&payload, -1)
			continue
		}
		writeInt32(&payload, int32(len(value)))
		payload.Write(value)
	}
	return encodeTagged('D', payload.Bytes())
}

func StartupParameterFrames(params map[string]string) [][]byte {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	frames := make([][]byte, 0, len(keys))
	for _, key := range keys {
		frames = append(frames, EncodeParameterStatus(key, params[key]))
	}
	return frames
}

func readPayload(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(lenBuf[:]))
	if size < 4 {
		return nil, fmt.Errorf("pgwire: invalid message length %d", size)
	}
	payload := make([]byte, size-4)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func parseCStringFields(payload []byte) ([]string, error) {
	if len(payload) == 0 || payload[len(payload)-1] != 0 {
		return nil, fmt.Errorf("pgwire: payload must end with NUL terminator")
	}
	fields := make([]string, 0, 4)
	start := 0
	for i, b := range payload {
		if b != 0 {
			continue
		}
		if i == start {
			if i != len(payload)-1 {
				return nil, fmt.Errorf("pgwire: unexpected empty cstring field")
			}
			return fields, nil
		}
		fields = append(fields, string(payload[start:i]))
		start = i + 1
	}
	return nil, fmt.Errorf("pgwire: unterminated cstring field")
}

func encodeTagged(tag byte, payload []byte) []byte {
	buf := make([]byte, 0, len(payload)+5)
	buf = append(buf, tag)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)+4))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, payload...)
	return buf
}

func writeCString(buf *bytes.Buffer, value string) {
	buf.WriteString(value)
	buf.WriteByte(0)
}

func writeField(buf *bytes.Buffer, code byte, value string) {
	buf.WriteByte(code)
	writeCString(buf, value)
}

func writeInt32(buf *bytes.Buffer, value int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(value))
	buf.Write(tmp[:])
}

func writeUint32(buf *bytes.Buffer, value uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], value)
	buf.Write(tmp[:])
}

func writeInt16(buf *bytes.Buffer, value int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(value))
	buf.Write(tmp[:])
}

func writeUint16(buf *bytes.Buffer, value uint16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], value)
	buf.Write(tmp[:])
}
