package pgclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// ExtendedQuery executes one unnamed prepared statement through the PostgreSQL
// Parse/Bind/Describe/Execute/Sync cycle using text result formats.
func (c *Client) ExtendedQuery(ctx context.Context, sql string, args ...any) (Result, error) {
	if c == nil || c.conn == nil {
		return Result{}, fmt.Errorf("pgclient: connection is not open")
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(timeZero())
	}
	params := make([]encodedParameter, 0, len(args))
	for _, arg := range args {
		param, err := encodeExtendedParameter(arg)
		if err != nil {
			return Result{}, err
		}
		params = append(params, param)
	}
	var request bytes.Buffer
	request.Write(parseFrame("", sql, nil))
	request.Write(bindFrame("", "", params, []uint16{0}))
	request.Write(describeFrame('P', ""))
	request.Write(executeFrame("", 0))
	request.Write(syncFrame())
	if _, err := c.conn.Write(request.Bytes()); err != nil {
		return Result{}, err
	}

	var result Result
	for {
		frame, err := readBackendFrame(c.conn)
		if err != nil {
			return Result{}, err
		}
		switch frame[0] {
		case '1', '2', 't', 'n':
			continue
		case 'T':
			columns, err := decodeRowDescription(frame)
			if err != nil {
				return Result{}, err
			}
			result.Columns = columns
		case 'D':
			row, err := decodeDataRow(frame)
			if err != nil {
				return Result{}, err
			}
			result.Rows = append(result.Rows, row)
		case 'C':
			result.CommandTag = decodeCommandTag(frame)
		case 'E':
			return Result{}, decodeError(frame)
		case 'Z':
			return result, nil
		}
	}
}

type encodedParameter struct {
	formatCode uint16
	value      []byte
	isNull     bool
}

func encodeExtendedParameter(arg any) (encodedParameter, error) {
	switch typed := arg.(type) {
	case nil:
		return encodedParameter{isNull: true}, nil
	case string:
		return encodedParameter{formatCode: 0, value: []byte(typed)}, nil
	case []byte:
		return encodedParameter{formatCode: 1, value: append([]byte(nil), typed...)}, nil
	case int:
		return encodedParameter{formatCode: 0, value: []byte(fmt.Sprintf("%d", typed))}, nil
	case int32:
		return encodedParameter{formatCode: 0, value: []byte(fmt.Sprintf("%d", typed))}, nil
	case int64:
		return encodedParameter{formatCode: 0, value: []byte(fmt.Sprintf("%d", typed))}, nil
	default:
		return encodedParameter{}, fmt.Errorf("pgclient: unsupported extended-query parameter type %T", arg)
	}
}

func parseFrame(name, sql string, parameterTypeOIDs []uint32) []byte {
	var payload bytes.Buffer
	writeCString(&payload, name)
	writeCString(&payload, sql)
	writeUint16(&payload, uint16(len(parameterTypeOIDs)))
	for _, oid := range parameterTypeOIDs {
		writeUint32(&payload, oid)
	}
	frame := []byte{'P'}
	return append(frame, encodePayload(payload.Bytes())...)
}

func bindFrame(portalName, statementName string, params []encodedParameter, resultFormats []uint16) []byte {
	var payload bytes.Buffer
	writeCString(&payload, portalName)
	writeCString(&payload, statementName)
	formatCodes := parameterFormatCodes(params)
	writeUint16(&payload, uint16(len(formatCodes)))
	for _, code := range formatCodes {
		writeUint16(&payload, code)
	}
	writeUint16(&payload, uint16(len(params)))
	for _, param := range params {
		if param.isNull {
			writeInt32(&payload, -1)
			continue
		}
		writeInt32(&payload, int32(len(param.value)))
		payload.Write(param.value)
	}
	writeUint16(&payload, uint16(len(resultFormats)))
	for _, code := range resultFormats {
		writeUint16(&payload, code)
	}
	frame := []byte{'B'}
	return append(frame, encodePayload(payload.Bytes())...)
}

func describeFrame(objectType byte, name string) []byte {
	var payload bytes.Buffer
	payload.WriteByte(objectType)
	writeCString(&payload, name)
	frame := []byte{'D'}
	return append(frame, encodePayload(payload.Bytes())...)
}

func executeFrame(portalName string, maxRows uint32) []byte {
	var payload bytes.Buffer
	writeCString(&payload, portalName)
	writeUint32(&payload, maxRows)
	frame := []byte{'E'}
	return append(frame, encodePayload(payload.Bytes())...)
}

func syncFrame() []byte {
	return []byte{'S', 0, 0, 0, 4}
}

func parameterFormatCodes(params []encodedParameter) []uint16 {
	if len(params) == 0 {
		return nil
	}
	codes := make([]uint16, 0, len(params))
	first := params[0].formatCode
	allSame := true
	for _, param := range params {
		codes = append(codes, param.formatCode)
		if param.formatCode != first {
			allSame = false
		}
	}
	if allSame {
		return []uint16{first}
	}
	return codes
}

func writeUint16(buf *bytes.Buffer, value uint16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], value)
	buf.Write(tmp[:])
}

func writeUint32(buf *bytes.Buffer, value uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], value)
	buf.Write(tmp[:])
}

func writeInt32(buf *bytes.Buffer, value int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(value))
	buf.Write(tmp[:])
}
