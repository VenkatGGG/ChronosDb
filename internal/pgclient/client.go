package pgclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sort"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
)

// Result is one simple-query result decoded from pgwire text frames.
type Result struct {
	CommandTag string
	Columns    []string
	Rows       [][]string
}

// Client is a minimal pgwire simple-query client used by the demo launcher.
type Client struct {
	conn net.Conn
}

// Dial opens a simple pgwire session and completes startup/authentication.
func Dial(ctx context.Context, addr, user string) (*Client, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := conn.Write(startupFrame(pgwire.ProtocolVersion30, map[string]string{"user": user})); err != nil {
		_ = conn.Close()
		return nil, err
	}
	client := &Client{conn: conn}
	if err := client.waitForReady(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(timeZero())
	return client, nil
}

// Close closes the underlying pgwire connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// SimpleQuery runs one simple SQL query and returns the decoded text result.
func (c *Client) SimpleQuery(ctx context.Context, sql string) (Result, error) {
	if c == nil || c.conn == nil {
		return Result{}, fmt.Errorf("pgclient: connection is not open")
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(timeZero())
	}
	if _, err := c.conn.Write(queryFrame(sql)); err != nil {
		return Result{}, err
	}
	var result Result
	for {
		frame, err := readBackendFrame(c.conn)
		if err != nil {
			return Result{}, err
		}
		switch frame[0] {
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

func (c *Client) waitForReady() error {
	sawReady := false
	for !sawReady {
		frame, err := readBackendFrame(c.conn)
		if err != nil {
			return err
		}
		switch frame[0] {
		case 'E':
			return decodeError(frame)
		case 'Z':
			sawReady = true
		}
	}
	return nil
}

func startupFrame(version uint32, params map[string]string) []byte {
	var payload bytes.Buffer
	_ = binary.Write(&payload, binary.BigEndian, version)
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeCString(&payload, key)
		writeCString(&payload, params[key])
	}
	payload.WriteByte(0)
	frame := make([]byte, 0, payload.Len()+4)
	_ = binary.Write(&payloadPrefix{target: &frame}, binary.BigEndian, int32(payload.Len()+4))
	frame = append(frame, payload.Bytes()...)
	return frame
}

func queryFrame(sql string) []byte {
	payload := append([]byte(sql), 0)
	frame := []byte{'Q'}
	frame = append(frame, encodePayload(payload)...)
	return frame
}

func readBackendFrame(r io.Reader) ([]byte, error) {
	var tag [1]byte
	if _, err := io.ReadFull(r, tag[:]); err != nil {
		return nil, err
	}
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	frame := make([]byte, 0, 1+4+len(payload))
	frame = append(frame, tag[0])
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(length))
	frame = append(frame, lengthBuf...)
	frame = append(frame, payload...)
	return frame, nil
}

func decodeRowDescription(frame []byte) ([]string, error) {
	payload := frame[5:]
	columnCount := int(binary.BigEndian.Uint16(payload[:2]))
	payload = payload[2:]
	columns := make([]string, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		name, rest, err := readCString(payload)
		if err != nil {
			return nil, err
		}
		columns = append(columns, name)
		if len(rest) < 18 {
			return nil, fmt.Errorf("pgclient: malformed row description")
		}
		payload = rest[18:]
	}
	return columns, nil
}

func decodeDataRow(frame []byte) ([]string, error) {
	payload := frame[5:]
	columnCount := int(binary.BigEndian.Uint16(payload[:2]))
	payload = payload[2:]
	values := make([]string, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		if len(payload) < 4 {
			return nil, fmt.Errorf("pgclient: malformed data row")
		}
		size := int(int32(binary.BigEndian.Uint32(payload[:4])))
		payload = payload[4:]
		if size < 0 {
			values = append(values, "")
			continue
		}
		if len(payload) < size {
			return nil, fmt.Errorf("pgclient: malformed data row payload")
		}
		values = append(values, string(payload[:size]))
		payload = payload[size:]
	}
	return values, nil
}

func decodeCommandTag(frame []byte) string {
	payload := frame[5:]
	if idx := bytes.IndexByte(payload, 0); idx >= 0 {
		payload = payload[:idx]
	}
	return string(payload)
}

func decodeError(frame []byte) error {
	payload := frame[5:]
	fields := make(map[byte]string)
	for len(payload) > 0 && payload[0] != 0 {
		code := payload[0]
		value, rest, err := readCString(payload[1:])
		if err != nil {
			return err
		}
		fields[code] = value
		payload = rest
	}
	message := fields['M']
	if message == "" {
		message = "unknown pgwire error"
	}
	if sqlState := fields['C']; sqlState != "" {
		return fmt.Errorf("%s (%s)", message, sqlState)
	}
	return fmt.Errorf("%s", message)
}

func readCString(payload []byte) (string, []byte, error) {
	idx := bytes.IndexByte(payload, 0)
	if idx < 0 {
		return "", nil, fmt.Errorf("pgclient: malformed cstring")
	}
	return string(payload[:idx]), payload[idx+1:], nil
}

func encodePayload(payload []byte) []byte {
	out := make([]byte, 4, len(payload)+4)
	binary.BigEndian.PutUint32(out, uint32(len(payload)+4))
	return append(out, payload...)
}

func writeCString(buf *bytes.Buffer, value string) {
	buf.WriteString(value)
	buf.WriteByte(0)
}

type payloadPrefix struct {
	target *[]byte
}

func (p *payloadPrefix) Write(data []byte) (int, error) {
	*p.target = append(*p.target, data...)
	return len(data), nil
}

func timeZero() time.Time {
	return time.Time{}
}
