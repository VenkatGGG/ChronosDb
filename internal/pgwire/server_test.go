package pgwire

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestServerServeConnStartupQueryTerminate(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	server := NewServer(staticHandler{
		result: QueryResult{
			Fields: []FieldDescription{
				{Name: "id", DataTypeOID: 20, TypeSize: 8},
			},
			CommandTag: "SELECT 0",
		},
	})
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ServeConn(context.Background(), serverConn)
	}()

	if _, err := clientConn.Write(startupFrame(ProtocolVersion30, map[string]string{"user": "chronos"})); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	frames := readFrames(t, clientConn, 4)
	if got := frameTags(frames); got != "RSSZ" {
		t.Fatalf("startup frame tags = %q, want RSSZ", got)
	}

	if _, err := clientConn.Write(queryFrame("select id from users")); err != nil {
		t.Fatalf("write query: %v", err)
	}
	frames = readFrames(t, clientConn, 3)
	if got := frameTags(frames); got != "TCZ" {
		t.Fatalf("query frame tags = %q, want TCZ", got)
	}

	if _, err := clientConn.Write(terminateFrame()); err != nil {
		t.Fatalf("write terminate: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("serve conn: %v", err)
	}
}

func TestServerServeConnRejectsSSLAndContinues(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	server := NewServer(staticHandler{result: QueryResult{CommandTag: "SELECT 0"}})
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ServeConn(context.Background(), serverConn)
	}()

	if _, err := clientConn.Write(startupFrame(SSLRequestCode, nil)); err != nil {
		t.Fatalf("write ssl request: %v", err)
	}
	var response [1]byte
	if _, err := io.ReadFull(clientConn, response[:]); err != nil {
		t.Fatalf("read ssl response: %v", err)
	}
	if response[0] != 'N' {
		t.Fatalf("ssl response = %q, want 'N'", response[0])
	}

	if _, err := clientConn.Write(startupFrame(ProtocolVersion30, map[string]string{"user": "chronos"})); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	frames := readFrames(t, clientConn, 4)
	if got := frameTags(frames); got != "RSSZ" {
		t.Fatalf("startup frame tags = %q, want RSSZ", got)
	}

	if _, err := clientConn.Write(terminateFrame()); err != nil {
		t.Fatalf("write terminate: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("serve conn: %v", err)
	}
}

func TestServerServeListenerAcceptsTCPConnections(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(staticHandler{result: QueryResult{CommandTag: "SELECT 0"}})
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ServeListener(ctx, listener)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial listener: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write(startupFrame(ProtocolVersion30, map[string]string{"user": "chronos"})); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	frames := readFrames(t, conn, 4)
	if got := frameTags(frames); got != "RSSZ" {
		t.Fatalf("startup frame tags = %q, want RSSZ", got)
	}

	cancel()
	select {
	case err := <-serverErr:
		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("ServeListener error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for listener shutdown")
	}
}

func startupFrame(version uint32, params map[string]string) []byte {
	var payload bytes.Buffer
	writeUint32(&payload, version)
	if len(params) > 0 {
		for key, value := range params {
			writeCString(&payload, key)
			writeCString(&payload, value)
		}
		payload.WriteByte(0)
	}
	frame := make([]byte, 0, payload.Len()+4)
	var lenBuf bytes.Buffer
	writeInt32(&lenBuf, int32(payload.Len()+4))
	frame = append(frame, lenBuf.Bytes()...)
	frame = append(frame, payload.Bytes()...)
	return frame
}

func queryFrame(sql string) []byte {
	return append([]byte{'Q'}, encodeLengthPrefixedPayload(append([]byte(sql), 0))...)
}

func terminateFrame() []byte {
	return append([]byte{'X'}, encodeLengthPrefixedPayload(nil)...)
}

func readFrames(t *testing.T, conn net.Conn, count int) [][]byte {
	t.Helper()
	frames := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		frame, err := readBackendFrame(conn)
		if err != nil {
			t.Fatalf("read backend frame: %v", err)
		}
		frames = append(frames, frame)
	}
	return frames
}

func readBackendFrame(r io.Reader) ([]byte, error) {
	var tag [1]byte
	if _, err := io.ReadFull(r, tag[:]); err != nil {
		return nil, err
	}
	payload, err := readPayload(r)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 0, len(payload)+5)
	frame = append(frame, tag[0])
	frame = append(frame, payload...)
	return frame, nil
}

func frameTags(frames [][]byte) string {
	var builder strings.Builder
	for _, frame := range frames {
		builder.WriteByte(frame[0])
	}
	return builder.String()
}
