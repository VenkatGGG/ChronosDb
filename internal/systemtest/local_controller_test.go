package systemtest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/observability"
	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
)

func TestLocalControllerStartsNodesAndServesEndpoints(t *testing.T) {
	t.Parallel()

	controller := newLocalController(t, 1, 2)

	pgAddr, err := controller.PGWireAddr(1)
	if err != nil {
		t.Fatalf("pgwire addr: %v", err)
	}
	conn := openPGConn(t, pgAddr)
	defer conn.Close()

	if _, err := conn.Write(queryFrame("select id, name from users where id = 7")); err != nil {
		t.Fatalf("write query: %v", err)
	}
	frames := readFrames(t, conn, 3)
	if got := frameTags(frames); got != "TCZ" {
		t.Fatalf("query frame tags = %q, want TCZ", got)
	}

	baseURL, err := controller.ObservabilityURL(1)
	if err != nil {
		t.Fatalf("observability url: %v", err)
	}
	if body := getText(t, baseURL+"/healthz"); strings.TrimSpace(body) != "ok" {
		t.Fatalf("healthz body = %q, want ok", body)
	}
	overview := getOverview(t, baseURL+"/debug/chronos/overview")
	if overview.Status != "ok" {
		t.Fatalf("overview status = %q, want ok", overview.Status)
	}
	if overview.Components["pgwire"] != "up" {
		t.Fatalf("overview components = %+v, want pgwire up", overview.Components)
	}
}

func TestLocalControllerCrashAndRestartNode(t *testing.T) {
	t.Parallel()

	controller := newLocalController(t, 1, 2)

	if err := controller.CrashNode(context.Background(), 2); err != nil {
		t.Fatalf("crash node: %v", err)
	}
	if _, err := controller.PGWireAddr(2); err == nil {
		t.Fatalf("expected crashed node to hide pgwire addr")
	}

	if err := controller.RestartNode(context.Background(), 2); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	pgAddr, err := controller.PGWireAddr(2)
	if err != nil {
		t.Fatalf("pgwire addr after restart: %v", err)
	}
	conn := openPGConn(t, pgAddr)
	defer conn.Close()
	if _, err := conn.Write(queryFrame("select id, name from users where id = 7")); err != nil {
		t.Fatalf("write query after restart: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 3)); got != "TCZ" {
		t.Fatalf("query frame tags after restart = %q, want TCZ", got)
	}
}

func TestLocalControllerPartitionAndHealExposeOverview(t *testing.T) {
	t.Parallel()

	controller := newLocalController(t, 1, 2)

	if err := controller.Partition(context.Background(), PartitionSpec{
		Left:  []uint64{1},
		Right: []uint64{2},
	}); err != nil {
		t.Fatalf("partition: %v", err)
	}

	baseURL, err := controller.ObservabilityURL(1)
	if err != nil {
		t.Fatalf("observability url: %v", err)
	}
	overview := getOverview(t, baseURL+"/debug/chronos/overview")
	if overview.Status != "degraded" {
		t.Fatalf("partitioned overview status = %q, want degraded", overview.Status)
	}
	if !containsNote(overview.Notes, "partitioned from nodes [2]") {
		t.Fatalf("overview notes = %+v, want partition note", overview.Notes)
	}

	if err := controller.Heal(context.Background()); err != nil {
		t.Fatalf("heal: %v", err)
	}
	overview = getOverview(t, baseURL+"/debug/chronos/overview")
	if overview.Status != "ok" {
		t.Fatalf("healed overview status = %q, want ok", overview.Status)
	}
}

func TestLocalControllerInjectAmbiguousCommitDropsConnection(t *testing.T) {
	t.Parallel()

	controller := newLocalController(t, 1, 2)
	if err := controller.InjectAmbiguousCommit(context.Background(), AmbiguousCommitSpec{
		GatewayNodeID: 1,
		TxnLabel:      "transfer-42",
		AckDelay:      50 * time.Millisecond,
		DropResponse:  true,
	}); err != nil {
		t.Fatalf("inject ambiguous commit: %v", err)
	}

	baseURL, err := controller.ObservabilityURL(1)
	if err != nil {
		t.Fatalf("observability url: %v", err)
	}
	overview := getOverview(t, baseURL+"/debug/chronos/overview")
	if !containsNote(overview.Notes, "ambiguous commit fault armed") {
		t.Fatalf("overview notes = %+v, want ambiguous fault note", overview.Notes)
	}

	pgAddr, err := controller.PGWireAddr(1)
	if err != nil {
		t.Fatalf("pgwire addr: %v", err)
	}
	conn := openPGConn(t, pgAddr)
	defer conn.Close()

	query := "insert into users (id, name, email) values (1, 'transfer-42', 'a@example.com')"
	if _, err := conn.Write(queryFrame(query)); err != nil {
		t.Fatalf("write ambiguous query: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := readBackendFrame(conn); err == nil {
		t.Fatalf("expected ambiguous fault to close connection without a response")
	} else if !errors.Is(err, io.EOF) {
		var netErr net.Error
		if !errors.As(err, &netErr) {
			t.Fatalf("read error = %v, want EOF or timeout/connection error", err)
		}
	}

	freshConn := openPGConn(t, pgAddr)
	defer freshConn.Close()
	if _, err := freshConn.Write(queryFrame("insert into users (id, name, email) values (2, 'normal', 'b@example.com')")); err != nil {
		t.Fatalf("write normal query: %v", err)
	}
	if got := frameTags(readFrames(t, freshConn, 2)); got != "CZ" {
		t.Fatalf("normal insert frame tags = %q, want CZ", got)
	}
}

func newLocalController(t *testing.T, nodes ...uint64) *LocalController {
	t.Helper()

	controller, err := NewLocalController(LocalControllerConfig{Nodes: nodes})
	if err != nil {
		t.Fatalf("new local controller: %v", err)
	}
	t.Cleanup(func() {
		if err := controller.Close(); err != nil {
			t.Fatalf("close local controller: %v", err)
		}
	})
	return controller
}

func getText(t *testing.T, url string) string {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status for %s = %d, want 200; body=%s", url, resp.StatusCode, body)
	}
	return string(body)
}

func getOverview(t *testing.T, url string) observability.Overview {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status for %s = %d, want 200; body=%s", url, resp.StatusCode, body)
	}
	var overview observability.Overview
	if err := json.NewDecoder(resp.Body).Decode(&overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	return overview
}

func containsNote(notes []string, needle string) bool {
	for _, note := range notes {
		if note == needle {
			return true
		}
	}
	return false
}

func openPGConn(t *testing.T, addr string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial pgwire: %v", err)
	}
	if _, err := conn.Write(startupFrame(pgwire.ProtocolVersion30, map[string]string{"user": "chronos"})); err != nil {
		_ = conn.Close()
		t.Fatalf("write startup: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 1)); got != "R" {
		_ = conn.Close()
		t.Fatalf("startup challenge tags = %q, want R", got)
	}
	if _, err := conn.Write(passwordFrame(DefaultPGWirePassword)); err != nil {
		_ = conn.Close()
		t.Fatalf("write password: %v", err)
	}
	if got := frameTags(readFrames(t, conn, 5)); got != "RSSSZ" {
		_ = conn.Close()
		t.Fatalf("startup frame tags = %q, want RSSSZ", got)
	}
	return conn
}

func startupFrame(version uint32, params map[string]string) []byte {
	var payload bytes.Buffer
	_ = binary.Write(&payload, binary.BigEndian, version)
	for key, value := range params {
		writeCString(&payload, key)
		writeCString(&payload, value)
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

func passwordFrame(password string) []byte {
	payload := append([]byte(password), 0)
	frame := []byte{'p'}
	return append(frame, encodePayload(payload)...)
}

func readFrames(t *testing.T, conn net.Conn, count int) [][]byte {
	t.Helper()

	frames := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		frame, err := readBackendFrame(conn)
		if err != nil {
			t.Fatalf("read backend frame %d: %v", i+1, err)
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

func frameTags(frames [][]byte) string {
	var builder strings.Builder
	for _, frame := range frames {
		builder.WriteByte(frame[0])
	}
	return builder.String()
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
