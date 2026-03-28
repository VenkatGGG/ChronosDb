package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildHandlerServesAPIAndUI(t *testing.T) {
	t.Parallel()

	uiDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(uiDir, "index.html"), []byte("<html>console</html>"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(uiDir, "asset.js"), []byte("console.log('ok')"), 0o644); err != nil {
		t.Fatalf("write asset: %v", err)
	}

	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/v1/cluster", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	handler, err := buildHandler(apiMux, uiDir)
	if err != nil {
		t.Fatalf("build handler: %v", err)
	}

	t.Run("api path", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("api status = %d", rec.Code)
		}
		if body := strings.TrimSpace(rec.Body.String()); body != `{"status":"ok"}` {
			t.Fatalf("api body = %q", body)
		}
	})

	t.Run("root path", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("root status = %d", rec.Code)
		}
		body, err := io.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("read root body: %v", err)
		}
		if !strings.Contains(string(body), "console") {
			t.Fatalf("root body = %q", string(body))
		}
	})

	t.Run("spa fallback", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ranges/11", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("spa status = %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "console") {
			t.Fatalf("spa body = %q", rec.Body.String())
		}
	})

	t.Run("asset path", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/asset.js", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("asset status = %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "console.log('ok')") {
			t.Fatalf("asset body = %q", rec.Body.String())
		}
	})
}
