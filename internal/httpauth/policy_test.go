package httpauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPolicyLoopbackOnly(t *testing.T) {
	t.Parallel()

	handler := Policy{LoopbackOnly: true}.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	loopbackReq := httptest.NewRequest(http.MethodGet, "/admin", nil)
	loopbackReq.RemoteAddr = "127.0.0.1:1234"
	loopbackRec := httptest.NewRecorder()
	handler.ServeHTTP(loopbackRec, loopbackReq)
	if loopbackRec.Code != http.StatusNoContent {
		t.Fatalf("loopback status = %d, want %d", loopbackRec.Code, http.StatusNoContent)
	}

	remoteReq := httptest.NewRequest(http.MethodGet, "/admin", nil)
	remoteReq.RemoteAddr = "10.0.0.8:4567"
	remoteRec := httptest.NewRecorder()
	handler.ServeHTTP(remoteRec, remoteReq)
	if remoteRec.Code != http.StatusForbidden {
		t.Fatalf("remote status = %d, want %d", remoteRec.Code, http.StatusForbidden)
	}
}

func TestPolicyBearerToken(t *testing.T) {
	t.Parallel()

	handler := Policy{BearerToken: "secret", LoopbackOnly: true, Realm: "chronos-console"}.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	req.RemoteAddr = "10.0.0.8:4567"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatal("missing WWW-Authenticate header")
	}

	authorizedReq := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	authorizedReq.RemoteAddr = "10.0.0.8:4567"
	authorizedReq.Header.Set("Authorization", "Bearer secret")
	authorizedRec := httptest.NewRecorder()
	handler.ServeHTTP(authorizedRec, authorizedReq)
	if authorizedRec.Code != http.StatusNoContent {
		t.Fatalf("authorized status = %d, want %d", authorizedRec.Code, http.StatusNoContent)
	}
}

func TestPolicyPublicPathsBypassAuth(t *testing.T) {
	t.Parallel()

	handler := Policy{
		BearerToken:  "secret",
		LoopbackOnly: true,
		PublicPaths:  []string{"/healthz", "/readyz"},
	}.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.RemoteAddr = "10.0.0.8:4567"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("public path status = %d, want %d", rec.Code, http.StatusOK)
	}
}
