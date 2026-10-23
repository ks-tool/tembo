package postgres

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ks-tool/tembo/internal/addrlist"
)

func TestNodeTrafficExemptsOnlyInternalPaths(t *testing.T) {
	var peers atomic.Pointer[addrlist.List]
	peers.Store(peerlist(map[string]string{"a": "10.0.0.1:8001"}))

	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	mux.HandleFunc("POST /raft/append", ok)
	mux.HandleFunc("GET /status", ok)
	mux.HandleFunc("POST /shell", ok)
	refuse := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "authentication required", http.StatusUnauthorized)
	})
	h := nodeTraffic(refuse, mux, &peers)

	for _, tc := range []struct {
		remote, method, path string
		want                 int
	}{
		{"10.0.0.1:5000", http.MethodPost, "/raft/append", http.StatusOK},     // a member's raft RPC
		{"10.0.0.1:5000", http.MethodGet, "/status", http.StatusOK},           // a member's poll
		{"10.0.0.1:5000", http.MethodPost, "/shell", http.StatusUnauthorized}, // the CLI on the node: must authenticate
		{"127.0.0.1:5000", http.MethodPost, "/shell", http.StatusUnauthorized},
		{"10.0.0.9:5000", http.MethodPost, "/raft/append", http.StatusUnauthorized}, // a stranger
		{"10.0.0.9:5000", http.MethodGet, "/status", http.StatusUnauthorized},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.RemoteAddr = tc.remote
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s %s from %s = %d, want %d", tc.method, tc.path, tc.remote, rec.Code, tc.want)
		}
	}
}

func TestProbesPassWithoutCredentials(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	// The authenticated side refuses everything, as Wrap does to a client
	// without credentials.
	refuse := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "authentication required", http.StatusUnauthorized)
	})
	h := probes(refuse, mux)

	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/health", http.StatusOK},
		{http.MethodGet, "/status", http.StatusUnauthorized},
		{http.MethodPost, "/health", http.StatusUnauthorized},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if rec.Code != tc.want {
			t.Errorf("%s %s = %d, want %d", tc.method, tc.path, rec.Code, tc.want)
		}
	}
}
