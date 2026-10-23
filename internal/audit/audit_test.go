package audit

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// capture swaps the global logger for a buffer and returns it.
func capture(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf)
	t.Cleanup(func() { log.Logger = prev })
	return &buf
}

// serve sends one request through the middleware to a handler that names
// actor (when set) and answers status.
func serve(method, target, actor string, status int) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if actor != "" {
			SetActor(r, actor)
		}
		w.WriteHeader(status)
	})
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = "192.0.2.7:5555"
	Middleware(h).ServeHTTP(httptest.NewRecorder(), req)
}

func TestChangeIsLoggedWithActor(t *testing.T) {
	buf := capture(t)
	serve(http.MethodPut, "/switchover/node2", "jwt:alice", http.StatusOK)
	out := buf.String()
	for _, want := range []string{`"audit":true`, `"actor":"jwt:alice"`, `"method":"PUT"`, `"path":"/switchover/node2"`, `"status":200`} {
		if !strings.Contains(out, want) {
			t.Errorf("log %q lacks %s", out, want)
		}
	}
}

func TestUnidentifiedChangeNamesAddress(t *testing.T) {
	buf := capture(t)
	serve(http.MethodPost, "/failover/node3", "", http.StatusOK)
	if !strings.Contains(buf.String(), `"actor":"addr:192.0.2.7"`) {
		t.Errorf("log %q does not name the address", buf.String())
	}
}

func TestReadsAreNotLogged(t *testing.T) {
	buf := capture(t)
	serve(http.MethodGet, "/status", "jwt:alice", http.StatusOK)
	serve(http.MethodHead, "/status", "", http.StatusOK)
	if buf.Len() != 0 {
		t.Errorf("reads logged: %q", buf.String())
	}
}

func TestRaftMessagesAreNotLogged(t *testing.T) {
	buf := capture(t)
	serve(http.MethodPost, "/raft/append", "", http.StatusOK)
	if buf.Len() != 0 {
		t.Errorf("raft heartbeat logged: %q", buf.String())
	}
}

func TestRefusalIsLoggedEvenForReads(t *testing.T) {
	buf := capture(t)
	serve(http.MethodGet, "/members", "ldap:bob", http.StatusForbidden)
	out := buf.String()
	if !strings.Contains(out, `"actor":"ldap:bob"`) || !strings.Contains(out, `"status":403`) {
		t.Errorf("refusal not logged: %q", out)
	}
}

func TestSetActorWithoutMiddlewareIsNoop(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	SetActor(req, "jwt:alice") // must not panic
}

// A handler that streams needs the flush of the writer under the middleware.
func TestMiddlewareKeepsResponseController(t *testing.T) {
	var flushErr error
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flushErr = http.NewResponseController(w).Flush()
	})
	req := httptest.NewRequest(http.MethodPost, "/shell", nil)
	Middleware(h).ServeHTTP(httptest.NewRecorder(), req)
	if flushErr != nil {
		t.Fatalf("flush through the middleware: %v", flushErr)
	}
}
