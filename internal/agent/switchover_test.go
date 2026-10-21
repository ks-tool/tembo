package agent_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/raft"
	"github.com/ks-tool/tembo/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// set writes cmd into the store directly, as a committed and applied raft
// entry would be: in the raft log and in the state machine.
func set(t *testing.T, st *store.Store, index uint64, cmd store.Command) {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	e := raft.Entry{Index: index, Term: 1, Data: data}
	if err := st.RaftStorage().SetLog(index, []raft.Entry{e}); err != nil {
		t.Fatal(err)
	}
	st.Apply(e)
}

func newServer(t *testing.T, st *store.Store) *agent.Server {
	t.Helper()
	srv, err := agent.NewServer("a", map[string]string{"a": "127.0.0.1:8001"}, st, 0, false)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.Start()
	t.Cleanup(srv.Stop)
	return srv
}

// put sends PUT /switchover/id and returns the answer. Until the node has
// elected itself leader it answers 503, so the caller retries a little.
func put(t *testing.T, srv *agent.Server, id string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/switchover/"+id, nil))
		if rec.Code != http.StatusServiceUnavailable || time.Now().After(deadline) {
			return rec.Code, rec.Body.String()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitLeader(t *testing.T, srv *agent.Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
		var st agent.Status
		if json.Unmarshal(rec.Body.Bytes(), &st) == nil && st.State == "leader" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPutSwitchoverWithFailingStore(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	srv := newServer(t, st)
	waitLeader(t, srv)                 // electing writes to the store, which a node cannot go on without
	if err := st.Close(); err != nil { // every read of the store now fails
		t.Fatalf("close: %v", err)
	}

	// a store error must surface as 500, not read as "no primary" or
	// "no switchover pending"
	code, body := put(t, srv, "b")
	if code != http.StatusInternalServerError {
		t.Fatalf("got %d %q, want 500", code, body)
	}
}

func TestPostFailoverWithFailingStore(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	srv := newServer(t, st)
	waitLeader(t, srv) // electing writes to the store, which a node cannot go on without
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// a store error must surface as 500, not read as "no primary" or
	// "no switchover pending" and run the failover
	code, body := post(t, srv, "/failover/b")
	if code != http.StatusInternalServerError {
		t.Fatalf("got %d %q, want 500", code, body)
	}
}

func TestPutSwitchoverConflictWhilePending(t *testing.T) {
	st := openStore(t)
	set(t, st, 1, store.Command{Op: "set", Key: agent.KeyPrimary, Value: "a"})
	set(t, st, 2, store.Command{Op: "set", Key: agent.KeySwitchover, Value: "b"})
	srv := newServer(t, st)

	code, body := put(t, srv, "c")
	if code != http.StatusConflict {
		t.Fatalf("got %d %q, want 409", code, body)
	}
}

// post sends POST path and returns the answer, retrying while the node has no
// leader or the leader is catching up.
func post(t *testing.T, srv *agent.Server, path string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		body := rec.Body.String()
		busy := strings.Contains(body, "no leader yet") || strings.Contains(body, "catching up")
		if !busy || time.Now().After(deadline) {
			return rec.Code, body
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPostFailoverNoPrimary(t *testing.T) {
	st := openStore(t)
	srv := newServer(t, st)

	if code, body := post(t, srv, "/failover/b"); code != http.StatusServiceUnavailable {
		t.Fatalf("no primary: got %d %q, want 503", code, body)
	}
}

func TestPostFailoverConflicts(t *testing.T) {
	st := openStore(t)
	set(t, st, 1, store.Command{Op: "set", Key: agent.KeyPrimary, Value: "a"})
	srv := newServer(t, st)

	if code, body := post(t, srv, "/failover/b"); code != http.StatusNotFound {
		t.Fatalf("unknown node: got %d %q, want 404", code, body)
	}
	if code, body := post(t, srv, "/failover/a"); code != http.StatusConflict {
		t.Fatalf("target is the primary: got %d %q, want 409", code, body)
	}

	// a pending switchover conflicts
	set(t, st, 3, store.Command{Op: "set", Key: agent.KeySwitchover, Value: "b"})
	if code, body := post(t, srv, "/failover/b"); code != http.StatusConflict {
		t.Fatalf("switchover pending: got %d %q, want 409", code, body)
	}
}
