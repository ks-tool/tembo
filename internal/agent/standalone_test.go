package agent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/store"

	"github.com/prometheus/client_golang/prometheus"
)

func newStandalone(t *testing.T) (*agent.Server, *store.Store) {
	t.Helper()
	st := openStore(t)
	return agent.NewStandalone("one", "127.0.0.1:8001", st), st
}

func TestStandaloneIsPrimaryWithoutRaft(t *testing.T) {
	srv, st := newStandalone(t)

	if !srv.IsLeader() || !srv.HasQuorum() || srv.Removed() {
		t.Fatal("a standalone node leads, has quorum and is never removed")
	}
	srv.Start() // a no-op that must not panic
	srv.Stop()

	primary, err := srv.WaitPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitPrimary: %v", err)
	}
	if primary != "one" {
		t.Fatalf("primary = %q, want %q", primary, "one")
	}
	if got, err := st.Get(agent.KeyPrimary); err != nil || got != "one" {
		t.Fatalf("store primary = %q, %v", got, err)
	}
}

func TestStandaloneAppliesDirectly(t *testing.T) {
	srv, st := newStandalone(t)

	cmd := store.Command{Op: "set", Key: "k", Value: "v"}
	if err := srv.Apply(context.Background(), cmd); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, err := st.Get("k"); err != nil || got != "v" {
		t.Fatalf("get = %q, %v", got, err)
	}

	rec := httptest.NewRecorder()
	srv.Propose(rec, httptest.NewRequest(http.MethodPatch, "/x", nil), store.Command{Op: "set", Key: "k", Value: "w"})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("propose = %d, want 204", rec.Code)
	}
	if got, err := st.Get("k"); err != nil || got != "w" {
		t.Fatalf("get = %q, %v", got, err)
	}
}

func TestStandaloneHandlerServesStatusOnly(t *testing.T) {
	srv, _ := newStandalone(t)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var st agent.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.State != "standalone" || st.Leader != "one" || len(st.Members) != 1 || st.Members[0] != "one" {
		t.Fatalf("unexpected status: %+v", st)
	}

	for _, path := range []string{"/switchover/b", "/members", "/raft/vote"} {
		rec = httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s = %d, want 404", path, rec.Code)
		}
	}
}

func TestStandaloneWatchReportsLocalPostgres(t *testing.T) {
	srv, _ := newStandalone(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Watch(ctx)

	srv.SetPostgres(agent.Postgres{Running: true})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if nodes := srv.Nodes(); nodes["one"].Running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Watch did not publish the local state")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStandaloneMetricsCollect(t *testing.T) {
	srv, _ := newStandalone(t)

	reg := prometheus.NewRegistry()
	if err := reg.Register(srv.Metrics()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Gather(); err != nil {
		t.Fatal(err)
	}
}
