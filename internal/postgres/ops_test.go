package postgres

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/ks-tool/tembo/internal/raft"
	"github.com/ks-tool/tembo/internal/store"
)

func TestGetHistory(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "ops.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	mux := http.NewServeMux()
	opsHandlers(mux, st, &Instance{}, "a")

	get := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/history", nil))
		return rec
	}

	if rec := get(); rec.Code != http.StatusOK || rec.Body.String() != "{}" {
		t.Fatalf("empty: got %d %q", rec.Code, rec.Body)
	}

	value, err := json.Marshal(map[string]string{"2026-10-19T00:00:00Z": "failover: a → b"})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := json.Marshal(store.Command{Op: "merge", Key: "history", Value: string(value)})
	if err != nil {
		t.Fatal(err)
	}
	st.Apply(raft.Entry{Index: 1, Data: cmd})

	rec := get()
	var history map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &history); err != nil {
		t.Fatal(err)
	}
	if history["2026-10-19T00:00:00Z"] != "failover: a → b" {
		t.Fatalf("got %v", history)
	}
}

// The health check must refuse when the local Postgres does not answer —
// a supervisor's healthcheck keys on the status code. The 200 path needs a
// live Postgres and is covered by the compose smoke test.
func TestHealthRefusesWithoutPostgres(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "health.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mux := http.NewServeMux()
	opsHandlers(mux, st, &Instance{DataDir: filepath.Join(t.TempDir(), "data")}, "a")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d %q, want 503", rec.Code, rec.Body)
	}
}
