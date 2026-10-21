package postgres

import (
	"context"
	"encoding/json"
	"errors"
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

func TestParsePeers(t *testing.T) {
	peers, err := parsePeers("a=127.0.0.1:8001, b=127.0.0.1:8002")
	if err != nil {
		t.Fatalf("parsePeers: %v", err)
	}
	if len(peers) != 2 || peers["a"] != "127.0.0.1:8001" || peers["b"] != "127.0.0.1:8002" {
		t.Fatalf("peers = %v", peers)
	}

	// a bare host:port names the node after its host, dashes to underscores
	peers, err = parsePeers("tembo-a:8001,tembo-b:8002, c=tembo-c:8003")
	if err != nil {
		t.Fatalf("parsePeers: %v", err)
	}
	if len(peers) != 3 || peers["tembo_a"] != "tembo-a:8001" || peers["tembo_b"] != "tembo-b:8002" || peers["c"] != "tembo-c:8003" {
		t.Fatalf("peers = %v", peers)
	}

	for _, bad := range []string{
		"", "a", "=8001", "a=", "a=1,b",
		"a=127.0.0.1:8001,a=127.0.0.1:8002",
		"127.0.0.1:8001",            // an IP does not sanitize into an id
		"Tembo-a:8001",              // nor an uppercase host
		"tembo-a:8001,tembo_a:8002", // two hosts, one id
	} {
		if _, err := parsePeers(bad); err == nil {
			t.Errorf("parsePeers(%q): want an error", bad)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "config.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	empty, err := loadConfig(st)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if len(empty.Settings) != 0 || empty.Hash == "" {
		t.Fatalf("empty config = %+v", empty)
	}

	// the config travels through the store as PATCH /config writes it
	patch := map[string]*string{"shared_buffers": new("256MB")}
	value, err := json.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(store.Command{Op: "merge", Key: keyConfig, Value: string(value)})
	if err != nil {
		t.Fatal(err)
	}
	st.Apply(raft.Entry{Index: 1, Data: data})

	cfg, err := loadConfig(st)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Settings["shared_buffers"] != "256MB" {
		t.Fatalf("settings = %v", cfg.Settings)
	}
	if cfg.Hash == empty.Hash {
		t.Fatalf("hash %q did not change", cfg.Hash)
	}
}

func TestLowersLimit(t *testing.T) {
	primary := map[string]int64{"max_connections": 100}
	for _, tc := range []struct {
		settings map[string]string
		want     bool
	}{
		{map[string]string{"max_connections": "99"}, true},
		{map[string]string{"max_connections": "100"}, false},
		{map[string]string{"max_connections": "101"}, false},
		{map[string]string{"work_mem": "4MB"}, false}, // not a hot standby limit
	} {
		if got := lowersLimit(tc.settings, primary); got != tc.want {
			t.Errorf("lowersLimit(%v) = %v, want %v", tc.settings, got, tc.want)
		}
	}
}

func TestRolloutTurn(t *testing.T) {
	cfg := Config{Hash: "h"}
	applied := agent.Postgres{Config: "h"}
	stale := agent.Postgres{Config: "old"}

	// usual order: the standbys in id order, the primary last
	nodes := map[string]agent.Postgres{"p": applied, "s1": stale, "s2": stale}
	if !rolloutTurn("s1", "p", cfg, nodes) {
		t.Error("s1: want its turn")
	}
	if rolloutTurn("s2", "p", cfg, nodes) {
		t.Error("s2: must wait for s1")
	}
	if rolloutTurn("p", "p", cfg, nodes) {
		t.Error("primary must wait for the standbys")
	}
	nodes["s1"] = applied
	if !rolloutTurn("s2", "p", cfg, nodes) {
		t.Error("s2: want its turn after s1")
	}
	if rolloutTurn("p", "p", cfg, nodes) {
		t.Error("primary must still wait for s2")
	}
	nodes["s2"] = applied
	if !rolloutTurn("p", "p", cfg, nodes) {
		t.Error("with every standby on cfg, the primary has its turn")
	}

	// a config that lowers a hot standby limit goes to the primary first
	low := Config{Hash: "low", Settings: map[string]string{"max_connections": "50"}}
	nodes = map[string]agent.Postgres{
		"p":  {Config: "old", Limits: map[string]int64{"max_connections": 100}},
		"s1": stale,
	}
	if !rolloutTurn("p", "p", low, nodes) {
		t.Error("a lowered limit must reach the primary first")
	}
	if rolloutTurn("s1", "p", low, nodes) {
		t.Error("s1 must wait for the primary")
	}
	nodes["p"] = agent.Postgres{Config: "low", Limits: nodes["p"].Limits}
	if !rolloutTurn("s1", "p", low, nodes) {
		t.Error("s1: want its turn after the primary")
	}

	// a node whose apply failed blocks the nodes after it
	nodes = map[string]agent.Postgres{"p": applied, "s1": {Config: "h", ConfigError: "boom"}, "s2": stale}
	if rolloutTurn("s2", "p", cfg, nodes) {
		t.Error("s2 must wait for the failed s1")
	}
	if rolloutTurn("p", "p", cfg, nodes) {
		t.Error("the primary must wait for the failed s1")
	}

	// a node the poll did not reach takes no part in the order
	if rolloutTurn("ghost", "p", cfg, nodes) {
		t.Error("an unknown node never has its turn")
	}
}

func TestParameterName(t *testing.T) {
	for _, ok := range []string{"work_mem", "a", "a_b_2", "auth.delay", "x1"} {
		if !parameterName.MatchString(ok) {
			t.Errorf("%q: want a match", ok)
		}
	}
	for _, bad := range []string{"", "Work_Mem", "1x", "-x", "a..b", "a.", ".a"} {
		if parameterName.MatchString(bad) {
			t.Errorf("%q: want no match", bad)
		}
	}
}

func TestPatchConfigRejectsCommandParameters(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "config.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv, err := agent.NewServer("a", map[string]string{"a": "127.0.0.1:0"}, st, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	// A blocked parameter is refused before validation and before raft, so
	// neither a leader nor a running Postgres is needed.
	validate := func(context.Context, map[string]*string) error {
		t.Error("a blocked parameter must not reach validation")
		return nil
	}
	mux := http.NewServeMux()
	configHandlers(mux, srv, st, validate, false)
	for _, name := range []string{
		"archive_command", "archive_cleanup_command", "archive_library",
		"dynamic_library_path", "local_preload_libraries", "recovery_end_command",
		"restore_command", "session_preload_libraries", "shared_preload_libraries",
		"ssl_passphrase_command", "ssl_passphrase_command_supports_reload",
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/config",
			strings.NewReader(`{"`+name+`":"/bin/false"}`)))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400 (%s)", name, rec.Code, rec.Body)
		}
	}
}

func TestPatchConfigValidates(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "config.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv, err := agent.NewServer("a", map[string]string{"a": "127.0.0.1:0"}, st, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	t.Cleanup(srv.Stop)

	var got map[string]*string
	validate := func(_ context.Context, patch map[string]*string) error {
		got = patch
		return errValidationFailed
	}
	mux := http.NewServeMux()
	configHandlers(mux, srv, st, validate, false)

	// a lone node elects itself, then proposals commit at once
	waitConfigLeader(t, srv)
	patch := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/config", strings.NewReader(body))
		mux.ServeHTTP(rec, req)
		return rec
	}

	rec := patch(`{"work_mem":"4096kB"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("rejected value: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if got["work_mem"] == nil || *got["work_mem"] != "4096kB" {
		t.Fatalf("validator saw %v", got)
	}

	validate = func(context.Context, map[string]*string) error { return errValidationUnavailable }
	mux2 := http.NewServeMux()
	configHandlers(mux2, srv, st, validate, false)
	rec = httptest.NewRecorder()
	mux2.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/config", strings.NewReader(`{"work_mem":"4096kB"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable validator: got %d, want 503 (%s)", rec.Code, rec.Body)
	}

	validate = func(context.Context, map[string]*string) error { return nil }
	mux3 := http.NewServeMux()
	configHandlers(mux3, srv, st, validate, false)
	rec = httptest.NewRecorder()
	mux3.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/config", strings.NewReader(`{"work_mem":"4096kB"}`)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("valid value: got %d, want 204 (%s)", rec.Code, rec.Body)
	}
	cfg, err := loadConfig(st)
	if err != nil || cfg.Settings["work_mem"] != "4096kB" {
		t.Fatalf("stored config: %v %v", cfg.Settings, err)
	}
}

var errValidationFailed = errors.New("invalid value for parameter \"work_mem\"")

func waitConfigLeader(t *testing.T, srv *agent.Server) {
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

func TestValidateSettingsResetsOnly(t *testing.T) {
	// with nothing to check there is no need for a running Postgres
	if err := (&Instance{}).validateSettings(context.Background(), map[string]*string{"work_mem": nil}); err != nil {
		t.Fatalf("resets only: %v", err)
	}
	if err := (&Instance{}).validateSettings(context.Background(), map[string]*string{"work_mem": new("1MB")}); err == nil {
		t.Fatal("a check without postgres: want an error")
	}
}
