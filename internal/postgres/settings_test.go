package postgres

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/store"
)

func TestParseSize(t *testing.T) {
	for s, want := range map[string]uint64{
		"0":       0,
		"1048576": 1048576,
		"1kB":     1 << 10,
		"16MB":    16 << 20,
		"16mb":    16 << 20, // case-insensitive
		"1GB":     1 << 30,
		"2 TB":    2 << 40,
	} {
		if got, err := parseSize(s); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", s, got, err, want)
		}
	}
	for _, bad := range []string{"", "MB", "-1", "1.5MB", "abc", "1PB", "18446744073709551615TB"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q): want an error", bad)
		}
	}
}

func TestRemoveHBALine(t *testing.T) {
	lines := []string{
		"host all all 10.0.0.0/24 scram-sha-256",
		"hostssl db1 app 192.168.1.7 scram-sha-256",
		"hostssl db1 app 192.168.1.7 scram-sha-256", // a duplicate
	}
	got, err := removeHBALine(lines, "hostssl db1 app 192.168.1.7 scram-sha-256")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"host all all 10.0.0.0/24 scram-sha-256", "hostssl db1 app 192.168.1.7 scram-sha-256"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want the first match dropped: %v", got, want)
	}
	if _, err := removeHBALine(lines, "host all all 10.9.0.0/16 scram-sha-256"); err == nil {
		t.Fatal("no error for a missing line")
	}
}

func TestPatchSettings(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "settings.db"))
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

	mux := http.NewServeMux()
	settingsHandlers(mux, srv, st, false)

	waitConfigLeader(t, srv) // a lone node elects itself, then proposals commit at once
	patch := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/settings", strings.NewReader(body))
		mux.ServeHTTP(rec, req)
		return rec
	}

	if rec := patch(`{"lag":1048576}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown setting: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if rec := patch(`{"max-lag-on-failover":"abc"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad value: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if rec := patch(`{"synchronous_mode":"sometimes"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad mode: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if rec := patch(`{"synchronous_node_count":"0"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("zero count: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if rec := patch(`{"paused":"maybe"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad bool: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if rec := patch(`{"hba":"host all all 10.0.0.0/33 scram-sha-256"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad hba network: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if rec := patch(`{"hba":"socket all all scram-sha-256"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad hba type: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if rec := patch(`{"hba":"host all all 10.0.0.0/24 clear-text"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad hba auth: got %d, want 400 (%s)", rec.Code, rec.Body)
	}
	if rec := patch(`{"hba":"host all all 10.0.0.0/24"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("hba without auth: got %d, want 400 (%s)", rec.Code, rec.Body)
	}

	rec := patch(`{"max-lag-on-failover":"16MB","synchronous_mode":"quorum","synchronous_node_count":"2","hba":"host all all 10.0.0.0/24 scram-sha-256\nhostssl db1 app 192.168.1.7 scram-sha-256\nlocal all all peer"}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("valid value: got %d, want 204 (%s)", rec.Code, rec.Body)
	}
	raw, err := st.Get(agent.KeySettings)
	if err != nil {
		t.Fatal(err)
	}
	var settings agent.Settings
	if err := json.Unmarshal([]byte(raw), &settings); err != nil {
		t.Fatal(err)
	}
	if settings.MaxLagOnFailover != 16<<20 || settings.SynchronousMode != agent.SyncQuorum ||
		settings.SynchronousNodeCount != 2 {
		t.Fatalf("stored settings: %+v", settings)
	}
	wantHBA := []string{
		"host all all 10.0.0.0/24 scram-sha-256",
		"hostssl db1 app 192.168.1.7 scram-sha-256",
		"local all all peer",
	}
	if !slices.Equal(settings.HBA, wantHBA) {
		t.Fatalf("stored hba: %v, want %v", settings.HBA, wantHBA)
	}

	if rec := patch(`{"max-lag-on-failover":null,"hba":null}`); rec.Code != http.StatusNoContent {
		t.Fatalf("reset: got %d, want 204 (%s)", rec.Code, rec.Body)
	}
	raw, err = st.Get(agent.KeySettings)
	if err != nil {
		t.Fatal(err)
	}
	var after map[string]any
	if err := json.Unmarshal([]byte(raw), &after); err != nil {
		t.Fatal(err)
	}
	if _, ok := after["max-lag-on-failover"]; ok || len(after) != 2 {
		t.Fatalf("after reset: %v, want only the two synchronous settings", after)
	}
}
