package agent

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ks-tool/tembo/internal/raft"
	"github.com/ks-tool/tembo/internal/store"
)

func TestPickStandby(t *testing.T) {
	standby := func(lsn uint64) Postgres {
		return Postgres{Running: true, InRecovery: true, LSN: lsn}
	}
	pg := map[string]Postgres{
		"p": {Running: true, LSN: 100}, // the primary
		"a": standby(90),
		"b": standby(80),
	}

	if got := pickStandby(pg, "p", 100, 0, nil); got != "a" {
		t.Errorf("no limit: got %q, want the one with the most WAL", got)
	}
	if got := pickStandby(pg, "p", 100, 10, nil); got != "a" {
		t.Errorf("a is exactly at the limit: got %q", got)
	}
	if got := pickStandby(pg, "p", 100, 9, nil); got != "" {
		t.Errorf("both lag too much: got %q, want none", got)
	}
	if got := pickStandby(pg, "p", 0, 10, nil); got != "a" {
		t.Errorf("a primary never seen running sets no reference: got %q", got)
	}

	pg["b"] = Postgres{Running: true, InRecovery: true, LSN: 95}
	if got := pickStandby(pg, "p", 100, 10, nil); got != "b" {
		t.Errorf("b lags less: got %q", got)
	}

	// a synchronous standby is preferred over one with more WAL
	if got := pickStandby(pg, "p", 100, 10, []string{"a"}); got != "a" {
		t.Errorf("a is synchronous: got %q", got)
	}
	if got := pickStandby(pg, "p", 100, 10, []string{"b"}); got != "b" {
		t.Errorf("b is synchronous: got %q", got)
	}

	pg["b"] = Postgres{InRecovery: true, LSN: 99} // not running
	if got := pickStandby(pg, "p", 100, 10, nil); got != "a" {
		t.Errorf("b is down: got %q", got)
	}

	pg["b"] = Postgres{Running: true, LSN: 99} // not in recovery
	if got := pickStandby(pg, "p", 100, 10, nil); got != "a" {
		t.Errorf("b is not a standby: got %q", got)
	}

	// tags: nofailover excludes, failover_priority breaks ties first
	pg["b"] = Postgres{Running: true, InRecovery: true, LSN: 95, NoFailover: true}
	if got := pickStandby(pg, "p", 100, 10, nil); got != "a" {
		t.Errorf("b is nofailover: got %q", got)
	}
	pg["a"] = Postgres{Running: true, InRecovery: true, LSN: 90}
	pg["b"] = Postgres{Running: true, InRecovery: true, LSN: 95, FailoverPriority: 1}
	if got := pickStandby(pg, "p", 100, 10, nil); got != "b" {
		t.Errorf("b has the higher priority: got %q", got)
	}
	if got := pickStandby(pg, "p", 100, 10, []string{"a"}); got != "b" {
		t.Errorf("priority beats the synchronous standby: got %q", got)
	}

	// a standby far ahead of the primary's last report must not read as
	// lagging: adding the limit to such an LSN would wrap around
	pg["b"] = Postgres{Running: true, InRecovery: true, LSN: ^uint64(0)}
	if got := pickStandby(pg, "p", 100, 10, nil); got != "b" {
		t.Errorf("b runs ahead: got %q", got)
	}

	if got := pickStandby(map[string]Postgres{"p": pg["p"]}, "p", 100, 0, nil); got != "" {
		t.Errorf("no standbys: got %q, want none", got)
	}
}

func TestLoadSettings(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	if got := LoadSettings(st); got.MaxLagOnFailover != defaultMaxLagOnFailover ||
		got.SynchronousMode != SyncOff || got.SynchronousNodeCount != 1 {
		t.Errorf("no settings: got %+v, want the defaults", got)
	}

	set := func(index uint64, value string) {
		t.Helper()
		st.Apply(raft.Entry{Index: index, Data: []byte(
			`{"op":"merge","key":"settings","value":` + strconv.Quote(value) + `}`)})
	}

	set(1, `{"max-lag-on-failover":16777216}`)
	if got := LoadSettings(st); got.MaxLagOnFailover != 16<<20 {
		t.Errorf("configured: got %+v", got)
	}

	set(2, `{"max-lag-on-failover":0}`)
	if got := LoadSettings(st); got.MaxLagOnFailover != 0 {
		t.Errorf("zero lifts the limit: got %+v", got)
	}

	set(3, `{"synchronous_mode":"quorum","synchronous_node_count":2}`)
	if got := LoadSettings(st); got.SynchronousMode != SyncQuorum || got.SynchronousNodeCount != 2 {
		t.Errorf("synchronous: got %+v", got)
	}

	set(4, `{"max-lag-on-failover":null}`)
	if got := LoadSettings(st); got.MaxLagOnFailover != defaultMaxLagOnFailover {
		t.Errorf("reset: got %+v", got)
	}
}
