package postgres

import (
	"testing"

	"github.com/ks-tool/tembo/internal/agent"
)

func TestFormatSyncStandbys(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		count uint64
		ids   []string
		want  string
	}{
		{agent.SyncOff, 1, []string{"b"}, ""},
		{agent.SyncQuorum, 1, nil, ""},
		{agent.SyncQuorum, 1, []string{"b"}, `ANY 1 ("b")`},
		{agent.SyncQuorum, 2, []string{"b", "c"}, `ANY 2 ("b", "c")`},
		{agent.SyncQuorum, 5, []string{"b"}, `ANY 1 ("b")`}, // capped at the standbys there are
		{agent.SyncQuorum, 0, []string{"b"}, `ANY 1 ("b")`}, // zero means one
		{agent.SyncStrict, 1, []string{"b", "c"}, `FIRST 1 ("b", "c")`},
	} {
		if got := formatSyncStandbys(tc.mode, tc.count, tc.ids); got != tc.want {
			t.Errorf("formatSyncStandbys(%q, %d, %v) = %q, want %q", tc.mode, tc.count, tc.ids, got, tc.want)
		}
	}
}

func TestSyncCandidates(t *testing.T) {
	nodes := map[string]agent.Postgres{
		"a": {Running: true},                   // self
		"b": {Running: true, InRecovery: true}, // running, streaming
		"c": {Running: true, InRecovery: true}, // running, not streaming
		"d": {InRecovery: true},                // down
		"e": {Running: true},                   // not a standby
	}
	got := syncCandidates(nodes, []string{"b", "d", "ghost"}, "a")
	if len(got) != 1 || got[0] != "b" {
		t.Errorf("got %v, want [b]: running, in recovery and streaming", got)
	}

	// a node tagged nosync never takes part
	nodes["b"] = agent.Postgres{Running: true, InRecovery: true, NoSync: true}
	if got := syncCandidates(nodes, []string{"b"}, "a"); len(got) != 0 {
		t.Errorf("nosync: got %v, want none", got)
	}
}

func TestSetSyncStandbys(t *testing.T) {
	i := &Instance{}

	if i.SetSyncStandbys(agent.SyncOff, 1, nil) {
		t.Error("off to off: want no change")
	}
	if !i.SetSyncStandbys(agent.SyncQuorum, 1, []string{"b"}) {
		t.Error("off to quorum: want a change")
	}
	if got := i.syncStandbyNames(); got != `ANY 1 ("b")` {
		t.Errorf("names = %q", got)
	}
	if i.SetSyncStandbys(agent.SyncQuorum, 1, []string{"b"}) {
		t.Error("same list: want no change")
	}
	if !i.SetSyncStandbys(agent.SyncQuorum, 1, []string{"b", "c"}) {
		t.Error("a new standby: want a change")
	}

	// an empty list in quorum mode turns synchronous commit off
	if !i.SetSyncStandbys(agent.SyncQuorum, 1, nil) {
		t.Error("quorum with no standbys: want a change to off")
	}
	if got := i.syncStandbyNames(); got != "" {
		t.Errorf("names = %q, want off", got)
	}

	// in strict mode an empty list keeps the previous one
	if !i.SetSyncStandbys(agent.SyncStrict, 1, []string{"b"}) {
		t.Error("strict with a standby: want a change")
	}
	if i.SetSyncStandbys(agent.SyncStrict, 1, nil) {
		t.Error("strict with no standbys: want the previous list kept")
	}
	if got := i.SyncStandbys(); len(got) != 1 || got[0] != "b" {
		t.Errorf("ids = %v, want [b]", got)
	}
	if got := i.syncStandbyNames(); got != `FIRST 1 ("b")` {
		t.Errorf("names = %q, want the strict list kept", got)
	}

	// off clears even the strict list
	if !i.SetSyncStandbys(agent.SyncOff, 1, nil) {
		t.Error("off after strict: want a change")
	}
	if got := i.SyncStandbys(); len(got) != 0 {
		t.Errorf("ids = %v, want none", got)
	}
}
