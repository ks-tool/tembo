package store_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

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

// apply runs cmd through Apply as raft would, at log index index.
func apply(t *testing.T, st *store.Store, index uint64, cmd store.Command) {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	st.Apply(raft.Entry{Index: index, Data: data})
}

func get(t *testing.T, st *store.Store, key string) string {
	t.Helper()
	v, err := st.Get(key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return v
}

func TestSetAddDelMerge(t *testing.T) {
	st := openStore(t)

	apply(t, st, 1, store.Command{Op: "set", Key: "a", Value: "1"})
	if v := get(t, st, "a"); v != "1" {
		t.Fatalf("a = %q, want %q", v, "1")
	}

	// add writes only a key that is absent
	apply(t, st, 2, store.Command{Op: "add", Key: "a", Value: "2"})
	if v := get(t, st, "a"); v != "1" {
		t.Fatalf("a = %q, want %q", v, "1")
	}
	apply(t, st, 3, store.Command{Op: "add", Key: "b", Value: "3"})
	if v := get(t, st, "b"); v != "3" {
		t.Fatalf("b = %q, want %q", v, "3")
	}

	// merge writes fields of a JSON object, null removes one
	apply(t, st, 4, store.Command{Op: "merge", Key: "m", Value: `{"x":"1"}`})
	if v := get(t, st, "m"); v != `{"x":"1"}` {
		t.Fatalf("m = %q, want %q", v, `{"x":"1"}`)
	}
	apply(t, st, 5, store.Command{Op: "merge", Key: "m", Value: `{"x":null,"y":2}`})
	if v := get(t, st, "m"); v != `{"y":2}` {
		t.Fatalf("m = %q, want %q", v, `{"y":2}`)
	}

	apply(t, st, 6, store.Command{Op: "del", Key: "b"})
	if _, err := st.Get("b"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get b: got %v, want ErrNotFound", err)
	}

	if idx, err := st.AppliedIndex(); err != nil || idx != 6 {
		t.Fatalf("applied index = %d, %v; want 6", idx, err)
	}
}

func TestReplayAppliesOnce(t *testing.T) {
	st := openStore(t)

	// the leader replays the log to a node that already applied it
	apply(t, st, 1, store.Command{Op: "set", Key: "k", Value: "1"})
	apply(t, st, 1, store.Command{Op: "set", Key: "k", Value: "2"})
	if v := get(t, st, "k"); v != "1" {
		t.Fatalf("k = %q, want %q", v, "1")
	}

	apply(t, st, 2, store.Command{Op: "del", Key: "k"})
	apply(t, st, 1, store.Command{Op: "set", Key: "k", Value: "3"}) // older than applied
	if _, err := st.Get("k"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get k: got %v, want ErrNotFound", err)
	}
}

func TestMalformedEntryIsSkipped(t *testing.T) {
	st := openStore(t)

	st.Apply(raft.Entry{Index: 1, Data: []byte("not json")})
	apply(t, st, 2, store.Command{Op: "bogus", Key: "k", Value: "v"})

	if _, err := st.Get("k"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get k: got %v, want ErrNotFound", err)
	}
	if idx, err := st.AppliedIndex(); err != nil || idx != 2 {
		t.Fatalf("applied index = %d, %v; want 2", idx, err)
	}
}

func TestMergeIntoNonObjectIsSkipped(t *testing.T) {
	st := openStore(t)

	apply(t, st, 1, store.Command{Op: "set", Key: "s", Value: "scalar"})
	apply(t, st, 2, store.Command{Op: "merge", Key: "s", Value: `{"x":1}`})

	if v := get(t, st, "s"); v != "scalar" {
		t.Fatalf("s = %q, want %q", v, "scalar")
	}
	if idx, err := st.AppliedIndex(); err != nil || idx != 2 {
		t.Fatalf("applied index = %d, %v; want 2", idx, err)
	}
}

func TestGetMissingKey(t *testing.T) {
	st := openStore(t)
	if _, err := st.Get("nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestRaftStorageRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rs := st.RaftStorage()
	if got, err := rs.Load(); err != nil || !reflect.DeepEqual(got, raft.Stored{}) {
		t.Fatalf("new store: %+v %v", got, err)
	}

	if err := rs.SaveState(3, "b"); err != nil {
		t.Fatal(err)
	}
	entries := []raft.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Data: []byte("x")}, {Index: 3, Term: 2, Data: []byte("y")}}
	if err := rs.SetLog(1, entries); err != nil {
		t.Fatal(err)
	}
	// a conflicting suffix replaces what was stored from its index on
	if err := rs.SetLog(3, []raft.Entry{{Index: 3, Term: 3, Data: []byte("z")}}); err != nil {
		t.Fatal(err)
	}
	if err := rs.SetLog(5, []raft.Entry{{Index: 6}}); err == nil {
		t.Fatal("SetLog accepted an entry at the wrong index")
	}

	if err := st.Close(); err != nil { // survives a reopen
		t.Fatal(err)
	}
	if st, err = store.Open(path); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	got, err := st.RaftStorage().Load()
	want := raft.Stored{Term: 3, VotedFor: "b", Log: []raft.Entry{entries[0], entries[1], {Index: 3, Term: 3, Data: []byte("z")}}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v %v, want %+v", got, err, want)
	}
}

func TestCompactAndInstallSnapshot(t *testing.T) {
	leader := openStore(t)
	rs := leader.RaftStorage()
	var log []raft.Entry
	for i := uint64(1); i <= 4; i++ {
		e := raft.Entry{Index: i, Term: 1}
		if i != 2 { // entry 2 is a no-op
			e.Data, _ = json.Marshal(store.Command{Op: "set", Key: "k", Value: fmt.Sprint("v", i)})
		}
		log = append(log, e)
		if err := rs.SetLog(i, []raft.Entry{e}); err != nil {
			t.Fatal(err)
		}
		leader.Apply(e)
	}
	if applied, _ := leader.AppliedIndex(); applied != 4 {
		t.Fatalf("a no-op was not counted as applied: %d", applied)
	}

	if err := rs.Compact(raft.Entry{Index: 5, Term: 1}, raft.Membership{}); err == nil {
		t.Fatal("compacted past the applied index")
	}
	config := raft.Membership{Voters: map[string]string{"a": "a:1", "b": "b:1"}}
	if err := rs.Compact(raft.Entry{Index: 3, Term: 1}, config); err != nil {
		t.Fatal(err)
	}
	got, err := rs.Load()
	if err != nil || got.Base.Index != 3 || got.Base.Term != 1 || !reflect.DeepEqual(got.Log, log[3:]) {
		t.Fatalf("after compacting: %+v %v", got, err)
	}
	if !reflect.DeepEqual(got.Config, &config) {
		t.Fatalf("config after compacting: %+v", got.Config)
	}

	data, applied, err := rs.Snapshot()
	if err != nil || applied != 4 {
		t.Fatalf("snapshot: %d %v", applied, err)
	}
	follower := openStore(t)
	apply(t, follower, 1, store.Command{Op: "set", Key: "stale", Value: "x"})
	if err = follower.RaftStorage().InstallSnapshot(raft.Entry{Index: 4, Term: 1}, false, config, data); err != nil {
		t.Fatal(err)
	}
	if v, err := follower.Get("k"); err != nil || v != "v4" {
		t.Fatalf("k: %q %v", v, err)
	}
	if _, err := follower.Get("stale"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stale key survived: %v", err)
	}
	if applied, _ := follower.AppliedIndex(); applied != 4 {
		t.Fatalf("applied %d, want 4", applied)
	}
	if st, err := follower.RaftStorage().Load(); err != nil || st.Base.Index != 4 || len(st.Log) != 0 {
		t.Fatalf("log after the snapshot: %+v %v", st, err)
	}
}
