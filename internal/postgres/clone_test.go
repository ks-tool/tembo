package postgres

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// cloneInstance is a standby that clones through a shell command.
func cloneInstance(t *testing.T, cmd string) *Instance {
	t.Helper()
	return &Instance{DataDir: filepath.Join(t.TempDir(), "data"), Password: "secret", CloneCmd: cmd}
}

func TestFailedCloneLeavesMarker(t *testing.T) {
	i := cloneInstance(t, "false")
	if err := i.createData(context.Background(), "primary", false); err == nil {
		t.Fatal("a failing clone command: want an error")
	}
	if _, err := os.Stat(i.cloneMarker()); err != nil {
		t.Fatalf("the marker must survive a failed clone: %v", err)
	}
	if _, err := os.Stat(filepath.Join(i.DataDir, "standby.signal")); err == nil {
		t.Fatal("standby.signal written after a failed clone")
	}
}

func TestUnfinishedCloneIsDiscarded(t *testing.T) {
	i := cloneInstance(t, `printf 17 > "$TEMBO_PGDATA/PG_VERSION"`)
	// What an interrupted copy leaves: PG_VERSION is already there, so only
	// the marker tells it apart from a directory with data.
	if err := os.MkdirAll(i.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	junk := filepath.Join(i.DataDir, "junk.bin")
	if err := os.WriteFile(junk, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(i.DataDir, "PG_VERSION"), []byte("17"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(i.cloneMarker(), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := i.createData(context.Background(), "primary", false); err != nil {
		t.Fatalf("createData: %v", err)
	}
	if _, err := os.Stat(junk); err == nil {
		t.Fatal("the leftovers of the unfinished clone were not discarded")
	}
	if _, err := os.Stat(filepath.Join(i.DataDir, "standby.signal")); err != nil {
		t.Fatalf("the clone did not complete: %v", err)
	}
	if _, err := os.Stat(i.cloneMarker()); err == nil {
		t.Fatal("the marker outlived a completed clone")
	}
}

func TestCompletedCloneRemovesMarker(t *testing.T) {
	i := cloneInstance(t, `printf 17 > "$TEMBO_PGDATA/PG_VERSION"`)
	if err := i.createData(context.Background(), "primary", false); err != nil {
		t.Fatalf("createData: %v", err)
	}
	if _, err := os.Stat(i.cloneMarker()); err == nil {
		t.Fatal("the marker outlived a completed clone")
	}
	if _, err := os.Stat(filepath.Join(i.DataDir, "PG_VERSION")); err != nil {
		t.Fatalf("PG_VERSION missing after the clone: %v", err)
	}
}
