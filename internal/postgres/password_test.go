package postgres

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/store"
)

func TestPutPassword(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "password.db"))
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
	passwordHandlers(mux, srv)
	waitConfigLeader(t, srv) // a lone node elects itself and commits at once

	put := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/password", strings.NewReader(body)))
		return rec
	}

	// a trailing newline is a file's end, not part of the password
	if rec := put("s3cret\n"); rec.Code != http.StatusNoContent {
		t.Fatalf("rotate: got %d, want 204 (%s)", rec.Code, rec.Body)
	}
	pw, err := st.Get(agent.KeyPassword)
	if err != nil || pw != "s3cret" {
		t.Fatalf("stored password %q, %v; want s3cret", pw, err)
	}

	for _, body := range []string{"", "\n"} {
		if rec := put(body); rec.Code != http.StatusBadRequest {
			t.Errorf("empty body %q: got %d, want 400", body, rec.Code)
		}
	}
}

func TestInstancePasswordSource(t *testing.T) {
	file := filepath.Join(t.TempDir(), "pgpass")
	if err := os.WriteFile(file, []byte("from-file\nsecond-line\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	i := &Instance{PasswordFile: file}
	if pw, err := i.password(); err != nil || pw != "from-file" {
		t.Fatalf("file: got %q, %v; want from-file", pw, err)
	}
	i = &Instance{Password: "from-flag", PasswordFile: file}
	if pw, err := i.password(); err != nil || pw != "from-flag" {
		t.Fatalf("flag over file: got %q, %v; want from-flag", pw, err)
	}
	i.SetPassword("rotated")
	if pw, err := i.password(); err != nil || pw != "rotated" {
		t.Fatalf("rotation over both: got %q, %v; want rotated", pw, err)
	}
}

func TestPwfile(t *testing.T) {
	// with --pg-password the file exists only for initdb, then it is gone
	i := &Instance{Password: "env-password"}
	path, cleanup, err := i.pwfile()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "env-password\n" {
		t.Fatalf("pwfile = %q, %v; want env-password\\n", data, err)
	}
	cleanup()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("pwfile survived: %v", err)
	}

	// with --pg-password-file initdb reads it as-is
	file := filepath.Join(t.TempDir(), "pgpass")
	if err := os.WriteFile(file, []byte("pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	i = &Instance{PasswordFile: file}
	if path, cleanup, err = i.pwfile(); err != nil || path != file {
		t.Fatalf("pwfile = %q, %v; want the flag's path", path, err)
	}
	cleanup()
}

// The startup password counts as applied: without a rotation the primary
// never runs an ALTER ROLE. The check needs no Postgres — a query without
// a server would fail the test.
func TestApplyPasswordSkipsStartupPassword(t *testing.T) {
	i := &Instance{}
	i.SeedPassword("startup")
	if err := i.ApplyPassword(context.Background()); err != nil {
		t.Fatalf("seeded: %v", err)
	}

	i.SetPassword("rotated")
	if err := i.ApplyPassword(context.Background()); err == nil {
		t.Fatal("a rotation must reach Postgres (and fail without one running)")
	}
}

func TestGeneratePassword(t *testing.T) {
	a, err := generatePassword(24)
	if err != nil {
		t.Fatal(err)
	}
	b, err := generatePassword(24)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 24 || len(b) != 24 || a == b {
		t.Fatalf("generated %q and %q", a, b)
	}
	for _, r := range a {
		if !strings.ContainsRune(passwordAlphabet, r) {
			t.Fatalf("%q: rune %q outside the alphabet", a, r)
		}
	}
}
