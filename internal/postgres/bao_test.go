package postgres

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/store"
)

// baoServer fakes the KV v2 read and the AppRole login of OpenBao. The
// mount holds one secret at tembo/pg; token count is how many logins ran.
func baoServer(t *testing.T, secret map[string]string, validToken *string, logins *int) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/secret/data/tembo/pg", func(w http.ResponseWriter, r *http.Request) {
		tok := r.Header.Get("X-Vault-Token")
		if tok == "" || tok != *validToken {
			http.Error(w, "permission denied", http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"data": secret},
		})
	})
	mux.HandleFunc("POST /v1/auth/approle/login", func(w http.ResponseWriter, r *http.Request) {
		*logins++
		var in map[string]string
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in["role_id"] == "" || in["secret_id"] == "" {
			http.Error(w, "bad credentials", http.StatusBadRequest)
			return
		}
		*validToken = "token-" + strings.Repeat("x", *logins) // a fresh token per login
		_ = json.NewEncoder(w).Encode(map[string]any{
			"auth": map[string]any{"client_token": *validToken},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestBaoReadsPasswordWithToken(t *testing.T) {
	token := "root"
	logins := 0
	srv := baoServer(t, map[string]string{"password": "from-bao\n"}, &token, &logins)

	c, err := newBaoClient(srv.URL, "root", "", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	pw, err := c.readPassword(context.Background(), "tembo/pg")
	if err != nil {
		t.Fatal(err)
	}
	// surrounding whitespace of a pasted value is not the password
	if pw != "from-bao" {
		t.Fatalf("password = %q, want from-bao", pw)
	}
	if logins != 0 {
		t.Fatalf("a token client must not log in (%d times)", logins)
	}
}

func TestBaoAppRoleLogsInAndAgain(t *testing.T) {
	token := ""
	logins := 0
	srv := baoServer(t, map[string]string{"password": "pw1"}, &token, &logins)

	c, err := newBaoClient(srv.URL, "", "", "/dev/null", "/dev/null", "", "")
	if err != nil {
		t.Fatal(err)
	}
	// the files do not exist; feed the credentials directly instead
	c.roleID, c.secretID = "role", "secret"

	if _, err := c.readPassword(context.Background(), "tembo/pg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if logins != 1 {
		t.Fatalf("logins = %d, want 1", logins)
	}

	// the stored token went stale: the next read logs in again and retries
	token = "stale"
	if _, err := c.readPassword(context.Background(), "tembo/pg"); err != nil {
		t.Fatalf("read after expiry: %v", err)
	}
	if logins != 2 {
		t.Fatalf("logins = %d, want 2 (a re-login on 403)", logins)
	}
}

func TestBaoSecretWithoutPassword(t *testing.T) {
	token := "root"
	logins := 0
	srv := baoServer(t, map[string]string{"other": "x"}, &token, &logins)
	c, err := newBaoClient(srv.URL, "root", "", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.readPassword(context.Background(), "tembo/pg"); err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("err = %v, want a missing-password complaint", err)
	}
}

func TestNewBaoClientRequiresCredentials(t *testing.T) {
	if _, err := newBaoClient("http://x", "", "", "", "", "", ""); err == nil {
		t.Fatal("no credentials: want an error")
	}
	if _, err := newBaoClient("", "t", "", "", "", "", ""); err == nil {
		t.Fatal("no address: want an error")
	}
	if _, err := newBaoClient("http://x", "", "", "/dev/null", "", "", ""); err == nil {
		t.Fatal("half an AppRole: want an error")
	}
}

// With the OpenBao integration the store rotation is off: the handler must
// refuse before raft, on every node.
func TestPutPasswordManagedByBao(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "bao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv, err := agent.NewServer("a", map[string]string{"a": "127.0.0.1:0"}, st, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	passwordHandlers(mux, srv)

	old := rc.BaoPasswordPath
	rc.BaoPasswordPath = "tembo/pg"
	t.Cleanup(func() { rc.BaoPasswordPath = old })

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/password", strings.NewReader("pw")))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("got %d %q, want 403", rec.Code, rec.Body)
	}
	if _, err := st.Get(agent.KeyPassword); err == nil {
		t.Fatal("nothing must reach the store in bao mode")
	}
}

// The AppRole credential files parse the way newBaoClient reads them.
func TestBaoCredentialFiles(t *testing.T) {
	dir := t.TempDir()
	for name, value := range map[string]string{"role": "my-role\n", "secret": "my-secret"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	c, err := newBaoClient("http://x", "", "", filepath.Join(dir, "role"), filepath.Join(dir, "secret"), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if c.roleID != "my-role" || c.secretID != "my-secret" {
		t.Fatalf("roleID = %q, secretID = %q", c.roleID, c.secretID)
	}
}

func gauge(t *testing.T, c *baoClient, name string) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func TestBaoMetrics(t *testing.T) {
	token := "root"
	logins := 0
	srv := baoServer(t, map[string]string{"password": "pw"}, &token, &logins)
	c, err := newBaoClient(srv.URL, "root", "", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}

	// nothing read yet
	if v := gauge(t, c, "tembo_bao_password_success"); v != 0 {
		t.Fatalf("fresh success = %v, want 0", v)
	}

	if _, err := c.readPassword(context.Background(), "tembo/pg"); err != nil {
		t.Fatal(err)
	}
	if v := gauge(t, c, "tembo_bao_password_success"); v != 1 {
		t.Fatalf("success = %v, want 1", v)
	}
	last := gauge(t, c, "tembo_bao_password_last_success_seconds")
	if last <= 0 {
		t.Fatalf("last success = %v, want a Unix time", last)
	}

	// a token client does not re-login: the stale token is a failed read
	token = "stale"
	if _, err := c.readPassword(context.Background(), "tembo/pg"); err == nil {
		t.Fatal("want an error for the stale token")
	}
	if v := gauge(t, c, "tembo_bao_password_success"); v != 0 {
		t.Fatalf("success after failure = %v, want 0", v)
	}
	if gauge(t, c, "tembo_bao_password_last_success_seconds") != last {
		t.Fatal("the last success must survive a failed read")
	}

	// and back: the server accepts the client's token again
	token = "root"
	if _, err := c.readPassword(context.Background(), "tembo/pg"); err != nil {
		t.Fatal(err)
	}
	if v := gauge(t, c, "tembo_bao_password_success"); v != 1 {
		t.Fatalf("success after recovery = %v, want 1", v)
	}
}
