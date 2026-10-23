package postgres

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func shellMux(t *testing.T) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	opsHandlers(mux, nil, &Instance{DataDir: t.TempDir()}, "a")
	return mux
}

func postShell(mux *http.ServeMux, target, user string) int {
	req := httptest.NewRequest(http.MethodPost, target, nil)
	if user != "" {
		req.Header.Set("X-Tembo-Pg-User", user)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Code
}

func TestShellNeedsRole(t *testing.T) {
	if code := postShell(shellMux(t), "/shell", ""); code != http.StatusBadRequest {
		t.Fatalf("no role: status %d, want 400", code)
	}
	if code := postShell(shellMux(t), "/shell", "app;drop"); code != http.StatusBadRequest {
		t.Fatalf("bad role: status %d, want 400", code)
	}
}

func TestShellRefusesBadDatabaseName(t *testing.T) {
	if code := postShell(shellMux(t), "/shell?db=a%20b", "app"); code != http.StatusBadRequest {
		t.Fatalf("bad database: status %d, want 400", code)
	}
}

func TestShellNeedsRunningPostgres(t *testing.T) {
	if code := postShell(shellMux(t), "/shell", "app"); code != http.StatusServiceUnavailable {
		t.Fatalf("no postgres: status %d, want 503", code)
	}
}

func TestShellIsPostOnly(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/shell", nil)
	req.Header.Set("X-Tembo-Pg-User", "app")
	shellMux(t).ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatal("GET /shell must not open a session")
	}
}

func TestPlainPasswordOnlyOverTLSOrLoopback(t *testing.T) {
	for _, tc := range []struct {
		url, password string
		ok            bool
	}{
		{"http://10.0.0.5:8001", "", true},        // no password, nothing to protect
		{"http://10.0.0.5:8001", "s3cret", false}, // clear text to another host
		{"https://10.0.0.5:8001", "s3cret", true}, // TLS
		{"http://127.0.0.1:8001", "s3cret", true}, // the node itself
		{"http://localhost:8001", "s3cret", true}, //
		{"http://[::1]:8001", "s3cret", true},     // IPv6 loopback
	} {
		err := plainPassword(tc.url, tc.password)
		if (err == nil) != tc.ok {
			t.Errorf("plainPassword(%s, pw=%q) = %v, want ok=%v", tc.url, tc.password, err, tc.ok)
		}
	}
}
