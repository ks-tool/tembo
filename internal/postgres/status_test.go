package postgres

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// withCLIFlags points the CLI flags at the values of the test and restores
// them after it.
func withCLIFlags(t *testing.T, addr, token, tokenFile, tlsCA string) {
	t.Helper()
	oldAddr, oldToken, oldTokenFile := agentAddr, cliToken, cliTokenFile
	oldCert, oldKey, oldCA := cliTLSCert, cliTLSKey, cliTLSCA
	agentAddr, cliToken, cliTokenFile, cliTLSCA = addr, token, tokenFile, tlsCA
	cliTLSCert, cliTLSKey = "", ""
	t.Cleanup(func() {
		agentAddr, cliToken, cliTokenFile = oldAddr, oldToken, oldTokenFile
		cliTLSCert, cliTLSKey, cliTLSCA = oldCert, oldKey, oldCA
	})
}

func TestAgentURL(t *testing.T) {
	withCLIFlags(t, "127.0.0.1:8001", "", "", "")
	if got := agentURL(); got != "http://127.0.0.1:8001" {
		t.Errorf("plain: got %q", got)
	}
	agentAddr = "https://tembo-a:8001"
	if got := agentURL(); got != "https://tembo-a:8001" {
		t.Errorf("prefixed: got %q", got)
	}
	agentAddr, cliTLSCA = "tembo-a:8001", "ca.pem"
	if got := agentURL(); got != "https://tembo-a:8001" {
		t.Errorf("a --tls-* flag implies https: got %q", got)
	}
}

func TestBearerToken(t *testing.T) {
	withCLIFlags(t, "", "tok", "", "")
	if tok, err := bearerToken(); tok != "tok" || err != nil {
		t.Errorf("--token: got %q, %v", tok, err)
	}

	f := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(f, []byte("from-file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cliToken, cliTokenFile = "", f
	if tok, err := bearerToken(); tok != "from-file" || err != nil {
		t.Errorf("--token-file: got %q, %v", tok, err)
	}
	cliToken = "tok" // --token wins
	if tok, err := bearerToken(); tok != "tok" || err != nil {
		t.Errorf("--token over --token-file: got %q, %v", tok, err)
	}
	cliToken, cliTokenFile = "", filepath.Join(t.TempDir(), "missing")
	if _, err := bearerToken(); err == nil {
		t.Error("a missing token file must fail")
	}
}

func TestCLITLSConfig(t *testing.T) {
	ca, cert, key := writeCert(t, t.TempDir())
	withCLIFlags(t, "", "", "", "")
	if cfg, err := cliTLSConfig(); cfg != nil || err != nil {
		t.Errorf("no flags: got %v, %v", cfg, err)
	}
	cliTLSCert = cert
	if _, err := cliTLSConfig(); err == nil {
		t.Error("a certificate without a key must fail")
	}
	cliTLSKey, cliTLSCA = key, ca
	cfg, err := cliTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Certificates) != 1 || cfg.RootCAs == nil {
		t.Errorf("cert, key and CA: got %+v", cfg)
	}
}

func TestRequestBearer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("got Authorization %q", got)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	withCLIFlags(t, srv.URL, "tok", "", "")
	data, err := request(context.Background(), http.MethodGet, "/status", nil)
	if err != nil || string(data) != "ok" {
		t.Fatalf("got %q, %v", data, err)
	}
}

func TestRequestHTTPS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	// trust the server's certificate
	ca := filepath.Join(t.TempDir(), "ca.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(ca, certPEM, 0600); err != nil {
		t.Fatal(err)
	}

	withCLIFlags(t, srv.URL, "", "", ca)
	data, err := request(context.Background(), http.MethodGet, "/status", nil)
	if err != nil || string(data) != "ok" {
		t.Fatalf("got %q, %v", data, err)
	}

	// without --tls-ca the certificate verifies against the system pool only
	cliTLSCA = ""
	if _, err := request(context.Background(), http.MethodGet, "/status", nil); err == nil {
		t.Fatal("an untrusted server certificate must fail")
	}
}
