package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ks-tool/tembo/internal/raft"
)

func TestHTTPTransportTLS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != pathVote || r.TLS == nil {
			http.Error(w, "want a TLS raft vote", http.StatusBadRequest)
			return
		}
		writeJSON(w, raft.RequestVoteResponse{Term: 3, Granted: true})
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "https://")
	resolve := func(string) (string, bool) { return addr, true }

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	tr := NewHTTPTransport(resolve)
	tr.UseTLS(&tls.Config{RootCAs: pool})
	resp, err := tr.RequestVote(context.Background(), "a", raft.RequestVoteRequest{Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Term != 3 || !resp.Granted {
		t.Errorf("got %+v", resp)
	}

	// without UseTLS the transport stays on plain HTTP and fails against the
	// TLS server
	plain := NewHTTPTransport(resolve)
	if _, err := plain.RequestVote(context.Background(), "a", raft.RequestVoteRequest{Term: 1}); err == nil {
		t.Error("plain HTTP against a TLS server must fail")
	}
}
