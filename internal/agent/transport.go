package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ks-tool/tembo/internal/raft"
)

const (
	pathVote     = "/raft/vote"
	pathAppend   = "/raft/append"
	pathSnapshot = "/raft/snapshot"
)

// HTTPTransport ships raft RPCs between nodes as JSON over HTTP POST. It is
// the outbound half; RegisterRaftHandlers is the inbound half.
type HTTPTransport struct {
	resolve func(id string) (string, bool) // node id → host:port
	scheme  string
	client  *http.Client
}

// NewHTTPTransport returns a transport that reaches a node by resolving its
// id, so membership changes take effect as they happen.
func NewHTTPTransport(resolve func(id string) (string, bool)) *HTTPTransport {
	return &HTTPTransport{resolve: resolve, scheme: "http", client: &http.Client{}}
}

// UseTLS makes the transport dial HTTPS, verifying the servers with cfg
// (its RootCAs; nodes carry no client certificates).
func (t *HTTPTransport) UseTLS(cfg *tls.Config) {
	t.scheme = "https"
	t.client = &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
}

// RequestVote implements raft.Transport.
func (t *HTTPTransport) RequestVote(ctx context.Context, to string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	return resp, t.post(ctx, to, pathVote, req, &resp)
}

// AppendEntries implements raft.Transport.
func (t *HTTPTransport) AppendEntries(ctx context.Context, to string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	return resp, t.post(ctx, to, pathAppend, req, &resp)
}

// InstallSnapshot implements raft.Transport.
func (t *HTTPTransport) InstallSnapshot(ctx context.Context, to string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	var resp raft.InstallSnapshotResponse
	return resp, t.post(ctx, to, pathSnapshot, req, &resp)
}

func (t *HTTPTransport) post(ctx context.Context, to, path string, req, resp any) error {
	addr, ok := t.resolve(to)
	if !ok {
		return fmt.Errorf("unknown peer %q", to)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, t.scheme+"://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	res, err := t.client.Do(httpReq)
	if err != nil {
		return err // raft treats any error as "no answer" and retries on its own
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s%s: %s", addr, path, res.Status)
	}
	return json.NewDecoder(res.Body).Decode(resp)
}

// RegisterRaftHandlers mounts the inbound side of the RPCs on mux, feeding
// them to h (normally the *raft.Node itself).
func RegisterRaftHandlers(mux *http.ServeMux, h raft.Handler) {
	mux.HandleFunc("POST "+pathVote, func(w http.ResponseWriter, r *http.Request) {
		var req raft.RequestVoteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, h.HandleRequestVote(req))
	})
	mux.HandleFunc("POST "+pathAppend, func(w http.ResponseWriter, r *http.Request) {
		var req raft.AppendEntriesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, h.HandleAppendEntries(req))
	})
	mux.HandleFunc("POST "+pathSnapshot, func(w http.ResponseWriter, r *http.Request) {
		var req raft.InstallSnapshotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, h.HandleInstallSnapshot(req))
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
