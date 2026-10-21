package agent

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/ks-tool/tembo/internal/raft"
	"github.com/ks-tool/tembo/internal/store"

	"github.com/rs/zerolog/log"
)

// Server glues one raft node, its BoltDB state machine and the HTTP surface:
// raft RPCs for peers plus the status of the node and the switchover for the
// CLI.
type Server struct {
	id        string
	node      *raft.Node
	store     *store.Store
	transport *HTTPTransport
	scheme    string      // of the URLs to the peers, "http" or "https"
	tlsCfg    *tls.Config // verifies the peers' servers when scheme is https

	mu       sync.Mutex
	peers    map[string]string   // id → host:port of the current membership, this node included
	postgres Postgres            // state of the local Postgres, see SetPostgres
	quorumAt time.Time           // when a leader and a majority were last seen, see Watch
	nodes    map[string]Postgres // nodes that answered the last poll, see Watch
	reported map[string][]string // the membership each polled node reported, see Watch

	// OnPeersChange is called when the membership in effect changes. The
	// addresses of new members become resolvable before the change commits.
	OnPeersChange func(peers map[string]string)
}

// KeyPrimary holds the id of the node whose Postgres is the primary.
const KeyPrimary = "primary"

// KeySettings holds the behavior settings of the agents as a JSON object,
// e.g. {"max-lag-on-failover":1048576}; PATCH /settings writes it.
const KeySettings = "settings"

// KeyHistory holds the failover and switchover history: a JSON object of
// timestamp → event, written by the leader.
const KeyHistory = "history"

// KeyPassword holds the cluster's Postgres password — the superuser's and
// tembo_repl's — as last rotated through PUT /password. Absent until the
// first rotation, and never handed back by any GET.
const KeyPassword = "password"

// Synchronous replication modes of Settings.SynchronousMode.
const (
	SyncOff    = "off"
	SyncQuorum = "quorum"
	SyncStrict = "strict"
)

// Settings is the behavior configuration of the agents, kept in the cluster
// under KeySettings with the values normalized by PATCH /settings.
type Settings struct {
	// MaxLagOnFailover is the most WAL a standby may be missing and still be
	// promoted; 0 lifts the limit.
	MaxLagOnFailover uint64 `json:"max-lag-on-failover"`
	// SynchronousMode is "off", "quorum" (ANY) or "strict" (FIRST).
	SynchronousMode string `json:"synchronous_mode"`
	// SynchronousNodeCount is how many standbys a commit waits for.
	SynchronousNodeCount uint64 `json:"synchronous_node_count"`
	// Paused suspends automatic failover, for maintenance. An explicit
	// switchover or failover still runs.
	Paused bool `json:"paused"`
	// HBA is the list of extra pg_hba.conf lines, written verbatim after the
	// lines tembo manages; the addresses of the host lines also restrict
	// the agents' proxies and API. See settings set-hba.
	HBA []string `json:"hba"`
}

// defaultMaxLagOnFailover applies while the settings key is absent: a standby
// missing more WAL than this is not promoted.
const defaultMaxLagOnFailover = 1 << 20

// LoadSettings reads the behavior settings from st; missing and unreadable
// values keep their defaults: 1MB of lag, asynchronous replication.
func LoadSettings(st *store.Store) Settings {
	s := Settings{MaxLagOnFailover: defaultMaxLagOnFailover, SynchronousMode: SyncOff, SynchronousNodeCount: 1}
	raw, err := st.Get(KeySettings)
	if err != nil || json.Unmarshal([]byte(raw), &s) != nil {
		return s
	}
	if s.SynchronousNodeCount == 0 {
		s.SynchronousNodeCount = 1
	}
	return s
}

// Status is what GET /status returns.
type Status struct {
	ID       string   `json:"id"`
	State    string   `json:"state"`
	Term     uint64   `json:"term"`
	Leader   string   `json:"leader"`
	Applied  uint64   `json:"applied"`
	Primary  string   `json:"primary"`
	Postgres Postgres `json:"postgres"`
	// Members are the ids of the committed membership: both sets of a joint
	// one. A node no committed configuration names anymore has left the
	// cluster.
	Members []string `json:"members"`
}

// NewServer wires a raft node for id over HTTP to peers, applying committed
// entries to st. peers is the initial membership of a new cluster; with join
// the node is joining a running one: peers names it and at least one member,
// and its elections stay frozen until the cluster's configuration arrives.
// The raft log keeps about retentionLogSize bytes of applied entries; 0 keeps
// all of it.
func NewServer(id string, peers map[string]string, st *store.Store, retentionLogSize int64, join bool) (*Server, error) {
	applied, err := st.AppliedIndex()
	if err != nil {
		return nil, err
	}
	s := &Server{id: id, peers: maps.Clone(peers), store: st, scheme: "http"}
	s.transport = NewHTTPTransport(s.resolvePeer)
	node, err := raft.New(raft.Config{
		ID:        id,
		Peers:     peers,
		Frozen:    join,
		Transport: s.transport,
		Storage:   st.RaftStorage(),
		Applied:   applied,

		RetentionLogSize: retentionLogSize,
		// A node that lost what it promised must not take part any longer.
		OnStorageError: func(err error) { log.Fatal().Err(err).Msg("raft: storage") },
		Apply:          st.Apply,
		// Runs with the raft node locked: it must not call back into it.
		OnMembershipChange: s.setPeers,
	})
	if err != nil {
		return nil, err
	}
	s.node = node
	return s, nil
}

// UseTLS switches the outbound traffic to the peers — raft RPCs, status
// polls and the redirect to the leader — to HTTPS; cfg verifies the peers'
// servers (its RootCAs; nodes carry no client certificates). Call before
// Start.
func (s *Server) UseTLS(cfg *tls.Config) {
	s.scheme = "https"
	s.tlsCfg = cfg
	s.transport.UseTLS(cfg)
}

// Peers returns the addresses of the current membership, this node included.
func (s *Server) Peers() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.peers)
}

// resolvePeer is the transport's view of the membership.
func (s *Server) resolvePeer(id string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	addr, ok := s.peers[id]
	return addr, ok
}

// isMember reports whether id is in the current membership, this node included.
func (s *Server) isMember(id string) bool {
	_, ok := s.resolvePeer(id)
	return ok
}

// setPeers follows the membership the raft node switched to; both sets of a
// joint one count. Called by the raft node while it is locked.
func (s *Server) setPeers(m raft.Membership) {
	peers := make(map[string]string, len(m.Voters)+len(m.Joint))
	maps.Copy(peers, m.Voters)
	maps.Copy(peers, m.Joint)
	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()
	if s.OnPeersChange != nil {
		s.OnPeersChange(peers)
	}
}

// Removed reports whether this node has left the cluster for good: its raft
// node committed a configuration without it, or a quorum of the membership
// it polls reported one. A removed node's agent stops Postgres and exits.
func (s *Server) Removed() bool {
	if s.node.Removed() {
		return true
	}
	if s.node.Frozen() {
		return false // a node that never was a member waits for its configuration
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	reports := 0
	for _, members := range s.reported {
		if !slices.Contains(members, s.id) {
			reports++
		}
	}
	return reports >= len(s.peers)/2+1
}

// Start begins participating in the cluster. Serve Handler() first so peers
// can reach this node.
func (s *Server) Start() { s.node.Start() }

// Stop leaves the cluster.
func (s *Server) Stop() { s.node.Stop() }

// WaitPrimary returns the id of the primary node, waiting until there is one.
// In a new cluster the raft leader chooses itself.
func (s *Server) WaitPrimary(ctx context.Context) (string, error) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		id, err := s.store.Get(KeyPrimary)
		if !errors.Is(err, store.ErrNotFound) {
			return id, err
		}
		if s.node.IsLeader() {
			// "add" keeps a primary chosen by a previous leader whose entry
			// is not applied here yet. Propose returns once the entry is
			// applied locally, so the next Get sees the winner.
			err = s.apply(ctx, store.Command{Op: "add", Key: KeyPrimary, Value: s.id})
			if err == nil {
				continue
			}
			log.Warn().Err(err).Msg("choose primary")
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-t.C:
		}
	}
}

// Handler serves peers (/raft/*) and the CLI (/status, /switchover, /members).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	RegisterRaftHandlers(mux, s.node)
	mux.HandleFunc("GET /status", s.status)
	mux.HandleFunc("PUT /switchover/{id}", s.putSwitchover)
	mux.HandleFunc("POST /failover/{id}", s.postFailover)
	mux.HandleFunc("GET /members", s.getMembers)
	mux.HandleFunc("PUT /members/{id}", s.putMember)
	mux.HandleFunc("DELETE /members/{id}", s.deleteMember)
	return mux
}

// Propose runs cmd through raft and answers r. A follower answers 307 to the
// leader, which keeps the method and body, so clients that follow redirects
// just work.
func (s *Server) Propose(w http.ResponseWriter, r *http.Request, cmd store.Command) {
	data, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	err = s.node.Propose(ctx, data)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, raft.ErrNotLeader):
		s.redirectToLeader(w, r)
	case errors.Is(err, raft.ErrLostLeadership), errors.Is(err, context.DeadlineExceeded):
		// Outcome unknown: the client decides whether to retry.
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// redirectToLeader answers 307 to the leader, which keeps the method and body,
// or 503 while there is no leader.
func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := s.node.Leader()
	if addr, ok := s.resolvePeer(leader); ok && leader != s.id {
		http.Redirect(w, r, s.scheme+"://"+addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
		return
	}
	http.Error(w, "no leader yet, retry", http.StatusServiceUnavailable)
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	applied, err := s.store.AppliedIndex()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	primary, err := s.store.Get(KeyPrimary)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.mu.Lock()
	pg := s.postgres
	s.mu.Unlock()
	writeJSON(w, Status{
		ID:       s.id,
		State:    s.node.State().String(),
		Term:     s.node.Term(),
		Leader:   s.node.Leader(),
		Applied:  applied,
		Primary:  primary,
		Postgres: pg,
		Members:  memberIDs(s.node.CommittedMembership()),
	})
}

// memberIDs returns the sorted ids of every voter of m, both sets of a joint
// one.
func memberIDs(m raft.Membership) []string {
	ids := make(map[string]bool, len(m.Voters)+len(m.Joint))
	for id := range m.Voters {
		ids[id] = true
	}
	for id := range m.Joint {
		ids[id] = true
	}
	return slices.Sorted(maps.Keys(ids))
}
