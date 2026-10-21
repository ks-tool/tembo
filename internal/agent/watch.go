package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"sync"
	"time"

	"github.com/ks-tool/tembo/internal/store"

	"github.com/rs/zerolog/log"
)

// Failure detection timings. A primary that has not seen a quorum for
// fenceTimeout stops its Postgres. The leader replaces a primary only after
// its Postgres has looked dead for failoverTimeout, which is longer, so an
// isolated primary has already stopped by then.
const (
	pollInterval    = time.Second
	fenceTimeout    = 5 * time.Second
	failoverTimeout = 15 * time.Second
)

// Postgres is the state of a node's Postgres as its agent last saw it.
type Postgres struct {
	Running    bool   `json:"running"`
	InRecovery bool   `json:"in_recovery"`
	LSN        uint64 `json:"lsn"`
	// ShutdownLSN is, for a server that was stopped cleanly, the position of
	// its shutdown checkpoint: all WAL that carries data lies before it.
	ShutdownLSN uint64 `json:"shutdown_lsn,omitempty"`
	// SyncStandbys are the standbys a primary commits synchronously with.
	SyncStandbys []string `json:"sync_standbys,omitempty"`
	// NoFailover excludes the node from failover and switchover targets,
	// NoSync from synchronous standbys; FailoverPriority breaks ties in
	// favour of the higher value. All three are local flags of the node.
	NoFailover       bool `json:"nofailover,omitempty"`
	NoSync           bool `json:"nosync,omitempty"`
	FailoverPriority int  `json:"failover_priority,omitempty"`
	// Limits are the settings a standby must keep at least as high as its primary.
	Limits map[string]int64 `json:"limits,omitempty"`
	// Config identifies the configuration the node runs with; ConfigError
	// explains why the cluster's configuration could not be applied.
	Config      string `json:"config"`
	ConfigError string `json:"config_error,omitempty"`
}

// Nodes returns the Postgres state of every node that answered the last poll,
// this one included.
func (s *Server) Nodes() map[string]Postgres {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.nodes)
}

// SetPostgres records the state of the local Postgres.
func (s *Server) SetPostgres(p Postgres) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.postgres = p
}

// HasQuorum reports whether, within fenceTimeout, this node knew a raft leader
// and reached a majority of the nodes.
func (s *Server) HasQuorum() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.quorumAt) < fenceTimeout
}

// Watch polls the status of every node until ctx is done. It keeps HasQuorum
// up to date and, on the leader, carries out a requested switchover or
// replaces a primary whose Postgres is dead.
func (s *Server) Watch(ctx context.Context) {
	client := &http.Client{Timeout: pollInterval / 2}
	if s.tlsCfg != nil {
		client.Transport = &http.Transport{TLSClientConfig: s.tlsCfg}
	}
	seen := make(map[string]time.Time)  // when each node's Postgres was last running
	lsn := make(map[string]uint64)      // its WAL position then
	syncOf := make(map[string][]string) // and the sync standbys a primary reported
	var leaderSince, switchoverSince time.Time
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		peers := s.Peers()
		pg, reported := s.poll(ctx, client, peers)
		for id, p := range pg {
			if p.Running {
				seen[id], lsn[id], syncOf[id] = now, p.LSN, p.SyncStandbys
			}
		}
		quorum := s.node.Leader() != "" && len(pg) > len(peers)/2
		s.mu.Lock()
		s.nodes = pg
		s.reported = reported
		if quorum {
			s.quorumAt = now
		}
		s.mu.Unlock()

		if !s.node.IsLeader() {
			leaderSince, switchoverSince = time.Time{}, time.Time{}
			continue
		}
		if leaderSince.IsZero() {
			// What the previous leader saw is lost: watch for a full timeout.
			leaderSince = now
		}
		// A leader cut off from the majority does not know it lost leadership:
		// its proposals would stay in its log and could commit much later.
		if !s.HasQuorum() {
			continue
		}
		// A new leader's store lacks what the previous leader committed until
		// its own first entry is applied.
		if !s.node.Ready() {
			continue
		}
		primary, err := s.store.Get(KeyPrimary)
		if err != nil {
			continue
		}
		if target, err := s.store.Get(KeySwitchover); err == nil {
			// A scheduled switchover waits for its time.
			if at, err := s.store.Get(KeySwitchoverAt); err == nil {
				if when, err := time.Parse(time.RFC3339, at); err == nil && now.Before(when) {
					continue
				}
			}
			if switchoverSince.IsZero() {
				switchoverSince = now
			}
			s.switchover(ctx, primary, target, pg, now.Sub(switchoverSince))
			continue
		}
		switchoverSince = time.Time{}
		settings := LoadSettings(s.store)
		if settings.Paused {
			continue // maintenance: no automatic failover
		}
		if now.Sub(leaderSince) < failoverTimeout || now.Sub(seen[primary]) < failoverTimeout {
			continue
		}
		next := pickStandby(pg, primary, lsn[primary], settings.MaxLagOnFailover, syncOf[primary])
		if next == "" {
			if pickStandby(pg, primary, 0, 0, nil) == "" {
				log.Error().Str("primary", primary).Msg("failover: primary is down and no standby is running")
			} else {
				log.Error().Str("primary", primary).Uint64("lsn", lsn[primary]).Uint64("max_lag", settings.MaxLagOnFailover).
					Msg("failover: primary is down and every standby lags too much")
			}
			continue
		}
		log.Warn().Str("primary", primary).Str("promote", next).Uint64("lsn", pg[next].LSN).Msg("failover: primary is down")
		if err = s.apply(ctx, store.Command{Op: "set", Key: KeyPrimary, Value: next}); err != nil {
			log.Error().Err(err).Msg("failover")
			continue
		}
		s.recordEvent(ctx, fmt.Sprintf("failover: %s → %s", primary, next))
	}
}

// poll returns the Postgres state of every node that answered, this one
// included, and the committed membership each peer reported.
func (s *Server) poll(ctx context.Context, client *http.Client, peers map[string]string) (map[string]Postgres, map[string][]string) {
	s.mu.Lock()
	pg := map[string]Postgres{s.id: s.postgres}
	s.mu.Unlock()
	reported := make(map[string][]string)

	var mu sync.Mutex
	var wg sync.WaitGroup
	for id, addr := range peers {
		if id == s.id {
			continue
		}
		wg.Go(func() {
			st, err := fetchStatus(ctx, client, s.scheme, addr)
			if err != nil {
				return
			}
			mu.Lock()
			pg[id] = st.Postgres
			reported[id] = st.Members
			mu.Unlock()
		})
	}
	wg.Wait()
	return pg, reported
}

// pickStandby returns the running standby with the most WAL that lags the
// primary's last known position by at most maxLag (0 lifts the limit), or ""
// if there is none. A node tagged nofailover is never picked. Candidates are
// ordered by failover priority, then by having been a synchronous standby of
// the primary (it misses no committed data), then by WAL position.
func pickStandby(pg map[string]Postgres, primary string, primaryLSN, maxLag uint64, prefer []string) string {
	pref := make(map[string]bool, len(prefer))
	for _, id := range prefer {
		pref[id] = true
	}
	better := func(id, other string) bool {
		p, q := pg[id], pg[other]
		if p.FailoverPriority != q.FailoverPriority {
			return p.FailoverPriority > q.FailoverPriority
		}
		if pref[id] != pref[other] {
			return pref[id]
		}
		return p.LSN > q.LSN
	}
	var best string
	for id, p := range pg {
		if id == primary || !p.Running || !p.InRecovery || p.NoFailover {
			continue
		}
		if maxLag > 0 && lagsMoreThan(p.LSN, primaryLSN, maxLag) {
			continue
		}
		if best == "" || better(id, best) {
			best = id
		}
	}
	return best
}

// lagsMoreThan reports whether a is behind b by more than d. A standby not
// behind — it may run ahead of the primary's last report — lags nothing.
// The difference is taken rather than a sum added: near the end of the
// uint64 range p.LSN+d would wrap and read as lagging.
func lagsMoreThan(a, b, d uint64) bool {
	if a >= b {
		return false
	}
	return b-a > d
}

func fetchStatus(ctx context.Context, client *http.Client, scheme, addr string) (Status, error) {
	var st Status
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+addr+"/status", nil)
	if err != nil {
		return st, err
	}
	res, err := client.Do(req)
	if err != nil {
		return st, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return st, fmt.Errorf("%s: %s", addr, res.Status)
	}
	return st, json.NewDecoder(res.Body).Decode(&st)
}

// apply replicates cmd through raft; it returns once cmd is applied locally.
func (s *Server) apply(ctx context.Context, cmd store.Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.node.Propose(ctx, data)
}

// IsLeader reports whether this node is the raft leader.
func (s *Server) IsLeader() bool { return s.node.IsLeader() }

// Apply replicates cmd through raft, for the node's own loops rather than an
// HTTP request; it returns once cmd is applied locally.
func (s *Server) Apply(ctx context.Context, cmd store.Command) error { return s.apply(ctx, cmd) }

// recordEvent adds event to the cluster's history; a failure only logs.
func (s *Server) recordEvent(ctx context.Context, event string) {
	at := time.Now().UTC().Format(time.RFC3339Nano)
	value, err := json.Marshal(map[string]string{at: event})
	if err != nil {
		return
	}
	if err := s.apply(ctx, store.Command{Op: "merge", Key: KeyHistory, Value: string(value)}); err != nil {
		log.Warn().Err(err).Str("event", event).Msg("history")
	}
}
