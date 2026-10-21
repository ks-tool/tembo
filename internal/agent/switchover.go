package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ks-tool/tembo/internal/store"

	"github.com/rs/zerolog/log"
)

// KeySwitchover holds the node the primary role is being moved to.
const KeySwitchover = "switchover"

// KeySwitchoverAt holds, for a scheduled switchover, when it runs (RFC3339).
const KeySwitchoverAt = "switchover_at"

// switchoverTimeout bounds how long the primary may take to stop and the
// target to receive its WAL.
const switchoverTimeout = time.Minute

// putSwitchover asks to move the primary role to node id and waits until the
// leader completes or abandons the switchover, see Watch. With an RFC3339
// timestamp as the body the switchover is scheduled instead, and the handler
// answers at once.
func (s *Server) putSwitchover(w http.ResponseWriter, r *http.Request) {
	if !s.node.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}
	if !s.node.Ready() {
		http.Error(w, "the new leader is catching up, retry", http.StatusServiceUnavailable)
		return
	}
	target := r.PathValue("id")
	primary, err := s.store.Get(KeyPrimary)
	pending, pendingErr := s.store.Get(KeySwitchover)
	nodes := s.Nodes()
	_, primaryReachable := nodes[primary]
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, "no primary yet", http.StatusServiceUnavailable)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	case pendingErr != nil && !errors.Is(pendingErr, store.ErrNotFound):
		// a failing store must not read as "no switchover pending"
		http.Error(w, pendingErr.Error(), http.StatusInternalServerError)
	case pendingErr == nil:
		http.Error(w, fmt.Sprintf("a switchover to %s is already in progress", pending), http.StatusConflict)
	case !s.isMember(target):
		http.Error(w, fmt.Sprintf("unknown node %q", target), http.StatusNotFound)
	case target == primary:
		http.Error(w, fmt.Sprintf("%s is the primary already", target), http.StatusConflict)
	case !s.HasQuorum():
		http.Error(w, "no quorum", http.StatusServiceUnavailable)
	case nodes[target].NoFailover:
		http.Error(w, fmt.Sprintf("%s is tagged nofailover", target), http.StatusConflict)
	case !nodes[target].Running || !nodes[target].InRecovery:
		http.Error(w, fmt.Sprintf("%s is not a running standby", target), http.StatusConflict)
	case !primaryReachable:
		http.Error(w, fmt.Sprintf("primary %s is unreachable, failover will replace it", primary), http.StatusConflict)
	default:
		at, _ := io.ReadAll(io.LimitReader(r.Body, 64))
		if len(at) > 0 {
			s.scheduleSwitchover(w, r, target, strings.TrimSpace(string(at)))
			return
		}
		err = s.apply(r.Context(), store.Command{Op: "set", Key: KeySwitchover, Value: target})
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		s.waitSwitchover(w, r, target)
	}
}

// scheduleSwitchover stores a switchover request that Watch picks up once its
// time comes.
func (s *Server) scheduleSwitchover(w http.ResponseWriter, r *http.Request, target, at string) {
	when, err := time.Parse(time.RFC3339, at)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad time %q, want RFC3339", at), http.StatusBadRequest)
		return
	}
	// The time goes first: Watch keys on the target alone.
	if err = s.apply(r.Context(), store.Command{Op: "set", Key: KeySwitchoverAt, Value: when.Format(time.RFC3339)}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if err = s.apply(r.Context(), store.Command{Op: "set", Key: KeySwitchover, Value: target}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	log.Warn().Str("target", target).Time("at", when).Msg("switchover scheduled")
	w.WriteHeader(http.StatusNoContent)
}

// postFailover moves the primary role to a standby while the primary is down:
// unlike a switchover it does not wait for the old primary, so it is the tool
// for when switchover refuses — and the way to fail over while paused. A
// primary that looks alive makes it a conflict.
func (s *Server) postFailover(w http.ResponseWriter, r *http.Request) {
	if !s.node.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}
	if !s.node.Ready() {
		http.Error(w, "the new leader is catching up, retry", http.StatusServiceUnavailable)
		return
	}
	target := r.PathValue("id")
	primary, err := s.store.Get(KeyPrimary)
	pending, pendingErr := s.store.Get(KeySwitchover)
	nodes := s.Nodes()
	p, primaryKnown := nodes[primary]
	t := nodes[target]
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, "no primary yet", http.StatusServiceUnavailable)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	case pendingErr != nil && !errors.Is(pendingErr, store.ErrNotFound):
		// a failing store must not read as "no switchover pending"
		http.Error(w, pendingErr.Error(), http.StatusInternalServerError)
	case pendingErr == nil:
		http.Error(w, fmt.Sprintf("a switchover to %s is already in progress", pending), http.StatusConflict)
	case !s.isMember(target):
		http.Error(w, fmt.Sprintf("unknown node %q", target), http.StatusNotFound)
	case target == primary:
		http.Error(w, fmt.Sprintf("%s is the primary already", target), http.StatusConflict)
	case !s.HasQuorum():
		http.Error(w, "no quorum", http.StatusServiceUnavailable)
	case primaryKnown && p.Running && !p.InRecovery:
		http.Error(w, fmt.Sprintf("primary %s is alive, use switchover", primary), http.StatusConflict)
	case t.NoFailover:
		http.Error(w, fmt.Sprintf("%s is tagged nofailover", target), http.StatusConflict)
	case !t.Running || !t.InRecovery:
		http.Error(w, fmt.Sprintf("%s is not a running standby", target), http.StatusConflict)
	default:
		if err := s.apply(r.Context(), store.Command{Op: "set", Key: KeyPrimary, Value: target}); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		log.Warn().Str("primary", primary).Str("promote", target).Msg("failover: requested")
		s.recordEvent(r.Context(), fmt.Sprintf("failover: %s → %s (requested)", primary, target))
		w.WriteHeader(http.StatusNoContent)
	}
}

// waitSwitchover answers once the switchover request is gone from the store.
func (s *Server) waitSwitchover(w http.ResponseWriter, r *http.Request, target string) {
	ctx, cancel := context.WithTimeout(r.Context(), switchoverTimeout+10*time.Second)
	defer cancel()
	t := time.NewTicker(pollInterval / 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			http.Error(w, "switchover is still in progress", http.StatusGatewayTimeout)
			return
		case <-t.C:
		}
		_, err := s.store.Get(KeySwitchover)
		if err == nil {
			continue // still in progress
		}
		if !errors.Is(err, store.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		primary, err := s.store.Get(KeyPrimary)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err != nil || primary != target {
			http.Error(w, fmt.Sprintf("switchover to %s was abandoned, see the log of the raft leader", target), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]string{KeyPrimary: target})
		return
	}
}

// switchover moves the primary role to target once the primary has stopped.
// A primary stopped cleanly has sent all its WAL to the connected standbys,
// and reports the position of its shutdown checkpoint: a target that has
// received WAL up to it has every transaction the primary committed. A primary
// that cannot be asked, e.g. because its agent is down, is waited for until
// the timeout. The request is dropped when done, when the target is no running
// standby, or after switchoverTimeout.
func (s *Server) switchover(ctx context.Context, primary, target string, pg map[string]Postgres, pending time.Duration) {
	p, primaryReachable := pg[primary]
	t, targetReachable := pg[target]
	stopped := primaryReachable && !p.Running && p.ShutdownLSN > 0
	ready := stopped && targetReachable && t.LSN >= p.ShutdownLSN

	var event string // empty: the request stays, the next tick retries
	switch {
	case target == primary:
		// A previous leader moved the primary but did not drop the request.
	case targetReachable && (!t.Running || !t.InRecovery):
		event = fmt.Sprintf("switchover: %s → %s abandoned: target is not a running standby", primary, target)
		log.Error().Str("target", target).Msg("switchover: target is not a running standby, giving up")
	case !ready && pending < switchoverTimeout:
		return // the primary is stopping or the target is receiving its last WAL
	case !ready:
		event = fmt.Sprintf("switchover: %s → %s abandoned: timed out", primary, target)
		log.Error().Str("primary", primary).Str("target", target).Bool("primary_stopped", stopped).
			Uint64("primary_shutdown_lsn", p.ShutdownLSN).Uint64("target_lsn", t.LSN).Msg("switchover: timed out, giving up")
	default:
		log.Warn().Str("primary", primary).Str("target", target).Uint64("lsn", t.LSN).Msg("switchover: primary stopped, promoting target")
		if err := s.apply(ctx, store.Command{Op: "set", Key: KeyPrimary, Value: target}); err != nil {
			log.Error().Err(err).Msg("switchover")
			return
		}
		event = fmt.Sprintf("switchover: %s → %s", primary, target)
	}
	for _, key := range []string{KeySwitchover, KeySwitchoverAt} {
		if err := s.apply(ctx, store.Command{Op: "del", Key: key}); err != nil {
			log.Error().Err(err).Msg("switchover")
		}
	}
	if event != "" {
		s.recordEvent(ctx, event)
	}
}
