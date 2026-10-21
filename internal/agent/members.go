package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ks-tool/tembo/internal/raft"
)

// membershipTimeout bounds a membership change: a joining node may first
// have to catch up on the whole log before the joint entry can commit.
const membershipTimeout = 30 * time.Second

// getMembers answers the membership in effect on this node, committed or
// not: id → address for the voting set, and the incoming set while a change
// is joint.
func (s *Server) getMembers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.node.Membership())
}

// putMember adds node id at the address given as the body, or updates the
// address of a member. Only the leader changes the membership; a follower
// redirects to it.
func (s *Server) putMember(w http.ResponseWriter, r *http.Request) {
	addr, err := io.ReadAll(io.LimitReader(r.Body, 1<<10))
	if err != nil || len(addr) == 0 {
		http.Error(w, "the request body is the node's host:port", http.StatusBadRequest)
		return
	}
	voters := s.node.Membership().Voters
	if voters == nil {
		voters = make(map[string]string)
	}
	voters[r.PathValue("id")] = string(addr)
	s.proposeMembership(w, r, voters)
}

// deleteMember removes node id from the membership. A removed node stops its
// Postgres and its agent exits, see Server.Removed.
func (s *Server) deleteMember(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	voters := s.node.Membership().Voters
	if _, ok := voters[id]; !ok {
		http.Error(w, fmt.Sprintf("unknown node %q", id), http.StatusNotFound)
		return
	}
	delete(voters, id)
	s.proposeMembership(w, r, voters)
}

// proposeMembership runs the change to voters through raft and answers r.
func (s *Server) proposeMembership(w http.ResponseWriter, r *http.Request, voters map[string]string) {
	ctx, cancel := context.WithTimeout(r.Context(), membershipTimeout)
	defer cancel()
	err := s.node.ProposeMembership(ctx, voters)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, raft.ErrNotLeader):
		s.redirectToLeader(w, r)
	case errors.Is(err, raft.ErrMembershipBusy):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, raft.ErrLostLeadership), errors.Is(err, context.DeadlineExceeded):
		// Outcome unknown: the client decides whether to retry.
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
