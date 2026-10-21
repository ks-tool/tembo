// Package raft is a minimal implementation of the Raft consensus algorithm:
// leader election, log replication and commit, with committed entries handed to
// the application through a single callback. Cluster membership changes go
// through the log as joint consensus, one change at a time
// (Node.ProposeMembership); pre-vote is left out, so a node joining a running
// cluster starts with its election timer frozen (Config.Frozen) until a
// configuration naming it arrives from the leader, and a node a committed
// configuration dropped never campaigns again.
//
// The term, the vote, the log and the membership are kept in memory and, with
// Config.Storage, also written there before they are acted on, so a node that
// restarts comes back with them. Without a Storage a restarted node is a new
// one. With Config.RetentionLogSize the applied part of the log is compacted:
// the Storage then also holds the state machine and the membership the
// compacted prefix left behind, and a node that needs entries compacted away
// is sent the state machine as a whole, with that membership.
//
// Wiring it into a project takes three things: a unique node ID, the initial
// membership, and a Transport that delivers three RPCs. Inbound RPCs are
// handed to Node.HandleRequestVote, Node.HandleAppendEntries and
// Node.HandleInstallSnapshot.
package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// State is the role a node currently plays.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

var (
	// ErrNotLeader is returned by Propose on a node that is not the leader.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrLostLeadership is returned by Propose when the node stopped being
	// leader before it observed the entry committing. The entry may or may
	// not commit later; the caller has to decide whether to retry.
	ErrLostLeadership = errors.New("raft: leadership lost before commit; outcome unknown")
	// ErrStopped is returned once Stop has been called.
	ErrStopped = errors.New("raft: node stopped")
	// ErrMembershipBusy is returned by ProposeMembership while an earlier
	// membership change has not settled; changes go one at a time.
	ErrMembershipBusy = errors.New("raft: a membership change is in progress")
)

// Membership is the cluster configuration: the voting nodes and, while a
// change to a new set is being committed (joint consensus), that new set.
// Both sets vote then; a committed joint entry is always followed by one
// naming the new set alone. The address a node ID maps to is opaque to raft;
// the transport layer resolves it.
type Membership struct {
	Voters map[string]string `json:"voters"`
	Joint  map[string]string `json:"joint,omitempty"`
}

// has reports whether id votes; members of both sets of a joint
// configuration do.
func (m Membership) has(id string) bool {
	if _, ok := m.Voters[id]; ok {
		return true
	}
	_, ok := m.Joint[id]
	return ok
}

// quorum reports whether grant names a majority of the voting set — of both
// sets while a change is joint.
func (m Membership) quorum(grant func(id string) bool) bool {
	majority := func(set map[string]string) bool {
		n := 0
		for id := range set {
			if grant(id) {
				n++
			}
		}
		return n > len(set)/2
	}
	return majority(m.Voters) && (m.Joint == nil || majority(m.Joint))
}

func (m Membership) clone() Membership {
	return Membership{Voters: maps.Clone(m.Voters), Joint: maps.Clone(m.Joint)}
}

// A configuration entry's data is a zero byte followed by the JSON
// membership; application data (JSON commands) never starts with it.
const configEntryPrefix = 0

func configEntryData(m Membership) []byte {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err) // maps of strings always marshal
	}
	return append([]byte{configEntryPrefix}, data...)
}

func decodeConfigEntry(data []byte) (Membership, bool) {
	if len(data) < 2 || data[0] != configEntryPrefix {
		return Membership{}, false
	}
	var m Membership
	if err := json.Unmarshal(data[1:], &m); err != nil || len(m.Voters) == 0 {
		return Membership{}, false
	}
	return m, true
}

func isConfigEntry(e Entry) bool {
	_, ok := decodeConfigEntry(e.Data)
	return ok
}

// Entry is one record of the replicated log.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// RequestVoteRequest is sent by candidates to gather votes.
type RequestVoteRequest struct {
	Term         uint64
	Candidate    string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse answers a RequestVoteRequest.
type RequestVoteResponse struct {
	Term    uint64
	Granted bool
}

// AppendEntriesRequest replicates log entries and doubles as a heartbeat.
type AppendEntriesRequest struct {
	Term         uint64
	Leader       string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesResponse answers an AppendEntriesRequest.
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	// MatchIndex is the highest index the follower confirms matching the
	// leader's log. On success it is the last replicated index; on failure it
	// is a hint that lets the leader skip back quickly (nextIndex = MatchIndex+1).
	MatchIndex uint64
}

// InstallSnapshotRequest hands a follower the state machine as of
// LastIncludedIndex, when the entries it needs are compacted away. Config is
// the membership in effect at LastIncludedIndex: the snapshot replaces the
// compacted prefix, which carried the configuration entries.
type InstallSnapshotRequest struct {
	Term              uint64
	Leader            string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Config            Membership
	Data              []byte
}

// InstallSnapshotResponse answers an InstallSnapshotRequest. Success is
// false when the node did not take the snapshot — it runs without a
// Storage to keep one — so the leader does not count as delivered what
// never arrived.
type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}

// Transport delivers RPCs to other nodes. Implementations must be safe for
// concurrent use. The context carries a deadline; an error means "no answer".
type Transport interface {
	RequestVote(ctx context.Context, to string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to string, req AppendEntriesRequest) (AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to string, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// Handler is the inbound side of the RPCs; *Node implements it. A Transport
// implementation calls these on the destination node when a request arrives.
type Handler interface {
	HandleRequestVote(RequestVoteRequest) RequestVoteResponse
	HandleAppendEntries(AppendEntriesRequest) AppendEntriesResponse
	HandleInstallSnapshot(InstallSnapshotRequest) InstallSnapshotResponse
}

// Stored is what Storage.Load returns.
type Stored struct {
	Term     uint64
	VotedFor string
	// Base is the last entry compacted away; zero until the first compaction.
	// Log holds the entries after it, from Base.Index+1 on. Config is the
	// membership in effect at Base, nil until a configuration entry was
	// compacted or a snapshot installed (the bootstrap set applies then).
	Base   Entry
	Config *Membership
	Log    []Entry
}

// Storage keeps what a node must not forget across restarts: the term, the
// vote, the log and the membership the compacted prefix left behind. Each
// write is durable when it returns, and the node acts on a change only after
// that. The log starts at index 1.
//
// The state machine must live with the log, in storage that changes
// atomically: a node compacts entries only after the state machine has
// applied them, and installs a snapshot of it in the same step as it cuts
// the log. Without RetentionLogSize those methods are never called.
type Storage interface {
	// Load returns the stored state; all zero for a new node.
	Load() (Stored, error)
	// SaveState stores the term and the vote.
	SaveState(term uint64, votedFor string) error
	// SetLog removes the entries from index `from` on and stores entries,
	// whose first index is `from`, in their place.
	SetLog(from uint64, entries []Entry) error
	// Compact removes the entries up to base.Index, which the state machine
	// has applied, and remembers base as the entry the log now follows and
	// config as the membership in effect at it.
	Compact(base Entry, config Membership) error
	// Snapshot returns the state machine and the index of the last entry it
	// has applied, read together.
	Snapshot() (data []byte, applied uint64, err error)
	// InstallSnapshot replaces the state machine with data, the state as of
	// base, and the membership with config, and in the same step makes the
	// log follow base: the entries after base are kept if keepLog is set,
	// otherwise the log is emptied.
	InstallSnapshot(base Entry, keepLog bool, config Membership, data []byte) error
}

// Config describes one node. Only ID and Transport are required.
type Config struct {
	// ID uniquely identifies this node in the cluster.
	ID string
	// Peers is the initial membership of a new cluster, every node's ID
	// mapped to the address the transport layer knows it by, this node
	// included. It applies while the storage holds no configuration: a
	// cluster forms from the same Peers on every node, and membership
	// changes (ProposeMembership) take over from there.
	Peers map[string]string
	// Frozen starts the node with elections off: it follows and replicates
	// but never campaigns, until a configuration naming it arrives from a
	// leader (or is already in its storage). A node joining a running
	// cluster starts frozen; without pre-vote its isolated campaigns would
	// otherwise keep raising its term and disrupt the cluster.
	Frozen bool
	// Transport delivers outbound RPCs.
	Transport Transport
	// Storage persists the node's state. Nil keeps it in memory only.
	Storage Storage
	// Applied is the index of the last entry the state machine has applied
	// before this start; Apply gets the entries after it. It lies within the
	// stored log, from the entry it was compacted to on.
	Applied uint64
	// RetentionLogSize is how many bytes of applied entries the log keeps;
	// older ones are compacted away, once the surplus is a quarter of it. Zero
	// keeps the whole log. It needs a Storage.
	RetentionLogSize int64
	// OnStorageError is called when Storage fails. The node cannot go on
	// without having stored what it promised, so the default is to panic.
	OnStorageError func(error)
	// Apply receives committed entries, in log order, exactly once per node
	// lifetime. It runs on a dedicated goroutine; it must not block on Propose.
	// Entries without data are no-ops, such as the one a new leader appends to
	// commit the entries of earlier terms: the state machine only counts them
	// as applied. Configuration entries are applied by the node itself and
	// never reach Apply. May be nil.
	Apply func(Entry)
	// OnMembershipChange is called when the membership in effect changes: a
	// configuration takes effect when its entry enters the log, and leaves
	// when the entry is dropped from it. It runs with the node locked and
	// must not call back into the node. May be nil.
	OnMembershipChange func(Membership)
	// ElectionTimeout is the base follower timeout; the actual timeout is
	// randomized in [ElectionTimeout, 2*ElectionTimeout). Default 600ms.
	ElectionTimeout time.Duration
	// HeartbeatInterval is how often the leader contacts followers. It also
	// sets the tick resolution of the node. Default 50ms.
	HeartbeatInterval time.Duration
}

type waiter struct {
	term uint64
	ch   chan error
}

// departedNode is a node a committed configuration dropped; the leader keeps
// it as a replication target until it has the entry and its commit, so it
// learns on its own that it left the cluster.
type departedNode struct {
	index uint64    // the committed configuration entry that dropped the node
	since time.Time // after a grace the node is on its own
}

// Node is one member of a Raft cluster.
type Node struct {
	cfg Config

	mu          sync.Mutex
	stopped     bool
	state       State
	term        uint64
	votedFor    string
	log         []Entry // log[0] is the entry before the first one, see base; the rest follow it
	commitIndex uint64
	lastApplied uint64
	leader      string
	electionDue time.Time
	votes       map[string]bool
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64 // the leader's own stored entries count too, at cfg.ID
	waiters     map[uint64]waiter
	sending     map[string]bool // peers a snapshot is on its way to
	noopIndex   uint64          // the entry the current leader appended on election, see Ready

	// members is the newest configuration the log carries, committed or not:
	// it is in effect from the moment its entry enters the log, and confIndex
	// is that entry's index (the base's, when the compacted prefix carries
	// it). committedMembers is the newest committed one; baseMembers the one
	// the compacted prefix left behind.
	members          Membership
	committedMembers Membership
	baseMembers      Membership
	confIndex        uint64
	frozen           bool // elections stay off until a configuration names this node
	departed         map[string]departedNode

	ctx       context.Context
	cancel    context.CancelFunc
	applyCh   chan struct{}
	wg        sync.WaitGroup
	startOnce sync.Once
	stopOnce  sync.Once
}

// New validates cfg, fills in defaults and returns a node that is not yet
// running. Register it with the transport, then call Start.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: Config.ID is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("raft: Config.Transport is required")
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 600 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.OnStorageError == nil {
		cfg.OnStorageError = func(err error) { panic("raft: storage: " + err.Error()) }
	}
	if cfg.HeartbeatInterval >= cfg.ElectionTimeout {
		return nil, errors.New("raft: HeartbeatInterval must be less than ElectionTimeout")
	}
	if cfg.RetentionLogSize > 0 && cfg.Storage == nil {
		return nil, errors.New("raft: RetentionLogSize needs a Storage")
	}
	bootstrap := Membership{Voters: make(map[string]string, len(cfg.Peers)+1)}
	for id, addr := range cfg.Peers {
		if id != "" {
			bootstrap.Voters[id] = addr
		}
	}
	bootstrap.Voters[cfg.ID] = cfg.Peers[cfg.ID]
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:              cfg,
		members:          bootstrap,
		committedMembers: bootstrap,
		baseMembers:      bootstrap,
		log:              []Entry{{}},
		waiters:          make(map[uint64]waiter),
		sending:          make(map[string]bool),
		departed:         make(map[string]departedNode),
		ctx:              ctx,
		cancel:           cancel,
		applyCh:          make(chan struct{}, 1),
	}
	storedConfig := false // the storage holds a configuration from a leader
	if cfg.Storage != nil {
		st, err := cfg.Storage.Load()
		if err != nil {
			cancel()
			return nil, fmt.Errorf("raft: load: %w", err)
		}
		for i, e := range st.Log {
			if e.Index != st.Base.Index+uint64(i)+1 {
				cancel()
				return nil, fmt.Errorf("raft: stored log has index %d after %d", e.Index, st.Base.Index+uint64(i))
			}
			storedConfig = storedConfig || isConfigEntry(e)
		}
		n.term, n.votedFor = st.Term, st.VotedFor
		n.log = append([]Entry{{Index: st.Base.Index, Term: st.Base.Term}}, st.Log...)
		if st.Config != nil {
			n.baseMembers = *st.Config
			storedConfig = true
		}
	}
	n.members, n.confIndex = n.configAsOf(n.lastIndex())
	if cfg.Applied > n.lastIndex() || cfg.Applied < n.base() {
		cancel()
		return nil, fmt.Errorf("raft: Config.Applied %d is outside the stored log, which covers %d to %d", cfg.Applied, n.base(), n.lastIndex())
	}
	n.commitIndex, n.lastApplied = cfg.Applied, cfg.Applied // applied entries were committed
	n.committedMembers, _ = n.configAsOf(cfg.Applied)
	// A node that joins a running cluster stays out of elections until a
	// configuration from a leader — not the bootstrap set it was started
	// with — names it.
	n.frozen = cfg.Frozen && !(storedConfig && n.members.has(cfg.ID))
	n.resetElectionTimer(time.Now())
	return n, nil
}

// Start launches the node's goroutines. Calling it twice is a no-op.
func (n *Node) Start() {
	n.startOnce.Do(func() {
		n.wg.Add(2)
		go n.run()
		go n.applier()
	})
}

// Stop shuts the node down and waits for its goroutines: the ticker, the
// applier and a snapshot in flight. The RequestVote and AppendEntries RPCs
// it had under way are not waited for — they notice the stop on their own
// and touch nothing but the node's locked state — and may briefly outlive
// Stop. Pending Propose calls return ErrStopped. Calling it twice is a
// no-op.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		n.stopped = true
		n.failWaiters(ErrStopped)
		n.mu.Unlock()
		n.cancel()
		n.wg.Wait()
	})
}

// Propose appends data to the log and blocks until the entry has been
// committed and applied on this node, or ctx expires. Only the leader accepts
// proposals; followers return ErrNotLeader and the caller should retry against
// Leader().
func (n *Node) Propose(ctx context.Context, data []byte) error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	idx := n.lastIndex() + 1
	n.log = append(n.log, Entry{Index: idx, Term: n.term, Data: append([]byte(nil), data...)})
	ch := make(chan error, 1)
	n.waiters[idx] = waiter{term: n.term, ch: ch}
	n.saveLog(idx)
	n.matchIndex[n.cfg.ID] = idx // the leader counts for the quorum only with the entry stored
	n.advanceCommit()            // a single-node cluster commits immediately
	n.broadcastAppend()
	n.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, idx)
		n.mu.Unlock()
		return ctx.Err()
	case <-n.ctx.Done():
		return ErrStopped
	}
}

// ProposeMembership moves the cluster to the voting set voters (node ID →
// address, as the transport knows it) by joint consensus: an entry naming
// both the current and the new set commits first, and the leader then
// appends one naming the new set alone. It returns once the joint entry is
// committed and applied here; from then on no configuration without the new
// set can commit, and the new set settles alone a roundtrip later. Only one
// change is in flight at a time: while the current configuration is joint or
// not yet committed, ProposeMembership returns ErrMembershipBusy.
func (n *Node) ProposeMembership(ctx context.Context, voters map[string]string) error {
	n.mu.Lock()
	switch {
	case n.stopped:
		n.mu.Unlock()
		return ErrStopped
	case n.state != Leader:
		n.mu.Unlock()
		return ErrNotLeader
	case n.members.Joint != nil || n.confIndex > n.commitIndex:
		n.mu.Unlock()
		return ErrMembershipBusy
	case len(voters) == 0:
		n.mu.Unlock()
		return errors.New("raft: an empty membership")
	case maps.Equal(n.members.Voters, voters):
		n.mu.Unlock()
		return nil
	}
	m := Membership{Voters: n.members.Voters, Joint: maps.Clone(voters)}
	idx := n.lastIndex() + 1
	n.log = append(n.log, Entry{Index: idx, Term: n.term, Data: configEntryData(m)})
	ch := make(chan error, 1)
	n.waiters[idx] = waiter{term: n.term, ch: ch}
	n.saveLog(idx)
	n.matchIndex[n.cfg.ID] = idx
	n.setMembership(m, idx)
	n.advanceCommit()
	n.broadcastAppend()
	n.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, idx)
		n.mu.Unlock()
		return ctx.Err()
	case <-n.ctx.Done():
		return ErrStopped
	}
}

// Membership returns the newest configuration in effect, committed or not.
func (n *Node) Membership() Membership {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.members.clone()
}

// CommittedMembership returns the newest committed configuration.
func (n *Node) CommittedMembership() Membership {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.committedMembers.clone()
}

// Removed reports whether a committed configuration no longer names this
// node: it has left the cluster for good and will never campaign again.
func (n *Node) Removed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.committedMembers.has(n.cfg.ID)
}

// Frozen reports whether the node's elections are still off because no
// configuration from a leader has named it yet, see Config.Frozen.
func (n *Node) Frozen() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.frozen
}

// State reports the node's current role.
func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// IsLeader reports whether the node currently believes it is the leader.
func (n *Node) IsLeader() bool { return n.State() == Leader }

// Ready reports whether the node is the leader and has applied everything
// committed before its election. Until then its state machine may lack entries
// the previous leader committed, since the new leader learns they are
// committed only through an entry of its own term.
func (n *Node) Ready() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader && n.lastApplied >= n.noopIndex
}

// Leader returns the ID of the node believed to be leader, or "" if unknown.
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term
}

// Stats are the node's log positions, for metrics.
type Stats struct {
	CommitIndex uint64
	Applied     uint64
	LastIndex   uint64
}

// Stats returns the node's log positions.
func (n *Node) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Stats{CommitIndex: n.commitIndex, Applied: n.lastApplied, LastIndex: n.lastIndex()}
}

// HandleRequestVote processes an inbound RequestVote RPC.
func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || req.Term < n.term {
		return RequestVoteResponse{Term: n.term}
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, "")
	}
	upToDate := req.LastLogTerm > n.lastTerm() ||
		(req.LastLogTerm == n.lastTerm() && req.LastLogIndex >= n.lastIndex())
	if (n.votedFor == "" || n.votedFor == req.Candidate) && upToDate {
		n.votedFor = req.Candidate
		n.saveState()
		n.resetElectionTimer(time.Now())
		return RequestVoteResponse{Term: n.term, Granted: true}
	}
	return RequestVoteResponse{Term: n.term}
}

// HandleAppendEntries processes an inbound AppendEntries RPC.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || req.Term < n.term {
		return AppendEntriesResponse{Term: n.term}
	}
	if req.Term > n.term || n.state != Follower {
		n.becomeFollower(req.Term, req.Leader)
	}
	n.leader = req.Leader
	n.resetElectionTimer(time.Now())

	if req.PrevLogIndex > n.lastIndex() {
		return AppendEntriesResponse{Term: n.term, MatchIndex: n.lastIndex()}
	}
	// What lies in the compacted part is committed, so it matches.
	if req.PrevLogIndex >= n.base() && n.termAt(req.PrevLogIndex) != req.PrevLogTerm {
		return AppendEntriesResponse{Term: n.term, MatchIndex: req.PrevLogIndex - 1}
	}
	var cutFrom uint64
	var tail []Entry
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + uint64(i)
		if idx <= n.base() {
			continue
		}
		if idx <= n.lastIndex() {
			if n.termAt(idx) == e.Term {
				continue
			}
			if idx <= n.commitIndex {
				// A correct leader never rewrites committed entries; refuse
				// rather than corrupt the state machine.
				return AppendEntriesResponse{Term: n.term, MatchIndex: n.commitIndex}
			}
		}
		// From the first entry that differs on, the leader's entries replace
		// ours.
		cutFrom, tail = idx, req.Entries[i:]
		n.log = append(n.log[:idx-n.base()], tail...)
		n.saveLog(idx)
		break
	}
	// A configuration is in effect from the moment its entry enters the log
	// — a joint quorum is counted in both sets already — and stops being in
	// effect when the entry is dropped.
	if tail != nil && (n.confIndex >= cutFrom || slices.ContainsFunc(tail, isConfigEntry)) {
		m, at := n.configAsOf(n.lastIndex())
		n.setMembership(m, at)
	}
	lastNew := req.PrevLogIndex + uint64(len(req.Entries))
	if req.LeaderCommit > n.commitIndex {
		n.setCommit(min(req.LeaderCommit, lastNew))
	}
	return AppendEntriesResponse{Term: n.term, Success: true, MatchIndex: lastNew}
}

// HandleInstallSnapshot processes an inbound InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || req.Term < n.term {
		return InstallSnapshotResponse{Term: n.term}
	}
	if req.Term > n.term || n.state != Follower {
		n.becomeFollower(req.Term, req.Leader)
	}
	n.leader = req.Leader
	n.resetElectionTimer(time.Now())

	idx := req.LastIncludedIndex
	if idx <= n.commitIndex {
		// The node already has, and applied, everything the snapshot
		// carries.
		return InstallSnapshotResponse{Term: n.term, Success: true}
	}
	if n.cfg.Storage == nil {
		// A node without a Storage cannot keep a snapshot; saying so beats
		// letting the leader count one that never arrived.
		return InstallSnapshotResponse{Term: n.term}
	}
	// The entries after the snapshot stay if the log has the entry it ends
	// with, as it does when the snapshot is just a shortcut.
	keep := idx <= n.lastIndex() && n.termAt(idx) == req.LastIncludedTerm
	base := Entry{Index: idx, Term: req.LastIncludedTerm}
	if err := n.cfg.Storage.InstallSnapshot(base, keep, req.Config, req.Data); err != nil {
		n.cfg.OnStorageError(err)
	}
	if keep {
		n.log = append([]Entry{base}, n.log[idx-n.base()+1:]...)
	} else {
		n.log = []Entry{base}
	}
	n.commitIndex, n.lastApplied = idx, idx
	n.baseMembers = req.Config
	n.committedMembers = req.Config
	m, at := n.configAsOf(n.lastIndex())
	n.setMembership(m, at)
	return InstallSnapshotResponse{Term: n.term, Success: true}
}

// --- internals; every method below expects n.mu to be held unless noted ---

func (n *Node) run() { // not locked: goroutine
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case now := <-t.C:
			n.mu.Lock()
			if n.state == Leader {
				n.broadcastAppend()
			} else if !n.frozen && n.members.has(n.cfg.ID) && !now.Before(n.electionDue) {
				// Only members campaign: a node the configuration dropped
				// never disrupts the cluster it left.
				n.startElection()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) applier() { // not locked: goroutine
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.applyCh:
		}
		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			batch := append([]Entry(nil), n.log[n.lastApplied+1-n.base():n.commitIndex+1-n.base()]...)
			n.mu.Unlock()
			for _, e := range batch {
				m, isConfig := decodeConfigEntry(e.Data)
				// A configuration entry is applied by the node itself; the
				// application never sees it.
				if !isConfig && n.cfg.Apply != nil {
					n.cfg.Apply(e)
				}
				n.mu.Lock()
				// A snapshot installed meanwhile may have moved it past e.
				n.lastApplied = max(n.lastApplied, e.Index)
				if isConfig {
					n.configCommitted(m, e.Index)
				}
				if w, ok := n.waiters[e.Index]; ok {
					delete(n.waiters, e.Index)
					if w.term == e.Term {
						w.ch <- nil
					} else {
						w.ch <- ErrLostLeadership
					}
				}
				n.mu.Unlock()
			}
			n.mu.Lock()
			n.compact()
			n.mu.Unlock()
		}
	}
}

// entrySize is what an entry counts for against RetentionLogSize.
func entrySize(e Entry) int64 { return int64(len(e.Data)) + 16 }

// compact drops the oldest applied entries once they exceed RetentionLogSize
// by a quarter, down to RetentionLogSize.
func (n *Node) compact() {
	keep := n.cfg.RetentionLogSize
	if keep <= 0 || n.cfg.Storage == nil {
		return
	}
	last := int(n.lastApplied - n.base()) // position in n.log of the last applied entry
	var total int64
	for _, e := range n.log[1 : last+1] {
		total += entrySize(e)
	}
	if total <= keep+keep/4 {
		return
	}
	// Entries after position i stay; the one at i is the new base.
	var kept int64
	i := last
	for ; i >= 1 && kept+entrySize(n.log[i]) <= keep; i-- {
		kept += entrySize(n.log[i])
	}
	if i < 1 {
		return
	}
	base := Entry{Index: n.log[i].Index, Term: n.log[i].Term}
	config, _ := n.configAsOf(base.Index)
	if err := n.cfg.Storage.Compact(base, config); err != nil {
		n.cfg.OnStorageError(err)
	}
	n.baseMembers = config
	n.log = append([]Entry{base}, n.log[i+1:]...)
}

func (n *Node) startElection() {
	n.state = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.saveState()
	n.leader = ""
	n.votes = map[string]bool{n.cfg.ID: true}
	n.resetElectionTimer(time.Now())
	if n.members.quorum(func(id string) bool { return n.votes[id] }) {
		n.becomeLeader()
		return
	}
	req := RequestVoteRequest{
		Term:         n.term,
		Candidate:    n.cfg.ID,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	asked := make(map[string]bool)
	for _, set := range []map[string]string{n.members.Voters, n.members.Joint} {
		for p := range set {
			if p != n.cfg.ID && !asked[p] {
				asked[p] = true
				go n.requestVote(p, req)
			}
		}
	}
}

func (n *Node) requestVote(peer string, req RequestVoteRequest) { // not locked: goroutine
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
	defer cancel()
	resp, err := n.cfg.Transport.RequestVote(ctx, peer, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return
	}
	if n.state != Candidate || n.term != req.Term || !resp.Granted {
		return
	}
	n.votes[peer] = true
	if n.members.quorum(func(id string) bool { return n.votes[id] }) {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leader = n.cfg.ID
	n.nextIndex = make(map[string]uint64)
	// The leader's own stored entries count for the quorum.
	n.matchIndex = map[string]uint64{n.cfg.ID: n.lastIndex()}
	// Entries of earlier terms commit only together with one of this term.
	n.noopIndex = n.lastIndex() + 1
	n.log = append(n.log, Entry{Index: n.noopIndex, Term: n.term})
	n.saveLog(n.noopIndex)
	n.matchIndex[n.cfg.ID] = n.noopIndex
	n.advanceCommit() // a single-node cluster commits immediately
	n.broadcastAppend()
}

func (n *Node) becomeFollower(term uint64, leader string) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.saveState()
	}
	if n.state == Leader {
		n.failWaiters(ErrLostLeadership)
	}
	n.state = Follower
	n.leader = leader
	n.votes, n.nextIndex, n.matchIndex = nil, nil, nil
	n.departed = make(map[string]departedNode)
	n.resetElectionTimer(time.Now())
}

func (n *Node) broadcastAppend() {
	for _, set := range []map[string]string{n.members.Voters, n.members.Joint} {
		for p := range set {
			if p != n.cfg.ID {
				n.sendAppend(p)
			}
		}
	}
	// Nodes a committed configuration dropped stay replication targets until
	// they have the entry and its commit, so they learn they left; a grace
	// covers one that is down (its agent polls the others for the same).
	for id, d := range n.departed {
		if n.members.has(id) || time.Since(d.since) > 10*n.cfg.ElectionTimeout {
			delete(n.departed, id)
			continue
		}
		n.sendAppend(id)
		if n.matchIndex[id] >= d.index {
			// This AppendEntries carries the commit past the entry.
			delete(n.departed, id)
		}
	}
}

func (n *Node) sendAppend(peer string) {
	next, ok := n.nextIndex[peer]
	if !ok {
		// A member the configuration just added starts at the end of the log.
		next = n.lastIndex() + 1
		n.nextIndex[peer] = next
	}
	next = max(next, 1)
	if next <= n.base() {
		n.sendSnapshot(peer) // the entries the peer needs are compacted away
		return
	}
	prev := next - 1
	req := AppendEntriesRequest{
		Term:         n.term,
		Leader:       n.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  n.termAt(prev),
		Entries:      append([]Entry(nil), n.log[next-n.base():]...),
		LeaderCommit: n.commitIndex,
	}
	go n.appendEntries(peer, req)
}

func (n *Node) appendEntries(peer string, req AppendEntriesRequest) { // not locked: goroutine
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
	defer cancel()
	resp, err := n.cfg.Transport.AppendEntries(ctx, peer, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return
	}
	if n.state != Leader || n.term != req.Term {
		return
	}
	if resp.Success {
		m := min(req.PrevLogIndex+uint64(len(req.Entries)), n.lastIndex())
		if m > n.matchIndex[peer] {
			n.matchIndex[peer] = m
			n.advanceCommit()
		}
		n.nextIndex[peer] = max(n.nextIndex[peer], m+1)
		return
	}
	// Consistency check failed. Ignore stale responses, otherwise back off
	// (using the follower's hint) and retry at once.
	next := n.nextIndex[peer]
	if req.PrevLogIndex+1 != next || next <= 1 {
		return
	}
	n.nextIndex[peer] = max(1, min(next-1, resp.MatchIndex+1))
	n.sendAppend(peer)
}

// sendSnapshot starts sending the state machine to peer, unless a snapshot is
// on its way already.
func (n *Node) sendSnapshot(peer string) {
	if n.sending[peer] || n.stopped {
		return
	}
	n.sending[peer] = true
	term := n.term
	n.wg.Go(func() { // not locked: goroutine
		n.installSnapshot(peer, term)
		n.mu.Lock()
		delete(n.sending, peer)
		n.mu.Unlock()
	})
}

func (n *Node) installSnapshot(peer string, term uint64) { // not locked
	data, applied, err := n.cfg.Storage.Snapshot()
	if err != nil {
		n.cfg.OnStorageError(err)
		return
	}
	n.mu.Lock()
	// The state machine is at or past the compacted part, unless a snapshot
	// was installed here meanwhile, which a leader does not get.
	if n.state != Leader || n.term != term || applied < n.base() || applied > n.lastIndex() {
		n.mu.Unlock()
		return
	}
	config, _ := n.configAsOf(applied)
	req := InstallSnapshotRequest{
		Term:              term,
		Leader:            n.cfg.ID,
		LastIncludedIndex: applied,
		LastIncludedTerm:  n.termAt(applied),
		Config:            config,
		Data:              data,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, 10*n.cfg.ElectionTimeout)
	defer cancel()
	resp, err := n.cfg.Transport.InstallSnapshot(ctx, peer, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return
	}
	if n.state != Leader || n.term != term {
		return
	}
	if !resp.Success {
		// The peer could not keep it, e.g. it runs without a Storage; a
		// later heartbeat retries.
		return
	}
	n.matchIndex[peer] = max(n.matchIndex[peer], applied)
	n.nextIndex[peer] = max(n.nextIndex[peer], applied+1)
	n.advanceCommit()
	n.sendAppend(peer)
}

// advanceCommit moves commitIndex to the highest index of the current term
// that is replicated on a quorum (Raft §5.4.2: entries from earlier terms are
// committed only indirectly). While a membership change is joint, a quorum
// means a majority of the current set and of the new one.
func (n *Node) advanceCommit() {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.term {
			break
		}
		if n.members.quorum(func(id string) bool { return n.matchIndex[id] >= idx }) {
			n.setCommit(idx)
			break
		}
	}
	n.maybeFinalize()
}

// maybeFinalize settles a committed joint configuration into the new set with
// a follow-up entry, which only that set needs to commit. The leader of the
// joint phase does it, as does a new leader that finds the joint entry
// committed in its log.
func (n *Node) maybeFinalize() {
	if n.state != Leader || n.members.Joint == nil || n.confIndex > n.commitIndex {
		return
	}
	m := Membership{Voters: n.members.Joint}
	idx := n.lastIndex() + 1
	n.log = append(n.log, Entry{Index: idx, Term: n.term, Data: configEntryData(m)})
	n.saveLog(idx)
	n.matchIndex[n.cfg.ID] = idx
	n.setMembership(m, idx)
	n.advanceCommit()
	n.broadcastAppend()
}

// configCommitted records the committed configuration m, set by the entry at
// index. On the leader it keeps the nodes the configuration dropped as
// replication targets until they learn the entry committed, and a leader the
// configuration dropped leaves office.
func (n *Node) configCommitted(m Membership, index uint64) {
	old := n.committedMembers
	n.committedMembers = m
	if n.state != Leader {
		return
	}
	now := time.Now()
	for _, set := range []map[string]string{old.Voters, old.Joint} {
		for id := range set {
			if id != n.cfg.ID && !m.has(id) {
				n.departed[id] = departedNode{index: index, since: now}
			}
		}
	}
	if m.Joint == nil && !m.has(n.cfg.ID) {
		n.becomeFollower(n.term, "")
	}
}

func (n *Node) setCommit(idx uint64) {
	if idx <= n.commitIndex {
		return
	}
	n.commitIndex = idx
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

// saveState stores the term and the vote.
func (n *Node) saveState() {
	if n.cfg.Storage == nil {
		return
	}
	if err := n.cfg.Storage.SaveState(n.term, n.votedFor); err != nil {
		n.cfg.OnStorageError(err)
	}
}

// saveLog stores the log from index `from` on, dropping what was stored there.
func (n *Node) saveLog(from uint64) {
	if n.cfg.Storage == nil {
		return
	}
	if err := n.cfg.Storage.SetLog(from, n.log[from-n.base():]); err != nil {
		n.cfg.OnStorageError(err)
	}
}

func (n *Node) failWaiters(err error) {
	for idx, w := range n.waiters {
		w.ch <- err
		delete(n.waiters, idx)
	}
}

func (n *Node) resetElectionTimer(now time.Time) {
	n.electionDue = now.Add(n.cfg.ElectionTimeout + rand.N(n.cfg.ElectionTimeout))
}

// setMembership switches the configuration in effect to m, set by the entry
// at index. A joining node whose elections were frozen may campaign once a
// configuration names it. The callback fires only when the membership itself
// changes.
func (n *Node) setMembership(m Membership, index uint64) {
	changed := !maps.Equal(n.members.Voters, m.Voters) || !maps.Equal(n.members.Joint, m.Joint)
	n.members, n.confIndex = m, index
	if n.frozen && m.has(n.cfg.ID) {
		n.frozen = false
	}
	if changed && n.cfg.OnMembershipChange != nil {
		n.cfg.OnMembershipChange(m.clone())
	}
}

// configAsOf returns the membership in effect at index idx, which lies from
// the base on, and the index of the entry that set it (the base's index when
// the compacted prefix carries it).
func (n *Node) configAsOf(idx uint64) (Membership, uint64) {
	for i := min(idx, n.lastIndex()); i > n.base(); i-- {
		if m, ok := decodeConfigEntry(n.log[i-n.base()].Data); ok {
			return m, i
		}
	}
	return n.baseMembers, n.base()
}

// base is the index of the entry before the first one the log holds: 0, or the
// last entry compacted away.
func (n *Node) base() uint64 { return n.log[0].Index }

func (n *Node) lastIndex() uint64 { return n.base() + uint64(len(n.log)-1) }
func (n *Node) lastTerm() uint64  { return n.log[len(n.log)-1].Term }

// termAt returns the term of the entry at idx, which lies from base on.
func (n *Node) termAt(idx uint64) uint64 { return n.log[idx-n.base()].Term }
