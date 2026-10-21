package raft_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ks-tool/tembo/internal/raft"
)

// Timings for the tests: elections resolve in tens of milliseconds, and a
// heartbeat fast enough to commit within the tolerance of eventually.
const (
	testElection  = 60 * time.Millisecond
	testHeartbeat = 15 * time.Millisecond
)

// network is an in-memory Transport that delivers RPCs synchronously, in the
// caller's goroutine. A node in cut is isolated: calls to or from it block
// until their context expires, as over a network that blackholes traffic.
type network struct {
	mu    sync.Mutex
	nodes map[string]raft.Handler
	cut   map[string]bool
}

func newNetwork() *network {
	return &network{nodes: map[string]raft.Handler{}, cut: map[string]bool{}}
}

func (n *network) transport(from string) raft.Transport {
	return &netTransport{net: n, from: from}
}

type netTransport struct {
	net  *network
	from string
}

func (t *netTransport) RequestVote(ctx context.Context, to string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	err := t.net.deliver(ctx, t.from, to, func(h raft.Handler) { resp = h.HandleRequestVote(req) })
	return resp, err
}

func (t *netTransport) AppendEntries(ctx context.Context, to string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	err := t.net.deliver(ctx, t.from, to, func(h raft.Handler) { resp = h.HandleAppendEntries(req) })
	return resp, err
}

func (t *netTransport) InstallSnapshot(ctx context.Context, to string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	var resp raft.InstallSnapshotResponse
	err := t.net.deliver(ctx, t.from, to, func(h raft.Handler) { resp = h.HandleInstallSnapshot(req) })
	return resp, err
}

// deliver hands the RPC to node to unless either end of it is isolated.
func (n *network) deliver(ctx context.Context, from, to string, call func(raft.Handler)) error {
	n.mu.Lock()
	if n.cut[from] || n.cut[to] {
		n.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}
	h := n.nodes[to]
	n.mu.Unlock()
	if h == nil {
		return fmt.Errorf("no node %q", to)
	}
	call(h)
	return nil
}

func (n *network) isolate(id string, cut bool) {
	n.mu.Lock()
	n.cut[id] = cut
	n.mu.Unlock()
}

func (n *network) register(id string, h raft.Handler) {
	n.mu.Lock()
	n.nodes[id] = h
	n.mu.Unlock()
}

// recorder collects the entries a node applied: the contract the state
// machine sees.
type recorder struct {
	mu      sync.Mutex
	entries []raft.Entry
}

func (r *recorder) apply(e raft.Entry) {
	if len(e.Data) == 0 {
		return // a no-op of raft
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, e)
}

func (r *recorder) snapshot() []raft.Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.entries)
}

func (r *recorder) data() []string {
	return entryData(r.snapshot())
}

func entryData(entries []raft.Entry) []string {
	data := make([]string, len(entries))
	for i, e := range entries {
		data[i] = string(e.Data)
	}
	return data
}

// peerSet is an initial membership with the node id as its address; the
// in-memory transport resolves by id and ignores it.
func peerSet(ids ...string) map[string]string {
	m := make(map[string]string, len(ids))
	for _, id := range ids {
		m[id] = id
	}
	return m
}

// cluster is a set of started nodes on one network.
type cluster struct {
	net   *network
	ids   []string
	nodes map[string]*raft.Node
	recs  map[string]*recorder
}

func startCluster(t *testing.T, ids ...string) *cluster {
	t.Helper()
	c := &cluster{
		net:   newNetwork(),
		ids:   ids,
		nodes: make(map[string]*raft.Node, len(ids)),
		recs:  make(map[string]*recorder, len(ids)),
	}
	for _, id := range ids {
		c.startNode(t, id, peerSet(ids...), false)
	}
	return c
}

// startNode starts one node of the cluster with the given initial membership
// and frozenness, and registers it for cleanup.
func (c *cluster) startNode(t *testing.T, id string, peers map[string]string, frozen bool) {
	t.Helper()
	rec := &recorder{}
	n, err := raft.New(raft.Config{
		ID:                id,
		Peers:             peers,
		Frozen:            frozen,
		Transport:         c.net.transport(id),
		Apply:             rec.apply,
		ElectionTimeout:   testElection,
		HeartbeatInterval: testHeartbeat,
	})
	if err != nil {
		t.Fatalf("raft.New(%q): %v", id, err)
	}
	c.net.register(id, n)
	c.nodes[id] = n
	c.recs[id] = rec
	n.Start()
	t.Cleanup(n.Stop)
}

// others returns the ids of the cluster without id.
func (c *cluster) others(id string) []string {
	return slices.DeleteFunc(slices.Clone(c.ids), func(s string) bool { return s == id })
}

// waitLeader waits until one of ids is the leader and returns it. In these
// tests at most one of the given ids can win an election at a time.
func (c *cluster) waitLeader(t *testing.T, ids ...string) string {
	t.Helper()
	var leader string
	eventually(t, 3*time.Second, func() bool {
		for _, id := range ids {
			if c.nodes[id].IsLeader() {
				leader = id
			}
		}
		return leader != ""
	})
	return leader
}

// waitApplied waits until each of ids has applied exactly want, in order.
func (c *cluster) waitApplied(t *testing.T, want []string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		eventually(t, 3*time.Second, func() bool {
			return slices.Equal(c.recs[id].data(), want)
		})
	}
}

// propose proposes data and returns what Propose returned.
func propose(t *testing.T, n *raft.Node, data string, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return n.Propose(ctx, []byte(data))
}

func eventually(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func TestNewRejectsBadConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  raft.Config
	}{
		{"no ID", raft.Config{Transport: newNetwork().transport("a")}},
		{"no transport", raft.Config{ID: "a"}},
		{"heartbeat not below election timeout", raft.Config{
			ID: "a", Transport: newNetwork().transport("a"),
			ElectionTimeout: testHeartbeat, HeartbeatInterval: testHeartbeat,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := raft.New(tc.cfg); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}

func TestElectAndReplicate(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	leader := c.waitLeader(t, c.ids...)

	// everyone follows the same leader in the same term
	eventually(t, 3*time.Second, func() bool {
		for _, n := range c.nodes {
			if n.Leader() != leader || n.Term() != c.nodes[leader].Term() {
				return false
			}
		}
		return true
	})

	if err := propose(t, c.nodes[leader], "one", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if err := propose(t, c.nodes[leader], "two", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.waitApplied(t, []string{"one", "two"}, c.ids...)

	entries := c.recs[leader].snapshot()
	for i, e := range entries {
		// a leader's no-op takes the indexes the entries skip
		if i > 0 && e.Index <= entries[i-1].Index {
			t.Fatalf("entry %d has index %d after %d", i, e.Index, entries[i-1].Index)
		}
		if e.Term != entries[0].Term {
			t.Fatalf("entry %d has term %d, want %d", i, e.Term, entries[0].Term)
		}
	}

	// a follower refuses proposals
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if err := propose(t, c.nodes[id], "x", time.Second); !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("propose on follower %s: got %v, want ErrNotLeader", id, err)
		}
		break
	}
}

func TestSingleNodeCommitsAlone(t *testing.T) {
	c := startCluster(t, "solo")
	c.waitLeader(t, "solo")
	if err := propose(t, c.nodes["solo"], "only", time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.waitApplied(t, []string{"only"}, "solo")
}

func TestProposeWithoutQuorumTimesOut(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	leader := c.waitLeader(t, c.ids...)

	c.net.isolate(leader, true)
	err := propose(t, c.nodes[leader], "never-committed", 300*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
}

func TestNewLeaderOverwritesDivergentLog(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	first := c.waitLeader(t, c.ids...)
	if err := propose(t, c.nodes[first], "committed", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.waitApplied(t, []string{"committed"}, c.ids...)

	// a majority that goes on without its leader
	c.net.isolate(first, true)
	second := c.waitLeader(t, c.others(first)...)
	if err := propose(t, c.nodes[second], "replacement", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// the isolated leader appends an entry it cannot commit
	if err := propose(t, c.nodes[first], "orphan", 400*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}

	// back in the majority, its divergent tail is overwritten, the committed
	// entry keeps its place and the orphan never applies
	c.net.isolate(first, false)
	c.waitApplied(t, []string{"committed", "replacement"}, c.ids...)
}

func TestProposeAfterLeadershipLost(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	first := c.waitLeader(t, c.ids...)
	c.net.isolate(first, true)

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errCh <- c.nodes[first].Propose(ctx, []byte("orphan"))
	}()

	second := c.waitLeader(t, c.others(first)...)
	c.net.isolate(first, false)

	select {
	case err := <-errCh:
		if !errors.Is(err, raft.ErrLostLeadership) {
			t.Fatalf("got %v, want ErrLostLeadership", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Propose did not return")
	}

	// the cluster goes on without the orphan
	if err := propose(t, c.nodes[second], "after", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.waitApplied(t, []string{"after"}, c.ids...)
}

func TestRestartedNodeGetsLogReplayed(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	leader := c.waitLeader(t, c.ids...)
	for _, data := range []string{"one", "two"} {
		if err := propose(t, c.nodes[leader], data, 2*time.Second); err != nil {
			t.Fatalf("propose %q: %v", data, err)
		}
	}
	c.waitApplied(t, []string{"one", "two"}, c.ids...)

	// a node that restarts comes back with an empty log — the library keeps
	// no persistence — and must receive the whole log again
	c.nodes["c"].Stop()
	rec := &recorder{}
	n, err := raft.New(raft.Config{
		ID:                "c",
		Peers:             peerSet(c.ids...),
		Transport:         c.net.transport("c"),
		Apply:             rec.apply,
		ElectionTimeout:   testElection,
		HeartbeatInterval: testHeartbeat,
	})
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}
	c.net.register("c", n)
	c.nodes["c"] = n
	c.recs["c"] = rec
	n.Start()
	t.Cleanup(n.Stop)

	c.waitApplied(t, []string{"one", "two"}, "c")
}

func TestConcurrentProposeCommitsEverything(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	leader := c.waitLeader(t, c.ids...)

	const n = 20
	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("v%02d", i)
	}
	errs := make(chan error, n)
	for i := range n {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			errs <- c.nodes[leader].Propose(ctx, []byte(want[i]))
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	// every value applies exactly once, in log order, on every node
	for _, id := range c.ids {
		eventually(t, 3*time.Second, func() bool {
			got := c.recs[id].data()
			slices.Sort(got)
			return slices.Equal(got, want)
		})
	}
	applied := c.recs[leader].snapshot()
	for i, e := range applied {
		if i > 0 && e.Index <= applied[i-1].Index {
			t.Fatalf("entry %d has index %d after %d", i, e.Index, applied[i-1].Index)
		}
	}
}

func TestStopRefusesPropose(t *testing.T) {
	c := startCluster(t, "solo")
	c.waitLeader(t, "solo")
	n := c.nodes["solo"]
	n.Stop()
	if err := propose(t, n, "x", time.Second); !errors.Is(err, raft.ErrStopped) {
		t.Fatalf("got %v, want ErrStopped", err)
	}
}

func TestLeaderIsReadyOnceItsNoopIsApplied(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	leader := c.waitLeader(t, c.ids...)

	eventually(t, 3*time.Second, c.nodes[leader].Ready)
	for _, id := range c.others(leader) {
		if c.nodes[id].Ready() {
			t.Fatalf("follower %s is ready", id)
		}
	}
}

// memStorage is a raft.Storage that outlives the nodes made from it, and the
// state machine kept with it: the values applied, in order.
type memStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor string
	base     raft.Entry
	config   *raft.Membership
	log      []raft.Entry
	applied  uint64
	values   []string
}

func (m *memStorage) Load() (raft.Stored, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return raft.Stored{Term: m.term, VotedFor: m.votedFor, Base: m.base, Config: m.config, Log: slices.Clone(m.log)}, nil
}

func (m *memStorage) SaveState(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term, m.votedFor = term, votedFor
	return nil
}

func (m *memStorage) SetLog(from uint64, entries []raft.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log[:from-m.base.Index-1], entries...)
	return nil
}

func (m *memStorage) Compact(base raft.Entry, config raft.Membership) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if base.Index > m.applied {
		return fmt.Errorf("compacting to %d, past the applied %d", base.Index, m.applied)
	}
	m.log = slices.Clone(m.log[base.Index-m.base.Index:])
	m.base = base
	m.config = &config
	return nil
}

func (m *memStorage) Snapshot() ([]byte, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := json.Marshal(m.values)
	return data, m.applied, err
}

func (m *memStorage) InstallSnapshot(base raft.Entry, keepLog bool, config raft.Membership, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := json.Unmarshal(data, &m.values); err != nil {
		return err
	}
	m.applied = base.Index
	if keepLog {
		m.log = slices.Clone(m.log[base.Index-m.base.Index:])
	} else {
		m.log = nil
	}
	m.base = base
	m.config = &config
	return nil
}

func (m *memStorage) apply(e raft.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.Index <= m.applied {
		return
	}
	m.applied = e.Index
	if len(e.Data) > 0 {
		m.values = append(m.values, string(e.Data))
	}
}

func (m *memStorage) snapshot() (values []string, applied uint64, base uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.values), m.applied, m.base.Index
}

// startStored starts a node per id on its storage, resuming where the storage
// left off. The nodes keep retention bytes of applied log.
func startStored(t *testing.T, net *network, ids []string, stores map[string]*memStorage, retention int64) map[string]*raft.Node {
	t.Helper()
	nodes := map[string]*raft.Node{}
	for _, id := range ids {
		_, applied, _ := stores[id].snapshot()
		n, err := raft.New(raft.Config{
			ID: id, Peers: peerSet(ids...), Transport: net.transport(id), Apply: stores[id].apply,
			Storage: stores[id], Applied: applied, RetentionLogSize: retention,
			ElectionTimeout: testElection, HeartbeatInterval: testHeartbeat,
		})
		if err != nil {
			t.Fatalf("raft.New(%q): %v", id, err)
		}
		net.register(id, n)
		nodes[id] = n
		n.Start()
		t.Cleanup(n.Stop)
	}
	return nodes
}

func waitValues(t *testing.T, stores map[string]*memStorage, want []string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		eventually(t, 5*time.Second, func() bool {
			got, _, _ := stores[id].snapshot()
			return slices.Equal(got, want)
		})
	}
}

func waitLeaderOf(t *testing.T, nodes map[string]*raft.Node) string {
	t.Helper()
	var leader string
	eventually(t, 3*time.Second, func() bool {
		for id, n := range nodes {
			if n.IsLeader() {
				leader = id
			}
		}
		return leader != ""
	})
	return leader
}

func TestPersistedClusterSurvivesRestartOfAllNodes(t *testing.T) {
	ids := []string{"a", "b", "c"}
	stores := map[string]*memStorage{"a": {}, "b": {}, "c": {}}

	nodes := startStored(t, newNetwork(), ids, stores, 0)
	leader := waitLeaderOf(t, nodes)
	for _, data := range []string{"one", "two"} {
		if err := propose(t, nodes[leader], data, 2*time.Second); err != nil {
			t.Fatalf("propose %q: %v", data, err)
		}
	}
	waitValues(t, stores, []string{"one", "two"}, ids...)
	term := stores[leader].term
	for _, n := range nodes {
		n.Stop()
	}

	// Every node comes back with its log and its term, and the state
	// machines are not handed what they applied.
	nodes = startStored(t, newNetwork(), ids, stores, 0)
	leader = waitLeaderOf(t, nodes)
	if got := nodes[leader].Term(); got <= term {
		t.Fatalf("term went back to %d, was %d", got, term)
	}
	if err := propose(t, nodes[leader], "three", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	waitValues(t, stores, []string{"one", "two", "three"}, ids...)

	nodes[leader].Stop()
	_, err := raft.New(raft.Config{ID: leader, Peers: peerSet(ids...), Transport: newNetwork().transport(leader),
		Storage: stores[leader], Applied: uint64(len(stores[leader].log)) + 1})
	if err == nil {
		t.Fatal("raft.New accepted Applied beyond the log")
	}
}

func TestLaggingFollowerGetsSnapshotOfCompactedLog(t *testing.T) {
	ids := []string{"a", "b", "c"}
	stores := map[string]*memStorage{"a": {}, "b": {}, "c": {}}
	net := newNetwork()
	nodes := startStored(t, net, ids, stores, 200)
	leader := waitLeaderOf(t, nodes)
	lagging := "a"
	if leader == lagging {
		lagging = "b"
	}

	net.isolate(lagging, true)
	var want []string
	for i := range 40 {
		v := fmt.Sprintf("value-%02d", i)
		if err := propose(t, nodes[leader], v, 2*time.Second); err != nil {
			t.Fatalf("propose %q: %v", v, err)
		}
		want = append(want, v)
	}
	// 40 entries are far over 200 bytes: the log has been cut.
	eventually(t, 3*time.Second, func() bool {
		_, _, base := stores[leader].snapshot()
		return base > 0
	})
	if _, _, base := stores[lagging].snapshot(); base != 0 {
		t.Fatalf("the isolated node compacted to %d", base)
	}

	net.isolate(lagging, false)
	waitValues(t, stores, want, ids...)
	if _, _, base := stores[lagging].snapshot(); base == 0 {
		t.Fatal("the lagging node caught up without a snapshot")
	}

	// While isolated the node kept raising its term, so reconnecting it costs
	// the cluster its leader. Re-elect, then propose on the current one.
	deadline := time.Now().Add(10 * time.Second)
	for {
		leader = waitLeaderOf(t, nodes)
		if err := propose(t, nodes[leader], "after", 2*time.Second); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader accepted a proposal after the reconnect")
		}
	}
	waitValues(t, stores, append(want, "after"), ids...)

	// and the log kept is about the size asked for
	stores[leader].mu.Lock()
	var size int64
	for _, e := range stores[leader].log {
		size += int64(len(e.Data)) + 16
	}
	stores[leader].mu.Unlock()
	if size > 200+200/4+60 {
		t.Fatalf("the log holds %d bytes", size)
	}
}

func TestInstallSnapshotWithoutStorageIsRefused(t *testing.T) {
	net := newNetwork()
	// A follower without a Storage cannot keep a snapshot; its answer must
	// say so, or the leader counts a state machine that never arrived.
	b, err := raft.New(raft.Config{ID: "b", Peers: peerSet("a", "b"),
		Transport: net.transport("b"), ElectionTimeout: testElection, HeartbeatInterval: testHeartbeat})
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}
	resp := b.HandleInstallSnapshot(raft.InstallSnapshotRequest{Term: 1, Leader: "a",
		LastIncludedIndex: 3, LastIncludedTerm: 1})
	if resp.Success {
		t.Fatal("a node without a Storage confirmed a snapshot it cannot keep")
	}

	// what the snapshot covers the node already has committed: having lost
	// nothing, it confirms the delivery
	if resp := b.HandleAppendEntries(raft.AppendEntriesRequest{Term: 1, Leader: "a", LeaderCommit: 2,
		Entries: []raft.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}}); !resp.Success {
		t.Fatalf("append: %+v", resp)
	}
	resp = b.HandleInstallSnapshot(raft.InstallSnapshotRequest{Term: 1, Leader: "a",
		LastIncludedIndex: 2, LastIncludedTerm: 1})
	if !resp.Success {
		t.Fatal("a node with everything the snapshot covers refused it")
	}
}

// --- membership changes (joint consensus) ---

// proposeMembership moves the cluster to voters and returns what
// ProposeMembership returned.
func proposeMembership(t *testing.T, n *raft.Node, voters map[string]string, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return n.ProposeMembership(ctx, voters)
}

// waitMembers waits until the configuration in effect on n is exactly ids,
// settled (no joint set in flight).
func waitMembers(t *testing.T, n *raft.Node, ids ...string) {
	t.Helper()
	eventually(t, 5*time.Second, func() bool {
		m := n.Membership()
		return m.Joint == nil && slices.Equal(slices.Sorted(maps.Keys(m.Voters)), slices.Sorted(slices.Values(ids)))
	})
}

// proposeOnLeader proposes on whoever is leader now, retrying across
// elections until the timeout.
func proposeOnLeader(t *testing.T, nodes map[string]*raft.Node, data string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var leader *raft.Node
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
			}
		}
		if leader != nil {
			if err := propose(t, leader, data, time.Second); err == nil {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no leader accepted %q within %s", data, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestJoinFourthNode(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	leader := c.waitLeader(t, c.ids...)
	if err := propose(t, c.nodes[leader], "before", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	for _, id := range c.others(leader) {
		if err := proposeMembership(t, c.nodes[id], peerSet("a", "b", "c", "d"), time.Second); !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("membership change on a follower: got %v, want ErrNotLeader", err)
		}
	}

	// A node that joins starts frozen, naming itself and one member; it
	// replicates and follows but does not campaign.
	c.startNode(t, "d", peerSet("d", leader), true)
	if !c.nodes["d"].Frozen() {
		t.Fatal("the joining node is not frozen")
	}

	if err := proposeMembership(t, c.nodes[leader], peerSet("a", "b", "c", "d"), 5*time.Second); err != nil {
		t.Fatalf("propose membership: %v", err)
	}
	// The new set settles on everyone, the joiner caught up and unfroze.
	for _, id := range []string{"a", "b", "c", "d"} {
		waitMembers(t, c.nodes[id], "a", "b", "c", "d")
	}
	eventually(t, 3*time.Second, func() bool { return !c.nodes["d"].Frozen() })

	if err := propose(t, c.nodes[leader], "after", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.waitApplied(t, []string{"before", "after"}, "a", "b", "c", "d")

	// Proposing the set in effect is a no-op.
	if err := proposeMembership(t, c.nodes[leader], peerSet("a", "b", "c", "d"), time.Second); err != nil {
		t.Fatalf("proposing the current membership: %v", err)
	}
}

func TestRemoveFollower(t *testing.T) {
	c := startCluster(t, "a", "b", "c", "d")
	leader := c.waitLeader(t, c.ids...)
	victim := "d"
	if leader == victim {
		victim = "a"
	}

	rest := c.others(victim)
	if err := proposeMembership(t, c.nodes[leader], peerSet(rest...), 5*time.Second); err != nil {
		t.Fatalf("propose membership: %v", err)
	}
	for _, id := range rest {
		waitMembers(t, c.nodes[id], rest...)
	}
	// The leader keeps the removed node as a target until it learns the
	// configuration that dropped it committed.
	eventually(t, 3*time.Second, c.nodes[victim].Removed)

	// The cluster works on without it; the removed node does not campaign.
	time.Sleep(3 * testElection)
	proposeOnLeader(t, c.nodes, "after", 5*time.Second)
	for _, id := range rest {
		if c.nodes[id].IsLeader() {
			c.waitApplied(t, []string{"after"}, rest...)
			break
		}
	}
	if c.nodes[victim].IsLeader() {
		t.Fatal("the removed node became leader")
	}
}

func TestRemoveLeader(t *testing.T) {
	c := startCluster(t, "a", "b", "c")
	leader := c.waitLeader(t, c.ids...)
	if err := propose(t, c.nodes[leader], "before", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	rest := c.others(leader)

	if err := proposeMembership(t, c.nodes[leader], peerSet(rest...), 5*time.Second); err != nil {
		t.Fatalf("propose membership: %v", err)
	}
	// The removed leader leads the joint phase through, then leaves office
	// when the final configuration commits.
	eventually(t, 3*time.Second, func() bool {
		return c.nodes[leader].Removed() && !c.nodes[leader].IsLeader()
	})
	second := c.waitLeader(t, rest...)
	for _, id := range rest {
		waitMembers(t, c.nodes[id], rest...)
	}
	if err := propose(t, c.nodes[second], "after", 2*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.waitApplied(t, []string{"before", "after"}, rest...)
}

func TestJointConfigReachesLaggingNodeBySnapshot(t *testing.T) {
	ids := []string{"a", "b", "c"}
	stores := map[string]*memStorage{"a": {}, "b": {}, "c": {}}
	net := newNetwork()
	nodes := startStored(t, net, ids, stores, 200)
	leader := waitLeaderOf(t, nodes)
	lagging := "a"
	if leader == lagging {
		lagging = "b"
	}

	net.isolate(lagging, true)
	var want []string
	for i := range 40 {
		v := fmt.Sprintf("value-%02d", i)
		if err := propose(t, nodes[leader], v, 2*time.Second); err != nil {
			t.Fatalf("propose %q: %v", v, err)
		}
		want = append(want, v)
	}
	// 40 entries are far over 200 bytes: the log has been cut.
	eventually(t, 3*time.Second, func() bool {
		_, _, base := stores[leader].snapshot()
		return base > 0
	})

	// A fourth node joins while the lagging one is cut off; the joint entry
	// commits on the leader, the reachable follower and the joiner.
	stores["d"] = &memStorage{}
	d, err := raft.New(raft.Config{
		ID: "d", Peers: peerSet("d", leader), Frozen: true,
		Transport: net.transport("d"), Apply: stores["d"].apply,
		Storage: stores["d"], RetentionLogSize: 200,
		ElectionTimeout: testElection, HeartbeatInterval: testHeartbeat,
	})
	if err != nil {
		t.Fatalf("raft.New(d): %v", err)
	}
	net.register("d", d)
	nodes["d"] = d
	d.Start()
	t.Cleanup(d.Stop)

	if err := proposeMembership(t, nodes[leader], peerSet("a", "b", "c", "d"), 10*time.Second); err != nil {
		t.Fatalf("propose membership: %v", err)
	}
	waitMembers(t, nodes["d"], "a", "b", "c", "d")

	// The lagging node returns to entries it no longer has and gets a
	// snapshot, which carries the configuration.
	net.isolate(lagging, false)
	waitMembers(t, nodes[lagging], "a", "b", "c", "d")
	waitValues(t, stores, want, "a", "b", "c", "d")
}
