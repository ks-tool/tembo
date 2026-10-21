package agent

import (
	"github.com/ks-tool/tembo/internal/raft"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	raftStateDesc = prometheus.NewDesc("tembo_raft_state",
		"The role this raft node plays: one series per state, 1 for the current one.", []string{"state"}, nil)
	raftTermDesc = prometheus.NewDesc("tembo_raft_term",
		"The current raft term.", nil, nil)
	raftLeaderDesc = prometheus.NewDesc("tembo_raft_leader",
		"The node believed to be the raft leader.", []string{"id"}, nil)
	raftCommitDesc  = prometheus.NewDesc("tembo_raft_commit_index", "The raft commit index.", nil, nil)
	raftAppliedDesc = prometheus.NewDesc("tembo_raft_applied_index", "The last raft index applied to the state machine.", nil, nil)
	raftLastDesc    = prometheus.NewDesc("tembo_raft_last_log_index", "The last index in the raft log.", nil, nil)
	raftMembersDesc = prometheus.NewDesc("tembo_raft_members",
		"The committed membership: voters, and the incoming set while a change is joint.", []string{"set"}, nil)
	agentQuorumDesc = prometheus.NewDesc("tembo_agent_quorum",
		"Whether this node recently knew a raft leader and reached a majority.", nil, nil)
	agentNodesDesc = prometheus.NewDesc("tembo_agent_nodes_running",
		"Nodes whose Postgres answered the last poll, this one included.", nil, nil)
)

// Metrics returns a prometheus.Collector over the agent and its raft node.
func (s *Server) Metrics() prometheus.Collector { return agentMetrics{s} }

type agentMetrics struct{ s *Server }

// Describe sends nothing: the leader series comes and goes, so the collector
// is unchecked.
func (agentMetrics) Describe(chan<- *prometheus.Desc) {}

func (m agentMetrics) Collect(ch chan<- prometheus.Metric) {
	n := m.s.node
	stats := n.Stats()
	state := n.State()
	for _, st := range []raft.State{raft.Follower, raft.Candidate, raft.Leader} {
		v := 0.0
		if state == st {
			v = 1
		}
		ch <- prometheus.MustNewConstMetric(raftStateDesc, prometheus.GaugeValue, v, st.String())
	}
	ch <- prometheus.MustNewConstMetric(raftTermDesc, prometheus.GaugeValue, float64(n.Term()))
	if leader := n.Leader(); leader != "" {
		ch <- prometheus.MustNewConstMetric(raftLeaderDesc, prometheus.GaugeValue, 1, leader)
	}
	ch <- prometheus.MustNewConstMetric(raftCommitDesc, prometheus.GaugeValue, float64(stats.CommitIndex))
	ch <- prometheus.MustNewConstMetric(raftAppliedDesc, prometheus.GaugeValue, float64(stats.Applied))
	ch <- prometheus.MustNewConstMetric(raftLastDesc, prometheus.GaugeValue, float64(stats.LastIndex))
	members := n.CommittedMembership()
	ch <- prometheus.MustNewConstMetric(raftMembersDesc, prometheus.GaugeValue, float64(len(members.Voters)), "voters")
	if members.Joint != nil {
		ch <- prometheus.MustNewConstMetric(raftMembersDesc, prometheus.GaugeValue, float64(len(members.Joint)), "joint")
	}

	quorum := 0.0
	if m.s.HasQuorum() {
		quorum = 1
	}
	ch <- prometheus.MustNewConstMetric(agentQuorumDesc, prometheus.GaugeValue, quorum)
	running := 0
	for _, p := range m.s.Nodes() {
		if p.Running {
			running++
		}
	}
	ch <- prometheus.MustNewConstMetric(agentNodesDesc, prometheus.GaugeValue, float64(running))
}
