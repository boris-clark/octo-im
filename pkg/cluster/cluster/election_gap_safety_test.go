package cluster

import (
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

// Deterministic reproduction of the controller election gap.
//
// Production pieces exercised:
//   - raft.Node (Step/Ready) for all three channel replicas: proposal,
//     follower sync, store acknowledgement and leader commit accounting.
//   - SelectFencedLeader, after Node.FenceElection persists each report.
//   - SaveChannelClusterConfig applies the expected full identity in real Pebble.
//   - Node.Step(ConfChange), the event Channel.applyConfigOnOwner sends to
//     install the elected term.
//
// Test seams (not production code):
//   - The probe response is built from the replica's stored (LogTerm,
//     LogIndex), which is what getChannelLastLogInfo reports through
//     channelServer.LastLogIndexAndTerm (last stored message term/seq).
//   - The ChannelClusterConfig -> raft Config mapping mirrors
//     channel.channelConfigToRaftConfig (unexported in package channel).
//   - Real slot Pebble stores the original synthetic payloads; the map is an
//     assertion mirror. Actual message storage is covered in the channel package.
//
// The barrier is the test itself: events move only when pump is called, so
// the order "sample probe -> old leader commits -> install term" is exact and
// needs no sleeps.

type gapReplica struct {
	id   uint64
	node *raft.Node
	disk *slot.PebbleShardLogStorage
	logs map[uint64]rafttype.Log // stored log, by index
}

type gapCluster struct {
	t        *testing.T
	replicas map[uint64]*gapReplica
	// down holds replicas whose inbound and outbound traffic is dropped.
	reports map[uint64]rafttype.ElectionReport
	down    map[uint64]bool
}

func newGapCluster(t *testing.T) *gapCluster {
	c := &gapCluster{t: t, replicas: map[uint64]*gapReplica{}, reports: map[uint64]rafttype.ElectionReport{}, down: map[uint64]bool{}}
	for _, id := range []uint64{1, 2, 3} {
		disk := slot.NewPebbleShardLogStorage(nil, t.TempDir(), 1)
		require.NoError(t, disk.Open())
		t.Cleanup(func() { require.NoError(t, disk.Close()) })
		opts := raft.NewOptions(
			raft.WithSaveHardState(func(st rafttype.HardState) error { return disk.SaveHardState("gap", st) }),
			raft.WithNodeId(id),
			raft.WithReplicas([]uint64{1, 2, 3}),
			raft.WithElectionOn(false),
			raft.WithElectionInterval(1000),
			raft.WithAdvance(func() {}),
			raft.WithKey("repro_g014____t6"),
		)
		c.replicas[id] = &gapReplica{id: id, disk: disk, node: raft.NewNode(0, rafttype.RaftState{}, opts), logs: map[uint64]rafttype.Log{}}
	}
	return c
}

// install applies a channel cluster config through ConfChange, mapped the way
// channelConfigToRaftConfig maps it.
func (c *gapCluster) install(id uint64, cfg wkdb.ChannelClusterConfig) {
	role := rafttype.RoleFollower
	if cfg.LeaderId == id {
		role = rafttype.RoleLeader
	}
	err := c.replicas[id].node.Step(rafttype.Event{Type: rafttype.ConfChange, Config: rafttype.Config{
		Replicas: cfg.Replicas, Learners: cfg.Learners, Term: cfg.Term, Leader: cfg.LeaderId,
		Role: role, Version: cfg.ConfVersion, InstalledTerm: cfg.InstalledTerm, BoundaryTerm: cfg.BoundaryTerm,
	}})
	if err != nil {
		c.t.Fatalf("install config on %d: %v", id, err)
	}
}

// pump delivers events until no replica has more. Traffic to or from a down
// replica is dropped.
func (c *gapCluster) pump() {
	for round := 0; round < 200; round++ {
		progressed := false
		for _, id := range []uint64{1, 2, 3} {
			r := c.replicas[id]
			if c.down[id] {
				r.node.Ready() // dropped
				continue
			}
			for _, e := range r.node.Ready() {
				progressed = true
				c.handle(r, e)
			}
		}
		if !progressed {
			return
		}
	}
	c.t.Fatal("cluster did not quiesce")
}

func (c *gapCluster) handle(r *gapReplica, e rafttype.Event) {
	if e.To != rafttype.LocalNode {
		if dst := c.replicas[e.To]; dst != nil && !c.down[e.To] {
			_ = dst.node.Step(e)
		}
		return
	}
	switch e.Type {
	case rafttype.StoreReq:
		require.NoError(c.t, r.disk.AppendLogs("gap", e.Logs, e.TermStartIndexInfo))
		for _, l := range e.Logs {
			r.logs[l.Index] = l
		}
		_ = r.node.Step(rafttype.StoreResponse(e, rafttype.Event{Type: rafttype.StoreResp, Index: e.Logs[len(e.Logs)-1].Index, Logs: e.Logs, Reason: rafttype.ReasonOk}))
	case rafttype.ApplyReq:
		require.NoError(c.t, r.disk.SetAppliedIndex("gap", e.EndIndex-1))
		_ = r.node.Step(rafttype.Event{Type: rafttype.ApplyResp, Index: e.EndIndex - 1, Reason: rafttype.ReasonOk})
	case rafttype.GetLogsReq:
		var logs []rafttype.Log
		for i := e.Index; i <= e.StoredIndex+1 || i <= r.node.LastLogIndex(); i++ {
			if l, ok := r.logs[i]; ok {
				logs = append(logs, l)
			}
		}
		_ = r.node.Step(rafttype.Event{Type: rafttype.GetLogsResp, To: e.From, Index: e.Index, Logs: logs, Reason: rafttype.ReasonOk})
	case rafttype.TruncateReq:
		require.NoError(c.t, r.disk.TruncateLogTo("gap", e.Index))
		for i := range r.logs {
			if i > e.Index {
				delete(r.logs, i)
			}
		}
		_ = r.node.Step(rafttype.Event{Type: rafttype.TruncateResp, Index: e.Index, Reason: rafttype.ReasonOk})
	}
}

func (c *gapCluster) propose(id uint64, data string) {
	n := c.replicas[id].node
	if err := n.Step(n.NewPropose([]byte(data))); err != nil {
		c.t.Fatalf("propose on %d: %v", id, err)
	}
	c.pump()
}

// probe returns what getChannelLastLogInfo would report for a replica.
func (c *gapCluster) probe(ids ...uint64) map[uint64]*ChannelLastLogInfoResponse {
	infos := map[uint64]*ChannelLastLogInfoResponse{}
	for _, id := range ids {
		n := c.replicas[id].node
		require.NoError(c.t, n.FenceElection(n.Config(), n.LastTerm()+1))
		state, err := c.replicas[id].disk.GetState("gap")
		require.NoError(c.t, err)
		c.reports[id] = rafttype.ElectionReport{NodeID: id, Term: state.HardState.FenceTerm, Fenced: true, LastIndex: state.LastLogIndex, LastTerm: state.LastTerm, Floor: state.HardState.CommittedFloor}
		infos[id] = &ChannelLastLogInfoResponse{LogTerm: state.LastTerm, LogIndex: state.LastLogIndex, Term: state.HardState.Term}
	}
	return infos
}

type gapOutcome struct {
	oldLeaderCommitted uint64
	committedEntry     rafttype.Log
	newCfg             wkdb.ChannelClusterConfig
	newLeaderLog       map[uint64]rafttype.Log
	reusedEntry        rafttype.Log
}

// runElectionGap runs one schedule. competingWrite controls whether the old
// leader appends and commits in the gap between probe and term install.
func runElectionGap(t *testing.T, competingWrite, probeAfterWrite bool) gapOutcome {
	c := newGapCluster(t)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "repro_g014____t6", ChannelType: 2, Replicas: []uint64{1, 2, 3}, ReplicaMaxCount: 3, LeaderId: 2, Term: 1, InstalledTerm: 1, ConfVersion: 1}
	for _, id := range []uint64{1, 2, 3} {
		c.install(id, cfg)
	}
	c.pump()

	// Committed prefix on all three replicas (stands in for seq 1..478).
	for i := 1; i <= 3; i++ {
		c.propose(2, fmt.Sprintf("prefix-%d", i))
	}
	for _, id := range []uint64{1, 2, 3} {
		if got := c.replicas[id].node.LastLogIndex(); got != 3 {
			t.Fatalf("setup: replica %d last index %d, want 3", id, got)
		}
	}

	// The controller suspects node 2 (switchLeaderIfNeed: cfg.LeaderId offline)
	// and probes the online replicas only. Barrier 1: probe sampled.
	var sampled map[uint64]*ChannelLastLogInfoResponse
	if !probeAfterWrite {
		sampled = c.probe(1, 3)
	}

	// Barrier 2: the gap. The controller has the samples but has not
	// installed term 2. Node 2 is alive for nodes 2<->3 (the controller's
	// suspicion is false or partial), so the old leader keeps serving.
	var out gapOutcome
	c.down[1] = true
	if competingWrite {
		c.propose(2, "s03-00004528")
	}
	out.oldLeaderCommitted = c.replicas[2].node.CommittedIndex()
	out.committedEntry = c.replicas[2].logs[out.oldLeaderCommitted]
	c.down[1] = false
	if probeAfterWrite {
		sampled = c.probe(1, 3)
	}
	for id, info := range sampled {
		t.Logf("probe sample replica=%d logTerm=%d logIndex=%d", id, info.LogTerm, info.LogIndex)
	}

	// Barrier 3: elect from the sample and install the new term on the
	// winner only (configuration propagation to 2 and 3 is later).
	newCfg := cfg.Clone()
	reports := map[uint64]rafttype.ElectionReport{}
	for id := range sampled {
		reports[id] = c.reports[id]
	}
	leader, err := rafttype.SelectFencedLeader(channelElectionConfig(cfg), 2, reports)
	if err != nil {
		t.Fatalf("electChannelLeader: %v", err)
	}
	newCfg.LeaderId = leader
	newCfg.Term = 2
	newCfg.InstalledTerm = 2
	// The production DB applies the full expected identity before publishing an install.
	authority := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, authority.Open())
	t.Cleanup(func() { require.NoError(t, authority.Close()) })
	require.NoError(t, authority.SaveChannelClusterConfig(cfg))
	expected, err := authority.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	newCfg.Expected = &expected
	newCfg.ConfVersion++
	require.NoError(t, authority.SaveChannelClusterConfig(newCfg))
	newCfg, err = authority.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.EqualValues(t, 2, newCfg.InstalledTerm)
	out.newCfg = newCfg
	for id := range c.replicas { // only the winner learns the new term here
		c.down[id] = id != newCfg.LeaderId
	}
	c.install(newCfg.LeaderId, newCfg)
	c.pump()
	out.newLeaderLog = map[uint64]rafttype.Log{}
	for i, l := range c.replicas[newCfg.LeaderId].logs {
		out.newLeaderLog[i] = l
	}

	// The new serving leader accepts the next send.
	c.propose(newCfg.LeaderId, "s05-00004499")
	w := c.replicas[newCfg.LeaderId]
	out.reusedEntry = w.logs[w.node.LastLogIndex()]
	return out
}

// Safety: every entry the old leader committed (and could have acknowledged
// as Persisted) must be in the new serving leader's log, or the old leader
// must not have been able to commit after the probe.
func TestElectionGapOldLeaderCommitIsLostByNewLeader(t *testing.T) {
	out := runElectionGap(t, true, false)
	t.Logf("old leader committed index=%d term=%d data=%q", out.oldLeaderCommitted, out.committedEntry.Term, out.committedEntry.Data)
	t.Logf("elected leader=%d term=%d from sampled probe", out.newCfg.LeaderId, out.newCfg.Term)
	t.Logf("new leader next entry index=%d term=%d data=%q", out.reusedEntry.Index, out.reusedEntry.Term, out.reusedEntry.Data)

	if out.oldLeaderCommitted <= 3 {
		return // old leader could not commit in the gap: invariant holds
	}
	got, ok := out.newLeaderLog[out.oldLeaderCommitted]
	if !ok || got.Term != out.committedEntry.Term || string(got.Data) != string(out.committedEntry.Data) {
		t.Errorf("SAFETY VIOLATION: old leader (term 1) committed index %d %q after the probe, "+
			"but the new leader %d (term %d) elected from the stale probe does not hold it (has %v); "+
			"the new leader assigned index %d to %q",
			out.oldLeaderCommitted, out.committedEntry.Data, out.newCfg.LeaderId, out.newCfg.Term, ok,
			out.reusedEntry.Index, out.reusedEntry.Data)
	}
}

// Negative control: same schedule, no write in the gap. The invariant holds.
func TestElectionGapNoCompetingWriteControl(t *testing.T) {
	out := runElectionGap(t, false, false)
	if out.oldLeaderCommitted != 3 {
		t.Fatalf("control: old leader committed %d, want 3", out.oldLeaderCommitted)
	}
	for i := uint64(1); i <= out.oldLeaderCommitted; i++ {
		if _, ok := out.newLeaderLog[i]; !ok {
			t.Fatalf("control: new leader lacks committed index %d", i)
		}
	}
	if out.reusedEntry.Index != 4 || out.reusedEntry.Term != out.newCfg.Term {
		t.Fatalf("control: next entry index=%d term=%d", out.reusedEntry.Index, out.reusedEntry.Term)
	}
}

// Control: the same write, but the probe is sampled after it. The election
// picks the replica that holds the committed entry, so the invariant holds.
// This isolates the probe-to-install gap as the failing edge.
func TestElectionGapProbeAfterWriteControl(t *testing.T) {
	out := runElectionGap(t, true, true)
	if out.oldLeaderCommitted != 4 {
		t.Fatalf("control: old leader committed %d, want 4", out.oldLeaderCommitted)
	}
	got, ok := out.newLeaderLog[4]
	if !ok || string(got.Data) != "s03-00004528" {
		t.Fatalf("control: new leader %d lacks committed index 4 (has %v %q)", out.newCfg.LeaderId, ok, got.Data)
	}
	if out.reusedEntry.Index != 5 {
		t.Fatalf("control: next entry index=%d, want 5", out.reusedEntry.Index)
	}
}
