package cluster

// Deterministic slot-path diagnostic: can a stale slot leader A commit an entry
// after the controller has moved the slot to Candidate and sampled the
// replicas' last logs, so that the elected leader B lacks an acknowledged
// entry?
//
// Real code under test:
//   - pkg/raft/raft.Node for every slot replica and every clusterconfig
//     replica (replication, commit, ConfChange handling).
//   - Server.collectSlotLogInfo and electSlotLeader (election_slot.go).
//   - The election gate is evaluated with the same expression as
//     handleSlotElection: NodeConfigVersionFromLeader(id) is
//     clusterconfig raft GetReplicaLastLogIndex(id), compared with the
//     controller's applied config version (= applied log index).
//
// Simulated (not production code):
//   - Transport and storage: an in-memory log per node and a link matrix.
//     Partitions drop messages; there are no sleeps and no timers.
//   - The Candidate slot config is built by candidateSlotConfig, which mirrors
//     slot.Server.slotToConfig for Status=Candidate (Role=Candidate, Leader and
//     Term kept). Version is left at 0 exactly as slotToConfig does.
//   - The controller's "mark A offline + slot -> Candidate" decision is an
//     explicit administrative step; this test does not claim that production
//     reaches it automatically.

import (
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

const (
	diagA uint64 = 1 // old slot leader
	diagB uint64 = 2 // controller (clusterconfig leader), elected slot leader
	diagC uint64 = 3 // slot follower still connected to A
)

var diagReplicas = []uint64{diagA, diagB, diagC}

type diagReplica struct {
	n     *raft.Node
	disk  *slot.PebbleShardLogStorage
	key   string
	store map[uint64]rafttypes.Log
}

func (r *diagReplica) lastStored() uint64 {
	var last uint64
	for idx := range r.store {
		if idx > last {
			last = idx
		}
	}
	return last
}

type diagGroup struct {
	key     string
	nodes   map[uint64]*diagReplica
	blocked map[[2]uint64]bool
}

func newDiagGroup(t *testing.T, key string, leader uint64, term uint32) *diagGroup {
	g := &diagGroup{key: key, nodes: map[uint64]*diagReplica{}, blocked: map[[2]uint64]bool{}}
	for _, id := range diagReplicas {
		disk := slot.NewPebbleShardLogStorage(nil, t.TempDir(), 1)
		require.NoError(t, disk.Open())
		t.Cleanup(func() { require.NoError(t, disk.Close()) })
		opts := raft.NewOptions(
			raft.WithSaveHardState(func(st rafttypes.HardState) error { return disk.SaveHardState(key, st) }),
			raft.WithNodeId(id),
			raft.WithReplicas(diagReplicas),
			raft.WithElectionOn(false),
			raft.WithElectionInterval(1000),
			raft.WithAdvance(func() {}),
			raft.WithKey(key),
		)
		n := raft.NewNode(0, rafttypes.RaftState{}, opts)
		if id == leader {
			n.BecomeLeader(term)
		} else {
			n.BecomeFollower(term, leader)
		}
		require.NoError(t, n.Step(rafttypes.Event{Type: rafttypes.ConfChange, Config: rafttypes.Config{Replicas: diagReplicas, Term: term, Leader: leader, InstalledTerm: term, Role: n.Config().Role}}))
		n.Ready()
		g.nodes[id] = &diagReplica{n: n, disk: disk, key: key, store: map[uint64]rafttypes.Log{}}
	}
	return g
}

func (g *diagGroup) cut(a, b uint64) {
	g.blocked[[2]uint64{a, b}] = true
	g.blocked[[2]uint64{b, a}] = true
}

func (g *diagGroup) heal(a, b uint64) {
	delete(g.blocked, [2]uint64{a, b})
	delete(g.blocked, [2]uint64{b, a})
}

// round ticks every node once and delivers every pending event in a fixed
// node order. Messages across a cut link are dropped.
func (g *diagGroup) round() {
	for _, id := range diagReplicas {
		g.nodes[id].n.Tick()
	}
	for pass := 0; pass < 8; pass++ {
		progressed := false
		for _, id := range diagReplicas {
			r := g.nodes[id]
			for _, e := range r.n.Ready() {
				progressed = true
				if e.To == rafttypes.LocalNode {
					g.local(r, e)
					continue
				}
				if g.blocked[[2]uint64{id, e.To}] {
					continue
				}
				if dst := g.nodes[e.To]; dst != nil {
					_ = dst.n.Step(e)
				}
			}
		}
		if !progressed {
			return
		}
	}
}

func (g *diagGroup) local(r *diagReplica, e rafttypes.Event) {
	switch e.Type {
	case rafttypes.StoreReq:
		if err := r.disk.AppendLogs(r.key, e.Logs, e.TermStartIndexInfo); err != nil {
			panic(err)
		}
		for _, l := range e.Logs {
			r.store[l.Index] = l
		}
		_ = r.n.Step(rafttypes.StoreResponse(e, rafttypes.Event{Type: rafttypes.StoreResp, Index: e.Logs[len(e.Logs)-1].Index, Logs: e.Logs, Reason: rafttypes.ReasonOk}))
	case rafttypes.ApplyReq:
		if err := r.disk.SetAppliedIndex(r.key, e.EndIndex-1); err != nil {
			panic(err)
		}
		_ = r.n.Step(rafttypes.Event{Type: rafttypes.ApplyResp, Index: e.EndIndex - 1, Reason: rafttypes.ReasonOk})
	case rafttypes.GetLogsReq:
		var logs []rafttypes.Log
		for idx := e.Index; idx <= e.StoredIndex; idx++ {
			if l, ok := r.store[idx]; ok {
				logs = append(logs, l)
			}
		}
		_ = r.n.Step(rafttypes.Event{Type: rafttypes.GetLogsResp, From: e.From, To: e.From, Index: e.Index, Logs: logs, Reason: rafttypes.ReasonOk})
	case rafttypes.TruncateReq:
		if err := r.disk.TruncateLogTo(r.key, e.Index); err != nil {
			panic(err)
		}
		for idx := range r.store {
			if idx > e.Index {
				delete(r.store, idx)
			}
		}
		_ = r.n.Step(rafttypes.Event{Type: rafttypes.TruncateResp, Index: e.Index, Reason: rafttypes.ReasonOk})
	}
}

// until runs rounds until cond holds; the bound is a liveness guard, not a
// timing assumption.
func (g *diagGroup) until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		g.round()
	}
	require.FailNow(t, "barrier not reached", what)
}

// try runs rounds until cond holds and reports whether it did.
func (g *diagGroup) try(cond func() bool) bool {
	for i := 0; i < 200; i++ {
		if cond() {
			return true
		}
		g.round()
	}
	return cond()
}

func (g *diagGroup) propose(t *testing.T, leader uint64, data string) uint64 {
	t.Helper()
	n := g.nodes[leader].n
	require.True(t, n.IsLeader())
	require.NoError(t, n.Step(n.NewPropose([]byte(data))))
	return n.LastLogIndex()
}

// candidateSlotConfig mirrors slot.Server.slotToConfig for a slot whose
// Status is Candidate.
func candidateSlotConfig(st *types.Slot) rafttypes.Config {
	return rafttypes.Config{Replicas: st.Replicas, Learners: st.Learners, Term: st.Term, Leader: st.Leader, InstalledTerm: st.InstalledTerm, BoundaryTerm: st.BoundaryTerm, Role: rafttypes.RoleCandidate}
}

func normalSlotConfig(st *types.Slot, self uint64) rafttypes.Config {
	role := rafttypes.RoleFollower
	if st.Leader == self {
		role = rafttypes.RoleLeader
	}
	return rafttypes.Config{Replicas: st.Replicas, Learners: st.Learners, Term: st.Term, Leader: st.Leader, InstalledTerm: st.InstalledTerm, BoundaryTerm: st.BoundaryTerm, Role: role}
}

func sampleSlotInfo(slotId uint32, r *diagReplica) SlotInfo {
	return SlotInfo{
		SlotId:   slotId,
		LogIndex: r.n.LastLogIndex(),
		LogTerm:  r.n.LastLogTerm(),
		Term:     r.n.LastTerm(),
		Applied:  r.n.AppliedIndex(),
		ExtState: SlotInfoExtValid,
	}
}

type diagOutcome struct {
	gatePassed       bool
	gateReplicated   map[uint64]uint64
	gateVersion      uint64
	cRoleAfterCand   rafttypes.Role
	cLeaderAfterCand uint64
	sampled          map[uint64]SlotInfo
	competingIndex   uint64
	aCommitted       bool
	newLeader        uint64
	ackedOnA         []uint64
	missingOnNew     []uint64
}

type diagMode int

const (
	modeCompetingWrite   diagMode = iota // write after sample, before install
	modeNoWrite                          // control: no competing write
	modeSampleAfterWrite                 // control: sample after the write commits
)

func runSlotCandidateDiag(t *testing.T, mode diagMode) diagOutcome {
	const slotId uint32 = 7
	out := diagOutcome{gateReplicated: map[uint64]uint64{}, sampled: map[uint64]SlotInfo{}}

	slotG := newDiagGroup(t, "slot7", diagA, 1)
	ccG := newDiagGroup(t, "clusterconfig", diagB, 1)

	// Baseline: slot entries 1..2 on all replicas; one clusterconfig entry.
	for i := 1; i <= 2; i++ {
		idx := slotG.propose(t, diagA, fmt.Sprintf("base-%d", i))
		slotG.until(t, "baseline replicated", func() bool {
			return slotG.nodes[diagB].n.LastLogIndex() >= idx && slotG.nodes[diagC].n.LastLogIndex() >= idx &&
				slotG.nodes[diagA].n.CommittedIndex() >= idx
		})
	}
	ccIdx := ccG.propose(t, diagB, "cc-base")
	ccG.until(t, "cc baseline", func() bool { return ccG.nodes[diagB].n.AppliedIndex() >= ccIdx })

	// Entry 3 reaches B but not yet C (C is momentarily cut from A).
	slotG.cut(diagA, diagC)
	idx3 := slotG.propose(t, diagA, "e3")
	slotG.until(t, "e3 on B, committed by A+B", func() bool {
		return slotG.nodes[diagB].n.LastLogIndex() >= idx3 && slotG.nodes[diagA].n.CommittedIndex() >= idx3
	})
	require.Equal(t, idx3-1, slotG.nodes[diagC].n.LastLogIndex())

	// Controller B perceives A offline: controller<->A cut on clusterconfig,
	// A<->B cut on the slot. A<->C stays cut until the sample barrier so the
	// sample is B=3, C=2 and the election is deterministic (no tie).
	ccG.cut(diagB, diagA)
	ccG.cut(diagC, diagA)
	slotG.cut(diagA, diagB)

	// Controller proposes "A offline, slot 7 -> Candidate" and waits for the
	// online-replica gate exactly as handleSlotElection evaluates it.
	candIdx := ccG.propose(t, diagB, "slot7-candidate")
	ccG.until(t, "candidate config applied on controller", func() bool { return ccG.nodes[diagB].n.AppliedIndex() >= candIdx })
	out.gateVersion = ccG.nodes[diagB].n.AppliedIndex() // clusterconfig Version = applied log index
	ccG.until(t, "online replicas replicated candidate config", func() bool {
		return ccG.nodes[diagB].n.GetReplicaLastLogIndex(diagC) >= out.gateVersion
	})
	out.gatePassed = true
	for _, id := range []uint64{diagB, diagC} { // A is offline: skipped by the gate
		v := ccG.nodes[diagB].n.GetReplicaLastLogIndex(id)
		out.gateReplicated[id] = v
		if v < out.gateVersion {
			out.gatePassed = false
		}
	}
	require.Less(t, ccG.nodes[diagB].n.GetReplicaLastLogIndex(diagA), out.gateVersion, "A must be stale on clusterconfig")

	// B and C install the Candidate slot config; A does not receive it.
	candSlot := &types.Slot{InstalledTerm: 1, Id: slotId, Leader: diagA, Term: 1, Replicas: diagReplicas, Status: types.SlotStatus_SlotStatusCandidate}
	for _, id := range []uint64{diagB, diagC} {
		require.NoError(t, slotG.nodes[id].n.Step(rafttypes.Event{Type: rafttypes.ConfChange, Config: candidateSlotConfig(candSlot)}))
	}
	slotG.round()
	out.cRoleAfterCand = slotG.nodes[diagC].n.Config().Role
	out.cLeaderAfterCand = slotG.nodes[diagC].n.LeaderId()

	competing := func() {
		out.competingIndex = slotG.propose(t, diagA, "e4-competing")
		out.aCommitted = slotG.try(func() bool { return slotG.nodes[diagA].n.CommittedIndex() >= out.competingIndex })
	}

	if mode == modeSampleAfterWrite {
		slotG.heal(diagA, diagC)
		competing()
	}

	// Fence the applied voters before sampling; the storage is fresh Pebble.
	reports := map[uint64]rafttypes.ElectionReport{}
	for _, id := range []uint64{diagB, diagC} {
		r := slotG.nodes[id]
		require.NoError(t, r.n.FenceElection(candidateSlotConfig(candSlot), 2))
		state, err := r.disk.GetState(r.key)
		require.NoError(t, err)
		reports[id] = rafttypes.ElectionReport{NodeID: id, Term: state.HardState.FenceTerm, Fenced: state.HardState.FenceTerm == 2, LastIndex: state.LastLogIndex, LastTerm: state.LastTerm, Floor: state.HardState.CommittedFloor}
	}
	// Controller samples last logs from the online replicas (B, C).
	for _, id := range []uint64{diagB, diagC} {
		out.sampled[id] = sampleSlotInfo(slotId, slotG.nodes[id])
	}

	// Barrier: sample taken. A<->C reconnects; C still holds the Candidate
	// config and keeps pulling from A.
	slotG.heal(diagA, diagC)
	if mode == modeCompetingWrite {
		competing()
	}
	// Catch-up waits until the conditional install releases the fenced voters.

	out.cRoleAfterCand = slotG.nodes[diagC].n.Config().Role
	out.cLeaderAfterCand = slotG.nodes[diagC].n.LeaderId()

	// Use the production applied-voter selection, not the historical probe selector.
	var err error
	out.newLeader, err = rafttypes.SelectFencedLeader(slotElectionConfig(candSlot), 2, reports)
	require.NoError(t, err)
	require.NotZero(t, out.newLeader)

	// Install the elected leader at Term+1 on the online replicas.
	newSlot := candSlot.Clone()
	newSlot.Leader = out.newLeader
	newSlot.Term++
	newSlot.InstalledTerm = newSlot.Term
	newSlot.Status = types.SlotStatus_SlotStatusNormal
	installed, ok := clusterconfig.ConditionalSlotInstall(candSlot, candSlot.Clone(), newSlot)
	require.True(t, ok)
	newSlot = installed
	for _, id := range []uint64{diagB, diagC} {
		require.NoError(t, slotG.nodes[id].n.Step(rafttypes.Event{Type: rafttypes.ConfChange, Config: normalSlotConfig(newSlot, id)}))
	}
	slotG.round()

	// A new-term proposal must actually commit, so safety cannot pass by refusing all work.
	next := slotG.propose(t, out.newLeader, "continued-after-fence")
	slotG.until(t, "new leader continued commit", func() bool { return slotG.nodes[out.newLeader].n.CommittedIndex() >= next })
	// Acked = committed on old leader A.
	for idx := uint64(1); idx <= slotG.nodes[diagA].n.CommittedIndex(); idx++ {
		out.ackedOnA = append(out.ackedOnA, idx)
		al, ok1 := slotG.nodes[diagA].store[idx]
		nl, ok2 := slotG.nodes[out.newLeader].store[idx]
		if !ok1 || !ok2 || string(al.Data) != string(nl.Data) || al.Term != nl.Term || al.Id != nl.Id {
			out.missingOnNew = append(out.missingOnNew, idx)
		}
	}
	t.Logf("gate: version=%d replicated=%v passed=%v | C after Candidate: role=%s leader=%d | sampled=%+v | competing idx=%d aCommitted=%v (A committed=%d) | newLeader=%d | ackedOnA=%v missingOnNewLeader=%v",
		out.gateVersion, out.gateReplicated, out.gatePassed, out.cRoleAfterCand, out.cLeaderAfterCand, out.sampled,
		out.competingIndex, out.aCommitted, slotG.nodes[diagA].n.CommittedIndex(), out.newLeader, out.ackedOnA, out.missingOnNew)
	return out
}

// Safety: the new leader retains every entry acknowledged by A, OR A could
// not commit the competing write.
func TestSlotCandidateOldLeaderCommitSafety(t *testing.T) {
	out := runSlotCandidateDiag(t, modeCompetingWrite)
	require.True(t, out.gatePassed, "online-replica clusterconfig replicated-index gate")
	require.Empty(t, out.missingOnNew,
		"SAFETY: old leader A committed %v with C's ack after the Candidate sample; new leader %d lacks it (aCommitted=%v)",
		out.missingOnNew, out.newLeader, out.aCommitted)
}

func TestSlotCandidateOldLeaderControlNoCompetingWrite(t *testing.T) {
	out := runSlotCandidateDiag(t, modeNoWrite)
	require.True(t, out.gatePassed)
	require.Equal(t, diagB, out.newLeader)
	require.Empty(t, out.missingOnNew)
}

func TestSlotCandidateOldLeaderControlSampleAfterWrite(t *testing.T) {
	out := runSlotCandidateDiag(t, modeSampleAfterWrite)
	require.True(t, out.gatePassed)
	require.True(t, out.aCommitted)
	require.Equal(t, diagC, out.newLeader)
	require.Empty(t, out.missingOnNew)
}

// Separate source defect: the election quorum comes from the global
// SlotMaxReplicaCount, not from the slot's own voter set. A slot with five
// voters elects a leader from two responders when SlotMaxReplicaCount is 3.
func TestSlotElectionQuorumUsesGlobalReplicaCount(t *testing.T) {
	s := testElectionServer(true) // SlotMaxReplicaCount = 3 -> quorum 2
	st := &types.Slot{Id: 9, Leader: 1, Term: 1, Replicas: []uint64{1, 2, 3, 4, 5}, Status: types.SlotStatus_SlotStatusCandidate}
	infos, err := s.collectSlotLogInfo([]*SlotLogInfoResp{
		{NodeId: 2, Slots: []SlotInfo{{SlotId: 9, LogIndex: 5, LogTerm: 1, Term: 1, Applied: 5}}},
		{NodeId: 3, Slots: []SlotInfo{{SlotId: 9, LogIndex: 4, LogTerm: 1, Term: 1, Applied: 4}}},
	})
	require.NoError(t, err)
	got := electSlotLeader(infos, st.Id, st)
	require.Zero(t, got, "2 of 5 voters is not a majority of the slot's replicas, yet leader %d was elected", got)
}
