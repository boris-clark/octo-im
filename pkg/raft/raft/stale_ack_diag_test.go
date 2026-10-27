package raft

// Deterministic stale-index ACK diagnostic (Issue87 / XIN-1965).
//
// Question: can a leader at a new term commit an entry that fewer than a
// quorum of voters actually hold, because a follower's SyncReq.StoredIndex
// is counted before the follower's divergent tail has been negotiated away?
//
// Real code under test (unmodified, package-internal):
//   - raft.Node for every replica: ConfChange install (switchConfig),
//     follower sync (sendSyncReq), stepLeader SyncReq -> updateSyncInfo ->
//     updateLeaderCommittedIndex -> committedIndexForLeader, StoreResp,
//     ResumeReplication, truncation (requestTruncate / TruncateResp) and the
//     storage-verdict quarantine (localDivergence / stepDiverged).
//   - Ready(): including the per-batch lastTermStartIndex update that feeds
//     SyncReq.LastLogTerm.
//
// Simulated (seams, not production code):
//   - S1 transport: an in-test FIFO; messages can be held, dropped or
//     re-delivered. There are no sleeps, timers or goroutines.
//   - S2 storage: an in-memory map per replica. GetLogsReq mirrors
//     raftgroup.handleGetLogsReq and getTrunctLogIndex (handle.go:85-150,
//     273-324) over that map. Holding a GetLogsReq models the off-loop
//     truncation decision still being pending.
//   - S3 install: the new leader is installed with an ordinary
//     ConfChange{Term:U+1, Leader:X, Version:v} stepped into every live
//     replica. The controller's choice of X is an explicit step; this test
//     does not claim production reaches it automatically.
//   - S4 (counterexample 2 only): storage refusing y's truncation with a
//     Diverged verdict. Current storage refuses only truncation below the
//     applied index; refusing a legacy tail is the proposed D5 behaviour, so
//     that sub-test is labelled as proposal emulation.
//
// Every commit observation reads the replicas' stored entries (index, term
// and bytes) at the moment the leader's committedIndex advances. Nothing is
// inferred from sync bookkeeping.
//
// Run: XIN2539_DIAG=1 go test -race -count=1 -run TestStaleAck ./pkg/raft/raft/
// Expected on the current code: the safety tests FAIL (RED). That is the
// finding, not a regression.

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

const (
	saL uint64 = 1 // old leader at term U; down for the whole test
	saX uint64 = 2 // installed leader at U+1
	saY uint64 = 3 // holder of the old uncommitted tail (U,11..12)
	saZ uint64 = 4
	saW uint64 = 5

	saU uint32 = 7 // old leader term
)

func saSkip(t *testing.T) {
	t.Helper()
	if os.Getenv("XIN2539_DIAG") == "" {
		t.Skip("diagnostic: set XIN2539_DIAG=1")
	}
}

type saReplica struct {
	id    uint64
	n     *Node
	store map[uint64]types.Log
}

type saCommit struct {
	leader  uint64
	from    uint64
	to      uint64
	holders map[uint64][]uint64 // index -> voters storing the leader's exact entry
	quorum  int
	via     string
}

type saCluster struct {
	t        *testing.T
	ids      []uint64
	voters   []uint64
	r        map[uint64]*saReplica
	down     map[uint64]bool
	cut      map[uint64]bool // node isolated in both directions (messages dropped)
	net      []types.Event
	held     []saHeld
	holdGet  map[uint64]bool // leader-side GetLogsReq for this follower is held
	refuseTr map[uint64]bool // S4: storage refuses this replica's truncation
	commits  []saCommit
	lastCI   map[uint64]uint64
	via      string
}

type saHeld struct {
	owner uint64
	e     types.Event
}

type saSpec struct {
	id   uint64
	logs []types.Log // stored history, contiguous from 1
	ci   uint64      // committed == applied
}

func saLog(term uint32, idx uint64, data string) types.Log {
	return types.Log{Term: term, Index: idx, Data: []byte(data)}
}

// saHistory returns (U-1, 1..9) followed by extra entries.
func saHistory(extra ...types.Log) []types.Log {
	var logs []types.Log
	for i := uint64(1); i <= 9; i++ {
		logs = append(logs, saLog(saU-1, i, fmt.Sprintf("h%d", i)))
	}
	return append(logs, extra...)
}

func newSaCluster(t *testing.T, voters []uint64, specs []saSpec) *saCluster {
	g := &saCluster{t: t, voters: voters, r: map[uint64]*saReplica{}, down: map[uint64]bool{},
		cut: map[uint64]bool{}, holdGet: map[uint64]bool{}, refuseTr: map[uint64]bool{}, lastCI: map[uint64]uint64{}}
	for _, s := range specs {
		g.ids = append(g.ids, s.id)
		store := map[uint64]types.Log{}
		var lastTerm uint32
		var lastIdx, termStart uint64
		for _, l := range s.logs {
			store[l.Index] = l
			if l.Term != lastTerm {
				termStart = l.Index
			}
			lastTerm, lastIdx = l.Term, l.Index
		}
		opts := NewOptions(
			WithNodeId(s.id),
			WithReplicas(voters),
			WithElectionOn(false),
			WithElectionInterval(1000),
			WithAdvance(func() {}),
			WithKey("xin2539"),
		)
		n := NewNode(termStart, types.RaftState{
			HardState:    types.HardState{Term: saU},
			LastLogIndex: lastIdx,
			LastTerm:     lastTerm,
			AppliedIndex: s.ci,
		}, opts)
		n.Ready()
		g.r[s.id] = &saReplica{id: s.id, n: n, store: store}
		g.lastCI[s.id] = n.queue.committedIndex
	}
	return g
}

func (g *saCluster) quorum() int { return len(g.voters)/2 + 1 }

// install steps the same ConfChange into every live replica (seam S3).
func (g *saCluster) install(leader uint64, term uint32, version uint64) {
	cfg := types.Config{Replicas: append([]uint64(nil), g.voters...), Leader: leader, Term: term, Version: version}
	for _, id := range g.ids {
		if g.down[id] {
			continue
		}
		if err := g.r[id].n.Step(types.Event{Type: types.ConfChange, Config: cfg}); err != nil {
			g.t.Fatalf("install on %d: %v", id, err)
		}
	}
	g.pump()
}

// observe records every leader commit advance with the actual holders.
func (g *saCluster) observe() {
	for _, id := range g.ids {
		r := g.r[id]
		ci := r.n.queue.committedIndex
		prev := g.lastCI[id]
		g.lastCI[id] = ci
		if !r.n.IsLeader() || ci <= prev {
			continue
		}
		c := saCommit{leader: id, from: prev, to: ci, holders: map[uint64][]uint64{}, quorum: g.quorum(), via: g.via}
		for idx := prev + 1; idx <= ci; idx++ {
			want, ok := r.store[idx]
			if !ok {
				g.t.Fatalf("leader %d committed %d it has not stored", id, idx)
			}
			for _, v := range g.voters {
				rr := g.r[v]
				if rr == nil {
					continue
				}
				got, ok := rr.store[idx]
				if ok && got.Term == want.Term && got.Index == want.Index && bytes.Equal(got.Data, want.Data) {
					c.holders[idx] = append(c.holders[idx], v)
				}
			}
		}
		g.commits = append(g.commits, c)
	}
}

func (g *saCluster) step(id uint64, e types.Event) {
	if err := g.r[id].n.Step(e); err != nil {
		g.t.Logf("step %d %s: %v", id, e.Type, err)
	}
	g.observe()
}

func (g *saCluster) drain(id uint64) bool {
	r := g.r[id]
	evs := r.n.Ready()
	g.observe()
	for _, e := range evs {
		if e.To == types.LocalNode {
			if e.Type == types.GetLogsReq && g.holdGet[e.From] {
				g.held = append(g.held, saHeld{owner: id, e: e})
				continue
			}
			g.local(r, e)
			continue
		}
		if e.Type == types.SyncReq || e.Type == types.SyncResp || e.Type == types.Ping || e.Type == types.NotifySync ||
			e.Type == types.ConfigReq || e.Type == types.ConfigResp {
			if g.down[e.To] || g.cut[e.To] || g.cut[id] {
				continue
			}
		}
		g.net = append(g.net, e)
	}
	return len(evs) > 0
}

// pump drains every live replica and delivers queued messages until quiet.
func (g *saCluster) pump() {
	for pass := 0; pass < 200; pass++ {
		progressed := false
		for _, id := range g.ids {
			if !g.down[id] && g.drain(id) {
				progressed = true
			}
		}
		q := g.net
		g.net = nil
		for _, e := range q {
			if g.down[e.To] || g.r[e.To] == nil || g.cut[e.To] || g.cut[e.From] {
				continue
			}
			progressed = true
			g.step(e.To, e)
		}
		if !progressed {
			return
		}
	}
	g.t.Fatalf("pump did not quiesce")
}

func (g *saCluster) tick(id uint64, n int) {
	for i := 0; i < n; i++ {
		g.r[id].n.Tick()
	}
}

// releaseGet processes held GetLogsReq for follower f (truncation decision lands).
func (g *saCluster) releaseGet(f uint64) {
	delete(g.holdGet, f)
	var keep []saHeld
	for _, h := range g.held {
		if h.e.From == f {
			g.local(g.r[h.owner], h.e)
		} else {
			keep = append(keep, h)
		}
	}
	g.held = keep
	g.pump()
}

func (g *saCluster) local(r *saReplica, e types.Event) {
	switch e.Type {
	case types.StoreReq:
		for _, l := range e.Logs {
			r.store[l.Index] = l
		}
		g.step(r.id, types.Event{Type: types.StoreResp, Index: e.Logs[len(e.Logs)-1].Index, Reason: types.ReasonOk})
	case types.ApplyReq:
		g.step(r.id, types.Event{Type: types.ApplyResp, Index: e.EndIndex - 1, Reason: types.ReasonOk})
	case types.GetLogsReq:
		g.getLogs(r, e)
	case types.TruncateReq:
		if g.refuseTr[r.id] {
			g.step(r.id, types.Event{Type: types.TruncateResp, Index: e.Index, Reason: types.ReasonError}.
				WithLocalStorageVerdict(types.StorageVerdictDiverged))
			return
		}
		for idx := range r.store {
			if idx > e.Index {
				delete(r.store, idx)
			}
		}
		g.step(r.id, types.Event{Type: types.TruncateResp, Index: e.Index, Reason: types.ReasonOk})
	}
}

// getLogs mirrors raftgroup.handleGetLogsReq over the in-memory store (S2).
func (g *saCluster) getLogs(r *saReplica, e types.Event) {
	lastIndex := r.n.LastLogIndex()
	if e.Reason != types.ReasonOnlySync {
		if tr := saTruncIndex(r.store, lastIndex, e); tr > 0 {
			g.step(r.id, types.Event{To: e.From, Type: types.GetLogsResp, Index: tr, Reason: types.ReasonTruncate})
			return
		}
		if e.Index > e.StoredIndex {
			g.step(r.id, types.Event{To: e.From, Type: types.GetLogsResp, Index: e.Index, Reason: types.ReasonOk})
			return
		}
	}
	var logs []types.Log
	for idx := e.Index; idx <= e.StoredIndex; idx++ {
		if l, ok := r.store[idx]; ok {
			logs = append(logs, l)
		}
	}
	g.step(r.id, types.Event{To: e.From, Type: types.GetLogsResp, Index: e.Index, Logs: logs, Reason: types.ReasonOk})
}

// saTruncIndex mirrors raftgroup.getTrunctLogIndex (handle.go:273-324).
func saTruncIndex(store map[uint64]types.Log, lastIndex uint64, e types.Event) uint64 {
	var idxs []uint64
	for i := range store {
		idxs = append(idxs, i)
	}
	sort.Slice(idxs, func(a, b int) bool { return idxs[a] < idxs[b] })
	var leaderLastTerm uint32
	if len(idxs) > 0 {
		leaderLastTerm = store[idxs[len(idxs)-1]].Term
	}
	termStart := func(term uint32) uint64 {
		for _, i := range idxs {
			if store[i].Term == term {
				return i
			}
		}
		return 0
	}
	if e.LastLogTerm == 0 {
		return 0
	}
	if e.LastLogTerm == leaderLastTerm {
		if e.Index > 0 && e.Index-1 > lastIndex {
			return lastIndex
		}
		return 0
	}
	var ts uint64
	if e.LastLogTerm < leaderLastTerm {
		var next uint32
		for _, i := range idxs {
			if t := store[i].Term; t >= e.LastLogTerm+1 && (next == 0 || t < next) {
				next = t
			}
		}
		ts = termStart(next)
	} else {
		ts = termStart(leaderLastTerm)
	}
	if ts > 0 {
		return ts - 1
	}
	return 0
}

func (g *saCluster) propose(id uint64, data ...string) {
	r := g.r[id]
	var logs []types.Log
	next := r.n.queue.lastLogIndex + 1
	for i, d := range data {
		logs = append(logs, types.Log{Term: r.n.cfg.Term, Index: next + uint64(i), Data: []byte(d)})
	}
	g.step(id, types.Event{Type: types.Propose, Logs: logs})
	g.pump()
}

// syncFrom makes follower f emit its periodic SyncReq through tickSync.
func (g *saCluster) syncFrom(f uint64) {
	g.tick(f, g.r[f].n.opts.SyncIntervalTick)
	g.pump()
}

func (g *saCluster) capture(f uint64) types.Event {
	g.tick(f, g.r[f].n.opts.SyncIntervalTick)
	var out *types.Event
	for _, e := range g.r[f].n.Ready() {
		if e.Type == types.SyncReq && out == nil {
			e := e
			out = &e
			continue
		}
		if e.To == types.LocalNode {
			g.local(g.r[f], e)
		} else {
			g.net = append(g.net, e)
		}
	}
	if out == nil {
		g.t.Fatalf("follower %d emitted no SyncReq", f)
	}
	return *out
}

// assertQuorumHolders is the safety property: every committed index is
// stored, with the leader's exact bytes, by at least a quorum of voters.
func (g *saCluster) assertQuorumHolders(t *testing.T) {
	t.Helper()
	for _, c := range g.commits {
		for idx := c.from + 1; idx <= c.to; idx++ {
			h := c.holders[idx]
			if len(h) < c.quorum {
				t.Errorf("SAFETY RED: leader %d committed index %d (term %d, %q) via %s with actual holders %v (%d < quorum %d)",
					c.leader, idx, g.r[c.leader].store[idx].Term, g.r[c.leader].store[idx].Data, c.via, h, len(h), c.quorum)
			}
		}
	}
}

func (g *saCluster) dump(t *testing.T) {
	t.Helper()
	for _, c := range g.commits {
		t.Logf("commit leader=%d (%d,%d] via=%q holders=%v quorum=%d", c.leader, c.from, c.to, c.via, c.holders, c.quorum)
	}
	for _, id := range g.ids {
		r := g.r[id]
		t.Logf("node %d: last=%d stored=%d committed=%d lastTermStart=%+v diverged=%v store=%s",
			id, r.n.queue.lastLogIndex, r.n.queue.storedIndex, r.n.queue.committedIndex, r.n.lastTermStartIndex, r.n.Diverged(), saStoreString(r.store))
	}
}

func saStoreString(store map[uint64]types.Log) string {
	var idxs []uint64
	for i := range store {
		if i >= 9 {
			idxs = append(idxs, i)
		}
	}
	sort.Slice(idxs, func(a, b int) bool { return idxs[a] < idxs[b] })
	s := ""
	for _, i := range idxs {
		s += fmt.Sprintf("(%d,%d,%s)", store[i].Term, i, store[i].Data)
	}
	return s
}

// five builds counterexample 1: R=5 {L,X,y,z,w}, L down, X holds (U,10),
// y holds the uncommitted (U,11..12), z holds (U,10), w lags at (U-1,9).
func five(t *testing.T) *saCluster {
	voters := []uint64{saL, saX, saY, saZ, saW}
	g := newSaCluster(t, voters, []saSpec{
		{id: saX, logs: saHistory(saLog(saU, 10, "c10")), ci: 10},
		{id: saY, logs: saHistory(saLog(saU, 10, "c10"), saLog(saU, 11, "old11"), saLog(saU, 12, "old12")), ci: 10},
		{id: saZ, logs: saHistory(saLog(saU, 10, "c10")), ci: 10},
		{id: saW, logs: saHistory(), ci: 9},
	})
	g.down[saL] = true
	return g
}

// TestStaleAckFiveVoterCommitsWithoutQuorum is counterexample 1 on the
// production ACK path: y's pre-negotiation SyncReq{StoredIndex:13,
// LastLogTerm:U} is counted, then z's real ack commits (U+1,11..12).
func TestStaleAckFiveVoterCommitsWithoutQuorum(t *testing.T) {
	saSkip(t)
	g := five(t)
	g.cut[saW] = true // w partitioned: only z can legitimately ack
	g.holdGet[saY] = true
	g.install(saX, saU+1, 1)

	stale := g.capture(saY)
	if stale.StoredIndex != 13 || stale.LastLogTerm != saU || stale.Term != saU+1 {
		t.Fatalf("precondition: y SyncReq = %+v", stale)
	}
	g.via = "SyncReq(z) after stale SyncReq(y)"
	g.step(saX, stale)
	g.pump()
	if got := g.r[saX].n.replicaSync[saY].StoredIndex; got != 13 {
		t.Fatalf("precondition: leader recorded y StoredIndex=%d", got)
	}

	g.propose(saX, "new11", "new12")
	g.syncFrom(saZ)
	g.syncFrom(saZ)

	g.dump(t)
	t.Logf("leader committedIndex=%d (safe bound with only {X,z} holding 11..12 is 10)", g.r[saX].n.queue.committedIndex)
	g.assertQuorumHolders(t)
}

// TestStaleAckReorderedDuplicateAfterTruncation: y negotiates and truncates,
// then a delayed copy of its stale SyncReq re-inflates the match index.
// Requires transport-level reorder/duplication (seam S1).
func TestStaleAckReorderedDuplicateAfterTruncation(t *testing.T) {
	saSkip(t)
	g := five(t)
	g.cut[saW] = true
	g.install(saX, saU+1, 1)

	stale := g.capture(saY)
	g.step(saX, stale)
	g.pump() // truncation decision lands; y truncates to 10, then OnlySync
	if _, ok := g.r[saY].store[11]; ok {
		t.Fatalf("precondition: y still holds index 11: %s", saStoreString(g.r[saY].store))
	}
	g.cut[saY] = true // y now partitioned; only its delayed duplicate arrives
	g.via = "SyncReq(z) after reordered duplicate SyncReq(y)"
	g.step(saX, stale)
	g.pump()
	g.propose(saX, "new11", "new12")
	g.syncFrom(saZ)
	g.syncFrom(saZ)
	g.dump(t)
	g.assertQuorumHolders(t)
}

// TestStaleAckRetainedUntilReset: SyncTick never expires a recorded
// StoredIndex; only reset()/switchConfig clears it.
func TestStaleAckRetainedUntilReset(t *testing.T) {
	saSkip(t)
	g := five(t)
	g.cut[saW] = true
	g.cut[saZ] = true
	g.holdGet[saY] = true
	g.install(saX, saU+1, 1)
	g.step(saX, g.capture(saY))
	g.pump()
	g.cut[saY] = true
	g.tick(saX, 500)
	g.pump()
	got := g.r[saX].n.replicaSync[saY].StoredIndex
	t.Logf("after 500 leader ticks with y silent: replicaSync[y].StoredIndex=%d", got)
	if got != 13 {
		t.Errorf("expected retention of stale StoredIndex 13, got %d", got)
	}
	g.install(saX, saU+1, 2) // version-only ConfChange -> switchConfig clears replicaSync
	if si := g.r[saX].n.replicaSync[saY]; si != nil && si.StoredIndex != 0 {
		t.Errorf("expected switchConfig to clear y's StoredIndex, got %d", si.StoredIndex)
	}
}

// TestStaleAckControlNoStaleAck: same topology, y silent. X commits only
// once z and w both hold (U+1,11..12). w receives one mixed-term batch
// (U,10),(U+1,11),(U+1,12), so Ready records lastTermStartIndex.Term=U and
// w's SyncReq.LastLogTerm stays U although it holds the U+1 entries.
func TestStaleAckControlNoStaleAck(t *testing.T) {
	saSkip(t)
	g := five(t)
	g.cut[saY] = true
	g.install(saX, saU+1, 1)
	g.via = "SyncReq(z,w) legit"
	g.propose(saX, "new11", "new12")
	for i := 0; i < 3; i++ {
		g.syncFrom(saZ)
		g.syncFrom(saW)
	}
	g.dump(t)
	w := g.r[saW]
	t.Logf("w lastTermStartIndex=%+v (holds (U+1,12)=%v)", w.n.lastTermStartIndex, w.store[12].Term == saU+1)
	g.assertQuorumHolders(t)
	if ci := g.r[saX].n.queue.committedIndex; ci != 12 {
		t.Errorf("LIVENESS: legit quorum {X,z,w} holds 11..12 but leader committedIndex=%d", ci)
	}
	// Follow-up write: w's next batch is single-term U+1 and refreshes LastLogTerm.
	g.propose(saX, "new13")
	for i := 0; i < 3; i++ {
		g.syncFrom(saZ)
		g.syncFrom(saW)
	}
	g.assertQuorumHolders(t)
	if ci := g.r[saX].n.queue.committedIndex; ci != 13 {
		t.Errorf("LIVENESS after follow-up write: committedIndex=%d want 13", ci)
	}
	t.Logf("after follow-up write: w lastTermStartIndex=%+v leader committed=%d", w.n.lastTermStartIndex, g.r[saX].n.queue.committedIndex)
}

// TestStaleAckControlNegotiatedNewTermAck: y's stale SyncReq is answered
// with a truncation first; y's later ack is a real new-term ack.
func TestStaleAckControlNegotiatedNewTermAck(t *testing.T) {
	saSkip(t)
	g := five(t)
	g.cut[saW] = true
	g.install(saX, saU+1, 1)
	g.syncFrom(saY) // truncation lands immediately
	g.via = "SyncReq(z,y) after negotiation"
	g.propose(saX, "new11", "new12")
	for i := 0; i < 3; i++ {
		g.syncFrom(saZ)
		g.syncFrom(saY)
	}
	g.dump(t)
	g.assertQuorumHolders(t)
	if ci := g.r[saX].n.queue.committedIndex; ci != 12 {
		t.Errorf("LIVENESS: {X,y,z} hold 11..12 after negotiation but committedIndex=%d", ci)
	}
}

// three builds counterexample 2: R=3 {A,y,z}; A holds (T,10), y holds
// (T,11..12), z holds (T,10). The install targets A at T+1.
func three(t *testing.T) *saCluster {
	const a, y, z = saX, saY, saZ
	return newSaCluster(t, []uint64{a, y, z}, []saSpec{
		{id: a, logs: saHistory(saLog(saU, 10, "c10")), ci: 10},
		{id: y, logs: saHistory(saLog(saU, 10, "c10"), saLog(saU, 11, "old11"), saLog(saU, 12, "old12")), ci: 10},
		{id: z, logs: saHistory(saLog(saU, 10, "c10")), ci: 10},
	})
}

// TestStaleAckThreeVoterLegacyQuarantine is counterexample 2. Sub-tests
// cover the commit consumers that read replicaSync: a heartbeat SyncReq
// from z that carries no new entry, and ResumeReplication with no SyncReq.
// The "quarantine" sub-test emulates the proposed D5 refusal (seam S4).
func TestStaleAckThreeVoterLegacyQuarantine(t *testing.T) {
	saSkip(t)
	for _, tc := range []string{"z-heartbeat", "resume-replication", "quarantine"} {
		t.Run(tc, func(t *testing.T) {
			g := three(t)
			g.cut[saZ] = true
			g.holdGet[saY] = true
			g.install(saX, saU+1, 1)
			g.step(saX, g.capture(saY))
			g.pump()
			g.propose(saX, "new11", "new12")
			switch tc {
			case "z-heartbeat":
				delete(g.cut, saZ)
				g.via = "SyncReq(z) holding only (T,10)"
				g.syncFrom(saZ)
			case "resume-replication":
				g.via = "ResumeReplication"
				g.r[saX].n.ResumeReplication()
				g.observe()
				g.pump()
			case "quarantine":
				g.refuseTr[saY] = true
				g.releaseGet(saY) // y told to truncate -> storage refuses -> quarantined
				if !g.r[saY].n.Diverged() {
					t.Fatalf("precondition: y not quarantined")
				}
				g.tick(saX, 500)
				g.tick(saY, 50)
				g.pump()
				t.Logf("y quarantined; leader replicaSync[y].StoredIndex=%d after 500 ticks", g.r[saX].n.replicaSync[saY].StoredIndex)
				delete(g.cut, saZ)
				g.via = "SyncReq(z) with quarantined y's match retained"
				g.syncFrom(saZ)
			}
			g.dump(t)
			g.assertQuorumHolders(t)
		})
	}
}
