package slot

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

// The old controller sample predates a real old-leader commit. A later durable
// fence must report that exact identity; a stale candidate cannot replace it.
func TestPebbleStaleSampleRetainsExactCommittedIdentityAndRecovery(t *testing.T) {
	type replica struct {
		disk *PebbleShardLogStorage
		node *raft.Node
		dir  string
	}
	open := func(dir string) *PebbleShardLogStorage {
		p := NewPebbleShardLogStorage(nil, dir, 1)
		require.NoError(t, p.Open())
		return p
	}
	nodes := map[uint64]*replica{}
	key := "1"
	cfg := types.Config{Replicas: []uint64{1, 2, 3}, Term: 7, InstalledTerm: 7, Leader: 1}
	for id := uint64(1); id <= 3; id++ {
		dir := t.TempDir()
		disk := open(dir)
		n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(key), raft.WithNodeId(id), raft.WithReplicas(cfg.Replicas), raft.WithSaveHardState(func(state types.HardState) error { return disk.SaveHardState(key, state) })))
		c := cfg.Clone()
		c.Role = types.RoleFollower
		if id == 1 {
			c.Role = types.RoleLeader
		}
		require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: c}))
		n.Ready()
		nodes[id] = &replica{disk, n, dir}
	}
	store := func(id uint64) []types.Log {
		r := nodes[id]
		var requests []types.Event
		for _, e := range r.node.Ready() {
			if e.Type == types.StoreReq {
				requests = append(requests, e)
			}
		}
		require.Len(t, requests, 1)
		req := requests[0]
		require.NoError(t, r.disk.AppendLogs(key, req.Logs, req.TermStartIndexInfo))
		require.NoError(t, r.node.Step(types.StoreResponse(req, types.Event{Type: types.StoreResp, Index: req.Logs[len(req.Logs)-1].Index, Logs: req.Logs, Reason: types.ReasonOk})))
		return req.Logs
	}
	for index := uint64(1); index <= 9; index++ {
		_, err := nodes[1].node.Admit([]types.Log{{Id: index, Data: []byte("prefix")}})
		require.NoError(t, err)
		logs := store(1)
		for _, id := range []uint64{2, 3} {
			require.NoError(t, nodes[id].node.Step(types.Event{Type: types.SyncResp, Term: 7, From: 1, Logs: logs, Reason: types.ReasonOk}))
			store(id)
		}
		require.NoError(t, nodes[1].node.Step(types.Event{Type: types.SyncReq, Term: 7, From: 3, Index: index + 1, StoredIndex: index + 1, LastLogTerm: 7}))
		for _, id := range []uint64{2, 3} {
			require.NoError(t, nodes[id].node.Step(types.Event{Type: types.Ping, Term: 7, From: 1, CommittedIndex: index}))
		}
	}
	sampled, err := nodes[2].disk.GetState(key)
	require.NoError(t, err)
	require.EqualValues(t, 9, sampled.LastLogIndex)
	_, err = nodes[1].node.Admit([]types.Log{{Id: 100, Data: []byte("accepted-before-stale-install")}})
	require.NoError(t, err)
	logs := store(1)
	original := logs[0]
	require.NoError(t, nodes[3].node.Step(types.Event{Type: types.SyncResp, Term: 7, From: 1, Logs: logs, Reason: types.ReasonOk}))
	store(3)
	require.NoError(t, nodes[1].node.Step(types.Event{Type: types.SyncReq, Term: 7, From: 3, Index: 11, StoredIndex: 11, LastLogTerm: 7}))
	require.EqualValues(t, 10, nodes[1].node.CommittedIndex())
	require.NoError(t, nodes[3].node.Step(types.Event{Type: types.Ping, Term: 7, From: 1, CommittedIndex: 10}))
	reports := map[uint64]types.ElectionReport{}
	for _, id := range []uint64{1, 3} {
		r := nodes[id]
		require.NoError(t, r.node.FenceElection(cfg, 8))
		state, err := r.disk.GetState(key)
		require.NoError(t, err)
		require.EqualValues(t, 10, state.HardState.CommittedFloor)
		reports[id] = types.ElectionReport{NodeID: id, Term: state.HardState.FenceTerm, Fenced: true, LastIndex: state.LastLogIndex, LastTerm: state.LastTerm, Floor: state.HardState.CommittedFloor}
	}
	// The old sample has no durable fence and is excluded, even if its index is forged high.
	reports[2] = types.ElectionReport{NodeID: 2, Term: 8, Fenced: false, LastIndex: 1000, LastTerm: 8}
	leader, err := types.SelectFencedLeader(cfg, 8, reports)
	require.NoError(t, err)
	require.EqualValues(t, 1, leader)
	for id, r := range nodes {
		next := cfg.Clone()
		next.Term = 8
		next.InstalledTerm = 8
		next.Role = types.RoleFollower
		if id == 1 {
			next.Role = types.RoleLeader
		}
		require.NoError(t, r.node.Step(types.Event{Type: types.ConfChange, Config: next}))
		r.node.Ready()
	}
	_, err = nodes[1].node.Admit([]types.Log{{Id: 200, Data: []byte("continued-after-safe-install")}})
	require.NoError(t, err)
	next := store(1)
	require.EqualValues(t, 11, next[0].Index)
	require.NoError(t, nodes[3].node.Step(types.Event{Type: types.SyncResp, From: 1, Term: 8, Logs: next, Reason: types.ReasonOk}))
	store(3)
	require.NoError(t, nodes[1].node.Step(types.Event{Type: types.SyncReq, From: 3, Term: 8, Index: 12, StoredIndex: 12, LastLogTerm: 8}))
	require.EqualValues(t, 11, nodes[1].node.CommittedIndex())
	for _, id := range []uint64{1, 3} {
		r := nodes[id]
		require.ErrorIs(t, r.disk.TruncateLogTo(key, 9), types.ErrTruncateBelowApplied)
		got, err := r.disk.GetLogs(key, 10, 11, 0)
		require.NoError(t, err)
		require.True(t, types.SameLogs([]types.Log{original}, got))
	}
	for id, r := range nodes {
		require.NoError(t, r.disk.Close())
		r.disk = open(r.dir)
		state, err := r.disk.GetState(key)
		require.NoError(t, err)
		if id == 1 || id == 3 {
			require.GreaterOrEqual(t, state.HardState.CommittedFloor, uint64(10))
			got, err := r.disk.GetLogs(key, 10, 11, 0)
			require.NoError(t, err)
			require.True(t, types.SameLogs([]types.Log{original}, got))
		}
		require.NoError(t, r.disk.Close())
	}
}

func TestPebbleTruncationAtomicallyKeepsOnlySurvivingTermRows(t *testing.T) {
	dir := t.TempDir()
	p := NewPebbleShardLogStorage(nil, dir, 1)
	require.NoError(t, p.Open())
	require.NoError(t, p.AppendLogs("1", []types.Log{{Index: 1, Term: 7, Id: 10, Data: []byte("survivor")}}, &types.TermStartIndexInfo{Term: 7, Index: 1}))
	require.NoError(t, p.AppendLogs("1", []types.Log{{Index: 2, Term: 8, Id: 20, Data: []byte("uncommitted")}}, &types.TermStartIndexInfo{Term: 8, Index: 2}))
	require.NoError(t, p.TruncateLogTo("1", 1))
	require.NoError(t, p.Close())
	p = NewPebbleShardLogStorage(nil, dir, 1)
	require.NoError(t, p.Open())
	defer p.Close()
	log, err := p.LastLog("1")
	require.NoError(t, err)
	require.EqualValues(t, 10, log.Id)
	row, err := p.GetTermStartIndex("1", 7)
	require.NoError(t, err)
	require.EqualValues(t, 1, row)
	row, err = p.GetTermStartIndex("1", 8)
	require.NoError(t, err)
	require.Zero(t, row)
}
