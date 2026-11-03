package raft

import (
	"bytes"
	"os"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// This adapter is replaced by a raw response only in the pinned-baseline
// overlay, whose owner predates local completion capabilities. The schedule
// and safety assertion are identical in both runs.
func issue87SealOriginal(req, response types.Event) types.Event {
	return types.StoreResponse(req, response)
}

// This deliberately open safety gate is opt-in: this increment has no durable
// election fence or committed floor. Apply requests are accepted by Ready but
// their effects are delayed while a stale controller snapshot installs B.
// Storage is a deterministic map; full group/Pebble coverage is separate.
func TestIssue87StaleInstallPreservesOldQuorumCommit(t *testing.T) {
	if os.Getenv("ISSUE87_OPEN_SAFETY") == "" {
		t.Skip("open election/floor gate: set ISSUE87_OPEN_SAFETY=1")
	}
	nodes := map[uint64]*Node{}
	disk := map[uint64]map[uint64]types.Log{}
	for id := uint64(1); id <= 3; id++ {
		n := NewNode(1, types.RaftState{LastLogIndex: 9, LastTerm: 7, AppliedIndex: 9}, NewOptions(WithKey("original-sample"), WithNodeId(id), WithReplicas([]uint64{1, 2, 3})))
		if id == 1 {
			n.BecomeLeader(7)
		} else {
			n.BecomeFollower(7, 1)
		}
		n.Ready()
		nodes[id] = n
		disk[id] = map[uint64]types.Log{}
	}
	store := func(id uint64) {
		n := nodes[id]
		var requests []types.Event
		for _, e := range n.Ready() {
			if e.Type == types.StoreReq {
				requests = append(requests, e)
			}
		}
		require.Len(t, requests, 1, "real owner must issue exactly one append")
		e := requests[0]
		for _, l := range e.Logs {
			disk[id][l.Index] = l
		}
		require.NoError(t, n.Step(issue87SealOriginal(e, types.Event{Type: types.StoreResp, Index: e.Logs[len(e.Logs)-1].Index, Logs: e.Logs, Reason: types.ReasonOk})))
	}
	// Controller samples B before A's next quorum commit. No fence protects it.
	sampledLast := nodes[2].LastLogIndex()
	require.EqualValues(t, 9, sampledLast)
	original := types.Log{Id: 100, Data: []byte("accepted-before-stale-install")}
	_, err := nodes[1].Admit([]types.Log{original})
	require.NoError(t, err)
	store(1)
	original = disk[1][10]
	require.NoError(t, nodes[3].Step(types.Event{Type: types.SyncResp, Term: 7, From: 1, Reason: types.ReasonOk, Logs: []types.Log{original}}))
	store(3)
	require.NoError(t, nodes[1].Step(types.Event{Type: types.SyncReq, Term: 7, From: 3, Index: 11, StoredIndex: 11, LastLogTerm: 7}))
	require.EqualValues(t, 10, nodes[1].CommittedIndex(), "old leader must actually quorum commit after sample")
	require.True(t, issue87SameOriginalLogs([]types.Log{original}, []types.Log{disk[3][10]}))
	require.NoError(t, nodes[3].Step(types.Event{Type: types.Ping, Term: 7, From: 1, CommittedIndex: 10}))
	// Preserve the real pending Apply state; no ApplyResp or business effect is forged.
	for _, id := range []uint64{1, 3} {
		found := false
		for _, e := range nodes[id].Ready() {
			if e.Type == types.ApplyReq {
				found = true
			}
		}
		require.True(t, found, "committed range must have a pending real Apply request")
		require.EqualValues(t, 9, nodes[id].AppliedIndex())
	}
	// The stale controller installs sampled B. This must eventually be fenced by
	// the full repair; a current-term ACK filter alone cannot protect old identity.
	for id, n := range nodes {
		role := types.RoleFollower
		if id == 2 {
			role = types.RoleLeader
		}
		require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1, 2, 3}, Leader: 2, Term: 8, Version: 2, Role: role}}))
	}
	_, err = nodes[2].Admit([]types.Log{{Id: 200, Data: []byte("index-reused-after-stale-install")}})
	require.NoError(t, err)
	store(2)
	replacement := disk[2][10]
	require.EqualValues(t, 10, replacement.Index)
	for _, id := range []uint64{1, 3} {
		require.NoError(t, nodes[id].Step(types.Event{Type: types.SyncResp, From: 2, Term: 8, Reason: types.ReasonTruncate, Index: sampledLast}))
		var cut *types.Event
		for _, e := range nodes[id].Ready() {
			if e.Type == types.TruncateReq {
				copy := e
				cut = &copy
			}
		}
		require.NotNil(t, cut, "term negotiation must request a real truncation")
		for index := range disk[id] {
			if index > cut.Index {
				delete(disk[id], index)
			}
		}
		require.NoError(t, nodes[id].Step(types.Event{Type: types.TruncateResp, Index: cut.Index, Reason: types.ReasonOk}))
		require.NoError(t, nodes[id].Step(types.Event{Type: types.SyncResp, From: 2, Term: 8, Reason: types.ReasonOk, Logs: []types.Log{replacement}}))
		store(id)
	}
	require.NoError(t, nodes[2].Step(types.Event{Type: types.SyncReq, From: 1, Term: 8, Index: 11, StoredIndex: 11, LastLogTerm: 8}))
	require.EqualValues(t, 10, nodes[2].CommittedIndex(), "replacement must actually commit before assessing loss")
	holders := 0
	for id := uint64(1); id <= 3; id++ {
		if issue87SameOriginalLogs([]types.Log{original}, []types.Log{disk[id][10]}) {
			holders++
		}
	}
	t.Logf("sample B last=%d; old A commit=10 holders={A,C}; stale install B term=8; replacement index=%d id=%d term=%d; original holders=%d", sampledLast, replacement.Index, replacement.Id, replacement.Term, holders)
	require.GreaterOrEqual(t, holders, 2, "SAFETY RED: stale install/index reuse erased an old-leader quorum committed identity")
}

func issue87SameOriginalLogs(a, b []types.Log) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || a[i].Id != b[i].Id || !bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}
