package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestDormantElectionRestoresOnlyAppliedIdentity(t *testing.T) {
	var disk types.HardState
	expected := types.Config{Version: 10, Term: 1, InstalledTerm: 1, Leader: 1, Replicas: []uint64{1, 2, 3}, Role: types.RoleFollower}
	state := types.RaftState{HardState: types.HardState{Term: 1}}
	save := WithSaveHardState(func(h types.HardState) error { disk = h; return nil })
	n := NewNode(0, state, NewOptions(WithNodeId(2), save))
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: expected}))
	n.Ready()
	state.HardState = disk
	state.HardState.Term = 5
	for _, mutation := range []func(*types.Config){
		func(c *types.Config) { c.Leader = 3 },
		func(c *types.Config) { c.Replicas = []uint64{1, 2} },
		func(c *types.Config) { c.Learners = []uint64{3} },
		func(c *types.Config) { c.MigrateFrom, c.MigrateTo = 1, 3 },
	} {
		restarted := NewNode(0, state, NewOptions(WithNodeId(2), save))
		bad := expected.Clone()
		mutation(&bad)
		require.ErrorIs(t, restarted.FenceElection(bad, 6), ErrElectionNotInstalled)
	}
	restarted := NewNode(0, state, NewOptions(WithNodeId(2), save))
	require.NoError(t, restarted.FenceElection(expected, 6))
	require.False(t, restarted.IsLeader())
	require.EqualValues(t, 6, disk.FenceTerm)
	state.HardState = disk
	resume := NewNode(0, state, NewOptions(WithNodeId(2), save))
	bad := expected.Clone()
	bad.Version++
	require.ErrorIs(t, resume.FenceElection(bad, 7), ErrElectionNotInstalled)
	require.NoError(t, resume.FenceElection(expected, 7))
	require.False(t, resume.IsLeader())
	require.EqualValues(t, 7, disk.FenceTerm)
}

func TestRecoveryAheadBoundaryKeepsShorterFencedPrefix(t *testing.T) {
	writes := 0
	n := NewNode(1, types.RaftState{LastLogIndex: 1, LastTerm: 1, AppliedIndex: 1,
		HardState: types.HardState{Term: 6, CommittedFloor: 1, BoundaryTerm: 1}},
		NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2}), WithSaveHardState(func(types.HardState) error { writes++; return nil })))
	n.BecomeFollower(6, 1)
	clearEvents(n)
	before := writes
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, Term: 6, From: 1, Reason: types.ReasonTruncate, Index: 2}))
	require.False(t, n.Diverged())
	require.EqualValues(t, 1, n.LastLogIndex())
	require.EqualValues(t, 1, n.CommittedIndex())
	require.EqualValues(t, 1, n.AppliedIndex())
	require.Equal(t, before, writes)
	events := collectEvents(n)
	_, deletes := findEvent(events, types.TruncateReq)
	require.False(t, deletes)
	_, sync := findEvent(events, types.SyncReq)
	require.True(t, sync)
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, Term: 6, From: 1, Reason: types.ReasonTruncate, Index: 0}))
	require.True(t, n.Diverged(), "a real deletion across the applied prefix must remain fenced")
}
