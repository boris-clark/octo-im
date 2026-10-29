package raft

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRecoveryNoopTruncateKeepsFencedPrefix(t *testing.T) {
	writes := 0
	n := NewNode(1, types.RaftState{LastLogIndex: 2, LastTerm: 1, AppliedIndex: 2, HardState: types.HardState{Term: 2, CommittedFloor: 2, BoundaryTerm: 1}}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithSaveHardState(func(types.HardState) error { writes++; return nil })))
	n.BecomeFollower(2, 2)
	clearEvents(n)
	before := writes
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonTruncate, Index: 2}))
	require.False(t, n.Diverged())
	require.EqualValues(t, 2, n.LastLogIndex())
	require.EqualValues(t, 2, n.CommittedIndex())
	require.EqualValues(t, 2, n.AppliedIndex())
	require.Equal(t, before, writes)
	events := collectEvents(n)
	_, ok := findEvent(events, types.TruncateReq)
	require.False(t, ok, "unchanged history must not schedule a storage delete")
	_, ok = findEvent(events, types.SyncReq)
	require.True(t, ok, "the real sync handshake must continue")
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonTruncate, Index: 1}))
	require.True(t, n.Diverged(), "a real cut across the same protected prefix must still fail")
}
