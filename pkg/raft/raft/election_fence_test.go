package raft

import (
	"errors"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestElectionFenceRejectsUninstalledAdoptionAndRestart(t *testing.T) {
	var disk types.HardState
	fail := false
	save := func(state types.HardState) error {
		if fail {
			return errors.New("sync failure")
		}
		disk = state
		return nil
	}
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithSaveHardState(save)))
	cfg := types.Config{Term: 1, Leader: 1, Replicas: []uint64{1, 2, 3}, Role: types.RoleLeader, InstalledTerm: 1}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.Ready()
	fail = true
	require.Error(t, n.FenceElection(cfg, 2))
	require.EqualValues(t, 1, disk.Term)
	for _, event := range []types.Event{{Type: types.Ping, From: 2, Term: 2}, {Type: types.SyncResp, From: 2, Term: 2}, {Type: types.ConfigResp, From: 2, Term: 2, Config: cfg}} {
		require.NoError(t, n.Step(event))
		require.Zero(t, n.LeaderId())
	}
	require.ErrorIs(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}), ErrElectionNotInstalled)
	zeroLeader := cfg.Clone()
	zeroLeader.Term = 2
	zeroLeader.Leader = 0
	zeroLeader.Role = types.RoleLeader
	require.ErrorIs(t, n.Step(types.Event{Type: types.ConfChange, Config: zeroLeader}), ErrElectionNotInstalled)
	fail = false
	require.NoError(t, n.FenceElection(cfg, 2))
	require.EqualValues(t, 2, disk.FenceTerm)
	// P1: metadata replay has not enabled the config bit at construction.
	n = NewNode(0, types.RaftState{HardState: disk}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithSaveHardState(save)))
	require.ErrorIs(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}), ErrElectionNotInstalled)
	badExpected := cfg.Clone()
	badExpected.Learners = []uint64{99}
	require.ErrorIs(t, n.RestoreElectionFence(badExpected), ErrElectionNotInstalled)
	require.NoError(t, n.RestoreElectionFence(cfg))
	require.Zero(t, n.LeaderId())
	require.NoError(t, n.FenceElection(cfg, 2))
	cfg.Term, cfg.InstalledTerm, cfg.Leader, cfg.Role = 2, 2, 1, types.RoleLeader
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.Ready()
	require.True(t, n.IsLeader())
	for _, mutate := range []func(*types.Config){func(c *types.Config) { c.Leader = 2 }, func(c *types.Config) { c.Replicas = []uint64{1, 2} }, func(c *types.Config) { c.Learners = []uint64{3} }, func(c *types.Config) { c.MigrateFrom = 1; c.MigrateTo = 2 }, func(c *types.Config) { c.InstalledTerm = 0 }} {
		bad := cfg.Clone()
		mutate(&bad)
		require.ErrorIs(t, n.Step(types.Event{Type: types.ConfChange, Config: bad}), ErrElectionNotInstalled)
	}
	require.EqualValues(t, 2, disk.InstalledTerm)
	n = NewNode(0, types.RaftState{HardState: disk}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithSaveHardState(save)))
	stale := cfg.Clone()
	stale.Term = 1
	require.ErrorIs(t, n.Step(types.Event{Type: types.ConfChange, Config: stale}), ErrElectionNotInstalled)
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.True(t, n.IsLeader())
}

func TestCommitFloorSyncErrorDoesNotPublishAndRetries(t *testing.T) {
	fail := false
	var disk types.HardState
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithSaveHardState(func(s types.HardState) error {
		if fail {
			return errors.New("sync failure")
		}
		disk = s
		return nil
	})))
	n.Ready()
	_, err := n.Admit([]types.Log{{Id: 42, Data: []byte("retained")}})
	require.NoError(t, err)
	req, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	fail = true
	require.NoError(t, n.Step(types.StoreResponse(req, types.Event{Type: types.StoreResp, Index: 1, Logs: req.Logs, Reason: types.ReasonOk})))
	require.Zero(t, n.CommittedIndex())
	require.Zero(t, disk.CommittedFloor)
	require.Empty(t, n.Ready())
	fail = false
	events := n.Ready()
	require.EqualValues(t, 1, n.CommittedIndex())
	require.EqualValues(t, 1, disk.CommittedFloor)
	_, ok = findEvent(events, types.ApplyReq)
	require.True(t, ok)
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{Term: 2, Leader: 2, Role: types.RoleFollower, Replicas: []uint64{1, 2}}}))
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonTruncate, Index: 0}))
	require.True(t, n.Diverged())
	require.EqualValues(t, 1, n.LastLogIndex())
}

func TestRecoveryDoesNotInferCommittedFloorFromAppliedOrTail(t *testing.T) {
	var persisted types.HardState
	n := NewNode(1, types.RaftState{LastLogIndex: 9, LastTerm: 7, AppliedIndex: 9, HardState: types.HardState{Term: 8}}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithSaveHardState(func(state types.HardState) error { persisted = state; return nil })))
	require.Zero(t, n.CommittedIndex())
	n.Ready()
	n.ResumeReplication()
	require.Zero(t, n.CommittedIndex())
	require.Zero(t, persisted.CommittedFloor)
	_, err := n.Admit([]types.Log{{Id: 100, Data: []byte("current-term-authenticated-store")}})
	require.NoError(t, err)
	req, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.NoError(t, n.Step(types.StoreResponse(req, types.Event{Type: types.StoreResp, Index: 10, Logs: req.Logs, Reason: types.ReasonOk})))
	require.EqualValues(t, 10, n.CommittedIndex())
	require.EqualValues(t, 10, persisted.CommittedFloor)
}
