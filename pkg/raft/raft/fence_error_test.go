package raft

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestFenceRefusalRetainsPhaseIdentityAndLocalCause(t *testing.T) {
	var disk types.HardState
	fail := false
	native := errors.New("sync /private/test token=secret 10.0.0.1:1234")
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithSaveHardState(func(state types.HardState) error {
		if fail {
			return native
		}
		disk = state
		return nil
	})))
	cfg := types.Config{Term: 1, InstalledTerm: 1, Version: 7, Leader: 1, Replicas: []uint64{1}, Role: types.RoleLeader}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.Ready()
	fail = true
	err := n.FenceElection(cfg, 2)
	require.ErrorIs(t, err, native)
	require.Contains(t, err.Error(), "phase=fence-persistence")
	require.NotContains(t, err.Error(), "secret")
	require.NotContains(t, err.Error(), "/private")
	require.EqualValues(t, 1, disk.InstalledTerm)
	fail = false
	require.NoError(t, n.FenceElection(cfg, 2))
	before := disk
	wrong := cfg.Clone()
	wrong.Version++
	err = n.FenceElection(wrong, 3)
	require.ErrorIs(t, err, ErrElectionNotInstalled)
	require.Contains(t, err.Error(), "phase=identity")
	require.Contains(t, err.Error(), "expected_version=8 current_version=7")
	require.Equal(t, before, disk, "full identity rejection must precede persistence")
	require.NoError(t, n.FenceElection(cfg, 3))
}
