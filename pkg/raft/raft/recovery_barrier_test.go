package raft

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestRecoveryBarrierRequiresStoreAndDurableCommit(t *testing.T) {
	var disk types.HardState
	fail := false
	n := NewNode(1, types.RaftState{LastLogIndex: 9, LastTerm: 7, AppliedIndex: 9, HardState: types.HardState{Term: 8}}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithSaveHardState(func(s types.HardState) error {
		if fail {
			return errors.New("injected floor failure")
		}
		disk = s
		return nil
	})))
	n.Ready()
	entry := types.Log{Data: types.NoopData()}
	index, err := n.ResumeReplicationWithEntry(entry)
	require.NoError(t, err)
	require.EqualValues(t, 10, index)
	require.Zero(t, n.CommittedIndex())
	req, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.EqualValues(t, 8, req.Logs[0].Term)
	require.EqualValues(t, 10, req.Logs[0].Index)
	require.True(t, types.IsNoop(req.Logs[0]))
	index, err = n.ResumeReplicationWithEntry(entry)
	require.NoError(t, err)
	require.EqualValues(t, 10, index, "owner retry reuses the admitted entry")
	require.NoError(t, n.Step(types.StoreResponse(req, types.Event{Type: types.StoreResp, Reason: types.ReasonError})))
	require.Zero(t, n.CommittedIndex())
	require.Zero(t, disk.CommittedFloor)
	req, ok = findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.EqualValues(t, 10, req.Logs[0].Index)
	fail = true
	require.NoError(t, n.Step(types.StoreResponse(req, types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 10, Logs: req.Logs})))
	require.Zero(t, n.CommittedIndex(), "a successful store alone is insufficient when the floor sync fails")
	require.Zero(t, disk.CommittedFloor)
	require.Empty(t, n.Ready())
	fail = false
	n.Ready()
	require.EqualValues(t, 10, n.CommittedIndex())
	require.EqualValues(t, 10, disk.CommittedFloor)
	index, err = n.ResumeReplicationWithEntry(entry)
	require.NoError(t, err)
	require.Zero(t, index, "no further barrier is necessary after recovery")
	require.EqualValues(t, 10, n.LastLogIndex())
}
