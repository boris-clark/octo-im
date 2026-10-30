package raft

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

// This test uses only the pinned baseline's API, so its safety assertion can
// also be executed without the candidate completion implementation.
func TestIssue87DecodedStoreCannotAcknowledgeDisk(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 9, LastTerm: 6, AppliedIndex: 9}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithKey("decoded-store")))
	n.BecomeFollower(7, 2)
	n.Ready()
	require.NoError(t, n.queue.append(types.Log{Index: 10, Term: 6, Id: 100, Data: []byte("old")}, types.Log{Index: 11, Term: 7, Id: 101, Data: []byte("new")}, types.Log{Index: 12, Term: 7, Id: 102, Data: []byte("tail")}))
	requests := n.Ready()
	require.NotEmpty(t, requests)
	bytes, err := (types.Event{Type: types.StoreResp, Index: 12, Reason: types.ReasonOk}).Marshal()
	require.NoError(t, err)
	var decoded types.Event
	require.NoError(t, decoded.Unmarshal(bytes))
	require.NoError(t, n.Step(decoded))
	require.EqualValues(t, 9, n.queue.storedIndex, "decoded success acknowledged bytes that storage never wrote")
	require.True(t, n.queue.appending, "decoded success consumed the actual pending append")
}
