package raft

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func issue87Store(t *testing.T, role types.Role) (*Node, types.Event) {
	t.Helper()
	n := NewNode(1, types.RaftState{LastLogIndex: 9, LastTerm: 6, AppliedIndex: 9}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithKey("issue87")))
	switch role {
	case types.RoleLeader:
		n.BecomeLeader(7)
	case types.RoleLearner:
		n.BecomeLearner(7, 2)
	default:
		n.BecomeFollower(7, 2)
	}
	n.Ready()
	require.NoError(t, n.queue.append(types.Log{Index: 10, Term: 6, Id: 100, Data: []byte("retained")}, types.Log{Index: 11, Term: 7, Id: 101, Data: []byte("next")}, types.Log{Index: 12, Term: 7, Id: 102, Data: []byte("tail")}))
	req, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.EqualValues(t, 6, n.LastLogTerm(), "Ready must not publish the pending term")
	return n, req
}

func issue87Response(req types.Event) types.Event {
	return types.StoreResponse(req, types.Event{Type: types.StoreResp, Index: req.Logs[len(req.Logs)-1].Index, Logs: req.Logs, Reason: types.ReasonOk})
}

func TestIssue87StoreIdentityBeforeConsumption(t *testing.T) {
	for _, role := range []types.Role{types.RoleLeader, types.RoleFollower, types.RoleLearner} {
		for _, mutation := range []string{"raw", "codec", "reuse", "decode-error", "index", "id", "data", "term", "row", "reason", "foreign-attempt", "foreign-node", "live-id", "live-data", "live-term", "live-index", "live-row", "truncate-reset"} {
			t.Run(fmt.Sprintf("%v/%s", role, mutation), func(t *testing.T) {
				n, req := issue87Store(t, role)
				good := issue87Response(req)
				bad := good
				restore := func() {}
				switch mutation {
				case "raw":
					bad = types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 12, Logs: req.Logs}
				case "codec", "reuse", "decode-error":
					data, err := good.Marshal()
					require.NoError(t, err)
					if mutation == "codec" {
						bad = types.Event{}
					}
					if mutation == "decode-error" {
						require.Error(t, bad.Unmarshal(nil))
						bad.Type = types.StoreResp
					} else {
						require.NoError(t, bad.Unmarshal(data))
					}
				case "index":
					bad.Index++
				case "id":
					bad.Logs = types.CloneLogs(bad.Logs)
					bad.Logs[0].Id++
				case "data":
					bad.Logs = types.CloneLogs(bad.Logs)
					bad.Logs[0].Data[0]++
				case "term":
					bad.Term = 99
				case "row":
					bad.TermStartIndexInfo = &types.TermStartIndexInfo{Term: 99, Index: 1}
				case "reason":
					bad.Reason = types.ReasonDiverged
					bad = bad.WithLocalStorageVerdict(types.StorageVerdictDiverged)
				case "foreign-attempt":
					_, foreign := types.NewStoreOperation(req)
					bad = issue87Response(foreign)
				case "foreign-node":
					_, foreign := issue87Store(t, role)
					bad = issue87Response(foreign)
				case "live-id", "live-data", "live-term", "live-index":
					old := types.CloneLogs(n.queue.logs)
					switch mutation {
					case "live-id":
						n.queue.logs[0].Id++
					case "live-data":
						n.queue.logs[0].Data[0]++
					case "live-term":
						n.queue.logs[0].Term++
					case "live-index":
						n.queue.logs[0].Index++
					}
					restore = func() { n.queue.logs = old }
				case "live-row":
					old := *n.pendingStoreTermStartIndex
					n.pendingStoreTermStartIndex.Index++
					restore = func() { *n.pendingStoreTermStartIndex = old }
				case "truncate-reset":
					n.queue.storageRevision++
					restore = func() { n.queue.storageRevision-- }
				}
				require.NoError(t, n.Step(bad))
				require.EqualValues(t, 7, n.LastTerm(), "invalid completion changed term")
				require.EqualValues(t, 6, n.LastLogTerm(), "invalid completion published row")
				require.EqualValues(t, 9, n.queue.storedIndex)
				require.EqualValues(t, 9, n.CommittedIndex())
				require.EqualValues(t, 9, n.AppliedIndex())
				require.True(t, n.queue.appending, "invalid completion consumed current append")
				require.NotNil(t, n.pendingStore)
				require.False(t, n.Diverged())
				restore()
				require.NoError(t, n.Step(good))
				require.EqualValues(t, 12, n.queue.storedIndex)
				require.EqualValues(t, 7, n.LastLogTerm())
				require.Nil(t, n.pendingStore)
				before := n.queue.storedIndex
				good.Term = 100
				require.NoError(t, n.Step(good))
				require.EqualValues(t, 7, n.LastTerm(), "duplicate completion changed term")
				require.Equal(t, before, n.queue.storedIndex)
			})
		}
	}
}

func TestIssue87StorePrefixAndRetry(t *testing.T) {
	for _, prefix := range []uint64{10, 11, 12} {
		t.Run(fmt.Sprint(prefix), func(t *testing.T) {
			n, req := issue87Store(t, types.RoleFollower)
			resp := types.StoreResponse(req, types.Event{Type: types.StoreResp, Index: prefix, Logs: req.Logs[:prefix-9], Reason: types.ReasonOk})
			require.NoError(t, n.Step(resp))
			want := uint32(6)
			if prefix >= 11 {
				want = 7
			}
			require.Equal(t, want, n.LastLogTerm())
			require.Equal(t, prefix, n.queue.storedIndex)
		})
	}
	n, req := issue87Store(t, types.RoleFollower)
	require.NoError(t, n.Step(types.StoreResponse(req, types.Event{Type: types.StoreResp, Reason: types.ReasonError})))
	require.EqualValues(t, 6, n.LastLogTerm())
	retry, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.Equal(t, req.TermStartIndexInfo, retry.TermStartIndexInfo)
	require.NoError(t, n.Step(issue87Response(req)))
	require.True(t, n.queue.appending, "old retry attempt consumed the current attempt")
	require.NoError(t, n.Step(issue87Response(retry)))
	require.EqualValues(t, 12, n.queue.storedIndex)
}

func TestIssue87StoreSplitAndBound(t *testing.T) {
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), func(o *Options) { o.MaxLogCountPerBatch = 2 }))
	require.NoError(t, n.queue.append(types.Log{Index: 1, Term: 1}, types.Log{Index: 2, Term: 2}, types.Log{Index: 3, Term: 2}, types.Log{Index: 4, Term: 3}, types.Log{Index: 5, Term: 3}))
	for _, want := range [][2]uint64{{1, 1}, {2, 3}, {4, 5}} {
		req, ok := findEvent(n.Ready(), types.StoreReq)
		require.True(t, ok)
		require.Equal(t, want[0], req.Logs[0].Index)
		require.Equal(t, want[1], req.Logs[len(req.Logs)-1].Index)
		require.LessOrEqual(t, len(req.Logs), 2)
		require.Equal(t, n.queue.storedIndex, n.CommittedIndex(), "commit advanced before persistence")
		require.NoError(t, n.Step(issue87Response(req)))
	}
	require.EqualValues(t, 5, n.queue.storedIndex)
	require.EqualValues(t, 3, n.LastLogTerm())
}

func TestIssue87CurrentTermVoterAck(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3, 4, 5})
	n.BecomeLeader(8)
	n.queue.storedIndex = 12
	n.queue.lastLogIndex = 12
	n.queue.committedIndex = 10
	n.replicaSync[2] = &SyncInfo{StoredIndex: 13, LastLogTerm: 7}
	n.replicaSync[3] = &SyncInfo{StoredIndex: 13, LastLogTerm: 8}
	n.replicaSync[99] = &SyncInfo{StoredIndex: 13, LastLogTerm: 8}
	require.EqualValues(t, 10, n.committedIndexForLeader(), "stale and foreign ACKs cannot form quorum")
	n.replicaSync[4] = &SyncInfo{StoredIndex: 13, LastLogTerm: 8}
	require.EqualValues(t, 12, n.committedIndexForLeader(), "three current-term voters must make progress")
}

func TestIssue87StoreByteBoundAllowsOversizedProgress(t *testing.T) {
	log := types.Log{Index: 1, Term: 1, Id: 100, Data: []byte("payload")}
	limit := uint64(log.LogSize())
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithMaxStoreBatchBytes(limit)))
	tail := log
	tail.Index = 2
	tail.Id = 200
	tail.Data = make([]byte, limit*2)
	require.NoError(t, n.queue.append(log, tail))
	first, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.Len(t, first.Logs, 1)
	require.EqualValues(t, 1, first.Logs[0].Index)
	require.NoError(t, n.Step(issue87Response(first)))
	second, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.Len(t, second.Logs, 1)
	require.Greater(t, uint64(second.Logs[0].LogSize()), limit)
	require.NoError(t, n.Step(issue87Response(second)))
	require.EqualValues(t, 2, n.queue.storedIndex)
}
