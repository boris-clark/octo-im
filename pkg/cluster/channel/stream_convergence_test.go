package channel

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// The original catch-up oracle is preserved in storage_streamno_test.go.
// A quorum write may complete on nodes 1 and 3 before node 2 stores the tail.
// Compare identical terminal rows only after both queried followers apply.
func TestChannelCatchUpConvergedReplicasPreserveLegacyStreamNo(t *testing.T) {
	logs, entries := legacyLogs(t)
	last := uint64(len(logs))
	key := wkutil.ChannelToKey("retry", 2)
	tr := retryNetwork()
	dbs := []wkdb.DB{retryDB(t), retryDB(t), retryDB(t), retryDB(t)}
	// Nodes 1 and 2 appended the original legacy Data; node 3 is empty.
	for _, db := range dbs[:2] {
		require.NoError(t, newStorage(db, nil).AppendLogs(key, logs, &types.TermStartIndexInfo{Term: 1, Index: 1}))
	}
	applied := func(db wkdb.DB, idx uint64) func() bool {
		return func() bool {
			n, err := db.GetChannelAppliedIndex("retry", 2)
			return err == nil && n >= idx
		}
	}
	nodes := make([]*Server, 4)
	for i := 0; i < 3; i++ {
		nodes[i] = retryServer(t, uint64(i+1), dbs[i], []uint64{1, 2, 3}, tr)
	}

	// GF-5 / SK2-R4: a new write proposed with SettingStream and a StreamNo.
	// Proposal Data is v5, so every replica stores StreamNo "".
	fresh := retryMessage(5000, "fresh", "fresh-1")
	fresh.Setting.Set(wkproto.SettingStream)
	fresh.StreamNo = "x"
	r := retryPropose(t, nodes[0], fresh)
	require.Equal(t, last+1, r[0].Index)
	require.NoError(t, ValidateMessageResults(retryRequests(t, fresh), r))
	tail := last + 1

	// Node 3 caught up from node 1 through GetLogs.
	// Wait for the applied index: a follower may still truncate an unapplied tail.
	require.Eventually(t, applied(dbs[2], tail), 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, applied(dbs[1], tail), 5*time.Second, 10*time.Millisecond)
	requireSameColumns(t, dbs[0], dbs[2], tail, "node1->node3")
	requireSameColumns(t, dbs[0], dbs[1], tail, "node1/node2")

	proposal, err := retryRequests(t, fresh)[0].Data, error(nil)
	require.NoError(t, err)
	var decoded wkdb.Message
	require.NoError(t, decoded.Unmarshal(proposal))
	for i := 0; i < 3; i++ {
		stored, err := dbs[i].LoadMsg("retry", 2, tail)
		require.NoError(t, err)
		require.Equal(t, "", stored.StreamNo, "node %d", i+1)
		require.True(t, sameMessageContent(stored, decoded), "node %d", i+1)
		want, err := stored.Marshal()
		require.NoError(t, err)
		served, err := newStorage(dbs[i], nil).GetLogs(key, tail, tail+1, 0)
		require.NoError(t, err)
		require.Equal(t, want, served[0].Data, "node %d emits the new write as before", i+1)
	}
	again := fresh
	again.MessageID = 5001
	r = retryPropose(t, nodes[0], again)
	require.True(t, r[0].Duplicate)
	require.Equal(t, uint64(5000), r[0].CanonicalID)

	// Second hop: node 1 is cut off, node 3 leads, and an empty node 4 catches
	// up from node 3, whose entries came from node 1's emission.
	tr.block(1, true)
	nodes[3] = retryServer(t, 4, dbs[3], []uint64{1, 2, 3, 4}, tr)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 3, Replicas: []uint64{1, 2, 3, 4}, Term: 2, ConfVersion: 2}
	for _, i := range []int{2, 1, 3} {
		require.NoError(t, nodes[i].Channel("retry", 2).switchConfig(channelConfigToRaftConfig(uint64(i+1), cfg)))
	}
	require.Eventually(t, applied(dbs[3], tail), 5*time.Second, 10*time.Millisecond)
	requireSameColumns(t, dbs[0], dbs[3], tail, "node3->node4")

	// SK2-R7 on the caught-up leader (class C): the outcome equals the one an
	// intact replica returns. A v5 retry of a legacy stream message conflicts,
	// a retry of a legacy message without StreamNo is a duplicate.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i, e := range entries {
		retry := e.msg
		retry.MessageID = int64(9000 + i)
		retry.MessageSeq = 0
		retry.Term = 0
		data, err := retry.Marshal()
		require.NoError(t, err)
		rs, err := nodes[2].proposeMessages(ctx, "retry", 2, types.ProposeReqSet{{Id: uint64(retry.MessageID), Data: data}})
		if e.stream {
			require.ErrorIs(t, err, ErrMessageConflict, "R-e, index %d", i+1)
			continue
		}
		require.NoError(t, err, "R-b, index %d", i+1)
		require.True(t, rs[0].Duplicate)
		require.Equal(t, uint64(e.msg.MessageID), rs[0].CanonicalID)
	}
}

// Hold precisely the replica that the original oracle reads without an apply
// barrier. The third replica can still complete the proposal's real quorum.
func TestChannelCatchUpQuorumCompletionPrecedesAllReplicaApply(t *testing.T) {
	logs, _ := legacyLogs(t)
	key := wkutil.ChannelToKey("retry", 2)
	tr := retryNetwork()
	dbs := []wkdb.DB{retryDB(t), retryDB(t), retryDB(t)}
	for _, db := range dbs[:2] {
		require.NoError(t, newStorage(db, nil).AppendLogs(key, logs, &types.TermStartIndexInfo{Term: 1, Index: 1}))
	}
	gate := &retryHookDB{DB: dbs[1], appendEntered: make(chan struct{}), appendRelease: make(chan struct{})}
	defer func() {
		select {
		case <-gate.appendRelease:
		default:
			close(gate.appendRelease)
		}
	}()
	nodes := []*Server{
		retryServer(t, 1, dbs[0], []uint64{1, 2, 3}, tr),
		retryServer(t, 2, gate, []uint64{1, 2, 3}, tr),
		retryServer(t, 3, dbs[2], []uint64{1, 2, 3}, tr),
	}
	fresh := retryMessage(5000, "fresh", "fresh-1")
	fresh.Setting.Set(wkproto.SettingStream)
	fresh.StreamNo = "x"
	result := retryPropose(t, nodes[0], fresh)
	tail := uint64(len(logs) + 1)
	require.Equal(t, tail, result[0].Index)
	select {
	case <-gate.appendEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("second replica did not enter its held append")
	}
	_, err := dbs[1].LoadMsg("retry", 2, tail)
	require.ErrorIs(t, err, wkdb.ErrNotFound, "quorum completion does not imply every voter stored row 13")
	close(gate.appendRelease)
	require.Eventually(t, func() bool {
		for _, db := range dbs {
			applied, err := db.GetChannelAppliedIndex("retry", 2)
			if err != nil || applied < tail {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	requireSameColumns(t, dbs[0], dbs[1], tail, "second replica after apply")
	requireSameColumns(t, dbs[0], dbs[2], tail, "third replica after apply")
}
