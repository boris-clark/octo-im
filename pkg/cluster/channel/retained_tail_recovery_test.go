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

func TestRecoverySameTermAppliedTailWaitsForRealApply(t *testing.T) {
	db := &retryHookDB{DB: retryDB(t)}
	old := retryMessage(10, "alice", "retained")
	old.Framer = wkproto.Framer{FrameType: wkproto.RECV}
	old.MessageSeq, old.Term = 1, 8
	require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{old}))
	require.NoError(t, db.UpdateChannelAppliedIndex("retry", 2, 1))
	key := wkutil.ChannelToKey("retry", 2)
	require.NoError(t, db.SaveRaftHardState(key, types.HardState{Term: 8}))
	db.failApply.Store(true)
	s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(1), WithTransport(retryNetwork())))
	require.NoError(t, s.Start())
	defer stopServer(t, s)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 8, ConfVersion: 12}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	err := s.createAndApplyConfig(ctx, cfg)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "config recovery must wait for the new barrier to apply")
	state, err := s.ReadLeaderState(context.Background(), "retry", 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, state.CommittedIndex)
	require.EqualValues(t, 1, state.AppliedIndex)
	ctx, cancel = context.WithTimeout(context.Background(), 80*time.Millisecond)
	_, err = s.proposeMessages(ctx, "retry", 2, retryRequests(t, retryMessage(11, "alice", "retained")))
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "the old applied index cannot satisfy the recovery apply wait")
	db.failApply.Store(false)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, s.createAndApplyConfig(ctx, cfg), "a cancelled config caller must retain the recovery target")
	r, err := s.proposeMessages(ctx, "retry", 2, retryRequests(t, retryMessage(12, "alice", "retained")))
	require.NoError(t, err)
	require.EqualValues(t, 10, r[0].CanonicalID)
	require.EqualValues(t, 1, r[0].Index)
	barriers, err := db.ChannelNoops(key, 1, 0)
	require.NoError(t, err)
	require.Len(t, barriers, 1)
	require.EqualValues(t, 2, barriers[0].Index)
	require.EqualValues(t, 8, barriers[0].Term)
	stored, err := db.LoadMsg("retry", 2, 1)
	require.NoError(t, err)
	require.Equal(t, old, stored)
}

// These stores were written and reopened by the pinned old binary, with an
// applied-only tail in the same term as the persisted three-voter config.
func TestPinnedOldStoreAppliedTailRetryCommitApplyAndReopen(t *testing.T) {
	var dirs [3]string
	var reports [3]oldFixtureReport
	for i, name := range []string{"retry1", "retry2", "retry3"} {
		dirs[i], reports[i] = readOldFixture(t, name)
	}
	for _, reopen := range []bool{false, true} {
		func() {
			tr := retryNetwork()
			var dbs [3]wkdb.DB
			var servers [3]*Server
			for i := range dbs {
				dbs[i] = oldFixtureDB(t, dirs[i], uint64(i+1))
				defer dbs[i].Close()
				servers[i] = NewServer(NewOptions(WithNodeId(uint64(i+1)), WithDB(dbs[i]), WithGroupCount(1), WithTransport(tr), WithCluster(oldFixtureCluster{db: dbs[i]})))
				require.NoError(t, servers[i].Start())
				defer stopServer(t, servers[i])
				tr.mu.Lock()
				tr.nodes[uint64(i+1)] = servers[i]
				tr.mu.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for _, c := range reports[0].Channels {
				key := wkutil.ChannelToKey(c.ID, c.Type)
				tail := uint64(len(c.Messages))
				require.Equal(t, tail, c.Applied)
				if !reopen {
					// Hold the network until the leader has requested recovery. No
					// floor is allowed merely because the legacy tail was applied.
					tr.block(1, true)
				}
				for _, s := range servers {
					require.NoError(t, s.createAndApplyConfig(ctx, c.Config))
				}
				old := c.Messages[len(c.Messages)-1]
				retry := old
				retry.MessageID += 100000
				data, err := retry.Marshal()
				require.NoError(t, err)
				request := types.ProposeReqSet{{Id: uint64(retry.MessageID), Data: data}}
				if !reopen {
					blocked, stop := context.WithTimeout(ctx, 80*time.Millisecond)
					_, err = servers[0].proposeMessages(blocked, c.ID, c.Type, request)
					stop()
					require.ErrorIs(t, err, context.DeadlineExceeded)
					hard, e := dbs[0].RaftHardState(key)
					require.NoError(t, e)
					require.Zero(t, hard.CommittedFloor, "missing quorum must not publish a legacy floor")
					tr.block(1, false)
				}
				r, err := servers[0].proposeMessages(ctx, c.ID, c.Type, request)
				require.NoError(t, err)
				require.True(t, r[0].Duplicate)
				require.EqualValues(t, old.MessageID, r[0].CanonicalID)
				require.EqualValues(t, old.MessageSeq, r[0].Index)
				want := tail + 2 // one recovery barrier and one new message
				if !reopen {
					fresh := atomicFixtureMessage(c.ID, c.Type, int64(190000+int(c.Type)*100+len(c.ID)), "after-applied-tail-retry")
					data, err = fresh.Marshal()
					require.NoError(t, err)
					r, err = servers[0].proposeMessages(ctx, c.ID, c.Type, types.ProposeReqSet{{Id: uint64(fresh.MessageID), Data: data}})
					require.NoError(t, err)
					require.False(t, r[0].Duplicate)
					require.Equal(t, want, r[0].Index)
				}
				for i, db := range dbs {
					require.Eventually(t, func() bool { a, e := db.GetChannelAppliedIndex(c.ID, c.Type); return e == nil && a == want }, 3*time.Second, time.Millisecond)
					hard, e := db.RaftHardState(key)
					require.NoError(t, e)
					require.Equal(t, want, hard.CommittedFloor)
					logs, e := servers[i].storage.GetLogs(key, 1, want+1, 0)
					require.NoError(t, e)
					require.Len(t, logs, int(want))
					require.True(t, types.IsNoop(logs[tail]))
					require.EqualValues(t, c.Config.Term, logs[tail].Term)
					m, e := db.LoadMsg(c.ID, c.Type, want)
					require.NoError(t, e)
					require.EqualValues(t, 190000+int(c.Type)*100+len(c.ID), m.MessageID)
					require.EqualValues(t, want, m.MessageSeq)
					require.Equal(t, []byte("after-applied-tail-retry"), m.Payload)
					require.EqualValues(t, c.Config.Term, m.Term)
					t.Logf("node=%d channel=%s term=%d retry=%d barrier=%d committed/applied=%d reopen=%v", i+1, key, c.Config.Term, old.MessageSeq, tail+1, want, reopen)
				}
			}
			for i, db := range dbs {
				assertOldBusinessRows(t, db, reports[i], 1)
			}
		}()
		for i, dir := range dirs {
			assertOldPhysicalRows(t, dir, reports[i], true)
		}
	}
}
