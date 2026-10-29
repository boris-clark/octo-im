package channel

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type recoveryBarrierDB struct {
	wkdb.DB
	failStore    atomic.Bool
	failFloor    atomic.Bool
	failTerm     atomic.Bool
	failMessages atomic.Bool
}

func (d *recoveryBarrierDB) AppendMessages(id string, typ uint8, messages []wkdb.Message) error {
	if d.failMessages.Load() {
		return errors.New("injected message sync failure")
	}
	return d.DB.AppendMessages(id, typ, messages)
}

func (d *recoveryBarrierDB) AppendChannelNoop(key string, log types.Log) error {
	if d.failStore.Load() {
		return errors.New("injected barrier sync failure")
	}
	return d.DB.AppendChannelNoop(key, log)
}

func (d *recoveryBarrierDB) SaveRaftHardState(key string, state types.HardState) error {
	if d.failTerm.Load() {
		return errors.New("injected term sync failure")
	}
	if state.CommittedFloor > 0 && d.failFloor.Load() {
		return errors.New("injected commit floor sync failure")
	}
	return d.DB.SaveRaftHardState(key, state)
}

// Exercise actual owner admission/store/commit/apply, caller cancellation,
// continued writes, and two reopens using only a new temporary database.
func TestConfigRecoveryBarrierFailuresContinuedWriteAndReopen(t *testing.T) {
	for _, failure := range []string{"term", "store", "floor"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			key := wkutil.ChannelToKey("retry", 2)
			cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 5, ConfVersion: 4}
			original := retryMessage(101, "alice", "retained")
			original.Framer = wkproto.Framer{FrameType: wkproto.RECV}
			original.MessageSeq, original.Term = 1, 1
			var before []types.Log
			phase := func(seed bool, action func(*Server, *recoveryBarrierDB)) {
				db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(1)))
				require.NoError(t, db.Open())
				defer func() { require.NoError(t, db.Close()) }()
				wrapped := &recoveryBarrierDB{DB: db}
				if seed {
					require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{original}))
					// Legacy applied history alone must not certify a commit floor.
					require.NoError(t, db.UpdateChannelAppliedIndex("retry", 2, 1))
					var err error
					before, err = newStorage(db, nil).GetLogs(key, 1, 2, 0)
					require.NoError(t, err)
				}
				s := NewServer(NewOptions(WithNodeId(1), WithDB(wrapped), WithGroupCount(1), WithTransport(retryNetwork())))
				require.NoError(t, s.Start())
				defer stopServer(t, s)
				action(s, wrapped)
			}
			phase(true, func(s *Server, db *recoveryBarrierDB) {
				if failure == "term" {
					db.failTerm.Store(true)
				} else if failure == "store" {
					db.failStore.Store(true)
				} else {
					db.failFloor.Store(true)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
				defer cancel()
				require.ErrorIs(t, s.createAndApplyConfig(ctx, cfg), context.DeadlineExceeded)
				state, err := s.ReadLeaderState(context.Background(), "retry", 2)
				require.NoError(t, err)
				require.Zero(t, state.CommittedIndex, "neither applied history nor an admitted barrier certifies commitment")
				hard, err := db.RaftHardState(key)
				require.NoError(t, err)
				require.Zero(t, hard.CommittedFloor)
				db.failStore.Store(false)
				db.failFloor.Store(false)
				db.failTerm.Store(false)
				// A retry can enter directly through the existing channel after
				// the original config caller expired, before any later wake.
				retryCtx, retryCancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer retryCancel()
				retry, err := s.proposeMessages(retryCtx, "retry", 2, retryRequests(t, retryMessage(103, "alice", "retained")))
				require.NoError(t, err)
				require.EqualValues(t, 1, retry[0].Index)
				require.EqualValues(t, 101, retry[0].CanonicalID)
				require.NoError(t, s.WakeLeaderIfNeed(cfg))
				barriers, err := db.ChannelNoops(key, 1, 0)
				require.NoError(t, err)
				require.Len(t, barriers, 1, "owner retries must reuse the pending barrier")
				require.EqualValues(t, 2, barriers[0].Index)
				require.EqualValues(t, 5, barriers[0].Term)
				require.Eventually(t, func() bool { n, e := db.GetChannelAppliedIndex("retry", 2); return e == nil && n == 2 }, time.Second, time.Millisecond)
			})
			// Reopen while the barrier, rather than a message, is the last Raft entry.
			phase(false, func(s *Server, db *recoveryBarrierDB) {
				require.NoError(t, s.WakeLeaderIfNeed(cfg))
				state, err := s.ReadLeaderState(context.Background(), "retry", 2)
				require.NoError(t, err)
				require.EqualValues(t, 2, state.CommittedIndex)
				require.EqualValues(t, 2, state.AppliedIndex)
				r, err := s.proposeMessages(context.Background(), "retry", 2, retryRequests(t, retryMessage(102, "alice", "continued")))
				require.NoError(t, err)
				require.EqualValues(t, 3, r[0].Index)
				logs, err := s.storage.GetLogs(key, 1, 4, 0)
				require.NoError(t, err)
				require.Len(t, logs, 3)
				require.Equal(t, before[0], logs[0], "retained index/term/id/payload must survive recovery unchanged")
				require.True(t, types.IsNoop(logs[1]))
				require.EqualValues(t, 5, logs[2].Term)
				// Catch-up adapters must carry the barrier, including bounded reads.
				follower := newStorage(retryDB(t), nil)
				for next := uint64(1); next < 4; {
					batch, e := s.storage.GetLogs(key, next, 4, 1)
					require.NoError(t, e)
					require.NotEmpty(t, batch)
					require.NoError(t, follower.AppendLogs(key, batch, nil))
					next = batch[len(batch)-1].Index + 1
				}
				copied, err := follower.GetLogs(key, 1, 4, 0)
				require.NoError(t, err)
				require.Equal(t, logs, copied)
				messages, err := db.LoadNextRangeMsgs("retry", 2, 1, 4, 0)
				require.NoError(t, err)
				require.Len(t, messages, 2, "barriers must never appear as user messages")
			})
			phase(false, func(s *Server, db *recoveryBarrierDB) {
				require.NoError(t, s.WakeLeaderIfNeed(cfg))
				state, err := s.ReadLeaderState(context.Background(), "retry", 2)
				require.NoError(t, err)
				require.EqualValues(t, 3, state.CommittedIndex)
				require.EqualValues(t, 3, state.AppliedIndex)
				retained, err := s.storage.GetLogs(key, 1, 2, 0)
				require.NoError(t, err)
				require.Equal(t, before, retained)
				require.ErrorIs(t, db.TruncateLogTo("retry", 2, 1), types.ErrTruncateBelowApplied)
			})
		})
	}
}

func TestStorageRecoveryBarrierTruncationAndIdentity(t *testing.T) {
	db := retryDB(t)
	st := newStorage(db, nil)
	key := wkutil.ChannelToKey("retry", 2)
	original := retryMessage(101, "alice", "retained")
	original.Framer = wkproto.Framer{FrameType: wkproto.RECV}
	original.MessageSeq, original.Term = 1, 1
	require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{original}))
	barrier := types.Log{Index: 2, Term: 5, Data: types.NoopData()}
	require.NoError(t, st.AppendLogs(key, []types.Log{barrier}, &types.TermStartIndexInfo{Term: 5, Index: 2}))
	require.NoError(t, db.AppendChannelNoop(key, barrier), "store replay is idempotent")
	bad := barrier
	bad.Term = 6
	require.ErrorIs(t, db.AppendChannelNoop(key, bad), types.ErrAppendBelowApplied)
	collision := original
	collision.MessageSeq = 2
	require.ErrorIs(t, db.AppendMessages("retry", 2, []wkdb.Message{collision}), types.ErrAppendBelowApplied)
	collision.MessageSeq, collision.Term, collision.MessageID = 3, 5, 103
	require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{collision}))
	// Retain a barrier as the Raft tail while deleting a later message. Message
	// APIs must keep returning the real earlier message instead of an empty row.
	require.NoError(t, st.TruncateLogTo(key, 2))
	last, err := db.GetLastMsg("retry", 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, last.MessageSeq)
	state, err := st.GetState("retry", 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, state.LastLogIndex)
	require.EqualValues(t, 5, state.LastTerm)
	require.NoError(t, st.TruncateLogTo(key, 1))
	barriers, err := db.ChannelNoops(key, 1, 0)
	require.NoError(t, err)
	require.Empty(t, barriers)
	term, err := db.LeaderLastTerm(key)
	require.NoError(t, err)
	require.Zero(t, term, "the discarded barrier's term row must be removed in the same truncation")
	require.NoError(t, db.AppendChannelNoop(key, barrier))
	require.NoError(t, db.SaveRaftHardState(key, types.HardState{Term: 5, CommittedFloor: 2}))
	require.ErrorIs(t, st.TruncateLogTo(key, 1), types.ErrTruncateBelowApplied)
	lastBarrier, err := db.LastChannelNoop(key)
	require.NoError(t, err)
	require.Equal(t, barrier, lastBarrier)
}

func TestConfigRecoveryBarrierReplicatesToJoiningVoter(t *testing.T) {
	tr := retryNetwork()
	dbs := []wkdb.DB{retryDB(t), retryDB(t)}
	original := retryMessage(201, "alice", "retained")
	original.Framer = wkproto.Framer{FrameType: wkproto.RECV}
	original.MessageSeq, original.Term = 1, 1
	require.NoError(t, dbs[0].AppendMessages("retry", 2, []wkdb.Message{original}))
	nodes := make([]*Server, 2)
	for i := range nodes {
		nodes[i] = NewServer(NewOptions(WithNodeId(uint64(i+1)), WithDB(dbs[i]), WithGroupCount(1), WithTransport(tr)))
		require.NoError(t, nodes[i].Start())
		s := nodes[i]
		t.Cleanup(func() { stopServer(t, s) })
		tr.mu.Lock()
		tr.nodes[uint64(i+1)] = s
		tr.mu.Unlock()
	}
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 5, ConfVersion: 4}
	require.NoError(t, nodes[0].WakeLeaderIfNeed(cfg))
	cfg.Replicas, cfg.ConfVersion = []uint64{1, 2}, 5
	require.NoError(t, nodes[0].SwitchConfig("retry", 2, cfg))
	require.NoError(t, nodes[1].createAndApplyConfig(context.Background(), cfg))
	r := retryPropose(t, nodes[0], retryMessage(202, "alice", "quorum-write"))
	require.EqualValues(t, 3, r[0].Index)
	require.Eventually(t, func() bool { n, e := dbs[1].GetChannelAppliedIndex("retry", 2); return e == nil && n == 3 }, 3*time.Second, 5*time.Millisecond)
	key := wkutil.ChannelToKey("retry", 2)
	leaderLogs, err := nodes[0].storage.GetLogs(key, 1, 4, 0)
	require.NoError(t, err)
	followerLogs, err := nodes[1].storage.GetLogs(key, 1, 4, 0)
	require.NoError(t, err)
	require.Equal(t, leaderLogs, followerLogs, "real owner sync must preserve the barrier and old message")
	require.True(t, types.IsNoop(followerLogs[1]))
	require.EqualValues(t, 1, followerLogs[0].Term)
}

func TestStorageRecoveryBarrierFailureKeepsDurablePrefix(t *testing.T) {
	for _, failure := range []string{"barrier", "message"} {
		t.Run(failure, func(t *testing.T) {
			db := &recoveryBarrierDB{DB: retryDB(t)}
			st := newStorage(db, nil)
			key := wkutil.ChannelToKey("retry", 2)
			old := retryMessage(301, "alice", "old")
			old.Framer = wkproto.Framer{FrameType: wkproto.RECV}
			old.MessageSeq, old.Term = 1, 1
			require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{old}))
			fresh := retryMessage(302, "alice", "fresh")
			fresh.Framer = wkproto.Framer{FrameType: wkproto.RECV}
			data, err := fresh.Marshal()
			require.NoError(t, err)
			logs := []types.Log{{Index: 2, Term: 5, Data: types.NoopData()}, {Id: 302, Index: 3, Term: 5, Data: data}}
			if failure == "barrier" {
				db.failStore.Store(true)
			} else {
				db.failMessages.Store(true)
			}
			require.Error(t, st.AppendLogs(key, logs, &types.TermStartIndexInfo{Term: 5, Index: 2}))
			state, err := st.GetState("retry", 2)
			require.NoError(t, err)
			want := uint64(1)
			if failure == "message" {
				want = 2
			}
			require.Equal(t, want, state.LastLogIndex, "failed mixed append must leave a durable contiguous prefix")
			_, err = db.LoadMsg("retry", 2, 3)
			require.ErrorIs(t, err, wkdb.ErrNotFound, "a later message cannot precede its barrier on disk")
			require.Zero(t, state.HardState.CommittedFloor)
			db.failStore.Store(false)
			db.failMessages.Store(false)
			require.NoError(t, st.AppendLogs(key, logs, &types.TermStartIndexInfo{Term: 5, Index: 2}))
			complete, err := st.GetLogs(key, 1, 4, 0)
			require.NoError(t, err)
			require.Len(t, complete, 3)
			fresh.MessageSeq, fresh.Term = 3, 5
			logs[1].Data, err = fresh.Marshal()
			require.NoError(t, err)
			require.Equal(t, logs, complete[1:])
		})
	}
}
