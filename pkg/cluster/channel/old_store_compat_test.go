package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// The fixture lookup reads the old persisted membership; it supplies neither
// a commit certificate nor controller/RPC coverage.
type oldFixtureCluster struct {
	icluster.ICluster
	db wkdb.DB
}

func (c oldFixtureCluster) GetOrCreateChannelClusterConfigFromSlotLeader(id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
	return c.db.GetChannelClusterConfig(id, typ)
}

type oldFixtureChannel struct {
	ID            string
	Type          uint8
	Messages      []wkdb.Message
	Subscribers   []wkdb.Member
	Conversations []wkdb.Conversation
	Config        wkdb.ChannelClusterConfig
	Applied       uint64
	Terms         map[uint32]uint64
}
type oldFixtureRow struct {
	Shard      int
	Key, Value []byte
}
type oldFixtureReport struct {
	Baseline  string
	Channels  []oldFixtureChannel
	RawRows   []oldFixtureRow
	SlotLogs  []types.Log
	SlotState types.RaftState
}

func readOldFixture(t *testing.T, name string) (string, oldFixtureReport) {
	t.Helper()
	root := os.Getenv("ISSUE87_OLD_FIXTURES")
	if root == "" {
		t.Skip("run experiments/issue87-compat/run.py to create pinned old-source fixtures")
	}
	dir := filepath.Join(root, name)
	marker, e := os.ReadFile(filepath.Join(dir, "DISPOSABLE-ISSUE87-FIXTURE"))
	require.NoError(t, e)
	require.Equal(t, "bdfbce6951787f58706e1ce15f0cfb1bf02252df", string(marker))
	raw, e := os.ReadFile(filepath.Join(root, name+".json"))
	require.NoError(t, e)
	var report oldFixtureReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Equal(t, string(marker), report.Baseline)
	require.NotEmpty(t, report.RawRows)
	return dir, report
}

func oldFixtureDB(t *testing.T, dir string, node uint64) wkdb.DB {
	t.Helper()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithNodeId(node), wkdb.WithShardNum(4), wkdb.WithMemTableSize(1<<20)))
	require.NoError(t, db.Open())
	return db
}

func assertOldBusinessRows(t *testing.T, db wkdb.DB, report oldFixtureReport, extra int) {
	t.Helper()
	for _, c := range report.Channels {
		msgs, e := db.LoadNextRangeMsgs(c.ID, c.Type, 1, 0, 0)
		require.NoError(t, e)
		require.Len(t, msgs, len(c.Messages)+extra)
		for i, old := range c.Messages {
			require.Equal(t, old, msgs[i], "all original message columns at %s/%d/%d", c.ID, c.Type, old.MessageSeq)
			indexed, e := db.LoadMsgBySenderClientMsgNo(context.Background(), c.ID, c.Type, old.FromUID, old.ClientMsgNo)
			require.NoError(t, e)
			require.Equal(t, old, indexed)
		}
		subscribers, e := db.GetSubscribers(c.ID, c.Type)
		require.NoError(t, e)
		sort.Slice(subscribers, func(i, j int) bool { return subscribers[i].Uid < subscribers[j].Uid })
		require.Equal(t, c.Subscribers, subscribers)
		conversations, e := db.GetConversations("alice")
		require.NoError(t, e)
		var scoped []wkdb.Conversation
		for _, v := range conversations {
			if v.ChannelId == c.ID && v.ChannelType == c.Type {
				scoped = append(scoped, v)
			}
		}
		require.Equal(t, c.Conversations, scoped)
		cfg, e := db.GetChannelClusterConfig(c.ID, c.Type)
		require.NoError(t, e)
		// JSON omitempty cannot distinguish nil and empty learner slices. Compare
		// the complete persisted encoding; the physical KV check also stays exact.
		expectedConfig, e := c.Config.Marshal()
		require.NoError(t, e)
		actualConfig, e := cfg.Marshal()
		require.NoError(t, e)
		require.Equal(t, expectedConfig, actualConfig, "all persisted configuration and membership fields must stay intact")
	}
}

// Compare every old physical KV after all writers have closed. Only the three
// explicitly checked progress fields may change during real commitment.
func assertOldPhysicalRows(t *testing.T, dir string, report oldFixtureReport, allowProgress bool) {
	t.Helper()
	allowed := map[string]bool{}
	if allowProgress {
		for _, c := range report.Channels {
			allowed[string(key.NewChannelLastMessageSeqKey(c.ID, c.Type))] = true
			allowed[string(key.NewChannelCommonColumnKey(c.ID, c.Type, key.TableChannelCommon.Column.AppliedIndex))] = true
			allowed[string(key.NewRaftHardStateKey(wkutil.ChannelToKey(c.ID, c.Type)))] = true
		}
	}
	preserved, changed := 0, 0
	for shard := 0; shard < 4; shard++ {
		db, e := pebble.Open(filepath.Join(dir, "wukongimdb", fmt.Sprintf("shard%03d", shard)), &pebble.Options{ReadOnly: true})
		require.NoError(t, e)
		for _, r := range report.RawRows {
			if r.Shard != shard {
				continue
			}
			got, c, e := db.Get(r.Key)
			require.NoError(t, e)
			if allowed[string(r.Key)] && !bytes.Equal(r.Value, got) {
				changed++
			} else {
				require.True(t, bytes.Equal(r.Value, got), "old KV shard=%d key=%x expected=%x actual=%x", shard, r.Key, r.Value, got)
				preserved++
			}
			require.NoError(t, c.Close())
		}
		require.NoError(t, db.Close())
	}
	t.Logf("original physical rows: %d preserved, %d explicitly allowed progress changes", preserved, changed)
	logs := slot.NewPebbleShardLogStorage(nil, filepath.Join(dir, "slot"), 2)
	require.NoError(t, logs.Open())
	entries, e := logs.GetLogs("1", 1, 3, 0)
	require.NoError(t, e)
	require.Equal(t, report.SlotLogs, entries)
	state, e := logs.GetState("1")
	require.NoError(t, e)
	require.Equal(t, report.SlotState, state)
	require.NoError(t, logs.Close())
}

func TestPinnedOldStoreThreeVoterActivationWriteAndReopen(t *testing.T) {
	names := []string{"node1", "node2", "node3"}
	dirs := make([]string, 3)
	reports := make([]oldFixtureReport, 3)
	for i, name := range names {
		dirs[i], reports[i] = readOldFixture(t, name)
	}
	run := func(reopen bool) {
		tr := retryNetwork()
		dbs := make([]wkdb.DB, 3)
		servers := make([]*Server, 3)
		for i := range dbs {
			dbs[i] = oldFixtureDB(t, dirs[i], uint64(i+1))
			defer dbs[i].Close()
		}
		if !reopen {
			for i, db := range dbs {
				assertOldBusinessRows(t, db, reports[i], 0)
			}
		}
		for i := range servers {
			s := NewServer(NewOptions(WithNodeId(uint64(i+1)), WithDB(dbs[i]), WithGroupCount(1), WithTransport(tr), WithCluster(oldFixtureCluster{db: dbs[i]})))
			require.NoError(t, s.Start())
			servers[i] = s
			tr.mu.Lock()
			tr.nodes[uint64(i+1)] = s
			tr.mu.Unlock()
			defer stopServer(t, s)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, c := range reports[0].Channels {
			// Same old membership/configuration; no injected commit certificate.
			for _, s := range servers {
				require.NoError(t, s.createAndApplyConfig(ctx, c.Config))
			}
			want := uint64(len(c.Messages) + 1)
			if !reopen {
				m := atomicFixtureMessage(c.ID, c.Type, int64(90000+int(c.Type)*100+len(c.ID)), "new-write")
				data, e := m.Marshal()
				require.NoError(t, e)
				rs, e := servers[0].proposeMessages(ctx, c.ID, c.Type, types.ProposeReqSet{{Id: uint64(m.MessageID), Data: data}})
				require.NoError(t, e)
				require.Len(t, rs, 1)
				require.False(t, rs[0].Duplicate, "fixture must require an actual new proposal")
				require.Equal(t, want, rs[0].Index)
			}
			for i, db := range dbs {
				require.Eventually(t, func() bool { a, e := db.GetChannelAppliedIndex(c.ID, c.Type); return e == nil && a == want }, 5*time.Second, time.Millisecond, "node %d applies actual committed proposal", i+1)
				hard, e := db.RaftHardState(wkutil.ChannelToKey(c.ID, c.Type))
				require.NoError(t, e)
				require.Equal(t, want, hard.CommittedFloor)
				m, e := db.LoadMsg(c.ID, c.Type, want)
				require.NoError(t, e)
				require.Equal(t, []byte("new-write"), m.Payload)
			}
		}
		for i, db := range dbs {
			assertOldBusinessRows(t, db, reports[i], 1)
		}
	}
	run(false)
	for i, dir := range dirs {
		assertOldPhysicalRows(t, dir, reports[i], true)
	}
	run(true)
	for i, dir := range dirs {
		assertOldPhysicalRows(t, dir, reports[i], true)
	}
}

func atomicFixtureMessage(id string, typ uint8, messageID int64, payload string) wkdb.Message {
	m := retryMessage(messageID, "continued", fmt.Sprintf("new-%s-%d", id, typ))
	m.ChannelID = id
	m.ChannelType = typ
	m.Payload = []byte(payload)
	return m
}

func TestPinnedOldStoreSingleVoterBarrierGapAndReopen(t *testing.T) {
	dir, report := readOldFixture(t, "single")
	run := func(reopen bool) {
		db := oldFixtureDB(t, dir, 1)
		defer db.Close()
		if !reopen {
			assertOldBusinessRows(t, db, report, 0)
		}
		s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(1), WithTransport(retryNetwork())))
		require.NoError(t, s.Start())
		defer stopServer(t, s)
		for _, c := range report.Channels {
			cfg := c.Config.Clone()
			cfg.Term++
			cfg.ConfVersion++
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			require.NoError(t, s.createAndApplyConfig(ctx, cfg))
			cancel()
			k := wkutil.ChannelToKey(c.ID, c.Type)
			barriers, e := db.ChannelNoops(k, 1, 0)
			require.NoError(t, e)
			require.Len(t, barriers, 1)
			require.EqualValues(t, 5, barriers[0].Index)
			_, e = db.LoadMsg(c.ID, c.Type, 5)
			require.ErrorIs(t, e, wkdb.ErrNotFound, "barrier index has no user message")
			if !reopen {
				m := atomicFixtureMessage(c.ID, c.Type, int64(95000+int(c.Type)*100+len(c.ID)), "after-gap")
				data, e := m.Marshal()
				require.NoError(t, e)
				rs, e := s.proposeMessages(context.Background(), c.ID, c.Type, types.ProposeReqSet{{Id: uint64(m.MessageID), Data: data}})
				require.NoError(t, e)
				require.EqualValues(t, 6, rs[0].Index)
			}
			all, e := s.storage.GetLogs(k, 1, 7, 0)
			require.NoError(t, e)
			require.Len(t, all, 6)
			require.True(t, types.IsNoop(all[4]))
			for _, start := range []uint64{1, 5, 6} {
				logs, e := s.storage.GetLogs(k, start, 7, 1)
				require.NoError(t, e)
				require.Len(t, logs, 1)
				require.Equal(t, start, logs[0].Index)
			}
			next, e := db.LoadNextRangeMsgs(c.ID, c.Type, 5, 7, 0)
			require.NoError(t, e)
			require.Len(t, next, 1)
			require.EqualValues(t, 6, next[0].MessageSeq)
			prev, e := db.LoadPrevRangeMsgs(c.ID, c.Type, 6, 0, 10)
			require.NoError(t, e)
			require.Len(t, prev, 5)
			last, e := db.LoadLastMsgs(c.ID, c.Type, 10)
			require.NoError(t, e)
			require.Len(t, last, 5)
			a, e := db.GetChannelAppliedIndex(c.ID, c.Type)
			require.NoError(t, e)
			require.EqualValues(t, 6, a)
			h, e := db.RaftHardState(k)
			require.NoError(t, e)
			require.EqualValues(t, 6, h.CommittedFloor)
		}
		assertOldBusinessRows(t, db, report, 1)
	}
	run(false)
	assertOldPhysicalRows(t, dir, report, true)
	run(true)
	assertOldPhysicalRows(t, dir, report, true)
}

func TestPinnedOldStoreLaggingDivergentReadAndSubscriberCycle(t *testing.T) {
	for _, name := range []string{"lagging", "divergent"} {
		t.Run(name, func(t *testing.T) {
			dir, report := readOldFixture(t, name)
			db := oldFixtureDB(t, dir, 1)
			assertOldBusinessRows(t, db, report, 0)
			for _, c := range report.Channels {
				k := wkutil.ChannelToKey(c.ID, c.Type)
				for term, index := range c.Terms {
					got, e := db.LeaderTermStartIndex(k, term)
					require.NoError(t, e)
					require.Equal(t, index, got)
				}
				state, e := newStorage(db, nil).GetState(c.ID, c.Type)
				require.NoError(t, e)
				require.EqualValues(t, len(c.Messages), state.LastLogIndex)
				require.Zero(t, state.HardState.CommittedFloor)
			}
			// This is subscriber membership, not a dynamic Raft-voter transition.
			const side = "subscriber-cycle"
			members, e := db.GetSubscribers(side, 2)
			require.NoError(t, e)
			require.Empty(t, members)
			now := time.Unix(1700000000, 0)
			require.NoError(t, db.AddSubscribers(side, 2, []wkdb.Member{{Uid: "one", CreatedAt: &now, UpdatedAt: &now}, {Uid: "two", CreatedAt: &now, UpdatedAt: &now}}))
			members, e = db.GetSubscribers(side, 2)
			require.NoError(t, e)
			require.Len(t, members, 2)
			require.NoError(t, db.RemoveSubscribers(side, 2, []string{"one", "two"}))
			members, e = db.GetSubscribers(side, 2)
			require.NoError(t, e)
			require.Empty(t, members)
			require.NoError(t, db.Close())
			assertOldPhysicalRows(t, dir, report, false)
			db = oldFixtureDB(t, dir, 1)
			defer db.Close()
			assertOldBusinessRows(t, db, report, 0)
			members, e = db.GetSubscribers(side, 2)
			require.NoError(t, e)
			require.Empty(t, members)
		})
	}
}

func TestPinnedOldStoreRejectsMalformedFramesWithoutChangingRows(t *testing.T) {
	dir, report := readOldFixture(t, "malformed")
	db := oldFixtureDB(t, dir, 1)
	assertOldBusinessRows(t, db, report, 0)
	for _, c := range report.Channels {
		_, e := newStorage(db, nil).GetLogs(wkutil.ChannelToKey(c.ID, c.Type), 1, 0, 0)
		require.ErrorContains(t, e, "header_not_recv")
	}
	require.NoError(t, db.Close())
	assertOldPhysicalRows(t, dir, report, false)
}

func TestPinnedOldStoreMissingQuorumDoesNotPublishFloor(t *testing.T) {
	dir, report := readOldFixture(t, "noquorum")
	func() {
		db := oldFixtureDB(t, dir, 1)
		defer db.Close()
		assertOldBusinessRows(t, db, report, 0)
		s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(1), WithTransport(retryNetwork())))
		require.NoError(t, s.Start())
		defer stopServer(t, s)
		for _, c := range report.Channels {
			require.NoError(t, s.createAndApplyConfig(context.Background(), c.Config))
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			m := atomicFixtureMessage(c.ID, c.Type, int64(99000+int(c.Type)*100+len(c.ID)), "uncommitted")
			data, e := m.Marshal()
			require.NoError(t, e)
			_, e = s.proposeMessages(ctx, c.ID, c.Type, types.ProposeReqSet{{Id: uint64(m.MessageID), Data: data}})
			cancel()
			require.Error(t, e)
			h, e := db.RaftHardState(wkutil.ChannelToKey(c.ID, c.Type))
			require.NoError(t, e)
			require.Zero(t, h.CommittedFloor, "a stored tail cannot manufacture commitment")
			a, e := db.GetChannelAppliedIndex(c.ID, c.Type)
			require.NoError(t, e)
			require.Equal(t, c.Applied, a)
		}
	}()
	assertOldPhysicalRows(t, dir, report, true)
	db := oldFixtureDB(t, dir, 1)
	defer db.Close()
	for _, c := range report.Channels {
		h, e := db.RaftHardState(wkutil.ChannelToKey(c.ID, c.Type))
		require.NoError(t, e)
		require.Zero(t, h.CommittedFloor)
		a, e := db.GetChannelAppliedIndex(c.ID, c.Type)
		require.NoError(t, e)
		require.Equal(t, c.Applied, a)
	}
}
