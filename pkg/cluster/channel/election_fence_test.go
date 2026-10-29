package channel

import (
	"context"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestExistingChannelDurableFenceAndContinuedProposal(t *testing.T) {
	tr := retryNetwork()
	nodes := make([]*Server, 3)
	dbs := make([]wkdb.DB, 3)
	for i := range nodes {
		dbs[i] = retryDB(t)
		nodes[i] = retryServer(t, uint64(i+1), dbs[i], []uint64{1, 2, 3}, tr)
	}
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Term: 1, ConfVersion: 1, Replicas: []uint64{1, 2, 3}}
	first := retryMessage(701, "sender", "before-fence")
	responses := retryPropose(t, nodes[0], first)
	require.Len(t, responses, 1)
	idx := responses[0].Index
	key := wkutil.ChannelToKey("retry", 2)
	require.Eventually(t, func() bool {
		for _, db := range dbs {
			a, e := db.GetChannelAppliedIndex("retry", 2)
			if e != nil || a < idx {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reports := map[uint64]types.ElectionReport{}
	for i, s := range nodes {
		report, err := s.FenceElection(ctx, cfg, 2)
		require.NoError(t, err)
		require.True(t, report.Fenced)
		require.GreaterOrEqual(t, report.Floor, idx)
		reports[uint64(i+1)] = report
	}
	leader, err := types.SelectFencedLeader(channelConfigToRaftConfig(1, cfg), 2, reports)
	require.NoError(t, err)
	require.EqualValues(t, 1, leader, "legacy history stays with the fenced leader of record")
	for i, s := range nodes {
		require.NoError(t, s.getRaftGroup(key).Do(ctx, key, func(r raftgroup.IRaft) error {
			require.False(t, r.IsLeader())
			_, err := r.Admit([]types.Log{{Id: 999, Data: []byte("must not be admitted")}})
			require.Error(t, err)
			return nil
		}))
		hard, err := dbs[i].RaftHardState(key)
		require.NoError(t, err)
		require.EqualValues(t, 2, hard.FenceTerm)
		require.EqualValues(t, 1, hard.BoundaryTerm)
	}
	// This actual storage CAS is the apply primitive used by the slot store.
	authority := retryDB(t)
	require.NoError(t, authority.SaveChannelClusterConfig(cfg))
	expected, err := authority.GetChannelClusterConfig("retry", 2)
	require.NoError(t, err)
	next := expected.Clone()
	next.Term = 2
	next.InstalledTerm = 2
	next.BoundaryTerm = 1
	next.ConfVersion = 2
	next.Expected = &expected
	require.NoError(t, authority.SaveChannelClusterConfig(next))
	installed, err := authority.GetChannelClusterConfig("retry", 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, installed.Term)
	for _, s := range nodes {
		require.NoError(t, s.SwitchConfig("retry", 2, installed))
	}
	after := retryMessage(702, "sender", "continued-after-fence")
	r := retryPropose(t, nodes[0], after)
	require.Equal(t, idx+1, r[0].Index)
	require.Eventually(t, func() bool {
		for _, db := range dbs {
			hard, e := db.RaftHardState(key)
			if e != nil || hard.CommittedFloor < idx+1 {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	for _, db := range dbs {
		before, err := db.LoadMsg("retry", 2, idx)
		require.NoError(t, err)
		require.EqualValues(t, 701, before.MessageID)
		require.EqualValues(t, 1, before.Term)
		require.Equal(t, first.Payload, before.Payload)
		require.ErrorIs(t, db.TruncateLogTo("retry", 2, idx), types.ErrTruncateBelowApplied)
		retained, err := db.LoadMsg("retry", 2, idx+1)
		require.NoError(t, err)
		require.EqualValues(t, 702, retained.MessageID)
		require.EqualValues(t, 2, retained.Term)
	}
}

func TestExistingChannelCommitFloorReopensAndProtectsExactLog(t *testing.T) {
	dir := t.TempDir()
	var db wkdb.DB
	open := func() {
		db = wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(1)))
		require.NoError(t, db.Open())
	}
	open()
	t.Cleanup(func() {
		if db != nil {
			require.NoError(t, db.Close())
		}
	})
	s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
	m := retryMessage(801, "sender", "durable-before-restart")
	response := retryPropose(t, s, m)
	require.EqualValues(t, 1, response[0].Index)
	key := wkutil.ChannelToKey("retry", 2)
	expected, err := s.storage.GetLogs(key, 1, 2, 0)
	require.NoError(t, err)
	require.Len(t, expected, 1)
	hard, err := db.RaftHardState(key)
	require.NoError(t, err)
	require.EqualValues(t, 1, hard.CommittedFloor)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Term: 1, ConfVersion: 1, Replicas: []uint64{1}}
	require.NoError(t, db.SaveChannelClusterConfig(cfg))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	report, err := s.FenceElection(ctx, cfg, 2)
	require.NoError(t, err)
	require.True(t, report.Fenced)
	stopServer(t, s)
	require.NoError(t, db.Close())
	db = nil
	open()
	s = retryServer(t, 1, db, []uint64{1}, retryNetwork())
	state, err := s.storage.GetState("retry", 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.HardState.CommittedFloor)
	again, err := s.storage.GetLogs(key, 1, 2, 0)
	require.NoError(t, err)
	require.True(t, types.SameLogs(expected, again))
	// A pre-install restart reconstructs only the persisted expected membership.
	report, err = s.FenceElection(ctx, cfg, 2)
	require.NoError(t, err)
	require.True(t, report.Fenced)
	expectedConfig, err := db.GetChannelClusterConfig("retry", 2)
	require.NoError(t, err)
	installed := expectedConfig.Clone()
	installed.Term = 2
	installed.InstalledTerm = 2
	installed.BoundaryTerm = 1
	installed.ConfVersion = 2
	installed.Expected = &expectedConfig
	require.NoError(t, db.SaveChannelClusterConfig(installed))
	installed, err = db.GetChannelClusterConfig("retry", 2)
	require.NoError(t, err)
	require.NoError(t, s.SwitchConfig("retry", 2, installed))
	response = retryPropose(t, s, retryMessage(802, "sender", "continued-after-restart"))
	require.EqualValues(t, 2, response[0].Index)
	require.ErrorIs(t, db.TruncateLogTo("retry", 2, 1), types.ErrTruncateBelowApplied)
	again, err = s.storage.GetLogs(key, 1, 2, 0)
	require.NoError(t, err)
	require.True(t, types.SameLogs(expected, again))
}
