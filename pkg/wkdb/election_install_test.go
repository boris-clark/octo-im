package wkdb_test

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestChannelConditionalInstallRejectsFullIdentityAndConcurrentControllers(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	defer db.Close()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "cas", ChannelType: 2, LeaderId: 1, Term: 7, InstalledTerm: 7, ConfVersion: 1, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: 3, MigrateTo: 4}
	require.NoError(t, db.SaveChannelClusterConfig(cfg))
	expected, err := db.GetChannelClusterConfig("cas", 2)
	require.NoError(t, err)
	next := expected.Clone()
	next.Term = 8
	next.InstalledTerm = 8
	next.ConfVersion = 2
	next.LeaderId = 2
	for _, mutate := range []func(*wkdb.ChannelClusterConfig){func(c *wkdb.ChannelClusterConfig) { c.Replicas = []uint64{1, 2} }, func(c *wkdb.ChannelClusterConfig) { c.Learners = nil }, func(c *wkdb.ChannelClusterConfig) { c.MigrateFrom = 2 }, func(c *wkdb.ChannelClusterConfig) { c.MigrateTo = 2 }, func(c *wkdb.ChannelClusterConfig) { c.Term = 6 }} {
		bad := expected.Clone()
		mutate(&bad)
		candidate := next.Clone()
		candidate.Expected = &bad
		require.NoError(t, db.SaveChannelClusterConfig(candidate))
		current, err := db.GetChannelClusterConfig("cas", 2)
		require.NoError(t, err)
		require.True(t, expected.Equal(current))
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, leader := range []uint64{2, 3} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidate := next.Clone()
			candidate.LeaderId = leader
			candidate.Expected = &expected
			results <- db.SaveChannelClusterConfig(candidate)
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	winner, err := db.GetChannelClusterConfig("cas", 2)
	require.NoError(t, err)
	require.EqualValues(t, 8, winner.Term)
	loser := next.Clone()
	if winner.LeaderId == 2 {
		loser.LeaderId = 3
	}
	loser.Expected = &expected
	require.NoError(t, db.SaveChannelClusterConfig(loser))
	current, err := db.GetChannelClusterConfig("cas", 2)
	require.NoError(t, err)
	require.True(t, winner.Equal(current))
	// An unconditional same-term or stale-term write cannot bypass applied CAS.
	loser.Expected = nil
	require.NoError(t, db.SaveChannelClusterConfig(loser))
	require.NoError(t, db.SaveChannelClusterConfig(expected))
	current, err = db.GetChannelClusterConfig("cas", 2)
	require.NoError(t, err)
	require.True(t, winner.Equal(current))
}
