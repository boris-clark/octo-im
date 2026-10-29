package wkdb_test

import (
	"sync"
	"testing"

	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestMembershipInstallCASAndReopen(t *testing.T) {
	opts := wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1))
	db := wkdb.NewWukongDB(opts)
	require.NoError(t, db.Open())
	old := wkdb.ChannelClusterConfig{ChannelId: "membership-cas", ChannelType: 2, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, LeaderId: 1, Term: 7, InstalledTerm: 7, ConfVersion: 11, MigrateFrom: 3, MigrateTo: 4}
	require.NoError(t, db.SaveChannelClusterConfig(old))
	old, err := db.GetChannelClusterConfig(old.ChannelId, 2)
	require.NoError(t, err)
	identity := rt.ElectionIdentity(rt.Config{Replicas: old.Replicas, Learners: old.Learners, Leader: 1, Term: 7, InstalledTerm: 7, Version: 11, MigrateFrom: 3, MigrateTo: 4})
	next := old.Clone()
	next.Replicas, next.Learners = []uint64{1, 2, 4}, nil
	next.Term, next.InstalledTerm, next.ConfVersion = 8, 8, 12
	next.MigrateFrom, next.MigrateTo = 0, 0
	next.Expected = &old
	// The old unconditional and same-membership expected guards remain active.
	require.NoError(t, db.SaveChannelClusterConfig(next))
	got, err := db.GetChannelClusterConfig(old.ChannelId, 2)
	require.NoError(t, err)
	require.True(t, old.Equal(got))
	reports := map[uint64]rt.ElectionReport{}
	for _, id := range []uint64{1, 2, 4} {
		reports[id] = rt.ElectionReport{Identity: identity, NodeID: id, Term: 8, Fenced: true, LastTerm: 7, LastIndex: 20, Floor: 20}
	}
	next.Membership = &rt.MembershipCertificate{Reports: reports}
	bad := next.Clone()
	bad.Expected = nil
	require.NoError(t, db.SaveChannelClusterConfig(bad))
	got, err = db.GetChannelClusterConfig(old.ChannelId, 2)
	require.NoError(t, err)
	require.True(t, old.Equal(got))
	// Competing an election against the membership transition has one winner.
	other := old.Clone()
	other.Term, other.InstalledTerm, other.ConfVersion, other.LeaderId = 8, 8, 13, 2
	other.Expected = &old
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, cfg := range []wkdb.ChannelClusterConfig{next, other} {
		wg.Add(1)
		go func(cfg wkdb.ChannelClusterConfig) { defer wg.Done(); errs <- db.SaveChannelClusterConfig(cfg) }(cfg)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	winner, err := db.GetChannelClusterConfig(old.ChannelId, 2)
	require.NoError(t, err)
	require.EqualValues(t, 8, winner.Term)
	require.True(t, winner.Equal(next) || winner.Equal(other))
	require.Nil(t, winner.Expected)
	require.Nil(t, winner.Membership, "a command witness must not become a reusable read certificate")
	require.NoError(t, db.SaveChannelClusterConfig(next))
	require.NoError(t, db.SaveChannelClusterConfig(other))
	require.NoError(t, db.Close())
	db = wkdb.NewWukongDB(opts)
	require.NoError(t, db.Open())
	defer db.Close()
	got, err = db.GetChannelClusterConfig(old.ChannelId, 2)
	require.NoError(t, err)
	require.True(t, winner.Equal(got))
}
