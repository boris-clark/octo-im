package wkdb_test

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestCertifiedChannelVersionRejectsUnconditionalPublication(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	defer db.Close()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "raw-version", ChannelType: 2, Term: 1, InstalledTerm: 1, LeaderId: 1, ConfVersion: 1, Replicas: []uint64{1, 2, 3}, ReplicaMaxCount: 3}
	require.NoError(t, db.SaveChannelClusterConfig(cfg))
	old, err := db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	next := old.Clone()
	next.ConfVersion++
	require.NoError(t, db.SaveChannelClusterConfig(next))
	current, err := db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.True(t, current.Equal(old), "an uncertified version change must not strand the old full fence identity")
	// An Expected pointer alone cannot certify a same-term version advance.
	next.Expected = &old
	require.NoError(t, db.SaveChannelClusterConfig(next))
	current, err = db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.True(t, current.Equal(old))
}
