package wkdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A cache update must reflect the applied conditional install, never a delayed
// uncertified leader overwrite. Reopening must expose the same full identity.
func TestConfigCacheReflectsConditionalInstall(t *testing.T) {
	opts := NewOptions(WithDir(t.TempDir()), WithShardNum(1))
	db := NewWukongDB(opts)
	require.NoError(t, db.Open())
	defer func() { require.NoError(t, db.Close()) }()
	cfg := ChannelClusterConfig{ChannelId: "cache-install", ChannelType: 2, Replicas: []uint64{1, 2, 3},
		LeaderId: 1, Term: 10, InstalledTerm: 10, ConfVersion: 100}
	require.NoError(t, db.SaveChannelClusterConfig(cfg))
	expected, err := db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	next := expected.Clone()
	next.Term, next.InstalledTerm, next.LeaderId, next.ConfVersion = 12, 12, 2, 200
	next.Expected = &expected
	require.NoError(t, db.SaveChannelClusterConfig(next))
	applied, err := db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.True(t, applied.Equal(next))
	require.Nil(t, applied.Expected)
	for _, stale := range []ChannelClusterConfig{expected, next} {
		stale.LeaderId, stale.ConfVersion = 3, 300
		require.NoError(t, db.SaveChannelClusterConfig(stale))
		current, err := db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
		require.NoError(t, err)
		require.True(t, applied.Equal(current))
	}
	require.NoError(t, db.Close())
	db = NewWukongDB(opts)
	require.NoError(t, db.Open())
	current, err := db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.True(t, applied.Equal(current))
}
