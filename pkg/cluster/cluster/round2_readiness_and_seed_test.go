package cluster

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func TestRound2_BFR2_5_ReversiblePostBootstrapStorageAndChannelReadiness(t *testing.T) {
	s, _ := newConversationConsistencyServer(t, nil)

	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, s.CheckLocalProcessHealth())

	// 1. Post-bootstrap local storage directory becomes read-only (chmod a-w):
	// CheckLocalServiceReady (/readyz) must fail and reset bootstrapReady to false,
	// while CheckLocalProcessHealth (/healthz) must remain healthy.
	wkdbDir := filepath.Join(s.opts.DataDir, "db", "wukongimdb")
	require.NoError(t, os.Chmod(wkdbDir, 0500))
	t.Cleanup(func() { _ = os.Chmod(wkdbDir, 0700) })

	err := s.CheckLocalServiceReady()
	require.Error(t, err, "CheckLocalServiceReady must fail when local wukongimdb directory is read-only after bootstrap")
	require.False(t, s.bootstrapReady.Load(), "bootstrapReady latch must reset to false on post-bootstrap storage failure")
	require.NoError(t, s.CheckLocalProcessHealth(), "CheckLocalProcessHealth (/healthz) must not kill liveness on storage readiness degradation")

	// Restore write permission: CheckLocalServiceReady must recover automatically.
	require.NoError(t, os.Chmod(wkdbDir, 0700))
	require.NoError(t, s.CheckLocalServiceReady())
	require.True(t, s.bootstrapReady.Load())

	// 2. Post-bootstrap channel Raft apply failure must remove readiness without killing liveness,
	// and recover once ApplyResp succeeds.
	cfg := consistencyConfig()
	_, err = s.store.SaveChannelClusterConfig(cfg)
	require.NoError(t, err)
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))

	msg := consistencyMessage(1)
	_, err = s.store.AppendMessages(s.cancelCtx, cfg.ChannelId, cfg.ChannelType, []wkdb.Message{msg})
	require.NoError(t, err)

	chKey := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	ch := s.channelServer.Channel(cfg.ChannelId, cfg.ChannelType)
	require.NotNil(t, ch)

	s.channelServer.AddEvent(chKey, rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Reason: rafttypes.ReasonError,
	})
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() != nil && !s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond, "CheckLocalServiceReady must fail when channel raft apply failure occurs")
	require.NoError(t, s.CheckLocalProcessHealth(), "liveness must remain healthy during channel apply failure")

	s.channelServer.AddEvent(chKey, rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Index:  ch.AppliedIndex(),
		Reason: rafttypes.ReasonOk,
	})
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond, "CheckLocalServiceReady must recover once channel ApplyResp succeeds")

	// 3. Slot Raft store/apply failure also removes readiness reversibly without killing liveness
	slotRaft := s.slotServer.GetSlotRaft(0)
	require.NotNil(t, slotRaft)
	s.slotServer.AddEvent("0", rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Reason: rafttypes.ReasonError,
	})
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() != nil && !s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, s.CheckLocalProcessHealth())

	s.slotServer.AddEvent("0", rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Index:  slotRaft.AppliedIndex(),
		Reason: rafttypes.ReasonOk,
	})
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond)
}

func TestRound3_BFR3_2_CrossSlotProposeAndLocalConversationFence(t *testing.T) {
	s, _ := newConversationConsistencyServer(t, nil)
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil
	}, 5*time.Second, 20*time.Millisecond)

	// 1. Cross-slot / general Store proposals (UpdateUserPluginNo, RemovePluginUser, SaveChannelClusterConfig, AddOrUpdateUserConversations)
	require.NoError(t, s.store.UpdateUserPluginNo("u_plugin_1", "plugin.alpha"))
	require.NoError(t, s.store.RemovePluginUser("u_plugin_1", "plugin.alpha"))

	now := time.Now()
	uid := "u_conv_fence_test"
	conv := wkdb.Conversation{
		Uid:          uid,
		Type:         wkdb.ConversationTypeChat,
		ChannelId:    "g_conv_1",
		ChannelType:  2,
		ReadToMsgSeq: 12,
		CreatedAt:    &now,
		UpdatedAt:    &now,
	}
	// On authoritative local slot leader, AddOrUpdateUserConversationsForLocalLeader succeeds:
	require.NoError(t, s.store.AddOrUpdateUserConversationsForLocalLeader(uid, []wkdb.Conversation{conv}))
}

func TestRound3_BFR3_5_AllShardsSlotLogdbAndConcurrentReadinessProbes(t *testing.T) {
	s, _ := newConversationConsistencyServer(t, nil)

	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 5*time.Second, 20*time.Millisecond)

	// 1. Verify read-only failure & recovery across wukongimdb shards, cluster/logdb shards, and cfglogdb
	targetDirs := []string{
		filepath.Join(s.opts.DataDir, "db", "wukongimdb", "shard000"),
		filepath.Join(s.opts.DataDir, "cluster", "logdb"),
		filepath.Join(s.opts.DataDir, "cluster", "logdb", "shard000"),
		filepath.Join(filepath.Dir(s.opts.ConfigOptions.ConfigPath), "cfglogdb"),
	}
	for _, dir := range targetDirs {
		if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
			continue
		}
		require.NoError(t, os.Chmod(dir, 0500), "chmod 0500 %s", dir)
		err := s.CheckLocalServiceReady()
		require.Error(t, err, "CheckLocalServiceReady must fail immediately when %s is read-only", dir)
		require.False(t, s.bootstrapReady.Load())
		require.NoError(t, os.Chmod(dir, 0700), "chmod 0700 %s", dir)
		require.NoError(t, s.CheckLocalServiceReady(), "CheckLocalServiceReady must recover when %s is writable", dir)
		require.True(t, s.bootstrapReady.Load())
	}

	// 2. Concurrent CheckLocalServiceReady calls (simulating concurrent onConnect + /readyz) must never race
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				require.NoError(t, s.CheckLocalServiceReady())
			}
		}()
	}
	wg.Wait()
}
