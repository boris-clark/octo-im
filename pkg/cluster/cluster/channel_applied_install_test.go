package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func waitAppliedInstallSlot(t *testing.T, ctx context.Context, servers []*Server, id string) (*Server, *Server) {
	t.Helper()
	var owner *Server
	require.Eventually(t, func() bool {
		owner = readyOrphanMigrationOwner(ctx, servers, id)
		return owner != nil
	}, 5*time.Second, 10*time.Millisecond)
	for _, peer := range servers {
		if peer != owner {
			return owner, peer
		}
	}
	t.Fatal("a remote message leader is required")
	return nil, nil
}

// Real slot publication and TCP reconciliation must repair a dormant store
// without first adopting a leader at a term below its persisted hard state.
func TestConfigRecoveryThroughAppliedInstallRPC(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 2, 1)
	id := "applied-install-recovery"
	owner, leader := waitAppliedInstallSlot(t, ctx, servers, id)
	leaderID := leader.opts.ConfigOptions.NodeId
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: leaderID,
		Term: 1, InstalledTerm: 1, Replicas: []uint64{leaderID}, ReplicaMaxCount: 1}
	_, err := owner.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg, err = owner.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	msg := consistencyMessage(101)
	msg.ChannelID, msg.MessageSeq, msg.Term = id, 1, 1
	require.NoError(t, leader.db.AppendMessages(id, 2, []wkdb.Message{msg}))
	require.NoError(t, leader.db.UpdateChannelAppliedIndex(id, 2, 1))
	key := wkutil.ChannelToKey(id, 2)
	require.NoError(t, leader.db.SaveRaftHardState(key, rt.HardState{Term: 5, Vote: 1}))
	require.False(t, leader.channelServer.ExistChannel(id, 2))
	require.ErrorIs(t, owner.requestChannelConfigReconcile(ctx, cfg), ErrConversationReadRetry)
	recovered, err := owner.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	require.EqualValues(t, 6, recovered.Term)
	require.Equal(t, recovered.Term, recovered.InstalledTerm)
	require.Equal(t, leaderID, recovered.LeaderId)
	require.Equal(t, cfg.Replicas, recovered.Replicas)
	require.Greater(t, recovered.ConfVersion, cfg.ConfVersion)
	require.NoError(t, owner.requestChannelConfigReconcile(ctx, recovered))
	require.NoError(t, leader.ValidateLocalChannelRead(ctx, recovered))
	stored, err := leader.db.LoadMsg(id, 2, 1)
	require.NoError(t, err)
	require.Equal(t, msg.MessageID, stored.MessageID)
	require.Equal(t, msg.Payload, stored.Payload)
	// The recovery barrier occupies its own index. A subsequent proposal must
	// preserve the original message and acquire a fresh committed/applied row.
	msg.MessageID, msg.MessageSeq = 102, 0
	resps, err := owner.store.AppendMessages(ctx, id, 2, []wkdb.Message{msg})
	require.NoError(t, err)
	require.Len(t, resps, 1)
	require.Greater(t, resps[0].Index, uint64(1))
	hard, err := leader.db.RaftHardState(key)
	require.NoError(t, err)
	require.GreaterOrEqual(t, hard.CommittedFloor, resps[0].Index)
	require.Equal(t, recovered.Term, hard.InstalledTerm)
	require.Equal(t, leaderID, hard.InstalledLeader)
}

func TestConfigSwitchRPCRequiresAppliedIdentity(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 2, 1)
	owner, leader := waitAppliedInstallSlot(t, ctx, servers, "install-rpc-negative")
	leaderID := leader.opts.ConfigOptions.NodeId
	cfg := wkdb.ChannelClusterConfig{ChannelId: "install-rpc-negative", ChannelType: 2,
		LeaderId: leaderID, Term: 1, InstalledTerm: 1, Replicas: []uint64{1, 2}, ReplicaMaxCount: 2}
	_, err := owner.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg, err = owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.NoError(t, leader.channelServer.WakeLeaderIfNeed(cfg))
	forged := cfg.Clone()
	forged.Term, forged.InstalledTerm, forged.LeaderId = 2, 2, owner.opts.ConfigOptions.NodeId
	forged.ConfVersion++
	require.Error(t, owner.rpcClient.RequestChannelSwitchConfig(leaderID, forged))
	state, err := leader.channelServer.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, cfg.Term, state.Term)
	require.Equal(t, cfg.ConfVersion, state.ConfigVersion)
	require.Equal(t, cfg.LeaderId, state.LeaderID)
	require.NoError(t, owner.rpcClient.RequestChannelSwitchConfig(leaderID, cfg))
	// The old encoding omits only the derived storage row id. It must still
	// round-trip a matching applied legacy identity without bypassing the fence.
	legacy := wkdb.ChannelClusterConfig{ChannelId: "install-rpc-legacy", ChannelType: 2,
		LeaderId: leaderID, Term: 1, Replicas: []uint64{1, 2}, ReplicaMaxCount: 2}
	_, err = owner.saveChannelConfig(ctx, legacy)
	require.NoError(t, err)
	legacy, err = owner.loadConversationConfig(ctx, legacy.ChannelId, legacy.ChannelType)
	require.NoError(t, err)
	require.NoError(t, leader.channelServer.WakeLeaderIfNeed(legacy))
	require.NoError(t, owner.rpcClient.RequestChannelSwitchConfig(leaderID, legacy))
}

// Fence persistence must precede publication. A transient pre-install failure
// remains retryable; it cannot publish a term that no voter durably fenced.
func TestConfigCallbackRetriesBeforeInstallPersistence(t *testing.T) {
	var db *callbackResumeDB
	s, ctx := newConversationConsistencyServer(t, func(s *Server) {
		db = &callbackResumeDB{DB: s.db, failed: make(chan struct{}, 1)}
		s.channelServer = channel.NewServer(channel.NewOptions(channel.WithNodeId(1), channel.WithGroupCount(1),
			channel.WithTransport(s.opts.ChannelTransport), channel.WithSlot(s.slotServer), channel.WithNode(s.cfgServer),
			channel.WithCluster(s), channel.WithDB(db), channel.WithRPC(s.rpcClient), channel.WithOnSaveConfig(s.onSaveChannelConfig)))
		s.store = store.New(store.NewOptions(store.WithNodeId(1), store.WithSlot(s.slotServer), store.WithChannel(s.channelServer), store.WithDB(s.db)))
	})
	s.configReconciler.stop()
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("callback-fence-retry", 2)
	require.NoError(t, err)
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))
	db.fail.Store(true)
	done := make(chan error, 1)
	go func() {
		done <- s.onSaveChannelConfig(cfg.ChannelId, cfg.ChannelType, rt.Config{
			Version: cfg.ConfVersion, Term: cfg.Term + 1, Leader: cfg.LeaderId, Replicas: cfg.Replicas})
	}()
	// Always collect the callback, including when an assertion fails.
	t.Cleanup(func() {
		db.fail.Store(false)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(6 * time.Second):
			t.Error("callback did not finish")
		}
	})
	select {
	case <-db.failed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	current, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, cfg.ConfVersion, current.ConfVersion)
	require.Equal(t, cfg.Term, current.Term)
	db.fail.Store(false)
	require.Eventually(t, func() bool {
		current, err = s.loadConversationConfig(context.Background(), cfg.ChannelId, cfg.ChannelType)
		return err == nil && current.Term == cfg.Term+1 && current.InstalledTerm == current.Term
	}, 3*time.Second, 10*time.Millisecond)
}

// Recovery must not repeatedly fence and then reject a quorum merely because
// the freshest voter differs from the leader recorded in the old metadata.
func TestConfigRecoveryPublishesFreshestFencedVoter(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 2, 1)
	id := "recovery-freshest-voter"
	owner, oldLeader := waitAppliedInstallSlot(t, ctx, servers, id)
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: oldLeader.opts.ConfigOptions.NodeId,
		Term: 1, InstalledTerm: 1, Replicas: []uint64{1, 2}, ReplicaMaxCount: 2}
	_, err := owner.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg, err = owner.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	first := consistencyMessage(201)
	first.ChannelID, first.MessageSeq, first.Term = id, 1, 1
	second := first
	second.MessageID, second.MessageSeq, second.Payload = 202, 2, []byte("freshest-retained-row")
	require.NoError(t, owner.db.AppendMessagesWithTerm(id, 2, []wkdb.Message{first, second}, &rt.TermStartIndexInfo{Term: 1, Index: 1}))
	require.NoError(t, oldLeader.db.AppendMessagesWithTerm(id, 2, []wkdb.Message{first}, &rt.TermStartIndexInfo{Term: 1, Index: 1}))
	require.NoError(t, oldLeader.db.UpdateChannelAppliedIndex(id, 2, 1))
	require.NoError(t, oldLeader.db.SaveRaftHardState(wkutil.ChannelToKey(id, 2), rt.HardState{Term: 5}))
	require.ErrorIs(t, owner.requestChannelConfigReconcile(ctx, cfg), ErrConversationReadRetry)
	recovered, err := owner.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	require.EqualValues(t, 6, recovered.Term)
	require.Equal(t, owner.opts.ConfigOptions.NodeId, recovered.LeaderId)
	require.Equal(t, recovered.Term, recovered.InstalledTerm)
	require.NoError(t, owner.reconcileChannelConfig(ctx, channelConfigKey{id, 2}))
	fresh := first
	fresh.MessageID, fresh.MessageSeq, fresh.ClientMsgNo = 203, 0, "after-freshest-election"
	resps, err := owner.store.AppendMessages(ctx, id, 2, []wkdb.Message{fresh})
	require.NoError(t, err)
	require.Len(t, resps, 1)
	require.Greater(t, resps[0].Index, uint64(2))
	require.Eventually(t, func() bool {
		applied, err := oldLeader.db.GetChannelAppliedIndex(id, 2)
		return err == nil && applied >= resps[0].Index
	}, 3*time.Second, 10*time.Millisecond)
	for _, s := range servers {
		stored, err := s.db.LoadMsg(id, 2, 2)
		require.NoError(t, err)
		require.Equal(t, second.MessageID, stored.MessageID)
		require.Equal(t, second.Payload, stored.Payload)
		require.Equal(t, second.Term, stored.Term)
	}
}
