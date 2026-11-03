package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

// A version advance must install a certified term against the old applied
// identity, including when a genuine partial fence already exists.
func TestConfigVersionPublicationCertifiesAppliedVoters(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("version-publication", 2)
	require.NoError(t, err)
	report, err := s.electionFence(ctx, electionFenceRequest{Channel: &cfg, Term: cfg.Term + 1})
	require.NoError(t, err)
	require.True(t, report.Fenced)
	version, err := s.saveChannelConfig(ctx, cfg)
	require.NoError(t, err, "a legitimate version advance must complete a certified installation")
	next, err := s.loadConversationConfigLocal(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Greater(t, version, cfg.ConfVersion, "a legitimate version advance must publish a new certified version")
	require.Greater(t, next.Term, cfg.Term, "version publication must certify the OLD applied identity before advancing it")
	require.Equal(t, next.Term, next.InstalledTerm)
	require.NoError(t, s.reconcileChannelConfig(ctx, channelConfigKey{cfg.ChannelId, cfg.ChannelType}))
	msg := consistencyMessage(8004)
	msg.ChannelID, msg.MessageSeq = cfg.ChannelId, 0
	resps, err := s.store.AppendMessages(ctx, cfg.ChannelId, cfg.ChannelType, []wkdb.Message{msg})
	require.NoError(t, err)
	require.Len(t, resps, 1)
	_, err = s.saveChannelConfig(ctx, cfg)
	require.ErrorIs(t, err, raft.ErrConfigVersionStale)
	// Repeated legitimate advances must remain serviceable, not freeze versions.
	_, err = s.saveChannelConfig(ctx, next)
	require.NoError(t, err)
}

func TestConfigVersionPublicationInstallsBeforeNextFence(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("version-install-before-fence", 2)
	require.NoError(t, err)
	_, err = s.electionFence(ctx, electionFenceRequest{Channel: &cfg, Term: cfg.Term + 1})
	require.NoError(t, err)
	_, err = s.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	next, err := s.loadConversationConfigLocal(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	// No runtime hint has installed next. The production fence entrypoint must
	// install this exact certified row before fencing its complete new identity.
	_, err = s.saveChannelConfig(ctx, next)
	require.NoError(t, err, "a certified applied version must install before the next fence, without waiting for a hint")
	latest, err := s.loadConversationConfigLocal(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Greater(t, latest.Term, next.Term)
	require.Greater(t, latest.ConfVersion, next.ConfVersion)
}

func TestConfigVersionPublicationCertifiesLegacyIdentity(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	legacy := wkdb.ChannelClusterConfig{ChannelId: "version-legacy", ChannelType: 2, LeaderId: 1, Term: 1, Replicas: []uint64{1}, ReplicaMaxCount: 1}
	_, err := s.saveChannelConfig(ctx, legacy)
	require.NoError(t, err)
	legacy, err = s.loadConversationConfigLocal(ctx, legacy.ChannelId, legacy.ChannelType)
	require.NoError(t, err)
	require.Zero(t, legacy.InstalledTerm)
	_, err = s.electionFence(ctx, electionFenceRequest{Channel: &legacy, Term: 2})
	require.NoError(t, err)
	_, err = s.saveChannelConfig(ctx, legacy)
	require.NoError(t, err)
	next, err := s.loadConversationConfigLocal(ctx, legacy.ChannelId, legacy.ChannelType)
	require.NoError(t, err)
	require.Greater(t, next.ConfVersion, legacy.ConfVersion)
	require.EqualValues(t, 2, next.InstalledTerm)
	require.EqualValues(t, 1, next.BoundaryTerm)
	require.NoError(t, s.reconcileChannelConfig(ctx, channelConfigKey{legacy.ChannelId, legacy.ChannelType}))
}

// The old leader is actually stopped before the production transition RPC.
// The remaining two voters must install, commit/apply and reopen without it.
func TestConfigVersionPublicationSurvivorQuorum(t *testing.T) {
	c := newOpCluster(t, 3, 3, 3)
	owned := map[*Server]bool{}
	for _, s := range c.servers {
		owned[s] = true
	}
	// Register after the helper's TempDirs so every test-owned server joins
	// before those directories are removed, including assertion failures.
	t.Cleanup(func() {
		for _, s := range c.servers {
			owned[s] = true
		}
		for s := range owned {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			if err := s.StopWithContext(ctx); err != nil {
				t.Errorf("join test server: %v", err)
			}
			cancel()
		}
		c.servers = map[uint64]*Server{}
	})
	ctrl := c.controller(t)
	st := slotSnapshot(ctrl, 0)
	if st.Leader != 3 {
		require.NoError(t, ctrl.cfgServer.ProposeMigrateSlot(st.Id, st.Leader, 3))
		_, ok := waitMigration(t, c, st.Id, func(s *pb.Slot) bool { return s.Leader == 3 && s.MigrateFrom == 0 && s.MigrateTo == 0 }, 30*time.Second)
		require.True(t, ok)
	}
	id := c.channelOnSlot(t, "failover", 0)
	indexes := map[int64]uint64{}
	for msg := int64(8001); msg <= 8003; msg++ {
		indexes[msg] = c.append(t, ctrl, id, msg)
	}
	for msg, index := range indexes {
		c.verify(t, id, index, msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg, err := ctrl.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	require.EqualValues(t, 3, cfg.LeaderId)
	for _, s := range c.servers {
		s.configReconciler.stop()
	}
	data, err := json.Marshal(electionFenceRequest{Channel: &cfg, Term: cfg.Term + 1})
	require.NoError(t, err)
	body, err := ctrl.rpcClient.requestWithContext(ctx, 2, "/rpc/election/fence", data)
	require.NoError(t, err)
	var partial rt.ElectionReport
	require.NoError(t, json.Unmarshal(body, &partial))
	require.True(t, partial.Fenced)
	require.EqualValues(t, 3, partial.Floor)
	stopped := c.servers[3]
	opts := *stopped.opts
	require.NoError(t, stopped.Stop())
	delete(c.servers, 3)
	var owner *Server
	require.Eventually(t, func() bool {
		for _, s := range c.servers {
			st := slotSnapshot(s, 0)
			if st != nil && st.Leader != 0 && st.Leader != 3 {
				owner = c.servers[st.Leader]
				return owner != nil
			}
		}
		return false
	}, 45*time.Second, 50*time.Millisecond)
	var next wkdb.ChannelClusterConfig
	caller := c.servers[uint64(3)-owner.opts.ConfigOptions.NodeId]
	require.Eventually(t, func() bool {
		var err error
		next, err = caller.saveChannelConfigTransition(ctx, id, 2, channelElectionConfig(cfg))
		if err != nil {
			t.Logf("bounded transition retry: %v", err)
			return false
		}
		return true
	}, 20*time.Second, 100*time.Millisecond)
	require.Greater(t, next.Term, cfg.Term, "a version-only request must complete a certified install with the survivor quorum")
	require.Greater(t, next.ConfVersion, cfg.ConfVersion)
	require.NotEqualValues(t, 3, next.LeaderId)
	t.Logf("slot0 owner=%d old channel leader=3 stopped; old=%+v new=%+v partial=%+v", owner.opts.ConfigOptions.NodeId, cfg, next, partial)
	for _, s := range c.servers {
		require.NoError(t, s.reconcileChannelConfig(ctx, channelConfigKey{id, 2}))
	}
	for msg := int64(8004); msg <= 8006; msg++ {
		indexes[msg] = c.append(t, owner, id, msg)
	}
	for _, s := range c.servers {
		requireVersionMessages(t, s, id, indexes)
	}
	exact := map[int64]wkdb.Message{}
	for msg, index := range indexes {
		stored, err := c.servers[1].db.LoadMsg(id, 2, index)
		require.NoError(t, err)
		exact[msg] = stored
	}
	// Reopen only the healthy quorum first. Voter3 is still stopped.
	old := c.servers
	hardBefore := map[uint64]rt.HardState{}
	for node, s := range old {
		hardBefore[node], err = s.db.RaftHardState(wkutil.ChannelToKey(id, 2))
		require.NoError(t, err)
		require.Equal(t, next.Term, hardBefore[node].InstalledTerm)
		require.Equal(t, next.LeaderId, hardBefore[node].InstalledLeader)
	}
	for _, s := range old {
		require.NoError(t, s.Stop())
	}
	c.servers = map[uint64]*Server{}
	for node, s := range old {
		op := *s.opts
		op.ChannelTransport, op.SlotTransport = nil, nil
		n := New(&op)
		owned[n] = true
		require.NoError(t, n.Start())
		c.servers[node] = n
		reopened, err := n.db.RaftHardState(wkutil.ChannelToKey(id, 2))
		require.NoError(t, err)
		require.Equal(t, hardBefore[node], reopened, "reopen must retain every exact durable fence/install identity, term and committed floor")
	}
	indexes[8007] = c.append(t, c.servers[1], id, 8007)
	for _, s := range c.servers {
		requireVersionMessages(t, s, id, indexes)
		for msg, stored := range exact {
			reopened, err := s.db.LoadMsg(id, 2, indexes[msg])
			require.NoError(t, err)
			require.Equal(t, stored, reopened, "all committed message columns and original indexes survive reopen")
		}
	}
	// Only after quorum recovery is proved may the stopped voter catch up.
	opts.ChannelTransport, opts.SlotTransport = nil, nil
	n := New(&opts)
	owned[n] = true
	require.NoError(t, n.Start())
	c.servers[3] = n
	c.waitReady(t)
	for msg, index := range indexes {
		c.verify(t, id, index, msg)
	}
	c.restartAll(t)
	indexes[8008] = c.append(t, c.servers[1], id, 8008)
	for msg, index := range indexes {
		c.verify(t, id, index, msg)
	}
}

func TestConfigVersionPublicationMissingQuorumCannotUseForgedCertificate(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "version-no-quorum", ChannelType: 2, Term: 1, InstalledTerm: 1, LeaderId: 1, Replicas: []uint64{1, 98, 99}, ReplicaMaxCount: 3}
	_, err := s.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg, err = s.loadConversationConfigLocal(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	for _, forged := range []bool{false, true} {
		next := cfg.Clone()
		if forged {
			next.Term, next.InstalledTerm = 2, 2
			next.Expected = &cfg
			next.Membership = &rt.MembershipCertificate{Reports: map[uint64]rt.ElectionReport{98: {NodeID: 98, Fenced: true, Term: 2}}}
		}
		_, err = s.saveChannelConfig(ctx, next)
		require.ErrorIs(t, err, ErrConversationReadRetry)
		require.Contains(t, err.Error(), "durable applied-voter fence has no majority")
		require.Contains(t, err.Error(), "voter=98 phase=transport")
		require.Contains(t, err.Error(), "voter=99 phase=transport")
		applied, readErr := s.loadConversationConfigLocal(ctx, cfg.ChannelId, cfg.ChannelType)
		require.NoError(t, readErr)
		require.True(t, applied.Equal(cfg), "failed certification must leave the exact OLD authoritative row intact")
	}
}

func TestElectionFenceRPCPreservesSafeFullIdentityRefusal(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 2, 1)
	owner, peer := waitAppliedInstallSlot(t, ctx, servers, "version-rpc-refusal")
	cfg := wkdb.ChannelClusterConfig{ChannelId: "version-rpc-refusal", ChannelType: 2, Term: 1, InstalledTerm: 1, LeaderId: peer.opts.ConfigOptions.NodeId, Replicas: []uint64{1, 2}, ReplicaMaxCount: 2}
	_, err := owner.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg, err = owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		applied, readErr := peer.fencedChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
		return readErr == nil && applied.Equal(cfg)
	}, 5*time.Second, 10*time.Millisecond)
	bad := cfg.Clone()
	bad.ConfVersion++
	data, err := json.Marshal(electionFenceRequest{Channel: &bad, Term: 2})
	require.NoError(t, err)
	_, err = owner.rpcClient.requestWithContext(ctx, peer.opts.ConfigOptions.NodeId, "/rpc/election/fence", data)
	var refusal *fenceRPCError
	require.True(t, errors.As(err, &refusal))
	require.Contains(t, err.Error(), "phase=applied-identity")
	require.Contains(t, err.Error(), fmt.Sprintf("current_version=%d", cfg.ConfVersion))
	require.Contains(t, err.Error(), fmt.Sprintf("expected_identity=%x", rt.ElectionIdentity(channelElectionConfig(bad))))
	require.Contains(t, err.Error(), fmt.Sprintf("current_identity=%x", rt.ElectionIdentity(channelElectionConfig(cfg))))
	collection, err := owner.collectElectionFences(ctx, channelElectionConfig(bad), electionFenceRequest{Channel: &bad, Term: 2})
	require.NoError(t, err)
	require.Empty(t, collection.reports)
	require.Contains(t, collection.refusalError().Error(), fmt.Sprintf("voter=%d phase=receiver", peer.opts.ConfigOptions.NodeId))
	require.Contains(t, collection.refusalError().Error(), "phase=applied-identity")
	for _, private := range []string{"database /private/path failed", "token=secret 10.0.0.1:1234", string(make([]byte, 1024))} {
		require.Equal(t, "election fence phase=receiver", safeFenceRefusal([]byte(private)))
	}
}

func requireVersionMessages(t *testing.T, s *Server, id string, indexes map[int64]uint64) {
	t.Helper()
	for msg, index := range indexes {
		require.Eventually(t, func() bool { a, err := s.db.GetChannelAppliedIndex(id, 2); return err == nil && a >= index }, 15*time.Second, 20*time.Millisecond)
		stored, err := s.db.LoadMsg(id, 2, index)
		require.NoError(t, err)
		require.Equal(t, msg, stored.MessageID)
		require.Equal(t, fmt.Sprintf("identity-%d", msg), string(stored.Payload))
		hard, err := s.db.RaftHardState(wkutil.ChannelToKey(id, 2))
		require.NoError(t, err)
		require.GreaterOrEqual(t, hard.CommittedFloor, index)
	}
}
