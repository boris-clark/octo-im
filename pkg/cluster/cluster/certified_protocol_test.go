package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

// protocolTransport only gates the channel data plane. Every delivered event
// still crosses the ordinary TCP transport; slot/config/RPC traffic is untouched.
type protocolTransport struct {
	raftgroup.ITransport
	mu       sync.RWMutex
	key      string
	isolated uint64
}

func (p *protocolTransport) Send(key string, e rt.Event) {
	p.mu.RLock()
	drop := p.key == key && p.isolated != 0 && (e.From == p.isolated || e.To == p.isolated)
	p.mu.RUnlock()
	if !drop {
		p.ITransport.Send(key, e)
	}
}

func (p *protocolTransport) isolate(key string, id uint64) {
	p.mu.Lock()
	p.key, p.isolated = key, id
	p.mu.Unlock()
}

type protocolCluster struct {
	servers    map[uint64]*Server
	transports map[uint64]*protocolTransport
	ctx        context.Context
}

func newProtocolCluster(t *testing.T) *protocolCluster {
	t.Helper()
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	c := &protocolCluster{servers: map[uint64]*Server{}, transports: map[uint64]*protocolTransport{}}
	addresses := map[uint64]string{}
	for id := uint64(1); id <= 3; id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[id] = l.Addr().String()
		require.NoError(t, l.Close())
	}
	t.Cleanup(func() {
		for _, s := range c.servers {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			err := s.StopWithContext(ctx)
			cancel()
			if err != nil {
				t.Errorf("protocol server stop: %v", err)
			}
		}
	})
	for id := uint64(1); id <= 3; id++ {
		p := &protocolTransport{}
		opts := newTestOptions(t, id, addresses, clusterconfig.WithSlotCount(3), clusterconfig.WithSlotMaxReplicaCount(3))
		s := New(NewOptions(WithAddr("tcp://"+addresses[id]), WithConfigOptions(opts), WithDataDir(t.TempDir()),
			WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20), WithChannelTransport(p)))
		p.ITransport = newChannelTransport(s)
		require.NoError(t, s.Start())
		s.configReconciler.stop()
		c.servers[id], c.transports[id] = s, p
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	c.ctx = ctx
	t.Cleanup(cancel)
	for _, s := range c.servers {
		require.NoError(t, s.WaitAllSlotReady(ctx, 3))
	}
	c.owner(t, "protocol-ready")
	return c
}

func (c *protocolCluster) owner(t *testing.T, id string) *Server {
	t.Helper()
	peers := []*Server{c.servers[1], c.servers[2], c.servers[3]}
	var owner *Server
	require.Eventually(t, func() bool { owner = readyOrphanMigrationOwner(c.ctx, peers, id); return owner != nil }, 5*time.Second, 10*time.Millisecond)
	return owner
}

func (c *protocolCluster) restart(t *testing.T, id uint64, channelID string) {
	t.Helper()
	old := c.servers[id]
	require.NoError(t, old.Stop())
	delete(c.servers, id)
	opts := *old.opts
	opts.ChannelTransport, opts.SlotTransport = nil, nil
	s := New(&opts)
	require.NoError(t, s.Start())
	s.configReconciler.stop()
	c.servers[id] = s
	require.NoError(t, s.WaitAllSlotReady(c.ctx, 3))
	require.Eventually(t, func() bool {
		_, err := s.loadConversationConfig(c.ctx, channelID, 2)
		return err == nil || err == wkdb.ErrNotFound
	}, 5*time.Second, 20*time.Millisecond)
}

func (c *protocolCluster) append(t *testing.T, s *Server, id string, msgID int64) uint64 {
	t.Helper()
	m := consistencyMessage(uint32(msgID))
	m.ChannelID, m.MessageSeq = id, 0
	m.Payload = []byte(fmt.Sprintf("identity-%d", msgID))
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	r, err := s.store.AppendMessages(ctx, id, 2, []wkdb.Message{m})
	require.NoError(t, err)
	require.Len(t, r, 1)
	return r[0].Index
}

func (c *protocolCluster) verify(t *testing.T, id string, index uint64, msgID int64, ids ...uint64) {
	t.Helper()
	for _, node := range ids {
		s := c.servers[node]
		require.Eventually(t, func() bool { a, err := s.db.GetChannelAppliedIndex(id, 2); return err == nil && a >= index }, 5*time.Second, 10*time.Millisecond)
		m, err := s.db.LoadMsg(id, 2, index)
		require.NoError(t, err)
		require.Equal(t, msgID, m.MessageID)
		require.EqualValues(t, index, m.MessageSeq)
		require.Equal(t, []byte(fmt.Sprintf("identity-%d", msgID)), m.Payload)
		h, err := s.db.RaftHardState(wkutil.ChannelToKey(id, 2))
		require.NoError(t, err)
		require.GreaterOrEqual(t, h.CommittedFloor, index)
	}
}

// A post-install one-voter channel expands through its ordinary controller
// entry point. Catch-up triggers real remote promotion callbacks; neither test
// selection nor direct metadata/install writes complete the transitions.
func TestCertifiedMembershipExpansionRPC(t *testing.T) {
	c := newProtocolCluster(t)
	id := "certified-membership"
	owner := c.owner(t, id)
	leaderID := uint64(1)
	if owner.opts.ConfigOptions.NodeId == leaderID {
		leaderID = 2
	}
	leader := c.servers[leaderID]
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, Term: 1, InstalledTerm: 1, LeaderId: leaderID, Replicas: []uint64{leaderID}, ReplicaMaxCount: 1}
	_, err := owner.saveChannelConfig(c.ctx, cfg)
	require.NoError(t, err)
	cfg, err = owner.loadConversationConfig(c.ctx, id, 2)
	require.NoError(t, err)
	index := c.append(t, leader, id, 301)
	c.verify(t, id, index, 301, leaderID)
	// Capacity is a metadata-only change, not a voting certificate.
	cfg.ReplicaMaxCount = 3
	_, err = owner.saveChannelConfig(c.ctx, cfg)
	require.NoError(t, err)
	var expanded wkdb.ChannelClusterConfig
	var lastVersion uint64
	require.Eventually(t, func() bool {
		var e error
		expanded, e = leader.GetOrCreateChannelClusterConfigFromSlotLeader(id, 2)
		if e != nil {
			t.Logf("expansion retry: %v", e)
			return false
		}
		if len(expanded.Learners) > 0 {
			e = owner.requestChannelConfigReconcile(c.ctx, expanded)
		}
		if lastVersion != expanded.ConfVersion {
			lastVersion = expanded.ConfVersion
			t.Logf("expansion term=%d voters=%v learners=%v migration=%d/%d reconcile=%v", expanded.Term, expanded.Replicas, expanded.Learners, expanded.MigrateFrom, expanded.MigrateTo, e)
		}
		return len(expanded.Replicas) == 3 && len(expanded.Learners) == 0 && expanded.MigrateFrom == 0 && expanded.MigrateTo == 0
	}, 10*time.Second, 20*time.Millisecond)
	require.Greater(t, expanded.Term, cfg.Term)
	require.Equal(t, expanded.Term, expanded.InstalledTerm)
	require.Greater(t, expanded.ConfVersion, cfg.ConfVersion)
	last := c.append(t, leader, id, 302)
	require.Greater(t, last, index)
	c.verify(t, id, index, 301, 1, 2, 3)
	c.verify(t, id, last, 302, 1, 2, 3)
	// A stale callback and an unregistered voter must not change authority.
	_, err = leader.saveChannelConfigTransition(c.ctx, id, 2, channelElectionConfig(cfg))
	require.Error(t, err)
	forged := channelElectionConfig(expanded)
	forged.Replicas = []uint64{leaderID, 99}
	_, err = leader.saveChannelConfigTransition(c.ctx, id, 2, forged)
	require.Error(t, err)
	after, err := owner.loadConversationConfig(c.ctx, id, 2)
	require.NoError(t, err)
	require.True(t, expanded.Equal(after))
	c.restart(t, leaderID, id)
	last = c.append(t, c.servers[leaderID], id, 303)
	c.verify(t, id, index, 301, 1, 2, 3)
	c.verify(t, id, last, 303, 1, 2, 3)
	t.Logf("certified expansion term=%d version=%d; continued/restart index=%d", expanded.Term, expanded.ConfVersion, last)
}
