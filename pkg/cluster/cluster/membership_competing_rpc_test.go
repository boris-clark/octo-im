package cluster

import (
	"sync"
	"testing"
	"time"

	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestCertifiedCompetingMembershipCallbacksRPC(t *testing.T) {
	c := newProtocolCluster(t)
	id := "competing-membership-rpc"
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
	first := c.append(t, leader, id, 501)
	cfg.ReplicaMaxCount = 2
	_, err = owner.saveChannelConfig(c.ctx, cfg)
	require.NoError(t, err)
	cfg, err = owner.loadConversationConfig(c.ctx, id, 2)
	require.NoError(t, err)
	var requests []rt.Config
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		if nodeID == leaderID {
			continue
		}
		next := channelElectionConfig(cfg)
		next.Learners = []uint64{nodeID}
		next.MigrateFrom, next.MigrateTo = nodeID, nodeID
		requests = append(requests, next)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	for _, req := range requests {
		workers.Add(1)
		go func(req rt.Config) {
			defer workers.Done()
			<-start
			var e error
			for {
				_, e = leader.saveChannelConfigTransition(c.ctx, id, 2, req)
				if e == nil {
					break
				}
				// A newly applied slot version may not have reached a remote voter yet.
				// The ordinary callback retries this pre-fence refusal; stop retrying a
				// losing decision as soon as another version has actually published.
				applied, readErr := owner.loadConversationConfig(c.ctx, id, 2)
				if readErr == nil && applied.ConfVersion > cfg.ConfVersion {
					break
				}
				select {
				case <-c.ctx.Done():
					e = c.ctx.Err()
				case <-time.After(10 * time.Millisecond):
					continue
				}
				break
			}
			t.Logf("target=%d transition result=%v", req.MigrateTo, e)
			errs <- e
		}(req)
	}
	close(start)
	workers.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		}
	}

	require.Equal(t, 1, successes, "only one competing remote callback may publish from the same applied version")
	winner, err := owner.loadConversationConfig(c.ctx, id, 2)
	require.NoError(t, err)
	require.Len(t, winner.Learners, 1)
	require.Equal(t, winner.Term, winner.InstalledTerm)
	require.Greater(t, winner.ConfVersion, cfg.ConfVersion)
	forged := channelElectionConfig(winner)
	forged.Replicas, forged.Learners = []uint64{leaderID, 99}, nil
	forged.MigrateFrom, forged.MigrateTo = 0, 0
	_, err = leader.saveChannelConfigTransition(c.ctx, id, 2, forged)
	require.Error(t, err)
	after, err := owner.loadConversationConfig(c.ctx, id, 2)
	require.NoError(t, err)
	require.True(t, winner.Equal(after))
	require.Eventually(t, func() bool { return owner.requestChannelConfigReconcile(c.ctx, winner) == nil }, 3*time.Second, 10*time.Millisecond)
	last := c.append(t, leader, id, 502)
	require.Greater(t, last, first)
	c.verify(t, id, first, 501, leaderID, winner.Learners[0])
	c.verify(t, id, last, 502, leaderID, winner.Learners[0])
}
