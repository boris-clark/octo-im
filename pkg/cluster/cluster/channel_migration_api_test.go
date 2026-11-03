package cluster

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func TestReviewLegacyChannelMigrationAPI(t *testing.T) {
	c := newProtocolCluster(t)
	id := "review-legacy-migration-api"
	owner := c.owner(t, id)
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, Term: 1, LeaderId: 1, Replicas: []uint64{1}, ReplicaMaxCount: 2}
	_, err := owner.saveChannelConfig(c.ctx, cfg)
	require.NoError(t, err)
	cfg, err = owner.loadConversationConfigLocal(c.ctx, id, 2)
	require.NoError(t, err)
	require.Zero(t, cfg.InstalledTerm)
	require.Eventually(t, func() bool {
		for _, s := range c.servers {
			applied, e := s.fencedChannelClusterConfig(id, 2)
			if e != nil || !applied.Equal(cfg) {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	http := wkhttp.New()
	http.POST("/channels/:channel_id/:channel_type/migrate", owner.channelMigrate)
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/channels/"+id+"/2/migrate", strings.NewReader(`{"migrate_from":1,"migrate_to":2}`))
	request.Header.Set("Content-Type", "application/json")
	http.GetGinRoute().ServeHTTP(response, request)
	applied, readErr := owner.loadConversationConfigLocal(c.ctx, id, 2)
	require.NoError(t, readErr)
	t.Logf("API status=%d body=%s; before=%+v applied=%+v", response.Code, response.Body.String(), cfg, applied)
	require.Equal(t, 200, response.Code, "a lawful legacy membership migration must return success after certified publication")
}

// Retain the independent HTTP reproduction above. This extension checks both
// wake paths before learner catch-up, then releases ordinary replication.
func TestLegacyChannelMigrationAPIInstallsAuthority(t *testing.T) {
	for _, local := range []bool{true, false} {
		t.Run(fmt.Sprintf("local-wake=%t", local), func(t *testing.T) {
			c := newProtocolCluster(t)
			var id string
			var owner *Server
			for i := 0; i < 100; i++ {
				id = fmt.Sprintf("legacy-api-authority-%d", i)
				owner = c.owner(t, id)
				if (owner.opts.ConfigOptions.NodeId == 1) == local {
					break
				}
			}
			require.Equal(t, local, owner.opts.ConfigOptions.NodeId == 1)
			cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, Term: 1, LeaderId: 1, Replicas: []uint64{1}, ReplicaMaxCount: 2}
			_, err := owner.saveChannelConfig(c.ctx, cfg)
			require.NoError(t, err)
			cfg, err = owner.loadConversationConfigLocal(c.ctx, id, 2)
			require.NoError(t, err)
			require.Zero(t, cfg.InstalledTerm)
			require.Eventually(t, func() bool {
				for _, s := range c.servers {
					row, e := s.fencedChannelClusterConfig(id, 2)
					if e != nil || !row.Equal(cfg) {
						return false
					}
				}
				return true
			}, 5*time.Second, 10*time.Millisecond)
			// Hold only the learner data plane until the synchronous wake and
			// durable identity have been inspected. Authority RPCs remain real.
			key := wkutil.ChannelToKey(id, 2)
			for _, transport := range c.transports {
				transport.isolate(key, 2)
			}
			http := wkhttp.New()
			http.POST("/channels/:channel_id/:channel_type/migrate", owner.channelMigrate)
			response := httptest.NewRecorder()
			request := httptest.NewRequest("POST", "/channels/"+id+"/2/migrate", strings.NewReader(`{"migrate_from":1,"migrate_to":2}`))
			request.Header.Set("Content-Type", "application/json")
			http.GetGinRoute().ServeHTTP(response, request)
			applied, err := owner.loadConversationConfigLocal(c.ctx, id, 2)
			require.NoError(t, err)
			t.Logf("local=%t HTTP=%d body=%s old=%+v applied=%+v", local, response.Code, response.Body.String(), cfg, applied)
			require.Equal(t, 200, response.Code, "success requires the real wake to install the certified row")
			require.Equal(t, cfg.Id, applied.Id)
			require.Greater(t, applied.ConfVersion, cfg.ConfVersion)
			require.EqualValues(t, 2, applied.Term)
			require.Equal(t, applied.Term, applied.InstalledTerm)
			require.EqualValues(t, 1, applied.BoundaryTerm)
			require.EqualValues(t, 1, applied.LeaderId)
			require.Equal(t, []uint64{1}, applied.Replicas)
			require.Equal(t, []uint64{2}, applied.Learners)
			require.EqualValues(t, 1, applied.MigrateFrom)
			require.EqualValues(t, 2, applied.MigrateTo)
			state, err := c.servers[1].channelServer.ReadLeaderState(c.ctx, id, 2)
			require.NoError(t, err)
			require.True(t, state.Exists)
			require.Equal(t, applied.ConfVersion, state.ConfigVersion)
			require.Equal(t, applied.Term, state.Term)
			require.Equal(t, applied.LeaderId, state.LeaderID)
			hard, err := c.servers[1].db.RaftHardState(key)
			require.NoError(t, err)
			require.Equal(t, applied.InstalledTerm, hard.InstalledTerm)
			require.Equal(t, applied.LeaderId, hard.InstalledLeader)
			require.Equal(t, applied.BoundaryTerm, hard.BoundaryTerm)
			identity := channelElectionConfig(applied)
			identity.Role, identity.Version, identity.FenceEnabled, identity.InstalledTerm, identity.BoundaryTerm = rt.RoleUnknown, 0, false, 0, 0
			encoded, err := json.Marshal(identity)
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256(encoded), hard.InstalledIdentity, "wake must persist the authority's complete membership/leader/term identity")
			for _, transport := range c.transports {
				transport.isolate(key, 0)
			}
			index := c.append(t, c.servers[1], id, 8011)
			var completed wkdb.ChannelClusterConfig
			require.Eventually(t, func() bool {
				latest, e := owner.loadConversationConfigLocal(c.ctx, id, 2)
				if e != nil || len(latest.Replicas) != 1 || latest.Replicas[0] != 2 || len(latest.Learners) != 0 || latest.MigrateFrom != 0 || latest.MigrateTo != 0 {
					return false
				}
				state, e := c.servers[2].channelServer.ReadLeaderState(c.ctx, id, 2)
				if e != nil || !state.Ready || state.ConfigVersion != latest.ConfVersion || state.Term != latest.Term || state.LeaderID != latest.LeaderId {
					return false
				}
				hard, e := c.servers[2].db.RaftHardState(key)
				if e != nil || hard.InstalledTerm != latest.InstalledTerm || hard.InstalledLeader != latest.LeaderId {
					return false
				}
				completed = latest
				return true
			}, 10*time.Second, 20*time.Millisecond, "ordinary promotion callbacks must finish the migration")
			t.Logf("completed migration and target installation: %+v", completed)
			c.verify(t, id, index, 8011, 2)
			stored, err := c.servers[2].db.LoadMsg(id, 2, index)
			require.NoError(t, err)
			c.restart(t, 2, id)
			reopened, err := c.servers[2].db.LoadMsg(id, 2, index)
			require.NoError(t, err)
			require.Equal(t, stored, reopened, "all message columns and the original index survive reopen")
			require.Eventually(t, func() bool {
				// Reconcile the applied row before asking maintenance for any
				// further capacity change on this newly reopened runtime.
				latest, e := c.servers[2].loadConversationConfig(c.ctx, id, 2)
				if e != nil {
					return false
				}
				if e = c.servers[2].channelServer.WakeLeaderIfNeed(latest); e != nil {
					return false
				}
				state, e := c.servers[2].channelServer.ReadLeaderState(c.ctx, id, 2)
				return e == nil && state.Ready
			}, 5*time.Second, 20*time.Millisecond, "reopened service must reconnect and apply its authoritative configuration before the next write")
			next := c.append(t, c.servers[2], id, 8012)
			require.Greater(t, next, index)
			c.verify(t, id, next, 8012, 2)
		})
	}
}

func TestChannelConfigAppliedReturnFencesConcurrentSave(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("applied-return-race", 2)
	require.NoError(t, err)
	type result struct {
		capacity uint16
		row      wkdb.ChannelClusterConfig
		err      error
	}
	start, results := make(chan struct{}), make(chan result, 2)
	for _, capacity := range []uint16{2, 3} {
		go func(capacity uint16) {
			next := cfg.Clone()
			next.ReplicaMaxCount = capacity
			<-start
			row, err := s.saveChannelConfigApplied(ctx, next)
			results <- result{capacity, row, err}
		}(capacity)
	}
	close(start)
	winners := 0
	// Collect both goroutines before assertions can terminate the test.
	first, second := <-results, <-results
	for _, result := range []result{first, second} {
		if result.err != nil {
			require.ErrorIs(t, result.err, raft.ErrConfigVersionStale)
			require.True(t, wkdb.IsEmptyChannelClusterConfig(result.row))
			continue
		}
		winners++
		require.Equal(t, result.capacity, result.row.ReplicaMaxCount)
		require.Greater(t, result.row.ConfVersion, cfg.ConfVersion)
		require.Greater(t, result.row.Term, cfg.Term)
		require.Equal(t, result.row.Term, result.row.InstalledTerm)
		applied, err := s.loadConversationConfigLocal(ctx, cfg.ChannelId, cfg.ChannelType)
		require.NoError(t, err)
		require.True(t, applied.Equal(result.row), "return the winning complete authority without grafting another proposal onto it")
	}
	require.Equal(t, 1, winners)
}
