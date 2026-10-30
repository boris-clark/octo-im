package cluster

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

// Delayed ordinary probes are diagnostic hints, never an installation witness.
// A still-serving old leader commits with the other follower before the fence.
// The candidate must then acquire fresh durable reports through the real RPCs,
// publish through the slot's applied CAS, install through the callback RPC, and
// preserve the acknowledged row while accepting further writes and restarting.
func TestDelayedProbeProductionInstallPreservesCommittedIdentity(t *testing.T) {
	c := newProtocolCluster(t)
	id := "delayed-probe-production"
	owner := c.owner(t, id)
	staleID := owner.opts.ConfigOptions.NodeId
	oldID := staleID%3 + 1
	freshID := uint64(6) - staleID - oldID
	old, fresh, stale := c.servers[oldID], c.servers[freshID], owner
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, Term: 1, InstalledTerm: 1, LeaderId: oldID,
		Replicas: []uint64{1, 2, 3}, ReplicaMaxCount: 3}
	_, err := owner.saveChannelConfig(c.ctx, cfg)
	require.NoError(t, err)
	cfg, err = owner.loadConversationConfig(c.ctx, id, 2)
	require.NoError(t, err)
	for n := int64(1); n <= 2; n++ {
		require.EqualValues(t, n, c.append(t, old, id, n))
	}
	c.verify(t, id, 2, 2, 1, 2, 3)
	key := wkutil.ChannelToKey(id, 2)
	// Give the two delayed probes distinct, real stored tails. This keeps the
	// sampled loser unambiguous for every bootstrap slot-owner placement.
	for _, p := range c.transports {
		p.isolate(key, freshID)
	}
	require.EqualValues(t, 3, c.append(t, old, id, 3))
	c.verify(t, id, 3, 3, oldID, staleID)

	// Sample both surviving candidates at their old stored tails, and hold the
	// response bytes. Repeated probes in a removal control receive those exact
	// bytes too; no guessed freshness value is injected.
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	type heldProbe struct{ nodeID, index uint64 }
	entered := make(chan heldProbe, 8)
	for _, peer := range []*Server{stale, fresh} {
		peer := peer
		var sampled sync.Once
		var body []byte
		var sampleIndex uint64
		var sampleErr error
		peer.Route("/rpc/channel/lastLogInfo", func(ctx *wkserver.Context) {
			// Execute the production request decoder and reader at RPC arrival, then
			// retain those actual response bytes across the deterministic holdpoint.
			sampled.Do(func() {
				var req channelReq
				if sampleErr = req.decode(ctx.Body()); sampleErr != nil {
					return
				}
				var info *ChannelLastLogInfoResponse
				info, sampleErr = peer.getChannelLastLogInfo(c.ctx, req.channelId, req.channelType)
				if sampleErr != nil {
					return
				}
				sampleIndex = info.LogIndex
				body, sampleErr = info.Marshal()
			})
			if sampleErr != nil {
				ctx.WriteErr(sampleErr)
				return
			}
			select {
			case entered <- heldProbe{peer.opts.ConfigOptions.NodeId, sampleIndex}:
			default:
			}
			<-release
			ctx.Write(body)
		})
	}

	old.Route("/rpc/channel/lastLogInfo", func(ctx *wkserver.Context) { ctx.WriteErr(ErrConversationReadRetry) })
	// Probe from the old leader so both samples cross TCP even if the slot
	// owner is one of the sampled followers.
	done := make(chan error, 1)
	go func() {
		_, e := old.requestChannelLastLogInfos(c.ctx, []uint64{staleID, freshID}, id, 2)
		done <- e
	}()
	collected := false
	t.Cleanup(func() {
		unblock()
		if !collected {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("probe worker did not join")
			}
		}
	})
	seenProbes := make(map[uint64]bool)
	for n := 0; n < 2; n++ {
		select {
		case peer := <-entered:
			seenProbes[peer.nodeID] = true
			if peer.nodeID == staleID {
				require.EqualValues(t, 3, peer.index)
			} else {
				require.Equal(t, freshID, peer.nodeID)
				require.EqualValues(t, 2, peer.index)
			}
		case <-c.ctx.Done():
			t.Fatal("probe holdpoint was not reached")
		}
	}

	require.True(t, seenProbes[staleID] && seenProbes[freshID], "both distinct TCP probe holdpoints must execute")

	// No durable fence has happened: the old leader must really commit here.
	for _, p := range c.transports {
		p.isolate(key, staleID)
	}
	accepted := c.append(t, old, id, 4004)
	require.EqualValues(t, 4, accepted)
	c.verify(t, id, accepted, 4004, oldID, freshID)
	tail, _, err := stale.db.GetChannelLastMessageSeq(id, 2)
	require.NoError(t, err)
	require.EqualValues(t, 3, tail, "the sampled candidate must remain behind the acknowledged row")
	t.Logf("holdpoints hit: stale voter %d sampled index 3; fresh voter %d sampled index 2; old leader %d committed identity 4004 at index %d before fencing", staleID, freshID, oldID, accepted)
	unblock()
	require.NoError(t, <-done)
	collected = true

	// The old leader is unreachable only to the election RPC, while its old
	// data-plane quorum was demonstrably live above. Count the actual fresh
	// voter fence RPC and inspect its durable response before publication.
	old.Route("/rpc/election/fence", func(ctx *wkserver.Context) { ctx.WriteErr(ErrConversationReadRetry) })
	var fences atomic.Int32
	fresh.Route("/rpc/election/fence", func(ctx *wkserver.Context) {
		var req electionFenceRequest
		if e := json.Unmarshal(ctx.Body(), &req); e != nil {
			ctx.WriteErr(e)
			return
		}
		report, e := fresh.electionFence(c.ctx, req)
		if e != nil {
			ctx.WriteErr(e)
			return
		}
		fences.Add(1)
		body, e := json.Marshal(report)
		if e != nil {
			ctx.WriteErr(e)
			return
		}
		ctx.Write(body)
	})
	requested := channelElectionConfig(cfg)
	requested.Term, requested.Leader = cfg.Term+1, staleID // stale candidate requested by the delayed hint
	require.NoError(t, old.onSaveChannelConfig(id, 2, requested))
	installed, err := owner.loadConversationConfig(c.ctx, id, 2)
	require.NoError(t, err)
	require.Greater(t, installed.Term, cfg.Term)
	require.Equal(t, installed.Term, installed.InstalledTerm)
	// This identity assertion is also the removal control's failure point.
	row, err := c.servers[installed.LeaderId].db.LoadMsg(id, 2, accepted)
	require.NoError(t, err, "SAFETY: the installed serving leader must retain the old quorum's acknowledged identity")
	require.EqualValues(t, 4004, row.MessageID, "SAFETY: acknowledged index must retain its identity")
	require.Equal(t, []byte("identity-4004"), row.Payload)
	require.Equal(t, freshID, installed.LeaderId)
	// The remote fence can be local when the fresh voter owns the slot. Its
	// persisted state, inspected below, is authoritative in either placement.
	if owner != fresh {
		require.Positive(t, fences.Load(), "fresh voter fence RPC must execute")
	}
	hard, err := fresh.db.RaftHardState(key)
	require.NoError(t, err)
	require.GreaterOrEqual(t, hard.FenceTerm, installed.Term)

	// Reconnect the lagging follower and use the ordinary append/wake/reload
	// paths. Neither a refusal nor a timeout satisfies this safety test.
	for _, p := range c.transports {
		p.isolate("", 0)
	}
	last := c.append(t, fresh, id, 4005)
	require.Greater(t, last, accepted, "no acknowledged index reuse")
	c.verify(t, id, accepted, 4004, 1, 2, 3)
	c.verify(t, id, last, 4005, 1, 2, 3)
	c.restart(t, freshID, id)
	last = c.append(t, c.servers[freshID], id, 4006)
	require.Greater(t, last, accepted)
	c.verify(t, id, accepted, 4004, 1, 2, 3)
	c.verify(t, id, last, 4006, 1, 2, 3)
	t.Logf("held real TCP probes at stale index 3/fresh index 2; old leader %d committed identity 4004 at index %d; fenced install leader=%d term=%d; continued/restart index=%d", oldID, accepted, installed.LeaderId, installed.Term, last)
}
