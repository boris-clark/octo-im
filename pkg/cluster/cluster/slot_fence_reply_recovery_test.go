package cluster

import (
	"context"
	"testing"
	"time"

	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestRejectedMembershipRecoversMissingFenceReplies(t *testing.T) {
	c := newOpCluster(t, 3, 3, 3)
	ctrl := c.controller(t)
	old := ctrl.cfgServer.AppliedSlot(0)
	require.NotNil(t, old)
	ch := c.channelOnSlot(t, "lost-fence-reply", old.Id)
	index := c.append(t, ctrl, ch, 9201)
	c.verify(t, ch, index, 9201)
	cfg, term := slotElectionConfig(old), old.Term+1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var reports map[uint64]rt.ElectionReport
	require.Eventually(t, func() bool {
		var err error
		reports, err = ctrl.requestElectionFences(ctx, cfg, electionFenceRequest{Slot: old, Term: term})
		if err != nil || len(reports) != len(old.Replicas) {
			return false
		}
		for _, report := range reports {
			if !report.Fenced || report.Term != term {
				return false
			}
		}
		return true
	}, 3*time.Second, 20*time.Millisecond, "all old voters must actually be fenced before losing replies")

	// The effects happened on the real voters, but the coordinator received
	// only one acknowledgement. Every subsequent RPC is available again.
	partial := map[uint64]rt.ElectionReport{old.Leader: reports[old.Leader]}
	_, err := rt.SelectFencedLeader(cfg, term, partial)
	require.Error(t, err, "one reply must not establish an old quorum")
	require.NoError(t, ctrl.resumeSlotAfterRejectedMembership(ctx, old, term, partial),
		"lost fence replies left the unchanged voter set fenced despite reachable voters")
	installed := ctrl.cfgServer.AppliedSlot(old.Id)
	require.Equal(t, old.Replicas, installed.Replicas)
	require.Equal(t, old.Learners, installed.Learners)
	require.Equal(t, term, installed.InstalledTerm)
	next := c.append(t, ctrl, ch, 9202)
	c.verify(t, ch, next, 9202)
	c.restartAll(t)
	c.verify(t, ch, index, 9201)
	c.verify(t, ch, next, 9202)
}
