package cluster

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/stretchr/testify/require"
)

func TestCertifiedSlotMigrationRecoversFromRejectedPromotion(t *testing.T) {
	c := newOpCluster(t, 4, 4, 3)
	ctrl := c.controller(t)
	st := ctrl.cfgServer.Slots()[0]
	var target, from uint64
	for id := uint64(1); id <= 4; id++ {
		if !containsNode(st.Replicas, id) {
			target = id
		}
	}
	for _, id := range st.Replicas {
		if id != st.Leader {
			from = id
			break
		}
	}
	require.NotZero(t, target)
	require.NotZero(t, from)
	fenced, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var released atomic.Bool
	var once sync.Once
	unblock := func() { once.Do(func() { released.Store(true); close(release) }) }
	defer unblock()
	learner := c.servers[target]
	learner.netServer.Route("/rpc/election/fence", func(ctx *wkserver.Context) {
		var req electionFenceRequest
		if err := json.Unmarshal(ctx.Body(), &req); err != nil {
			ctx.WriteErr(err)
			return
		}
		callCtx, cancel := context.WithTimeout(learner.cancelCtx, 4*time.Second)
		defer cancel()
		report, err := learner.electionFence(callCtx, req)
		if err != nil {
			ctx.WriteErr(err)
			return
		}
		if !released.Load() {
			if first.CompareAndSwap(false, true) {
				close(fenced)
			}
			select {
			case <-release:
			case <-callCtx.Done():
				ctx.WriteErr(callCtx.Err())
				return
			}
		}
		body, err := json.Marshal(report)
		if err != nil {
			ctx.WriteErr(err)
			return
		}
		ctx.Write(body)
	})
	ch := c.channelOnSlot(t, "rejected-promotion", st.Id)
	index := c.append(t, c.servers[st.Leader], ch, 9101)
	require.NoError(t, ctrl.cfgServer.ProposeMigrateSlot(st.Id, from, target))
	select {
	case <-fenced:
	case <-time.After(5 * time.Second):
		t.Fatal("promoted learner fence not reached")
	}
	// The learner has genuinely fenced its stored tail. Commit a newer slot
	// barrier on the still-active old quorum before returning that learner report.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := ctrl.slotServer.ProposeUntilAppliedTimeout(ctx, st.Id, rt.NoopData())
	cancel()
	require.NoError(t, err)
	unblock()
	var installedTerm uint32
	require.Eventually(t, func() bool {
		current := slotSnapshot(ctrl, st.Id)
		if current == nil {
			return false
		}
		installedTerm = current.InstalledTerm
		return containsNode(current.Replicas, target) && !containsNode(current.Replicas, from) && len(current.Learners) == 0 && current.MigrateFrom == 0 && current.MigrateTo == 0
	}, 20*time.Second, 20*time.Millisecond, "rejected witness left migration fenced")
	require.GreaterOrEqual(t, installedTerm, st.Term+3, "a rejected promotion must resume the unchanged voters before retry")
	c.verify(t, ch, index, 9101)
	next := c.append(t, ctrl, ch, 9102)
	c.verify(t, ch, next, 9102)
	c.restartAll(t)
	c.verify(t, ch, index, 9101)
	c.verify(t, ch, next, 9102)
}

func containsNode(ids []uint64, id uint64) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}
