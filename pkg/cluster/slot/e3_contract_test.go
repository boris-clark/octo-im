package slot

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestE3All24EntriesRefusalAndActiveAccess(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	p.s.storage = p
	key := SlotIdToKey(1)

	// Before domain attachment, ForTestFixture is permitted.
	require.NoError(t, p.ForTestFixture().SaveHardState(key, types.HardState{Term: 1}))

	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	applyCalls := 0
	saveConfigCalls := 0
	p.s.opts.OnApply = func(uint32, []types.Log) error {
		applyCalls++
		once.Do(func() { close(entered); <-release })
		return nil
	}
	p.s.opts.OnSaveConfig = func(uint32, types.Config) error {
		saveConfigCalls++
		return nil
	}

	tr := &issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}}
	g := raftgroup.New(raftgroup.NewOptions(
		raftgroup.WithStorage(p),
		raftgroup.WithTransport(tr),
		raftgroup.WithTickInterval(5*time.Millisecond),
	))
	p.s.raftGroup = g

	// Once attached to a RaftGroup domain, ForTestFixture is irrevocably disabled.
	_, err := p.ForTestFixture().GetState(key)
	require.ErrorIs(t, err, ErrStorageUnbound)
	require.ErrorIs(t, err, raftgroup.ErrRaftBusy)

	require.NoError(t, g.Start())
	old := e3Install(t, p, g)

	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() {
		unblock()
		g.Stop()
		require.NoError(t, g.WaitQuiescence(context.Background()))
		require.NoError(t, p.Close())
	}()

	// Server convenience reads have nonblocking admission while workers execute.
	for _, read := range []func() error{
		func() error { _, e := p.s.AppliedIndex(1); return e },
		func() error { _, e := p.s.LastIndex(1); return e },
		func() error { _, e := p.s.LastLog(1); return e },
		func() error { _, e := p.s.GetLogsInReverseOrder(1, 0, 1, 1); return e },
	} {
		require.Eventually(t, func() bool { return read() == nil }, time.Second, time.Millisecond)
	}

	// Start a proposal and hold OnApply, then retire the instance so it is draining.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_, _ = g.ProposeBatchUntilAppliedTimeout(ctx, key, types.ProposeReqSet{{Id: 101, Data: []byte("e3-drain")}})
	}()
	issue87Await(t, entered)
	g.RemoveRaft(old)

	// Direct Close while draining must refuse with ErrIncompleteQuiescence.
	require.ErrorIs(t, p.Close(), raftgroup.ErrIncompleteQuiescence)

	rawCalls := []struct {
		name string
		call func() error
	}{
		{"GetState", func() error { _, e := p.GetState(key); return e }},
		{"AppendLogs", func() error { return p.AppendLogs(key, nil, nil) }},
		{"TruncateLogTo", func() error { return p.TruncateLogTo(key, 1) }},
		{"GetLogs", func() error { _, e := p.GetLogs(key, 1, 2, 0); return e }},
		{"Apply", func() error { return p.Apply(key, nil) }},
		{"SaveConfig", func() error { return p.SaveConfig(key, types.Config{}) }},
		{"GetLogsInReverseOrder", func() error { _, e := p.GetLogsInReverseOrder(key, 1, 2, 1); return e }},
		{"LastIndex", func() error { _, e := p.LastIndex(key); return e }},
		{"LastIndexAndTerm", func() error { _, _, e := p.LastIndexAndTerm(key); return e }},
		{"LastLog", func() error { _, e := p.LastLog(key); return e }},
		{"SetAppliedIndex", func() error { return p.SetAppliedIndex(key, 0) }},
		{"AppliedIndex", func() error { _, e := p.AppliedIndex(key); return e }},
		{"SetLeaderTermStartIndex", func() error { return p.SetLeaderTermStartIndex(key, 1, 1) }},
		{"LeaderLastLogTerm", func() error { _, e := p.LeaderLastLogTerm(key); return e }},
		{"GetTermStartIndex", func() error { _, e := p.GetTermStartIndex(key, 1); return e }},
		{"LeaderTermGreaterEqThan", func() error { _, e := p.LeaderTermGreaterEqThan(key, 1); return e }},
		{"DeleteLeaderTermStartIndexGreaterThanTerm", func() error { return p.DeleteLeaderTermStartIndexGreaterThanTerm(key, 1) }},
		{"SaveHardState", func() error { return p.SaveHardState(key, types.HardState{Term: 9}) }},
		{"MarkDiverged", func() error { return p.MarkDiverged(key, errors.New("unbound")) }},
		{"DivergedMarker", func() error { _, e := p.DivergedMarker(key); return e }},
	}
	require.Len(t, rawCalls, 20)
	for _, rc := range rawCalls {
		callErr := rc.call()
		require.ErrorIs(t, callErr, ErrStorageUnbound, "raw %s must return ErrStorageUnbound", rc.name)
		require.ErrorIs(t, callErr, raftgroup.ErrRaftBusy, "raw %s must wrap ErrRaftBusy", rc.name)
	}
	require.Equal(t, 1, applyCalls, "raw Apply must not invoke OnApply")
	require.Equal(t, 0, saveConfigCalls, "raw SaveConfig must not invoke OnSaveConfig")
	require.Equal(t, ReplicaHealthy, p.s.ReplicaState(1), "raw MarkDiverged must not fence replica")

	serverCalls := []struct {
		name string
		call func() error
	}{
		{"Server.AppliedIndex", func() error { _, e := p.s.AppliedIndex(1); return e }},
		{"Server.LastIndex", func() error { _, e := p.s.LastIndex(1); return e }},
		{"Server.LastLog", func() error { _, e := p.s.LastLog(1); return e }},
		{"Server.GetLogsInReverseOrder", func() error { _, e := p.s.GetLogsInReverseOrder(1, 1, 2, 1); return e }},
	}
	require.Len(t, serverCalls, 4)
	for _, sc := range serverCalls {
		callErr := sc.call()
		require.ErrorIs(t, callErr, raftgroup.ErrRaftBusy, "%s must refuse during drain", sc.name)
	}
}

func TestSharedDomainCloseRequiresAllAttachedGroupsQuiescent(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	tr := &issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}}
	g1 := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTransport(tr), raftgroup.WithTickInterval(5*time.Millisecond)))
	g2 := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTransport(tr), raftgroup.WithTickInterval(5*time.Millisecond)))

	t.Cleanup(func() {
		g1.Stop()
		require.NoError(t, g1.WaitQuiescence(context.Background()))
		g2.Stop()
		require.NoError(t, g2.WaitQuiescence(context.Background()))
		require.NoError(t, p.Close())
	})
	require.NoError(t, g1.Start())
	require.NoError(t, g2.Start())

	// Neither group stopped -> Close refuses.
	require.ErrorIs(t, p.Close(), raftgroup.ErrIncompleteQuiescence)

	// Stop g1 only; g2 is still running -> Close still refuses.
	g1.Stop()
	require.NoError(t, g1.WaitQuiescence(context.Background()))
	require.ErrorIs(t, p.Close(), raftgroup.ErrIncompleteQuiescence)

	// Stop g2 and wait quiescence -> Close succeeds.
	g2.Stop()
	require.NoError(t, g2.WaitQuiescence(context.Background()))
	require.NoError(t, p.Close())
}
