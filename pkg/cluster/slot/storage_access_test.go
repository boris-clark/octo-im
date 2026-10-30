package slot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func e3Install(t *testing.T, p *PebbleShardLogStorage, g *raftgroup.RaftGroup) *Slot {
	t.Helper()
	p.s.Log = p.Log
	p.s.opts.NodeId = 1
	p.s.storage, p.s.raftGroup = p, g
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cfg := &pb.Slot{Id: 1, Leader: 1, Replicas: []uint64{1}, Term: 1, InstalledTerm: 1}
	r, err := g.InitializeRaftWithStorage(ctx, "1", func(b *raftgroup.StorageBinding) (raftgroup.IRaft, error) {
		return newSlot(cfg, p.s, b)
	}, func(r raftgroup.IRaft) error {
		return r.Step(types.Event{Type: types.ConfChange, Config: p.s.slotToConfig(cfg)})
	})
	require.NoError(t, err)
	return r.(*Slot)
}

func e3Group(t *testing.T, p *PebbleShardLogStorage) *raftgroup.RaftGroup {
	t.Helper()
	g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTickInterval(time.Hour), raftgroup.WithTransport(&issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}})))
	require.NoError(t, g.Start())
	t.Cleanup(func() {
		g.Stop()
		require.NoError(t, g.WaitQuiescence(context.Background()))
		require.NoError(t, p.Close())
	})
	return g
}

// e3WithStorage waits only for initial nonblocking admission, never retries an
// entered callback or an uncertain storage result.
func e3WithStorage(b *raftgroup.StorageBinding, fn func(*raftgroup.StorageCall) error) error {
	deadline := time.Now().Add(time.Second)
	for {
		entered := false
		err := b.WithStorage(func(c *raftgroup.StorageCall) error { entered = true; return fn(c) })
		if entered || !errors.Is(err, raftgroup.ErrRaftBusy) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
}

func TestE3ScopedApplyDrainReloadAndCallbackReads(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	g := e3Group(t, p)
	old := e3Install(t, p, g)
	binding := g.StorageBinding(old)
	var logs []types.Log
	for i := uint64(1); i <= 12; i++ {
		logs = append(logs, types.Log{Index: i, Term: 1, Id: 100 + i, Data: []byte("preserved")})
	}
	require.NoError(t, e3WithStorage(binding, func(c *raftgroup.StorageCall) error {
		v := p.BindStorage(c).(*storageView)
		if err := v.AppendLogs("1", logs, &types.TermStartIndexInfo{Term: 1, Index: 1}); err != nil {
			return err
		}
		return v.SetAppliedIndex("1", 9)
	}))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var saved *CallbackReads
	p.s.opts.OnApplyWithReads = func(_ uint32, _ []types.Log, reads *CallbackReads) error {
		saved = reads
		n, err := reads.AppliedIndex()
		if err != nil || n != 9 {
			return errors.New("callback did not read applied9")
		}
		n, err = reads.LastIndex()
		if err != nil || n != 12 {
			return errors.New("callback did not read tail12")
		}
		_, err = p.s.AppliedIndex(1)
		if !errors.Is(err, raftgroup.ErrRaftBusy) {
			return errors.New("public callback reentry was admitted")
		}
		close(entered)
		<-release
		_, err = reads.GetState()
		return err
	}
	go func() {
		done <- e3WithStorage(binding, func(c *raftgroup.StorageCall) error { return p.BindStorage(c).Apply("1", logs[9:]) })
	}()
	issue87Await(t, entered)
	// A forged capability cannot borrow the accepted Apply, even before retirement.
	_, err := p.BindStorage(&raftgroup.StorageCall{}).GetLogs("1", 1, 2, 0)
	require.ErrorIs(t, err, raftgroup.ErrRaftBusy)
	g.RemoveRaft(old)
	builds := 0
	_, err = g.InitializeRaftWithStorage(context.Background(), "1", func(*raftgroup.StorageBinding) (raftgroup.IRaft, error) { builds++; return nil, nil }, nil)
	require.ErrorIs(t, err, raftgroup.ErrRaftBusy)
	require.Zero(t, builds, "reload entered before sync Apply drained")
	require.ErrorIs(t, p.Close(), raftgroup.ErrIncompleteQuiescence)
	require.ErrorIs(t, binding.WithStorage(func(*raftgroup.StorageCall) error { return nil }), raftgroup.ErrRaftBusy)
	unblock()
	require.NoError(t, <-done)
	_, err = saved.LastIndex()
	require.ErrorIs(t, err, ErrStorageUnbound, "escaped callback scope remained usable")
	// The next reconstruction must actually read A12, including the same identities.
	var captured *storageView
	next, err := g.InitializeRaftWithStorage(context.Background(), "1", func(b *raftgroup.StorageBinding) (raftgroup.IRaft, error) {
		err := b.WithStorage(func(c *raftgroup.StorageCall) error {
			captured = p.BindStorage(c).(*storageView)
			state, e := captured.GetState("1")
			if e != nil {
				return e
			}
			require.EqualValues(t, 12, state.AppliedIndex)
			actual, e := captured.GetLogs("1", 1, 13, 0)
			if e != nil {
				return e
			}
			require.Len(t, actual, len(logs))
			for i := range logs {
				require.True(t, sameLogEntry(logs[i], actual[i]), "replicated identity at %d", i+1)
			}
			require.ErrorIs(t, captured.SetAppliedIndex("1", 0), raftgroup.ErrRaftBusy, "initial reservation allowed ordinary mutation")
			return nil
		})
		if err != nil {
			return nil, err
		}
		return newSlot(&pb.Slot{Id: 1, Leader: 1, Replicas: []uint64{1}, Term: 2, InstalledTerm: 2}, p.s, b)
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, next)
	_, err = captured.GetState("1")
	require.ErrorIs(t, err, raftgroup.ErrRaftBusy, "expired init view admitted")
	require.ErrorIs(t, binding.WithStorage(func(*raftgroup.StorageCall) error { return nil }), raftgroup.ErrRaftBusy, "old epoch borrowed replacement")
	t.Log("actual sync Apply: A9 held, reload callback=0, then A12 and all 12 identities reconstructed")
}

func TestE3SynchronousCallsJoinShutdownAndDomainTransfer(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	g := e3Group(t, p)
	old := e3Install(t, p, g)
	binding := g.StorageBinding(old)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		done <- e3WithStorage(binding, func(c *raftgroup.StorageCall) error {
			close(entered)
			<-release
			_, err := p.BindStorage(c).GetLogs("1", 1, 2, 0)
			return err
		})
	}()
	issue87Await(t, entered)
	g.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	require.ErrorIs(t, g.WaitQuiescence(ctx), raftgroup.ErrIncompleteQuiescence)
	cancel()
	require.ErrorIs(t, p.Close(), raftgroup.ErrIncompleteQuiescence)
	g2 := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTransport(&issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}})))
	require.NoError(t, g2.Start())
	defer func() { g2.Stop(); require.NoError(t, g2.WaitQuiescence(context.Background())) }()
	var builds atomic.Int64
	_, err := g2.InitializeRaftWithStorage(context.Background(), "1", func(*raftgroup.StorageBinding) (raftgroup.IRaft, error) { builds.Add(1); return nil, nil }, nil)
	require.ErrorIs(t, err, raftgroup.ErrRaftBusy)
	require.Zero(t, builds.Load())
	unblock()
	require.NoError(t, <-done)
	require.NoError(t, g.WaitQuiescence(context.Background()))
	// A fully joined old group permits a fresh reservation in the receiving group.
	next := e3Install(t, p, g2)
	require.NotNil(t, next)
	require.ErrorIs(t, p.Close(), raftgroup.ErrIncompleteQuiescence)
	g2.Stop()
	require.NoError(t, g2.WaitQuiescence(context.Background()))
	require.NoError(t, p.Close())
}

func TestE3InitialReservationStopAndCompetingBuild(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	g := e3Group(t, p)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		_, err := g.InitializeRaftWithStorage(context.Background(), "1", func(b *raftgroup.StorageBinding) (raftgroup.IRaft, error) {
			close(entered)
			<-release
			return nil, errors.New("collected builder failure")
		}, nil)
		done <- err
	}()
	issue87Await(t, entered)
	builds := 0
	_, err := g.InitializeRaftWithStorage(context.Background(), "1", func(*raftgroup.StorageBinding) (raftgroup.IRaft, error) { builds++; return nil, nil }, nil)
	require.ErrorIs(t, err, raftgroup.ErrRaftBusy)
	require.Zero(t, builds)
	g.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	require.ErrorIs(t, g.WaitQuiescence(ctx), raftgroup.ErrIncompleteQuiescence)
	cancel()
	require.ErrorIs(t, p.Close(), raftgroup.ErrIncompleteQuiescence)
	unblock()
	require.EqualError(t, <-done, "collected builder failure")
	require.NoError(t, g.WaitQuiescence(context.Background()))
	require.Nil(t, g.GetRaft("1"))
	require.NoError(t, p.Close())
}

func TestE3OfflineScanExcludesLiveAndSymlinkAliases(t *testing.T) {
	dir := t.TempDir()
	p := issue87Open(t, dir)
	require.NoError(t, p.ForTestFixture().AppendLogs("1", []types.Log{{Index: 1, Term: 1, Id: 99, Data: []byte("offline")}}, nil))
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(dir, alias))
	_, err := ScanDivergence(alias, 1, 2)
	require.Error(t, err, "scanner opened a live domain through an alias")
	require.NoError(t, p.Close())
	flagged, err := ScanDivergence(alias, 1, 2)
	require.NoError(t, err)
	require.Empty(t, flagged)
	state, logs, err := InspectStoppedSlot(alias, 1, 1, 1, 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.LastLogIndex)
	require.EqualValues(t, 99, logs[0].Id)
	// A partially opened scanner must collect shard0 before returning a shard1 error.
	_, err = ScanDivergence(alias, 2, 2)
	require.Error(t, err)
	q := NewPebbleShardLogStorage(nil, dir, 1)
	require.NoError(t, q.Open())
	require.NoError(t, q.Close())
}

func TestE3BoundEntriesUseOneOriginalCall(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	g := e3Group(t, p)
	st := e3Install(t, p, g)
	binding := g.StorageBinding(st)
	var escaped *storageView
	require.NoError(t, e3WithStorage(binding, func(c *raftgroup.StorageCall) error {
		v := p.BindStorage(c).(*storageView)
		escaped = v
		require.NoError(t, v.AppendLogs("1", []types.Log{{Index: 1, Term: 1, Id: 77, Data: []byte("entry")}}, &types.TermStartIndexInfo{Term: 1, Index: 1}))
		require.NoError(t, v.SetLeaderTermStartIndex("1", 1, 1))
		require.NoError(t, v.SaveConfig("1", types.Config{}))
		require.NoError(t, v.Apply("1", []types.Log{{Index: 1, Term: 1, Id: 77, Data: []byte("entry")}}))
		require.NoError(t, v.SetAppliedIndex("1", 1))
		state, err := v.GetState("1")
		require.NoError(t, err)
		require.EqualValues(t, 1, state.AppliedIndex)
		require.NoError(t, v.SaveHardState("1", state.HardState))
		n, err := v.LastIndex("1")
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		n, term, err := v.LastIndexAndTerm("1")
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		require.EqualValues(t, 1, term)
		log, err := v.LastLog("1")
		require.NoError(t, err)
		require.EqualValues(t, 77, log.Id)
		logs, err := v.GetLogs("1", 1, 2, 0)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		logs, err = v.GetLogsInReverseOrder("1", 1, 2, 1)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		n, err = v.AppliedIndex("1")
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		n, err = v.GetTermStartIndex("1", 1)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		term, err = v.LeaderLastLogTerm("1")
		require.NoError(t, err)
		require.EqualValues(t, 1, term)
		term, err = v.LeaderTermGreaterEqThan("1", 1)
		require.NoError(t, err)
		require.EqualValues(t, 1, term)
		require.NoError(t, v.TruncateLogTo("1", 1))
		require.NoError(t, v.DeleteLeaderTermStartIndexGreaterThanTerm("1", 1))
		_, err = v.GetState("2")
		require.ErrorIs(t, err, raftgroup.ErrRaftBusy)
		require.ErrorIs(t, v.MarkDiverged("2", errors.New("wrong key")), raftgroup.ErrRaftBusy)
		marked, err := v.DivergedMarker("1")
		require.NoError(t, err)
		require.False(t, marked)
		require.NoError(t, v.MarkDiverged("1", errors.New("explicit authorized quarantine")))
		marked, err = v.DivergedMarker("1")
		require.NoError(t, err)
		require.True(t, marked)
		return nil
	}))
	_, err := escaped.GetState("1")
	require.ErrorIs(t, err, raftgroup.ErrRaftBusy)
}

func TestE3SaveConfigReadScopeAndFailureDisposition(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	g := e3Group(t, p)
	st := e3Install(t, p, g)
	var escaped *CallbackReads
	failure := errors.New("controller rejected save")
	p.s.opts.OnSaveConfigWithReads = func(_ uint32, _ types.Config, r *CallbackReads) error {
		escaped = r
		_, err := r.GetState()
		if err != nil {
			return err
		}
		_, err = p.s.LastIndex(1)
		if !errors.Is(err, raftgroup.ErrRaftBusy) {
			return errors.New("save callback public reentry admitted")
		}
		return failure
	}
	err := e3WithStorage(g.StorageBinding(st), func(c *raftgroup.StorageCall) error { return p.BindStorage(c).SaveConfig("1", types.Config{}) })
	require.ErrorIs(t, err, failure, "callback failure was converted into success")
	_, err = escaped.GetState()
	require.ErrorIs(t, err, ErrStorageUnbound)
	require.NoError(t, e3WithStorage(g.StorageBinding(st), func(c *raftgroup.StorageCall) error { _, err := p.BindStorage(c).GetLogs("1", 1, 2, 0); return err }))
}
