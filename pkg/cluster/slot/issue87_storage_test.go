package slot

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type issue87Transport struct {
	mu     sync.RWMutex
	groups map[uint64]*raftgroup.RaftGroup
}

func (tr *issue87Transport) Send(key string, e types.Event) {
	tr.mu.RLock()
	g := tr.groups[e.To]
	tr.mu.RUnlock()
	if g == nil {
		return
	}
	data, err := e.Marshal()
	if err != nil {
		panic(err)
	}
	var wire types.Event
	if err = wire.Unmarshal(data); err != nil {
		panic(err)
	}
	g.AddEvent(key, wire)
	g.Advance()
}

func issue87Open(t *testing.T, path string) *PebbleShardLogStorage {
	t.Helper()
	s := &Server{opts: &Options{OnApply: func(uint32, []types.Log) error { return nil }}}
	p := NewPebbleShardLogStorage(s, path, 1)
	require.NoError(t, p.Open())
	return p
}
func issue87Build(t *testing.T, p *PebbleShardLogStorage, id uint64, voters []uint64) *raft.Node {
	t.Helper()
	key := SlotIdToKey(1)
	state, err := p.GetState(key)
	require.NoError(t, err)
	row, err := p.GetTermStartIndex(key, state.LastTerm)
	require.NoError(t, err)
	n := raft.NewNode(row, state, raft.NewOptions(raft.WithKey(key), raft.WithNodeId(id), raft.WithReplicas(voters), raft.WithSaveHardState(func(h types.HardState) error { return p.SaveHardState(key, h) })))
	if id == 1 {
		n.BecomeLeader(max(1, state.HardState.Term))
	} else {
		n.BecomeFollower(max(1, state.HardState.Term), 1)
	}
	return n
}
func issue87Await(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(3 * time.Second):
		t.Fatal("storage barrier was not reached")
	}
}
func issue87State(t *testing.T, g *raftgroup.RaftGroup, storedTerm, committed, applied uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var term uint32
	var commit, apply uint64
	require.NoError(t, g.Do(ctx, SlotIdToKey(1), func(r raftgroup.IRaft) error {
		term = r.(*raft.Node).LastLogTerm()
		commit = r.CommittedIndex()
		apply = r.AppliedIndex()
		return nil
	}))
	require.Equal(t, uint32(storedTerm), term)
	require.Equal(t, committed, commit)
	require.Equal(t, applied, apply)
}

func TestIssue87PebbleCommitPublicationBarriers(t *testing.T) {
	path := t.TempDir()
	p := issue87Open(t, path)
	key := SlotIdToKey(1)
	before, allowCommit, committed, allowDelivery := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	p.appendCommitHook = func(done bool) {
		if !done {
			close(before)
			<-allowCommit
		} else {
			close(committed)
			<-allowDelivery
		}
	}
	tr := &issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}}
	g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTransport(tr), raftgroup.WithTickInterval(10*time.Millisecond)))
	require.NoError(t, g.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := g.InitializeRaft(ctx, key, func() (raftgroup.IRaft, error) { return issue87Build(t, p, 1, []uint64{1}), nil }, nil)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, err := g.ProposeBatchUntilAppliedTimeout(ctx, key, types.ProposeReqSet{{Id: 99005, Data: []byte("durable-payload")}})
		result <- err
	}()
	var commitOnce, deliveryOnce sync.Once
	releaseCommit := func() { commitOnce.Do(func() { close(allowCommit) }) }
	releaseDelivery := func() { deliveryOnce.Do(func() { close(allowDelivery) }) }
	defer func() {
		releaseCommit()
		releaseDelivery()
		g.Stop()
		require.NoError(t, g.WaitQuiescence(context.Background()))
		require.NoError(t, p.Close())
	}()
	issue87Await(t, before)
	last, err := p.LastIndex(key)
	require.NoError(t, err)
	require.Zero(t, last)
	row, err := p.GetTermStartIndex(key, 1)
	require.NoError(t, err)
	require.Zero(t, row)
	require.ErrorIs(t, g.AddEventWait(key, types.Event{Type: types.StoreResp, Term: 100, Index: 1, Reason: types.ReasonOk}), raftgroup.ErrUnauthenticatedStorageResult)
	issue87State(t, g, 0, 0, 0)
	releaseCommit()
	issue87Await(t, committed)
	logs, err := p.GetLogs(key, 1, 2, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.EqualValues(t, 99005, logs[0].Id)
	require.Equal(t, []byte("durable-payload"), logs[0].Data)
	row, err = p.GetTermStartIndex(key, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, row)
	issue87State(t, g, 0, 0, 0)
	diskApplied, err := p.AppliedIndex(key)
	require.NoError(t, err)
	require.Zero(t, diskApplied)
	releaseDelivery()
	require.NoError(t, <-result)
	issue87State(t, g, 1, 1, 1)
	g.Stop()
	require.NoError(t, g.WaitQuiescence(context.Background()))
	require.NoError(t, p.Close())
	p = issue87Open(t, path)
	state, err := p.GetState(key)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.AppliedIndex)
	require.EqualValues(t, 1, state.LastLogIndex)
	logs, err = p.GetLogs(key, 1, 2, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.EqualValues(t, 99005, logs[0].Id)
	require.EqualValues(t, 1, logs[0].Index)
	require.EqualValues(t, 1, logs[0].Term)
	require.Equal(t, []byte("durable-payload"), logs[0].Data)
}

func TestIssue87ThreeNodeDurableCommitApplyRestart(t *testing.T) {
	key := SlotIdToKey(1)
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	expected := []types.Log{}
	for generation := 0; generation < 2; generation++ {
		tr := &issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}}
		stores := make([]*PebbleShardLogStorage, 3)
		groups := make([]*raftgroup.RaftGroup, 3)
		for i := range stores {
			stores[i] = issue87Open(t, paths[i])
			p := stores[i]
			g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTransport(tr), raftgroup.WithTickInterval(5*time.Millisecond)))
			require.NoError(t, g.AddRaft(issue87Build(t, p, uint64(i+1), []uint64{1, 2, 3})))
			groups[i] = g
			tr.groups[uint64(i+1)] = g
		}
		func() {
			defer func() {
				for i, g := range groups {
					g.Stop()
					require.NoError(t, g.WaitQuiescence(context.Background()))
					require.NoError(t, stores[i].Close())
				}
			}()
			for _, g := range groups {
				require.NoError(t, g.Start())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			id := uint64(99005 + generation)
			payload := []byte(fmt.Sprintf("payload-%d", id))
			responses, err := groups[0].ProposeBatchUntilAppliedTimeout(ctx, key, types.ProposeReqSet{{Id: id, Data: payload}})
			require.NoError(t, err)
			require.Len(t, responses, 1)
			expected = append(expected, types.Log{Index: uint64(generation + 1), Term: 1, Id: id, Data: payload})
			require.EqualValues(t, generation+1, responses[0].Index)
			require.Eventually(t, func() bool {
				for i, p := range stores {
					index, err := p.AppliedIndex(key)
					if err != nil || index != uint64(len(expected)) {
						return false
					}
					state, err := groups[i].ReadLeaderState(ctx, key)
					if err != nil || state.AppliedIndex != uint64(len(expected)) {
						return false
					}
				}
				return true
			}, 3*time.Second, 5*time.Millisecond)
			for i, p := range stores {
				logs, err := p.GetLogs(key, 1, uint64(len(expected)+1), 0)
				require.NoError(t, err)
				require.True(t, types.SameLogs(expected, logs), "replica %d generation %d identity mismatch: %+v", i, generation, logs)
				var committed, applied uint64
				require.NoError(t, groups[i].Do(ctx, key, func(r raftgroup.IRaft) error { committed = r.CommittedIndex(); applied = r.AppliedIndex(); return nil }))
				require.GreaterOrEqual(t, committed, uint64(len(expected)))
				require.EqualValues(t, len(expected), applied)
			}
		}()
	}
	for _, path := range paths {
		p := issue87Open(t, path)
		logs, err := p.GetLogs(key, 1, 3, 0)
		require.NoError(t, err)
		require.True(t, types.SameLogs(expected, logs))
		require.True(t, bytes.Equal(expected[1].Data, logs[1].Data))
		require.NoError(t, p.Close())
	}
}

func TestIssue87PebbleAcceptedApplyDrainsBeforeReload(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	key := SlotIdToKey(1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	p.s.opts.OnApply = func(uint32, []types.Log) error { once.Do(func() { close(entered); <-release }); return nil }
	tr := &issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}}
	g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTransport(tr), raftgroup.WithTickInterval(5*time.Millisecond)))
	old := issue87Build(t, p, 1, []uint64{1})
	require.NoError(t, g.AddRaft(old))
	require.NoError(t, g.Start())
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() {
		unblock()
		g.Stop()
		require.NoError(t, g.WaitQuiescence(context.Background()))
		require.NoError(t, p.Close())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := g.ProposeBatchUntilAppliedTimeout(ctx, key, types.ProposeReqSet{{Id: 100, Data: []byte("old-apply")}})
		result <- err
	}()
	issue87Await(t, entered)
	g.RemoveRaft(old)
	require.ErrorIs(t, <-result, types.ErrProposalOutcomeUnknown)
	reads := 0
	build := func() (raftgroup.IRaft, error) { reads++; return issue87Build(t, p, 1, []uint64{1}), nil }
	_, err := g.InitializeRaft(ctx, key, build, nil)
	require.Zero(t, reads, "reload read while accepted Apply holds the original storage lock")
	require.ErrorIs(t, err, raftgroup.ErrRaftBusy)
	unblock()
	var fresh raftgroup.IRaft
	require.Eventually(t, func() bool { var err error; fresh, err = g.InitializeRaft(ctx, key, build, nil); return err == nil }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, reads)
	state, err := p.GetState(key)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.AppliedIndex)
	var oldApplied uint64
	require.NoError(t, g.Do(ctx, key, func(raftgroup.IRaft) error { oldApplied = old.AppliedIndex(); return nil }))
	require.EqualValues(t, 1, oldApplied, "accepted Apply terminal result must reach the detached original instance")
	require.NotSame(t, old, fresh)
	responses, err := g.ProposeBatchUntilAppliedTimeout(ctx, key, types.ProposeReqSet{{Id: 200, Data: []byte("replacement")}})
	require.NoError(t, err)
	require.EqualValues(t, 2, responses[0].Index)
	logs, err := p.GetLogs(key, 1, 3, 0)
	require.NoError(t, err)
	require.True(t, types.SameLogs([]types.Log{{Index: 1, Term: 1, Id: 100, Data: []byte("old-apply")}, {Index: 2, Term: 1, Id: 200, Data: []byte("replacement")}}, logs))
}
