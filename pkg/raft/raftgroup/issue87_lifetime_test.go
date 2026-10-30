package raftgroup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestIssue87DetachRetainsOwnershipThroughReceiptCleanup(t *testing.T) {
	g := New(NewOptions(WithStorage(&memStorage{}), WithTransport(dropTransport{})))
	old := issue87Node(admKey, 0, 0)
	fresh := issue87Node(admKey, 0, 0)
	require.NoError(t, g.AddRaft(old))
	defer func() {
		g.Stop()
		require.NoError(t, g.WaitQuiescence(context.Background()))
		g.goPool.Release()
	}()

	// Hold actual receipt cleanup after detachment. A replacement must be
	// refused while the old key-wide cleanup can still affect its waiters.
	bucket := g.wait.bucket(admKey)
	bucket.mu.Lock()
	locked := true
	defer func() {
		if locked {
			bucket.mu.Unlock()
		}
	}()
	removed := make(chan struct{})
	go func() { g.RemoveRaft(old); close(removed) }()
	waitFor(t, "old instance detached", func() bool { return g.GetRaft(admKey) == nil })
	added := make(chan error, 1)
	go func() { added <- g.AddRaft(fresh) }()
	var addErr error
	returned := false
	select {
	case addErr = <-added:
		returned = true
	case <-time.After(100 * time.Millisecond):
		t.Error("replacement must refuse until detached receipt cleanup finishes")
	}
	bucket.mu.Unlock()
	locked = false
	<-removed
	if !returned {
		addErr = <-added
	}
	require.ErrorIs(t, addErr, ErrRaftBusy)
	require.NoError(t, g.AddRaft(fresh))
	w, _ := g.wait.register(receipt{lease: g.raftStorageLease(fresh), raftKey: admKey, seq: 200, term: 1, first: 1, last: 1, logs: []types.Log{{Id: 200, Index: 1}}})
	g.RemoveRaft(old)
	select {
	case <-w.doneC:
		t.Fatal("stale retirement cleanup settled a replacement receipt")
	default:
	}
	require.Same(t, fresh, g.GetRaft(admKey))
}

func issue87Pending(g *RaftGroup) (effects, dispositions int) {
	g.storageMu.Lock()
	defer g.storageMu.Unlock()
	for op := range g.storageOperations {
		if !op.effectsDone {
			effects++
		}
		if !op.disposed {
			dispositions++
		}
	}
	return
}

func issue87Node(key string, last, applied uint64) *raft.Node {
	return raft.NewNode(1, types.RaftState{LastLogIndex: last, LastTerm: 1, AppliedIndex: applied}, raft.NewOptions(raft.WithKey(key), raft.WithNodeId(1), raft.WithReplicas([]uint64{1})))
}

func TestIssue87ReplacementWaitsForEffectsAndDisposition(t *testing.T) {
	gate := make(chan struct{})
	storage := &memStorage{appendGate: gate}
	g := New(NewOptions(WithStorage(storage), WithTransport(dropTransport{}), WithTickInterval(time.Hour)))
	old := issue87Node(admKey, 0, 0)
	require.NoError(t, g.AddRaft(old))
	released := false
	defer func() {
		if !released {
			close(gate)
		}
		g.Stop()
		require.NoError(t, g.WaitQuiescence(context.Background()))
		g.goPool.Release()
	}()
	_, err := old.Admit([]types.Log{{Id: 100, Data: []byte("accepted-old")}})
	require.NoError(t, err)
	w, _ := g.wait.register(receipt{lease: g.raftStorageLease(old), raftKey: admKey, seq: 1, term: 1, first: 1, last: 1, logs: []types.Log{{Id: 100, Index: 1, Data: []byte("accepted-old")}}})
	require.True(t, g.handleReady(old))
	var accepted *storageOperation
	g.storageMu.Lock()
	for op := range g.storageOperations {
		accepted = op
	}
	g.storageMu.Unlock()
	require.NotNil(t, accepted)
	waitFor(t, "accepted effect", func() bool { effects, _ := issue87Pending(g); return effects == 1 })
	g.RemoveRaft(old)
	require.ErrorIs(t, <-w.doneC, types.ErrProposalOutcomeUnknown)
	var builds atomic.Int32
	build := func() (IRaft, error) { builds.Add(1); return issue87Node(admKey, 1, 0), nil }
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = g.InitializeRaft(ctx, admKey, build, nil)
	require.Zero(t, builds.Load(), "reload read while old effect is accepted")
	require.ErrorIs(t, err, ErrRaftBusy)
	close(gate)
	released = true
	waitFor(t, "effects finished before delivery", func() bool { g.storageMu.Lock(); defer g.storageMu.Unlock(); return accepted.effectsDone })
	g.storageMu.Lock()
	pending := accepted.lease.pending
	g.storageMu.Unlock()
	require.Equal(t, 1, pending, "terminal disposition was released before owner consumption")
	require.Equal(t, []uint64{100}, storage.storedIDs(), "old accepted effect must finish")
	_, err = g.InitializeRaft(ctx, admKey, build, nil)
	require.Zero(t, builds.Load(), "terminal disposition is part of drain")
	require.ErrorIs(t, err, ErrRaftBusy)
	g.handleReceivedEvents()
	require.Equal(t, 0, len(g.storageOperations))
	select {
	case <-w.doneC:
		t.Fatal("old waiter completed twice")
	default:
	}
	fresh := issue87Node(admKey, 1, 0)
	require.NoError(t, g.AddRaft(fresh))
	g.RemoveRaft(old)
	require.Same(t, fresh, g.GetRaft(admKey), "stale detach removed a replacement")
	require.NoError(t, g.Start())
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resps, err := g.ProposeBatchUntilAppliedTimeout(ctx, admKey, types.ProposeReqSet{{Id: 200, Data: []byte("new-waiter")}})
	require.NoError(t, err)
	require.EqualValues(t, 2, resps[0].Index)
	require.Equal(t, []uint64{100, 200}, storage.storedIDs())
	require.EqualValues(t, 0, g.wait.pending())
}

func TestIssue87InitialReservationBeforeReadAndShutdown(t *testing.T) {
	g := New(NewOptions(WithStorage(&memStorage{}), WithTransport(dropTransport{})))
	require.NoError(t, g.Start())
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := g.InitializeRaft(context.Background(), admKey, func() (IRaft, error) { close(entered); <-release; return issue87Node(admKey, 0, 0), nil }, nil)
		result <- err
	}()
	<-entered
	var reads atomic.Int32
	_, err := g.InitializeRaft(context.Background(), admKey, func() (IRaft, error) { reads.Add(1); return nil, errors.New("must not read") }, nil)
	require.ErrorIs(t, err, ErrRaftBusy)
	require.Zero(t, reads.Load())
	g.BeginStop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	require.ErrorIs(t, g.WaitQuiescence(ctx), ErrIncompleteQuiescence)
	cancel()
	close(release)
	require.ErrorIs(t, <-result, ErrGroupStopped)
	require.NoError(t, g.WaitQuiescence(context.Background()))
	require.Nil(t, g.GetRaft(admKey), "shutdown allowed late initialization")
	g.Stop()
	g.goPool.Release()
}

func TestIssue87LocalResultsRequirePrivateWorkerRoute(t *testing.T) {
	g := New(NewOptions(WithStorage(&memStorage{}), WithTransport(dropTransport{}), WithTickInterval(time.Hour)))
	n := issue87Node(admKey, 0, 0)
	require.NoError(t, g.AddRaft(n))
	require.NoError(t, g.Start())
	defer func() { g.Stop(); require.NoError(t, g.WaitQuiescence(context.Background())); g.goPool.Release() }()
	for _, typ := range []types.EventType{types.StoreResp, types.ApplyResp, types.TruncateResp, types.GetLogsResp, types.LearnerToLeaderResp} {
		require.ErrorIs(t, g.AddEventWait(admKey, types.Event{Type: typ, Term: 100, Reason: types.ReasonOk, Index: 100}), ErrUnauthenticatedStorageResult)
	}
	var term uint32
	var applied uint64
	require.NoError(t, g.Do(context.Background(), admKey, func(r IRaft) error { term = r.LastTerm(); applied = r.AppliedIndex(); return nil }))
	require.EqualValues(t, 1, term)
	require.Zero(t, applied)
}

func TestIssue87StorePoolRefusalCanRetry(t *testing.T) {
	gate := make(chan struct{})
	storage := &memStorage{appendGate: gate}
	g := New(NewOptions(WithStorage(storage), WithTransport(dropTransport{}), WithGoPoolSize(1)))
	n := issue87Node(admKey, 0, 0)
	other := issue87Node("other", 0, 0)
	require.NoError(t, g.AddRaft(n))
	require.NoError(t, g.AddRaft(other))
	defer func() { g.Stop(); require.NoError(t, g.WaitQuiescence(context.Background())); g.goPool.Release() }()
	_, err := n.Admit([]types.Log{{Id: 100}})
	require.NoError(t, err)
	g.handleReady(n)
	_, err = other.Admit([]types.Log{{Id: 200}})
	require.NoError(t, err)
	g.handleReady(other)
	waitFor(t, "refused completion", func() bool { g.mq.mu.Lock(); defer g.mq.mu.Unlock(); return len(g.mq.nodrop) > 0 })
	g.handleReceivedEvents()
	require.Zero(t, other.CommittedIndex(), "submission refusal must not commit")
	close(gate)
	waitFor(t, "first worker exit", func() bool { effects, _ := issue87Pending(g); return effects == 0 })
	g.handleReceivedEvents()
	// Now the bounded executor accepts a fresh request for the refused range.
	g.handleReady(other)
	waitFor(t, "retry worker exit", func() bool { effects, _ := issue87Pending(g); return effects == 0 })
	g.handleReceivedEvents()
	require.EqualValues(t, 1, other.CommittedIndex())
}

func TestIssue87WorkerCopiesRequestBeforeSubmission(t *testing.T) {
	gate := make(chan struct{})
	storage := &memStorage{appendGate: gate}
	g := New(NewOptions(WithStorage(storage), WithTransport(dropTransport{})))
	n := issue87Node(admKey, 0, 0)
	require.NoError(t, g.AddRaft(n))
	defer func() { g.Stop(); require.NoError(t, g.WaitQuiescence(context.Background())); g.goPool.Release() }()
	_, err := n.Admit([]types.Log{{Id: 100, Data: []byte("original")}})
	require.NoError(t, err)
	var req types.Event
	for _, e := range n.Ready() {
		if e.Type == types.StoreReq {
			req = e
		}
	}
	require.NotEmpty(t, req.Logs)
	g.handleStoreReq(n, req)
	req.Logs[0].Id = 200
	req.Logs[0].Data[0] = 'x'
	req.TermStartIndexInfo.Index = 99
	close(gate)
	waitFor(t, "immutable worker result", func() bool { effects, _ := issue87Pending(g); return effects == 0 })
	g.handleReceivedEvents()
	require.Equal(t, []uint64{100}, storage.storedIDs(), "a reused request mutated bytes accepted by the original worker")
	logs, err := storage.GetLogs(admKey, 1, 2, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("original"), logs[0].Data)
	row, err := storage.GetTermStartIndex(admKey, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, row)
	require.EqualValues(t, 1, n.CommittedIndex(), "the genuine unchanged request must complete")
}
