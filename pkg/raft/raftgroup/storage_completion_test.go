package raftgroup

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestStorageCompletionRetainsDispositionAndReleasesGateOnce(t *testing.T) {
	g := newWaitTestGroup(t, WithStorage(&waitTestStorage{}))
	r := g.GetRaft("slot")
	binding := g.StorageBinding(r)
	op := g.beginStorageOperation(r, types.Event{Type: types.ApplyReq})
	require.NotNil(t, op)
	completed, workerRelease := make(chan struct{}), make(chan struct{})
	var workerOnce sync.Once
	releaseWorker := func() { workerOnce.Do(func() { close(workerRelease) }) }
	defer releaseWorker()
	require.NoError(t, g.submitStorageOperation(op, func() {
		g.completeStorageOperation(op, types.Event{Type: types.ApplyResp, Reason: types.ReasonOk, Index: 1})
		close(completed)
		<-workerRelease
	}))
	<-completed
	require.ErrorIs(t, op.access.Run(op.lease.storage, op.key, func() error {
		t.Error("completed operation retained storage authority")
		return nil
	}), ErrRaftBusy)
	g.storageMu.Lock()
	pending, effectsDone := op.lease.pending, op.effectsDone
	g.storageMu.Unlock()
	require.True(t, effectsDone)
	require.Equal(t, 1, pending, "queued disposition must retain its original lease")

	// A new synchronous call can acquire the released gate before the old
	// worker returns. Its gate must survive that worker's deferred cleanup.
	entered, syncRelease := make(chan struct{}), make(chan struct{})
	var syncOnce sync.Once
	releaseSync := func() { syncOnce.Do(func() { close(syncRelease) }) }
	defer releaseSync()
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- binding.WithStorage(func(*StorageCall) error {
			close(entered)
			<-syncRelease
			return nil
		})
	}()
	select {
	case <-entered:
	case err := <-syncDone:
		require.NoError(t, err)
		t.Fatal("synchronous call did not enter")
	}
	releaseWorker()
	waitFor(t, "old worker exit", func() bool {
		g.verificationMu.Lock()
		defer g.verificationMu.Unlock()
		return g.poolTasks == 1
	})
	require.ErrorIs(t, binding.WithStorage(func(*StorageCall) error {
		t.Error("old worker cleanup unlocked a newer call's gate")
		return nil
	}), ErrRaftBusy)
	events := g.mq.Get()
	require.Len(t, events, 1)
	require.Same(t, op, events[0].storageOperation)
	g.disposeStorageOperation(op)
	g.storageMu.Lock()
	pending = op.lease.pending
	_, retained := g.storageOperations[op]
	g.storageMu.Unlock()
	require.False(t, retained)
	require.Equal(t, 1, pending, "only the synchronous call should remain")
	g.BeginStop()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, g.WaitQuiescence(ctx), ErrIncompleteQuiescence)
	releaseSync()
	require.NoError(t, <-syncDone)
	require.NoError(t, g.WaitQuiescence(context.Background()))
}
