package raftgroup

import (
	"errors"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

// Pause a normal worker after result publication, modeling preemption between
// completeStorageOperation and the closure return in handleApplyReq.
func TestXin2683CompletionRequiresReleasedEffectGate(t *testing.T) {
	g := newWaitTestGroup(t, WithStorage(&waitTestStorage{}))
	r := g.GetRaft("slot")
	op := g.beginStorageOperation(r, types.Event{Type: types.ApplyReq})
	require.NotNil(t, op)
	published, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	defer g.disposeStorageOperation(op)
	require.NoError(t, g.submitStorageOperation(op, func() {
		g.completeStorageOperation(op, types.Event{Type: types.ApplyResp, Reason: types.ReasonOk, Index: 1})
		close(published)
		<-release
	}))
	<-published
	events := g.mq.Get()
	require.Len(t, events, 1, "terminal result was not published")
	require.Same(t, op, events[0].storageOperation)
	err := g.StorageBinding(r).WithStorage(func(*StorageCall) error { return nil })
	if errors.Is(err, ErrRaftBusy) {
		t.Fatal("terminal ApplyResp is visible to the owner while its worker still owns the effect gate")
	}
	require.NoError(t, err)
}
