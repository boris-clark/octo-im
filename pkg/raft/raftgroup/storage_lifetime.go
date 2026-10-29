package raftgroup

import (
	"errors"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// ErrRaftBusy means an attached or draining instance still owns this key.
var ErrRaftBusy = errors.New("raftgroup: raft storage is still owned")

// A lease outlives detachment. Its operations retain the original owner until
// both effects and terminal completion disposal have finished. Registry locks
// never cover storage I/O, queue delivery, callbacks or owner joins.
type raftLease struct {
	storage IStorage
	raft    IRaft
	retired bool
	pending int
}

type storageOperation struct {
	storage     IStorage
	lease       *raftLease
	key         string
	request     types.Event
	once        sync.Once
	effectsDone bool
	disposed    bool
}

func (rg *RaftGroup) raftStorageLease(r IRaft) *raftLease {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	lease := rg.storageLeases[r.Key()]
	if lease == nil || lease.retired || lease.raft != r {
		return nil
	}
	return lease
}

func (rg *RaftGroup) isCurrentStorageLease(lease *raftLease, key string) bool {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	return lease != nil && !lease.retired && rg.storageLeases[key] == lease
}

func (rg *RaftGroup) beginReceiptVerification(w *receiptWaiter) *storageOperation {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	lease := w.lease
	if rg.stopBegun.Load() || lease == nil || lease.retired || rg.storageLeases[w.raftKey] != lease {
		return nil
	}
	lease.pending++
	op := &storageOperation{lease: lease, key: w.raftKey, storage: lease.storage}
	rg.storageOperations[op] = struct{}{}
	return op
}

func (rg *RaftGroup) beginStorageOperation(r IRaft, request types.Event) *storageOperation {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	lease := rg.storageLeases[r.Key()]
	if rg.stopBegun.Load() || lease == nil || lease.retired || lease.raft != r {
		return nil
	}
	lease.pending++
	request.Logs = types.CloneLogs(request.Logs)
	request.Config = request.Config.Clone()
	if request.TermStartIndexInfo != nil {
		request.TermStartIndexInfo = request.TermStartIndexInfo.Clone()
	}
	op := &storageOperation{lease: lease, key: r.Key(), request: request, storage: lease.storage}
	rg.storageOperations[op] = struct{}{}
	return op
}

func (rg *RaftGroup) submitStorageOperation(op *storageOperation, fn func()) error {
	if op == nil {
		return ErrRaftBusy
	}
	err := rg.submit(func() {
		defer rg.finishStorageEffects(op)
		fn()
	})
	if err != nil {
		rg.finishStorageEffects(op)
	}
	return err
}

func (rg *RaftGroup) finishStorageEffects(op *storageOperation) {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	op.effectsDone = true
	rg.releaseStorageOperationLocked(op)
}

func (rg *RaftGroup) disposeStorageOperation(op *storageOperation) {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	op.disposed = true
	rg.releaseStorageOperationLocked(op)
}

func (rg *RaftGroup) releaseStorageOperationLocked(op *storageOperation) {
	if !op.effectsDone || !op.disposed {
		return
	}
	if _, exists := rg.storageOperations[op]; !exists {
		return
	}
	delete(rg.storageOperations, op)
	op.lease.pending--
}

// completeStorageOperation is the only asynchronous storage result route. The
// public AddEvent route cannot mint this private envelope. Result data is copied
// before enqueue and never returned to callers for mutation or reuse.
func (rg *RaftGroup) completeStorageOperation(op *storageOperation, e types.Event) {
	if op == nil {
		return
	}
	op.once.Do(func() {
		if e.Type == types.StoreResp {
			e = types.StoreResponse(op.request, e)
		}
		e.Logs = types.CloneLogs(e.Logs)
		e.Config = e.Config.Clone()
		if e.TermStartIndexInfo != nil {
			e.TermStartIndexInfo = e.TermStartIndexInfo.Clone()
		}
		rg.storageMu.Lock()
		stopped := rg.stopBegun.Load()
		if stopped {
			op.disposed = true
			rg.releaseStorageOperationLocked(op)
		} else {
			// BeginStop serializes its final queue disposal with this enqueue.
			rg.mq.MustAdd(Event{RaftKey: op.key, Event: e, storageOperation: op})
		}
		rg.storageMu.Unlock()
		rg.Advance()
	})
}

func (rg *RaftGroup) currentStorageOperation(op *storageOperation, r IRaft) bool {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	return !op.disposed && !op.lease.retired && op.lease.raft == r && rg.storageLeases[op.key] == op.lease
}

func localStorageResponse(t types.EventType) bool {
	switch t {
	case types.StoreResp, types.ApplyResp, types.TruncateResp, types.GetLogsResp,
		types.LearnerToFollowerResp, types.LearnerToLeaderResp, types.FollowerToLeaderResp:
		return true
	}
	return false
}

// disposeStoppedStorage runs after the owner has joined. Workers that have not
// delivered yet dispose directly; no executor capacity is needed for cleanup.
func (rg *RaftGroup) disposeStoppedStorage() {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	for op := range rg.storageOperations {
		op.disposed = true
		rg.releaseStorageOperationLocked(op)
	}
}
