package raftgroup

import (
	"errors"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// ErrRaftBusy means an attached or draining instance still owns this key.
var ErrRaftBusy = errors.New("raftgroup: raft storage is still owned")

// BoundStorage is implemented by storage backends whose raw exported methods
// refuse unbound access and require an active RaftGroup lease, reservation, or
// in-flight worker operation.
type BoundStorage interface {
	AttachStorageDomain(g *RaftGroup)
	DetachStorageDomain(g *RaftGroup)
	BoundGetState(key string) (types.RaftState, error)
	BoundAppendLogs(key string, logs []types.Log, termStartIndexInfo *types.TermStartIndexInfo) error
	BoundTruncateLogTo(key string, index uint64) error
	BoundGetLogs(key string, startLogIndex uint64, endLogIndex uint64, limitSize uint64) ([]types.Log, error)
	BoundApply(key string, logs []types.Log) error
	BoundSaveConfig(key string, cfg types.Config) error
	BoundGetTermStartIndex(key string, term uint32) (uint64, error)
	BoundLeaderLastLogTerm(key string) (uint32, error)
	BoundLeaderTermGreaterEqThan(key string, term uint32) (uint32, error)
	BoundDeleteLeaderTermStartIndexGreaterThanTerm(key string, term uint32) error
	BoundMarkDiverged(key string, reason error) error
	BoundSaveHardState(key string, state types.HardState) error
}

// A lease outlives detachment. Its operations retain the original owner until
// both effects and terminal completion disposal have finished. Registry locks
// never cover storage I/O, queue delivery, callbacks or owner joins.
type raftLease struct {
	storage      IStorage
	raft         IRaft
	initializing IRaft
	retired      bool
	pending      int
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
	request = cloneStorageRequest(request)
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
	for _, lease := range rg.storageLeases {
		lease.retired = true
	}
}

// WithActiveStorageLease executes fn under the current active (installed and
// non-retired) lease for key, holding a pending reference so replacement and
// domain close wait for fn to return.
func (rg *RaftGroup) WithActiveStorageLease(key string, fn func() error) error {
	rg.storageMu.Lock()
	if rg.stopBegun.Load() {
		rg.storageMu.Unlock()
		return ErrRaftBusy
	}
	lease := rg.storageLeases[key]
	if lease == nil || lease.retired || lease.raft == nil {
		rg.storageMu.Unlock()
		return ErrRaftBusy
	}
	lease.pending++
	rg.storageMu.Unlock()
	defer func() {
		rg.storageMu.Lock()
		lease.pending--
		rg.storageMu.Unlock()
	}()
	return fn()
}

// WithInitializingStorageLease executes fn while key holds an active
// pre-install reservation created by InitializeRaft.
func (rg *RaftGroup) WithInitializingStorageLease(key string, fn func() error) error {
	rg.storageMu.Lock()
	if rg.stopBegun.Load() {
		rg.storageMu.Unlock()
		return ErrRaftBusy
	}
	lease := rg.storageLeases[key]
	if lease == nil || lease.retired || lease.raft != nil || lease.pending == 0 {
		rg.storageMu.Unlock()
		return ErrRaftBusy
	}
	lease.pending++
	rg.storageMu.Unlock()
	defer func() {
		rg.storageMu.Lock()
		lease.pending--
		rg.storageMu.Unlock()
	}()
	return fn()
}

// WithInstanceStorageLease executes fn under the active lease of the specific
// IRaft instance r, refusing if r has been retired or replaced by another
// instance for the same key.
func (rg *RaftGroup) WithInstanceStorageLease(r IRaft, fn func() error) error {
	if r == nil {
		return ErrRaftBusy
	}
	rg.storageMu.Lock()
	if rg.stopBegun.Load() {
		rg.storageMu.Unlock()
		return ErrRaftBusy
	}
	lease := rg.storageLeases[r.Key()]
	if lease == nil || lease.retired || (lease.raft != r && (lease.raft != nil || lease.pending == 0 || lease.initializing != r)) {
		rg.storageMu.Unlock()
		return ErrRaftBusy
	}
	lease.pending++
	rg.storageMu.Unlock()
	defer func() {
		rg.storageMu.Lock()
		lease.pending--
		rg.storageMu.Unlock()
	}()
	return fn()
}

// HasInFlightStorageEffect reports whether key currently has an accepted worker
// storageOperation whose effects have not yet completed, or an active/reserved
// lease operation.
func (rg *RaftGroup) HasInFlightStorageEffect(key string) bool {
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	for op := range rg.storageOperations {
		if op.key == key && !op.effectsDone {
			return true
		}
	}
	lease := rg.storageLeases[key]
	return lease != nil && !lease.retired && !rg.stopBegun.Load()
}

// IsStorageQuiescent reports whether this RaftGroup has no running event loop,
// no active or initializing leases, no pending lease operations, and no active
// pool tasks.
func (rg *RaftGroup) IsStorageQuiescent() bool {
	rg.verificationMu.Lock()
	started := rg.started.Load()
	stopBegun := rg.stopBegun.Load()
	loopJoined := rg.loopJoined
	poolTasks := rg.poolTasks
	rg.verificationMu.Unlock()

	if started {
		if !stopBegun || !loopJoined || poolTasks != 0 {
			return false
		}
		select {
		case <-rg.poolIdle:
		default:
			return false
		}
	} else if poolTasks != 0 {
		return false
	}

	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	if len(rg.storageOperations) != 0 {
		return false
	}
	for _, lease := range rg.storageLeases {
		if !lease.retired || lease.pending != 0 {
			return false
		}
	}
	return true
}

func cloneStorageRequest(e types.Event) types.Event {
	e.Logs = types.CloneLogs(e.Logs)
	e.Config = e.Config.Clone()
	if e.TermStartIndexInfo != nil {
		e.TermStartIndexInfo = e.TermStartIndexInfo.Clone()
	}
	return e
}
