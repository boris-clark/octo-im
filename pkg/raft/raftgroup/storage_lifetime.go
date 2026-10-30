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
	AttachStorageDomain(g *RaftGroup) error
	DetachStorageDomain(g *RaftGroup)
	ClaimStorageKey(g *RaftGroup, key string) error
	BindStorage(call *StorageCall) IStorage
}

// StorageCall is an opaque, original-operation capability. It cannot be
// constructed from a key or borrowed from another in-flight operation.
// Run joins entered calls before revocation, including callback reads.
type StorageCall struct {
	mu      sync.RWMutex
	storage IStorage
	key     string
	active  bool
	initial bool
}

func (c *StorageCall) Run(storage IStorage, key string, fn func() error) error {
	if c == nil {
		return ErrRaftBusy
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.active || c.storage != storage || c.key != key {
		return ErrRaftBusy
	}
	return fn()
}

// RunWrite refuses ordinary mutation through a reconstruction reservation.
func (c *StorageCall) RunWrite(storage IStorage, key string, hardState bool, fn func() error) error {
	return c.Run(storage, key, func() error {
		if c.initial && !hardState {
			return ErrRaftBusy
		}
		return fn()
	})
}

func (c *StorageCall) revoke() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.active = false
	c.mu.Unlock()
}

// StorageBinding retains one immutable lease epoch, even after replacement.
// Only RaftGroup can issue it. Each WithStorage call admits exactly once.
type StorageBinding struct {
	group *RaftGroup
	lease *raftLease
	key   string
}

func (rg *RaftGroup) StorageBinding(r IRaft) *StorageBinding {
	if r == nil {
		return nil
	}
	rg.storageMu.Lock()
	defer rg.storageMu.Unlock()
	lease := rg.storageLeases[r.Key()]
	if lease == nil || lease.raft != r || lease.retired {
		return nil
	}
	return &StorageBinding{group: rg, lease: lease, key: r.Key()}
}

func (b *StorageBinding) WithStorage(fn func(*StorageCall) error) error {
	if b == nil || b.group == nil {
		return ErrRaftBusy
	}
	rg := b.group
	rg.verificationMu.Lock()
	rg.storageMu.Lock()
	if rg.stopBegun.Load() || b.lease.retired || rg.storageLeases[b.key] != b.lease || !b.lease.effect.TryLock() {
		rg.storageMu.Unlock()
		rg.verificationMu.Unlock()
		return ErrRaftBusy
	}
	initial := b.lease.raft == nil
	b.lease.pending++
	rg.poolTasks++
	rg.storageMu.Unlock()
	rg.verificationMu.Unlock()
	call := &StorageCall{storage: b.lease.storage, key: b.key, active: true, initial: initial}
	defer func() {
		call.revoke()
		b.lease.effect.Unlock()
		rg.storageMu.Lock()
		b.lease.pending--
		rg.storageMu.Unlock()
		rg.poolTaskDone()
	}()
	return fn(call)
}

// A lease outlives detachment. Its operations retain the original owner until
// both effects and terminal completion disposal have finished. Registry locks
// never cover storage I/O, queue delivery, callbacks or owner joins.
type raftLease struct {
	effect       sync.Mutex
	storage      IStorage
	raft         IRaft
	initializing IRaft
	retired      bool
	pending      int
}

type storageOperation struct {
	access      *StorageCall
	storage     IStorage
	lease       *raftLease
	key         string
	request     types.Event
	once        sync.Once
	effectOnce  sync.Once
	effectHeld  bool // Set by the accepted worker before enabling access.
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
	op.access = &StorageCall{storage: lease.storage, key: op.key}
	if bound, ok := lease.storage.(BoundStorage); ok {
		op.storage = bound.BindStorage(op.access)
	}
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
	op.access = &StorageCall{storage: lease.storage, key: op.key}
	if bound, ok := lease.storage.(BoundStorage); ok {
		op.storage = bound.BindStorage(op.access)
	}
	rg.storageOperations[op] = struct{}{}
	return op
}

func (rg *RaftGroup) submitStorageOperation(op *storageOperation, fn func()) error {
	if op == nil {
		return ErrRaftBusy
	}
	err := rg.submit(func() {
		defer rg.finishStorageEffects(op)
		op.lease.effect.Lock()
		op.effectHeld = true
		op.access.mu.Lock()
		op.access.active = true
		op.access.mu.Unlock()
		fn()
	})
	if err != nil {
		rg.finishStorageEffects(op)
	}
	return err
}

// releaseStorageEffectGate revokes and joins entered calls before releasing the
// original lease's gate. Completion and worker cleanup may both reach it; only
// the first call may unlock, even if a new operation has since acquired the gate.
// It runs on the accepted worker, or after submission was refused (no worker).
func (rg *RaftGroup) releaseStorageEffectGate(op *storageOperation) {
	op.effectOnce.Do(func() {
		op.access.revoke()
		if op.effectHeld {
			op.lease.effect.Unlock()
		}
	})
}

func (rg *RaftGroup) finishStorageEffects(op *storageOperation) {
	rg.releaseStorageEffectGate(op)
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
		// The copied result is terminal: no later storage access through this
		// operation is permitted. Pending disposition still owns the lease.
		rg.releaseStorageEffectGate(op)
		rg.storageMu.Lock()
		op.effectsDone = true
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

	if !stopBegun || !loopJoined {
		return false
	}
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
