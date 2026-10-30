package raftgroup

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/track"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/lni/goutils/syncutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type RaftGroup struct {
	ownerMu           sync.Mutex
	storageMu         sync.Mutex
	storageLeases     map[string]*raftLease
	storageOperations map[*storageOperation]struct{}

	raftList   *linkedList
	stopper    *syncutil.Stopper
	opts       *Options
	advanceC   chan struct{}
	readStateC chan readStateRequest
	ownerC     chan ownerRequest

	tmpRafts []IRaft
	stopped  bool

	goPool *ants.Pool
	wklog.Log

	mq *EventQueue

	wait *wait

	// Forwarded proposals awaiting the leader's response; see forward_token.go.
	forwardMu          sync.Mutex
	forwardPending     map[string]*forwardRegistration
	forwardIncarnation uint64
	forwardAttempt     uint64
	forwardStats       forwardStats
	forwardWarned      map[uint64]uint8
	// forwardObserve, if set by a test, sees correlation events under forwardMu.
	forwardObserve func(key, phase, detail string)

	proposalSeq   atomic.Uint64 // receipt identity, unique within the group
	admissionSems sync.Map      // raftKey -> chan struct{}, see admissionSem
	proposalStats proposalStats
	started       atomic.Bool
	stopOnce      sync.Once
	stopBegun     atomic.Bool
	// loopDone closes once the shutdown lifecycle joined the event loop,
	// fenced pool submission and settled receipts.
	loopDone       chan struct{}
	verificationMu sync.Mutex
	// verificationStopped fences pool submission. BeginStop sets it before it
	// joins anything, so no storage task is accepted once shutdown began.
	verificationStopped bool
	// loopJoined is set once the event loop has exited.
	loopJoined          bool
	verificationWorkers atomic.Int64
	// poolTasks counts accepted pool tasks, all of which may touch Storage.
	// poolIdle closes once the loop is joined and the count is zero.
	poolTasks int
	poolIdle  chan struct{}
	// beforeAdmit, if set by a test, runs on the event loop after a proposal
	// left pending and before it is admitted.
	beforeAdmit func(seq uint64)
}

func New(opts *Options) *RaftGroup {
	rg := &RaftGroup{
		raftList:           newLinkedList(),
		storageLeases:      make(map[string]*raftLease),
		storageOperations:  make(map[*storageOperation]struct{}),
		stopper:            syncutil.NewStopper(),
		opts:               opts,
		advanceC:           make(chan struct{}, 1),
		readStateC:         make(chan readStateRequest),
		ownerC:             make(chan ownerRequest),
		Log:                wklog.NewWKLog(fmt.Sprintf("RaftGroup[%s]", opts.LogPrefix)),
		mq:                 NewEventQueue(opts.ReceiveQueueLength, false, 0, 0),
		forwardPending:     make(map[string]*forwardRegistration),
		forwardIncarnation: newForwardIncarnation(),
		forwardWarned:      make(map[uint64]uint8),
		poolIdle:           make(chan struct{}),
		loopDone:           make(chan struct{}),
	}
	storage := opts.Storage
	if bs, ok := storage.(BoundStorage); ok {
		bs.AttachStorageDomain(rg)
	}
	rg.wait = newWait(func(key string, index uint64) (types.Log, bool, error) { return readStorageLog(storage, key, index) })
	var err error
	rg.goPool, err = ants.NewPool(opts.GoPoolSize, ants.WithNonblocking(true), ants.WithPanicHandler(func(i interface{}) {
		rg.Panic("raftgroup:go pool panic", zap.Any("panic", i), zap.Stack("stack"))
	}))
	if err != nil {
		rg.Panic("create go pool failed", zap.Error(err))
	}
	rg.wait.dispatchVerification = rg.verifyReceiptAsync
	return rg
}

func (rg *RaftGroup) Options() *Options {
	return rg.opts
}

// AddRaft refuses replacement until accepted old storage effects and their
// terminal dispositions have drained. Callers must retry with a freshly read
// candidate after ErrRaftBusy; construction itself is not yet reserved here.
func (rg *RaftGroup) AddRaft(r IRaft) error {
	rg.ownerMu.Lock()
	defer rg.ownerMu.Unlock()
	return rg.addRaft(r)
}

// addRaft runs with ownerMu held.
func (rg *RaftGroup) addRaft(r IRaft) error {
	rg.storageMu.Lock()
	if rg.stopBegun.Load() {
		rg.storageMu.Unlock()
		return ErrGroupStopped
	}
	old := rg.storageLeases[r.Key()]
	if old != nil && (!old.retired || old.pending != 0 || old.raft == r) {
		rg.storageMu.Unlock()
		return ErrRaftBusy
	}
	rg.wait.removeKey(r.Key(), proposalOutcomeUnknown(ErrRaftNotExist))
	rg.storageLeases[r.Key()] = &raftLease{raft: r, storage: rg.opts.Storage}
	rg.raftList.push(r)
	rg.storageMu.Unlock()
	if rg.opts.Event != nil {
		rg.opts.Event.OnAddRaft(r)
	}
	return nil
}

func (rg *RaftGroup) RemoveRaft(r IRaft) {
	// Fence admission immediately; AddRaft's owner serialization separately
	// acknowledges detachment before any replacement can run.
	rg.removeRaft(r)
}

// removeRaft fences the exact instance without waiting for its storage effects.
func (rg *RaftGroup) removeRaft(r IRaft) {
	rg.storageMu.Lock()
	lease := rg.storageLeases[r.Key()]
	if lease == nil || lease.retired || lease.raft != r {
		rg.storageMu.Unlock()
		return
	}
	lease.retired = true
	// Key-wide receipt and admission cleanup still belongs to this instance.
	// Keep replacement fenced until that cleanup and its callback return.
	lease.pending++
	rg.raftList.remove(r.Key())
	rg.storageMu.Unlock()
	defer func() {
		rg.storageMu.Lock()
		lease.pending--
		rg.storageMu.Unlock()
	}()
	rg.wait.removeKey(r.Key(), proposalOutcomeUnknown(ErrRaftNotExist))
	rg.admissionSems.Delete(r.Key())
	if rg.opts.Event != nil {
		rg.opts.Event.OnRemoveRaft(r)
	}
}

func (rg *RaftGroup) GetRaft(raftKey string) IRaft {
	return rg.raftList.get(raftKey)
}

func (rg *RaftGroup) GetRafts() []IRaft {

	return rg.raftList.all()
}

func (rg *RaftGroup) GetRaftCount() int {
	return rg.raftList.count()
}

func (rg *RaftGroup) IsLeader(raftKey string) bool {
	raft := rg.raftList.get(raftKey)
	if raft == nil {
		return false
	}
	return raft.IsLeader()
}

func (rg *RaftGroup) Start() error {
	rg.started.Store(true)
	rg.stopper.RunWorker(rg.loopEvent)
	return nil
}

// Stop runs the shutdown lifecycle and waits until it joined the event loop
// and settled receipts. Submission is fenced from BeginStop on. Stop does not
// wait for pool tasks accepted before the fence, which may still be inside
// Storage: a caller that releases Storage must use WaitQuiescence. Because Stop
// never joins pool tasks, a task that hands its result to the loop cannot
// deadlock it.
func (rg *RaftGroup) Stop() {
	rg.BeginStop()
	<-rg.loopDone
}

// BeginStop starts the single shutdown lifecycle without waiting for it.
// On return no pool task can be accepted any more and AddEventWait refuses,
// so no storage task starts after this point; tasks accepted before stay
// owned until WaitQuiescence. Callers blocked on the group observe the stop
// signal immediately; the event loop may still be inside synchronous Storage
// I/O such as SaveHardState, so its join runs on the lifecycle goroutine and
// WaitQuiescence bounds it.
func (rg *RaftGroup) BeginStop() {
	rg.stopOnce.Do(func() {
		rg.verificationMu.Lock()
		rg.verificationStopped = true
		rg.stopBegun.Store(true)
		rg.verificationMu.Unlock()
		go func() {
			// Storage work the loop requests from here on is refused and
			// answered with ReasonError, which it never steps: it exits at its
			// next iteration.
			rg.stopper.Stop()
			rg.verificationMu.Lock()
			rg.disposeStoppedStorage()
			rg.loopJoined = true
			if rg.poolTasks == 0 {
				close(rg.poolIdle)
			}
			rg.verificationMu.Unlock()
			// Proposals still queued are abandoned by their callers; receipts of
			// admitted ones can no longer complete.
			rg.wait.completeAll(proposalOutcomeUnknown(ErrGroupStopped))
			close(rg.loopDone)
		}()
	})
}

// readLog point-reads one stored log for receipt verification.
func readStorageLog(storage IStorage, raftKey string, index uint64) (types.Log, bool, error) {
	var (
		logs []types.Log
		err  error
	)
	if bs, ok := storage.(BoundStorage); ok {
		logs, err = bs.BoundGetLogs(raftKey, index, index+1, 0)
	} else {
		logs, err = storage.GetLogs(raftKey, index, index+1, 0)
	}
	if err != nil {
		return types.Log{}, false, err
	}
	for _, log := range logs {
		if log.Index == index {
			return log, true, nil
		}
	}
	return types.Log{}, false, nil
}

func (rg *RaftGroup) Advance() {
	select {
	case rg.advanceC <- struct{}{}:
	default:
	}
}

// AddEvent 添加事件
func (rg *RaftGroup) AddEvent(raftKey string, e types.Event) {

	if e.Type == types.SendPropose {
		err := rg.submit(func() {
			rg.handleSendPropose(raftKey, e)
		})
		if err != nil {
			rg.Error("submit go pool failed", zap.Error(err))
		}
		return
	} else if e.Type == types.SendProposeResp {
		rg.handleSendProposeResp(raftKey, e)
		return
	}

	rg.mq.MustAdd(Event{
		RaftKey: raftKey,
		Event:   e,
	})
}

// AddEventWait steps e on the event loop and returns its result. It returns
// ErrGroupStopped, and e is never stepped, if shutdown began before the loop
// took the event; a result the loop already produced is kept.
func (rg *RaftGroup) AddEventWait(raftKey string, e types.Event) error {
	if rg.stopBegun.Load() {
		return ErrGroupStopped
	}
	waitC := make(chan error, 1)
	claim := new(atomic.Bool)
	rg.mq.MustAdd(Event{
		RaftKey: raftKey,
		Event:   e,
		WaitC:   waitC,
		claim:   claim,
	})
	rg.Advance()
	select {
	case err := <-waitC:
		return err
	case <-rg.stopper.ShouldStop():
	}
	if claim.CompareAndSwap(false, true) {
		// The loop never took the event and now never will.
		return ErrGroupStopped
	}
	// The loop is handling it in memory; its result follows.
	return <-waitC
}

func (rg *RaftGroup) TryAddEvent(raftKey string, e types.Event) {
	if e.Type == types.SendPropose {
		rg.handleSendPropose(raftKey, e)
		return
	} else if e.Type == types.SendProposeResp {
		rg.handleSendProposeResp(raftKey, e)
		return
	}

	rg.mq.Add(Event{
		RaftKey: raftKey,
		Event:   e,
	})
}

func (rg *RaftGroup) loopEvent() {
	tk := time.NewTicker(rg.opts.TickInterval)
	defer tk.Stop()
	for !rg.stopped {
		if rg.stopBegun.Load() {
			// Storage submission is fenced; do not step its refusals.
			return
		}
		rg.readyEvents()
		select {
		case <-tk.C:
			rg.ticks()
		case <-rg.advanceC:
		case req := <-rg.ownerC:
			if err := req.ctx.Err(); err != nil {
				req.result <- err
			} else {
				rg.ownerMu.Lock()
				err := req.fn(rg.GetRaft(req.key))
				rg.ownerMu.Unlock()
				req.result <- err
			}
		case req := <-rg.readStateC:
			rg.ownerMu.Lock()
			rg.handleReadState(req)
			rg.ownerMu.Unlock()
		case <-rg.stopper.ShouldStop():
			return
		}
	}
}

func (rg *RaftGroup) readyEvents() {
	rg.ownerMu.Lock()
	defer rg.ownerMu.Unlock()
	hasEvent := false
	// 处理收到的事件
	has := rg.handleReceivedEvents()
	if has {
		hasEvent = true
	}
	// 处理产生的事件
	has = rg.handleReadys()
	if has {
		hasEvent = true
	}

	if hasEvent {
		rg.Advance()
	}
}

// 处理收到的事件
func (rg *RaftGroup) handleReceivedEvents() bool {
	events := rg.mq.Get()
	if len(events) == 0 {
		return false
	}
	hasEvent := false
	for _, e := range events {
		if e.claim != nil && !e.claim.CompareAndSwap(false, true) {
			continue // abandoned by its waiter after shutdown began
		}
		raft := rg.raftList.get(e.RaftKey)
		if e.storageOperation != nil {
			op := e.storageOperation
			if !rg.currentStorageOperation(op, raft) {
				// Accepted Apply remains truthful for its detached original
				// instance. It cannot resolve replacement waiters.
				if e.Type == types.ApplyResp {
					_ = op.lease.raft.Step(e.Event)
				}
				rg.disposeStorageOperation(op)
				continue
			}
		} else if localStorageResponse(e.Type) {
			if e.WaitC != nil {
				e.WaitC <- ErrUnauthenticatedStorageResult
			}
			continue
		}
		if e.Proposal != nil {
			if raft == nil {
				rg.Warn("raft not found", zap.String("raftKey", e.RaftKey), zap.String("event", e.Event.Type.String()))
			}
			rg.admitProposal(raft, e)
			hasEvent = true
			continue
		}
		if raft == nil {
			rg.Warn("raft not found", zap.String("raftKey", e.RaftKey), zap.String("event", e.Event.Type.String()))
			if e.WaitC != nil {
				e.WaitC <- ErrRaftNotExist
			}
			continue
		}

		// 轨迹记录
		if e.Type == types.Propose {
			for i := range e.Logs {
				e.Logs[i].Record.Add(track.PositionPropose)
			}
		}
		hasEvent = true
		err := raft.Step(e.Event)
		if e.storageOperation != nil && e.Type == types.ApplyResp && e.Reason == types.ReasonOk && err == nil {
			rg.wait.didApply(e.RaftKey, e.Index, e.Logs)
		}
		if e.storageOperation != nil {
			rg.disposeStorageOperation(e.storageOperation)
		}
		if err != nil {
			rg.Error("step failed", zap.Error(err), zap.String("handleKey", raft.Key()))
		}
		if e.WaitC != nil {
			e.WaitC <- err
		}
	}
	return hasEvent
}

func (rg *RaftGroup) handleReadys() bool {
	rg.raftList.readHandlers(&rg.tmpRafts)
	hasEvent := false
	for _, r := range rg.tmpRafts {
		if r.HasReady() {
			has := rg.handleReady(r)
			if has {
				hasEvent = true
			}
		}
	}
	rg.tmpRafts = rg.tmpRafts[:0]
	return hasEvent
}

func (rg *RaftGroup) ticks() {
	rg.ownerMu.Lock()
	defer rg.ownerMu.Unlock()
	rg.raftList.readHandlers(&rg.tmpRafts)
	for _, r := range rg.tmpRafts {
		r.Tick()
	}
	rg.tmpRafts = rg.tmpRafts[:0]
}

func (rg *RaftGroup) handleReady(r IRaft) bool {
	events := r.Ready()
	if len(events) == 0 {
		return false
	}
	for _, e := range events {
		switch e.Type {
		case types.StoreReq: // 处理存储请求
			r.KeepAlive()
			rg.handleStoreReq(r, e)
			continue
		case types.GetLogsReq: // 处理获取日志请求
			r.KeepAlive()
			rg.handleGetLogsReq(r, e)
			continue
		case types.TruncateReq: // 处理截断请求
			r.KeepAlive()
			rg.handleTruncateReq(r, e)
		case types.ApplyReq: // 处理应用请求
			r.KeepAlive()
			rg.handleApplyReq(r, e)
			continue
		case types.Destory: // 处理销毁请求
			rg.handleDestory(r)
			continue
		case types.ProposalFence: // 领导任期结束，旧回执不能再按本地日志身份完成
			rg.wait.fence(r.Key(), e.Term)
			continue

			// 角色转换
		case types.LearnerToFollowerReq,
			types.LearnerToLeaderReq,
			types.FollowerToLeaderReq:
			r.KeepAlive()
			rg.handleRoleChangeReq(r, e)
			continue
		}
		if e.To == 0 {
			rg.Foucs("none node event", zap.Any("event", e))
			continue
		}
		if e.To == types.LocalNode {
			err := r.Step(e)
			if err != nil {
				rg.Error("step error", zap.Error(err))
			}
			continue
		}
		rg.opts.Transport.Send(r.Key(), e)
	}
	return true
}
