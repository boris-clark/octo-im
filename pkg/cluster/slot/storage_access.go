package slot

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"sync"
)

// storageView carries one accepted operation, never a current-key lookup.
type storageView struct {
	p    *PebbleShardLogStorage
	call *raftgroup.StorageCall
}

func (p *PebbleShardLogStorage) BindStorage(call *raftgroup.StorageCall) raftgroup.IStorage {
	return &storageView{p: p, call: call}
}

func (p *PebbleShardLogStorage) ClaimStorageKey(g *raftgroup.RaftGroup, key string) error {
	p.domainMu.Lock()
	defer p.domainMu.Unlock()
	if p.domainClosed {
		return ErrStorageUnbound
	}
	if p.domainOwners == nil {
		p.domainOwners = make(map[string]*raftgroup.RaftGroup)
	}
	if owner := p.domainOwners[key]; owner != nil && owner != g && !owner.IsStorageQuiescent() {
		return raftgroup.ErrRaftBusy
	}
	p.domainOwners[key] = g
	return nil
}

func (v *storageView) withDomain(fn func() error) error {
	v.p.domainMu.Lock()
	if v.p.domainClosed || !v.p.domainOpen {
		v.p.domainMu.Unlock()
		return ErrStorageUnbound
	}
	v.p.domainOps++
	v.p.domainMu.Unlock()
	defer func() { v.p.domainMu.Lock(); v.p.domainOps--; v.p.domainMu.Unlock() }()
	return fn()
}

func (v *storageView) with(key string, fn func() error) error {
	if v == nil || v.p == nil {
		return ErrStorageUnbound
	}
	return v.call.Run(v.p, key, func() error { return v.withDomain(fn) })
}

func (v *storageView) withWrite(key string, hardState bool, fn func() error) error {
	if v == nil || v.p == nil {
		return ErrStorageUnbound
	}
	return v.call.RunWrite(v.p, key, hardState, func() error { return v.withDomain(fn) })
}

func (*storageView) AtomicLogTruncation() {}

func (v *storageView) GetState(key string) (types.RaftState, error) {
	var a0 types.RaftState
	err := v.with(key, func() error { var err error; a0, err = v.p.getState(key); return err })
	return a0, err
}

func (v *storageView) AppendLogs(key string, logs []types.Log, info *types.TermStartIndexInfo) error {
	return v.withWrite(key, false, func() error { return v.p.appendLogs(key, logs, info) })
}

func (v *storageView) TruncateLogTo(key string, index uint64) error {
	return v.withWrite(key, false, func() error { return v.p.truncateLogTo(key, index) })
}

func (v *storageView) GetLogs(key string, start, end, limit uint64) ([]types.Log, error) {
	var a0 []types.Log
	err := v.with(key, func() error { var err error; a0, err = v.p.getLogs(key, start, end, limit); return err })
	return a0, err
}

func (v *storageView) Apply(key string, logs []types.Log) error {
	return v.withWrite(key, false, func() error { return v.p.applyWithReads(key, logs, v) })
}

func (v *storageView) SaveConfig(key string, cfg types.Config) error {
	return v.withWrite(key, false, func() error { return v.p.saveConfigWithReads(key, cfg, v) })
}

func (v *storageView) GetLogsInReverseOrder(key string, start, end uint64, limit int) ([]types.Log, error) {
	var a0 []types.Log
	err := v.with(key, func() error { var err error; a0, err = v.p.getLogsInReverseOrder(key, start, end, limit); return err })
	return a0, err
}

func (v *storageView) LastIndex(key string) (uint64, error) {
	var a0 uint64
	err := v.with(key, func() error { var err error; a0, err = v.p.lastIndex(key); return err })
	return a0, err
}

func (v *storageView) LastIndexAndTerm(key string) (uint64, uint32, error) {
	var a0 uint64
	var a1 uint32
	err := v.with(key, func() error { var err error; a0, a1, err = v.p.lastIndexAndTerm(key); return err })
	return a0, a1, err
}

func (v *storageView) LastLog(key string) (types.Log, error) {
	var a0 types.Log
	err := v.with(key, func() error { var err error; a0, err = v.p.lastLog(key); return err })
	return a0, err
}

func (v *storageView) SetAppliedIndex(key string, index uint64) error {
	return v.withWrite(key, false, func() error { return v.p.setAppliedIndex(key, index) })
}

func (v *storageView) AppliedIndex(key string) (uint64, error) {
	var a0 uint64
	err := v.with(key, func() error { var err error; a0, err = v.p.appliedIndex(key); return err })
	return a0, err
}

func (v *storageView) SetLeaderTermStartIndex(key string, term uint32, index uint64) error {
	return v.withWrite(key, false, func() error { return v.p.setLeaderTermStartIndex(key, term, index) })
}

func (v *storageView) LeaderLastLogTerm(key string) (uint32, error) {
	var a0 uint32
	err := v.with(key, func() error { var err error; a0, err = v.p.leaderLastLogTerm(key); return err })
	return a0, err
}

func (v *storageView) GetTermStartIndex(key string, term uint32) (uint64, error) {
	var a0 uint64
	err := v.with(key, func() error { var err error; a0, err = v.p.getTermStartIndex(key, term); return err })
	return a0, err
}

func (v *storageView) LeaderTermGreaterEqThan(key string, term uint32) (uint32, error) {
	var a0 uint32
	err := v.with(key, func() error { var err error; a0, err = v.p.leaderTermGreaterEqThan(key, term); return err })
	return a0, err
}

func (v *storageView) DeleteLeaderTermStartIndexGreaterThanTerm(key string, term uint32) error {
	return v.withWrite(key, false, func() error { return v.p.deleteLeaderTermStartIndexGreaterThanTerm(key, term) })
}

func (v *storageView) SaveHardState(key string, state types.HardState) error {
	return v.withWrite(key, true, func() error { return v.p.saveHardState(key, state) })
}

func (v *storageView) MarkDiverged(key string, cause error) error {
	return v.withWrite(key, false, func() error { return v.p.markDiverged(key, cause) })
}

func (v *storageView) DivergedMarker(key string) (bool, error) {
	var a0 bool
	err := v.with(key, func() error { var err error; a0, err = v.p.divergedMarker(key); return err })
	return a0, err
}

// CallbackReads is a read-only scope of an already accepted storage call.
// It is revoked and joined before its callback returns to Apply or SaveConfig.
// The parent call already owns the effect gate and domain admission until that
// join. Reads use private primitives so revoking the parent cannot deadlock on a
// nested read lock, and the Apply slot lock remains held throughout.
type CallbackReads struct {
	mu     sync.RWMutex
	active bool
	key    string
	view   *storageView
}

func (r *CallbackReads) with(fn func() error) error {
	if r == nil {
		return ErrStorageUnbound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.active || r.view == nil || r.view.p == nil {
		return ErrStorageUnbound
	}
	return fn()
}
func (r *CallbackReads) revoke() { r.mu.Lock(); r.active = false; r.mu.Unlock() }

func (r *CallbackReads) GetState() (types.RaftState, error) {
	var a0 types.RaftState
	err := r.with(func() error { var err error; a0, err = r.view.p.getState(r.key); return err })
	return a0, err
}

func (r *CallbackReads) GetLogs(start, end, limit uint64) ([]types.Log, error) {
	var a0 []types.Log
	err := r.with(func() error { var err error; a0, err = r.view.p.getLogs(r.key, start, end, limit); return err })
	return a0, err
}

func (r *CallbackReads) GetLogsInReverseOrder(start, end uint64, limit int) ([]types.Log, error) {
	var a0 []types.Log
	err := r.with(func() error {
		var err error
		a0, err = r.view.p.getLogsInReverseOrder(r.key, start, end, limit)
		return err
	})
	return a0, err
}

func (r *CallbackReads) LastIndex() (uint64, error) {
	var a0 uint64
	err := r.with(func() error { var err error; a0, err = r.view.p.lastIndex(r.key); return err })
	return a0, err
}

func (r *CallbackReads) LastIndexAndTerm() (uint64, uint32, error) {
	var a0 uint64
	var a1 uint32
	err := r.with(func() error { var err error; a0, a1, err = r.view.p.lastIndexAndTerm(r.key); return err })
	return a0, a1, err
}

func (r *CallbackReads) LastLog() (types.Log, error) {
	var a0 types.Log
	err := r.with(func() error { var err error; a0, err = r.view.p.lastLog(r.key); return err })
	return a0, err
}

func (r *CallbackReads) AppliedIndex() (uint64, error) {
	var a0 uint64
	err := r.with(func() error { var err error; a0, err = r.view.p.appliedIndex(r.key); return err })
	return a0, err
}

func (r *CallbackReads) LeaderLastLogTerm() (uint32, error) {
	var a0 uint32
	err := r.with(func() error { var err error; a0, err = r.view.p.leaderLastLogTerm(r.key); return err })
	return a0, err
}

func (r *CallbackReads) GetTermStartIndex(term uint32) (uint64, error) {
	var a0 uint64
	err := r.with(func() error { var err error; a0, err = r.view.p.getTermStartIndex(r.key, term); return err })
	return a0, err
}

func (r *CallbackReads) LeaderTermGreaterEqThan(term uint32) (uint32, error) {
	var a0 uint32
	err := r.with(func() error { var err error; a0, err = r.view.p.leaderTermGreaterEqThan(r.key, term); return err })
	return a0, err
}

func (r *CallbackReads) DivergedMarker() (bool, error) {
	var a0 bool
	err := r.with(func() error { var err error; a0, err = r.view.p.divergedMarker(r.key); return err })
	return a0, err
}
