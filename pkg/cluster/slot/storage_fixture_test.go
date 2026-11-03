package slot

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

// TestFixtureView provides explicit test-fixture access to an unattached
// PebbleShardLogStorage before any RaftGroup attaches to the storage domain.
// Once a RaftGroup attaches to the domain, all TestFixtureView methods are
// irrevocably disabled and return ErrStorageUnbound.
type TestFixtureView struct {
	p *PebbleShardLogStorage
}

func (p *PebbleShardLogStorage) ForTestFixture() *TestFixtureView {
	return &TestFixtureView{p: p}
}

func (v *TestFixtureView) withOp(fn func() error) error {
	if v == nil || v.p == nil {
		return ErrStorageUnbound
	}
	v.p.domainMu.Lock()
	if v.p.domainClosed || !v.p.domainOpen || v.p.fixtureDisabled || len(v.p.attachedGroups) != 0 || len(v.p.dbs) == 0 {
		v.p.domainMu.Unlock()
		return ErrStorageUnbound
	}
	v.p.domainOps++
	v.p.domainMu.Unlock()
	defer func() {
		v.p.domainMu.Lock()
		v.p.domainOps--
		v.p.domainMu.Unlock()
	}()
	return fn()
}

func (v *TestFixtureView) GetState(shardNo string) (types.RaftState, error) {
	var st types.RaftState
	err := v.withOp(func() error {
		var err error
		st, err = v.p.getState(shardNo)
		return err
	})
	return st, err
}

func (v *TestFixtureView) AppendLogs(shardNo string, logs []types.Log, info *types.TermStartIndexInfo) error {
	return v.withOp(func() error { return v.p.appendLogs(shardNo, logs, info) })
}

func (v *TestFixtureView) TruncateLogTo(shardNo string, index uint64) error {
	return v.withOp(func() error { return v.p.truncateLogTo(shardNo, index) })
}

func (v *TestFixtureView) GetLogs(shardNo string, start, end, limit uint64) ([]types.Log, error) {
	var logs []types.Log
	err := v.withOp(func() error {
		var err error
		logs, err = v.p.getLogs(shardNo, start, end, limit)
		return err
	})
	return logs, err
}

func (v *TestFixtureView) SetAppliedIndex(shardNo string, index uint64) error {
	return v.withOp(func() error { return v.p.setAppliedIndex(shardNo, index) })
}

func (v *TestFixtureView) SaveHardState(shardNo string, state types.HardState) error {
	return v.withOp(func() error { return v.p.saveHardState(shardNo, state) })
}

func (v *TestFixtureView) MarkDiverged(shardNo string, cause error) error {
	return v.withOp(func() error { return v.p.markDiverged(shardNo, cause) })
}
