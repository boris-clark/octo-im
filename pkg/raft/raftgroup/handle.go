package raftgroup

import (
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

// 处理存储请求
func (rg *RaftGroup) handleStoreReq(r IRaft, e types.Event) {

	op := rg.beginStorageOperation(r, e)
	if op == nil {
		return
	}
	e = cloneStorageRequest(op.request)
	err := rg.submitStorageOperation(op, func() {
		err := op.storage.AppendLogs(op.key, e.Logs, e.TermStartIndexInfo)
		reason := types.ReasonOk
		if err != nil {
			if !isDivergence(err) {
				rg.Panic("append logs failed", zap.Error(err))
			}
			reason = types.ReasonError
			if isDivergence(err) {
				reason = rg.markDiverged(op.storage, op.key, err)
			}
		}

		rg.completeStorageOperation(op, localStorageResult(types.Event{
			Type:   types.StoreResp,
			Index:  e.Logs[len(e.Logs)-1].Index,
			Logs:   e.Logs,
			Reason: reason,
		}))
		rg.Advance()
	})
	if err != nil {
		rg.Error("submit append logs failed", zap.Error(err))
		rg.completeStorageOperation(op, types.Event{
			Type:   types.StoreResp,
			Reason: types.ReasonError,
		})
		rg.Advance()
	}
}

// DivergenceMarker is implemented by storage that can durably mark a replica
// as diverged. MarkDiverged must fence the replica in memory before it
// writes, and return an error only when the marker was not persisted.
type DivergenceMarker interface {
	MarkDiverged(key string, cause error) error
}

func isDivergence(err error) bool {
	return errors.Is(err, types.ErrTruncateBelowApplied) || errors.Is(err, types.ErrAppendBelowApplied)
}

// markDiverged persists the divergence marker for a typed storage refusal
// and returns the local reason for the node: ReasonDiverged when the marker
// is durable, ReasonDivergedFailStop when it is not (or cannot be) written.
func (rg *RaftGroup) markDiverged(storage IStorage, key string, cause error) types.Reason {
	rg.Error("storage refused a request below the applied index; quarantining replica", zap.String("key", key), zap.Error(cause))
	marker, ok := storage.(DivergenceMarker)
	if !ok {
		rg.Error("storage cannot persist a divergence marker; replica fail-stopped", zap.String("key", key))
		return types.ReasonDivergedFailStop
	}
	if err := marker.MarkDiverged(key, cause); err != nil {
		rg.Error("divergence marker not persisted; replica fail-stopped", zap.String("key", key), zap.Error(err))
		return types.ReasonDivergedFailStop
	}
	return types.ReasonDiverged
}

// localStorageResult attaches the in-process storage verdict for a divergence
// reason. The node acts only on the verdict, which is never encoded, so the
// same Type and Reason arriving from a peer cannot quarantine a replica.
func localStorageResult(e types.Event) types.Event {
	switch e.Reason {
	case types.ReasonDiverged:
		return e.WithLocalStorageVerdict(types.StorageVerdictDiverged)
	case types.ReasonDivergedFailStop:
		return e.WithLocalStorageVerdict(types.StorageVerdictFailStop)
	}
	return e
}

func (rg *RaftGroup) handleGetLogsReq(r IRaft, e types.Event) {
	// Capture mutable Raft state on its owner before launching storage work.
	lastIndex := r.LastLogIndex()
	op := rg.beginStorageOperation(r, e)
	if op == nil {
		return
	}
	e = cloneStorageRequest(op.request)
	err := rg.submitStorageOperation(op, func() {
		// 获取裁断日志下标
		var (
			trunctIndex uint64
		)
		if e.Reason != types.ReasonOnlySync {
			var treason types.Reason
			trunctIndex, treason = rg.getTrunctLogIndex(op.storage, op.key, lastIndex, e)
			if treason != types.ReasonOk {
				rg.completeStorageOperation(op, types.Event{
					To:     e.From,
					Type:   types.GetLogsResp,
					Reason: treason,
				})
				return
			}
		}
		// 需要裁剪
		if trunctIndex > 0 {
			rg.completeStorageOperation(op, types.Event{
				To:     e.From,
				Type:   types.GetLogsResp,
				Index:  trunctIndex,
				Reason: types.ReasonTruncate,
			})
			rg.Advance()
			return
		}
		if e.Reason != types.ReasonOnlySync && e.Index > e.StoredIndex {
			rg.Debug("index is greater than stored index", zap.String("key", op.key), zap.Uint64("index", e.Index), zap.Uint64("storedIndex", e.StoredIndex))
			rg.completeStorageOperation(op, types.Event{
				To:     e.From,
				Type:   types.GetLogsResp,
				Index:  e.Index,
				Reason: types.ReasonOk,
			})
			rg.Advance()
			return
		}
		// 获取日志数据
		logs, err := op.storage.GetLogs(op.key, e.Index, e.StoredIndex+1, rg.opts.MaxLogSizePerBatch)
		if err != nil {
			rg.Error("get logs failed", zap.Error(err))
			rg.completeStorageOperation(op, types.Event{
				To:     e.From,
				Type:   types.GetLogsResp,
				Index:  e.Index,
				Reason: types.ReasonError,
			})
			return
		}
		if len(logs) == 0 {
			rg.Warn("logs is empty", zap.String("key", op.key), zap.Uint64("index", e.Index), zap.Uint64("storedIndex", e.StoredIndex))
		}
		rg.completeStorageOperation(op, types.Event{
			To:     e.From,
			Type:   types.GetLogsResp,
			Index:  e.Index,
			Logs:   logs,
			Reason: types.ReasonOk,
		})
		rg.Advance()
	})
	if err != nil {
		rg.Error("submit get logs failed", zap.Error(err))
		rg.completeStorageOperation(op, types.Event{
			Type:   types.GetLogsResp,
			Reason: types.ReasonError,
		})
	}
}

func (rg *RaftGroup) handleTruncateReq(r IRaft, e types.Event) {
	op := rg.beginStorageOperation(r, e)
	if op == nil {
		return
	}
	e = cloneStorageRequest(op.request)
	err := rg.submitStorageOperation(op, func() {
		err := op.storage.TruncateLogTo(op.key, e.Index)
		if err != nil {
			reason := types.ReasonError
			if isDivergence(err) {
				// The refusal mutated nothing. Newer leader term start
				// indexes are kept: deleting them would be the second half
				// of a truncation that did not happen.
				reason = rg.markDiverged(op.storage, op.key, err)
			} else {
				rg.Error("truncate logs failed", zap.Error(err))
			}
			rg.completeStorageOperation(op, localStorageResult(types.Event{
				Type:   types.TruncateResp,
				Index:  e.Index,
				Reason: reason,
			}))
			rg.Advance()
			return
		}
		// 删除本地的leader term start index
		err = op.storage.DeleteLeaderTermStartIndexGreaterThanTerm(op.key, e.LastLogTerm)
		if err != nil {
			rg.Error("delete leader term start index failed", zap.Error(err), zap.Uint32("LastLogTerm", e.LastLogTerm))
			rg.completeStorageOperation(op, types.Event{
				Type:   types.TruncateResp,
				Reason: types.ReasonError,
			})
			return
		}
		rg.completeStorageOperation(op, types.Event{
			Type:   types.TruncateResp,
			Index:  e.Index,
			Reason: types.ReasonOk,
		})
	})
	if err != nil {
		rg.Error("submit truncate req failed", zap.Error(err))
		rg.completeStorageOperation(op, types.Event{
			Type:   types.TruncateResp,
			Reason: types.ReasonError,
		})
	}
}

func (rg *RaftGroup) handleApplyReq(r IRaft, e types.Event) {
	op := rg.beginStorageOperation(r, e)
	if op == nil {
		return
	}
	e = cloneStorageRequest(op.request)
	err := rg.submitStorageOperation(op, func() {
		// 已提交
		// rg.wait.didCommit(op.key, e.EndIndex-1)
		var (
			lastLogIndex uint64
			logs         []types.Log
			err          error
		)
		if storage, ok := op.storage.(CommittedRangeApplier); ok && !rg.opts.NotNeedApplied {
			if err := storage.ApplyCommittedRange(op.key, e.StartIndex, e.EndIndex); err != nil {
				rg.Error("apply committed range failed", zap.Error(err))
				rg.completeStorageOperation(op, types.Event{Type: types.ApplyResp, Reason: types.ReasonError})
				return
			}
			lastLogIndex = e.EndIndex - 1
		} else if !rg.opts.NotNeedApplied {
			logs, err = op.storage.GetLogs(op.key, e.StartIndex, min(e.EndIndex, e.StartIndex+1000), rg.opts.MaxLogSizePerBatch)
			if err != nil {
				rg.Error("get logs failed", zap.Error(err))
				rg.completeStorageOperation(op, types.Event{
					Type:   types.ApplyResp,
					Reason: types.ReasonError,
				})
				return
			}
			if len(logs) == 0 {
				rg.Error("logs is empty", zap.String("key", op.key), zap.Uint64("startIndex", e.StartIndex), zap.Uint64("endIndex", e.EndIndex))
				rg.completeStorageOperation(op, types.Event{
					Type:   types.ApplyResp,
					Reason: types.ReasonError,
				})
				return
			}
			err = op.storage.Apply(op.key, logs)
			if err != nil {
				rg.Error("apply logs failed", zap.Error(err))
				rg.completeStorageOperation(op, types.Event{
					Type:   types.ApplyResp,
					Reason: types.ReasonError,
				})
				return
			}
			lastLogIndex = logs[len(logs)-1].Index
		} else {
			lastLogIndex = e.EndIndex - 1
		}

		rg.completeStorageOperation(op, types.Event{
			Type:   types.ApplyResp,
			Reason: types.ReasonOk,
			Index:  lastLogIndex,
			Logs:   logs,
		})
		// 已应用：按回执身份完成等待者（NotNeedApplied 时 logs 为 nil）
		rg.Advance()
	})
	if err != nil {
		rg.Error("submit apply req failed", zap.Error(err))
		rg.completeStorageOperation(op, types.Event{
			Type:   types.ApplyResp,
			Reason: types.ReasonError,
		})
	}
}

// 根据副本的同步数据，来获取副本的需要裁剪的日志下标，如果不需要裁剪，则返回0
func (rg *RaftGroup) getTrunctLogIndex(storage IStorage, key string, lastIndex uint64, e types.Event) (uint64, types.Reason) {
	leaderLastLogTerm, err := storage.LeaderLastLogTerm(key)
	if err != nil {
		rg.Error("get leader last log term failed", zap.Error(err))
		return 0, types.ReasonError
	}
	// 副本的最新日志任期为0，说明副本没有日志，不需要裁剪
	if e.LastLogTerm == 0 {
		return 0, types.ReasonOk
	}
	// 如果副本的最新日志任期等于当前领导的最新日志任期
	//  如果副本的最新日志下标大于领导者最新日志下标，则需要裁剪至领导者最新日志下标
	if e.LastLogTerm == leaderLastLogTerm {
		if e.Index > 0 {
			replicaLastLogIndex := e.Index - 1 // 副本日志下标等于同步下标-1
			if replicaLastLogIndex > lastIndex {
				return lastIndex, types.ReasonOk
			}
		}
		return 0, types.ReasonOk
	}

	// 如果副本的最新日志任期小于当前领导的最新日志任期，则需要裁剪
	if e.LastLogTerm < leaderLastLogTerm {
		term, err := storage.LeaderTermGreaterEqThan(key, e.LastLogTerm+1)
		if err != nil {
			rg.Error("LeaderTermGreaterEqThan: get leader last term failed", zap.Error(err))
			return 0, types.ReasonError
		}
		// 获取副本的最新日志任期+1的开始日志下标
		termStartIndex, err := storage.GetTermStartIndex(key, term)
		if err != nil {
			rg.Error("get term start index failed", zap.Error(err))
			return 0, types.ReasonError
		}
		if termStartIndex > 0 {
			return termStartIndex - 1, types.ReasonOk
		}
		return termStartIndex, types.ReasonOk
	} else {
		termStartIndex, err := storage.GetTermStartIndex(key, leaderLastLogTerm)
		if err != nil {
			rg.Error("get term start index failed", zap.Error(err))
			return 0, types.ReasonError
		}
		rg.Foucs("getTrunctLogIndex: lastLogTerm > leaderLastLogTerm", zap.Uint32("e.LastLogTerm", e.LastLogTerm), zap.Uint32("leaderLastLogTerm", leaderLastLogTerm), zap.Uint64("termStartIndex", termStartIndex))
		if termStartIndex > 0 {
			return termStartIndex - 1, types.ReasonOk
		}
		return termStartIndex, types.ReasonOk
	}
}

func (rg *RaftGroup) handleRoleChangeReq(r IRaft, e types.Event) {

	cfg := r.Config().Clone()
	leaderID := r.LeaderId()
	op := rg.beginStorageOperation(r, e)
	if op == nil {
		return
	}
	e = cloneStorageRequest(op.request)
	err := rg.submitStorageOperation(op, func() {

		var (
			newCfg        types.Config
			err           error
			respEventType types.EventType
		)
		switch e.Type {
		case types.LearnerToLeaderReq,
			types.LearnerToFollowerReq:
			newCfg, err = rg.learnTo(cfg, leaderID, e.From)
			if err != nil {
				rg.Error("learn switch failed", zap.Error(err), zap.String("key", op.key), zap.String("type", e.Type.String()), zap.String("cfg", cfg.String()))
			}
			if e.Type == types.LearnerToFollowerReq {
				respEventType = types.LearnerToFollowerResp
			} else if e.Type == types.LearnerToLeaderReq {
				respEventType = types.LearnerToLeaderResp
			}
		case types.FollowerToLeaderReq:
			newCfg, err = rg.followerToLeader(cfg, e.From)
			if err != nil {
				rg.Error("follower switch to leader failed", zap.Error(err), zap.String("key", op.key), zap.String("cfg", cfg.String()))
			}
			respEventType = types.FollowerToLeaderResp
		default:
			err = errors.New("unknown role switch")
		}

		if err != nil {
			rg.completeStorageOperation(op, types.Event{
				Type:   respEventType,
				From:   e.From,
				Reason: types.ReasonError,
			})
			return
		}

		err = op.storage.SaveConfig(op.key, newCfg)
		if err != nil {
			if err != types.ErrNotLeader {
				rg.Error("change role failed", zap.Error(err), zap.String("key", op.key), zap.String("cfg", newCfg.String()))
			}
			rg.completeStorageOperation(op, types.Event{
				Type:   respEventType,
				From:   e.From,
				Reason: types.ReasonError,
			})
			return
		}
		rg.completeStorageOperation(op, types.Event{
			Type:   respEventType,
			From:   e.From,
			Reason: types.ReasonOk,
			Config: newCfg,
		})
		rg.Advance()
	})
	if err != nil {
		rg.Error("submit role change req failed", zap.Error(err))

		var respEventType types.EventType

		switch e.Type {
		case types.LearnerToLeaderReq:
			respEventType = types.LearnerToLeaderResp
		case types.LearnerToFollowerReq:
			respEventType = types.LearnerToFollowerResp
		case types.FollowerToLeaderReq:
			respEventType = types.FollowerToLeaderResp
		}

		rg.completeStorageOperation(op, types.Event{
			Type:   respEventType,
			From:   e.From,
			Reason: types.ReasonError,
		})
	}
}

// 学习者转换
func (rg *RaftGroup) learnTo(cfg types.Config, leaderID uint64, learnerId uint64) (types.Config, error) {
	if !wkutil.ArrayContainsUint64(cfg.Learners, learnerId) {
		return types.Config{}, errors.New("promotion target is not a learner")
	}
	cfg = cfg.Clone()

	// orphan learner: 前次迁移已完成（MigrateFrom/MigrateTo 都为 0），
	// 但 learnerId 仍残留在 cfg.Learners 中。此时既无迁移源也无迁移目标，
	// 不能走常规迁移路径，否则会被 learnerId != cfg.MigrateTo 误拒绝。
	orphan := cfg.MigrateTo == 0 && cfg.MigrateFrom == 0 &&
		wkutil.ArrayContainsUint64(cfg.Learners, learnerId)

	if !orphan && learnerId != cfg.MigrateTo {
		rg.Warn("learnerId not equal migrateTo", zap.Uint64("learnerId", learnerId), zap.Uint64("migrateTo", cfg.MigrateTo))
		return types.Config{}, errors.New("learnerId not equal migrateTo")
	}

	cfg.Learners = wkutil.RemoveUint64(cfg.Learners, learnerId)

	if orphan {
		// orphan: 把 learnerId 直接加入 Replicas，不切换 leader，不移除旧节点。
		if !wkutil.ArrayContainsUint64(cfg.Replicas, learnerId) {
			cfg.Replicas = append(cfg.Replicas, learnerId)
		}
	} else {
		// 正常迁移路径
		if !wkutil.ArrayContainsUint64(cfg.Replicas, cfg.MigrateTo) {
			cfg.Replicas = append(cfg.Replicas, cfg.MigrateTo)
		}

		if cfg.MigrateFrom != cfg.MigrateTo {
			cfg.Replicas = wkutil.RemoveUint64(cfg.Replicas, cfg.MigrateFrom)
		}

		if cfg.MigrateFrom == leaderID { // 学习者转领导
			cfg.Leader = cfg.MigrateTo
		}
	}

	cfg.MigrateFrom = 0
	cfg.MigrateTo = 0

	return cfg, nil
}

// follower转换成leader
func (rg *RaftGroup) followerToLeader(cfg types.Config, followerId uint64) (types.Config, error) {
	cfg = cfg.Clone()
	if !wkutil.ArrayContainsUint64(cfg.Replicas, followerId) {
		rg.Error("followerToLeader: follower not in replicas", zap.Uint64("followerId", followerId))
		return types.Config{}, fmt.Errorf("follower not in replicas")
	}

	cfg.Leader = followerId
	cfg.Term = cfg.Term + 1
	cfg.MigrateFrom = 0
	cfg.MigrateTo = 0

	return cfg, nil

}

func (rg *RaftGroup) handleDestory(r IRaft) {
	rg.removeRaft(r)
}
