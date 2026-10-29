package cluster

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// 槽选举
func (s *Server) OnSlotElection(slots []*types.Slot) error {

	err := s.handleSlotElection(slots)
	if err != nil {
		s.Error("handleSlotElection failed", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) handleSlotElection(slots []*types.Slot) error {
	ctx, cancel := context.WithTimeout(s.cancelCtx, 4*time.Second)
	defer cancel()
	for _, st := range slots {
		if st.Status != types.SlotStatus_SlotStatusCandidate {
			continue
		}
		expected := st.Clone()
		cfg := slotElectionConfig(expected)
		reports, err := s.requestElectionFences(ctx, cfg, electionFenceRequest{Slot: expected})
		if err != nil {
			return err
		}
		leader, err := rafttypes.SelectFencedLeader(cfg, expected.Term+1, reports)
		if err != nil {
			return err
		}
		next := expected.Clone()
		next.Leader = leader
		next.Term++
		next.InstalledTerm = next.Term
		next.ExpectLeader = 0
		next.Status = types.SlotStatus_SlotStatusNormal
		for _, report := range reports {
			next.BoundaryTerm = max(next.BoundaryTerm, report.BoundaryTerm)
		}
		if err = s.cfgServer.ProposeSlotInstall(expected, next); err != nil {
			return err
		}
	}
	return nil
}

// 请求槽的日志高度
func (s *Server) requestSlotInfos(waitElectionSlots map[uint64][]uint32) ([]*SlotLogInfoResp, error) {
	return s.requestSlotInfosWith(waitElectionSlots, s.rpcClient.RequestSlotLastLogInfo)
}

func (s *Server) requestSlotInfosWith(waitElectionSlots map[uint64][]uint32, fetch func(uint64, *SlotLogInfoReq) (*SlotLogInfoResp, error)) ([]*SlotLogInfoResp, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*4)
	defer cancel()
	requestGroup, _ := errgroup.WithContext(timeoutCtx)
	// Each request writes only its own result slot; the slice is read after
	// Wait, so concurrent responses never race on a shared append.
	results := make([]*SlotLogInfoResp, len(waitElectionSlots))
	i := 0
	for nodeId, slotIds := range waitElectionSlots {
		idx := i
		i++
		if nodeId == s.opts.ConfigOptions.NodeId { // 本节点
			slotInfos, err := s.slotInfos(slotIds)
			if err != nil {
				s.Error("get slot infos error", zap.Error(err))
				continue
			}
			results[idx] = &SlotLogInfoResp{
				NodeId: nodeId,
				Slots:  slotInfos,
			}
			continue
		}
		requestGroup.Go(func(nID uint64, sids []uint32) func() error {
			return func() error {
				resp, err := fetch(nID, &SlotLogInfoReq{
					SlotIds: sids,
				})
				if err != nil {
					s.Warn("request slot log info error", zap.Error(err))
					return nil
				}
				results[idx] = resp
				return nil
			}
		}(nodeId, slotIds))
	}
	_ = requestGroup.Wait()
	slotInfoResps := make([]*SlotLogInfoResp, 0, len(results))
	for _, resp := range results {
		if resp != nil {
			slotInfoResps = append(slotInfoResps, resp)
		}
	}
	return slotInfoResps, nil

}

// slotInfos reports the local replicas' election info, including the
// divergence extension. A fail-stopped replica is omitted: its marker was not
// persisted, so it must not take part in any election.
func (s *Server) slotInfos(slotIds []uint32) ([]SlotInfo, error) {
	slotInfos := make([]SlotInfo, 0, len(slotIds))
	for _, slotId := range slotIds {
		st := s.slotServer.GetSlotRaft(slotId)
		if st == nil {
			continue
		}
		state := s.slotServer.ReplicaState(slotId)
		if state == slot.ReplicaFailStopped {
			continue
		}
		lastLog, err := s.slotServer.LastLog(slotId)
		if err != nil {
			s.Error("get slot last log error", zap.Error(err))
			continue
		}
		applied, err := s.slotServer.AppliedIndex(slotId)
		if err != nil {
			s.Error("get slot applied index error", zap.Error(err))
			continue
		}
		slotInfos = append(slotInfos, SlotInfo{
			SlotId:   slotId,
			LogIndex: lastLog.Index,
			LogTerm:  lastLog.Term,
			Term:     st.LastTerm(),
			Diverged: state != slot.ReplicaHealthy,
			Applied:  applied,
		})
	}
	return slotInfos, nil
}

// 收集slot的各个副本的日志高度
//
// Only healthy responders count towards the quorum and can be elected. A
// responder is excluded when it reports itself diverged, when its applied
// index is beyond its last log index, or, in strict mode (the default), when
// its divergence extension is missing or malformed. Exclusion comes only from
// a replica's own report.
func (s *Server) collectSlotLogInfo(slotInfoResps []*SlotLogInfoResp) (map[uint64]map[uint32]SlotInfo, error) {
	slotLogInfos := make(map[uint64]map[uint32]SlotInfo, len(slotInfoResps))
	// 合法的最小选举人数
	quorum := int(s.opts.ConfigOptions.SlotMaxReplicaCount/2) + 1

	slotQuorumMap := make(map[uint32]int)

	healthy := func(nodeId uint64, info SlotInfo) bool {
		switch info.ExtState {
		case SlotInfoExtInvalid:
			slotLogInfoExtInvalid.Add(1)
			if s.opts.SlotDivergeStrictExtension {
				s.Warn("slot election: malformed divergence extension, responder excluded", zap.Uint64("nodeId", nodeId), zap.Uint32("slotId", info.SlotId))
				return false
			}
			return true
		case SlotInfoExtMissing:
			if s.opts.SlotDivergeStrictExtension {
				s.Warn("slot election: divergence extension missing, responder excluded", zap.Uint64("nodeId", nodeId), zap.Uint32("slotId", info.SlotId))
				return false
			}
			return true
		}
		if info.Diverged {
			s.Warn("slot election: diverged replica excluded", zap.Uint64("nodeId", nodeId), zap.Uint32("slotId", info.SlotId))
			return false
		}
		if info.Applied > info.LogIndex {
			s.Warn("slot election: replica applied beyond its log excluded", zap.Uint64("nodeId", nodeId), zap.Uint32("slotId", info.SlotId), zap.Uint64("applied", info.Applied), zap.Uint64("logIndex", info.LogIndex))
			return false
		}
		return true
	}

	healthyResps := make(map[uint64][]SlotInfo, len(slotInfoResps))
	for _, resp := range slotInfoResps {
		for _, slotInfo := range resp.Slots {
			if !healthy(resp.NodeId, slotInfo) {
				continue
			}
			healthyResps[resp.NodeId] = append(healthyResps[resp.NodeId], slotInfo)
			slotQuorumMap[slotInfo.SlotId]++
		}
	}

	for _, resp := range slotInfoResps {
		slotLogInfos[resp.NodeId] = make(map[uint32]SlotInfo, len(resp.Slots))
		for _, slotInfo := range healthyResps[resp.NodeId] {
			if slotQuorumMap[slotInfo.SlotId] < quorum {
				s.Foucs("slot quorum < quorum", zap.Uint32("slotId", slotInfo.SlotId), zap.Int("quorum", quorum), zap.Int("slotQuorum", slotQuorumMap[slotInfo.SlotId]))
				continue
			}
			slotLogInfos[resp.NodeId][slotInfo.SlotId] = slotInfo
		}
	}
	return slotLogInfos, nil
}

// 计算每个槽的领导节点
func (s *Server) calculateSlotLeader(slotLogInfos map[uint64]map[uint32]SlotInfo, slotIds []uint32) map[uint32]uint64 {
	slotLeaderMap := make(map[uint32]uint64, len(slotIds))
	for _, slotId := range slotIds {
		slotLeaderMap[slotId] = s.electionSlotLeaderBySlot(slotLogInfos, slotId)
	}
	return slotLeaderMap
}

// 计算槽的领导节点
// slotLogInfos 槽在各个副本上的日志信息
// slotId 计算领导节点的槽Id
func (s *Server) electionSlotLeaderBySlot(slotLogInfos map[uint64]map[uint32]SlotInfo, slotId uint32) uint64 {
	return electSlotLeader(slotLogInfos, slotId, s.cfgServer.Slot(slotId))
}

// electSlotLeader picks the leader among the responders left after
// collectSlotLogInfo. A diverged or unknown ExpectLeader has been dropped
// there, so it is skipped here.
func electSlotLeader(slotLogInfos map[uint64]map[uint32]SlotInfo, slotId uint32, st *types.Slot) uint64 {
	if st == nil {
		return 0
	}
	voters := make(map[uint64]bool)
	for _, id := range st.Replicas {
		if id != 0 {
			voters[id] = true
		}
	}
	for _, id := range st.Learners {
		delete(voters, id)
	}
	responses := 0
	for id, infos := range slotLogInfos {
		if _, ok := infos[slotId]; ok && voters[id] {
			responses++
		}
	}
	if responses < len(voters)/2+1 {
		return 0
	}
	var expectLeader uint64 // 期望领导
	var lastLogIndex uint64 // 最新一条日志下标
	var lastLogTerm uint32  // 最新一条日志任期
	var leaderTerm uint32   // 领导任期

	// 如果槽正在进行领导者转移，则优先计算转移节点的日志信息
	if st != nil && st.ExpectLeader != 0 && st.ExpectLeader != st.Leader {
		for replicaId, logIndexMap := range slotLogInfos {
			slotInfo, ok := logIndexMap[slotId]
			if !ok {
				continue
			}
			if replicaId == st.ExpectLeader {
				expectLeader = replicaId
				lastLogIndex = slotInfo.LogIndex
				lastLogTerm = slotInfo.LogTerm
				leaderTerm = slotInfo.Term
				break
			}
		}
	}

	/**
	候选者的任期号（Term）不小于自身的当前任期号。
	候选者的日志至少和自己的日志一样新，即：
	候选者的最后日志条目的任期号较大，或者
	如果任期号相同，候选者的日志更长。
		**/
	for replicaId, logIndexMap := range slotLogInfos {
		candidate, ok := logIndexMap[slotId]
		if !ok {
			continue
		}
		if expectLeader == 0 {
			expectLeader = replicaId
			lastLogIndex = candidate.LogIndex
			lastLogTerm = candidate.LogTerm
			leaderTerm = candidate.Term
			continue
		}

		// 候选者的任期号（Term）不小于自身的当前任期号。
		if candidate.Term < leaderTerm {
			continue
		}

		// 候选者的最后日志条目的任期号较大，或者
		// 如果任期号相同，候选者的日志更长。
		if candidate.LogTerm > lastLogTerm {
			expectLeader = replicaId
			lastLogIndex = candidate.LogIndex
			lastLogTerm = candidate.LogTerm
			leaderTerm = candidate.Term
		} else if candidate.LogTerm == lastLogTerm {
			if candidate.LogIndex > lastLogIndex {
				expectLeader = replicaId
				lastLogIndex = candidate.LogIndex
				lastLogTerm = candidate.LogTerm
				leaderTerm = candidate.Term
			}
		}

	}
	return expectLeader
}
