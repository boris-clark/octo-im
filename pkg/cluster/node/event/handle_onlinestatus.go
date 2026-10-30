package event

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

func (h *handler) handleOnlineStatus(nodeId uint64, online bool) {
	// 提案节点状态
	err := h.cfgServer.ProposeNodeOnlineStatus(nodeId, online)
	if err != nil {
		h.Error("propose node offline", zap.Error(err))
		return
	}

	// 处理槽迁移
	err = h.handleSlotChangeByNodeOnlineStatus(nodeId, online)
	if err != nil {
		h.Error("handleSlotChangeByNodeOnlineStatus failed", zap.Error(err))
		return
	}
}

func (h *handler) handleSlotChangeByNodeOnlineStatus(nodeId uint64, online bool) error {
	var newSlots []*types.Slot
	if online { // 节点上线

		h.Info("节点上线", zap.Uint64("nodeId", nodeId))
		slots := h.cfgServer.Slots()

		onlineNodeCount := h.cfgServer.AllowVoteAndJoinedOnlineNodeCount()

		avgSlotLeaderCount := h.cfgServer.GetClusterConfig().SlotCount / uint32(onlineNodeCount) // 平均每个节点的槽领导数量

		// 每个节点当前的槽领导数量
		nodeSlotLeaderCountMap := make(map[uint64]uint32)
		for _, slot := range slots {
			nodeSlotLeaderCountMap[slot.Leader]++
		}

		currentNodeSlotLeaderCount := nodeSlotLeaderCountMap[nodeId] // 当前节点的槽领导数量

		if currentNodeSlotLeaderCount >= avgSlotLeaderCount { // 当前节点的槽领导数量已经达到平均值，不需要迁入
			return nil
		}

		// 计算需要迁入的槽领导数量
		needSlotLeaderCount := avgSlotLeaderCount - currentNodeSlotLeaderCount

		for nId, slotLeaderCount := range nodeSlotLeaderCountMap {
			if slotLeaderCount < avgSlotLeaderCount { // 槽领导数量小于平均值的节点不参与计算
				continue
			}

			if needSlotLeaderCount <= 0 {
				break
			}

			if nId == nodeId {
				continue
			}

			exportSlotLeaderCount := slotLeaderCount - avgSlotLeaderCount // 需要迁出的槽领导数量

			for _, slot := range slots {
				if slot.Leader == nId &&
					exportSlotLeaderCount > 0 &&
					needSlotLeaderCount > 0 &&
					slot.MigrateFrom == 0 &&
					slot.MigrateTo == 0 &&
					wkutil.ArrayContainsUint64(slot.Replicas, nodeId) {

					newSlot := slot.Clone()
					newSlot.MigrateFrom = nId
					newSlot.MigrateTo = nodeId
					newSlots = append(newSlots, newSlot)
					exportSlotLeaderCount--
					needSlotLeaderCount--
				}
			}

		}

	} else { // A liveness decision requests election; it never chooses a voter.
		h.Info("node offline", zap.Uint64("nodeId", nodeId))
		newSlots = offlineSlotCandidates(h.cfgServer.Slots(), nodeId)

	}
	if len(newSlots) > 0 {
		err := h.cfgServer.ProposeSlots(newSlots)
		if err != nil {
			h.Error("handleNodeOnlineChange failed,ProposeSlots failed", zap.Error(err))
			return err
		}
	}
	return nil
}

// offlineSlotCandidates marks every affected slot, including when surviving
// nodes currently lead zero slots. Quorum and history are decided by election.
func offlineSlotCandidates(slots []*types.Slot, nodeId uint64) []*types.Slot {
	var candidates []*types.Slot
	for _, st := range slots {
		if st.Leader != nodeId || st.Status == types.SlotStatus_SlotStatusCandidate {
			continue
		}
		if st.InstalledTerm == 0 && (st.MigrateFrom != 0 || st.MigrateTo != 0) {
			continue
		}
		next := st.Clone()
		next.Status = types.SlotStatus_SlotStatusCandidate
		next.ExpectLeader = 0
		candidates = append(candidates, next)
	}
	return candidates
}
