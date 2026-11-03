package clusterconfig

import (
	"encoding/binary"
	"errors"
	"slices"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"go.uber.org/zap"
)

var (
	ErrSlotMigrateInvalidEndpoint    = errors.New("slot migrate endpoints are invalid")
	ErrSlotMigrateSlotNotFound       = errors.New("slot not found")
	ErrSlotMigrateInProgress         = errors.New("slot migration is already in progress")
	ErrSlotMigrateSourceNotReplica   = errors.New("slot migrate source is not a replica")
	ErrSlotMigrateFollowerToFollower = errors.New("slot transition between followers is not supported")
	ErrSlotMigrateTargetOffline      = errors.New("slot migrate target is not an online voting node")
	ErrSlotMigrateNotInstalled       = errors.New("installed slot migration requires certified slot authority")
	ErrSlotMigrateRejected           = errors.New("slot migration was rejected by applied state")
)

// 提案配置
func (s *Server) ProposeConfig(cfg *pb.Config) error {
	data, err := cfg.Marshal()
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeConfigChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}
	_, err = s.ProposeUntilApplied(s.genConfigId(), cmdBytes)
	return err
}

// ProposeApiServerAddr 提案节点api server地址变更
func (s *Server) ProposeApiServerAddr(nodeId uint64, apiServerAddr string) error {

	data, err := EncodeApiServerAddrChange(nodeId, apiServerAddr)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeConfigApiServerAddrChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	_, err = s.ProposeUntilApplied(s.genConfigId(), cmdBytes)
	if err != nil {
		s.Error("ProposeApiServerAddr failed", zap.Error(err))
		return err
	}
	return nil
}

// ProposeLeave 提案节点在线状态变更
func (s *Server) ProposeNodeOnlineStatus(nodeId uint64, online bool) error {
	data, err := EncodeNodeOnlineStatusChange(nodeId, online)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeNodeOnlineStatusChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}
	_, err = s.ProposeUntilApplied(s.genConfigId(), cmdBytes)
	if err != nil {
		s.Error("ProposeNodeOnlineStatus failed", zap.Error(err))
		return err
	}

	return nil
}

// ProposeSlots 提案槽变更
func (s *Server) ProposeSlots(slots []*pb.Slot) error {
	slotSet := pb.SlotSet(slots)
	data, err := slotSet.Marshal()
	if err != nil {
		return err
	}
	cmd := NewCMD(CMDTypeSlotUpdate, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()

	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeSlots failed", zap.Error(err))
		return err
	}
	return nil
}

// ProposeJoined 提案节点加入
func (s *Server) ProposeJoined(nodeId uint64, slots []*pb.Slot) error {

	data, err := EncodeNodeJoined(nodeId, slots)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeNodeJoined, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()
	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeJoined failed", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) ProposeJoining(nodeId uint64) error {

	nodeIdBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nodeIdBytes, nodeId)

	cmd := NewCMD(CMDTypeNodeJoining, nodeIdBytes)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		s.Error("ProposeJoining cmd marshal failed", zap.Error(err))
		return err
	}

	logId := s.genConfigId()
	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeJoining failed", zap.Error(err))
		return err
	}
	return nil

}

// ProposeJoin 提案节点加入
func (s *Server) ProposeJoin(node *pb.Node) error {

	data, err := node.Marshal()
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeNodeJoin, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()
	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeJoin failed", zap.Error(err))
		return err
	}
	return nil
}

// ProposeMigrateSlot 提案槽迁移
func (s *Server) ProposeMigrateSlot(slotId uint32, fromNodeId, toNodeId uint64) error {
	if fromNodeId == 0 || toNodeId == 0 || fromNodeId == toNodeId {
		return ErrSlotMigrateInvalidEndpoint
	}
	slot := s.Slot(slotId)
	if slot == nil {
		return ErrSlotMigrateSlotNotFound
	}
	if slot.MigrateFrom != 0 || slot.MigrateTo != 0 || len(slot.Learners) > 0 {
		return ErrSlotMigrateInProgress
	}
	if !slices.Contains(slot.Replicas, fromNodeId) {
		return ErrSlotMigrateSourceNotReplica
	}
	if slices.Contains(slot.Replicas, toNodeId) {
		if fromNodeId != slot.Leader {
			return ErrSlotMigrateFollowerToFollower
		}
	} else {
		onlineTarget := slices.ContainsFunc(s.AllowVoteAndJoinedOnlineNodes(), func(n *pb.Node) bool {
			return n != nil && n.Id == toNodeId
		})
		if !onlineTarget {
			return ErrSlotMigrateTargetOffline
		}
	}
	if slot.InstalledTerm != 0 {
		if s.onProposeMigrateSlot == nil {
			return ErrSlotMigrateNotInstalled
		}
		return s.onProposeMigrateSlot(slotId, fromNodeId, toNodeId)
	}
	data, err := EncodeMigrateSlot(slotId, fromNodeId, toNodeId)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeSlotMigrate, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()
	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeMigrateSlot failed", zap.Error(err))
		return err
	}
	updated := s.Slot(slotId)
	if updated == nil || (updated.MigrateFrom != fromNodeId || updated.MigrateTo != toNodeId) && updated.Leader != toNodeId && !slices.Contains(updated.Replicas, toNodeId) {
		return ErrSlotMigrateRejected
	}

	return nil
}

// ProposeSlotStatusChange 提案槽状态变更
func (s *Server) ProposeSlotStatusChange(slotId uint32, status pb.SlotStatus) error {
	data, err := EncodeSlotStatusChange(slotId, status)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeSlotStatusChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()
	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeSlotStatusChange failed", zap.Error(err))
		return err
	}
	return nil
}
