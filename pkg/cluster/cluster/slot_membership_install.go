package cluster

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func (s *Server) notifySlotMigration() {
	if s == nil || s.slotMigrateNotify == nil {
		return
	}
	select {
	case s.slotMigrateNotify <- struct{}{}:
	default:
	}
}

func (s *Server) certifySlotMembership(ctx context.Context, old, next *pb.Slot) (*rt.MembershipCertificate, error) {
	if old == nil || next == nil || old.Id != next.Id || old.InstalledTerm == 0 || old.Term == math.MaxUint32 {
		return nil, raft.ErrElectionNotInstalled
	}
	maxReplicas := int(s.opts.ConfigOptions.SlotMaxReplicaCount)
	if maxReplicas > 0 && len(next.Replicas) > max(len(old.Replicas), maxReplicas) {
		return nil, raft.ErrElectionNotInstalled
	}
	allowed := make(map[uint64]bool)
	for _, node := range s.cfgServer.AllowVoteAndJoinedOnlineNodes() {
		if node != nil {
			allowed[node.Id] = true
		}
	}
	for _, id := range next.Learners {
		if !slices.Contains(old.Learners, id) && !allowed[id] {
			return nil, clusterconfig.ErrSlotMigrateTargetOffline
		}
	}
	for _, id := range next.Replicas {
		if !slices.Contains(old.Replicas, id) && !allowed[id] {
			return nil, clusterconfig.ErrSlotMigrateTargetOffline
		}
	}
	if !rt.ValidMembershipTransition(slotElectionConfig(old), slotElectionConfig(next)) {
		return nil, raft.ErrElectionNotInstalled
	}
	applied := s.cfgServer.AppliedSlot(old.Id)
	if applied == nil || !proto.Equal(applied, old) {
		return nil, raft.ErrConfigVersionStale
	}
	term := old.Term + 1
	reports, err := s.requestElectionFences(ctx, slotElectionConfig(old), electionFenceRequest{
		Slot:            old,
		Term:            term,
		IncludeLearners: true,
	})
	if err != nil {
		return nil, err
	}
	next.Term = term
	next.InstalledTerm = term
	next.ExpectLeader = 0
	next.Status = pb.SlotStatus_SlotStatusNormal
	next.BoundaryTerm = old.BoundaryTerm
	for _, report := range reports {
		if report.Fenced && report.Term == term {
			next.BoundaryTerm = max(next.BoundaryTerm, report.BoundaryTerm)
		}
	}
	cert := &rt.MembershipCertificate{Reports: reports}
	if err := rt.ValidateMembershipInstall(slotElectionConfig(old), slotElectionConfig(next), cert); err != nil {
		return nil, err
	}
	selected, err := rt.SelectFencedLeader(slotElectionConfig(old), term, reports)
	if err != nil {
		return nil, err
	}
	tail := reports[selected]
	for _, id := range next.Replicas {
		if !slices.Contains(old.Replicas, id) {
			r, ok := reports[id]
			if !ok || !r.Fenced || r.Term != term || r.LastIndex != tail.LastIndex || r.LastTerm != tail.LastTerm {
				return nil, errors.New("promoted slot learner has not stored selected history")
			}
		}
	}
	return cert, nil
}

func (s *Server) installCertifiedSlotMembership(ctx context.Context, old, next *pb.Slot) error {
	candidate := next.Clone()
	cert, err := s.certifySlotMembership(ctx, old, candidate)
	if err != nil {
		return err
	}
	return s.cfgServer.ProposeSlotInstallWithMembership(old, candidate, cert)
}

func (s *Server) slotNodeLogInfo(nodeId uint64, slotId uint32) (SlotInfo, bool) {
	if nodeId == s.opts.ConfigOptions.NodeId {
		infos, err := s.slotInfos([]uint32{slotId})
		if err != nil || len(infos) == 0 || infos[0].Diverged {
			return SlotInfo{}, false
		}
		return infos[0], true
	}
	resp, err := s.rpcClient.RequestSlotLastLogInfo(nodeId, &SlotLogInfoReq{SlotIds: []uint32{slotId}})
	if err != nil || resp == nil || len(resp.Slots) == 0 || resp.Slots[0].Diverged {
		return SlotInfo{}, false
	}
	return resp.Slots[0], true
}

func (s *Server) ensureSlotCurrentTermApplied(ctx context.Context, st *pb.Slot) (SlotInfo, bool) {
	if st == nil || st.Leader == 0 {
		return SlotInfo{}, false
	}
	leaderInfo, ok := s.slotNodeLogInfo(st.Leader, st.Id)
	if !ok {
		return SlotInfo{}, false
	}
	if leaderInfo.LogIndex > 0 && (leaderInfo.LogTerm < st.Term || leaderInfo.Applied < leaderInfo.LogIndex) {
		proposeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, _ = s.slotServer.ProposeUntilAppliedTimeout(proposeCtx, st.Id, rt.NoopData())
		cancel()
		leaderInfo, ok = s.slotNodeLogInfo(st.Leader, st.Id)
		if !ok {
			return SlotInfo{}, false
		}
	}
	if leaderInfo.Applied < leaderInfo.LogIndex || (leaderInfo.LogIndex > 0 && leaderInfo.LogTerm < st.Term) {
		return leaderInfo, false
	}
	return leaderInfo, true
}

func (s *Server) waitSlotTargetCaughtUp(ctx context.Context, slotId uint32, fromNodeId, toNodeId uint64) (*pb.Slot, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		current := s.cfgServer.AppliedSlot(slotId)
		if current == nil {
			return nil, clusterconfig.ErrSlotMigrateSlotNotFound
		}
		if current.Leader == toNodeId && current.MigrateFrom == 0 && current.MigrateTo == 0 {
			return current, nil
		}
		if !slices.Contains(current.Replicas, fromNodeId) {
			return nil, clusterconfig.ErrSlotMigrateSourceNotReplica
		}
		leaderInfo, ok := s.ensureSlotCurrentTermApplied(ctx, current)
		if ok {
			targetInfo, ok2 := s.slotNodeLogInfo(toNodeId, slotId)
			if ok2 && targetInfo.LogTerm == leaderInfo.LogTerm && targetInfo.LogIndex == leaderInfo.LogIndex && targetInfo.Applied >= leaderInfo.Applied {
				return current, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.stopper.ShouldStop():
			return nil, errors.New("server stopping")
		case <-ticker.C:
		}
	}
}

func (s *Server) proposeMigrateSlot(slotId uint32, fromNodeId, toNodeId uint64) error {
	if fromNodeId == 0 || toNodeId == 0 || fromNodeId == toNodeId {
		return clusterconfig.ErrSlotMigrateInvalidEndpoint
	}
	ctx, cancel := context.WithTimeout(s.cancelCtx, 15*time.Second)
	defer cancel()

	current := s.cfgServer.AppliedSlot(slotId)
	if current == nil {
		return clusterconfig.ErrSlotMigrateSlotNotFound
	}
	if current.MigrateFrom != 0 || current.MigrateTo != 0 || len(current.Learners) > 0 {
		return clusterconfig.ErrSlotMigrateInProgress
	}
	if !slices.Contains(current.Replicas, fromNodeId) {
		return clusterconfig.ErrSlotMigrateSourceNotReplica
	}
	if slices.Contains(current.Replicas, toNodeId) {
		if fromNodeId != current.Leader {
			return clusterconfig.ErrSlotMigrateFollowerToFollower
		}
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			caughtUpSlot, err := s.waitSlotTargetCaughtUp(ctx, slotId, fromNodeId, toNodeId)
			if err != nil {
				return err
			}
			if caughtUpSlot.Leader == toNodeId && caughtUpSlot.MigrateFrom == 0 && caughtUpSlot.MigrateTo == 0 {
				return nil
			}
			if caughtUpSlot.MigrateFrom != 0 || caughtUpSlot.MigrateTo != 0 || len(caughtUpSlot.Learners) > 0 {
				return clusterconfig.ErrSlotMigrateInProgress
			}
			next := caughtUpSlot.Clone()
			next.Leader = toNodeId
			next.MigrateFrom = 0
			next.MigrateTo = 0
			if err := s.installCertifiedSlotMembership(ctx, caughtUpSlot, next); err == nil {
				if installed := s.cfgServer.AppliedSlot(slotId); installed != nil {
					_, _ = s.ensureSlotCurrentTermApplied(ctx, installed)
				}
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stopper.ShouldStop():
				return errors.New("server stopping")
			case <-ticker.C:
			}
		}
	}

	onlineTarget := slices.ContainsFunc(s.cfgServer.AllowVoteAndJoinedOnlineNodes(), func(n *pb.Node) bool {
		return n != nil && n.Id == toNodeId
	})
	if !onlineTarget {
		return clusterconfig.ErrSlotMigrateTargetOffline
	}

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		current = s.cfgServer.AppliedSlot(slotId)
		if current == nil {
			return clusterconfig.ErrSlotMigrateSlotNotFound
		}
		if current.MigrateFrom == fromNodeId && current.MigrateTo == toNodeId && slices.Contains(current.Learners, toNodeId) {
			s.notifySlotMigration()
			return nil
		}
		if current.MigrateFrom != 0 || current.MigrateTo != 0 || len(current.Learners) > 0 {
			return clusterconfig.ErrSlotMigrateInProgress
		}
		if !slices.Contains(current.Replicas, fromNodeId) {
			return clusterconfig.ErrSlotMigrateSourceNotReplica
		}
		next := current.Clone()
		next.Learners = append(slices.Clone(current.Learners), toNodeId)
		next.MigrateFrom = fromNodeId
		next.MigrateTo = toNodeId
		next.Leader = current.Leader
		if err := s.installCertifiedSlotMembership(ctx, current, next); err == nil {
			s.notifySlotMigration()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopper.ShouldStop():
			return errors.New("server stopping")
		case <-ticker.C:
		}
	}
}

func (s *Server) tryCompleteSlotMigration(ctx context.Context, slotId uint32) error {
	current := s.cfgServer.AppliedSlot(slotId)
	if current == nil || current.InstalledTerm == 0 || current.MigrateFrom == 0 || current.MigrateTo == 0 || !slices.Contains(current.Learners, current.MigrateTo) {
		return nil
	}
	leaderInfo, ok := s.ensureSlotCurrentTermApplied(ctx, current)
	if !ok {
		return ErrConversationReadRetry
	}
	learnerInfo, ok := s.slotNodeLogInfo(current.MigrateTo, slotId)
	if !ok || learnerInfo.LogTerm != leaderInfo.LogTerm || learnerInfo.LogIndex != leaderInfo.LogIndex || learnerInfo.Applied < leaderInfo.Applied {
		return ErrConversationReadRetry
	}
	next := current.Clone()
	next.Learners = slices.DeleteFunc(slices.Clone(current.Learners), func(id uint64) bool {
		return id == current.MigrateTo
	})
	if current.MigrateFrom != 0 && current.MigrateFrom != current.MigrateTo {
		next.Replicas = slices.DeleteFunc(slices.Clone(current.Replicas), func(id uint64) bool {
			return id == current.MigrateFrom
		})
	}
	if !slices.Contains(next.Replicas, current.MigrateTo) {
		next.Replicas = append(next.Replicas, current.MigrateTo)
	}
	if current.MigrateFrom == current.Leader {
		next.Leader = current.MigrateTo
	} else {
		next.Leader = current.Leader
	}
	next.MigrateFrom = 0
	next.MigrateTo = 0
	if err := s.installCertifiedSlotMembership(ctx, current, next); err != nil {
		return err
	}
	if installed := s.cfgServer.AppliedSlot(slotId); installed != nil {
		_, _ = s.ensureSlotCurrentTermApplied(ctx, installed)
	}
	return nil
}

func (s *Server) slotMigrationLoop() {
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopper.ShouldStop():
			return
		case <-s.cancelCtx.Done():
			return
		case <-s.slotMigrateNotify:
		case <-ticker.C:
		}
		for _, st := range s.cfgServer.Slots() {
			if st == nil || st.InstalledTerm == 0 || st.MigrateFrom == 0 || st.MigrateTo == 0 || !slices.Contains(st.Learners, st.MigrateTo) {
				continue
			}
			if st.Leader != s.opts.ConfigOptions.NodeId && !s.cfgServer.IsLeader() {
				continue
			}
			ctx, cancel := context.WithTimeout(s.cancelCtx, 3*time.Second)
			if err := s.tryCompleteSlotMigration(ctx, st.Id); err != nil && !errors.Is(err, ErrConversationReadRetry) {
				s.Debug("slot migration completion retry", zap.Uint32("slotId", st.Id), zap.Error(err))
			}
			cancel()
		}
	}
}
