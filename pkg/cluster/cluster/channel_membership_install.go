package cluster

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

func sameChannelMembership(a, b wkdb.ChannelClusterConfig) bool {
	return slices.Equal(a.Replicas, b.Replicas) && slices.Equal(a.Learners, b.Learners) && a.MigrateFrom == b.MigrateFrom && a.MigrateTo == b.MigrateTo
}

// certifyChannelMembership treats callbacks as requests. Only current applied
// voters/learners can supply a witness; online capacity is never a quorum.
func (s *Server) certifyChannelMembership(ctx context.Context, old wkdb.ChannelClusterConfig, next *wkdb.ChannelClusterConfig) error {
	if old.ReplicaMaxCount > 0 && len(next.Replicas)+len(next.Learners) > int(old.ReplicaMaxCount) {
		return raft.ErrElectionNotInstalled
	}
	allowed := make(map[uint64]bool)
	for _, node := range s.cfgServer.AllowVoteAndJoinedOnlineNodes() {
		allowed[node.Id] = true
	}
	for _, id := range next.Learners {
		if !slices.Contains(old.Learners, id) && !allowed[id] {
			return raft.ErrElectionNotInstalled
		}
	}
	if old.Term == math.MaxUint32 || !rt.ValidMembershipTransition(channelElectionConfig(old), channelElectionConfig(*next)) {
		return raft.ErrElectionNotInstalled
	}
	applied, err := s.loadConversationConfigLocal(ctx, old.ChannelId, old.ChannelType)
	if err != nil {
		return err
	}
	if !applied.Equal(old) {
		return raft.ErrConfigVersionStale
	}
	term := max(next.Term, old.Term+1)
	reports, err := s.requestElectionFences(ctx, channelElectionConfig(old), electionFenceRequest{Channel: &old, Term: term, IncludeLearners: true})
	if err != nil {
		return err
	}
	next.Term, next.InstalledTerm = term, term
	next.Expected = &old
	next.Membership = &rt.MembershipCertificate{Reports: reports}
	for _, report := range reports {
		if report.Fenced && report.Term == term {
			next.BoundaryTerm = max(next.BoundaryTerm, report.BoundaryTerm)
		}
	}
	if err := rt.ValidateMembershipInstall(channelElectionConfig(old), channelElectionConfig(*next), next.Membership); err != nil {
		return fmt.Errorf("%w: %w", ErrConversationReadRetry, err)
	}
	return nil
}
