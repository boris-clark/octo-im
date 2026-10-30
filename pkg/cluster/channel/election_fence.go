package channel

import (
	"context"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

func (s *Server) FenceElection(ctx context.Context, expected wkdb.ChannelClusterConfig, term uint32) (types.ElectionReport, error) {
	var report types.ElectionReport
	key := wkutil.ChannelToKey(expected.ChannelId, expected.ChannelType)
	// Dormant existing channels are reconstructed through the same owner reservation.
	if s.getRaftGroup(key).GetRaft(key) == nil {
		rg := s.getRaftGroup(key)
		if _, err := rg.InitializeRaft(ctx, key, func() (raftgroup.IRaft, error) {
			return createChannel(expected, s, rg)
		}, func(r raftgroup.IRaft) error {
			return r.(*Channel).Node.FenceElection(channelConfigToRaftConfig(s.opts.NodeId, expected), term)
		}); err != nil {
			return report, err
		}
	}
	err := s.getRaftGroup(key).Do(ctx, key, func(r raftgroup.IRaft) error {
		ch := r.(*Channel)
		// The caller has compared the complete applied authority. A version-only
		// metadata update can precede its runtime hint; adopt that version before
		// fencing, while never reactivating an already pending old-term fence.
		if !ch.PendingElectionFence() && ch.Config().Version < expected.ConfVersion {
			if err := ch.applyConfigOnOwner(channelConfigToRaftConfig(s.opts.NodeId, expected)); err != nil {
				return err
			}
		}
		if err := ch.Node.FenceElection(channelConfigToRaftConfig(s.opts.NodeId, expected), term); err != nil {
			return err
		}
		state, err := s.storage.GetState(expected.ChannelId, expected.ChannelType)
		if err != nil {
			return err
		}
		report = types.ElectionReport{Identity: types.ElectionIdentity(channelConfigToRaftConfig(s.opts.NodeId, expected)), NodeID: s.opts.NodeId, Term: term, LastIndex: state.LastLogIndex, LastTerm: state.LastTerm, Floor: state.HardState.CommittedFloor, BoundaryTerm: state.HardState.BoundaryTerm, Fenced: state.HardState.FenceTerm == term && state.HardState.InstalledTerm < term && !state.Diverged}
		return nil
	})
	return report, err
}
