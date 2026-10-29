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
		if err := ch.Node.FenceElection(channelConfigToRaftConfig(s.opts.NodeId, expected), term); err != nil {
			return err
		}
		state, err := s.storage.GetState(expected.ChannelId, expected.ChannelType)
		if err != nil {
			return err
		}
		report = types.ElectionReport{NodeID: s.opts.NodeId, Term: term, LastIndex: state.LastLogIndex, LastTerm: state.LastTerm, Floor: state.HardState.CommittedFloor, BoundaryTerm: state.HardState.BoundaryTerm, Fenced: state.HardState.FenceTerm == term && state.HardState.InstalledTerm < term && !state.Diverged}
		return nil
	})
	return report, err
}
