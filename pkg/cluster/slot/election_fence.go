package slot

import (
	"context"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// FenceElection persists on the existing owner before sampling actual stored entries.
func (s *Server) FenceElection(ctx context.Context, expected *pb.Slot, term uint32) (types.ElectionReport, error) {
	var report types.ElectionReport
	key := SlotIdToKey(expected.Id)
	err := s.raftGroup.Do(ctx, key, func(r raftgroup.IRaft) error {
		if r == nil {
			return raftgroup.ErrRaftNotExist
		}
		st := r.(*Slot)
		cfg := s.slotToConfig(expected)
		if !st.PendingElectionFence() && !raft.SameElectionIdentity(st.Node.Config(), cfg) && cfg.Term >= st.Node.Config().Term {
			if err := st.Node.Step(types.Event{Type: types.ConfChange, Config: cfg}); err != nil {
				return err
			}
		}
		if err := st.Node.FenceElection(cfg, term); err != nil {
			return err
		}
		var state types.RaftState
		if err := s.raftGroup.StorageBinding(st).WithStorage(func(call *raftgroup.StorageCall) error {
			var err error
			state, err = s.storage.BindStorage(call).(*storageView).GetState(key)
			return err
		}); err != nil {
			return err
		}
		report = types.ElectionReport{
			NodeID:       s.opts.NodeId,
			Term:         term,
			Identity:     types.ElectionIdentity(cfg),
			LastIndex:    state.LastLogIndex,
			LastTerm:     state.LastTerm,
			Floor:        state.HardState.CommittedFloor,
			BoundaryTerm: state.HardState.BoundaryTerm,
			Fenced:       state.HardState.FenceTerm == term && state.HardState.InstalledTerm < term && !state.Diverged,
		}
		return nil
	})
	return report, err
}
