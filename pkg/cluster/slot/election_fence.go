package slot

import (
	"context"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// FenceElection persists on the existing owner before sampling actual stored entries.
func (s *Server) FenceElection(ctx context.Context, expected *pb.Slot, term uint32) (types.ElectionReport, error) {
	var report types.ElectionReport
	err := s.raftGroup.Do(ctx, SlotIdToKey(expected.Id), func(r raftgroup.IRaft) error {
		st := r.(*Slot)
		if err := st.Node.FenceElection(s.slotToConfig(expected), term); err != nil {
			return err
		}
		state, err := s.storage.GetState(SlotIdToKey(expected.Id))
		if err != nil {
			return err
		}
		report = types.ElectionReport{NodeID: s.opts.NodeId, Term: term, LastIndex: state.LastLogIndex, LastTerm: state.LastTerm, Floor: state.HardState.CommittedFloor, BoundaryTerm: state.HardState.BoundaryTerm, Fenced: state.HardState.FenceTerm == term && state.HardState.InstalledTerm < term && !state.Diverged}
		return nil
	})
	return report, err
}
