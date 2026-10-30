package slot

import (
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type Slot struct {
	*raft.Node
	slot    *types.Slot
	shardNo string
	wklog.Log
}

func newSlot(slot *types.Slot, s *Server, bindings ...*raftgroup.StorageBinding) (*Slot, error) {
	shardNo := SlotIdToKey(slot.Id)
	st := &Slot{
		slot:    slot.Clone(),
		shardNo: shardNo,
		Log:     wklog.NewWKLog("slot"),
	}
	if len(bindings) != 1 || bindings[0] == nil {
		return nil, ErrStorageUnbound
	}
	binding := bindings[0]
	var state rafttypes.RaftState
	var lastLogIndex uint64
	err := binding.WithStorage(func(call *raftgroup.StorageCall) error {
		view := s.storage.BindStorage(call).(*storageView)
		var err error
		state, err = view.GetState(shardNo)
		if err != nil {
			return err
		}
		lastLogIndex, err = view.GetTermStartIndex(shardNo, state.LastTerm)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("get slot state: %w", err)
	}
	if state.Diverged {
		// The fence is in place before the raft exists, so no read or
		// proposal of this slot is served by this replica.
		s.quarantine(slot.Id, ReplicaQuarantined, fmt.Errorf("%w: slot %d opened with applied %d last %d or a divergence marker",
			rafttypes.ErrSlotDiverged, slot.Id, state.AppliedIndex, state.LastLogIndex))
	}
	node := raft.NewNode(lastLogIndex, state, raft.NewOptions(
		raft.WithKey(shardNo),
		raft.WithNodeId(s.opts.NodeId),
		raft.WithSaveHardState(func(state rafttypes.HardState) error {
			return binding.WithStorage(func(call *raftgroup.StorageCall) error {
				return s.storage.BindStorage(call).(*storageView).SaveHardState(shardNo, state)
			})
		}),
		// Leader hand-off by role switch keeps the term (learner) or bypasses
		// the healthy-quorum election; slot leadership changes only by election.
		raft.WithLeaderRoleSwitchDisabled(true),
		// Storage raises the read fence before it writes the marker; this
		// keeps the registry in step with every other quarantine path.
		raft.WithOnQuarantine(func(failStop bool, why string) {
			to := ReplicaQuarantined
			if failStop {
				to = ReplicaFailStopped
			}
			s.quarantine(slot.Id, to, fmt.Errorf("%w: %s", rafttypes.ErrSlotDiverged, why))
		}),
	))
	st.Node = node

	return st, nil
}

// needUpdate 判断是否需要更新
func (s *Slot) needUpdate(newSlot *types.Slot) bool {

	return !s.slot.Equal(newSlot)
}

func (s *Slot) LastLogIndexAndTerm() (uint64, uint32) {
	return s.LastLogIndex(), s.LastLogTerm()
}
