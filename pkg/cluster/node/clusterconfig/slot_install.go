package clusterconfig

import (
	"encoding/json"
	"errors"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"google.golang.org/protobuf/proto"
)

var ErrSlotInstallConflict = errors.New("slot conditional install did not match applied identity")

type slotInstall struct {
	Expected *pb.Slot
	Next     *pb.Slot
}

// ProposeSlotInstall never retries an unknown result with the old decision.
// Applied state, including all membership and migration fields, decides the result.
func (s *Server) ProposeSlotInstall(expected, next *pb.Slot) error {
	data, err := json.Marshal(slotInstall{expected.Clone(), next.Clone()})
	if err != nil {
		return err
	}
	cmd := NewCMD(CMDTypeSlotInstall, data)
	b, err := cmd.Marshal()
	if err != nil {
		return err
	}
	_, proposalErr := s.ProposeUntilApplied(s.genConfigId(), b)
	current := s.AppliedSlot(expected.Id)
	if current != nil && proto.Equal(current, next) {
		return nil
	}
	if proposalErr != nil {
		return proposalErr
	}
	return ErrSlotInstallConflict
}

func (c *Config) installSlot(expected, next *pb.Slot) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, current := range c.cfg.Slots {
		if result, ok := ConditionalSlotInstall(current, expected, next); ok {
			c.cfg.Slots[i] = result
			return true
		}
	}
	return false
}

// ConditionalSlotInstall is the full-identity comparison used by applied commands.
func ConditionalSlotInstall(current, expected, next *pb.Slot) (*pb.Slot, bool) {
	if current == nil || expected == nil || next == nil || !proto.Equal(current, expected) || expected.Id != next.Id || expected.Term == ^uint32(0) || next.Term != expected.Term+1 || next.InstalledTerm != next.Term || next.Leader == 0 {
		return nil, false
	}
	a, b := expected.Clone(), next.Clone()
	a.Leader, b.Leader = 0, 0
	a.Term, b.Term = 0, 0
	a.InstalledTerm, b.InstalledTerm = 0, 0
	a.BoundaryTerm, b.BoundaryTerm = 0, 0
	a.ExpectLeader, b.ExpectLeader = 0, 0
	a.Status, b.Status = 0, 0
	if !proto.Equal(a, b) || expected.BoundaryTerm != 0 && next.BoundaryTerm != expected.BoundaryTerm {
		return nil, false
	}
	voter := false
	for _, id := range expected.Replicas {
		if id == next.Leader {
			voter = true
		}
	}
	for _, id := range expected.Learners {
		if id == next.Leader {
			voter = false
		}
	}
	if !voter {
		return nil, false
	}
	return next.Clone(), true
}

func (s *Server) handleSlotInstall(cmd *CMD) error {
	var change slotInstall
	if err := json.Unmarshal(cmd.Data, &change); err != nil {
		return err
	}
	s.config.installSlot(change.Expected, change.Next)
	return nil
}

// AppliedSlot excludes speculative in-memory mutations whose config sync failed.
func (s *Server) AppliedSlot(id uint32) *pb.Slot {
	s.config.mu.RLock()
	defer s.config.mu.RUnlock()
	if st := s.config.durableSlots[id]; st != nil {
		return st.Clone()
	}
	return nil
}
