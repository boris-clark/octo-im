package types

import (
	"encoding/binary"
	"fmt"
)

// HardState is local election state, independent of the last log's term.
// All markers and the authenticated committed floor survive log truncation.
type HardState struct {
	FenceIdentity     [32]byte
	Quarantined       bool
	InstalledIdentity [32]byte
	Term              uint32
	Vote              uint64
	FenceTerm         uint32
	InstalledTerm     uint32
	InstalledLeader   uint64
	CommittedFloor    uint64
	BoundaryTerm      uint32
}

func (s HardState) Marshal() []byte {
	data := make([]byte, 105)
	binary.BigEndian.PutUint32(data, s.Term)
	binary.BigEndian.PutUint64(data[4:], s.Vote)
	binary.BigEndian.PutUint32(data[12:], s.FenceTerm)
	binary.BigEndian.PutUint32(data[16:], s.InstalledTerm)
	binary.BigEndian.PutUint64(data[20:], s.InstalledLeader)
	binary.BigEndian.PutUint64(data[28:], s.CommittedFloor)
	binary.BigEndian.PutUint32(data[36:], s.BoundaryTerm)
	copy(data[40:72], s.InstalledIdentity[:])
	if s.Quarantined {
		data[72] = 1
	}
	copy(data[73:], s.FenceIdentity[:])
	return data
}

func (s *HardState) Unmarshal(data []byte) error {
	if len(data) != 12 && len(data) != 105 {
		return fmt.Errorf("invalid raft hard state length: %d", len(data))
	}
	*s = HardState{}
	if len(data) == 105 {
		copy(s.FenceIdentity[:], data[73:])
		if data[72] > 1 {
			return fmt.Errorf("invalid quarantine marker")
		}
		s.Quarantined = data[72] == 1
		copy(s.InstalledIdentity[:], data[40:72])
		s.FenceTerm = binary.BigEndian.Uint32(data[12:])
		s.InstalledTerm = binary.BigEndian.Uint32(data[16:])
		s.InstalledLeader = binary.BigEndian.Uint64(data[20:])
		s.CommittedFloor = binary.BigEndian.Uint64(data[28:])
		s.BoundaryTerm = binary.BigEndian.Uint32(data[36:])
	}
	s.Term = binary.BigEndian.Uint32(data)
	s.Vote = binary.BigEndian.Uint64(data[4:])
	if s.Term == 0 && s.Vote != 0 {
		return fmt.Errorf("raft vote without a term")
	}
	return nil
}

// ValidateHardStateTransition refuses regressions even through stale storage callbacks.
func ValidateHardStateTransition(old, next HardState) error {
	if old.Quarantined && !next.Quarantined ||
		old.FenceTerm != 0 && next.FenceTerm == old.FenceTerm && old.FenceIdentity != next.FenceIdentity || next.Term < old.Term || next.FenceTerm < old.FenceTerm || next.InstalledTerm < old.InstalledTerm || next.CommittedFloor < old.CommittedFloor ||
		old.BoundaryTerm != 0 && next.BoundaryTerm != old.BoundaryTerm ||
		next.InstalledTerm == old.InstalledTerm && next.InstalledIdentity != old.InstalledIdentity ||
		next.InstalledTerm == old.InstalledTerm && old.InstalledLeader != 0 && next.InstalledLeader != old.InstalledLeader {
		return fmt.Errorf("regressive raft hard state")
	}
	return nil
}
