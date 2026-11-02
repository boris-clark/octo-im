package raft

import (
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// ElectionFenceError retains local error identity while exposing only bounded
// authority facts to RPC peers. Native storage errors may contain private paths.
type ElectionFenceError struct {
	Phase             string
	Expected, Current types.Config
	RequestedTerm     uint32
	CurrentFence      [32]byte
	Err               error
}

// NewElectionFenceError copies owner state before it leaves the owner callback.
func NewElectionFenceError(phase string, expected, current types.Config, term uint32, err error) *ElectionFenceError {
	return &ElectionFenceError{Phase: phase, Expected: expected.Clone(), Current: current.Clone(), RequestedTerm: term, Err: err}
}

func (e *ElectionFenceError) Error() string {
	return fmt.Sprintf("election fence phase=%s requested_term=%d expected_term=%d current_term=%d expected_version=%d current_version=%d expected_identity=%x current_identity=%x expected_fence=%x current_fence=%x",
		e.Phase, e.RequestedTerm, e.Expected.Term, e.Current.Term, e.Expected.Version, e.Current.Version,
		types.ElectionIdentity(e.Expected), types.ElectionIdentity(e.Current), fenceIdentity(e.Expected), e.CurrentFence)
}

func (e *ElectionFenceError) Unwrap() error { return e.Err }

func (n *Node) fenceError(phase string, expected types.Config, term uint32, err error) error {
	rejection := NewElectionFenceError(phase, expected, n.cfg, term, err)
	rejection.CurrentFence = n.safetyState.FenceIdentity
	return rejection
}
