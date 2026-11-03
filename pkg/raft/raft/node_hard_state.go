package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

func (n *Node) setTerm(term uint32) {
	if term > n.cfg.Term {
		n.voteFor = None
		n.cfg.Term = term
	}
}

func (n *Node) hardState() types.HardState {
	state := n.safetyState
	state.Term, state.Vote = n.cfg.Term, n.voteFor
	return state
}

func (n *Node) hasUnpersistedHardState() bool {
	return n.opts.SaveHardState != nil && n.hardState() != n.persistedHardState
}

// persistCommit publishes no commit until the authenticated owner transition is durable.
func (n *Node) persistCommit(index uint64) bool {
	if index <= n.queue.committedIndex {
		return true
	}
	n.pendingCommit = max(n.pendingCommit, index)
	state := n.hardState()
	state.CommittedFloor = max(state.CommittedFloor, index)
	if n.opts.SaveHardState != nil {
		if err := n.opts.SaveHardState(state); err != nil {
			return false
		}
		n.persistedHardState = state
	}
	n.safetyState = state
	n.pendingCommit = 0
	return true
}
