package raft

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// LeaderReadReady must be called on the owner's event loop, just like Step.
// A leader draining proposals for transfer must not certify a read boundary.
func (n *Node) LeaderReadReady() bool {
	return n.IsLeader() && !n.Diverged() && !n.hasUnpersistedHardState() && !n.stopPropose && !n.truncating
}

// BufferedLogs exposes the not-yet-stored suffix to the owner callback only.
// Neither the slice nor its entries may be retained or modified.
func (n *Node) BufferedLogs() []types.Log { return n.queue.logs }

// ResumeReplication retries replication without inferring commitment from an
// old-term tail. Multi-voter logs still require the quorum calculation.
func (n *Node) ResumeReplication() {
	if !n.LeaderReadReady() {
		return
	}
	n.KeepAlive()
	n.updateLeaderCommittedIndex()
	n.sendNotifySync(All)
}

// ResumeReplicationWithEntry admits a current-term barrier when a retained
// suffix cannot yet be committed. The caller must wait for the returned index
// to commit; admission alone is never proof of storage or commitment.
// Call on the owner, using an entry supported by the group's storage adapter.
func (n *Node) ResumeReplicationWithEntry(entry types.Log) (uint64, error) {
	if !n.LeaderReadReady() {
		return 0, errors.New("raft: recovery leader is not ready")
	}
	n.ResumeReplication()
	if n.LastLogIndex() <= n.CommittedIndex() {
		return 0, nil
	}
	if n.LastLogTerm() == n.cfg.Term {
		return n.LastLogIndex(), nil
	}
	// Ready may still be storing this term's entry. Do not admit duplicates on
	// owner retries or after caller cancellation.
	if logs := n.queue.logs; len(logs) > 0 && logs[len(logs)-1].Term == n.cfg.Term {
		return n.LastLogIndex(), nil
	}
	admission, err := n.Admit([]types.Log{entry})
	return admission.LastIndex, err
}

// LogRevision fences off-owner lookups against replacement as well as appends.
// Call only on the owner loop. Store/apply progress does not change log content.
func (n *Node) LogRevision() uint64 { return n.queue.revision }
