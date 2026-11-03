package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

// consumeStoreResponse runs before term handling, divergence containment or
// clearing appending. Invalid results must leave the current attempt intact.
func (n *Node) consumeStoreResponse(e types.Event) bool {
	o := n.pendingStore
	if !o.ValidResponse(e) || e.Type != types.StoreResp || n.pendingStoreRevision != n.queue.storageRevision {
		return false
	}
	req := o.Request()
	if len(req.Logs) == 0 {
		return false
	}
	end := req.Logs[len(req.Logs)-1].Index
	live := make([]types.Log, 0, len(req.Logs))
	for _, l := range n.queue.logs {
		if l.Index >= req.Logs[0].Index && l.Index <= end {
			live = append(live, l)
		}
	}
	if !types.SameLogs(req.Logs, live) {
		return false
	}
	if (n.pendingStoreTermStartIndex == nil) != (req.TermStartIndexInfo == nil) {
		return false
	}
	if req.TermStartIndexInfo != nil && *req.TermStartIndexInfo != *n.pendingStoreTermStartIndex {
		return false
	}
	if e.Term != 0 || e.From != 0 || e.To != 0 {
		return false
	}
	if e.Reason == types.ReasonOk {
		if e.Index < req.Logs[0].Index || e.Index > end {
			return false
		}
		prefix := req.Logs[:e.Index-req.Logs[0].Index+1]
		if !types.SameLogs(e.Logs, prefix) {
			return false
		}
		if row := n.pendingStoreTermStartIndex; row != nil && e.Index >= row.Index {
			n.updateLastTermStartIndex(row.Term, row.Index)
		}
	}
	n.pendingStore = nil
	n.pendingStoreTermStartIndex = nil
	return true
}
