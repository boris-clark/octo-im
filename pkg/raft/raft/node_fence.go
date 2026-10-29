package raft

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"slices"
)

var ErrElectionNotInstalled = errors.New("election has no matching applied install")

func (n *Node) fenceEnabled() bool {
	return n.cfg.FenceEnabled || n.safetyState.FenceTerm != 0 || n.safetyState.InstalledTerm != 0
}

// FenceElection runs only on the owner, before any report reads durable logs.
// The caller must compare expect with the applied authority before invoking it.
func (n *Node) FenceElection(expect types.Config, term uint32) error {
	if n.opts.SaveHardState == nil || n.Diverged() || term == 0 || term <= expect.Term ||
		n.cfg.Term > term || n.safetyState.InstalledTerm >= term || !n.matchesFenceExpected(expect, term) {
		return ErrElectionNotInstalled
	}
	if n.pendingCommit > n.queue.committedIndex {
		index := n.pendingCommit
		if !n.persistCommit(index) {
			return ErrElectionNotInstalled
		}
		n.queue.committedIndex = index
	}
	n.BecomeFollower(term, None)
	n.cfg.FenceEnabled = true
	n.cfg.Leader = None
	n.safetyState.FenceTerm = term
	n.safetyState.FenceIdentity = fenceIdentity(expect)
	if n.safetyState.InstalledTerm == 0 && n.safetyState.BoundaryTerm == 0 {
		n.safetyState.BoundaryTerm = expect.Term
	}
	state := n.hardState()
	if err := n.opts.SaveHardState(state); err != nil {
		return err
	}
	n.persistedHardState = state
	if n.pendingStore != nil || n.truncating {
		return ErrElectionNotInstalled
	}
	return nil
}

// SameElectionIdentity includes membership and migration, not only leader/term.
func SameElectionIdentity(a, b types.Config) bool {
	return a.Term == b.Term && a.Leader == b.Leader && a.Version == b.Version &&
		a.MigrateFrom == b.MigrateFrom && a.MigrateTo == b.MigrateTo &&
		slices.Equal(a.Replicas, b.Replicas) && slices.Equal(a.Learners, b.Learners)
}

func (n *Node) checkInstalledConfig(cfg types.Config) error {
	if !n.fenceEnabled() && !cfg.FenceEnabled && cfg.InstalledTerm == 0 {
		return nil
	}
	// Check the raw incoming term before legacy normalization can raise it.
	if cfg.Term < n.cfg.Term || cfg.Leader == 0 {
		return ErrElectionNotInstalled
	}
	if cfg.Leader != 0 {
		if cfg.InstalledTerm != cfg.Term || cfg.Term < n.safetyState.FenceTerm ||
			cfg.Term == n.safetyState.InstalledTerm && (cfg.Leader != n.safetyState.InstalledLeader || configIdentity(cfg) != n.safetyState.InstalledIdentity) {
			return ErrElectionNotInstalled
		}
		n.safetyState.InstalledTerm, n.safetyState.InstalledLeader = cfg.Term, cfg.Leader
		n.safetyState.InstalledIdentity = configIdentity(cfg)
	}
	n.safetyState.BoundaryTerm = max(n.safetyState.BoundaryTerm, cfg.BoundaryTerm)
	return nil
}

func (n *Node) rejectUninstalledEvent(e types.Event) bool {
	if !n.fenceEnabled() {
		return false
	}
	switch e.Type {
	case types.Ping, types.SyncResp, types.NotifySync, types.ConfigResp:
		return n.safetyState.InstalledTerm != n.cfg.Term || e.Term != n.cfg.Term || e.From != n.safetyState.InstalledLeader
	case types.Campaign, types.VoteReq, types.VoteResp:
		return !n.opts.ElectionOn
	}
	return false
}

func (n *Node) matchesFenceExpected(expect types.Config, term uint32) bool {
	current := n.cfg.Clone()
	if n.safetyState.FenceTerm == term && n.safetyState.InstalledTerm < term {
		if n.safetyState.FenceIdentity != fenceIdentity(expect) {
			return false
		}
		current.Term, current.Leader = expect.Term, expect.Leader
	}
	return SameElectionIdentity(current, expect)
}
func configIdentity(cfg types.Config) [32]byte {
	cfg.Replicas = append([]uint64(nil), cfg.Replicas...)
	cfg.Learners = append([]uint64(nil), cfg.Learners...)
	cfg.Role, cfg.Version, cfg.FenceEnabled, cfg.InstalledTerm, cfg.BoundaryTerm = types.RoleUnknown, 0, false, 0, 0
	b, _ := json.Marshal(cfg)
	return sha256.Sum256(b)
}

func fenceIdentity(cfg types.Config) [32]byte {
	identity := configIdentity(cfg)
	var data [40]byte
	copy(data[:32], identity[:])
	binary.BigEndian.PutUint64(data[32:], cfg.Version)
	return sha256.Sum256(data[:])
}

func (n *Node) PendingElectionFence() bool {
	return n.safetyState.FenceTerm != 0 && n.safetyState.FenceTerm > n.safetyState.InstalledTerm
}

// RestoreElectionFence restores only membership after a reread of applied state.
// No leader is adopted and no election decision is replayed.
func (n *Node) RestoreElectionFence(expected types.Config) error {
	if n.Diverged() || !n.PendingElectionFence() || expected.Term == ^uint32(0) || expected.Term+1 != n.safetyState.FenceTerm || n.safetyState.FenceIdentity != fenceIdentity(expected) {
		return ErrElectionNotInstalled
	}
	n.BecomeFollower(n.safetyState.FenceTerm, None)
	expected = expected.Clone()
	expected.Term = n.safetyState.FenceTerm
	expected.Leader = None
	expected.Role = types.RoleFollower
	expected.FenceEnabled = true
	n.cfg = expected
	return nil
}
