package types

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
)

// ElectionIdentity binds a report to the complete applied authority, including
// its version and installation markers. Local roles are not authority fields.
func ElectionIdentity(cfg Config) [32]byte {
	cfg.Role, cfg.FenceEnabled = RoleUnknown, false
	cfg.Replicas = append([]uint64(nil), cfg.Replicas...)
	cfg.Learners = append([]uint64(nil), cfg.Learners...)
	b, _ := json.Marshal(cfg)
	return sha256.Sum256(b)
}

// MembershipCertificate is a transient slot-command witness, not a read hint.
// Old voters stop first; a new majority must already hold the selected tail.
type MembershipCertificate struct {
	Reports map[uint64]ElectionReport
}

func validMembers(cfg Config) bool {
	seen := make(map[uint64]bool)
	for _, ids := range [][]uint64{cfg.Replicas, cfg.Learners} {
		for _, id := range ids {
			if id == 0 || seen[id] {
				return false
			}
			seen[id] = true
		}
	}
	return len(cfg.Replicas) > 0 && slices.Contains(cfg.Replicas, cfg.Leader)
}

// ValidMembershipTransition accepts learner registration, one registered
// learner's promotion/replacement, and completion of a declared leader transfer.
// A callback cannot introduce an unregistered voting replica.
func ValidMembershipTransition(old, next Config) bool {
	if !validMembers(old) || !validMembers(next) {
		return false
	}
	if slices.Equal(old.Replicas, next.Replicas) {
		if slices.Equal(old.Learners, next.Learners) && next.MigrateFrom == 0 && next.MigrateTo == 0 {
			return old.MigrateFrom == old.Leader && old.MigrateTo == next.Leader
		}
		if next.Leader != old.Leader || !slices.Contains(next.Learners, next.MigrateTo) ||
			(next.MigrateFrom != next.MigrateTo && !slices.Contains(old.Replicas, next.MigrateFrom)) {
			return false
		}
		for _, id := range old.Learners {
			if !slices.Contains(next.Learners, id) {
				return false
			}
		}
		// Do not redirect an active migration. Equal orphan endpoints whose
		// target is absent may be replaced by the maintenance path.
		active := slices.Contains(old.Learners, old.MigrateTo) || slices.Contains(old.Replicas, old.MigrateTo)
		return !active || old.MigrateFrom == next.MigrateFrom && old.MigrateTo == next.MigrateTo
	}
	if next.MigrateFrom != 0 || next.MigrateTo != 0 {
		return false
	}
	for _, promoted := range old.Learners {
		if (old.MigrateTo != 0 || old.MigrateFrom != 0) && (old.MigrateTo != promoted || old.MigrateFrom == 0) {
			continue
		}
		replicas := slices.Clone(old.Replicas)
		if old.MigrateFrom != 0 && old.MigrateFrom != promoted {
			replicas = slices.DeleteFunc(replicas, func(id uint64) bool { return id == old.MigrateFrom })
		}
		replicas = append(replicas, promoted)
		learners := slices.DeleteFunc(slices.Clone(old.Learners), func(id uint64) bool { return id == promoted })
		leader := old.Leader
		if old.MigrateFrom == leader {
			leader = promoted
		}
		if slices.Equal(replicas, next.Replicas) && slices.Equal(learners, next.Learners) && leader == next.Leader {
			return true
		}
	}
	return false
}

// ValidateMembershipInstall is shared by admission and deterministic slot apply.
// Equality of the stored tail (term,index) uses the Raft log matching invariant;
// a lagging new quorum cannot inherit acknowledged old-configuration history.
func ValidateMembershipInstall(old, next Config, cert *MembershipCertificate) error {
	if cert == nil || next.Term <= old.Term || next.InstalledTerm != next.Term ||
		!ValidMembershipTransition(old, next) {
		return errors.New("invalid membership install")
	}
	identity := ElectionIdentity(old)
	reports := make(map[uint64]ElectionReport)
	for id, report := range cert.Reports {
		if report.Identity == identity && report.NodeID == id && report.Fenced && report.Term == next.Term &&
			(slices.Contains(old.Replicas, id) || slices.Contains(old.Learners, id)) {
			reports[id] = report
		}
	}
	selected, err := SelectFencedLeader(old, next.Term, reports)
	if err != nil {
		return err
	}
	tail := reports[selected]
	leader, ok := reports[next.Leader]
	if !ok || leader.LastIndex != tail.LastIndex || leader.LastTerm != tail.LastTerm {
		return errors.New("membership leader does not hold selected old-voter history")
	}
	matched := 0
	for _, id := range next.Replicas {
		r, ok := reports[id]
		if ok && r.LastIndex == tail.LastIndex && r.LastTerm == tail.LastTerm && r.Floor <= tail.LastIndex {
			matched++
		}
	}
	if matched < len(next.Replicas)/2+1 {
		return errors.New("new voter majority has not stored selected history")
	}
	return nil
}
