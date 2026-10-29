package types

import "errors"

// ElectionReport is returned only after the voter has durably stopped the old term.
type ElectionReport struct {
	Identity     [32]byte
	NodeID       uint64
	Term         uint32
	LastIndex    uint64
	LastTerm     uint32
	Floor        uint64
	BoundaryTerm uint32
	Fenced       bool
}

// SelectFencedLeader uses the applied voter set, never online capacity.
func SelectFencedLeader(cfg Config, term uint32, reports map[uint64]ElectionReport) (uint64, error) {
	voters := make(map[uint64]bool)
	for _, id := range cfg.Replicas {
		if id == 0 {
			continue
		}
		learner := false
		for _, l := range cfg.Learners {
			if l == id {
				learner = true
			}
		}
		if !learner {
			voters[id] = true
		}
	}
	valid := make(map[uint64]ElectionReport)
	var floor uint64
	for id, report := range reports {
		if voters[id] && report.NodeID == id && report.Fenced && report.Term == term {
			valid[id] = report
			floor = max(floor, report.Floor)
		}
	}
	if term <= cfg.Term || len(valid) < len(voters)/2+1 {
		return 0, errors.New("durable applied-voter fence has no majority")
	}
	// A legacy history cannot be reconciled by maximum term/index alone.
	if cfg.InstalledTerm == 0 {
		if r, ok := valid[cfg.Leader]; ok && r.LastIndex >= floor {
			return cfg.Leader, nil
		}
		// A leaderless bootstrap with an empty fenced majority has no old
		// quorum history to preserve. A nonempty report closes this exception.
		emptyBootstrap := cfg.Leader == 0 && floor == 0
		for _, r := range valid {
			emptyBootstrap = emptyBootstrap && r.LastIndex == 0
		}
		if !emptyBootstrap {
			return 0, errors.New("legacy leader of record is not a fenced reporter")
		}
	}
	var leader uint64
	var newest ElectionReport
	for id, r := range valid {
		if r.LastIndex < floor {
			continue
		}
		if leader == 0 || r.LastTerm > newest.LastTerm || r.LastTerm == newest.LastTerm && (r.LastIndex > newest.LastIndex || r.LastIndex == newest.LastIndex && id < leader) {
			leader, newest = id, r
		}
	}
	if leader == 0 {
		return 0, errors.New("no fenced candidate covers committed floor")
	}
	return leader, nil
}
