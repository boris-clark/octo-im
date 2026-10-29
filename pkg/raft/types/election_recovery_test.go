package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryFenceSkipsUnusedTermsWithExactQuorum(t *testing.T) {
	cfg := Config{Term: 7, InstalledTerm: 7, Leader: 1, Replicas: []uint64{1, 2, 3}}
	reports := map[uint64]ElectionReport{
		1: {NodeID: 1, Term: 10, Fenced: true, LastIndex: 12, LastTerm: 7, Floor: 10},
		2: {NodeID: 2, Term: 10, Fenced: true, LastIndex: 11, LastTerm: 7, Floor: 10},
	}
	leader, err := SelectFencedLeader(cfg, 10, reports)
	require.NoError(t, err)
	require.EqualValues(t, 1, leader)
	stale := reports[1]
	stale.Term = 9
	reports[1] = stale
	_, err = SelectFencedLeader(cfg, 10, reports)
	require.Error(t, err, "a fence for another term cannot complete this quorum")
	_, err = SelectFencedLeader(cfg, 7, reports)
	require.Error(t, err)
}

func TestRecoveryEmptyBootstrapDoesNotAdoptLegacyHistory(t *testing.T) {
	cfg := Config{Term: 7, Replicas: []uint64{1, 2, 3}}
	reports := map[uint64]ElectionReport{
		1: {NodeID: 1, Term: 8, Fenced: true},
		2: {NodeID: 2, Term: 8, Fenced: true},
	}
	leader, err := SelectFencedLeader(cfg, 8, reports)
	require.NoError(t, err)
	require.EqualValues(t, 1, leader)
	nonempty := reports[1]
	nonempty.LastIndex = 1
	reports[1] = nonempty
	_, err = SelectFencedLeader(cfg, 8, reports)
	require.Error(t, err, "a nonempty legacy report closes the empty-bootstrap exception")
	cfg.Leader = 3
	reports[1] = ElectionReport{NodeID: 1, Term: 8, Fenced: true}
	_, err = SelectFencedLeader(cfg, 8, reports)
	require.Error(t, err, "an existing legacy leader cannot be replaced by this exception")
}
