package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAppliedVoterFenceQuorumAndLegacyTarget(t *testing.T) {
	cfg := Config{Term: 7, InstalledTerm: 7, Leader: 1, Replicas: []uint64{1, 2, 3, 4, 5}, Learners: []uint64{99}}
	reports := map[uint64]ElectionReport{}
	for _, id := range []uint64{2, 3, 99} {
		reports[id] = ElectionReport{NodeID: id, Term: 8, Fenced: true, LastIndex: 12, LastTerm: 7}
	}
	_, err := SelectFencedLeader(cfg, 8, reports)
	require.Error(t, err)
	reports[4] = ElectionReport{NodeID: 4, Term: 7, Fenced: true, LastIndex: 12, LastTerm: 7}
	_, err = SelectFencedLeader(cfg, 8, reports)
	require.Error(t, err)
	reports[4] = ElectionReport{NodeID: 4, Term: 8, Fenced: true, LastIndex: 12, LastTerm: 7}
	leader, err := SelectFencedLeader(cfg, 8, reports)
	require.NoError(t, err)
	require.EqualValues(t, 2, leader)
	cfg.InstalledTerm = 0
	_, err = SelectFencedLeader(cfg, 8, reports)
	require.Error(t, err)
	reports[1] = ElectionReport{NodeID: 1, Term: 8, Fenced: true, LastIndex: 10, LastTerm: 7}
	leader, err = SelectFencedLeader(cfg, 8, reports)
	require.NoError(t, err)
	require.EqualValues(t, 1, leader, "legacy target must be leader of record, not the longer tail")
}
