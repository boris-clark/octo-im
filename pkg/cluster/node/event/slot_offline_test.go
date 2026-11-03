package event

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOfflineLeaderWithZeroLeaderSurvivorsStillRequestsElection(t *testing.T) {
	slots := []*types.Slot{
		{Id: 0, Leader: 2, Term: 7, InstalledTerm: 7, BoundaryTerm: 3, Replicas: []uint64{1, 2, 3}},
		{Id: 1, Leader: 2, Term: 8, InstalledTerm: 8, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: 2, MigrateTo: 4},
	}
	candidates := offlineSlotCandidates(slots, 2)
	require.Len(t, candidates, 2, "absence of surviving leaders in the distribution map must not suppress elections")
	for i, st := range candidates {
		require.Equal(t, types.SlotStatus_SlotStatusCandidate, st.Status)
		require.Zero(t, st.ExpectLeader)
		original := slots[i].Clone()
		st.Status = original.Status
		require.True(t, st.Equal(original), "election hint changed certified membership or history markers")
	}
	require.Equal(t, types.SlotStatus_SlotStatusNormal, slots[0].Status)
}
