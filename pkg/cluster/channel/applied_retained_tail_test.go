package channel

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

// A multi-voter channel needs a current-term recovery entry even when its old
// retained suffix was never applied. Neither the old tail nor a local marker
// can certify commitment before the other voter actually stores the barrier.
func TestConfigRecoveryRetainedOldTermTailNeedsQuorum(t *testing.T) {
	tr := retryNetwork()
	dbs := []wkdb.DB{retryDB(t), retryDB(t)}
	original := retryMessage(701, "old", "retained-old-term")
	original.MessageSeq, original.Term = 1, 1
	for _, db := range dbs {
		require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{original}))
	}
	key := wkutil.ChannelToKey("retry", 2)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1,
		Replicas: []uint64{1, 2}, Term: 5, InstalledTerm: 5, ConfVersion: 4}
	nodes := make([]*Server, 2)
	for i := range nodes {
		nodes[i] = NewServer(NewOptions(WithNodeId(uint64(i+1)), WithDB(dbs[i]), WithGroupCount(1), WithTransport(tr)))
		require.NoError(t, nodes[i].Start())
		s := nodes[i]
		t.Cleanup(func() { stopServer(t, s) })
		tr.mu.Lock()
		tr.nodes[uint64(i+1)] = s
		tr.mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tr.block(2, true)
	require.NoError(t, nodes[0].createAndApplyConfig(ctx, cfg))
	state, err := nodes[0].ReadLeaderState(ctx, "retry", 2)
	require.NoError(t, err)
	require.Zero(t, state.CommittedIndex)
	require.Zero(t, state.AppliedIndex)
	require.NoError(t, nodes[1].createAndApplyConfig(ctx, cfg))
	tr.block(2, false)
	require.Eventually(t, func() bool {
		for _, db := range dbs {
			applied, err := db.GetChannelAppliedIndex("retry", 2)
			if err != nil || applied < 2 {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	for _, db := range dbs {
		stored, err := db.LoadMsg("retry", 2, 1)
		require.NoError(t, err)
		require.Equal(t, original.MessageID, stored.MessageID)
		require.Equal(t, original.Term, stored.Term)
		require.Equal(t, original.Payload, stored.Payload)
		barriers, err := db.ChannelNoops(key, 1, 0)
		require.NoError(t, err)
		require.Len(t, barriers, 1)
		require.True(t, types.IsNoop(barriers[0]))
		require.EqualValues(t, 5, barriers[0].Term)
		require.EqualValues(t, 2, barriers[0].Index)
	}
	result := retryPropose(t, nodes[0], retryMessage(702, "new", "after-recovery"))
	require.EqualValues(t, 3, result[0].Index)
}
