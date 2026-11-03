package channel

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type blockedReloadCluster struct {
	icluster.ICluster
	entered, release chan struct{}
}

func (c blockedReloadCluster) GetOrCreateChannelClusterConfigFromSlotLeader(string, uint8) (wkdb.ChannelClusterConfig, error) {
	close(c.entered)
	<-c.release
	return wkdb.EmptyChannelClusterConfig, context.Canceled
}

func TestConfigReloadShutdownRetainsStorageOwnership(t *testing.T) {
	c := blockedReloadCluster{entered: make(chan struct{}), release: make(chan struct{})}
	s := NewServer(NewOptions(WithNodeId(1), WithDB(retryDB(t)), WithGroupCount(1), WithTransport(retryNetwork()), WithCluster(c)))
	require.NoError(t, s.Start())
	t.Cleanup(func() { stopServer(t, s) })
	t.Cleanup(func() {
		select {
		case <-c.release:
		default:
			close(c.release)
		}
	})
	s.wakeFollowerIfNeedAsync("reload", 2)
	<-c.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := s.StopWithContext(ctx)
	require.ErrorIs(t, err, raftgroup.ErrIncompleteQuiescence)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(c.release)
	require.NoError(t, s.StopWithContext(context.Background()))
	// Admission is closed; a later hint cannot start another metadata call.
	s.wakeFollowerIfNeedAsync("reload", 2)
}
