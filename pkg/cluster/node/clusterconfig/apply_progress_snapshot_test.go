package clusterconfig

import (
	"context"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestAppliedProgressSnapshotDuringActualApply(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json")))
	defer s.config.cfgFile.Close()
	data, err := NewCMD(CMDTypeUnknown, nil).Marshal()
	require.NoError(t, err)
	var readers sync.WaitGroup
	stop := make(chan struct{})
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snapshot := s.GetClusterConfig()
			if uint64(snapshot.Term) != snapshot.Version {
				t.Errorf("torn progress: %d/%d", snapshot.Term, snapshot.Version)
				return
			}
		}
	}()
	for i := uint32(1); i <= 100; i++ {
		require.NoError(t, s.applyLog(types.Log{Index: uint64(i), Term: i, Data: data}))
	}
	close(stop)
	readers.Wait()
	require.EqualValues(t, 100, s.GetClusterConfig().Version)
}

func TestDefaultProposalContextStopsWithoutWaitingForOwner(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json")))
	defer s.config.cfgFile.Close()
	require.NoError(t, s.storage.Open())
	defer s.storage.Close()
	require.NoError(t, s.initRaft())
	defer s.raft.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	s.SetProposalContext(ctx)
	cancel()
	_, err := s.ProposeUntilApplied(1, []byte("cancelled before admission"))
	require.ErrorIs(t, err, context.Canceled)
}
