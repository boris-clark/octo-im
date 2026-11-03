package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

// Keep the real Config Raft listener dispatch and store/read hint entrypoints
// running while the stopped worker is replaced, as in orphan recovery.
func TestConfigReconcilerReinitCallbacks(t *testing.T) {
	servers, _ := newConfigRPCServersWithSlots(t, 1, 1)
	s := servers[0]
	done := make(chan struct{})
	var callbacks sync.WaitGroup
	callbacks.Add(1)
	go func() {
		defer callbacks.Done()
		for {
			select {
			case <-done:
				return
			default:
				s.cfgServer.NotifyConfigChangeEvent()
				s.hintChannelConfig("lifetime-hint", 2)
				s.onChannelConfigSaved("lifetime-saved", 2)
			}
		}
	}()
	defer func() { close(done); callbacks.Wait() }()
	for i := 0; i < 64; i++ {
		s.initChannelConfigReconciler()
		r := s.configReconciler
		require.NotNil(t, r.process)
		require.NotNil(t, r.page)
		require.NotNil(t, r.owned)
		r.start()
		r.stop()
	}
}

// Hold actual storage I/O across replacement and shutdown. Every retired
// generation must remain a joined owner until that I/O returns.
func TestConfigReconcilerReinitJoinsStorageOwner(t *testing.T) {
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	releaseRead := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseRead)
	s, _ := newConversationConsistencyServer(t, func(s *Server) {
		page := s.configReconciler.page
		s.configReconciler.page = func(offset uint64, limit int) ([]wkdb.ChannelClusterConfig, error) {
			if armed.Load() {
				enteredOnce.Do(func() { close(entered) })
				<-release
			}
			return page(offset, limit)
		}
	})
	old := s.configReconciler
	armed.Store(true)
	old.rescan()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("storage holdpoint was not reached")
	}
	replaced := make(chan struct{})
	go func() { s.initChannelConfigReconciler(); close(replaced) }()
	defer func() { releaseRead(); <-replaced; old.stop() }()
	select {
	case <-old.ctx.Done():
	case <-replaced:
		t.Fatal("replacement returned without retiring the previous storage owner")
	case <-time.After(time.Second):
		t.Fatal("replacement did not cancel the previous generation")
	}
	select {
	case <-replaced:
		t.Fatal("replacement returned while its retired storage owner was blocked")
	default:
	}
	// A callback reentered from a proposal must not wait for storage retirement.
	callback := make(chan struct{})
	go func() { s.cfgServer.NotifyConfigChangeEvent(); close(callback) }()
	select {
	case <-callback:
	case <-time.After(time.Second):
		releaseRead()
		<-callback
		t.Fatal("callback blocked behind the retiring worker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := s.StopWithContext(ctx)
	cancel()
	require.ErrorIs(t, err, raft.ErrIncompleteQuiescence)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = s.db.GetChannelClusterConfigs(0, 1)
	require.NoError(t, err, "retired generation still owns shared storage")
	current := s.configReconciler
	s.initChannelConfigReconciler()
	require.Same(t, current, s.configReconciler, "shutdown must reject replacement generations")
	current.start()
	require.ErrorIs(t, current.ctx.Err(), context.Canceled, "shutdown generation cannot be restarted")
	releaseRead()
	<-replaced
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.StopWithContext(ctx))
}

func TestConfigReconcilerStartStopHandoff(t *testing.T) {
	for i := 0; i < 32; i++ {
		r := newChannelConfigReconciler(context.Background())
		r.page = func(uint64, int) ([]wkdb.ChannelClusterConfig, error) { return nil, nil }
		r.process = func(ctx context.Context, _ channelConfigKey) error { return ctx.Err() }
		r.owned = func(channelConfigKey) bool { return true }
		var callers sync.WaitGroup
		for j := 0; j < 8; j++ {
			callers.Add(1)
			go func() {
				defer callers.Done()
				r.start()
				r.stop()
			}()
		}
		callers.Wait()
		r.start()
		r.stop()
		require.False(t, r.add(channelConfigKey{"after-stop", 2}, true))
	}
}
