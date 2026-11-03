package slot

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestXin2683RealCallbackReadDuringScopeJoin(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTickInterval(time.Hour),
		raftgroup.WithTransport(&issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}})))
	require.NoError(t, g.Start())
	st := e3Install(t, p, g)
	entered, readNow, returning := make(chan struct{}), make(chan struct{}), make(chan struct{})
	configDone, scopeDone := make(chan error, 1), make(chan error, 1)
	p.s.opts.OnSaveConfigWithReads = func(_ uint32, _ types.Config, r *CallbackReads) error {
		close(entered)
		<-readNow
		_, err := r.GetState()
		return err
	}
	go func() {
		scopeDone <- e3WithStorage(g.StorageBinding(st), func(c *raftgroup.StorageCall) error {
			v := p.BindStorage(c)
			go func() { configDone <- v.SaveConfig("1", types.Config{}) }()
			<-entered
			close(returning)
			return nil
		})
	}()
	<-returning
	// The admitted SaveConfig is entered before WithStorage returns and joins it.
	time.Sleep(25 * time.Millisecond)
	close(readNow)
	select {
	case err := <-configDone:
		require.NoError(t, err)
		require.NoError(t, <-scopeDone)
		g.Stop()
		require.NoError(t, g.WaitQuiescence(context.Background()))
		require.NoError(t, p.Close())
	case <-time.After(250 * time.Millisecond):
		stack := make([]byte, 1<<18)
		stack = stack[:runtime.Stack(stack, true)]
		t.Logf("blocked actual callback and scope join:\n%s", stack)
		g.BeginStop()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, g.WaitQuiescence(ctx), raftgroup.ErrIncompleteQuiescence)
		require.ErrorIs(t, p.Close(), raftgroup.ErrIncompleteQuiescence)
		t.Fatal("actual SaveConfig callback GetState deadlocked with WithStorage revocation; shutdown cannot join")
	}
}
