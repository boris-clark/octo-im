package slot

// Reviewer-only probe (XIN-2666). Records whether the approved E3 24-entry
// contract (raw keyed PebbleShardLogStorage methods and the four Server
// forwarding accessors refuse unbound access) holds while a key is draining
// behind an accepted, held Apply. Observations are logged and asserted
// against the E3 contract; a failure means the contract is not implemented.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestXin2666E3RawEntriesRefuseDuringDrain(t *testing.T) {
	p := issue87Open(t, t.TempDir())
	p.s.storage = p
	key := SlotIdToKey(1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	p.s.opts.OnApply = func(uint32, []types.Log) error { once.Do(func() { close(entered); <-release }); return nil }
	tr := &issue87Transport{groups: map[uint64]*raftgroup.RaftGroup{}}
	g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(p), raftgroup.WithTransport(tr), raftgroup.WithTickInterval(5*time.Millisecond)))
	require.NoError(t, g.Start())
	old := e3Install(t, p, g)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() {
		unblock()
		g.Stop()
		require.NoError(t, g.WaitQuiescence(context.Background()))
		require.NoError(t, p.Close())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_, _ = g.ProposeBatchUntilAppliedTimeout(ctx, key, types.ProposeReqSet{{Id: 100, Data: []byte("old-apply")}})
	}()
	issue87Await(t, entered)
	g.RemoveRaft(old)
	// The key is now retired with an accepted Apply still holding effects.
	refused := func(err error) bool { return err != nil && errors.Is(err, raftgroup.ErrRaftBusy) }
	type row struct {
		name string
		err  error
	}
	var rows []row
	_, err := p.GetState(key)
	rows = append(rows, row{"storage.GetState", err})
	_, err = p.LastIndex(key)
	rows = append(rows, row{"storage.LastIndex", err})
	_, err = p.AppliedIndex(key)
	rows = append(rows, row{"storage.AppliedIndex", err})
	_, err = p.GetLogs(key, 1, 2, 0)
	rows = append(rows, row{"storage.GetLogs", err})
	_, err = p.GetTermStartIndex(key, 1)
	rows = append(rows, row{"storage.GetTermStartIndex", err})
	_, err = p.s.AppliedIndex(1)
	rows = append(rows, row{"Server.AppliedIndex", err})
	_, err = p.s.LastIndex(1)
	rows = append(rows, row{"Server.LastIndex", err})
	_, err = p.s.LastLog(1)
	rows = append(rows, row{"Server.LastLog", err})
	_, err = p.s.GetLogsInReverseOrder(1, 1, 2, 1)
	rows = append(rows, row{"Server.GetLogsInReverseOrder", err})
	// A raw no-op write of the current applied value; E3 requires refusal
	// before any effect, including no-op arguments.
	applied, _ := p.AppliedIndex(key)
	err = p.SetAppliedIndex(key, applied)
	rows = append(rows, row{"storage.SetAppliedIndex(no-op)", err})
	unbound := 0
	for _, r := range rows {
		t.Logf("drain probe %-34s err=%v refused=%v", r.name, r.err, refused(r.err))
		if !refused(r.err) {
			unbound++
		}
	}
	t.Logf("held-Apply observation: raw AppliedIndex during drain = %d (pre-apply value)", applied)
	require.Zero(t, unbound, "E3 24-entry contract: %d/%d sampled raw keyed entries were admitted while the key drained behind an accepted Apply", unbound, len(rows))
}
