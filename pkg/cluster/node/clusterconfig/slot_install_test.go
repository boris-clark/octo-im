package clusterconfig

import (
	"encoding/json"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
)

func TestSlotAppliedInstallFullIdentityAndConcurrentControllers(t *testing.T) {
	opts := NewOptions()
	opts.ConfigPath = t.TempDir() + "/config.json"
	c := NewConfig(opts)
	defer c.cfgFile.Close()
	expected := &pb.Slot{Id: 7, Leader: 1, Term: 7, InstalledTerm: 7, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: 3, MigrateTo: 4, Status: pb.SlotStatus_SlotStatusCandidate}
	c.cfg.Slots = []*pb.Slot{expected.Clone()}
	s := &Server{config: c}
	next := expected.Clone()
	next.Term = 8
	next.InstalledTerm = 8
	next.Leader = 2
	next.Status = pb.SlotStatus_SlotStatusNormal
	for _, mutate := range []func(*pb.Slot){func(st *pb.Slot) { st.Replicas = []uint64{1, 2} }, func(st *pb.Slot) { st.Learners = nil }, func(st *pb.Slot) { st.MigrateFrom = 2 }, func(st *pb.Slot) { st.MigrateTo = 2 }, func(st *pb.Slot) { st.Term = 6 }, func(st *pb.Slot) { st.Leader = 2 }} {
		stale := expected.Clone()
		mutate(stale)
		require.False(t, c.installSlot(stale, next))
	}
	var wg sync.WaitGroup
	wins := make(chan bool, 2)
	for _, leader := range []uint64{2, 3} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidate := next.Clone()
			candidate.Leader = leader
			wins <- c.installSlot(expected, candidate)
		}()
	}
	wg.Wait()
	close(wins)
	n := 0
	for won := range wins {
		if won {
			n++
		}
	}
	require.Equal(t, 1, n)
	// Apply the same deterministic loser through the real command dispatcher and durable config file.
	data, err := json.Marshal(slotInstall{Expected: expected, Next: next})
	require.NoError(t, err)
	cmd := NewCMD(CMDTypeSlotInstall, data)
	b, err := cmd.Marshal()
	require.NoError(t, err)
	require.NoError(t, s.applyLog(types.Log{Index: 1, Term: 1, Data: b}))
	winner := c.slot(7).Clone()
	require.NoError(t, c.saveConfig())
	reopened := NewConfig(opts)
	defer reopened.cfgFile.Close()
	require.True(t, winner.Equal(reopened.slot(7)))
	require.EqualValues(t, 8, reopened.slot(7).InstalledTerm)
}

func TestSlotInstallRereadExcludesFailedDurablePublication(t *testing.T) {
	opts := NewOptions()
	opts.ConfigPath = t.TempDir() + "/config.json"
	c := NewConfig(opts)
	defer func() { _ = c.cfgFile.Close() }()
	s := &Server{config: c}
	expected := &pb.Slot{Id: 7, Leader: 1, Term: 7, InstalledTerm: 7, Replicas: []uint64{1, 2, 3}}
	c.cfg.Slots = []*pb.Slot{expected}
	require.NoError(t, c.saveConfig())
	next := expected.Clone()
	next.Term = 8
	next.InstalledTerm = 8
	next.Leader = 2
	require.True(t, c.installSlot(expected, next))
	require.NoError(t, c.cfgFile.Close())
	require.Error(t, c.saveConfig())
	require.EqualValues(t, 8, c.slot(7).Term)
	require.EqualValues(t, 7, s.AppliedSlot(7).Term, "an unknown install must reread durable applied state")
	var err error
	c.cfgFile, err = os.OpenFile(opts.ConfigPath, os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, c.saveConfig())
	require.EqualValues(t, 8, s.AppliedSlot(7).Term)
}
