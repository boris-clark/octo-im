package clusterconfig

import (
	"sync"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func snapshotTestSlot() *pb.Slot {
	return &pb.Slot{
		Id: 7, Leader: 1, Term: 2, BoundaryTerm: 1, ExpectLeader: 2,
		Replicas: []uint64{1, 2, 3}, Learners: []uint64{4},
		MigrateFrom: 3, MigrateTo: 4, Status: pb.SlotStatus_SlotStatusCandidate,
	}
}

func TestConfigSlotsSnapshotIsolation(t *testing.T) {
	for name, mutate := range map[string]func([]*pb.Slot){
		"slice":    func(slots []*pb.Slot) { slots[0] = &pb.Slot{Id: 99} },
		"identity": func(slots []*pb.Slot) { slots[0].Leader, slots[0].Term = 2, 3 },
		"replicas": func(slots []*pb.Slot) { slots[0].Replicas[0] = 99 },
		"learners": func(slots []*pb.Slot) { slots[0].Learners[0] = 99 },
		"migration": func(slots []*pb.Slot) {
			slots[0].MigrateFrom, slots[0].MigrateTo, slots[0].ExpectLeader = 9, 10, 11
		},
	} {
		t.Run(name, func(t *testing.T) {
			original := snapshotTestSlot()
			c := &Config{cfg: &pb.Config{Slots: []*pb.Slot{original.Clone()}}}
			s := &Server{config: c}
			first, second := s.Slots(), s.Slots()
			mutate(first)
			require.True(t, proto.Equal(original, c.slot(7)), "snapshot mutation escaped into owner state")
			require.True(t, proto.Equal(original, second[0]), "independent snapshots share mutable state")
			require.True(t, proto.Equal(original, s.Slots()[0]), "later reads must retain owner state")
		})
	}
}

func TestConfigSlotsSnapshotSurvivesOwnerUpdates(t *testing.T) {
	original := snapshotTestSlot()
	c := &Config{cfg: &pb.Config{Slots: []*pb.Slot{original.Clone()}}}
	s := &Server{config: c}
	before := s.Slots()
	c.updateSlotStatus(7, pb.SlotStatus_SlotStatusNormal)
	require.True(t, proto.Equal(original, before[0]), "in-place status update changed a retained snapshot")
	next := c.slot(7)
	next.Learners[0], next.Replicas[2], next.MigrateTo = 5, 6, 5
	c.updateSlots([]*pb.Slot{next})
	require.True(t, proto.Equal(original, before[0]), "slot replacement changed a retained snapshot")
	require.True(t, proto.Equal(next, s.Slots()[0]), "owner updates must remain visible on a new read")
	installed := next.Clone()
	installed.Term++
	installed.InstalledTerm = installed.Term
	installed.Leader, installed.ExpectLeader = 2, 0
	require.True(t, c.installSlot(next, installed))
	require.True(t, proto.Equal(original, before[0]), "conditional install changed a retained snapshot")
	require.True(t, proto.Equal(installed, s.Slots()[0]), "conditional install must publish through the owner")

	// Migration updates mutate the existing element and append a learner.
	c = &Config{cfg: &pb.Config{Slots: []*pb.Slot{{Id: 8, Leader: 1, Replicas: []uint64{1, 2, 3}}}}}
	before = c.slots()
	c.updateSlotMigrate(8, 3, 4)
	require.Zero(t, before[0].MigrateFrom)
	require.Zero(t, before[0].MigrateTo)
	require.Empty(t, before[0].Learners)
	require.EqualValues(t, 4, c.slot(8).MigrateTo)
	require.Equal(t, []uint64{4}, c.slot(8).Learners)
}

func TestConfigSlotsSnapshotConcurrentApply(t *testing.T) {
	original := snapshotTestSlot()
	c := &Config{cfg: &pb.Config{Slots: []*pb.Slot{original.Clone()}}}
	s := &Server{config: c}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 1000; i++ {
			next := original.Clone()
			next.Status = pb.SlotStatus(i % 2)
			c.updateSlots([]*pb.Slot{next})
			c.updateSlotStatus(7, pb.SlotStatus_SlotStatusCandidate)
		}
	}()
	for reader := 0; reader < 2; reader++ {
		go func() {
			defer workers.Done()
			<-start
			for i := 0; i < 1000; i++ {
				st := s.Slots()[0]
				if st.Replicas[0] != 1 || st.Learners[0] != 4 || st.MigrateTo != 4 {
					t.Error("snapshot observed another reader's mutation")
					return
				}
				st.Replicas[0], st.Learners[0], st.MigrateTo = 99, 99, 99
			}
		}()
	}
	close(start)
	workers.Wait()
	current := c.slot(7)
	current.Status = original.Status
	require.True(t, proto.Equal(original, current), "readers must not alter applied state")
}

func TestConfigSlotsSnapshotEmpty(t *testing.T) {
	c := &Config{cfg: &pb.Config{}}
	require.Nil(t, c.slots())
	c.cfg.Slots = []*pb.Slot{}
	require.NotNil(t, c.slots())
	require.Empty(t, c.slots())
}
