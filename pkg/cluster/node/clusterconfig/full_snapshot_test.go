package clusterconfig

import (
	"sync"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func fullSnapshotFixture() *pb.Config {
	return &pb.Config{Version: 1, SlotCount: 1, Learners: []uint64{3}, MigrateFrom: 1, MigrateTo: 3,
		Nodes: []*pb.Node{{Id: 1, ApiServerAddr: "before", Online: true}},
		Slots: []*pb.Slot{{Id: 0, Leader: 1, Term: 1, Replicas: []uint64{1, 2}, Learners: []uint64{3}, MigrateFrom: 1, MigrateTo: 3}},
	}
}

func TestFullConfigSnapshotIsolation(t *testing.T) {
	c := &Config{cfg: fullSnapshotFixture()}
	s := &Server{config: c}
	want := c.cfg.Clone()
	snapshot := s.GetClusterConfig()
	snapshot.Version = 99
	snapshot.Learners[0] = 99
	snapshot.Nodes[0].ApiServerAddr = "changed"
	snapshot.Slots[0].Leader = 99
	snapshot.Slots[0].Replicas[0] = 99
	snapshot.Slots[0].Learners[0] = 99
	snapshot.Slots[0].MigrateTo = 99
	snapshot.Nodes[0] = &pb.Node{Id: 99}
	snapshot.Slots[0] = &pb.Slot{Id: 99}
	require.True(t, proto.Equal(want, s.GetClusterConfig()), "snapshot mutation escaped into owner state")

	before := s.GetClusterConfig()
	c.updateApiServerAddr(1, "owner-update")
	c.updateNodeOnlineStatus(1, false)
	c.update(fullSnapshotFixture())
	c.updateApiServerAddr(1, "installed")
	require.True(t, proto.Equal(want, before), "owner update mutated an earlier snapshot")
	require.Equal(t, "installed", s.GetClusterConfig().Nodes[0].ApiServerAddr)
	require.Nil(t, (&Server{config: &Config{}}).GetClusterConfig())
}

func TestFullConfigSnapshotConcurrentUpdate(t *testing.T) {
	c := &Config{cfg: fullSnapshotFixture()}
	s := &Server{config: c}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			c.update(fullSnapshotFixture())
			c.updateApiServerAddr(1, "owner-update")
			c.updateNodeOnlineStatus(1, false)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			cfg := s.GetClusterConfig()
			cfg.Nodes[0].ApiServerAddr = "reader"
			cfg.Slots[0].Replicas[0] = 99
			cfg.Learners[0] = 99
			_, _ = cfg.Marshal()
		}
	}()
	close(start)
	wg.Wait()
	require.Equal(t, uint64(1), s.GetClusterConfig().Slots[0].Replicas[0])
}
