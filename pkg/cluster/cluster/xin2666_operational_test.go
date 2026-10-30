package cluster

// Reviewer-only harness (XIN-2666). Exercises operational slot membership
// replacement and slot leader transfer through the production controller
// command (clusterconfig.ProposeMigrateSlot, the same call the
// POST /slots/:id/migrate handler makes after validation) on disposable local
// TCP/Pebble nodes, then continued writes and a full stop/reopen.

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

type opCluster struct {
	servers map[uint64]*Server
	slots   int
}

func newOpCluster(t *testing.T, nodes, slots, replicas int) *opCluster {
	t.Helper()
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	addresses := map[uint64]string{}
	for id := uint64(1); id <= uint64(nodes); id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[id] = l.Addr().String()
		require.NoError(t, l.Close())
	}
	c := &opCluster{servers: map[uint64]*Server{}, slots: slots}
	t.Cleanup(func() {
		for id, s := range c.servers {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			if err := s.StopWithContext(ctx); err != nil {
				t.Errorf("stop %d: %v", id, err)
			}
			cancel()
		}
	})
	for id := uint64(1); id <= uint64(nodes); id++ {
		opts := newTestOptions(t, id, addresses, clusterconfig.WithSlotCount(uint32(slots)), clusterconfig.WithSlotMaxReplicaCount(uint32(replicas)), clusterconfig.WithChannelMaxReplicaCount(uint32(replicas)))
		s := New(NewOptions(WithAddr("tcp://"+addresses[id]), WithConfigOptions(opts), WithDataDir(t.TempDir()),
			WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
		require.NoError(t, s.Start())
		c.servers[id] = s
	}
	c.waitReady(t)
	return c
}

func (c *opCluster) waitReady(t *testing.T) {
	t.Helper()
	// WaitAllSlotReady requires every node to host every slot, which is false
	// when replicas < nodes; wait for each node's assigned slots instead.
	require.Eventually(t, func() bool {
		for id, s := range c.servers {
			slots := s.cfgServer.Slots()
			if len(slots) != c.slots {
				return false
			}
			for _, st := range slots {
				if !wkutil.ArrayContainsUint64(st.Replicas, id) {
					continue
				}
				r := s.slotServer.GetSlotRaft(st.Id)
				if r == nil || r.LeaderId() == 0 {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 20*time.Millisecond)
}

// requireMigrateApplied checks that a ProposeMigrateSlot that returned nil
// actually recorded the migration (or completed it) in controller state.
func requireMigrateApplied(t *testing.T, ctrl *Server, before *pb.Slot, from, to uint64) {
	t.Helper()
	var st *pb.Slot
	ok := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st = slotSnapshot(ctrl, before.Id)
		if st != nil && ((st.MigrateFrom == from && st.MigrateTo == to) || st.Leader != before.Leader || !slicesEqualU64(st.Replicas, before.Replicas)) {
			ok = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("controller slot after accepted migrate %d->%d: leader=%d replicas=%v learners=%v migrateFrom=%d migrateTo=%d installedTerm=%d", from, to, st.Leader, st.Replicas, st.Learners, st.MigrateFrom, st.MigrateTo, st.InstalledTerm)
	require.True(t, ok, "ProposeMigrateSlot(%d,%d->%d) returned nil but controller slot state is unchanged (installedTerm=%d)", before.Id, from, to, st.InstalledTerm)
}

func slicesEqualU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (c *opCluster) controller(t *testing.T) *Server {
	t.Helper()
	var leader *Server
	require.Eventually(t, func() bool {
		for _, s := range c.servers {
			if s.cfgServer.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)
	return leader
}

func (c *opCluster) channelOnSlot(t *testing.T, prefix string, slotID uint32) string {
	t.Helper()
	any := c.servers[1]
	for i := 0; i < 100000; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		if any.getSlotId(id) == slotID {
			return id
		}
	}
	t.Fatalf("no channel on slot %d", slotID)
	return ""
}

func (c *opCluster) append(t *testing.T, s *Server, id string, msgID int64) uint64 {
	t.Helper()
	m := consistencyMessage(uint32(msgID))
	m.ChannelID, m.MessageSeq = id, 0
	m.MessageID = msgID
	m.Payload = []byte(fmt.Sprintf("identity-%d", msgID))
	var idx uint64
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r, err := s.store.AppendMessages(ctx, id, 2, []wkdb.Message{m})
		if err != nil {
			t.Logf("append %s/%d via node %d: %v", id, msgID, s.opts.ConfigOptions.NodeId, err)
			return false
		}
		idx = r[0].Index
		return len(r) == 1
	}, 30*time.Second, 200*time.Millisecond)
	return idx
}

// verify requires that every current channel replica stores msgID at index.
func (c *opCluster) verify(t *testing.T, id string, index uint64, msgID int64) {
	t.Helper()
	any := c.servers[1]
	var cfg wkdb.ChannelClusterConfig
	require.Eventually(t, func() bool {
		var err error
		cfg, err = any.loadConversationConfig(context.Background(), id, 2)
		return err == nil
	}, 10*time.Second, 20*time.Millisecond)
	require.NotEmpty(t, cfg.Replicas)
	for _, node := range cfg.Replicas {
		s := c.servers[node]
		require.NotNil(t, s, "replica %d", node)
		require.Eventually(t, func() bool {
			a, err := s.db.GetChannelAppliedIndex(id, 2)
			return err == nil && a >= index
		}, 15*time.Second, 20*time.Millisecond, "node %d applied index for %s >= %d", node, id, index)
		m, err := s.db.LoadMsg(id, 2, index)
		require.NoError(t, err, "node %d %s@%d", node, id, index)
		require.Equal(t, msgID, m.MessageID, "node %d %s@%d", node, id, index)
		require.Equal(t, fmt.Sprintf("identity-%d", msgID), string(m.Payload))
	}
}

func (c *opCluster) restartAll(t *testing.T) {
	t.Helper()
	old := c.servers
	c.servers = map[uint64]*Server{}
	for _, s := range old {
		require.NoError(t, s.Stop())
	}
	for id, s := range old {
		opts := *s.opts
		opts.ChannelTransport, opts.SlotTransport = nil, nil
		n := New(&opts)
		require.NoError(t, n.Start())
		c.servers[id] = n
	}
	c.waitReady(t)
}

func slotSnapshot(s *Server, id uint32) *pb.Slot {
	st := s.cfgServer.Slot(id)
	if st == nil {
		return nil
	}
	return st.Clone()
}

func waitMigration(t *testing.T, c *opCluster, slotID uint32, done func(*pb.Slot) bool, within time.Duration) (*pb.Slot, bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last *pb.Slot
	for time.Now().Before(deadline) {
		all := true
		for _, s := range c.servers {
			st := slotSnapshot(s, slotID)
			last = st
			if st == nil || !done(st) {
				all = false
				break
			}
		}
		if all {
			return last, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return last, false
}

func slotLeaderTransferCase(t *testing.T, nodes int) {
	c := newOpCluster(t, nodes, 3, 3)
	ctrl := c.controller(t)
	var slotID uint32
	var st *pb.Slot
	for _, cand := range ctrl.cfgServer.Slots() {
		if len(cand.Replicas) == 3 {
			slotID, st = cand.Id, cand.Clone()
			break
		}
	}
	require.NotNil(t, st)
	from := st.Leader
	var to uint64
	for _, r := range st.Replicas {
		if r != from {
			to = r
			break
		}
	}
	ch := c.channelOnSlot(t, "xfer", slotID)
	idx := map[int64]uint64{}
	for msg := int64(5001); msg <= 5003; msg++ {
		idx[msg] = c.append(t, c.servers[from], ch, msg)
	}
	for msg, i := range idx {
		c.verify(t, ch, i, msg)
	}
	t.Logf("before transfer slot=%d leader=%d replicas=%v term=%d", slotID, st.Leader, st.Replicas, st.Term)
	require.NoError(t, ctrl.cfgServer.ProposeMigrateSlot(slotID, from, to))
	requireMigrateApplied(t, ctrl, st, from, to)
	final, ok := waitMigration(t, c, slotID, func(s *pb.Slot) bool { return s.Leader == to && s.MigrateFrom == 0 && s.MigrateTo == 0 }, 30*time.Second)
	require.True(t, ok, "slot %d leader transfer %d->%d did not complete through the controller; last=%+v", slotID, from, to, final)
	t.Logf("after transfer slot=%d leader=%d replicas=%v learners=%v term=%d", slotID, final.Leader, final.Replicas, final.Learners, final.Term)
	for msg := int64(5004); msg <= 5006; msg++ {
		idx[msg] = c.append(t, c.servers[to], ch, msg)
	}
	ch2 := c.channelOnSlot(t, "xfer-new", slotID)
	idx2 := c.append(t, c.servers[to], ch2, 5100)
	for msg, i := range idx {
		c.verify(t, ch, i, msg)
	}
	c.verify(t, ch2, idx2, 5100)
	c.restartAll(t)
	idx[5007] = c.append(t, c.servers[to], ch, 5007)
	for msg, i := range idx {
		c.verify(t, ch, i, msg)
	}
	c.verify(t, ch2, idx2, 5100)
	t.Logf("leader transfer + reopen retained %d committed messages, last index %d", len(idx)+1, idx[5007])
}

// TestXin2666SlotLeaderTransferThroughController moves a slot leader to an
// existing follower through the controller migrate command.
func TestXin2666SlotLeaderTransferThroughController(t *testing.T) {
	slotLeaderTransferCase(t, 3)
}

// TestXin2666SlotFollowerReplacementThroughController replaces a follower
// with a node outside the replica set through the controller migrate command.
func TestXin2666SlotFollowerReplacementThroughController(t *testing.T) {
	c := newOpCluster(t, 4, 4, 3)
	ctrl := c.controller(t)
	var st *pb.Slot
	var to uint64
	for _, cand := range ctrl.cfgServer.Slots() {
		if len(cand.Replicas) != 3 {
			continue
		}
		for id := uint64(1); id <= 4; id++ {
			if !wkutil.ArrayContainsUint64(cand.Replicas, id) {
				st, to = cand.Clone(), id
			}
		}
		if st != nil {
			break
		}
	}
	require.NotNil(t, st)
	var from uint64
	for _, r := range st.Replicas {
		if r != st.Leader {
			from = r
			break
		}
	}
	ch := c.channelOnSlot(t, "replace", st.Id)
	idx := map[int64]uint64{}
	for msg := int64(6001); msg <= 6003; msg++ {
		idx[msg] = c.append(t, c.servers[st.Leader], ch, msg)
	}
	t.Logf("before replacement slot=%d leader=%d replicas=%v from=%d to=%d", st.Id, st.Leader, st.Replicas, from, to)
	require.NoError(t, ctrl.cfgServer.ProposeMigrateSlot(st.Id, from, to))
	requireMigrateApplied(t, ctrl, st, from, to)
	final, ok := waitMigration(t, c, st.Id, func(s *pb.Slot) bool {
		return wkutil.ArrayContainsUint64(s.Replicas, to) && !wkutil.ArrayContainsUint64(s.Replicas, from) && s.MigrateFrom == 0 && s.MigrateTo == 0 && len(s.Learners) == 0
	}, 30*time.Second)
	require.True(t, ok, "slot %d replacement %d->%d did not complete; last=%+v", st.Id, from, to, final)
	t.Logf("after replacement slot=%d leader=%d replicas=%v learners=%v term=%d", st.Id, final.Leader, final.Replicas, final.Learners, final.Term)
	writer := c.servers[final.Leader]
	for msg := int64(6004); msg <= 6006; msg++ {
		idx[msg] = c.append(t, writer, ch, msg)
	}
	ch2 := c.channelOnSlot(t, "replace-new", st.Id)
	idx2 := c.append(t, writer, ch2, 6100)
	for msg, i := range idx {
		c.verify(t, ch, i, msg)
	}
	c.verify(t, ch2, idx2, 6100)
	// The new slot replica serves the slot-owned channel metadata.
	newRep := c.servers[to]
	require.Eventually(t, func() bool {
		a, err := newRep.slotServer.AppliedIndex(st.Id)
		b, err2 := writer.slotServer.AppliedIndex(st.Id)
		return err == nil && err2 == nil && a > 0 && a >= b
	}, 15*time.Second, 50*time.Millisecond)
	c.restartAll(t)
	idx[6007] = c.append(t, c.servers[final.Leader], ch, 6007)
	for msg, i := range idx {
		c.verify(t, ch, i, msg)
	}
	c.verify(t, ch2, idx2, 6100)
	t.Logf("replacement + reopen retained %d committed messages, last index %d", len(idx)+1, idx[6007])
}

// TestXin2666SlotLeaderReplacementThroughController replaces the current slot
// leader with a node outside the replica set (learner -> leader hand-off).
func TestXin2666SlotLeaderReplacementThroughController(t *testing.T) {
	c := newOpCluster(t, 4, 4, 3)
	ctrl := c.controller(t)
	var st *pb.Slot
	var to uint64
	for _, cand := range ctrl.cfgServer.Slots() {
		for id := uint64(1); id <= 4; id++ {
			if len(cand.Replicas) == 3 && !wkutil.ArrayContainsUint64(cand.Replicas, id) {
				st, to = cand.Clone(), id
			}
		}
		if st != nil {
			break
		}
	}
	require.NotNil(t, st)
	from := st.Leader
	ch := c.channelOnSlot(t, "lreplace", st.Id)
	idx := map[int64]uint64{}
	for msg := int64(7001); msg <= 7003; msg++ {
		idx[msg] = c.append(t, c.servers[from], ch, msg)
	}
	t.Logf("before leader replacement slot=%d leader=%d replicas=%v to=%d", st.Id, st.Leader, st.Replicas, to)
	require.NoError(t, ctrl.cfgServer.ProposeMigrateSlot(st.Id, from, to))
	requireMigrateApplied(t, ctrl, st, from, to)
	final, ok := waitMigration(t, c, st.Id, func(s *pb.Slot) bool {
		return s.Leader == to && !wkutil.ArrayContainsUint64(s.Replicas, from) && s.MigrateFrom == 0 && s.MigrateTo == 0
	}, 30*time.Second)
	require.True(t, ok, "slot %d leader replacement %d->%d did not complete; last=%+v", st.Id, from, to, final)
	for msg := int64(7004); msg <= 7006; msg++ {
		idx[msg] = c.append(t, c.servers[to], ch, msg)
	}
	c.restartAll(t)
	idx[7007] = c.append(t, c.servers[to], ch, 7007)
	for msg, i := range idx {
		c.verify(t, ch, i, msg)
	}
}

// TestXin2666SlotLeaderFailoverByElection stops a slot leader process and
// lets the production controller elect a replacement leader (the only slot
// leadership change the candidate permits), then continues writing and
// reopens every node.
func TestXin2666SlotLeaderFailoverByElection(t *testing.T) {
	c := newOpCluster(t, 3, 3, 3)
	ctrl := c.controller(t)
	ctrlID := ctrl.opts.ConfigOptions.NodeId
	var st *pb.Slot
	for _, cand := range ctrl.cfgServer.Slots() {
		if cand.Leader != ctrlID && len(cand.Replicas) == 3 {
			st = cand.Clone()
			break
		}
	}
	if st == nil {
		t.Skip("every slot is led by the controller leader in this placement")
	}
	from := st.Leader
	ch := c.channelOnSlot(t, "failover", st.Id)
	idx := map[int64]uint64{}
	for msg := int64(8001); msg <= 8003; msg++ {
		idx[msg] = c.append(t, ctrl, ch, msg)
	}
	for msg, i := range idx {
		c.verify(t, ch, i, msg)
	}
	stopped := c.servers[from]
	opts := *stopped.opts
	require.NoError(t, stopped.Stop())
	delete(c.servers, from)
	var final *pb.Slot
	require.Eventually(t, func() bool {
		final = slotSnapshot(ctrl, st.Id)
		return final != nil && final.Leader != from && final.Leader != 0
	}, 45*time.Second, 50*time.Millisecond, "slot %d leader %d was not replaced by election", st.Id, from)
	t.Logf("failover slot=%d old leader=%d new leader=%d term=%d installedTerm=%d", st.Id, from, final.Leader, final.Term, final.InstalledTerm)
	for msg := int64(8004); msg <= 8006; msg++ {
		idx[msg] = c.append(t, ctrl, ch, msg)
	}
	opts.ChannelTransport, opts.SlotTransport = nil, nil
	n := New(&opts)
	require.NoError(t, n.Start())
	c.servers[from] = n
	c.waitReady(t)
	c.restartAll(t)
	idx[8007] = c.append(t, c.servers[final.Leader], ch, 8007)
	for msg, i := range idx {
		c.verify(t, ch, i, msg)
	}
	t.Logf("failover + reopen retained %d committed messages, last index %d", len(idx), idx[8007])
}
