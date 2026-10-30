package clusterconfig

import (
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestProposeMigrateSlotRefusalContracts(t *testing.T) {
	opts := NewOptions()
	opts.ConfigPath = t.TempDir() + "/config.json"
	c := NewConfig(opts)
	defer func() { _ = c.cfgFile.Close() }()
	c.cfg.Nodes = []*pb.Node{
		{Id: 1, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusJoined, Online: true},
		{Id: 2, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusJoined, Online: true},
		{Id: 3, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusJoined, Online: true},
		{Id: 4, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusJoined, Online: true},
		{Id: 5, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusJoined, Online: false},
	}
	c.cfg.Slots = []*pb.Slot{
		{Id: 0, Leader: 1, Term: 2, InstalledTerm: 2, Replicas: []uint64{1, 2, 3}},
		{Id: 1, Leader: 1, Term: 2, InstalledTerm: 2, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: 2, MigrateTo: 4},
	}
	s := &Server{config: c}

	require.ErrorIs(t, s.ProposeMigrateSlot(0, 0, 2), ErrSlotMigrateInvalidEndpoint)
	require.ErrorIs(t, s.ProposeMigrateSlot(0, 1, 0), ErrSlotMigrateInvalidEndpoint)
	require.ErrorIs(t, s.ProposeMigrateSlot(0, 1, 1), ErrSlotMigrateInvalidEndpoint)
	require.ErrorIs(t, s.ProposeMigrateSlot(99, 1, 2), ErrSlotMigrateSlotNotFound)
	require.ErrorIs(t, s.ProposeMigrateSlot(1, 2, 4), ErrSlotMigrateInProgress)
	require.ErrorIs(t, s.ProposeMigrateSlot(0, 4, 2), ErrSlotMigrateSourceNotReplica)
	require.ErrorIs(t, s.ProposeMigrateSlot(0, 2, 3), ErrSlotMigrateFollowerToFollower)
	require.ErrorIs(t, s.ProposeMigrateSlot(0, 2, 5), ErrSlotMigrateTargetOffline)
	require.ErrorIs(t, s.ProposeMigrateSlot(0, 2, 4), ErrSlotMigrateNotInstalled)
}

func TestConditionalSlotInstallWithMembershipCertificate(t *testing.T) {
	opts := NewOptions()
	opts.ConfigPath = t.TempDir() + "/config.json"
	c := NewConfig(opts)
	defer func() { _ = c.cfgFile.Close() }()

	old := &pb.Slot{Id: 3, Leader: 1, Term: 4, InstalledTerm: 4, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: 1, MigrateTo: 4}
	c.cfg.Slots = []*pb.Slot{old.Clone()}

	next := old.Clone()
	next.Replicas = []uint64{2, 3, 4}
	next.Learners = nil
	next.Leader = 4
	next.Term = 5
	next.InstalledTerm = 5
	next.MigrateFrom = 0
	next.MigrateTo = 0

	// Without a membership certificate, changing Replicas/Learners is rejected.
	require.False(t, c.installSlotWithMembership(old, next, nil))

	identity := rt.ElectionIdentity(slotToRaftConfig(old))
	cert := &rt.MembershipCertificate{
		Reports: map[uint64]rt.ElectionReport{
			1: {Identity: identity, NodeID: 1, Term: 5, Fenced: true, LastTerm: 4, LastIndex: 12, Floor: 12},
			2: {Identity: identity, NodeID: 2, Term: 5, Fenced: true, LastTerm: 4, LastIndex: 12, Floor: 12},
			4: {Identity: identity, NodeID: 4, Term: 5, Fenced: true, LastTerm: 4, LastIndex: 12, Floor: 12},
		},
	}
	require.True(t, c.installSlotWithMembership(old, next, cert))
	require.Equal(t, []uint64{2, 3, 4}, c.slot(3).Replicas)
	require.EqualValues(t, 4, c.slot(3).Leader)
	require.EqualValues(t, 5, c.slot(3).InstalledTerm)
}

func TestCertifiedSlotInstallRefusesInvalidWitnesses(t *testing.T) {
	old := &pb.Slot{Id: 1, Leader: 1, Term: 4, InstalledTerm: 4, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: 1, MigrateTo: 4}
	next := old.Clone()
	next.Term = 5
	next.InstalledTerm = 5
	next.BoundaryTerm = 4
	next.Leader = 4
	next.Replicas = []uint64{2, 3, 4}
	next.Learners = nil
	next.MigrateFrom = 0
	next.MigrateTo = 0
	for _, name := range []string{"valid", "stale identity", "wrong reporter", "not fenced", "wrong term", "lagging learner", "missing old majority", "missing new majority", "discarded boundary", "unknown learner"} {
		t.Run(name, func(t *testing.T) {
			candidate := next.Clone()
			cert := &rt.MembershipCertificate{Reports: map[uint64]rt.ElectionReport{}}
			for _, id := range []uint64{1, 2, 4} {
				cert.Reports[id] = rt.ElectionReport{NodeID: id, Identity: rt.ElectionIdentity(slotToRaftConfig(old)), Term: 5, Fenced: true, LastIndex: 12, LastTerm: 4, Floor: 12, BoundaryTerm: 4}
			}
			report := cert.Reports[1]
			switch name {
			case "stale identity":
				report.Identity = [32]byte{}
				cert.Reports[1] = report
			case "wrong reporter":
				report.NodeID = 3
				cert.Reports[1] = report
			case "not fenced":
				report.Fenced = false
				cert.Reports[1] = report
			case "wrong term":
				report.Term = 4
				cert.Reports[1] = report
			case "lagging learner":
				report = cert.Reports[4]
				report.LastIndex = 11
				cert.Reports[4] = report
			case "missing old majority":
				delete(cert.Reports, 2)
			case "missing new majority":
				delete(cert.Reports, 4)
			case "discarded boundary":
				candidate.BoundaryTerm = 0
			case "unknown learner":
				candidate.Replicas = []uint64{2, 3, 5}
				candidate.Leader = 5
			}
			_, ok := ConditionalSlotInstallWithMembership(old, old, candidate, cert)
			require.Equal(t, name == "valid", ok)
		})
	}
}
