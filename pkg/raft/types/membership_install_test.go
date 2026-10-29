package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMembershipCertificateRequiresBothQuorumsAndExactAuthority(t *testing.T) {
	old := Config{Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, Leader: 1, Term: 7, InstalledTerm: 7, Version: 11, MigrateFrom: 3, MigrateTo: 4}
	next := old.Clone()
	next.Replicas, next.Learners = []uint64{1, 2, 4}, nil
	next.Term, next.InstalledTerm, next.Version = 8, 8, 12
	next.MigrateFrom, next.MigrateTo = 0, 0
	makeCertificate := func() *MembershipCertificate {
		reports := map[uint64]ElectionReport{}
		for _, id := range []uint64{1, 2, 4} {
			reports[id] = ElectionReport{Identity: ElectionIdentity(old), NodeID: id, Term: 8, Fenced: true, LastTerm: 7, LastIndex: 20, Floor: 20}
		}
		return &MembershipCertificate{Reports: reports}
	}
	require.NoError(t, ValidateMembershipInstall(old, next, makeCertificate()))
	for _, name := range []string{"old minority", "new minority", "lagging leader", "lagging new majority", "stale version", "stale migration", "unfenced", "wrong term", "wrong sender", "unregistered voter", "duplicate voter", "missing witness"} {
		t.Run(name, func(t *testing.T) {
			before, after, cert := old.Clone(), next.Clone(), makeCertificate()
			switch name {
			case "old minority":
				delete(cert.Reports, 2)
			case "new minority":
				delete(cert.Reports, 2)
				delete(cert.Reports, 4)
				r := cert.Reports[1]
				r.NodeID = 3
				cert.Reports[3] = r
			case "lagging leader":
				r := cert.Reports[1]
				r.LastIndex--
				cert.Reports[1] = r
			case "lagging new majority":
				for _, id := range []uint64{2, 4} {
					r := cert.Reports[id]
					r.LastIndex--
					cert.Reports[id] = r
				}
			case "stale version":
				before.Version++
			case "stale migration":
				before.MigrateFrom = 2
			case "unfenced":
				r := cert.Reports[2]
				r.Fenced = false
				cert.Reports[2] = r
			case "wrong term":
				r := cert.Reports[2]
				r.Term++
				cert.Reports[2] = r
			case "wrong sender":
				r := cert.Reports[2]
				r.NodeID = 4
				cert.Reports[2] = r
			case "unregistered voter":
				after.Replicas = []uint64{1, 2, 99}
			case "duplicate voter":
				after.Replicas = []uint64{1, 1, 4}
			case "missing witness":
				cert = nil
			}
			require.Error(t, ValidateMembershipInstall(before, after, cert))
		})
	}
}
