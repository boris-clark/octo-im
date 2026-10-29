package cluster

import (
	"context"
	"encoding/json"
	"errors"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"math"
	"sync"
	"time"
)

type electionFenceRequest struct {
	Slot    *pb.Slot
	Channel *wkdb.ChannelClusterConfig
	Term    uint32
}

func slotElectionConfig(st *pb.Slot) rt.Config {
	return rt.Config{Replicas: st.Replicas, Learners: st.Learners, MigrateFrom: st.MigrateFrom, MigrateTo: st.MigrateTo, Leader: st.Leader, Term: st.Term, InstalledTerm: st.InstalledTerm, BoundaryTerm: st.BoundaryTerm}
}
func channelElectionConfig(cfg wkdb.ChannelClusterConfig) rt.Config {
	return rt.Config{Replicas: cfg.Replicas, Learners: cfg.Learners, MigrateFrom: cfg.MigrateFrom, MigrateTo: cfg.MigrateTo, Leader: cfg.LeaderId, Term: cfg.Term, Version: cfg.ConfVersion, InstalledTerm: cfg.InstalledTerm, BoundaryTerm: cfg.BoundaryTerm}
}

func (s *Server) electionFence(ctx context.Context, req electionFenceRequest) (rt.ElectionReport, error) {
	if req.Slot != nil && req.Channel == nil {
		current := s.cfgServer.AppliedSlot(req.Slot.Id)
		if current == nil || !proto.Equal(current, req.Slot) {
			return rt.ElectionReport{}, errors.New("slot fence expected identity is not applied")
		}
		return s.slotServer.FenceElection(ctx, req.Slot, req.Term)
	}
	if req.Channel != nil && req.Slot == nil {
		current, err := s.fencedChannelClusterConfig(req.Channel.ChannelId, req.Channel.ChannelType)
		if err != nil {
			return rt.ElectionReport{}, err
		}
		if !current.Equal(*req.Channel) {
			return rt.ElectionReport{}, errors.New("channel fence expected identity is not applied")
		}
		return s.channelServer.FenceElection(ctx, current, req.Term)
	}
	return rt.ElectionReport{}, errors.New("invalid election fence request")
}

func (r *rpcServer) handleElectionFence(c *wkserver.Context) {
	var req electionFenceRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(err)
		return
	}
	ctx, cancel := context.WithTimeout(r.s.cancelCtx, 4*time.Second)
	defer cancel()
	report, err := r.s.electionFence(ctx, req)
	if err != nil {
		c.WriteErr(err)
		return
	}
	b, err := json.Marshal(report)
	if err != nil {
		c.WriteErr(err)
		return
	}
	c.Write(b)
}

func (s *Server) requestElectionFences(ctx context.Context, cfg rt.Config, req electionFenceRequest) (map[uint64]rt.ElectionReport, error) {
	if cfg.Term == math.MaxUint32 {
		return nil, errors.New("election term exhausted")
	}
	budget := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/2)
	}
	if budget <= 0 {
		return nil, context.DeadlineExceeded
	}
	probeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	req.Term = max(req.Term, cfg.Term+1)
	reports := make(map[uint64]rt.ElectionReport)
	var mu sync.Mutex
	var group errgroup.Group
	seen := make(map[uint64]bool)
	for _, id := range cfg.Replicas {
		if id == 0 || seen[id] {
			continue
		}
		seen[id] = true
		group.Go(func() error {
			var report rt.ElectionReport
			var err error
			if id == s.opts.ConfigOptions.NodeId {
				report, err = s.electionFence(probeCtx, req)
			} else {
				var b, body []byte
				b, err = json.Marshal(req)
				if err == nil {
					body, err = s.rpcClient.requestWithContext(probeCtx, id, "/rpc/election/fence", b)
				}
				if err == nil {
					err = json.Unmarshal(body, &report)
				}
			}
			if err == nil && report.NodeID == id {
				mu.Lock()
				reports[id] = report
				mu.Unlock()
			}
			return nil
		})
	}
	_ = group.Wait()
	return reports, ctx.Err()
}
