package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"math"
	"slices"
	"sync"
	"time"
)

type electionFenceRequest struct {
	// IncludeLearners requests stored-tail witnesses only for a membership install.
	IncludeLearners bool
	Slot            *pb.Slot
	Channel         *wkdb.ChannelClusterConfig
	Term            uint32
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
			actual := rt.Config{}
			if current != nil {
				actual = slotElectionConfig(current)
			}
			return rt.ElectionReport{}, raft.NewElectionFenceError("applied-identity", slotElectionConfig(req.Slot), actual, req.Term, raft.ErrConfigVersionStale)
		}
		return s.slotServer.FenceElection(ctx, req.Slot, req.Term)
	}
	if req.Channel != nil && req.Slot == nil {
		current, err := s.fencedChannelClusterConfig(req.Channel.ChannelId, req.Channel.ChannelType)
		if err != nil {
			return rt.ElectionReport{}, err
		}
		if !current.Equal(*req.Channel) {
			return rt.ElectionReport{}, raft.NewElectionFenceError("applied-identity", channelElectionConfig(*req.Channel), channelElectionConfig(current), req.Term, raft.ErrConfigVersionStale)
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
		var refusal *raft.ElectionFenceError
		if errors.As(err, &refusal) {
			c.WriteErr(refusal)
		} else {
			// Never put native storage paths or transport addresses on the wire.
			c.WriteErr(errors.New("election fence phase=receiver"))
		}
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
	targets := append([]uint64(nil), cfg.Replicas...)
	if req.IncludeLearners {
		targets = append(targets, cfg.Learners...)
	}
	return s.requestElectionFencesFrom(ctx, cfg, req, targets)
}

func (s *Server) requestElectionFencesFrom(ctx context.Context, cfg rt.Config, req electionFenceRequest, targets []uint64) (map[uint64]rt.ElectionReport, error) {
	result, err := s.collectElectionFencesFrom(ctx, cfg, req, targets)
	if refused := result.refusalError(); refused != nil {
		s.Debug("election fence refusals", zap.Error(refused))
	}
	return result.reports, err
}

// A failed quorum retains its voter evidence without making a single failed
// peer fatal to an otherwise sufficient quorum. Summaries have a fixed bound.
type electionFenceCollection struct {
	reports  map[uint64]rt.ElectionReport
	refusals map[uint64]error
}

func (c electionFenceCollection) refusalError() error {
	ids := make([]uint64, 0, len(c.refusals))
	for id := range c.refusals {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var failures []error
	for _, id := range ids[:min(len(ids), 16)] {
		failures = append(failures, c.refusals[id])
	}
	if len(ids) > 16 {
		failures = append(failures, fmt.Errorf("%d further voter refusals omitted", len(ids)-16))
	}
	return errors.Join(failures...)
}

func (s *Server) collectElectionFences(ctx context.Context, cfg rt.Config, req electionFenceRequest) (electionFenceCollection, error) {
	targets := append([]uint64(nil), cfg.Replicas...)
	if req.IncludeLearners {
		targets = append(targets, cfg.Learners...)
	}
	return s.collectElectionFencesFrom(ctx, cfg, req, targets)
}

func (s *Server) collectElectionFencesFrom(ctx context.Context, cfg rt.Config, req electionFenceRequest, targets []uint64) (electionFenceCollection, error) {
	result := electionFenceCollection{reports: make(map[uint64]rt.ElectionReport), refusals: make(map[uint64]error)}
	if cfg.Term == math.MaxUint32 {
		return result, errors.New("election term exhausted")
	}
	budget := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/2)
	}
	if budget <= 0 {
		return result, context.DeadlineExceeded
	}
	probeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	req.Term = max(req.Term, cfg.Term+1)
	var mu sync.Mutex
	var group errgroup.Group
	seen := make(map[uint64]bool)
	for _, id := range targets {
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
			if err == nil && (report.NodeID != id || report.Identity != rt.ElectionIdentity(cfg) || report.Term != req.Term || !report.Fenced) {
				err = errors.New("invalid fence report")
			}
			mu.Lock()
			if err == nil {
				result.reports[id] = report
			} else {
				phase := "transport"
				detail := "unavailable"
				var ownerError *raft.ElectionFenceError
				var rpcError *fenceRPCError
				switch {
				case errors.As(err, &ownerError):
					phase, detail = "receiver", ownerError.Error()
				case errors.As(err, &rpcError):
					phase, detail = "receiver", rpcError.Error()
				case errors.Is(err, context.DeadlineExceeded):
					detail = "deadline"
				case errors.Is(err, context.Canceled):
					detail = "canceled"
				case id == s.opts.ConfigOptions.NodeId:
					phase, detail = "receiver", "unavailable"
				case err.Error() == "invalid fence report":
					phase, detail = "report", "wrong identity, term, voter or durable fence"
				}
				result.refusals[id] = fmt.Errorf("voter=%d phase=%s expected_term=%d requested_term=%d expected_version=%d expected_identity=%x: %s", id, phase, cfg.Term, req.Term, cfg.Version, rt.ElectionIdentity(cfg), detail)
			}
			mu.Unlock()
			return nil
		})
	}
	_ = group.Wait()
	return result, ctx.Err()
}
