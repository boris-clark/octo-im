package cluster

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// saveChannelConfig keeps the existing slot log format, but fences the decision
// at admission. cfg.ConfVersion is the version being replaced (zero for create).
// All production metadata writers use this method on the current slot leader.
func (s *Server) saveChannelConfig(ctx context.Context, cfg wkdb.ChannelClusterConfig) (uint64, error) {
	applied, err := s.saveChannelConfigApplied(ctx, cfg)
	return applied.ConfVersion, err
}

// saveChannelConfigApplied returns the complete verified authority row. Admission
// may certify a different term/leader/install identity than the caller proposed.
func (s *Server) saveChannelConfigApplied(ctx context.Context, cfg wkdb.ChannelClusterConfig) (wkdb.ChannelClusterConfig, error) {
	key := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	if err := s.channelInstallLock.LockContext(ctx, key); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	defer s.channelInstallLock.Unlock(key)
	return s.saveChannelConfigLocked(ctx, cfg)
}

// The local guard spans fence persistence and slot apply, so another callback
// cannot mistake an in-flight fence for a crashed installation and supersede it.
// Distributed ownership still comes from the slot read barrier and applied CAS.
func (s *Server) saveChannelConfigLocked(ctx context.Context, cfg wkdb.ChannelClusterConfig) (wkdb.ChannelClusterConfig, error) {
	slotID := s.getSlotId(cfg.ChannelId)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		// A quarantined local replica fails before anything is proposed;
		// its read state could otherwise only end in retries.
		if err := s.slotServer.CheckSlotReadable(slotID); err != nil {
			return wkdb.EmptyChannelClusterConfig, err
		}
		before, err := s.slotServer.ReadLeaderState(ctx, slotID)
		if err != nil {
			return wkdb.EmptyChannelClusterConfig, err
		}
		if before.LeaderID != s.opts.ConfigOptions.NodeId || s.cfgServer.SlotLeaderId(slotID) != s.opts.ConfigOptions.NodeId {
			return wkdb.EmptyChannelClusterConfig, ErrConversationReadRetry
		}
		if before.Ready {
			current, err := s.fencedChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
			if err != nil && !errors.Is(err, wkdb.ErrNotFound) {
				return wkdb.EmptyChannelClusterConfig, err
			}
			if current.ConfVersion != cfg.ConfVersion {
				return wkdb.EmptyChannelClusterConfig, raft.ErrConfigVersionStale
			}
			if cfg.Expected != nil && !current.Equal(*cfg.Expected) {
				return wkdb.EmptyChannelClusterConfig, raft.ErrConfigVersionStale
			}
			if !wkdb.IsEmptyChannelClusterConfig(current) && !sameChannelMembership(current, cfg) {
				if err := s.certifyChannelMembership(ctx, current, &cfg); err != nil {
					return wkdb.EmptyChannelClusterConfig, err
				}
			}
			if !wkdb.IsEmptyChannelClusterConfig(current) && sameChannelMembership(current, cfg) {
				// Slot apply assigns a new ConfVersion even to an identical save.
				// That changes persistedFenceIdentity. Stop the OLD applied voters
				// and complete a new install before publishing any new version;
				// a partial fence must never be stranded by a metadata-only write.
				if cfg.Term < current.Term || current.Term == math.MaxUint32 ||
					cfg.Term == current.Term && cfg.LeaderId != current.LeaderId {
					return wkdb.EmptyChannelClusterConfig, raft.ErrElectionNotInstalled
				}
				certified := current.Clone()
				if _, err := s.fenceChannelConfig(ctx, &certified, max(cfg.Term, current.Term+1)); err != nil {
					return wkdb.EmptyChannelClusterConfig, err
				}
				if cfg.Term > current.Term && certified.LeaderId != cfg.LeaderId {
					return wkdb.EmptyChannelClusterConfig, ErrConversationReadRetry
				}
				cfg.Term, cfg.LeaderId = certified.Term, certified.LeaderId
				cfg.Expected = &current
				cfg.InstalledTerm, cfg.BoundaryTerm = certified.InstalledTerm, certified.BoundaryTerm
			}
			validate := func() error {
				if s.cfgServer.SlotLeaderId(slotID) != s.opts.ConfigOptions.NodeId {
					return ErrConversationReadRetry
				}
				current, err := s.fencedChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
				if err != nil && !errors.Is(err, wkdb.ErrNotFound) {
					return err
				}
				if current.ConfVersion != cfg.ConfVersion || cfg.Term < current.Term || cfg.Expected != nil && !current.Equal(*cfg.Expected) {
					return raft.ErrConfigVersionStale
				}
				return nil
			}
			// Reject already-stale decisions cheaply. Recheck after the fixed
			// admitted prefix applies, while holding proposal admission.
			if err := validate(); err != nil {
				return wkdb.EmptyChannelClusterConfig, err
			}
			data, err := cfg.Marshal()
			if err != nil {
				return wkdb.EmptyChannelClusterConfig, err
			}
			data, err = store.EncodeCMDChannelClusterConfigSave(cfg.ChannelId, cfg.ChannelType, data)
			if err != nil {
				return wkdb.EmptyChannelClusterConfig, err
			}
			cmd := store.NewCMD(store.CMDChannelClusterConfigSave, data)
			data, err = cmd.Marshal()
			if err != nil {
				return wkdb.EmptyChannelClusterConfig, err
			}
			version, err := s.slotServer.ProposeAfterRead(ctx, slotID, before, data, validate)
			if !errors.Is(err, raftgroup.ErrReadStateChanged) {
				if cfg.Expected != nil || err == nil {
					current, readErr := s.fencedChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
					desired := cfg.Clone()
					if desired.Id == 0 {
						desired.Id = current.Id
					}
					desired.ConfVersion = version
					// Bind readback to this admitted log index, including an
					// outcome-unknown proposal. A later authority is not our result.
					if readErr == nil && version > cfg.ConfVersion && current.Equal(desired) {
						return current, nil
					}
					if err == nil {
						err = raft.ErrConfigVersionStale
					}
				}
				return wkdb.EmptyChannelClusterConfig, err
			}
		}
		select {
		case <-ctx.Done():
			return wkdb.EmptyChannelClusterConfig, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) saveChannelConfigTimeout(cfg wkdb.ChannelClusterConfig) (wkdb.ChannelClusterConfig, error) {
	ctx, cancel := context.WithTimeout(s.cancelCtx, 5*time.Second)
	defer cancel()
	return s.saveChannelConfigApplied(ctx, cfg)
}
