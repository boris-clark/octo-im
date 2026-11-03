package raftgroup

import "context"

// InitializeRaft reserves storage ownership before build reads persistent
// state, then initializes and installs on the owner. A busy/draining key never
// invokes build. Cancellation or shutdown retains the reservation through the
// return of entered storage calls and collection of an accepted owner command.
// build must return a fresh, unstarted instance and must not call this group.
// initialize runs on the owner and has the same restrictions as Do.
func (rg *RaftGroup) InitializeRaft(ctx context.Context, key string, build func() (IRaft, error), initialize func(IRaft) error) (IRaft, error) {
	return rg.initializeRaft(ctx, key, func(_ *StorageBinding) (IRaft, error) { return build() }, initialize)
}

// InitializeRaftWithStorage passes the reservation's immutable binding to build.
// The binding remains private to this candidate through activation or failure.
func (rg *RaftGroup) InitializeRaftWithStorage(ctx context.Context, key string, build func(*StorageBinding) (IRaft, error), initialize func(IRaft) error) (IRaft, error) {
	return rg.initializeRaft(ctx, key, build, initialize)
}

func (rg *RaftGroup) initializeRaft(ctx context.Context, key string, build func(*StorageBinding) (IRaft, error), initialize func(IRaft) error) (IRaft, error) {
	if rg.stopBegun.Load() {
		return nil, ErrGroupStopped
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A fast refusal does not queue behind an owner held by an old call.
	rg.storageMu.Lock()
	old := rg.storageLeases[key]
	busy := old != nil && (!old.retired || old.pending != 0)
	rg.storageMu.Unlock()
	if busy {
		return nil, ErrRaftBusy
	}
	lease := &raftLease{pending: 1, storage: rg.opts.Storage}
	// Owner acknowledgement precedes reconstruction, including synchronous
	// Ready/HardState effects. No registry lock is held while awaiting it.
	err := rg.Do(ctx, key, func(current IRaft) error {
		if bs, ok := rg.opts.Storage.(BoundStorage); ok {
			if err := bs.ClaimStorageKey(rg, key); err != nil {
				return err
			}
		}
		rg.verificationMu.Lock()
		defer rg.verificationMu.Unlock()
		if rg.verificationStopped {
			return ErrGroupStopped
		}
		rg.storageMu.Lock()
		defer rg.storageMu.Unlock()
		old := rg.storageLeases[key]
		if current != nil || (old != nil && (!old.retired || old.pending != 0)) {
			return ErrRaftBusy
		}
		rg.storageLeases[key] = lease
		rg.poolTasks++
		return nil
	})
	if err != nil {
		return nil, err
	}
	installed := false
	defer func() {
		rg.storageMu.Lock()
		lease.pending--
		if !installed {
			lease.retired = true
		}
		rg.storageMu.Unlock()
		rg.poolTaskDone()
	}()
	if rg.stopBegun.Load() {
		return nil, ErrGroupStopped
	}
	candidate, err := build(&StorageBinding{group: rg, lease: lease, key: key})
	if err != nil {
		return nil, err
	}
	if old != nil && candidate == old.raft {
		return nil, ErrRaftBusy
	}
	if candidate == nil || candidate.Key() != key {
		return nil, ErrRaftNotExist
	}
	err = rg.Do(ctx, key, func(current IRaft) error {
		rg.storageMu.Lock()
		stopped := rg.stopBegun.Load()
		valid := !stopped && rg.storageLeases[key] == lease && current == nil
		if valid {
			lease.initializing = candidate
		}
		rg.storageMu.Unlock()
		if stopped {
			return ErrGroupStopped
		}
		if !valid {
			return ErrRaftBusy
		}
		if initialize != nil {
			if err := initialize(candidate); err != nil {
				rg.storageMu.Lock()
				lease.initializing = nil
				rg.storageMu.Unlock()
				return err
			}
		}
		// RemoveRaft cannot detach a never-published reservation. Shutdown may
		// fence here, so activation compares under the same registry lock.
		rg.storageMu.Lock()
		if rg.stopBegun.Load() {
			lease.initializing = nil
			rg.storageMu.Unlock()
			return ErrGroupStopped
		}
		rg.wait.removeKey(key, proposalOutcomeUnknown(ErrRaftNotExist))
		lease.initializing = nil
		lease.raft = candidate
		rg.raftList.push(candidate)
		installed = true
		rg.storageMu.Unlock()
		if rg.opts.Event != nil {
			rg.opts.Event.OnAddRaft(candidate)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	rg.Advance()
	return candidate, nil
}
