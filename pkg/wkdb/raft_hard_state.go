package wkdb

import (
	"errors"
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"hash/fnv"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

func (wk *wukongDB) SaveRaftHardState(shardNo string, state types.HardState) error {
	lock := wk.raftLock(shardNo)
	lock.Lock()
	defer lock.Unlock()
	old, err := wk.RaftHardState(shardNo)
	if err != nil {
		return err
	}
	if err = types.ValidateHardStateTransition(old, state); err != nil {
		return err
	}
	if state.CommittedFloor > old.CommittedFloor {
		id, typ := wkutil.ChannelFromlKey(shardNo)
		msg, err := wk.GetLastMsg(id, typ)
		if err != nil {
			return err
		}
		barrier, err := wk.LastChannelNoop(shardNo)
		if err != nil {
			return err
		}
		if state.CommittedFloor > max(uint64(msg.MessageSeq), barrier.Index) {
			return fmt.Errorf("commit floor beyond stored channel tail")
		}
	}
	return wk.shardDB(shardNo).Set(key.NewRaftHardStateKey(shardNo), state.Marshal(), pebble.Sync)
}

func (wk *wukongDB) RaftHardState(shardNo string) (types.HardState, error) {
	var state types.HardState
	data, closer, err := wk.shardDB(shardNo).Get(key.NewRaftHardStateKey(shardNo))
	if errors.Is(err, pebble.ErrNotFound) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	defer closer.Close()
	err = state.Unmarshal(data)
	return state, err
}

func (wk *wukongDB) raftLock(key string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &wk.raftLocks[h.Sum32()%uint32(len(wk.raftLocks))]
}
