package wkdb

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/cockroachdb/pebble"
)

// AppendChannelNoop durably stores a barrier and its term boundary together.
// The barrier consumes a Raft index but never creates a message or client index.
func (wk *wukongDB) AppendChannelNoop(shardNo string, log types.Log) error {
	if !types.IsNoop(log) || log.Index == 0 || log.Index > math.MaxUint32 || log.Term == 0 {
		return fmt.Errorf("invalid channel replication barrier")
	}
	lock := wk.raftLock(shardNo)
	lock.Lock()
	defer lock.Unlock()
	hard, err := wk.RaftHardState(shardNo)
	if err != nil {
		return err
	}
	id, typ := wkutil.ChannelFromlKey(shardNo)
	applied, err := wk.GetChannelAppliedIndex(id, typ)
	if err != nil {
		return err
	}
	logs, err := wk.ChannelNoops(shardNo, log.Index, log.Index+1)
	if err != nil {
		return err
	}
	if len(logs) == 1 && logs[0].Term == log.Term {
		return nil
	}
	if log.Index <= max(applied, hard.CommittedFloor) || len(logs) > 0 {
		return fmt.Errorf("%w: barrier would replace retained history", types.ErrAppendBelowApplied)
	}
	msg, err := wk.LoadMsg(id, typ, log.Index)
	if err != nil && err != ErrNotFound {
		return err
	}
	if !IsEmptyMessage(msg) {
		return fmt.Errorf("%w: barrier would replace a message", types.ErrAppendBelowApplied)
	}
	batch := wk.shardDB(shardNo).NewBatch()
	defer batch.Close()
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, log.Term)
	if err := batch.Set(key.NewChannelNoopKey(shardNo, log.Index), data, nil); err != nil {
		return err
	}
	index := make([]byte, 8)
	wk.endian.PutUint64(index, log.Index)
	if err := batch.Set(key.NewLeaderTermSequenceTermKey(shardNo, log.Term), index, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

// ChannelNoops returns barriers in [start,end), independently of message scans.
func (wk *wukongDB) ChannelNoops(shardNo string, start, end uint64) ([]types.Log, error) {
	if end == 0 {
		end = math.MaxUint64
	}
	iter := wk.shardDB(shardNo).NewIter(&pebble.IterOptions{LowerBound: key.NewChannelNoopKey(shardNo, start), UpperBound: key.NewChannelNoopKey(shardNo, end)})
	defer iter.Close()
	var logs []types.Log
	for iter.First(); iter.Valid(); iter.Next() {
		log, err := decodeChannelNoop(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, iter.Error()
}

// LastChannelNoop supplies the Raft tail when no user message follows a barrier.
func (wk *wukongDB) LastChannelNoop(shardNo string) (types.Log, error) {
	iter := wk.shardDB(shardNo).NewIter(&pebble.IterOptions{LowerBound: key.NewChannelNoopKey(shardNo, 0), UpperBound: key.NewChannelNoopKey(shardNo, math.MaxUint64)})
	defer iter.Close()
	if !iter.Last() {
		return types.Log{}, iter.Error()
	}
	return decodeChannelNoop(iter.Key(), iter.Value())
}

func decodeChannelNoop(k, v []byte) (types.Log, error) {
	if len(k) < 8 || len(v) != 4 || binary.BigEndian.Uint32(v) == 0 {
		return types.Log{}, fmt.Errorf("invalid stored channel barrier")
	}
	return types.Log{Index: binary.BigEndian.Uint64(k[len(k)-8:]), Term: binary.BigEndian.Uint32(v), Data: types.NoopData()}, nil
}
