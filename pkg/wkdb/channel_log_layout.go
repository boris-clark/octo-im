package wkdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

var channelLogRoutingKey = []byte{0x1B, 0x05, 4, 0}

// channelLogDBLocked adopts legacy term/barrier metadata into the message shard.
// The source stays intact. One synced target batch publishes every copied row
// and the routing marker, so interruption leaves either the legacy layout or
// the complete canonical layout. Callers hold logMetadataMu; old writers must
// be stopped before adoption and cannot be used for rollback afterward.
func (wk *wukongDB) channelLogDBLocked(k string) (*pebble.DB, bool, error) {
	parts := strings.SplitN(k, "&", 2)
	if len(parts) != 2 || parts[1] == "" {
		return wk.shardDB(k), false, nil
	}
	typ, err := strconv.ParseUint(parts[0], 10, 8)
	if err != nil || strconv.FormatUint(typ, 10) != parts[0] {
		return wk.shardDB(k), false, nil
	}
	route := binary.BigEndian.AppendUint32([]byte{1}, wk.shardNum)
	v, closer, err := wk.dbs[0].Get(channelLogRoutingKey)
	if err == nil {
		valid := bytes.Equal(v, route)
		closer.Close()
		if !valid {
			return nil, false, fmt.Errorf("channel log shard routing changed")
		}
	} else if errors.Is(err, pebble.ErrNotFound) {
		if err = wk.dbs[0].Set(channelLogRoutingKey, route, pebble.Sync); err != nil {
			return nil, false, err
		}
	} else {
		return nil, false, err
	}
	db := wk.channelDb(parts[1], uint8(typ))
	marker := binary.BigEndian.AppendUint32(bytes.Clone(route), wk.shardId(k))
	marker = binary.BigEndian.AppendUint32(marker, wk.channelDbIndex(parts[1], uint8(typ)))
	v, closer, err = db.Get(key.NewChannelLogLayoutKey(k))
	if err == nil {
		valid := bytes.Equal(v, marker)
		closer.Close()
		if !valid {
			return nil, false, fmt.Errorf("invalid channel log layout marker")
		}
		return db, true, nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return nil, false, err
	}
	lowCanonical, highCanonical := termBounds(k, true)
	probe := db.NewIter(&pebble.IterOptions{LowerBound: lowCanonical, UpperBound: highCanonical})
	exists := probe.First()
	probeErr := probe.Error()
	probe.Close()
	if probeErr != nil {
		return nil, false, probeErr
	}
	if exists {
		return nil, false, fmt.Errorf("canonical channel terms have no layout marker")
	}
	batch := db.NewBatch()
	defer batch.Close()
	source := wk.shardDB(k)
	low := key.NewLeaderTermSequenceTermKey(k, 0)
	high := prefixEnd(low[:len(low)-4])
	it := source.NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: high})
	for it.First(); it.Valid(); it.Next() {
		term, parseErr := key.ParseLeaderTermSequenceTermKey(it.Key())
		if parseErr != nil || len(it.Value()) != 8 {
			it.Close()
			return nil, false, fmt.Errorf("malformed legacy term metadata")
		}
		if err = batch.Set(key.NewChannelTermKey(k, term), it.Value(), nil); err != nil {
			it.Close()
			return nil, false, err
		}
	}
	err = it.Error()
	it.Close()
	if err != nil {
		return nil, false, err
	}
	it = source.NewIter(&pebble.IterOptions{LowerBound: key.NewChannelNoopKey(k, 0), UpperBound: key.NewChannelNoopKey(k, math.MaxUint64)})
	for it.First(); it.Valid(); it.Next() {
		if _, err = decodeChannelNoop(it.Key(), it.Value()); err != nil {
			it.Close()
			return nil, false, err
		}
		if source != db {
			if err = batch.Set(it.Key(), it.Value(), nil); err != nil {
				it.Close()
				return nil, false, err
			}
		}
	}
	err = it.Error()
	it.Close()
	if err != nil {
		return nil, false, err
	}
	if err = batch.Set(key.NewChannelLogLayoutKey(k), marker, nil); err != nil {
		return nil, false, err
	}
	if err = batch.Commit(pebble.Sync); err != nil {
		return nil, false, err
	}
	return db, true, nil
}

func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 255 {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func termKey(k string, term uint32, canonical bool) []byte {
	if canonical {
		return key.NewChannelTermKey(k, term)
	}
	return key.NewLeaderTermSequenceTermKey(k, term)
}

func termBounds(k string, canonical bool) ([]byte, []byte) {
	low := termKey(k, 0, canonical)
	return low, prefixEnd(low[:len(low)-4])
}
