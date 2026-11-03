package wkdb

import (
	"fmt"
	"math"

	"github.com/cockroachdb/pebble"
)

func (wk *wukongDB) SetLeaderTermStartIndex(k string, term uint32, index uint64) error {
	wk.metrics.SetLeaderTermStartIndexAdd(1)
	wk.logMetadataMu.Lock()
	defer wk.logMetadataMu.Unlock()
	db, canonical, err := wk.channelLogDBLocked(k)
	if err != nil {
		return err
	}
	value := make([]byte, 8)
	wk.endian.PutUint64(value, index)
	return db.Set(termKey(k, term, canonical), value, pebble.Sync)
}

func (wk *wukongDB) LeaderLastTerm(k string) (uint32, error) {
	wk.metrics.LeaderLastTermAdd(1)
	wk.logMetadataMu.Lock()
	defer wk.logMetadataMu.Unlock()
	db, canonical, err := wk.channelLogDBLocked(k)
	if err != nil {
		return 0, err
	}
	low, high := termBounds(k, canonical)
	it := db.NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: high})
	defer it.Close()
	if it.Last() {
		return wk.endian.Uint32(it.Key()[len(it.Key())-4:]), nil
	}
	return 0, it.Error()
}

func (wk *wukongDB) LeaderTermStartIndex(k string, term uint32) (uint64, error) {
	wk.metrics.LeaderTermStartIndexAdd(1)
	wk.logMetadataMu.Lock()
	defer wk.logMetadataMu.Unlock()
	db, canonical, err := wk.channelLogDBLocked(k)
	if err != nil {
		return 0, err
	}
	return wk.readTermIndex(db, termKey(k, term, canonical))
}

func (wk *wukongDB) readTermIndex(db *pebble.DB, k []byte) (uint64, error) {
	v, closer, err := db.Get(k)
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, fmt.Errorf("invalid channel term row")
	}
	return wk.endian.Uint64(v), nil
}

func (wk *wukongDB) LeaderLastTermGreaterEqThan(k string, term uint32) (uint32, error) {
	wk.metrics.LeaderLastTermGreaterThanAdd(1)
	wk.logMetadataMu.Lock()
	defer wk.logMetadataMu.Unlock()
	db, canonical, err := wk.channelLogDBLocked(k)
	if err != nil {
		return 0, err
	}
	_, high := termBounds(k, canonical)
	it := db.NewIter(&pebble.IterOptions{LowerBound: termKey(k, term, canonical), UpperBound: high})
	defer it.Close()
	if it.First() {
		return wk.endian.Uint32(it.Key()[len(it.Key())-4:]), nil
	}
	return term, it.Error()
}

func (wk *wukongDB) DeleteLeaderTermStartIndexGreaterThanTerm(k string, term uint32) error {
	wk.metrics.DeleteLeaderTermStartIndexGreaterThanTermAdd(1)
	wk.logMetadataMu.Lock()
	defer wk.logMetadataMu.Unlock()
	db, canonical, err := wk.channelLogDBLocked(k)
	if err != nil {
		return err
	}
	if term == math.MaxUint32 {
		return nil
	}
	_, high := termBounds(k, canonical)
	return db.DeleteRange(termKey(k, term+1, canonical), high, pebble.Sync)
}
