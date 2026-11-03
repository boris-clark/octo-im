package wkdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// syncCutFS retains only WAL syncs up to a selected boundary. Closing while
// ignoring later syncs and resetting StrictMem models a loss of unsynced writes.
type syncCutFS struct {
	vfs.FS
	mem       *vfs.MemFS
	remaining atomic.Int64
	syncs     atomic.Int64
	armed     atomic.Bool
}
type syncCutFile struct {
	vfs.File
	owner *syncCutFS
	wal   bool
}

func (f *syncCutFS) wrap(name string, file vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &syncCutFile{File: file, owner: f, wal: strings.HasSuffix(name, ".log")}, nil
}
func (f *syncCutFS) Create(name string) (vfs.File, error) {
	x, e := f.FS.Create(name)
	return f.wrap(name, x, e)
}
func (f *syncCutFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	x, e := f.FS.Open(name, opts...)
	return f.wrap(name, x, e)
}
func (f *syncCutFile) Sync() error {
	err := f.File.Sync()
	if err == nil && f.wal && f.owner.armed.Load() {
		f.owner.syncs.Add(1)
		if f.owner.remaining.Add(-1) == 0 {
			f.owner.mem.SetIgnoreSyncs(true)
		}
	}
	return err
}
func (f *syncCutFile) SyncData() error            { return f.Sync() }
func (f *syncCutFile) SyncTo(int64) (bool, error) { return true, f.Sync() }
func (f *syncCutFS) arm(cut int64) {
	f.remaining.Store(cut)
	f.syncs.Store(0)
	f.armed.Store(true)
	if cut == 0 {
		f.mem.SetIgnoreSyncs(true)
	}
}
func (f *syncCutFS) crash(t *testing.T, db *wukongDB) {
	t.Helper()
	f.mem.SetIgnoreSyncs(true)
	require.NoError(t, db.Close())
	f.armed.Store(false)
	f.mem.ResetToSyncedState()
	f.mem.SetIgnoreSyncs(false)
}
func atomicDB(t *testing.T, fs vfs.FS) *wukongDB {
	t.Helper()
	db := NewWukongDB(NewOptions(WithDir("/atomic"), WithShardNum(4), WithFS(fs), WithMemTableSize(1<<20))).(*wukongDB)
	require.NoError(t, db.Open())
	// Pebble syncs its own directory. The fixture owns and must sync its newly
	// created ancestor directories before simulating a later power loss.
	for _, name := range []string{"/", "/atomic", "/atomic/wukongimdb"} {
		directory, e := fs.OpenDir(name)
		require.NoError(t, e)
		require.NoError(t, directory.Sync())
		require.NoError(t, directory.Close())
	}
	return db
}
func crossShardChannel(db *wukongDB) string {
	for i := 0; ; i++ {
		id := fmt.Sprintf("cross-%d&retained", i)
		if db.shardId(wkutil.ChannelToKey(id, 2)) != db.channelDbIndex(id, 2) {
			return id
		}
	}
}
func atomicMessage(id string, typ uint8, seq uint32, term uint64) Message {
	return Message{RecvPacket: wkproto.RecvPacket{Framer: wkproto.Framer{FrameType: wkproto.RECV}, ChannelID: id, ChannelType: typ, MessageID: int64(100 + seq), MessageSeq: seq, FromUID: "alice", ClientMsgNo: fmt.Sprintf("key-%d", seq), Timestamp: 10, Payload: []byte(fmt.Sprintf("payload-%d", seq))}, Term: term}
}
func legacyTerms(t *testing.T, db *wukongDB, k string, rows map[uint32]uint64) map[string][]byte {
	t.Helper()
	before := map[string][]byte{}
	for term, index := range rows {
		raw := binary.BigEndian.AppendUint64(nil, index)
		rawKey := key.NewLeaderTermSequenceTermKey(k, term)
		require.NoError(t, db.shardDB(k).Set(rawKey, raw, pebble.Sync))
		before[string(rawKey)] = raw
	}
	return before
}
func assertLegacyTerms(t *testing.T, db *wukongDB, k string, before map[string][]byte) {
	t.Helper()
	for rawKey, want := range before {
		v, c, e := db.shardDB(k).Get([]byte(rawKey))
		require.NoError(t, e)
		require.Equal(t, want, bytes.Clone(v))
		require.NoError(t, c.Close())
	}
}

func TestChannelLogCanonicalAllTermOperationsAndReopen(t *testing.T) {
	fs := vfs.NewMem()
	db := atomicDB(t, fs)
	id := crossShardChannel(db)
	originals := map[string]map[string][]byte{}
	for _, typ := range []uint8{2, 3} {
		k := wkutil.ChannelToKey(id, typ)
		originals[k] = legacyTerms(t, db, k, map[uint32]uint64{7: 1, 8: 3, math.MaxUint32: 4})
		// No new fields are required to read an ordinary old store.
		index, e := db.LeaderTermStartIndex(k, 7)
		require.NoError(t, e)
		require.EqualValues(t, 1, index)
		last, e := db.LeaderLastTerm(k)
		require.NoError(t, e)
		require.EqualValues(t, math.MaxUint32, last)
		next, e := db.LeaderLastTermGreaterEqThan(k, 9)
		require.NoError(t, e)
		require.EqualValues(t, math.MaxUint32, next)
		require.NoError(t, db.DeleteLeaderTermStartIndexGreaterThanTerm(k, math.MaxUint32))
		require.NoError(t, db.DeleteLeaderTermStartIndexGreaterThanTerm(k, 8))
		index, e = db.LeaderTermStartIndex(k, math.MaxUint32)
		require.NoError(t, e)
		require.Zero(t, index)
		require.NoError(t, db.SetLeaderTermStartIndex(k, 9, 5))
		last, e = db.LeaderLastTerm(k)
		require.NoError(t, e)
		require.EqualValues(t, 9, last)
		assertLegacyTerms(t, db, k, originals[k])
	}
	require.NoError(t, db.Close())
	db = atomicDB(t, fs)
	defer db.Close()
	for k, original := range originals {
		last, e := db.LeaderLastTerm(k)
		require.NoError(t, e)
		require.EqualValues(t, 9, last)
		assertLegacyTerms(t, db, k, original)
	}
	// Non-channel callers retain their original routing and maximum-term support.
	require.NoError(t, db.SetLeaderTermStartIndex("slot", math.MaxUint32, 12))
	last, e := db.LeaderLastTerm("slot")
	require.NoError(t, e)
	require.EqualValues(t, math.MaxUint32, last)
	require.NoError(t, db.DeleteLeaderTermStartIndexGreaterThanTerm("slot", math.MaxUint32))
}

func TestChannelLogAtomicAppendAndTruncateAcrossSyncBoundaries(t *testing.T) {
	for _, operation := range []string{"append", "truncate"} {
		for _, cut := range []int64{0, 1, 2} {
			t.Run(fmt.Sprintf("%s/cut-%d", operation, cut), func(t *testing.T) {
				mem := vfs.NewStrictMem()
				fs := &syncCutFS{FS: mem, mem: mem}
				db := atomicDB(t, fs)
				id := crossShardChannel(db)
				k := wkutil.ChannelToKey(id, 2)
				m1, m2 := atomicMessage(id, 2, 1, 7), atomicMessage(id, 2, 2, 8)
				require.NoError(t, db.AppendMessagesWithTerm(id, 2, []Message{m1}, &types.TermStartIndexInfo{Term: 7, Index: 1}))
				if operation == "truncate" {
					require.NoError(t, db.AppendMessagesWithTerm(id, 2, []Message{m2}, &types.TermStartIndexInfo{Term: 8, Index: 2}))
					require.NoError(t, db.AppendChannelNoop(k, types.Log{Index: 3, Term: 9, Data: types.NoopData()}))
				}
				fs.arm(cut)
				if operation == "append" {
					require.NoError(t, db.AppendMessagesWithTerm(id, 2, []Message{m2}, &types.TermStartIndexInfo{Term: 8, Index: 2}))
				} else {
					require.NoError(t, db.TruncateLogTo(id, 2, 1))
				}
				count := fs.syncs.Load()
				require.EqualValues(t, 1, count, "one WAL sync must publish both log and term changes")
				fs.crash(t, db)
				db = atomicDB(t, fs)
				defer db.Close()
				tail, _, e := db.GetChannelLastMessageSeq(id, 2)
				require.NoError(t, e)
				boundary, e := db.LeaderTermStartIndex(k, 8)
				require.NoError(t, e)
				barrier, e := db.LastChannelNoop(k)
				require.NoError(t, e)
				present := operation == "append" && cut > 0 || operation == "truncate" && cut == 0
				if present {
					require.EqualValues(t, 2, tail)
					require.EqualValues(t, 2, boundary)
					got, e := db.LoadMsg(id, 2, 2)
					require.NoError(t, e)
					require.Equal(t, m2.MessageID, got.MessageID)
					require.Equal(t, m2.Payload, got.Payload)
					if operation == "truncate" {
						require.EqualValues(t, 3, barrier.Index)
					}
				} else {
					require.EqualValues(t, 1, tail)
					require.Zero(t, boundary)
					require.Zero(t, barrier.Index)
					_, e := db.LoadMsg(id, 2, 2)
					require.ErrorIs(t, e, ErrNotFound)
				}
				got, e := db.LoadMsg(id, 2, 1)
				require.NoError(t, e)
				require.Equal(t, m1.Payload, got.Payload)
			})
		}
	}
}

func TestChannelLogAdoptionInterruptedAndOriginalRowsPreserved(t *testing.T) {
	for _, cut := range []int64{0, 1, 2} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			mem := vfs.NewStrictMem()
			fs := &syncCutFS{FS: mem, mem: mem}
			db := atomicDB(t, fs)
			id := crossShardChannel(db)
			k := wkutil.ChannelToKey(id, 2)
			original := legacyTerms(t, db, k, map[uint32]uint64{7: 1, 8: 3, math.MaxUint32: 4})
			// Seed the legacy barrier directly in its original metadata shard.
			require.NoError(t, db.shardDB(k).Set(key.NewChannelNoopKey(k, 5), binary.BigEndian.AppendUint32(nil, 9), pebble.Sync))
			fs.arm(cut)
			term, e := db.LeaderLastTerm(k)
			require.NoError(t, e)
			require.EqualValues(t, math.MaxUint32, term)
			fs.crash(t, db)
			db = atomicDB(t, fs)
			defer db.Close()
			// Lazy adoption after reopen must finish from intact original metadata.
			index, e := db.LeaderTermStartIndex(k, 8)
			require.NoError(t, e)
			require.EqualValues(t, 3, index)
			barrier, e := db.LastChannelNoop(k)
			require.NoError(t, e)
			require.EqualValues(t, 5, barrier.Index)
			assertLegacyTerms(t, db, k, original)
			require.NoError(t, db.DeleteLeaderTermStartIndexGreaterThanTerm(k, 7))
			assertLegacyTerms(t, db, k, original)
		})
	}
}

func TestChannelLogTruncateIndexesCacheAndProtectedFloor(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(4))).(*wukongDB)
	require.NoError(t, db.Open())
	defer db.Close()
	id := crossShardChannel(db)
	k := wkutil.ChannelToKey(id, 2)
	for i := uint32(1); i <= 3; i++ {
		m := atomicMessage(id, 2, i, uint64(i))
		require.NoError(t, db.AppendMessagesWithTerm(id, 2, []Message{m}, &types.TermStartIndexInfo{Term: i, Index: uint64(i)}))
	}
	seq, e := db.GetUserLastMsgSeq("alice", id, 2)
	require.NoError(t, e)
	require.EqualValues(t, 3, seq)
	require.NoError(t, db.UpdateChannelAppliedIndex(id, 2, 1))
	require.NoError(t, db.SaveRaftHardState(k, types.HardState{Term: 3, CommittedFloor: 1}))
	require.NoError(t, db.TruncateLogTo(id, 2, 1))
	seq, e = db.GetUserLastMsgSeq("alice", id, 2)
	require.NoError(t, e)
	require.EqualValues(t, 1, seq)
	for i := uint32(2); i <= 3; i++ {
		m := atomicMessage(id, 2, i, uint64(i))
		_, c, e := db.channelDb(id, 2).Get(key.NewMessageIndexMessageIdKey(uint64(m.MessageID)))
		if c != nil {
			c.Close()
		}
		require.ErrorIs(t, e, pebble.ErrNotFound)
	}
	require.NoError(t, db.AppendMessagesWithTerm(id, 2, []Message{atomicMessage(id, 2, 2, 4)}, &types.TermStartIndexInfo{Term: 4, Index: 2}))
	require.NoError(t, db.SaveRaftHardState(k, types.HardState{Term: 4, CommittedFloor: 2}))
	require.ErrorIs(t, db.TruncateLogTo(id, 2, 1), types.ErrTruncateBelowApplied)
	tail, _, e := db.GetChannelLastMessageSeq(id, 2)
	require.NoError(t, e)
	require.EqualValues(t, 2, tail)
}

func TestChannelLogRejectsAmbiguousCanonicalRowsAndRoutingChange(t *testing.T) {
	fs := vfs.NewMem()
	db := atomicDB(t, fs)
	id := crossShardChannel(db)
	k := wkutil.ChannelToKey(id, 2)
	require.NoError(t, db.channelDb(id, 2).Set(key.NewChannelTermKey(k, 8), binary.BigEndian.AppendUint64(nil, 99), pebble.Sync))
	_, e := db.LeaderLastTerm(k)
	require.ErrorContains(t, e, "no layout marker")
	require.NoError(t, db.Close())
	// A changed shard count must not make the routing marker appear absent.
	db = NewWukongDB(NewOptions(WithDir(filepath.Clean("/atomic")), WithShardNum(2), WithFS(fs))).(*wukongDB)
	require.NoError(t, db.Open())
	defer db.Close()
	_, e = db.LeaderLastTerm(k)
	require.ErrorContains(t, e, "routing changed")
}

func TestChannelLogNoopTruncationPreservesLegacyFence(t *testing.T) {
	db := atomicDB(t, vfs.NewMem())
	defer db.Close()
	id := crossShardChannel(db)
	k := wkutil.ChannelToKey(id, 2)
	require.NoError(t, db.AppendMessagesWithTerm(id, 2, []Message{atomicMessage(id, 2, 1, 1)}, &types.TermStartIndexInfo{Term: 1, Index: 1}))
	require.NoError(t, db.UpdateChannelAppliedIndex(id, 2, 1))
	require.NoError(t, db.SaveRaftHardState(k, types.HardState{Term: 2, BoundaryTerm: 1, CommittedFloor: 1}))
	require.NoError(t, db.TruncateLogTo(id, 2, 1), "an unchanged tail is not a legacy cut")
	h, e := db.RaftHardState(k)
	require.NoError(t, e)
	require.False(t, h.Quarantined)
	require.NoError(t, db.AppendMessagesWithTerm(id, 2, []Message{atomicMessage(id, 2, 2, 2)}, &types.TermStartIndexInfo{Term: 2, Index: 2}))
	require.ErrorIs(t, db.TruncateLogTo(id, 2, 1), types.ErrTruncateBelowApplied, "a real legacy-boundary cut remains prohibited")
}
