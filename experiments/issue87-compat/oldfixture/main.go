// Command oldfixture creates only new, disposable legacy fixtures. Build this
// driver inside an exact baseline source export, without issue87compat tags.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

var sourceRevision string

const baseline = "bdfbce6951787f58706e1ce15f0cfb1bf02252df"

type channelView struct {
	ID            string
	Type          uint8
	Messages      []wkdb.Message
	Subscribers   []wkdb.Member
	Conversations []wkdb.Conversation
	Config        wkdb.ChannelClusterConfig
	Applied       uint64
	HardState     types.HardState
	Terms         map[uint32]uint64
	LastTerm      uint32
}

type rawRow struct {
	Shard int
	Key   []byte
	Value []byte
}

type report struct {
	RawRows   []rawRow
	Baseline  string
	Channels  []channelView
	SlotLogs  []types.Log
	SlotState types.RaftState
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if sourceRevision != baseline {
		panic("build driver from verified pinned source with sourceRevision supplied")
	}
	root := flag.String("root", "", "owned fresh fixture directory")
	mode := flag.String("mode", "create", "create or inspect")
	profile := flag.String("profile", "normal", "normal, lagging, divergent, maximum, or damaged")
	output := flag.String("report", "", "JSON report path")
	flag.Parse()
	if *root == "" || *output == "" {
		panic("root and report are required")
	}
	if *profile != "normal" && *profile != "lagging" && *profile != "divergent" && *profile != "maximum" && *profile != "damaged" && *profile != "single" && *profile != "malformed" && *profile != "applied-tail" {
		panic("unknown profile")
	}
	marker := filepath.Join(*root, "DISPOSABLE-ISSUE87-FIXTURE")
	if *mode == "create" {
		entries, err := os.ReadDir(*root)
		check(err)
		if len(entries) != 0 {
			panic("fixture root must be empty; refusing existing data")
		}
		check(os.WriteFile(marker, []byte(baseline), 0600))
	} else if *mode == "inspect" {
		data, err := os.ReadFile(marker)
		check(err)
		if string(data) != baseline {
			panic("fixture ownership mismatch")
		}
	} else {
		panic("unknown mode")
	}
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(*root), wkdb.WithNodeId(1), wkdb.WithShardNum(4)))
	check(db.Open())
	logs := slot.NewPebbleShardLogStorage(nil, filepath.Join(*root, "slot"), 2)
	check(logs.Open())
	views := []channelView{{ID: "shared", Type: 2}, {ID: "shared", Type: 3}, {ID: "other", Type: 2}}
	now := time.Unix(1700000000, 123000000)
	if *mode == "create" {
		for n, c := range views {
			k := wkutil.ChannelToKey(c.ID, c.Type)
			cfg := wkdb.ChannelClusterConfig{ChannelId: c.ID, ChannelType: c.Type, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}, LeaderId: 1, Term: 8, ConfVersion: 12, CreatedAt: &now, UpdatedAt: &now}
			if *profile == "single" {
				cfg.Replicas = []uint64{1}
				cfg.ReplicaMaxCount = 1
			}
			check(db.SaveChannelClusterConfig(cfg))
			check(db.AddSubscribers(c.ID, c.Type, []wkdb.Member{{Uid: "alice", CreatedAt: &now, UpdatedAt: &now}, {Uid: "bob", CreatedAt: &now, UpdatedAt: &now}}))
			check(db.AddOrUpdateConversationsWithUser("alice", []wkdb.Conversation{{Id: uint64(n + 1), Uid: "alice", Type: wkdb.ConversationTypeChat, ChannelId: c.ID, ChannelType: c.Type, UnreadCount: 3, ReadToMsgSeq: 1, CreatedAt: &now, UpdatedAt: &now}}))
			var msgs []wkdb.Message
			tail := 4
			if *profile == "lagging" {
				tail = 2
			}
			for i := 1; i <= tail; i++ {
				term := uint64(7)
				if i > 2 {
					term = 8
				}
				payload := fmt.Sprintf("legacy/%s/%d/%d", c.ID, c.Type, i)
				messageID := int64(1000 + n*100 + i)
				if *profile == "divergent" && i == 4 {
					term = 9
					payload = "divergent-tail"
					messageID += 10000
				}
				if *profile == "maximum" && i == 4 {
					term = math.MaxUint32
				}
				msgs = append(msgs, wkdb.Message{RecvPacket: wkproto.RecvPacket{Framer: wkproto.Framer{FrameType: wkproto.RECV}, MessageID: messageID, MessageSeq: uint32(i), ClientMsgNo: fmt.Sprintf("c%d-%d", n, i), ChannelID: c.ID, ChannelType: c.Type, FromUID: "alice", Timestamp: int32(now.Unix()), Payload: []byte(payload)}, Term: term})
			}
			if *profile == "malformed" {
				for i := range msgs {
					msgs[i].Framer = wkproto.Framer{}
				}
			}
			check(db.AppendMessages(c.ID, c.Type, msgs))
			check(db.SetLeaderTermStartIndex(k, 7, 1))
			if tail > 2 {
				check(db.SetLeaderTermStartIndex(k, 8, 3))
			}
			if *profile == "divergent" {
				check(db.SetLeaderTermStartIndex(k, 9, 4))
			}
			if *profile == "maximum" {
				check(db.SetLeaderTermStartIndex(k, math.MaxUint32, 4))
			}
			applied := uint64(2)
			if *profile == "applied-tail" {
				applied = uint64(tail)
			}
			if *profile == "lagging" {
				applied = 1
			}
			if *profile == "damaged" {
				applied = 100
			}
			check(db.UpdateChannelAppliedIndex(c.ID, c.Type, applied))
			check(db.SaveRaftHardState(k, types.HardState{Term: 8, Vote: 1}))
		}
		check(logs.AppendLogs("1", []types.Log{{Id: 5001, Index: 1, Term: 7, Data: []byte("slot-one")}, {Id: 5002, Index: 2, Term: 7, Data: []byte("slot-two")}}, &types.TermStartIndexInfo{Term: 7, Index: 1}))
		check(logs.SetAppliedIndex("1", 1))
		check(logs.SaveHardState("1", types.HardState{Term: 8, Vote: 1}))
	}
	// Read the report from a cold old-reader instance, matching a real reopen.
	check(db.Close())
	db = wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(*root), wkdb.WithNodeId(1), wkdb.WithShardNum(4)))
	check(db.Open())
	result := report{Baseline: baseline}
	for _, c := range views {
		var err error
		c.Messages, err = db.LoadNextRangeMsgs(c.ID, c.Type, 1, 0, 100)
		check(err)
		c.Subscribers, err = db.GetSubscribers(c.ID, c.Type)
		check(err)
		sort.Slice(c.Subscribers, func(i, j int) bool { return c.Subscribers[i].Uid < c.Subscribers[j].Uid })
		all, err := db.GetConversations("alice")
		check(err)
		for _, v := range all {
			if v.ChannelId == c.ID && v.ChannelType == c.Type {
				c.Conversations = append(c.Conversations, v)
			}
		}
		c.Config, err = db.GetChannelClusterConfig(c.ID, c.Type)
		check(err)
		c.Applied, err = db.GetChannelAppliedIndex(c.ID, c.Type)
		check(err)
		c.HardState, err = db.RaftHardState(wkutil.ChannelToKey(c.ID, c.Type))
		check(err)
		c.Terms = make(map[uint32]uint64)
		for _, term := range []uint32{7, 8, 9, math.MaxUint32} {
			c.Terms[term], err = db.LeaderTermStartIndex(wkutil.ChannelToKey(c.ID, c.Type), term)
			check(err)
		}
		c.LastTerm, err = db.LeaderLastTerm(wkutil.ChannelToKey(c.ID, c.Type))
		check(err)
		result.Channels = append(result.Channels, c)
	}
	var err error
	result.SlotLogs, err = logs.GetLogs("1", 1, 3, 0)
	check(err)
	result.SlotState, err = logs.GetState("1")
	check(err)
	check(logs.Close())
	check(db.Close())
	for shard := 0; shard < 4; shard++ {
		raw, err := pebble.Open(filepath.Join(*root, "wukongimdb", fmt.Sprintf("shard%03d", shard)), &pebble.Options{ReadOnly: true})
		check(err)
		it := raw.NewIter(nil)
		for it.First(); it.Valid(); it.Next() {
			result.RawRows = append(result.RawRows, rawRow{Shard: shard, Key: append([]byte(nil), it.Key()...), Value: append([]byte(nil), it.Value()...)})
		}
		check(it.Error())
		check(it.Close())
		check(raw.Close())
	}
	data, err := json.MarshalIndent(result, "", "  ")
	check(err)
	check(os.WriteFile(*output, data, 0600))
}
