package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chEvent "github.com/WuKongIM/WuKongIM/internal/channel/event"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	clusterchannel "github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type realChannelClusterAdapter struct {
	icluster.ICluster
	icluster.RPC
	nodeId      uint64
	slotCount   uint32
	failChannel string
	peers       map[uint64]*realSeedClusterNode
	onSendFunc  func(toNodeId uint64, msg *proto.Message) error
}

type realChannelSlotAdapter struct {
	icluster.Slot
	adapter *realChannelClusterAdapter
}

func (s *realChannelSlotAdapter) GetSlotId(v string) uint32 {
	return s.adapter.GetSlotId(v)
}

func (s *realChannelSlotAdapter) SlotLeaderId(slotId uint32) uint64 {
	return s.adapter.SlotLeaderId(slotId)
}

type realChannelNodeAdapter struct {
	icluster.Node
	adapter *realChannelClusterAdapter
}

func (a *realChannelClusterAdapter) GetSlotId(v string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(v))
	if a.slotCount == 0 {
		return 0
	}
	return h.Sum32() % a.slotCount
}

func (a *realChannelClusterAdapter) SlotLeaderId(slotId uint32) uint64 {
	if len(a.peers) <= 1 {
		return a.nodeId
	}
	return uint64(1001 + (slotId % uint32(len(a.peers))))
}

func (a *realChannelClusterAdapter) leaderForChannel(channelId string) uint64 {
	if len(a.peers) <= 1 {
		return a.nodeId
	}
	slotId := a.GetSlotId(channelId)
	return uint64(1001 + (slotId % uint32(len(a.peers))))
}

func (a *realChannelClusterAdapter) GetOrCreateChannelClusterConfigFromSlotLeader(channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	if a.failChannel != "" && channelId == a.failChannel {
		return wkdb.EmptyChannelClusterConfig, errors.New("injected channel storage failure")
	}
	leaderId := a.leaderForChannel(channelId)
	return wkdb.ChannelClusterConfig{
		ChannelId:       channelId,
		ChannelType:     channelType,
		ReplicaMaxCount: 1,
		Replicas:        []uint64{leaderId},
		LeaderId:        leaderId,
		Term:            1,
		ConfVersion:     1,
	}, nil
}

func (a *realChannelClusterAdapter) SlotLeaderIdOfChannel(channelId string, channelType uint8) (uint64, error) {
	return a.leaderForChannel(channelId), nil
}

func (a *realChannelClusterAdapter) SlotLeaderOfChannel(channelId string, channelType uint8) (*clustertypes.Node, error) {
	id := a.leaderForChannel(channelId)
	return &clustertypes.Node{Id: id, Online: true}, nil
}

func (a *realChannelClusterAdapter) LeaderIdOfChannel(channelId string, channelType uint8) (uint64, error) {
	return a.leaderForChannel(channelId), nil
}

func (a *realChannelClusterAdapter) RequestChannelProposeBatchUntilApplied(ctx context.Context, toNodeId uint64, channelId string, channelType uint8, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	if a.failChannel != "" && channelId == a.failChannel {
		return nil, errors.New("injected channel storage failure")
	}
	peer, ok := a.peers[toNodeId]
	if !ok || peer == nil {
		return nil, fmt.Errorf("peer node %d not found", toNodeId)
	}
	return peer.chSrv.ProposeBatchUntilAppliedTimeoutForLocal(ctx, channelId, channelType, reqs)
}

func (a *realChannelClusterAdapter) Send(toNodeId uint64, msg *proto.Message) error {
	if a.onSendFunc != nil {
		return a.onSendFunc(toNodeId, msg)
	}
	return nil
}

type realSeedEventbusChannel struct {
	pool          *chEvent.EventPool
	inflight      atomic.Int64
	handledEvents atomic.Int64
	forwardedMsgs atomic.Int64
	adapter       *realChannelClusterAdapter
}

func (c *realSeedEventbusChannel) OnEvent(ctx *eventbus.ChannelContext) {
	if ctx == nil {
		return
	}
	n := int64(len(ctx.Events))
	if c.adapter != nil && ctx.SlotLeaderId != 0 && ctx.SlotLeaderId != c.adapter.nodeId && ctx.EventType == eventbus.EventChannelOnSend {
		if peer, ok := c.adapter.peers[ctx.SlotLeaderId]; ok && peer != nil {
			c.forwardedMsgs.Add(n)
			for _, ev := range ctx.Events {
				if ev != nil {
					peer.eb.SendMessage(ctx.ChannelId, ctx.ChannelType, ev.Clone())
				}
			}
			peer.eb.Advance(ctx.ChannelId, ctx.ChannelType)
			c.inflight.Add(-n)
			return
		}
	}
	c.handledEvents.Add(n)
	c.inflight.Add(-n)
}

func (c *realSeedEventbusChannel) SendMessage(channelId string, channelType uint8, event *eventbus.Event) {
	c.inflight.Add(1)
	c.pool.AddEvent(channelId, channelType, event)
}

func (c *realSeedEventbusChannel) AddEvent(channelId string, channelType uint8, event *eventbus.Event) {
	c.inflight.Add(1)
	c.pool.AddEvent(channelId, channelType, event)
}

func (c *realSeedEventbusChannel) Advance(channelId string, channelType uint8) {
	c.pool.Advance(channelId, channelType)
}

type realSeedClusterNode struct {
	nodeId  uint64
	wdb     wkdb.DB
	chSrv   *clusterchannel.Server
	store   *store.Store
	eb      *realSeedEventbusChannel
	adapter *realChannelClusterAdapter
}

type seedTierAccountingResult struct {
	TierLabel                  string  `json:"tier_label"`
	TargetMessages             int     `json:"target_messages"`
	AcceptedMessages           int     `json:"accepted_messages"`
	CallerRecordedMessageIDs   int     `json:"caller_recorded_message_ids"`
	CallerRecordedClientNos    int     `json:"caller_recorded_client_msg_nos"`
	ExplicitFailedMessages     int     `json:"explicit_failed_messages"`
	PersistedMessages          int     `json:"persisted_messages"`
	EventbusHandledEvents      int64   `json:"eventbus_handled_events"`
	PersistedEqualsAccepted    bool    `json:"persisted_equals_accepted"`
	UniquePersistedMessageIDs  int     `json:"unique_persisted_message_ids"`
	UniquePersistedClientNos   int     `json:"unique_persisted_client_msg_nos"`
	MissingAcceptedMessageIDs  int     `json:"missing_accepted_message_ids"`
	MissingAcceptedClientNos   int     `json:"missing_accepted_client_msg_nos"`
	DuplicateMessageIDs        int     `json:"duplicate_message_ids"`
	DuplicateClientMsgNos      int     `json:"duplicate_client_msg_nos"`
	SequenceGaps               int     `json:"sequence_gaps"`
	ChannelsVerified           int     `json:"channels_verified"`
	ElapsedSeconds             float64 `json:"elapsed_seconds"`
}

func runSeedTierAccounting(t *testing.T, tierLabel string, targetMsgs int, failCh string) seedTierAccountingResult {
	t.Helper()
	gin.SetMode(gin.ReleaseMode)
	oldOpts, oldStore, oldChannel, oldPerm, oldCluster := options.G, service.Store, eventbus.Channel, service.Permission, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Store = oldStore
		eventbus.Channel = oldChannel
		service.Permission = oldPerm
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Channel.ProcessTimeout = 8 * time.Second
	opts.Poller.ChannelCount = 8
	opts.Poller.ChannelGoroutine = 128
	options.G = opts
	service.Permission = nil

	dbDir := t.TempDir()
	wdb := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(dbDir),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(4),
		wkdb.WithMemTableSize(4*1024*1024),
		wkdb.WithBlockCacheSize(16*1024*1024),
		wkdb.WithMaxMessagesPerBatch(500),
		wkdb.WithMaxBatchBytes(4*1024*1024),
	))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	adapter := &realChannelClusterAdapter{
		nodeId:      1001,
		slotCount:   64,
		failChannel: failCh,
	}
	service.Cluster = adapter

	slotAdapter := &realChannelSlotAdapter{adapter: adapter}
	nodeAdapter := &realChannelNodeAdapter{adapter: adapter}
	chSrv := clusterchannel.NewServer(clusterchannel.NewOptions(
		clusterchannel.WithNodeId(1001),
		clusterchannel.WithGroupCount(16),
		clusterchannel.WithDB(wdb),
		clusterchannel.WithSlot(slotAdapter),
		clusterchannel.WithNode(nodeAdapter),
		clusterchannel.WithCluster(adapter),
		clusterchannel.WithRPC(adapter),
		clusterchannel.WithOnSaveConfig(func(channelId string, channelType uint8, cfg types.Config) error {
			return nil
		}),
		clusterchannel.WithMaxLogCountPerBatch(500),
		clusterchannel.WithMaxLogSizePerBatch(4*1024*1024),
	))
	require.NoError(t, chSrv.Start())
	t.Cleanup(func() { chSrv.Stop() })

	eb := &realSeedEventbusChannel{adapter: adapter}
	eb.pool = chEvent.NewEventPool(eb)
	require.NoError(t, eb.pool.Start())
	t.Cleanup(func() { eb.pool.Stop() })
	eventbus.RegisterChannel(eb)

	service.Store = store.New(store.NewOptions(
		store.WithNodeId(1001),
		store.WithDB(wdb),
		store.WithSlot(slotAdapter),
		store.WithChannel(chSrv),
	))

	router := wkhttp.NewWithLogger(func(c *wkhttp.Context) { c.Next() })
	mAPI := newMessage(&Server{})
	mAPI.route(router)

	personSubs := make([]string, 80)
	for i := 0; i < 80; i++ {
		personSubs[i] = fmt.Sprintf("p_%04d", i)
	}
	groupChs := make([]string, 64)
	for i := 0; i < 64; i++ {
		groupChs[i] = fmt.Sprintf("g_%04d", i)
	}
	subBatches := make([][]string, 20)
	for b := 0; b < 20; b++ {
		slice := personSubs[b*4 : (b+1)*4]
		batch := make([]string, 0, 200)
		for rep := 0; rep < 50; rep++ {
			batch = append(batch, slice...)
		}
		subBatches[b] = batch
	}

	groupCalls := min(1600, targetMsgs/5)
	if targetMsgs <= 4000 {
		groupCalls = 200
	}
	batchCalls := (targetMsgs - groupCalls) / 200
	groupCalls = targetMsgs - batchCalls*200
	workers := 8

	var (
		acceptedTotal atomic.Int64
		failedTotal   atomic.Int64
		acceptedMu    sync.Mutex
		acceptedMsgID = make(map[int64]string, targetMsgs)
		acceptedCNo   = make(map[string]int64, targetMsgs)
	)

	payloadB64 := base64.StdEncoding.EncodeToString([]byte(`{"pad":"XIN2841-R2-SEED-256B-PAYLOAD-PAD-0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF"}`))

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		bCount := batchCalls / workers
		if w < batchCalls%workers {
			bCount++
		}
		gCount := groupCalls / workers
		if w < groupCalls%workers {
			gCount++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < bCount; i++ {
				subs := subBatches[(w*3+i)%20]
				reqBody, _ := json.Marshal(map[string]interface{}{
					"header":      map[string]int{"no_persist": 0, "red_dot": 1, "sync_once": 0},
					"from_uid":    "u_sender_00",
					"subscribers": subs,
					"payload":     payloadB64,
				})
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodPost, "/message/sendbatch", bytes.NewReader(reqBody))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-WK-Include-Accepted-Ids", "1")
				router.ServeHTTP(rec, req)
				if rec.Code == http.StatusOK {
					var resp struct {
						FailUids     []string `json:"fail_uids"`
						MessageIDs   []int64  `json:"message_ids"`
						ClientMsgNos []string `json:"client_msg_nos"`
					}
					_ = json.Unmarshal(rec.Body.Bytes(), &resp)
					failed := len(resp.FailUids)
					failedTotal.Add(int64(failed))
					acceptedTotal.Add(int64(len(subs) - failed))
					acceptedMu.Lock()
					for idx, mid := range resp.MessageIDs {
						cno := ""
						if idx < len(resp.ClientMsgNos) {
							cno = resp.ClientMsgNos[idx]
						}
						acceptedMsgID[mid] = cno
						if cno != "" {
							acceptedCNo[cno] = mid
						}
					}
					acceptedMu.Unlock()
				} else {
					failedTotal.Add(int64(len(subs)))
				}
			}

			for j := 0; j < gCount; j++ {
				gch := groupChs[(w*17+j)%64]
				cno := fmt.Sprintf("%s_grp_w%d_%d", tierLabel, w, j)
				reqBody, _ := json.Marshal(map[string]interface{}{
					"header":        map[string]int{"no_persist": 0, "red_dot": 1, "sync_once": 0},
					"client_msg_no": cno,
					"from_uid":      "u_sender_00",
					"channel_id":    gch,
					"channel_type":  2,
					"payload":       payloadB64,
				})
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodPost, "/message/send", bytes.NewReader(reqBody))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(rec, req)
				if rec.Code == http.StatusOK {
					var resp struct {
						Data struct {
							MessageID   int64  `json:"message_id"`
							ClientMsgNo string `json:"client_msg_no"`
						} `json:"data"`
					}
					_ = json.Unmarshal(rec.Body.Bytes(), &resp)
					acceptedTotal.Add(1)
					acceptedMu.Lock()
					acceptedMsgID[resp.Data.MessageID] = resp.Data.ClientMsgNo
					acceptedCNo[resp.Data.ClientMsgNo] = resp.Data.MessageID
					acceptedMu.Unlock()
				} else {
					failedTotal.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	require.Eventually(t, func() bool {
		return eb.inflight.Load() == 0
	}, 15*time.Second, 10*time.Millisecond, "eventbus.Channel pollers must drain all accepted events")
	elapsed := time.Since(start).Seconds()

	// Post-drain reconciliation across all 80 person channels and 64 group channels
	var (
		persistedCount   int
		dupMsgIDs        int
		dupClientNos     int
		seqGaps          int
		channelsVerified int
		seenMsgIDs       = make(map[int64]struct{}, targetMsgs)
		seenClientNos    = make(map[string]struct{}, targetMsgs)
	)

	scanChannel := func(chID string, chType uint8) {
		var curSeq uint64 = 1
		var expectedSeq uint32 = 1
		hasAny := false
		for {
			msgs, err := wdb.LoadNextRangeMsgs(chID, chType, curSeq, 0, 2000)
			require.NoError(t, err)
			if len(msgs) == 0 {
				break
			}
			hasAny = true
			for _, m := range msgs {
				persistedCount++
				if m.MessageSeq != expectedSeq {
					seqGaps++
				}
				expectedSeq = m.MessageSeq + 1
				if _, exists := seenMsgIDs[m.MessageID]; exists {
					dupMsgIDs++
				}
				seenMsgIDs[m.MessageID] = struct{}{}
				if m.ClientMsgNo == "" {
					dupClientNos++
				} else {
					if _, exists := seenClientNos[m.ClientMsgNo]; exists {
						dupClientNos++
					}
					seenClientNos[m.ClientMsgNo] = struct{}{}
				}
			}
			curSeq = uint64(msgs[len(msgs)-1].MessageSeq) + 1
		}
		if hasAny {
			channelsVerified++
		}
	}

	for _, p := range personSubs {
		fakeCh := options.GetFakeChannelIDWith("u_sender_00", p)
		scanChannel(fakeCh, wkproto.ChannelTypePerson)
	}
	for _, g := range groupChs {
		scanChannel(g, wkproto.ChannelTypeGroup)
	}

	missingMsgIDs := 0
	for id := range acceptedMsgID {
		if _, ok := seenMsgIDs[id]; !ok {
			missingMsgIDs++
		}
	}
	missingClientNos := 0
	for cno := range acceptedCNo {
		if _, ok := seenClientNos[cno]; !ok {
			missingClientNos++
		}
	}

	return seedTierAccountingResult{
		TierLabel:                 tierLabel,
		TargetMessages:            targetMsgs,
		AcceptedMessages:          int(acceptedTotal.Load()),
		CallerRecordedMessageIDs:  len(acceptedMsgID),
		CallerRecordedClientNos:   len(acceptedCNo),
		ExplicitFailedMessages:    int(failedTotal.Load()),
		PersistedMessages:         persistedCount,
		EventbusHandledEvents:     eb.handledEvents.Load(),
		PersistedEqualsAccepted:   persistedCount == int(acceptedTotal.Load()),
		UniquePersistedMessageIDs: len(seenMsgIDs),
		UniquePersistedClientNos:  len(seenClientNos),
		MissingAcceptedMessageIDs: missingMsgIDs,
		MissingAcceptedClientNos:  missingClientNos,
		DuplicateMessageIDs:       dupMsgIDs,
		DuplicateClientMsgNos:     dupClientNos,
		SequenceGaps:              seqGaps,
		ChannelsVerified:          channelsVerified,
		ElapsedSeconds:            elapsed,
	}
}

func TestRound2_BFR2_4_SeedWorkloads100k_500k_1m_UniqueIDReconciliation(t *testing.T) {
	tiers := []struct {
		label  string
		target int
	}{
		{label: "tier1_100k", target: 100000},
		{label: "tier2_500k", target: 500000},
		{label: "tier3_1m", target: 1000000},
	}

	for _, tier := range tiers {
		tier := tier
		t.Run(tier.label, func(t *testing.T) {
			res := runSeedTierAccounting(t, tier.label, tier.target, "")
			rawJSON, err := json.Marshal(res)
			require.NoError(t, err)
			t.Logf("SEED_ACCOUNTING_RESULT[%s]: %s", tier.label, string(rawJSON))

			require.Equal(t, tier.target, res.AcceptedMessages)
			require.Equal(t, res.AcceptedMessages, res.CallerRecordedMessageIDs, "100%% of accepted send + sendbatch message_ids must be recorded by caller")
			require.Equal(t, res.AcceptedMessages, res.CallerRecordedClientNos, "100%% of accepted send + sendbatch client_msg_nos must be recorded by caller")
			require.Equal(t, 0, res.ExplicitFailedMessages)
			require.Equal(t, res.AcceptedMessages, res.PersistedMessages, "persisted_msg_total must equal acked_msg_total (0 gap)")
			require.Equal(t, int64(res.AcceptedMessages), res.EventbusHandledEvents, "all accepted messages must drain through eventbus.Channel")
			require.True(t, res.PersistedEqualsAccepted)
			require.Equal(t, res.PersistedMessages, res.UniquePersistedMessageIDs, "every persisted message must have a unique message_id")
			require.Equal(t, res.PersistedMessages, res.UniquePersistedClientNos, "every persisted message must have a unique client_msg_no")
			require.Equal(t, 0, res.MissingAcceptedMessageIDs)
			require.Equal(t, 0, res.MissingAcceptedClientNos)
			require.Equal(t, 0, res.DuplicateMessageIDs)
			require.Equal(t, 0, res.DuplicateClientMsgNos)
			require.Equal(t, 0, res.SequenceGaps)
			require.Equal(t, 144, res.ChannelsVerified, "all 80 person + 64 group channels must be populated and verified")
		})
	}

	t.Run("explicit_failure_accounting", func(t *testing.T) {
		failCh := options.GetFakeChannelIDWith("u_sender_00", "p_0000")
		res := runSeedTierAccounting(t, "tier_fail_inject", 2000, failCh)
		rawJSON, err := json.Marshal(res)
		require.NoError(t, err)
		t.Logf("SEED_ACCOUNTING_RESULT[tier_fail_inject]: %s", string(rawJSON))

		require.Greater(t, res.ExplicitFailedMessages, 0, "injected channel failure must be explicitly reported in fail_uids")
		require.Equal(t, 2000, res.AcceptedMessages+res.ExplicitFailedMessages)
		require.Equal(t, res.AcceptedMessages, res.CallerRecordedMessageIDs, "100%% of accepted message_ids must be recorded by caller")
		require.Equal(t, res.AcceptedMessages, res.CallerRecordedClientNos, "100%% of accepted client_msg_nos must be recorded by caller")
		require.Equal(t, res.AcceptedMessages, res.PersistedMessages, "persisted_msg_total must equal accepted_messages even under partial channel failure")
		require.Equal(t, int64(res.AcceptedMessages), res.EventbusHandledEvents)
		require.True(t, res.PersistedEqualsAccepted)
		require.Equal(t, 0, res.MissingAcceptedMessageIDs)
		require.Equal(t, 0, res.MissingAcceptedClientNos)
		require.Equal(t, 0, res.DuplicateMessageIDs)
		require.Equal(t, 0, res.DuplicateClientMsgNos)
		require.Equal(t, 0, res.SequenceGaps)
	})

	t.Run("multi_node_cluster_raft_forwarding", func(t *testing.T) {
		oldOpts, oldStore, oldChannel, oldPerm, oldCluster := options.G, service.Store, eventbus.Channel, service.Permission, service.Cluster
		t.Cleanup(func() {
			options.G = oldOpts
			service.Store = oldStore
			eventbus.Channel = oldChannel
			service.Permission = oldPerm
			service.Cluster = oldCluster
		})

		opts := options.New()
		opts.Cluster.NodeId = 1001
		opts.Channel.ProcessTimeout = 8 * time.Second
		opts.Poller.ChannelCount = 4
		opts.Poller.ChannelGoroutine = 64
		options.G = opts
		service.Permission = nil

		peers := make(map[uint64]*realSeedClusterNode, 3)
		for _, nid := range []uint64{1001, 1002, 1003} {
			nid := nid
			dbDir := t.TempDir()
			wdb := wkdb.NewWukongDB(wkdb.NewOptions(
				wkdb.WithDir(dbDir),
				wkdb.WithNodeId(nid),
				wkdb.WithShardNum(2),
				wkdb.WithMemTableSize(2*1024*1024),
				wkdb.WithBlockCacheSize(8*1024*1024),
				wkdb.WithMaxMessagesPerBatch(500),
				wkdb.WithMaxBatchBytes(4*1024*1024),
			))
			require.NoError(t, wdb.Open())
			t.Cleanup(func() { _ = wdb.Close() })

			ad := &realChannelClusterAdapter{
				nodeId:    nid,
				slotCount: 64,
				peers:     peers,
			}
			slotAd := &realChannelSlotAdapter{adapter: ad}
			nodeAd := &realChannelNodeAdapter{adapter: ad}
			chSrv := clusterchannel.NewServer(clusterchannel.NewOptions(
				clusterchannel.WithNodeId(nid),
				clusterchannel.WithGroupCount(8),
				clusterchannel.WithDB(wdb),
				clusterchannel.WithSlot(slotAd),
				clusterchannel.WithNode(nodeAd),
				clusterchannel.WithCluster(ad),
				clusterchannel.WithRPC(ad),
				clusterchannel.WithOnSaveConfig(func(channelId string, channelType uint8, cfg types.Config) error {
					return nil
				}),
			))
			require.NoError(t, chSrv.Start())
			t.Cleanup(func() { chSrv.Stop() })

			eb := &realSeedEventbusChannel{adapter: ad}
			eb.pool = chEvent.NewEventPool(eb)
			require.NoError(t, eb.pool.Start())
			t.Cleanup(func() { eb.pool.Stop() })

			st := store.New(store.NewOptions(
				store.WithNodeId(nid),
				store.WithDB(wdb),
				store.WithSlot(slotAd),
				store.WithChannel(chSrv),
			))
			peers[nid] = &realSeedClusterNode{
				nodeId:  nid,
				wdb:     wdb,
				chSrv:   chSrv,
				store:   st,
				eb:      eb,
				adapter: ad,
			}
		}

		// Ingress HTTP requests hit node 1001 while channels are led across 1001, 1002, and 1003.
		service.Cluster = peers[1001].adapter
		service.Store = peers[1001].store
		eventbus.RegisterChannel(peers[1001].eb)

		router := wkhttp.NewWithLogger(func(c *wkhttp.Context) { c.Next() })
		mAPI := newMessage(&Server{})
		mAPI.route(router)

		acceptedMsgID := make(map[int64]string, 3000)
		acceptedCNo := make(map[string]int64, 3000)
		payloadB64 := base64.StdEncoding.EncodeToString([]byte(`{"msg":"multi-node-raft-forwarding"}`))

		subs := make([]string, 60)
		for i := 0; i < 60; i++ {
			subs[i] = fmt.Sprintf("mn_p_%04d", i)
		}
		for b := 0; b < 40; b++ {
			reqBody, _ := json.Marshal(map[string]interface{}{
				"header":      map[string]int{"no_persist": 0, "red_dot": 1, "sync_once": 0},
				"from_uid":    "u_sender_00",
				"subscribers": subs,
				"payload":     payloadB64,
			})
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/message/sendbatch", bytes.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-WK-Include-Accepted-Ids", "1")
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			var resp struct {
				FailUids     []string `json:"fail_uids"`
				MessageIDs   []int64  `json:"message_ids"`
				ClientMsgNos []string `json:"client_msg_nos"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Empty(t, resp.FailUids)
			require.Len(t, resp.MessageIDs, len(subs))
			for idx, mid := range resp.MessageIDs {
				cno := resp.ClientMsgNos[idx]
				acceptedMsgID[mid] = cno
				acceptedCNo[cno] = mid
			}
		}

		for j := 0; j < 600; j++ {
			gch := fmt.Sprintf("mn_g_%04d", j%30)
			cno := fmt.Sprintf("mn_grp_%04d", j)
			reqBody, _ := json.Marshal(map[string]interface{}{
				"header":        map[string]int{"no_persist": 0, "red_dot": 1, "sync_once": 0},
				"client_msg_no": cno,
				"from_uid":      "u_sender_00",
				"channel_id":    gch,
				"channel_type":  2,
				"payload":       payloadB64,
			})
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/message/send", bytes.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			var resp struct {
				Data struct {
					MessageID   int64  `json:"message_id"`
					ClientMsgNo string `json:"client_msg_no"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			acceptedMsgID[resp.Data.MessageID] = resp.Data.ClientMsgNo
			acceptedCNo[resp.Data.ClientMsgNo] = resp.Data.MessageID
		}

		require.Eventually(t, func() bool {
			return peers[1001].eb.inflight.Load() == 0 && peers[1002].eb.inflight.Load() == 0 && peers[1003].eb.inflight.Load() == 0
		}, 10*time.Second, 10*time.Millisecond)

		var totalHandled int64
		for _, nid := range []uint64{1001, 1002, 1003} {
			totalHandled += peers[nid].eb.handledEvents.Load()
		}
		require.Equal(t, int64(3000), totalHandled, "all 3,000 messages across 3 nodes must be handled by their respective slot leaders")
		require.Greater(t, peers[1001].eb.forwardedMsgs.Load(), int64(1500), "remote slot leader events must be forwarded from ingress node 1001 to nodes 1002 and 1003")

		seenMsgIDs := make(map[int64]struct{}, 3000)
		seenClientNos := make(map[string]struct{}, 3000)
		scanLeader := func(chID string, chType uint8) {
			leaderId := peers[1001].adapter.leaderForChannel(chID)
			msgs, err := peers[leaderId].wdb.LoadNextRangeMsgs(chID, chType, 1, 0, 5000)
			require.NoError(t, err)
			var expectedSeq uint32 = 1
			for _, m := range msgs {
				require.Equal(t, expectedSeq, m.MessageSeq)
				expectedSeq++
				seenMsgIDs[m.MessageID] = struct{}{}
				seenClientNos[m.ClientMsgNo] = struct{}{}
			}
		}
		for _, sub := range subs {
			scanLeader(options.GetFakeChannelIDWith("u_sender_00", sub), wkproto.ChannelTypePerson)
		}
		for j := 0; j < 30; j++ {
			scanLeader(fmt.Sprintf("mn_g_%04d", j), wkproto.ChannelTypeGroup)
		}
		require.Len(t, seenMsgIDs, 3000)
		require.Len(t, seenClientNos, 3000)
		for mid := range acceptedMsgID {
			_, ok := seenMsgIDs[mid]
			require.True(t, ok, "missing accepted message_id %d", mid)
		}
		for cno := range acceptedCNo {
			_, ok := seenClientNos[cno]
			require.True(t, ok, "missing accepted client_msg_no %s", cno)
		}
	})
}
