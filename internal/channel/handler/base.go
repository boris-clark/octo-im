package handler

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

type channelWatermarkKey struct {
	channelId   string
	channelType uint8
}

type channelSeqWatermark struct {
	contiguous uint64
	above      map[uint64]struct{}
}

type seqWatermarkTracker struct {
	mu       sync.Mutex
	channels map[channelWatermarkKey]*channelSeqWatermark
}

func (t *seqWatermarkTracker) hasSeen(channelId string, channelType uint8, messageSeq uint64) bool {
	if t == nil || messageSeq == 0 {
		return false
	}
	k := channelWatermarkKey{channelId: channelId, channelType: channelType}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.channels == nil {
		return false
	}
	wm := t.channels[k]
	if wm == nil {
		return false
	}
	if messageSeq <= wm.contiguous {
		return true
	}
	if wm.above != nil {
		_, exists := wm.above[messageSeq]
		return exists
	}
	return false
}

func (t *seqWatermarkTracker) record(channelId string, channelType uint8, messageSeq uint64) bool {
	if t == nil || messageSeq == 0 {
		return false
	}
	k := channelWatermarkKey{channelId: channelId, channelType: channelType}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.channels == nil {
		t.channels = make(map[channelWatermarkKey]*channelSeqWatermark, 256)
	}
	wm := t.channels[k]
	if wm == nil {
		wm = &channelSeqWatermark{}
		t.channels[k] = wm
	}
	if messageSeq <= wm.contiguous {
		return true
	}
	if wm.above != nil {
		if _, exists := wm.above[messageSeq]; exists {
			return true
		}
	}
	if messageSeq == wm.contiguous+1 {
		wm.contiguous = messageSeq
		for wm.above != nil {
			next := wm.contiguous + 1
			if _, ok := wm.above[next]; !ok {
				break
			}
			delete(wm.above, next)
			wm.contiguous = next
		}
		if len(wm.above) == 0 {
			wm.above = nil
		}
		return false
	}
	if wm.above == nil {
		wm.above = make(map[uint64]struct{})
	}
	wm.above[messageSeq] = struct{}{}
	return false
}

type NodeDeps struct {
	NodeId        uint64
	ChannelBus    eventbus.IChannel
	Cluster       icluster.ICluster
	Store         *store.Store
	Permission    *service.PermissionService
	TagManager    service.ITagManager
	Conversation  service.IConversationManager
	UserBus       eventbus.IUser
	PusherBus     eventbus.IPusher
}

type Handler struct {
	wklog.Log
	client              *ingress.Client
	commonService       *common.Service
	forwardCapabilities forward.CapabilityCache
	forwardGate         *forward.Gate
	boundNodeId         uint64
	boundChannelBus     eventbus.IChannel
	boundCluster        icluster.ICluster
	boundStore          *store.Store
	boundPermission     *service.PermissionService
	boundTagManager     service.ITagManager
	boundConversation   service.IConversationManager
	boundUser           eventbus.IUser
	boundPusher         eventbus.IPusher

	pluginWebhookTracker seqWatermarkTracker
	distributeTracker    seqWatermarkTracker
}

func (h *Handler) BindNode(nodeId uint64, chBus eventbus.IChannel) {
	if h == nil {
		return
	}
	h.boundNodeId = nodeId
	h.boundChannelBus = chBus
}

func (h *Handler) BindNodeDeps(deps NodeDeps) {
	if h == nil {
		return
	}
	h.boundNodeId = deps.NodeId
	h.boundChannelBus = deps.ChannelBus
	h.boundCluster = deps.Cluster
	h.boundStore = deps.Store
	h.boundPermission = deps.Permission
	h.boundTagManager = deps.TagManager
	h.boundConversation = deps.Conversation
	h.boundUser = deps.UserBus
	h.boundPusher = deps.PusherBus
	if h.client != nil && deps.Cluster != nil {
		h.client.BindCluster(deps.Cluster)
	}
	if h.commonService != nil {
		h.commonService.BindDeps(deps.NodeId, deps.Cluster, deps.TagManager, h.client)
	}
}

func (h *Handler) localNodeId() uint64 {
	if h != nil && h.boundNodeId != 0 {
		return h.boundNodeId
	}
	if options.G != nil {
		return options.G.Cluster.NodeId
	}
	return 0
}

func (h *Handler) isLocalNode(nodeId uint64) bool {
	if h != nil && h.boundNodeId != 0 {
		return h.boundNodeId == nodeId
	}
	return options.G != nil && options.G.IsLocalNode(nodeId)
}

func (h *Handler) channelBus() eventbus.IChannel {
	if h != nil && h.boundChannelBus != nil {
		return h.boundChannelBus
	}
	if eventbus.Channel != nil {
		return eventbus.Channel
	}
	return nil
}

func (h *Handler) cluster() icluster.ICluster {
	if h != nil && h.boundCluster != nil {
		return h.boundCluster
	}
	return service.Cluster
}

func (h *Handler) store() *store.Store {
	if h != nil && h.boundStore != nil {
		return h.boundStore
	}
	return service.Store
}

func (h *Handler) permissionService() *service.PermissionService {
	if h != nil && h.boundPermission != nil {
		return h.boundPermission
	}
	return service.Permission
}

func (h *Handler) tagManager() service.ITagManager {
	if h != nil && h.boundTagManager != nil {
		return h.boundTagManager
	}
	return service.TagManager
}

func (h *Handler) conversationManager() service.IConversationManager {
	if h != nil && h.boundConversation != nil {
		return h.boundConversation
	}
	return service.ConversationManager
}

func (h *Handler) userAuthedConns(uid string) []*eventbus.Conn {
	if h != nil && h.boundUser != nil {
		return h.boundUser.AuthedConnsByUid(uid)
	}
	if eventbus.User != nil {
		return eventbus.User.AuthedConnsByUid(uid)
	}
	return nil
}

func (h *Handler) pushEvents(events []*eventbus.Event, advance bool) {
	if len(events) == 0 {
		return
	}
	if h != nil && h.boundPusher != nil {
		id := h.boundPusher.RandHandlerId()
		for _, ev := range events {
			h.boundPusher.AddEvent(id, ev)
		}
		if advance {
			h.boundPusher.Advance(id)
		}
		return
	}
	if eventbus.Pusher != nil {
		id := eventbus.Pusher.AddEvents(events)
		if advance {
			eventbus.Pusher.Advance(id)
		}
	}
}

func (h *Handler) recordPluginWebhookSideEffect(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	return h.pluginWebhookTracker.record(channelId, channelType, messageSeq)
}

func (h *Handler) recordDistributeSideEffect(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	return h.distributeTracker.record(channelId, channelType, messageSeq)
}

func (h *Handler) hasDistributed(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	return h.distributeTracker.hasSeen(channelId, channelType, messageSeq)
}

func NewHandler() *Handler {
	workerCount := 1
	if options.G != nil {
		workerCount = options.G.Poller.ChannelGoroutine
	}
	h := &Handler{
		Log:           wklog.NewWKLog("handler"),
		client:        ingress.NewClient(),
		commonService: common.NewService(),
		forwardGate:   forward.NewGate(workerCount),
	}
	h.routes()
	return h
}

func (h *Handler) routes() {

	// 发送消息
	eventbus.RegisterChannelHandlers(eventbus.EventChannelOnSend, h.onSend)
	// webhook
	eventbus.RegisterChannelHandlers(eventbus.EventChannelWebhook, h.webhook)
	// 分发消息
	eventbus.RegisterChannelHandlers(eventbus.EventChannelDistribute, h.distribute)

}

// 收到消息
func (h *Handler) OnMessage(m *proto.Message) {
	switch msgType(m.MsgType) {
	case msgForwardChannelEvent:
		h.onForwardChannelEvent(m)
	}
}

// 收到事件
func (h *Handler) OnEvent(ctx *eventbus.ChannelContext) {
	if h.isLocalNode(ctx.SlotLeaderId) || h.notForwardToLeader(ctx.EventType) {
		// 执行本地事件 ,频道永远在自己的槽领导节点上执行逻辑。
		if h.boundNodeId != 0 {
			switch ctx.EventType {
			case eventbus.EventChannelOnSend:
				h.onSend(ctx)
			case eventbus.EventChannelWebhook:
				h.webhook(ctx)
			case eventbus.EventChannelDistribute:
				h.distribute(ctx)
			default:
				eventbus.ExecuteChannelEvent(ctx)
			}
			return
		}
		eventbus.ExecuteChannelEvent(ctx)
	} else {
		h.forwardsToNode(ctx.SlotLeaderId, ctx.ChannelId, ctx.ChannelType, ctx.Events)
	}
}

// 不需要转发给领导的事件
func (h *Handler) notForwardToLeader(eventType eventbus.EventType) bool {
	switch eventType {
	case eventbus.EventChannelWebhook,
		eventbus.EventChannelDistribute:
		return true
	}
	return false

}

func canForwardChannelSendBatch(events []*eventbus.Event) bool {
	if forward.OnlyRetryableSends(events) {
		return true
	}
	if len(events) == 0 {
		return false
	}
	for _, event := range events {
		if event == nil || event.Conn == nil || event.Type != eventbus.EventChannelOnSend {
			return false
		}
		packet, ok := event.Frame.(*wkproto.SendPacket)
		if !ok || packet == nil || strings.TrimSpace(packet.ClientMsgNo) == "" {
			return false
		}
	}
	return true
}

func hasPersistedEvents(events []*eventbus.Event) bool {
	for _, event := range events {
		if event != nil && event.MessageSeq > 0 {
			return true
		}
	}
	return false
}

func (h *Handler) requeuePersistedForwardFailures(channelId string, channelType uint8, events []*eventbus.Event, countHop bool) {
	chBus := h.channelBus()
	if chBus == nil {
		return
	}
	nowMs := time.Now().UnixMilli()
	defaultDeadline := time.Now().Add(forward.Budget).UnixMilli()
	requeued := false
	backoff := 25 * time.Millisecond
	for _, ev := range events {
		if ev == nil || ev.MessageSeq == 0 {
			continue
		}
		nextHops := ev.ForwardHops
		if countHop {
			nextHops++
		}
		deadline := ev.ForwardDeadline
		if deadline == 0 {
			deadline = defaultDeadline
		}
		// Never drop an already-persisted message after acknowledgment: reset the per-window
		// hop/deadline budget and continue retrying with bounded backoff until slot leadership stabilizes.
		if nextHops >= forward.MaxHops || deadline <= nowMs {
			nextHops = 0
			deadline = defaultDeadline
			backoff = 50 * time.Millisecond
		}
		clone := ev.Clone()
		clone.ForwardHops = nextHops
		clone.ForwardDeadline = deadline
		chBus.AddEvent(channelId, channelType, clone)
		requeued = true
	}
	if requeued {
		time.AfterFunc(backoff, func() {
			if bus := h.channelBus(); bus != nil {
				bus.Advance(channelId, channelType)
			}
		})
	}
}

func (h *Handler) forwardsToNode(nodeId uint64, channelId string, channelType uint8, events []*eventbus.Event) {
	persistedBatch := hasPersistedEvents(events)
	canForwardSend := canForwardChannelSendBatch(events)
	localNode := h.localNodeId()

	if !canForwardSend {
		req := &forwardChannelEventReq{
			channelId:   channelId,
			channelType: channelType,
			fromNode:    localNode,
			events:      events,
		}
		data, err := req.encode()
		if err != nil {
			h.Error("forwardToLeader: encode failed", zap.Error(err))
			forward.Fail(events, err)
			return
		}
		// Distribution and webhook work has no client-side retry path. Keep it
		// out of the v1 ambiguity window and retain it in the base transport.
		if err = h.sendToNode(nodeId, &proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: data}); err != nil {
			h.Error("channel forwarding through retained transport failed", zap.Error(err), zap.String("channelId", channelId))
			forward.Fail(events, err)
			if persistedBatch {
				h.requeuePersistedForwardFailures(channelId, channelType, events, true)
			}
		}
		return
	}

	if !h.forwardGate.TryAcquire() {
		err := forward.ErrUnavailable
		h.Error("channel forwarding concurrency limit reached", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
		if persistedBatch {
			h.requeuePersistedForwardFailures(channelId, channelType, events, false)
		}
		return
	}
	defer h.forwardGate.Release()

	wireEvents := events
	persistedLegacySlotLeader := false
	permSvc := h.permissionService()
	if persistedBatch && len(events) > 0 && events[0].Type == eventbus.EventChannelOnSend &&
		permSvc != nil && permSvc.CheckOrProbeLegacyPeer(nodeId, channelId, channelType) {
		persistedLegacySlotLeader = true
		h.executePersistedLocalSideEffects(channelId, channelType, events)
		distEvents := make([]*eventbus.Event, 0, len(events))
		for _, ev := range events {
			if ev == nil || !shouldDistributePersistResult(ev) || h.hasDistributed(channelId, channelType, ev.MessageSeq) {
				continue
			}
			clone := ev.Clone()
			clone.Type = eventbus.EventChannelDistribute
			clone.SourceNodeId = 0
			distEvents = append(distEvents, clone)
		}
		if len(distEvents) == 0 {
			return
		}
		wireEvents = distEvents
	}

	req := &forwardChannelEventReq{
		channelId:   channelId,
		channelType: channelType,
		fromNode:    localNode,
		events:      wireEvents,
	}
	data, err := req.encode()
	if err != nil {
		h.Error("forwardToLeader: encode failed", zap.Error(err))
		forward.Fail(events, err)
		return
	}
	legacy := func(targetNode uint64) error {
		return h.sendToNode(targetNode, &proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: data})
	}
	target := func() uint64 {
		if len(events) > 0 && h.notForwardToLeader(events[0].Type) && !persistedLegacySlotLeader {
			return nodeId
		}
		cl := h.cluster()
		if cl == nil {
			return 0
		}
		leader, err := cl.SlotLeaderIdOfChannel(channelId, channelType)
		if err != nil {
			return 0
		}
		return leader
	}
	err = forward.RequestCompatible(forward.ChannelPath, data, events, target, localNode, h.acceptForward, &h.forwardCapabilities, legacy)
	if err != nil {
		h.Error("channel forwarding failed", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
		if persistedBatch {
			h.requeuePersistedForwardFailures(channelId, channelType, events, true)
		}
		return
	}
	if persistedLegacySlotLeader {
		for _, ev := range wireEvents {
			if ev != nil && ev.MessageSeq > 0 {
				_ = h.recordDistributeSideEffect(channelId, channelType, ev.MessageSeq)
			}
		}
	}
}

// func (h *Handler) forwardToNode(nodeId uint64, channelId string, channelType uint8, event *eventbus.Event) {
// 	h.forwardsToNode(nodeId, channelId, channelType, []*eventbus.Event{event})
// }

func (h *Handler) sendToNode(toNodeId uint64, msg *proto.Message) error {
	return h.cluster().Send(toNodeId, msg)
}

// 收到转发用户事件
func (h *Handler) onForwardChannelEvent(m *proto.Message) {
	req := &forwardChannelEventReq{}
	err := req.decode(m.Content)
	if err != nil {
		h.Error("onForwardChannelEvent: decode failed", zap.Error(err))
		return
	}

	chBus := h.channelBus()
	if chBus == nil {
		return
	}
	for _, e := range req.events {
		chBus.AddEvent(req.channelId, req.channelType, e)
	}
	chBus.Advance(req.channelId, req.channelType)

}

func (h *Handler) WithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), options.G.Channel.ProcessTimeout)

}
