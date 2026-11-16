package handler

import (
	"context"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

type Handler struct {
	wklog.Log
	client              *ingress.Client
	commonService       *common.Service
	forwardCapabilities forward.CapabilityCache
	forwardGate         *forward.Gate
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
	if options.G.IsLocalNode(ctx.SlotLeaderId) || h.notForwardToLeader(ctx.EventType) {
		// 执行本地事件 ,频道永远在自己的槽领导节点上执行逻辑。
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

func requeuePersistedForwardFailures(channelId string, channelType uint8, events []*eventbus.Event) {
	if eventbus.Channel == nil {
		return
	}
	requeued := false
	for _, ev := range events {
		if ev == nil || ev.MessageSeq == 0 || ev.ForwardHops >= 8 {
			continue
		}
		clone := ev.Clone()
		clone.ForwardHops = ev.ForwardHops + 1
		clone.ForwardDeadline = 0
		eventbus.Channel.AddEvent(channelId, channelType, clone)
		requeued = true
	}
	if requeued {
		time.AfterFunc(25*time.Millisecond, func() {
			if eventbus.Channel != nil {
				eventbus.Channel.Advance(channelId, channelType)
			}
		})
	}
}

func (h *Handler) forwardsToNode(nodeId uint64, channelId string, channelType uint8, events []*eventbus.Event) {

	req := &forwardChannelEventReq{
		channelId:   channelId,
		channelType: channelType,
		fromNode:    options.G.Cluster.NodeId,
		events:      events,
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
	persistedBatch := hasPersistedEvents(events)
	if !canForwardChannelSendBatch(events) {
		// Distribution and webhook work has no client-side retry path. Keep it
		// out of the v1 ambiguity window and retain it in the base transport.
		if err = legacy(nodeId); err != nil {
			h.Error("channel forwarding through retained transport failed", zap.Error(err), zap.String("channelId", channelId))
			forward.Fail(events, err)
			if persistedBatch {
				requeuePersistedForwardFailures(channelId, channelType, events)
			}
		}
		return
	}
	acquiredGate := h.forwardGate.TryAcquire()
	if !acquiredGate && !persistedBatch {
		err = forward.ErrUnavailable
		h.Error("channel forwarding concurrency limit reached", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
		return
	}
	if acquiredGate {
		defer h.forwardGate.Release()
	}
	target := func() uint64 {
		if len(events) > 0 && h.notForwardToLeader(events[0].Type) {
			return nodeId
		}
		leader, err := service.Cluster.SlotLeaderIdOfChannel(channelId, channelType)
		if err != nil {
			return 0
		}
		return leader
	}
	err = forward.RequestCompatible(forward.ChannelPath, data, events, target, options.G.Cluster.NodeId, h.acceptForward, &h.forwardCapabilities, legacy)
	if err != nil {
		h.Error("channel forwarding failed", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
		if persistedBatch {
			requeuePersistedForwardFailures(channelId, channelType, events)
		}
	}
}

// func (h *Handler) forwardToNode(nodeId uint64, channelId string, channelType uint8, event *eventbus.Event) {
// 	h.forwardsToNode(nodeId, channelId, channelType, []*eventbus.Event{event})
// }

func (h *Handler) sendToNode(toNodeId uint64, msg *proto.Message) error {
	return service.Cluster.Send(toNodeId, msg)
}

// 收到转发用户事件
func (h *Handler) onForwardChannelEvent(m *proto.Message) {
	req := &forwardChannelEventReq{}
	err := req.decode(m.Content)
	if err != nil {
		h.Error("onForwardChannelEvent: decode failed", zap.Error(err))
		return
	}

	for _, e := range req.events {
		eventbus.Channel.AddEvent(req.channelId, req.channelType, e)
	}
	eventbus.Channel.Advance(req.channelId, req.channelType)

}

func (h *Handler) WithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), options.G.Channel.ProcessTimeout)

}
