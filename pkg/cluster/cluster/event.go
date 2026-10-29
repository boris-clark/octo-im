package cluster

import (
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
)

// 收到消息
func (s *Server) onMessage(conn gnet.Conn, m *proto.Message) {
	currentTrace := trace.GetGlobalTrace()

	switch m.MsgType {
	case MsgTypeNode:
		s.onNodeMessage(conn, m)
		if currentTrace != nil {
			currentTrace.Metrics.Cluster().MessageIncomingCountAdd(trace.ClusterKindConfig, 1)
			currentTrace.Metrics.Cluster().MessageIncomingBytesAdd(trace.ClusterKindConfig, int64(m.Size()))
		}
	case MsgTypeSlot:
		if currentTrace != nil {
			currentTrace.Metrics.Cluster().MessageIncomingCountAdd(trace.ClusterKindSlot, 1)
			currentTrace.Metrics.Cluster().MessageIncomingBytesAdd(trace.ClusterKindSlot, int64(m.Size()))
		}
		s.onSlotMessage(conn, m)
	case MsgTypeChannel:
		if currentTrace != nil {
			currentTrace.Metrics.Cluster().MessageIncomingCountAdd(trace.ClusterKindChannel, 1)
			currentTrace.Metrics.Cluster().MessageIncomingBytesAdd(trace.ClusterKindChannel, int64(m.Size()))
		}
		s.onChannelMessage(conn, m)
	default:
		if currentTrace != nil {
			currentTrace.Metrics.Cluster().MessageIncomingCountAdd(trace.ClusterKindOther, 1)
			currentTrace.Metrics.Cluster().MessageIncomingBytesAdd(trace.ClusterKindOther, int64(m.Size()))
		}
		if s.onMessageFnc != nil {
			fromNodeId := s.uidToServerId(wkserver.GetUidFromContext(conn))
			err := s.onMessagePool.Submit(func() {
				s.onMessageFnc(fromNodeId, m)
			})
			if err != nil {
				s.Error("onMessage: submit onMessageFnc failed", zap.Error(err))
			}
		}

	}
}

// 节点消息
func (s *Server) onNodeMessage(_ gnet.Conn, m *proto.Message) {
	currentTrace := trace.GetGlobalTrace()

	var event rafttype.Event
	err := event.Unmarshal(m.Content)
	if err != nil {
		s.Error("onNodeMessage: unmarshal event failed", zap.Error(err))
		return
	}

	if currentTrace != nil && event.Type == rafttype.SyncReq {
		currentTrace.Metrics.Cluster().MsgSyncIncomingCountAdd(trace.ClusterKindConfig, 1)
		currentTrace.Metrics.Cluster().MsgSyncIncomingBytesAdd(trace.ClusterKindConfig, int64(m.Size()))
	}
	s.eventServer.Step(event)
}

func (s *Server) onSlotMessage(_ gnet.Conn, m *proto.Message) {
	currentTrace := trace.GetGlobalTrace()

	dec := wkproto.NewDecoder(m.Content)
	key, err := dec.String()
	if err != nil {
		s.Error("onSlotMessage: decode key failed", zap.Error(err))
		return
	}
	data, err := dec.BinaryAll()
	if err != nil {
		s.Error("onSlotMessage: decode data failed", zap.Error(err))
		return
	}
	var event rafttype.Event
	err = event.Unmarshal(data)
	if err != nil {
		s.Error("onSlotMessage: unmarshal event failed", zap.Error(err))
		return
	}

	if currentTrace != nil && event.Type == rafttype.SyncReq {
		currentTrace.Metrics.Cluster().MsgSyncIncomingCountAdd(trace.ClusterKindSlot, 1)
		currentTrace.Metrics.Cluster().MsgSyncIncomingBytesAdd(trace.ClusterKindSlot, int64(m.Size()))
	}

	s.slotServer.AddEvent(key, event)
}

// channel消息
func (s *Server) onChannelMessage(_ gnet.Conn, m *proto.Message) {
	currentTrace := trace.GetGlobalTrace()

	dec := wkproto.NewDecoder(m.Content)
	key, err := dec.String()
	if err != nil {
		s.Error("onChannelMessage: decode key failed", zap.Error(err))
		return
	}
	data, err := dec.BinaryAll()
	if err != nil {
		s.Error("onChannelMessage: decode data failed", zap.Error(err))
		return
	}
	var event rafttype.Event
	err = event.Unmarshal(data)
	if err != nil {
		s.Error("onChannelMessage: unmarshal event failed", zap.Error(err))
		return
	}
	if currentTrace != nil && event.Type == rafttype.SyncReq {
		currentTrace.Metrics.Cluster().MsgSyncIncomingCountAdd(trace.ClusterKindChannel, 1)
		currentTrace.Metrics.Cluster().MsgSyncIncomingBytesAdd(trace.ClusterKindChannel, int64(m.Size()))
	}

	s.channelServer.AddEvent(key, event)
}
