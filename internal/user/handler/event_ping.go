package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func (h *Handler) ping(event *eventbus.Event) {
	currentTrace := trace.GetGlobalTrace()

	conn := event.Conn

	currentTrace.Metrics.App().PingCountAdd(1)
	currentTrace.Metrics.App().PingBytesAdd(1) // ping的大小就是1

	currentTrace.Metrics.App().PongCountAdd(1)
	currentTrace.Metrics.App().PongBytesAdd(1)
	eventbus.User.ConnWrite(event.ReqId, conn, &wkproto.PongPacket{})
}
