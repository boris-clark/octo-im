package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
)

// Exercise publication while real transports and DB collectors remain active.
func TestTracePublicationWithLiveConsumers(t *testing.T) {
	servers, _ := newConfigRPCServers(t, 2)
	node := newNodeTransport(servers[0])
	slot := newSlotTransport(servers[0])
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 60; i++ {
			trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
			time.Sleep(20 * time.Millisecond)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 240; i++ {
			event := rafttype.Event{Type: rafttype.Ping, From: 1, To: 2, Term: 1}
			node.Send(event)
			slot.Send("0", event)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	close(start)
	workers.Wait()
}
