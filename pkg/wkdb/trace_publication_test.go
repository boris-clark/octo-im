package wkdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/stretchr/testify/require"
)

type publicationDBMetrics struct {
	trace.IDBMetrics
	collections atomic.Int64
}

func (m *publicationDBMetrics) CompactTotalCountSet(shard uint32, value int64) {
	m.collections.Add(1)
	m.IDBMetrics.CompactTotalCountSet(shard, value)
}

func TestDBMetricsOwnershipAcrossTraceReplacement(t *testing.T) {
	previous := trace.GetGlobalTrace()
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	installed := trace.New(context.Background(), trace.NewOptions())
	trace.SetGlobalTrace(installed)
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	metrics := &publicationDBMetrics{IDBMetrics: installed.Metrics.DB()}
	db.metrics = metrics
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < 60; i++ {
			trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
			time.Sleep(20 * time.Millisecond)
		}
	}()
	// DB construction is also a real consumer of the publication API.
	for i := 0; i < 100; i++ {
		constructed := NewWukongDB(NewOptions(WithShardNum(1))).(*wukongDB)
		constructed.metrics.CompactTotalCountSet(0, 0)
		constructed.cancelFunc()
	}
	workers.Wait()
	require.Positive(t, metrics.collections.Load(), "the running collector must retain its metrics owner")
	trace.SetGlobalTrace(nil)
	before := metrics.collections.Load()
	db.collectMetrics()
	require.Greater(t, metrics.collections.Load(), before, "restoring nil must not disable an owned DB collector")
}
