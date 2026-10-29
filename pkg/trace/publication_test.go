package trace

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGlobalTracePublicationAndRestoration(t *testing.T) {
	previous := GetGlobalTrace()
	t.Cleanup(func() { SetGlobalTrace(previous) })
	SetGlobalTrace(nil)
	require.Nil(t, GetGlobalTrace())
	first := New(context.Background(), NewOptions())
	SetGlobalTrace(first)
	require.Same(t, first, GetGlobalTrace())
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 100; i++ {
			SetGlobalTrace(New(context.Background(), NewOptions()))
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 100; i++ {
			current := GetGlobalTrace()
			require.NotNil(t, current.Metrics.App())
			require.NotNil(t, current.Metrics.System())
			require.NotNil(t, current.Metrics.Cluster())
			require.NotNil(t, current.Metrics.DB())
			current.Metrics.Cluster().MessageOutgoingCountAdd(ClusterKindConfig, 1)
		}
	}()
	workers.Wait()
	SetGlobalTrace(first)
	require.Same(t, first, GetGlobalTrace())
	// A reference acquired before replacement remains usable until its owner
	// finishes, including when the installed global is restored to nil.
	SetGlobalTrace(nil)
	first.Metrics.Cluster().MessageOutgoingCountAdd(ClusterKindSlot, 1)
	require.Nil(t, GetGlobalTrace())
}
