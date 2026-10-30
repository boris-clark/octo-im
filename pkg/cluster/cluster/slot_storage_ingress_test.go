package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/stretchr/testify/require"
)

func TestE3SlotReplicaHTTPDuringTransferAndAfterStop(t *testing.T) {
	c := newOpCluster(t, 3, 4, 3)
	ctrl := c.controller(t)
	slot := ctrl.cfgServer.Slots()[0]
	from := slot.Leader
	var to uint64
	for _, id := range slot.Replicas {
		if id != from {
			to = id
			break
		}
	}
	routes := map[uint64]http.Handler{}
	for id, s := range c.servers {
		route := wkhttp.New()
		route.GET("/slots/:id/localReplica", s.slotLocalReplicaGet)
		routes[id] = route.GetGinRoute()
	}
	read := func(id uint64) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		routes[id].ServeHTTP(w, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/slots/%d/localReplica", slot.Id), nil))
		return w
	}
	require.Eventually(t, func() bool {
		w := read(from)
		if w.Code != http.StatusOK {
			return false
		}
		var response slotReplicaResp
		if json.Unmarshal(w.Body.Bytes(), &response) != nil {
			return false
		}
		return response.Running == 1 && response.LeaderId == from && response.Term > 0
	}, time.Second, 5*time.Millisecond)
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for id := range c.servers {
		readers.Add(1)
		go func(id uint64) {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				w := read(id)
				if w.Code != http.StatusOK && w.Code != http.StatusBadRequest {
					t.Errorf("replica HTTP status %d", w.Code)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(id)
	}
	err := ctrl.cfgServer.ProposeMigrateSlot(slot.Id, from, to)
	close(stop)
	readers.Wait()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		w := read(to)
		if w.Code != http.StatusOK {
			return false
		}
		var response slotReplicaResp
		if json.Unmarshal(w.Body.Bytes(), &response) != nil {
			return false
		}
		return response.Running == 1 && response.LeaderId == to && response.Term > slot.Term
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, c.servers[to].Stop())
	require.Equal(t, http.StatusBadRequest, read(to).Code, "stopped replica must refuse before touching closed storage")
}
