package slot

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/cockroachdb/pebble"
)

// SlotScanResult is the pre-upgrade view of one local slot replica.
type SlotScanResult struct {
	SlotId       uint32 `json:"slot_id"`
	AppliedIndex uint64 `json:"applied_index"`
	LastLogIndex uint64 `json:"last_log_index"`
	// Marker reports a durable divergence marker.
	Marker bool `json:"marker"`
	// AppliedBeyondLog reports applied_index > last_log_index.
	AppliedBeyondLog bool `json:"applied_beyond_log"`
}

// Quarantined reports whether this replica opens quarantined after the
// upgrade.
func (r SlotScanResult) Quarantined() bool {
	return r.Marker || r.AppliedBeyondLog
}

// SlotLogDBPath is the slot log storage directory under a cluster data dir.
func SlotLogDBPath(clusterDataDir string) string {
	return path.Join(clusterDataDir, "logdb")
}

// ScanDivergence opens the slot log storage of a stopped node read-only and
// reports every slot in [0, slotCount) whose replica would open quarantined.
// It writes nothing; the Pebble directory lock makes it fail on a running
// node. It does not detect divergence that left applied == last (see the
// runbook): a clean scan is not a proof that the state matches the log.
func ScanDivergence(logDBPath string, shardNum uint32, slotCount uint32) ([]SlotScanResult, error) {
	var flagged []SlotScanResult
	err := withStoppedStorage(logDBPath, shardNum, func(p *PebbleShardLogStorage) error {
		for slotId := uint32(0); slotId < slotCount; slotId++ {
			shardNo := SlotIdToKey(slotId)
			applied, err := p.appliedIndex(shardNo)
			if err != nil {
				return fmt.Errorf("slot scan: slot %d applied index: %w", slotId, err)
			}
			last, err := p.lastIndex(shardNo)
			if err != nil {
				return fmt.Errorf("slot scan: slot %d last index: %w", slotId, err)
			}
			marker, err := p.divergedMarker(shardNo)
			if err != nil {
				return fmt.Errorf("slot scan: slot %d marker: %w", slotId, err)
			}
			r := SlotScanResult{SlotId: slotId, AppliedIndex: applied, LastLogIndex: last, Marker: marker, AppliedBeyondLog: applied > last}
			if r.Quarantined() {
				flagged = append(flagged, r)
			}
		}
		return nil
	})
	return flagged, err
}

// InspectStoppedSlot reads copied state and logs under exclusive directory
// locks for every shard. It never exposes a writable or retained DB handle.
func InspectStoppedSlot(logDBPath string, shardNum uint32, slotId uint32, start, end uint64) (state types.RaftState, logs []types.Log, err error) {
	err = withStoppedStorage(logDBPath, shardNum, func(p *PebbleShardLogStorage) error {
		var readErr error
		state, readErr = p.getState(SlotIdToKey(slotId))
		if readErr != nil {
			return readErr
		}
		logs, readErr = p.getLogs(SlotIdToKey(slotId), start, end, 0)
		return readErr
	})
	return
}

// withStoppedStorage collects every opened handle on success or partial open
// failure. Pebble takes exclusive directory locks even for read-only opens.
func withStoppedStorage(logDBPath string, shardNum uint32, read func(*PebbleShardLogStorage) error) (err error) {
	if shardNum == 0 {
		return fmt.Errorf("slot scan: shard count must be positive")
	}
	logDBPath, err = filepath.EvalSymlinks(logDBPath)
	if err != nil {
		return fmt.Errorf("slot scan: resolve storage domain: %w", err)
	}
	p := &PebbleShardLogStorage{shardNum: shardNum, path: logDBPath, Log: wklog.NewWKLog("slotScan")}
	defer func() {
		for _, db := range p.dbs {
			err = errors.Join(err, db.Close())
		}
	}()
	for i := 0; i < int(shardNum); i++ {
		db, err := pebble.Open(fmt.Sprintf("%s/shard%03d", logDBPath, i), &pebble.Options{ReadOnly: true, ErrorIfNotExists: true})
		if err != nil {
			return fmt.Errorf("slot scan: open shard %d: %w", i, err)
		}
		p.dbs = append(p.dbs, db)
	}
	return read(p)
}
