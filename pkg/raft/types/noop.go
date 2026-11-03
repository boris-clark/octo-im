package types

import "bytes"

// NoopData identifies a replication barrier, never an application message.
// Storage adapters must explicitly support this entry before proposing it.
func NoopData() []byte { return []byte("WKRAFT-NOOP\x01") }

// IsNoop recognizes only the reserved entry with no application identity.
func IsNoop(log Log) bool { return log.Id == 0 && bytes.Equal(log.Data, NoopData()) }
