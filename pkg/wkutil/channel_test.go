package wkutil

import "testing"

func TestChannelKeyPreservesCompleteIdentity(t *testing.T) {
	for _, id := range []string{"ordinary", "room&history", "a&&b"} {
		for _, typ := range []uint8{0, 2, 3, 255} {
			got, gotType := ChannelFromlKey(ChannelToKey(id, typ))
			if got != id || gotType != typ {
				t.Fatalf("channel identity changed: %q/%d -> %q/%d", id, typ, got, gotType)
			}
		}
	}
}
