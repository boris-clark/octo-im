# Channel storage continuation for issue 87

Ordinary channel message rows, client indexes, tail and the supplied term
boundary now share one synced message-shard batch. Existing stores keep their
original term rows. On first use, a synced target batch copies the old term and
barrier metadata and publishes a complete-channel-identity layout marker.
Every term operation and barrier reader subsequently uses this canonical
layout; there is no dual-write or read fallback after publication.

The routing anchor records the immutable shard count. Per-channel markers also
bind the source and target shards. Missing new fields on an old store trigger
adoption, rather than classifying that store as damaged. Malformed legacy term
rows, malformed markers and canonical rows without a marker are refused.
An interrupted copy leaves the original source available for a retry.

Canonical terms use namespace 0x1B03, layout markers 0x1B04 and the routing
anchor 0x1B05. Barriers keep namespace 0x1B02 in the message shard. Original
application rows and legacy term rows are preserved during adoption. New
metadata changes, message writes and truncation serialize through a local
metadata mutex; throughput and shared-domain ownership still require review.

Truncation removes discarded messages, their secondary indexes, canonical term
rows and barriers in one synced batch, then invalidates tail caches. A request
naming the tail already retained removes nothing and continues the actual
follower sync handshake. Real cuts below the applied/committed floor or across
a legacy boundary retain their existing refusal and quarantine behavior.
Channel-key parsing preserves ampersands inside the complete channel ID.

## Reproduce the bounded storage checks

~~~sh
go test -p 1 ./pkg/wkdb ./pkg/wkutil ./pkg/raft/raft   -run '^(TestChannelLog.*|TestChannelKeyPreservesCompleteIdentity|TestRecoveryNoopTruncateKeepsFencedPrefix)$'   -count=1 -v -timeout=90s
python3 experiments/issue87-compat/run.py --output ../issue87-fresh-evidence
~~~

The runner exports exactly bdfbce6951787f58706e1ce15f0cfb1bf02252df, adds only
the fixture driver, and builds an old-code executable with the revision bound
at build time. It records source, driver, binary, fixture and raw-log hashes.
Each fixture directory must be new and empty. The driver writes valid receive
frames through the old APIs, closes and reopens with the old reader, and records
every original physical KV plus messages, subscribers, conversations, channel
membership, term rows and slot logs/state. A separate malformed profile
deliberately retains invalid headers for a refusal check.

The new-reader suite requires these old-written fixtures; its default skip when
the fixture environment is absent is not compatibility evidence. It activates
three production channel servers with unchanged three-voter membership over
in-process event transport, requires a new committed/applied proposal on every
node, reopens and checks retained content and progress. A one-voter fixture
covers a real current-term barrier and the resulting message sequence gap.
The `applied-tail` profile also restores a same-term applied-only tail on three
voters. Activation admits a fresh recovery entry beyond that tail, without
blocking the other voters from initializing. A partitioned retry cannot publish
a floor. After quorum returns, the retry waits for the actual barrier apply,
returns the original message identity, and is followed by a new proposal and
restart. Config recovery retains its target through caller cancellation, and
duplicate sends also wait when a durable floor is ahead of application.
Lagging and divergent stores are checked for retained data and no inferred
floor; these are read/reopen diagnostics, not automatic historical repair.
A missing-quorum proposal must leave the durable floor and applied index
unchanged through reopen. The 0-to-2-to-0 cycle covers subscriber membership,
not dynamic Raft voter membership.

All original physical bytes remain subject to exact comparison. Only channel
tail, applied index and hard state may change during the explicitly checked
progress transitions. Configuration comparisons use complete persisted bytes,
because JSON omits empty learner slices. The fixture configuration lookup reads
the old persisted configuration; it does not supply a certificate or test the
controller RPC. Existing blocked-store hooks also cover the atomic append port.
The direct-storage fixture transport registers endpoints after owner creation;
its original source and all assertions remain available in the delivery evidence.

## Compatibility and acceptance limits

This proves bounded NEW-reading-OLD behavior on disposable fixtures. Older
binaries cannot read the expanded hard state and do not interpret canonical
terms or barriers. Old-reading-NEW, mixed writers and rollback are unsupported.
No existing service, historical/live database, shared stack or container is
used by these tests.

The original raw loss oracles, E3 and 24-entry/shared-domain ownership,
controller/RPC/startup failure recovery, dynamic membership, network process
transport and full business acceptance remain separate gates. StrictMem tests
exercise WAL sync boundaries; they do not establish arbitrary hardware power
loss or operational adoption. Full regression and independent integrated
review must be reported separately. Keep the candidate PR in Draft.
