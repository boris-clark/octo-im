# Issue 87 first repair increment

This increment provides group-local storage ownership through worker effects and
terminal disposition, reserved construction on the owner, immutable single-use
store completion identity, current-term voter ACK filtering, split term batches,
and durable-prefix term publication. It does not complete issue 87.

`run.py` runs one exact command and records its exit, raw output, timing and hash.
`overlays.py` reconstructs the pinned baseline and prepares one-change removal
controls without editing the worktree. Use an evidence directory outside this
worktree, for example:

```sh
python3 experiments/issue87-repair/run.py --evidence ../issue87-repair-evidence \
  --name increment -- go test -race ./pkg/raft/raft ./pkg/raft/raftgroup \
  ./pkg/cluster/slot -run '^TestIssue87' -count=1 -v -timeout=60s
python3 experiments/issue87-repair/overlays.py ../issue87-repair-evidence/controls
```

The original sample/old-leader quorum commit/stale install/index-reuse oracle is
an explicitly open gate. It uses real Nodes with deterministic storage scheduling
and intentionally delayed Apply effects. It remains RED on both the pinned
baseline and this candidate:

```sh
ISSUE87_OPEN_SAFETY=1 go test ./pkg/raft/raft \
  -run '^TestIssue87StaleInstall' -count=1 -v -timeout=20s
```

The separate three-node test uses real RaftGroups, fresh Pebble stores, codec
transport, actual proposal/commit/apply and orderly restart. Store barrier tests
hold the real slot CommitWait boundary. These do not prove power-loss safety or
the missing fence/floor protocol.

Remaining work includes shared storage-domain ownership and bound raw storage
views/callbacks, complete startup error/ack delivery, authenticated durable
commit/floor production and recovery, atomic monotonic truncation, durable quorum
fencing and conditional install for slots and existing channels, and Stage 2
legacy compatibility. No historical-data or full repair acceptance is asserted.

The recorded broad race selection excludes the six previously established
failures: TestLogConflict1, TestLogConflict2, TestElection, TestPropose,
TestProposeBatch and TestApply. Two large property tests are omitted from the
bounded broad runs; they are not claimed as passing at the final head. No test
assertion or timeout is relaxed.

## Current-term channel recovery continuation (XIN-2632)

When an existing channel has an old-term suffix without an authenticated commit
floor, the single-voter configuration callback now admits a current-term no-op
and waits for its ordinary store completion and durable commit transition.
Direct message retries can resume the same path after a config caller expires.
Owner retries reuse an already buffered current-term entry. Stored or applied
history alone still cannot manufacture a committed floor.

The no-op uses log identity zero and the reserved payload `WKRAFT-NOOP` followed
by byte 1. Its index/term are persisted outside message rows under key namespace
`0x1B02`, together with its term boundary in one synced metadata batch. Recovery
and replication merge these entries with messages; application message scans
never expose a synthetic message. A barrier consumes a sequence number, so the
next real message can have a gap in its sequence. Mixed appends store segments
in log order so a failure cannot expose a later message before its barrier.
Colocated truncation removes discarded barriers and term rows in the same batch.

`recovery_barrier_test.go` in the channel and Raft packages covers term, store
and floor failures, caller expiry and direct retry, exact retained identity,
continued writes, two reopens, catch-up and a subsequent two-voter commitment.
`recovery_controls.py` prepares removal controls without modifying the checkout.
The existing config-resume assertions now expect the extra committed no-op;
they retain positive recovery and continued-write requirements. The original
test sources remain available at the continuation's starting commit.

This is a storage and replication format increment, not a compatibility
acceptance. Older readers do not understand barriers. Historic store behavior,
mixed binaries, sequence-gap consumers and operational adoption require a
separate acceptance gate. Ordinary message/term atomic append, cross-shard
truncation, shared-domain/E3/24-entry admission, full controller/RPC/startup
failure recovery, and the inherited unmodified loss oracles remain open.
