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
