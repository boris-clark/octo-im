# Certified membership and delayed probes (XIN-2651)

This continuation starts at `d1e9a25b84e4d40774c852440c888682071326fb`
and updates only the existing Draft PR #3. The original comparison base stays
`bdfbce6951787f58706e1ce15f0cfb1bf02252df`.

## Behavior

Post-install membership changes are authority transitions. The current slot
owner serializes its local fence-to-publication decisions, rereads the full
applied identity, and collects durable witnesses from the old voters and any
registered learners. A new term stops an old voting majority. A majority of the
proposed voters, including the proposed leader, must already store the selected
old-voter tail. Learner registration, registered learner promotion/replacement,
and completion of a declared leader transfer have explicit shape checks.
Unregistered voters, duplicate members, stale versions, and stale witnesses are
rejected. Newly registered learners must be eligible joined online nodes.

Slot apply retains the expected-identity CAS and unconditional overwrite guards.
Only a validated membership witness opens the existing same-membership guard.
The witness binds reports to the old applied identity and is discarded after
apply; subsequent reads are not reusable membership certificates. The installed
identity, committed floor, truncation protection, and durable fence remain in
force. Capacity/version hints can reach metadata before a runtime; the voter
adopts freshly verified applied metadata on its owner before fencing.

A callback's requested election candidate is a hint. The authority selects from
fresh durable fences, returns its actual applied identity, and uses the normal
switch RPC. The startup order now constructs config Raft before TCP/event
readers. Client generation/heartbeat counters use atomics, and DB Close joins
its metrics collector before returning storage/trace ownership.

## Coverage

`TestCertifiedMembershipExpansionRPC` starts with an installed one-voter
channel, registers learners through ordinary controller maintenance, and lets
real catch-up callbacks promote three voters. It checks the complete message
identity, continued commit/apply, stale/forged refusal, and restart writes.
`TestCertifiedCompetingMembershipCallbacksRPC` sends two competing requests
through the remote authority RPC: exactly one publishes, the other is refused,
and the successful membership continues applying writes. Certificate unit tests
separately reject missing old/new majorities, lagging tails, stale identities,
wrong senders/terms, duplicates and unregistered voters; Pebble tests check CAS
competition and reopen without retaining the transient witness.

`TestDelayedProbeProductionInstallPreservesCommittedIdentity` uses disposable
three-node TCP/Pebble servers. Transport gates first leave real sampled tails
at stale=3 and fresh=2, independent of bootstrap slot-owner placement. The
ordinary last-log RPC decoder/reader executes at arrival, and the response bytes
are held. The old leader then commits identity 4004 at index 4 with the fresh
follower while the sampled candidate stays at 3. The actual role callback,
authority RPC, voter fence RPC, slot applied CAS and switch RPC must preserve
that acknowledged identity. Reconnected replicas apply further writes with
indices above 4; restarting the installed leader preserves identity and writes.
No direct selector or test-side install completes this schedule. A refusal or
timeout cannot satisfy its terminal assertions.

The removal control changes only `fenceChannelConfig` to use the production
sampled-probe selector without durable fencing. With the exact same test bytes,
it installs a leader missing acknowledged index 4 and fails the explicit
`SAFETY` identity assertion. The frozen `d1e9a25b` control also fails the new
post-install expansion requirement. All preexisting test files are unchanged.

Run the focused acceptance checks:

```sh
go test ./pkg/cluster/cluster -run '^Test(Certified.*RPC|DelayedProbeProductionInstallPreservesCommittedIdentity)$' -count=1 -v -timeout=120s
go test -race ./pkg/cluster/cluster ./pkg/raft/types ./pkg/wkdb -run '^Test(Certified.*RPC|DelayedProbeProductionInstallPreservesCommittedIdentity|Membership.*|ChannelConditionalInstall.*)$' -count=1 -v -timeout=120s
go test -p 1 -run '^$' ./...
python3 experiments/issue87-compat/run.py --output /tmp/issue87-fresh-compat
```

Task evidence contains exact commands/exits, source and immutable-test hashes,
untouched positive/failed-control logs, the one-function removal diff, fresh
pinned-old fixture reports, and the archive checksum. Tests cover TCP transport,
in-process ownership, and disposable Pebble stores; they do not establish full
business acceptance or historical repair.

## Unwaived gates

All four original controller contract tests were rerun and remain red. Original
fixed-seed/conflict/cache and restored-tail requirements remain unwaived, as do
E3, 24-entry/shared-domain ownership and complete independent acceptance. The
local publication guard does not claim to solve those ownership requirements.
The original raw loss drivers are preserved; this continuation adds the real
protocol schedule instead of weakening their setup/terminal assertions.

All six freshly generated pinned-old-source acceptance cases pass. A separate
fresh applied-tail fixture passes retry, new proposal, commit/apply and reopen
under race detection. This establishes only OLD data -> NEW service on those
disposable fixtures. OLD-reading-NEW, mixed writers and rollback remain
unsupported for the 105-byte hard state. Project automation still needs its
external `github-token` administration; no workflow/secret/permission change,
CI-green claim, Ready, merge, deployment or release is included.
