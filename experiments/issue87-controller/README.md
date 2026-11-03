# Controller installation continuation (XIN-2641)

This increment starts at `679e051c09c8d8e2b461d38d85e761980a1120ed`
and updates the existing Draft PR #3. The old-source comparison remains
`bdfbce6951787f58706e1ce15f0cfb1bf02252df`.

## Production behavior

Channel term changes now collect durable fences from the exact applied voter
configuration before conditional publication. Recovery can skip unused terms
when hard state is ahead of metadata, but every counted report must fence the
same selected term. Callbacks return the complete applied identity, including
installation markers and the derived row id. A lost version race reloads the
winner; transient fence persistence stays retryable within the callback budget.

Dormant replicas reconstruct passive membership for fencing without first
adopting the old leader. Recovery selects the freshest eligible fenced voter,
which can differ from the leader recorded in the old metadata. A switch RPC
must match a fresh applied-authority read; an uncommitted peer snapshot cannot
install a leader. The legacy wire format's omitted derived row id is resolved
from its channel key, without relaxing any authority field.

A settled legacy channel establishes its boundary while its old leader can
still witness the retained history. Existing legacy expansion finishes before
that identity is frozen. This ordering preserves the existing orphan migration
and promotion path. It does not authorize arbitrary membership changes after
installation or certify unavailable, ambiguous legacy history.

Newer peer pings trigger a metadata reload on existing followers. The ping is
only a hint and cannot bypass the persisted fence. These reload workers join
shutdown before storage ownership is returned; an expired join reports
`ErrIncompleteQuiescence`.

Multi-voter channels with an uncommitted old-term tail now request a supported
current-term barrier. Store, quorum commit and apply remain distinct. A sync
boundary at or beyond a shorter follower's tail deletes no local row and
continues the handshake. A real deletion across applied/committed or protected
legacy history remains fenced.

## Validation and controls

All original test files are unchanged. Raw commands, original copies, adapter
diffs, source hashes and logs are attached to XIN-2641.

| Coverage | Result |
| --- | --- |
| Unchanged real TCP/Pebble stopped-leader failover, new writes and old-leader restart | Pass |
| Busy-slot publication with 0/1/2/4 writers, notification/scan/read-hint reconciliation, foreground version race | Pass |
| Unchanged legacy orphan expansion and competing maintenance decisions | Pass |
| Dormant RPC recovery, freshest fenced voter, forged switch RPC refusal, callback persistence retry | Pass |
| Multi-voter retained tail, shutdown join, cache install/reopen, exact dormant identity | Pass |
| Fixed seeds 7/2095/2113 with an explicit supported recovery entry | Pass |
| Stream comparison after both queried followers apply; held-second-voter control | Pass |
| Fresh old-source stores: all six `TestPinnedOldStore*` cases | Pass |
| Fresh old-source applied-tail retry/commit/apply/reopen under race detection | Pass |
| Focused channel/Raft/types/cache race checks | Pass |
| Combined controller/network race checks | Red; startup/client stacks also reproduce on the old base; trace cleanup remains separately unresolved |
| Old executable reading the newly written hard state | Unsupported: rejects length 105 |

Frozen-start negative controls reject the new recovery requirements or accept
the forged switch that the corrected handler refuses. Removing the follower
reload hook makes the unchanged stopped-leader failover test red. These controls
change no terminal assertions.

The stream control holds node 2's append while nodes 1 and 3 complete a real
quorum. Row 13 is absent on node 2 until its append is released, then all message
columns agree after apply. The original test waits only for node 3 before
reading node 2; its sequence-sensitive raw failure remains preserved. The
additional converged variant retains every original terminal assertion.

## Open contracts and acceptance gates

The following raw tests remain red and have not been waived:

- `TestConfigRecoveryRepairsDurableTermAndReads` attempts to wake an installed
  old term below overwritten hard state. Recovery must first publish through
  the authority, as exercised by the new dormant RPC tests.
- `TestConfigDelayedRoleCallbackCannotOverwriteNewMetadata` combines a term
  change with a new learner without an install. Membership transition coverage
  after installation remains open.
- `TestChannelConfigReconcileRPC` directly overwrites leader/membership without
  the conditional install. That write does not become applied authority.
- `TestConfigRoleCallbackWaitsForResume` expects new metadata to exist while
  pre-install fence persistence is failing. The new test instead verifies that
  publication waits for the durable fence, then resumes successfully.
- Restored-tail `/true` expects one of three applied replicas to certify a
  duplicate without a quorum. Local applied progress cannot certify that result.
- The original fixed-seed driver changes terms and waits for the old tail
  without supplying a supported current-term entry. Its additional recovery
  variant preserves cancellation, exact identities and all outcome accounting.
- Both conflict fixtures mark seeded history applied and then expect it to be
  truncated. The applied-prefix guard remains intact.
- The original cache integration fixture requests an unconditional same-term
  leader overwrite. The additional cache test uses the full expected identity,
  verifies stale refusal and checks the result after reopen.
- The opt-in original stale-install oracle still fails during its requested
  truncation setup. It has not reached or passed its terminal loss assertion.

No complete repair or approval is claimed. The earlier 81-package runtime
regression remains red and was not repeated wholesale. The broader 24-entry,
shared-domain ownership and remaining independent acceptance gates remain
open. The old-store evidence establishes NEW-reading-OLD for disposable pinned
fixtures; it does not establish OLD-reading-NEW, mixed writers or rollback.

Project automation still requires an authorized owner/admin to supply the
missing `github-token`. No workflow, secret, permission, upstream PR, Ready,
merge, deployment or live-store operation is part of this increment.
