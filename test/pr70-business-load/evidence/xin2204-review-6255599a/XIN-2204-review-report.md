## Independent review: CHANGES_REQUESTED

Reviewed exact head `6255599a3de8602b250f684826b0df19edc7b946` (tree `bf662e69ee75f77cddc89f1c1448f13113d268fa`, parent/base `bbc2be6125011ede5deaaca31b35abcb0480e567`). Product contract read at PR70 head `a90215884e730a5761f307ea005acb264d477ab6`. `git diff bbc2be61 a9021588` is empty for `internal/api`, `pkg/wkhttp`, `internal/service`, `pkg/wkdb`, `pkg/cluster/cluster/{api_channel,server_api}.go`, so the API contract is identical at base and head.

Provenance, scope, workload shape, token handling and the self-tests check out. Two oracle soundness problems block approval, because the oracle can return PASS while state is wrong.

### Blocking

**B1. The node-divergence check never reads replica state, and it passes when nodes are unreadable or left out.**
- `GET /cluster/channels/:id/:type/subscribers` (`pkg/cluster/cluster/api_channel.go:242-268`): on any node that is not the slot leader, the handler forwards to `leader.ApiServerAddr`. Only the slot leader reads its local DB. So on a real cluster, every manager answer is the leader's answer, and `divergent_subscriber_read` cannot detect replica divergence.
- `POST /channel/messagesync` (`internal/api/message.go:1198-1215`) and `GET /channel/max_message_seq` both forward to the channel leader. The oracle also only queries `apis[0]`.
- On top of that, `Oracle.subscribers()` (`oracle.py:203-227`) turns per-node read errors into `info` only. Nothing checks that `--manager` covers the topology.
- Reviewer controls against the mock, 3 nodes, with node 1 diverged:
  - node 1 left out of `--manager`: **PASS** rc=0;
  - node 1's manager unreachable: **PASS** rc=0, with only `subscriber_read_node_error` in info.
- Required changes:
  - (a) Fail closed. If any configured manager node has a read error, the result is INCONCLUSIVE. Require one `--manager` per `--api`.
  - (b) Stop claiming replica or node divergence coverage in the README, ADAPTATION and evidence. On PR70, the public APIs expose only leader state. Describe the check as forwarding consistency.
  - (c) Record which slot or channel leader served each read (`/cluster/nodes`, or the slot leader), so that T3/T4 can show that reads after a restart came from a different leader.

**B2. The sticky uncertain set hides real loss after rejected or unknown writes.**
- `SubscriberModel.apply` (`wkload/world.py:117-130`) moves a uid into `uncertain` permanently after any rejected or unknown write, whichever direction the write was. After that, `check_subscribers` neither requires the uid to be present nor forbids it.
- A rejected or unknown **add** cannot make a confirmed-present uid absent. A rejected or unknown **remove** cannot make a confirmed-absent uid present. The model throws that information away.
- Reviewer control: a seeded uid gets a rejected add (no effect), and then the store drops the uid. Result: **PASS** rc=0, with only `uncertain_uid_absent=1` in info.
- The campaign targets propose-timeout behaviour, which is exactly when 400 and unknown outcomes are common. In that regime the oracle loses coverage quietly, at the moment it matters most.
- Required changes:
  - Track the possible states per (channel, uid): the final certain state plus the effect of each uncertain op. A uid must be present if it is present in every possible state, and must be absent if it is absent in every possible state.
  - Report a coverage figure, the share of (cid, uid) keys that are uncertain. Make a pre-declared threshold, or at least a prominent figure in the verdict, part of the arm result.

### Non-blocking (fix or disclose)
- **N1. Restart and T3 wording is inconsistent.** The delivery says the rolling snapshot supports a T3 follower-then-leader sequence, but the README runs T3 restarts mid-load with only `post` afterwards. The README also lists S3 as "after S1", while the generic procedure runs `teardown` before any S3 restart. State one exact sequence per arm.
- **N2. "0→378→0" counts subscribers, not channels.**
  - `subscriber_add` creates channels.
  - `messagesync` creates channel cluster configs through `GetOrCreateChannelClusterConfigFromSlotLeader`.
  - There is no channel delete.
  - So teardown proves empty subscriber sets only. Say so.
- **N3. Preflight can miss stale messages.** `max_message_seq` uses `LeaderOfChannelForRead`, which returns 0 when no channel config is loaded. On a reused or restarted data directory, preflight can therefore miss stale messages. Fresh data directories stay mandatory.
- **N4. The 6 s client timeout is per socket operation, not a total deadline.** That is a `http.client` property. Either document it or enforce a wall-clock deadline.
- **N5. `analyze.py` reads whole server logs into memory** (`f.read()`). Stream them for full-hour multi-node logs.
- **N6. The mock does not cover the forwarding gaps above.** It uses HTTP 500 for errors where the real server returns 400 (harmless, since any non-200 is rejected), lets every node read a shared store, and does not reset flags on `/channel/info`. Green mock runs say nothing about B1.
- **N7. Side effects to disclose.**
  - The fork PR body links Mininglamp-OSS/octo-im#63, which created a `cross-referenced` event on that upstream issue. Nothing was pushed upstream, but the delivery does not mention this event.
  - The fork `Auto add to project` run failed because no token was supplied. It had no effect beyond the fork, and it is not a test result.

### Verified
- **Live state:** PR #1 is open, draft, head `6255599a`, base ref `test/pr70-harness-base` = `bbc2be61`. PR70 head `a9021588` and base `bbc2be61` are unchanged.
- **Hashes:** bundle `10f0e23f…aaafb`, archive `109ef7b1…c667c`, receipt `91795286…94677`, report `953400a4…92c` all match. `git bundle verify` passes.
- **Archive:** commit id `6255599a`; contents are identical to `git archive` of the commit; no absolute paths, `..` paths or links.
- **Scope:** one commit, 22 files, all under `test/pr70-business-load/`.
- **Originals:** all five files are byte-identical to the recovered mini2 `repro/` sources and to `original/SHA256SUMS`. `MANIFEST.sha256` is 21/21 OK.
- **Workload defaults:** 42×(1+8)=378 channels, 150/20 seed members, 42/42/16 workers, 3600 s, 5 s budget, client timeout 6 s (7 s for seed). Per-worker `Random("seed:role:wid")`, endpoint `gwid % N`, no retries. The token is read from a 0600 file and redacted, and `--token` is refused.
- **Product semantics match the harness assumptions:**
  - subscriber add/remove/remove_all wait for apply on the slot leader;
  - adding a present uid or removing an absent uid is a no-op;
  - an add can return 400 after the store step;
  - `/channel/info` does not touch subscribers;
  - `/message/send` is asynchronous, and its errors happen before enqueue;
  - seq equals the raft index and is contiguous;
  - `more` is set when `len >= limit`.
- **Clean-export `./selftest.sh 60` (Python 3.9.6):** 33/33 OK; the 1-node and 3-node full-shape dry runs PASS in every mode; `token_leaks: []`; rc=0.

Formal review submitted on exact head `6255599a`.

### Commands run (reviewer, local only)
```
octo-daemon attachment download <5 delivery attachments>; shasum -a 256 *
git bundle verify pr70-business-load-harness.bundle; git fetch <bundle> test/pr70-business-load-harness:review
git rev-parse review review^{tree} review^; git diff --name-only bbc2be61 review
git ls-remote https://github.com/boris-clark/octo-im.git test/pr70-business-load-harness test/pr70-harness-base
gh api repos/boris-clark/octo-im/pulls/1 ; gh api repos/Mininglamp-OSS/octo-im/pulls/70
tar -tzvf <archive>; diff -r <archive extract> <git archive review>; git get-tar-commit-id
shasum -a 256 -c MANIFEST.sha256; (cd original && shasum -a 256 -c SHA256SUMS); cmp vs 460252cb/workdir/repro/*
git archive review test/pr70-business-load | tar -x; ./selftest.sh 60
python3 -I controls.py <clean export>       # reviewer mutation controls X0-X3
git show a9021588:pkg/cluster/cluster/api_channel.go; git show a9021588:internal/api/message.go (+ channel.go, wkdb, raft idempotency)
```

### Reviewer controls (controls-result.json)
| Control | Expected from a sound oracle | Actual |
|---|---|---|
| X0: 3 nodes, node 1 diverged, node 1 left out of `--manager` | INCONCLUSIVE or FAIL | PASS rc=0 |
| X1: 3 nodes, node 1 diverged, node 1 manager unreachable | INCONCLUSIVE | PASS rc=0 (info only) |
| X2: confirmed uid, rejected add, then the uid is lost | FAIL missing_subscriber | PASS rc=0 (`uncertain_uid_absent=1`) |
| X3a: paging at PAGE=2 with `more` | PASS | PASS (pagination sound) |

### Limitations
- Only the mock and source code were used. No live service, deployment or workload run.
- The product semantics come from reading source at a9021588. They were not observed on a cluster.
- The full-hour feasibility estimate is analytical: about 7 B per send in the ledger, streamed readback, a bounded journal queue, latency deques capped at 200k per kind, and a 600 s drain bound. It is consistent with the approximately 9.5–10 h budget, but it was not executed.

### Evidence hashes (reviewer)
- controls.py `e256417e838d33da9270733d36e1cc07fe1e0274b941e259a7e26a63fdcf609e`
- controls-result.json `6d0f6d87e1f1a651d6b0a67388ec0ea3c504d053d0310a32e4dac4135ed8b808`
- selftest-reviewer.out `6983467d94f2884230e006c8fe5d8427421c2a093baabfe907d25e4e2f1cff1e`

GitHub formal review: id 5529927495, state CHANGES_REQUESTED, commit 6255599a3de8602b250f684826b0df19edc7b946, reviewer Jerry-Xin (non-author).
