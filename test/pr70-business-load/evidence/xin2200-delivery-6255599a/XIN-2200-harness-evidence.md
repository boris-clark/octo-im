# XIN-2200: PR70 business-load harness, development and self-test evidence

Scope: harness development and self-test only. Self-tests ran only against disposable in-process mocks on 127.0.0.1. No deployed candidate, shared service, container, image, workflow dispatch, upstream branch or PR70 object was touched. Nothing was deployed and no acceptance was executed. This artifact is ready for independent review.

## 1. Exact artifact

| Item | Value |
|---|---|
| Repository / fork | `boris-clark/octo-im` (fork of `Mininglamp-OSS/octo-im`) |
| Test-only branch | `test/pr70-business-load-harness` |
| Commit | `6255599a3de8602b250f684826b0df19edc7b946` |
| Tree | `bf662e69ee75f77cddc89f1c1448f13113d268fa` |
| Parent | `bbc2be6125011ede5deaaca31b35abcb0480e567` (exact PR70 base) |
| Paths changed | 22 files added under `test/pr70-business-load/` only (+3270 / -0). No product code, config, workflow or Go file |
| Draft PR (fork-internal) | https://github.com/boris-clark/octo-im/pull/1 (draft; base `test/pr70-harness-base` pinned at `bbc2be61`; head `6255599a`) |
| Upstream | Nothing pushed. The local clone has the upstream push URL disabled. PR70 head `a9021588` / base `bbc2be61` were re-checked unchanged after delivery. PR71 was not involved |
| In-tree manifest | `test/pr70-business-load/MANIFEST.sha256` (sha256 of every file) |

Attached, verifiable without GitHub:

| File | SHA-256 |
|---|---|
| `pr70-business-load-harness.bundle` (git bundle `bbc2be61..6255599a`, `git bundle verify` okay) | `10f0e23fceeec5c4fc051cb79c98cfd5850e2b512f4d066081ba436f9c8aaafb` |
| `pr70-business-load-harness-6255599a.tar.gz` (`git archive` of the harness directory; no absolute or `..` paths, no symlinks) | `109ef7b12cd1bda980a972e180fbc5f504305bc388dfd31edcc7f1e6119c667c` |
| `selftest-receipt.txt` (full self-test output from a clean export of the commit) | `917952868f9f2ef67ab3708190a1fb8cfdd4224e47d523f993085f8cd0894677` |

Reviewer reproduction:

```sh
git clone https://github.com/Mininglamp-OSS/octo-im && cd octo-im
git fetch ../pr70-business-load-harness.bundle test/pr70-business-load-harness:review   # or fetch from the fork
git rev-parse review            # 6255599a3de8602b250f684826b0df19edc7b946
git diff --stat bbc2be6125011ede5deaaca31b35abcb0480e567 review   # only test/pr70-business-load/
cd test/pr70-business-load && shasum -a 256 -c MANIFEST.sha256 && ./selftest.sh 60
```

## 2. Provenance

The five recovered XIN-1968 files are byte-identical in `original/` with `SHA256SUMS`. A unit test asserts these hashes:
`harness.py 3d3a7fce…cc47d`, `postcheck.py 219696f6…daa15`, `poller.py cd739d91…ea0c`, `monitor_run.py b3139d55…d6aa`, `analyze.py dcafbaef…2e9a`. Source location: `460252cb/workdir/repro/`. They were rehashed before copying and match the XIN-2199 report (SHA-256 `c923ba5f…6ec72`, read before implementation). The XIN-1972 Go lineage (`loadharness_main.go 4b55d154…`) was consulted for the `/channel/info` upsert shape and the seeded-determinism idea, and was not copied.

PR70 contract was read from source at head and base. `git diff base..head` is empty for `internal/api`, `pkg/wkhttp`, `pkg/cluster/cluster/api_channel.go`, `server_api.go`, `internal/service/permission.go` and `pkg/wkdb/message.go`, so the harness is API-fair across S1/S2 and T1/T2. The old PR58 routes `/channel/subscriber_reconcile` and `/channel/subscriber_recovery` are never called; a test enforces this.

## 3. Historical-to-adapted semantic mapping (summary; full table in `ADAPTATION.md`)

| Historical | Adapted | Semantic difference |
|---|---|---|
| Seed: full reconcile snapshot per channel, paged at 128, revision + snapshot_id | Paged delta `subscriber_add` (`reset=0`) at 128 on fresh channels; abort before load on any non-ok page; `seedcheck` verifies 0→378 by independent read | PR70 has no snapshot atomicity or revision |
| Add/remove: mutate list, post full snapshot via reconcile | One delta `subscriber_add` / `subscriber_remove` per op (approved D1-a; never `reset=1`) | No revision fencing, stale rejection or idempotency key. About 1 uid per request instead of ≤150, so cost profile differs from PR58 and counts are not comparable 1:1 |
| Remove picks a current member | Picks from seed members + uids this worker added (state-independent); may be absent → server no-op | Keeps per-worker streams deterministic; slight size drift possible |
| Upsert: reconcile with random `large` | `POST /channel/info {large}` | Full channel-info upsert resets ban/disband/send_ban/allow_stranger to 0. `large` is not oracle-verified |
| Send: from current member | Same body; from seed member; deterministic `client_msg_no` | API sends use the system device, which bypasses sender permission (`permission.go:108-111`), so delivery is unchanged |
| Global `random.seed(42)` + shared RNG for opid/snapshot_id | `Random("<seed>:<role>:<wid>")` per worker; no global RNG (statically tested) | Per-worker op streams are byte-identical across processes and arms; interleaving and op count remain latency-dependent |
| `--token` on argv, optional | `--token-file` (0600 enforced); redacted from every output; `allow_abbrev=False` (argparse would otherwise silently expand `--token` to `--token-file`) | — |
| Single `--base`; 15001/15300 hard-coded | `--api` / `--manager` lists; worker g → `api[g % N]`, recorded in the run header | — |
| Sampled failure deque, PR58 409/503 classes | Complete gzip journal of every request; outcomes ok/rejected/unknown/not_sent; PR70 taxonomy | Historical 503 counts must not be compared with PR70 failures |
| `postcheck` recovery drain on `pending_units` | Bounded drain until every 200-accepted message is readable (`--drain-timeout` 600 s) | No recovery backlog API exists on PR70 |
| Readable = send returns 200 | Probe send **and** readback through `messagesync` | Write receipts are never used as evidence |
| Teardown by empty snapshot @ rev 50,000,000 | `subscriber_remove_all` per channel + independent empty-set read | — |

Unchanged: 42 × (1+8) = 378 channels; 150/20 seed members; 42 + 42 + 16 workers; 3600 s; 5 s budget / 6 s client timeout (7 s seed); payload `repro-load-message`; `red_dot=1`. No retries, no timeout inflation.

Claims not made, because PR70 has none: idempotent replay, revision fencing, snapshot atomicity, recovery backlog metrics, cross-worker per-channel ordering. HTTP 200 from `/message/send` means "queued" (the handler returns before persistence). The oracle still fails on any accepted-but-missing message, which is a stricter business expectation and is stated as such.

## 4. Oracle

* **Subscribers.** `GET <manager>/cluster/channels/:id/:type/subscribers` (`server_api.go:38`, `api_channel.go:242`; manager `token` auth) is queried on every manager node. All 200 answers must agree (`divergent_subscriber_read`). No duplicates are allowed. `confirmed - uncertain` must be present, and nothing outside `confirmed ∪ uncertain` may be present. A uid becomes uncertain only after a rejected or unknown write to it; this is sticky. Uncertain present/absent counts are reported.
* **Messages.** `POST /channel/messagesync` (pull_mode 1, from seq 1, 1000 per page, streamed) and `GET /channel/max_message_seq` are checked per channel:
  * seq starts at 1, is contiguous, and is strictly increasing;
  * max seq equals the last seq read;
  * no duplicate or unknown `client_msg_no`;
  * payload and from_uid match;
  * no rejected or not_sent send is persisted;
  * every accepted send is present;
  * unknown sends are counted present/absent and never fail the run.

  No global or cross-channel sequence is assumed.
* **0→378→0.** `preflight` requires every channel to have zero subscribers and seq 0 (stale-state detection). `seedcheck` verifies 378 seeded channels. `teardown` runs `remove_all`, then verifies empty sets and does a probe readback.
* **Restart.** The `restart` mode checks subscriber sets, requires the readback prefix to be sha256-identical to the pre-restart snapshot, and allows only this restart's probes after it. The snapshot rolls forward, so T3 can check follower then leader restarts.
* **Verdict.** FAIL / INCONCLUSIVE (read errors only) / PASS, with exit codes 1 / 3 / 0. Read failures are never folded into PASS.
* **Scale.** The ledger uses about 7 bytes per send and readback is streamed: 3M synthetic sends load in about 10 s at about 61 MB RSS.

## 5. Self-test receipts (`selftest-receipt.txt`, clean `git archive` export of `6255599a`, Darwin arm64, Python 3.9.6)

* Manifest: 21/21 OK. Originals: 5/5 OK.
* `python3 -I -m unittest discover -s tests -v`: **Ran 33 tests, OK**, zero warnings. They also pass under `-W error::ResourceWarning`.
* Full default-shape dry run (378 channels, 100 workers, 60 s load) against the mock, run once with 1 node and once with 3 nodes. Each run executes preflight → seed (420/420 pages ok) → seedcheck → load → post → restart1 → restart2 → teardown → analyze. Every oracle mode was **PASS** with rc=0, and `token_leaks: []`.

Deterministic negative controls (mock fault → required oracle signal):

| Control | Result |
|---|---|
| Missing subscriber (add acknowledged, not stored) | FAIL `missing_subscriber` = 6 |
| Unexpected member (phantom uid) | FAIL `unexpected_subscriber` = 6 |
| Duplicate subscriber in read | FAIL `duplicate_subscriber` = 6 |
| One node diverges (3 nodes) | FAIL `divergent_subscriber_read` = 6 |
| Stale pre-existing channel | preflight FAIL `stale_state_subscribers` = 1 |
| Accepted message dropped | FAIL `missing_accepted_message` = 1, drain not drained |
| Message stored twice | FAIL `duplicate_message` = 1 |
| Seq gap | FAIL `seq_gap` = 1 |
| Send answered 500 but stored | FAIL `rejected_but_persisted` = 1 |
| Foreign message appears after restart | FAIL `new_message_after_restart` = 1 + `unexpected_message` = 1 |
| Last message per channel lost on restart | FAIL `restart_readback_changed` = 6 + `missing_accepted_message` |
| Unhealthy `/health` | FAIL `unhealthy_endpoint` |
| Message reads fail (500) | **INCONCLUSIVE** (exit 3), not PASS |
| Client timeouts (server 2 s > client 1.2 s) | unknown outcomes recorded and uids uncertain; no false FAIL |
| HTTP 500 and apply-then-500 writes | `rejected` recorded with class `http_500`; no false missing/unexpected |
| Connection refused | `not_sent` / `conn_refused` |
| Wrong token | seed all `http_401`, harness exits 2 before load |
| Group-readable token file / `--token` on argv | refused |
| Token in any state file, journal, report or CLI output | absent |
| Determinism | identical streams for the same seed across processes and `PYTHONHASHSEED`; different streams per seed/worker |
| Accounting | journal records = per-kind n = sum of outcomes; no stuck workers |

## 6. Commands and budget for the downstream campaign

The full per-arm procedure is in `README.md`. In short, each arm needs a fresh data directory, its own `--state-dir` and an alphanumeric `--run-id`, and runs:

```
oracle preflight → harness --phase seed → oracle seedcheck → (poller --seconds 5400 &, monitor_run --max-seconds 4000 &) → harness --phase load (3600 s) → oracle post → [graceful restart → oracle restart --restart-index k] → oracle teardown → analyze
```

Endpoints from XIN-2195 are single API 25195 / manager 25295 and three-node API 26195–26197 / manager 26295–26297. Base deployments use ports chosen by the deployment-agent. The token is a 0600 file passed by `--token-file`.

| ID | Envelope | Target | Procedure |
|---|---|---|---|
| P0 (only if safe/isolated) | single 2C/4G | disposable container from `octo-im:pr58`, fresh data dir | `original/harness.py --base … --seconds 900` + `original/monitor_run.py`. Stop at first timeout. Instance must have no manager token (original takes argv token only) |
| S1 | single 2C/4G | head | full arm |
| S2 | single 2C/4G | exact base | full arm, same seed and harness commit |
| S3 | single 2C/4G | head after S1 | graceful restart → `oracle restart` → `teardown` |
| T1 | three, 1C/2G each | head | full arm (`wid % 3`) |
| T2 | three, 1C/2G each | exact base | full arm |
| T3 | three, 1C/2G each | head | load with graceful follower restart at ~T+30 and leader restart at ~T+45. Identify nodes via `GET <manager>/cluster/nodes` and record it. Then `post` |
| T4 | three, 1C/2G each | head after T1 | stop and start all three gracefully → `oracle restart` → `teardown` |

Graceful restart means SIGTERM with a long grace period (for example `docker stop --time 120`). Exit code 137 means the stop escalated to SIGKILL; such a restart is invalid and must be reported, not silently re-run. Never use SIGKILL.

Compare head with base only within the same topology and envelope. Make no claim of equivalence to the historical 2C/4G clusters.

Budget is unchanged from the approved estimate. Each arm takes about 85–95 min: about 3 min preflight/seed, 60 min load, 5–15 min post including the drain bound of 10 min, 5–10 min restart and 3 min teardown. P0 takes about 25 min, S1–S3 about 3.2 h, T1–T4 about 5 h, and base builds/deploys about 1 h, for a total of **about 9.5–10 h sequential**. The design adds no arm or duration. The only scale-dependent cost is post readback, at one request per 1000 accepted messages per channel.

## 7. Side effects and deviations to note

* Opening the fork-internal Draft PR triggered three `pull_request_target` automations from the fork's default branch: PR Labeler (success, added label `size/XL`), PR Title Lint (success) and Auto add to project (failure). The last one failed with `Input required and not supplied: github-token`, so nothing was added to the `Mininglamp-OSS` project. No build, image or publish workflow ran. Upstream CI was not involved.
* The self-tests use a mock that models PR70 response shapes as read from source. The mock does not prove anything about the real server. On the real cluster, the manager subscriber route forwards non-leader reads to `ApiServerAddr + /cluster/...`. Whether that forward resolves depends on deployment config, so per-node forward errors are reported as `subscriber_read_node_error:<ep>` (info). The read still succeeds when the slot leader's own manager answers. All nodes failing → INCONCLUSIVE.
* Product residuals XIN-2183, XIN-2187 and XIN-2188 remain unwaived.

Next step: independent reviewer. No self-approval, deployment or acceptance execution was done.
