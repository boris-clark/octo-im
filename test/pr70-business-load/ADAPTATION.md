# Historical-to-PR70 semantic mapping

Source of truth for the historical side: `original/` (verbatim copies of the
XIN-1968 harness, hashes in `original/SHA256SUMS`, recovered from the executor's
task workdir `460252cb/workdir/repro/` on the deployment host). PR70 contract
read at head `a90215884e730a5761f307ea005acb264d477ab6` and base
`bbc2be6125011ede5deaaca31b35abcb0480e567`; `git diff base..head` is empty for
`internal/api`, `pkg/wkhttp`, `pkg/cluster/cluster/api_channel.go`,
`pkg/cluster/cluster/server_api.go`, `internal/service/permission.go` and
`pkg/wkdb/message.go`, so every route below behaves the same in both arms.

## Routes

| Historical call (PR58) | PR70 head/base | Adapted call | Evidence |
|---|---|---|---|
| `POST /channel/subscriber_reconcile` (seed, add/remove, upsert, teardown) | absent | seed: `POST /channel/subscriber_add`; add: `/channel/subscriber_add`; remove: `/channel/subscriber_remove`; upsert: `POST /channel/info`; teardown: `POST /channel/subscriber_remove_all` | `internal/api/channel.go:38-45` |
| `GET /channel/subscriber_recovery` (pending/runtime oracle) | absent | not called; replaced by the readback oracle and bounded drain below | |
| none (write receipts only) | `GET <manager>/cluster/channels/:id/:type/subscribers` | independent subscriber-set read, every manager node | `pkg/cluster/cluster/server_api.go:38`, `api_channel.go:242` |
| none | `POST /channel/messagesync`, `GET /channel/max_message_seq` | independent message readback | `internal/api/message.go:63-64,1154,1499` |
| `POST /message/send` | unchanged | unchanged plus deterministic `client_msg_no` | `internal/api/message.go:49,69` |
| `/metrics` on 15300, `/varz` on 15001 | manager port (`/metrics` is auth-exempt) | per-node manager `/metrics` and `/varz` | `internal/api/server_manager.go:81-120,142` |

## Workload semantics

| Aspect | Historical | Adapted | Difference / what is NOT claimed |
|---|---|---|---|
| Dataset | 42 groups `repro_gNNN` (type 2) x 8 threads `<g>____t1..8` (type 5) = 378; seed 150 / 20 | identical (defaults); `--channel-prefix` exists but default keeps the historical IDs | Different IDs hash to different slots, so arms must keep the default |
| Seed | one full authoritative snapshot per channel, paged at 128 with `snapshot_id`, `revision`, `Idempotency-Key`, 7 s timeout | paged delta `subscriber_add` (`reset=0`) at 128 per request on fresh channels, 7 s timeout; abort before load if any page is not ok | PR70 has no snapshot/revision/atomic page semantics. A paged seed is not atomic; `oracle.py seedcheck` verifies 0 -> 378 against the recorded model instead |
| Add/remove worker | mutate local member list by +-1, bump per-group revision, post FULL snapshot via reconcile | one delta `subscriber_add` or `subscriber_remove` for one uid (D1 option a, approved) | No revision fencing, no stale-write rejection, no idempotency key (PR70 ignores the header and has no dedup). Delta writes carry ~1 uid instead of up to 150, so per-request payload and storage cost differ from the PR58 snapshot load; numbers are not comparable 1:1 with XIN-1968 |
| Remove target | random current member (state-dependent) | random uid from channel seed members + uids this worker added there (state-independent, deterministic); may already be absent | Removing an absent uid is a server no-op. Channel sizes can drift slightly versus historical |
| Upsert worker | reconcile with random `large` | `POST /channel/info {channel_id, channel_type, large}` (XIN-1972 lineage) | `/channel/info` is a full channel-info upsert: `ban`, `disband`, `send_ban`, `allow_stranger` are written as 0. The `large` flag is not oracle-verified (no read route is used for it) |
| Send worker | `from_uid` random current member, `red_dot=1`, payload `repro-load-message` | same body, `from_uid` random seed member, `client_msg_no = <run>-sWW-NNNNNNNN` | API sends use the system device id, which passes sender permission regardless of membership (`internal/service/permission.go:108-111`), so the from-uid choice does not change delivery |
| Concurrency | 42 + 42 + 16 = 100 closed-loop threads, 3600 s | identical defaults | Harness serialises writes to the same (channel, uid) so the model stays exact; different uids of one channel still hit the server concurrently |
| Budget | 5 s budget, client timeout 6 s (`budget + 1`) | identical; no retries; no timeout inflation | PR70 has no server-side 5 s subscriber budget; the client timeout is the only bound |
| RNG | `random.seed(42)` global + per-worker `Random(1000/2000/3000+wid)`; `opid`/`snapshot_id` from the shared global RNG (not byte-deterministic) | `random.Random("<seed>:<role>:<wid>")` per worker, no global RNG anywhere, default seed 42 | Each worker's operation stream is byte-identical across arms (tested across processes). Cross-worker interleaving and op count in 3600 s still depend on server latency |
| Token | optional `--token` on argv | `--token-file` (must be 0600), never logged; every output passes a redactor; `allow_abbrev=False` so `--token` is rejected instead of being expanded | |
| Endpoints | single `--base`, hard-coded 15001/15300 elsewhere | `--api` / `--manager` comma lists; worker `g` uses `api[g % N]` (recorded in the run header) | |

## Accounting and error taxonomy

Historical `Metrics` kept a 400-entry sampled failure deque. The adapted
driver keeps the same counters and adds:

* a complete gzip JSONL journal of every request (`seed_journal`, `load_journal`);
* four outcomes per request: `ok`, `rejected` (HTTP answer other than 200, or a
  non-200 `status` field), `unknown` (no complete response; may have applied),
  `not_sent` (TCP connect failed; proven no effect);
* a PR70 error taxonomy (`client_timeout`, `conn_refused`, `conn_reset`,
  `http_<code>`, `http_<code>_propose_timeout`, `http_<code>_context_deadline`, ...).
  The PR58 classes (`fence_*_409`, `BUDGET_EXHAUSTED_503` `state:pending`)
  cannot occur on PR70 and are removed. Historical 503 counts must not be
  compared 1:1 with PR70 failures.

World model: a subscriber write with outcome `ok` updates the confirmed set;
`rejected` and `unknown` mark that uid uncertain for that channel (sticky,
because a timed-out request can still apply later, and PR70 can return an error
after `AddSubscribers` already committed). The oracle requires
`confirmed - uncertain` to be present and nothing outside `confirmed | uncertain`
to be present, and reports how many uncertain uids ended up present or absent.

## Oracle (replaces `postcheck.py`)

| Historical check | Adapted check |
|---|---|
| `/message/send` returns 200 on all 378 channels ("readable") | probe send on every channel **and** readback of that probe through `messagesync` |
| `subscriber_recovery` pending/runtime drains to 0 within 60 s / 180 s | bounded drain (`--drain-timeout`, default 600 s): every message whose send returned 200 becomes readable |
| none | subscriber set of every channel equals the recorded model; every manager node that answers must agree |
| none | per-channel message accounting: seq starts at 1 and is contiguous within the channel, `max_message_seq` equals the last read seq, no duplicate `client_msg_no`, no unknown `client_msg_no`, payload and `from_uid` match, no message whose send was `rejected`/`not_sent` is persisted, every accepted (200) message is present; `unknown` sends are counted as present/absent and never fail the run |
| 378 -> 0 by empty snapshot at revision 50,000,000 | `subscriber_remove_all` on every channel, then every subscriber set must be empty, then probe + readback |
| `restart` mode: probe + recovery drain | `restart` mode: subscriber sets, readback prefix byte-identical (sha256 of `seq,client_msg_no,message_id`) to the previous snapshot, only this restart's probes may follow, full accounting. Snapshot is rolled forward so T3 can check follower then leader restarts |

Sequence semantics used: WukongIM message seq is per channel. Nothing assumes a
global or cross-channel sequence. Within one channel, cross-worker order is not
claimed; `same_worker_order_inversion` is reported as information only.

Explicit non-guarantees. The harness does not claim or test these, because PR70 has none:
idempotent replay, revision fencing, snapshot atomicity, recovery backlog
metrics. HTTP 200 from `/message/send` means "queued" (the handler returns
before persistence). The oracle still treats an accepted-but-missing message as
FAIL. That is the business expectation, and it is stricter than the HTTP contract.

## Observability tools

* `poller.py`: same metric filter, plus slot/inflight/leader series. It samples
  every manager node and has a hard `--seconds` bound. The recovery sample is
  removed.
* `monitor_run.py`: takes the report path as an argument and watches PR70 write kinds.
  It announces the first non-ok outcome and is bounded by `--max-seconds`.
* `analyze.py`: classifies failures from the complete journal and gives
  non-ok outcomes per minute. It reports metric deltas per node, counts
  `propose batch until applied timeout` / `context deadline exceeded` / panic /
  fatal in operator-supplied logs, and lists oracle verdicts.
