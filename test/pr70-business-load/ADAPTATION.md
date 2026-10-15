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
| Remove target | random current member (state-dependent) | random uid from channel seed members + uids this worker added there (state-independent, deterministic); may already be absent | Removing an absent uid is a server no-op. Channel sizes can drift slightly versus historical. The pool is now an int array indexed by `randrange(len)`, which draws the RNG exactly like `choice(list)` in 6255599a; a test asserts the first 20,000 operations are identical |
| Upsert worker | reconcile with random `large` | `POST /channel/info {channel_id, channel_type, large}` (XIN-1972 lineage) | `/channel/info` is a full channel-info upsert: `ban`, `disband`, `send_ban`, `allow_stranger` are written as 0. The `large` flag is not oracle-verified (no read route is used for it) |
| Send worker | `from_uid` random current member, `red_dot=1`, payload `repro-load-message` | same body, `from_uid` random seed member, `client_msg_no = <run>-sWW-NNNNNNNN` | API sends use the system device id, which passes sender permission regardless of membership (`internal/service/permission.go:108-111`), so the from-uid choice does not change delivery |
| Concurrency | 42 + 42 + 16 = 100 closed-loop threads, 3600 s | identical defaults | Harness serialises writes to the same (channel, uid) so the model stays exact; different uids of one channel still hit the server concurrently |
| Budget | 5 s budget, client timeout 6 s (`budget + 1`) via `http.client` (per socket operation) | same values; no retries; no timeout inflation. The 6 s is enforced as one wall-clock deadline for the whole request (connect, send, headers, body) | Historically a slow trickling response could exceed 6 s; now it cannot, except header lines trickled individually, bounded by one remaining-time window per line. PR70 has no server-side 5 s subscriber budget; the client timeout is the only bound |
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

World model (corrected after XIN-2204 B2). Each (channel, uid) carries its
possible states. The encoding uses one byte per uid u00000..u99999 per channel,
about 38 MB at most. A write in direction d (add -> present, remove -> absent) has
one of three effects:

| Write outcome | Effect | Transition |
|---|---|---|
| `ok` (HTTP 200; applied on the slot leader before the reply) | applied | possible := {d}; earlier floating writes are kept |
| `not_sent` (TCP connect failed) or HTTP 401 (token middleware aborts before any handler, `internal/api/server_http.go:50-61`) | no effect | unchanged: prior known state preserved |
| any other `rejected`, or `unknown` | ambiguous | possible := possible + {d}; d also stays "floating" (may still land later) |

Why rejected is ambiguous: `subscriber_add` can return 400 after
`AddSubscribers` committed (conversation or tag update failure). A node that
forwards to the leader can answer with an error while the leader still applies
the write. A timed-out request can land at any later time.

The final possible states are the current possibilities plus every floating
direction. Narrowing is exact. A rejected or unknown add on a certainly-present
uid leaves it certainly present, so the XIN-2204 reproducer (confirmed uid,
rejected add, uid lost) now FAILs. A remove cannot create a member. An unknown
add followed by an ok add stays certainly present, while an unknown add followed
by an ok remove allows both states. `remove_all` applies the same rule to every
touched uid.

The oracle requires every uid whose only possible state is present to be
present. It requires every other uid to be absent: possible state "absent" only,
or never touched. Uids with both states possible are unconstrained and counted.
The share of ambiguous keys among touched keys is reported in every subscriber
check as the verdict scope. Above `--max-ambiguous-ratio` (default zero,
pre-declared, recorded in the report) the result is INCONCLUSIVE, never PASS.
No nonzero allowance is approved; the earlier 1% proposal remains pending an Owner decision.
Every check also reports absolute ambiguous keys in successfully read channels, non-OK load request count, and their ratio
(`ambiguous_keys_per_non_ok_load_write`). The denominator counts rejected, unknown and not-sent
subscriber add/remove, message-send and channel-info requests in the load journal, excluding seed,
oracle probes and teardown. Full outcome counts, total, non-OK request share (`non_ok_ratio`) and journal availability are recorded as
`load_write_accounting`. This is a descriptive keys/requests ratio, not causal attribution or a
probability; it may exceed one (for example with seed ambiguity). A zero denominator yields JSON
null. Counting streams the journal and does not retain requests in memory.

Teardown additionally requires strict empty served subscriber sets after successful remove_all,
regardless of load-era floating adds. A survivor fails `not_empty_after_teardown`; any non-OK
remove_all fails `teardown_not_ok`. Load workers have stopped before teardown. This strict
assertion does not change the B2 model or clear its reported ambiguity; an empty teardown with
ambiguous keys remains INCONCLUSIVE under the zero default.

## Oracle (replaces `postcheck.py`)

| Historical check | Adapted check |
|---|---|
| `/message/send` returns 200 on all 378 channels ("readable") | probe send on every channel **and** readback of that probe through `messagesync` |
| `subscriber_recovery` pending/runtime drains to 0 within 60 s / 180 s | bounded drain (`--drain-timeout`, default 600 s): every message whose send returned 200 becomes readable |
| none | subscriber set of every channel matches the possible-state model (above). The read goes through every configured node. On PR70 non-leaders forward it to the slot leader, so this is leader state plus forwarding consistency. Follower replica state is **not** observable and not verified. Any node read error, incomplete endpoint list or mismatched `--api`/`--manager` pair gives INCONCLUSIVE |
| none | topology: `GET /cluster/node` per endpoint and `GET /cluster/nodes` must cover the whole cluster, one node each |
| none | leaders: `GET /cluster/channels/:id/:type/config` (not forwarded; only the slot leader answers 200, the body carries the channel leader as `learder_id`) records the serving slot and channel leader of every channel per mode, and restart modes report how many moved |
| none | per-channel message accounting: seq starts at 1 and is contiguous within the channel, `max_message_seq` equals the last read seq, no duplicate `client_msg_no`, no unknown `client_msg_no`, payload and `from_uid` match, no message whose send was `rejected`/`not_sent` is persisted, every accepted (200) message is present; `unknown` sends are counted as present/absent and never fail the run |
| 378 -> 0 by empty snapshot at revision 50,000,000 | `subscriber_remove_all` on every channel, then strict empty subscriber sets, model coverage, probe + readback. No ambiguous load-era write excuses a survivor. "0" means empty subscriber sets: channels and channel cluster configs persist (no delete route is used; `messagesync` creates configs) |
| none | preflight: every channel has an empty subscriber set **and an empty `messagesync` readback**. `max_message_seq` alone answers 0 while the channel config is not loaded (`LeaderOfChannelForRead`), so it cannot prove there are no stale messages. Side effect: the readback creates channel configs. A fresh data directory per arm stays mandatory |
| `restart` mode: probe + recovery drain | `restart` mode: subscriber sets, readback prefix byte-identical (sha256 of `seq,client_msg_no,message_id`) to the previous snapshot, only this restart's probes may follow, full accounting. The snapshot is rolled forward, so one arm can check several sequential restarts; T3's mid-load restarts are judged by `post` (README, per-arm table) |

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

## Completed evidence and stored identity correction

B4 adds independent producer hash chains and counters, finalized writer counts and explicit
worker/writer completion. Post/restart/teardown reconcile them with the durable gzip journal before
performing their write/readback stages. Absent or incomplete evidence is INCONCLUSIVE, not an
empty accepted ledger. Seed-only seedcheck remains supported. Every failed attempt is retained.

B5 replaces sequence-indexed message arrays with disk-backed identities, seen counters and ID
uniqueness. Accepted sends and probes retain the actual receipt int64 ID and compare it against
independent stored readback, including after restart. Invalid/duplicate stored IDs and receipt
mismatch FAIL even if a fresh snapshot would otherwise bless them. This relies only on the
unique, unretried client-number workload and the verified product ID-to-storage contract; it
introduces no global contiguous-ID assumption. Resource and evidence limits are in README.
