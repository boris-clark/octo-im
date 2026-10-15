# PR70 business-load harness (test-only)

This is the XIN-1968 business-concurrent workload, adapted to the HTTP contract that exists
in PR70 head `a9021588` and base `bbc2be61`. Nothing here is built into, or
imported by, the server. The historical files are kept verbatim in `original/`.
`ADAPTATION.md` has the full historical-to-PR70 mapping and lists every
semantic difference. `evidence/` keeps earlier delivery and review evidence byte-for-byte.

Requirements: Python 3.9+ standard library only. No third-party packages.

## Layout

| Path | Purpose |
|---|---|
| `harness.py` | seed (0 -> 378 subscriber sets) and 3600 s load: 42 add/remove + 42 send + 16 channel-info workers |
| `oracle.py` | independent checks: `preflight`, `seedcheck`, `post`, `restart`, `teardown` |
| `poller.py` | bounded per-node `/metrics` + `/varz` sampler |
| `monitor_run.py` | bounded event watcher on `repro_run.jsonl` |
| `analyze.py` | per-arm summary from the journal, metrics, operator-captured logs (streamed) and oracle reports |
| `wkload/` | shared client/outcome model, dataset, deterministic streams, possible-state world model |
| `mock/` | disposable mock with leader forwarding, full-shape dry run, mutation controls; self-tests only |
| `tests/` | unit tests and deterministic red/green/limitation controls |
| `original/` | verbatim XIN-1968 files + `SHA256SUMS` |
| `evidence/` | preserved earlier evidence (XIN-2200 first delivery, XIN-2204 review) + `SHA256SUMS` |
| `selftest.sh` | runs everything above against local mocks |

## Self-test (no service contacted)

```sh
./selftest.sh            # hashes, unit/negative controls, mutation controls, 1-node and 3-node full-shape dry runs
```

## What a verdict means (observation limits)

- **Leader state only.** On PR70 every public read is served by the leader. Subscriber reads go to the
  slot leader: non-leaders forward (`pkg/cluster/cluster/api_channel.go:242-268`). `messagesync` and
  `max_message_seq` go to the channel leader. Follower replica state is not observable through these
  routes, and the harness does not claim to verify it.
- **Cross-node checks.** These verify forwarding consistency. Every configured node must return the
  same answer. Any node read error, an incomplete endpoint list or a mismatched `--api`/`--manager`
  pair is a read error, and the result is INCONCLUSIVE.
- **Leader records.** The serving slot leader and channel leader of every channel are recorded per
  mode in `leaders_<mode>.json`. Restart modes report how many slot leaders moved.
- **Subscriber scope.** Each subscriber check reports its coverage: certain-present, certain-absent
  and ambiguous keys. A uid with an ambiguous write (rejected or unknown, except a provable no-effect
  such as `not_sent` or HTTP 401) is constrained only to the states that write could produce. If the
  ambiguous share of touched keys exceeds `--max-ambiguous-ratio` (default **zero**), the result is
  INCONCLUSIVE instead of PASS. The threshold is pre-declared and recorded in every report. Changing
  it to any nonzero value requires an Owner decision; no nonzero allowance is approved. The earlier
  1% proposal is pending, not policy. A single ambiguous key makes the default result INCONCLUSIVE.
  Reports include absolute `ambiguous_keys` in successfully read channels, the touched-key ratio, `non_ok_load_writes` and
  `ambiguous_keys_per_non_ok_load_write`. The latter divides final ambiguous keys by the number of
  `rejected` + `unknown` + `not_sent` load requests across subscriber add/remove, message send and
  channel-info upsert. Seed, oracle probes and teardown writes are excluded. `load_write_accounting`
  records the complete outcome counts, total, non-OK request share (`non_ok_ratio`) and journal availability. This keys/requests ratio is
  descriptive, not a probability or causal attribution; repeated writes, seed ambiguity and narrowed
  states mean it need not be at most one. With zero non-OK load writes it is JSON `null`, not zero.
- **Strict teardown.** After successful `subscriber_remove_all`, every served subscriber set must
  be empty, including uids with ambiguous load-era adds. Any survivor fails
  `not_empty_after_teardown`, even with an explicitly widened scope. Non-OK remove-all fails
  `teardown_not_ok`. The possible-state model and its coverage remain reported; scope errors can
  still make an empty teardown INCONCLUSIVE. All load workers must have stopped before teardown.
- **0 -> 378 -> 0** refers to subscriber sets. Channels and channel cluster configs are created by
  the workload, or by the preflight readback, and are never deleted.
- **Timeouts.** The client timeout (`budget + 1` = 6 s; 7 s for seed; 8 s for oracle reads) is one
  wall-clock deadline for the whole request: connect, send, headers and body. The body is read with
  `read1`, and the socket timeout is re-armed to the time remaining before each step. A server that
  trickles response *headers* can exceed the deadline by at most one remaining-time window per
  header line. The value is unchanged from the historical policy.

## Operator procedure

Every arm starts on a fresh data directory, with its own new private `--state-dir` and an
alphanumeric `--run-id`. The token file must be mode 0600. Pass every node of the topology, in the
same order, to both `--api` and `--manager`. Arms run strictly sequentially.
Run each oracle mode once per state directory (restart uses a new `--restart-index` each time):
re-running a mode re-sends its probe client message ids and is not an independent check.

```sh
H=test/pr70-business-load
API=http://127.0.0.1:25195            # three-node: http://127.0.0.1:26195,http://127.0.0.1:26196,http://127.0.0.1:26197
MGR=http://127.0.0.1:25295            # three-node: http://127.0.0.1:26295,http://127.0.0.1:26296,http://127.0.0.1:26297
TOK=/path/to/private/manager-token    # 0600
ST=/path/to/evidence/<arm>            # new, empty, private
RUN=<arm>                             # alphanumeric, unique per arm
C="--api $API --token-file $TOK --state-dir $ST --run-id $RUN"
O="$C --manager $MGR"
```

Building blocks:

| Step | Command |
|---|---|
| PRE | `python3 -I $H/oracle.py preflight $O` (must PASS) |
| SEED | `python3 -I $H/harness.py --phase seed $C` (exit 2 = a seed page failed: stop the arm) |
| SC | `python3 -I $H/oracle.py seedcheck $O` (must PASS) |
| OBS | `python3 -I $H/poller.py --manager $MGR --token-file $TOK --seconds 5400 --out $ST/metrics.jsonl &` and `python3 -I $H/monitor_run.py --state-dir $ST --max-seconds 4000 &` |
| LOAD | `python3 -I $H/harness.py --phase load $C` (3600 s) |
| POST | `python3 -I $H/oracle.py post $O` |
| GR | graceful restart (see the rule below), then wait until `/health` returns 200 on every node |
| RCHK k | `python3 -I $H/oracle.py restart --restart-index k $O` |
| TD | `python3 -I $H/oracle.py teardown $O` |
| AN | `wait; python3 -I $H/analyze.py --state-dir $ST --metrics $ST/metrics.jsonl --log <captured log per node> --json-out $ST/summary.json` |

Exact sequence per arm (one fresh deployment and data directory per row unless stated):

| Arm | Target / envelope | Sequence |
|---|---|---|
| P0 | disposable `octo-im:pr58`, single 2C/4G | original files only, see below |
| S1+S3 | head, single 2C/4G | PRE, SEED, SC, OBS, LOAD, POST, **GR** (single node), RCHK 1, TD, AN. S3 is the GR + RCHK 1 part of the same arm. |
| S2 | exact base, single 2C/4G | PRE, SEED, SC, OBS, LOAD, POST, TD, AN |
| T1+T4 | head, three nodes 1C/2G each | PRE, SEED, SC, OBS, LOAD, POST, **GR of all three nodes** (stop all, start all), RCHK 1, TD, AN. T4 is the full-restart part of the same arm. |
| T2 | exact base, three nodes 1C/2G each | PRE, SEED, SC, OBS, LOAD, POST, TD, AN |
| T3 | head, three nodes 1C/2G each, separate fresh deployment | PRE, SEED, SC, OBS, LOAD with a GR of one follower at about T+30 min and a GR of the current Config leader at about T+45 min (both while load runs; identify nodes with `GET <manager>/cluster/nodes` and record the output), POST, TD, AN. Requests that fail during the restart windows are recorded as `rejected`/`unknown`/`not_sent`; nothing is retried. Mid-load restarts are judged by POST, not by RCHK. |

The rolling restart snapshot supports more than one sequential RCHK on one arm, for example a
follower GR, RCHK 1, then a leader GR, RCHK 2. The matrix above uses one RCHK per arm.

Graceful restart rule: stop with SIGTERM and a generous grace period (for example
`docker stop --time 120 <ctr>`). Then read the exit code with `docker inspect`. Exit code 137 means
the stop escalated to SIGKILL, so that restart is invalid. Report the arm as invalid; do not re-run
it silently. Never use `docker kill` or SIGKILL.

Preflight side effect: the readback creates channel cluster configs for the 378 channels, because
`messagesync` calls `GetOrCreateChannelClusterConfigFromSlotLeader`. It writes no subscribers and no
messages. A fresh data directory per arm remains mandatory. Preflight detects stale state but cannot
replace a fresh deployment.

P0 positive control (only on a disposable container from the retained `octo-im:pr58` image, with a
fresh data directory; never reuse `wk-repro` data):

```sh
python3 -I $H/original/harness.py --base http://127.0.0.1:<pr58-port> --seconds 900   # writes /tmp/repro_run.jsonl
python3 -I $H/original/monitor_run.py                                                  # stop at "TIMEOUT REPRODUCED"
```

The original files are byte-identical to XIN-1968. They take the token only on argv, so P0 must
target an instance with no manager token configured.

Exit codes: oracle `0` PASS, `1` FAIL, `3` INCONCLUSIVE (read or scope errors). Harness: `0` ok,
`2` seed failure, `4` a worker or journal writer failed, did not stop, or did not finalize successfully. A non-ok request outcome is a
recorded result, not a harness error.

## Budget (unchanged from the XIN-2199 estimate)

| Step | Wall time per 60-min arm |
|---|---|
| PRE + SEED + SC | ~4 min (378 channels; per node: health, identity, leader probe, subscriber read, readback) |
| LOAD | 60 min |
| POST: drain (bounded 10 min) + full readback | ~5-15 min; readback is 1 request per 1000 messages per channel |
| GR + RCHK (S1+S3, T1+T4) | ~5-10 min plus the restart itself |
| TD | ~3 min |
| **Arm total** | **~85-95 min** |

Matrix: P0 about 25 min, S1+S3 and S2 about 3.2 h, T1+T4, T2 and T3 about 5 h, base builds and
deployments about 1 h. Total about 9.5-10 h sequential, as approved. The correction adds per-node
identity, leader and forwarding reads (about 378 x nodes small GETs per mode) and a preflight
readback. Neither changes the estimate materially.

Memory: the subscriber model is about 38 MB at most (one byte per channel and uid). Message
receipts, seen counts and stored-ID uniqueness use a disposable SQLite file with an 8 MiB page
cache, disk temp storage and a 16 GiB database cap. Reserve at least 16 GiB of scratch space per
active oracle in addition to journals/logs; exhaustion is INCONCLUSIVE. Data is streamed; no
allocation depends on an untrusted message sequence. Full-hour disk use and timing still require
campaign measurement; mock rates are not product capacity evidence.

Envelope rule: compare head against base only within the same topology and envelope (single
2 CPU / 4 GiB; three-node 1 CPU / 2 GiB per node). Do not compare three-node numbers with
single-node numbers or with the historical 2 CPU / 4 GiB results.

### Completed-load evidence and message identity (B4/B5)

Use a fresh state directory and unique run ID per arm. New runs record the run ID and payload in
the load header. Producers independently record per-kind/outcome totals and one SHA-256 chain per
worker over every request record, including client number, channel, sender and receipt message ID.
After all workers and the gzip writer finish, `world_final.json` records those chains, the writer's
record count, worker errors and `journal_complete`. The oracle reconciles every journal record
against this completed record and final counters before post/restart/teardown. Missing, empty,
truncated, malformed, partial or mismatched evidence is INCONCLUSIVE; no accepted-message ledger,
new snapshot, probes or teardown is derived from it. A seed-only seedcheck remains valid before
load. Older run directories without these receipts must not be accepted as complete new runs.

The gzip writer runs in a separate collected process with bounded socket backpressure: 5-second
send and 10-second close bounds, followed by process cleanup on failure. Failure reaches the
load exit status and final record. These are evidence-I/O limits, not changes to request timeouts,
workload, server restart procedures or the campaign budget. JSON evidence is limited to regular
files of at most 256 MiB. Journal reads enforce 1 MiB per record, 64 GiB decoded and a 30-minute
streaming scan bound; local filesystem stalls remain subject to OS behavior. Errors never PASS.

The product creates an int64 message ID for each unique unretried send and carries that ID to
storage (`internal/api/message.go`, `internal/channel/handler/event_persist.go`). The oracle keeps
receipt IDs for both load sends and probes. Every stored ID must be a positive int64, unique across
the full dataset, and equal to its acknowledged receipt ID. This independent check runs before
snapshot comparison and again after each restart. IDs need not be globally contiguous; sequence
checks remain channel-local. Unknown sends have no receipt-ID equality assertion, but their
stored IDs must still be valid and unique. Duplicate client numbers or receipt IDs invalidate
the evidence rather than silently replacing an earlier record.
