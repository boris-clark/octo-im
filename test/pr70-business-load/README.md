# PR70 business-load harness (test-only)

This is the XIN-1968 business-concurrent workload, adapted to the HTTP contract that exists
in PR70 head `a9021588` and base `bbc2be61`. Nothing here is built into, or
imported by, the server. The historical files are kept verbatim in `original/`.
`ADAPTATION.md` has the full historical-to-PR70 mapping and lists every
semantic difference.

Requirements: Python 3.9+ standard library only. No third-party packages.

## Layout

| Path | Purpose |
|---|---|
| `harness.py` | seed (0 -> 378) and 3600 s load: 42 add/remove + 42 send + 16 channel-info workers |
| `oracle.py` | independent checks: `preflight`, `seedcheck`, `post`, `restart`, `teardown` |
| `poller.py` | bounded per-node `/metrics` + `/varz` sampler |
| `monitor_run.py` | bounded event watcher on `repro_run.jsonl` |
| `analyze.py` | per-arm summary from the journal, metrics, operator-captured logs and oracle reports |
| `wkload/` | shared client/outcome model, dataset, deterministic streams, world model |
| `mock/` | disposable in-process mock plus a full-shape dry run, for self-tests only |
| `tests/` | unit tests and deterministic red/green negative controls |
| `original/` | verbatim XIN-1968 files + `SHA256SUMS` |
| `selftest.sh` | runs everything above against local mocks |

## Self-test (no service contacted)

```sh
./selftest.sh            # originals hash check, 33 unit/negative-control tests, 1-node and 3-node full-shape dry runs
```

## Operator procedure for one arm

Every arm uses its own fresh data directory and its own `--state-dir` and `--run-id`. The token file
must be mode 0600. All commands are sequential; never run two arms at once.

```sh
H=test/pr70-business-load
API=http://127.0.0.1:25195            # three-node: http://127.0.0.1:26195,http://127.0.0.1:26196,http://127.0.0.1:26197
MGR=http://127.0.0.1:25295            # three-node: http://127.0.0.1:26295,http://127.0.0.1:26296,http://127.0.0.1:26297
TOK=/path/to/private/manager-token    # 0600
ST=/path/to/evidence/S1               # new, empty, private
RUN=s1                                # alphanumeric, unique per arm
C="--api $API --token-file $TOK --state-dir $ST --run-id $RUN"
O="$C --manager $MGR"

python3 -I $H/oracle.py preflight $O                         # must PASS (fresh, empty, healthy)
python3 -I $H/harness.py --phase seed $C                     # exit 2 = a seed page failed -> stop
python3 -I $H/oracle.py seedcheck $O                         # 0 -> 378 must PASS
python3 -I $H/poller.py --manager $MGR --token-file $TOK --seconds 5400 --out $ST/metrics.jsonl &
python3 -I $H/monitor_run.py --state-dir $ST --max-seconds 4000 &
python3 -I $H/harness.py --phase load $C                     # 3600 s, writes repro_run.jsonl + load_journal.jsonl.gz
python3 -I $H/oracle.py post $O                              # drain <= 600 s, subscribers, messages, probes
#   S3 / T4: graceful restart by the operator (see below), wait for /health, then:
python3 -I $H/oracle.py restart --restart-index 1 $O
python3 -I $H/oracle.py teardown $O                          # 378 -> 0
wait                                                         # poller and monitor are bounded
python3 -I $H/analyze.py --state-dir $ST --metrics $ST/metrics.jsonl --log <captured server log> --json-out $ST/summary.json
```

Exit codes: oracle `0` PASS, `1` FAIL, `3` INCONCLUSIVE (read errors only).
Harness: `0` ok, `2` seed failure, `4` a worker did not stop within the bound.
A non-ok outcome is a recorded result, not a harness error.

Graceful restart rule: stop with SIGTERM and a generous grace period (for
example `docker stop --time 120 <ctr>`). Then check `docker inspect` on the exit code.
Exit code 137 means the stop escalated to SIGKILL; that restart is invalid, and
the arm must be reported as such, not re-run silently. Never use `docker kill` or SIGKILL.

Three-node T3: run the `load` phase. At about T+30 min, gracefully restart one follower.
At about T+45 min, gracefully restart the current leader. Identify the nodes first with
`GET <manager>/cluster/nodes` and record the identification. The harness keeps
running throughout. Requests that fail during the restart window are recorded
as `rejected`/`unknown`/`not_sent`, with no retries. After the load, run `post`.
T4: after T1's `post`, gracefully stop all three nodes, start them, wait for
`/health`, then run `oracle.py restart`.

P0 positive control (only on a disposable container from the retained
`octo-im:pr58` image, with a fresh data directory; never reuse `wk-repro` data):

```sh
python3 -I $H/original/harness.py --base http://127.0.0.1:<pr58-port> --seconds 900   # writes /tmp/repro_run.jsonl
python3 -I $H/original/monitor_run.py                                                  # stop at "TIMEOUT REPRODUCED"
```

The original files are byte-identical to XIN-1968. They take the token only on argv, so P0
must target an instance with no manager token configured.

## Budget (unchanged from the XIN-2199 estimate)

| Step | Wall time per 60-min arm |
|---|---|
| preflight + seed + seedcheck | ~3 min (378 channels, 420 seed requests, 2-3 reads per channel) |
| load | 60 min |
| post: drain (bounded 10 min) + full readback | ~5-15 min; readback is 1 request per 1000 messages per channel |
| restart re-check (S3/T4) | ~5-10 min plus the restart itself |
| teardown | ~3 min |
| **Arm total** | **~85-95 min** |

Matrix: P0 ~25 min. S1+S2+S3 ~3.2 h. T1+T2+T3+T4 ~5 h. Base builds and
deployments ~1 h. Total about 9.5-10 h sequential, the same as approved. The
design adds no arm and no duration. The only scale-dependent item is post readback,
which grows with the number of accepted sends. The oracle streams pages and
keeps about 7 bytes per send: 3M sends take ~61 MB RSS and ~10 s to load.

Envelope rule: compare head against base only within the same topology and
envelope (single 2 CPU / 4 GiB; three-node 1 CPU / 2 GiB per node). Do not compare
three-node numbers with single-node numbers or with the historical 2 CPU / 4 GiB
results.
