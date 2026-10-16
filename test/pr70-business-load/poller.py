#!/usr/bin/env python3
"""Bounded metrics sampler, adapted from original/poller.py (sha256 cd739d91...ea0c).

Per node every --interval seconds: manager /metrics (filtered series), manager
/varz. The historical /channel/subscriber_recovery sample is removed because
the route does not exist in PR70. Runs for --seconds then exits (the historical
poller looped forever). The token is read from --token-file and never written.
"""
import argparse
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from wkload.common import OK, Client, Redactor, read_token_file, split_endpoints  # noqa: E402

KEEP = re.compile(r'^(cluster_channel_propose_(count|failed_count|latency_(over|under)_500ms)_total'
                  r'|cluster_(channel|slot)_propose_latency_(sum|count|bucket)'
                  r'|cluster_channel_active_count'
                  r'|cluster_[a-z_]*(inflight|pending|timeout|leader)[a-z_]*'
                  r'|db_commit_count_total'
                  r'|db_wal_(bytes_written|size|physical_size|files_count|fsync[a-z_]*)'
                  r'|db_compact_(num_in_progress|total_count|estimated_debt|in_progress_bytes)'
                  r'|db_alllevel_(bytes_compacted|compact_score)'
                  r'|db_memtable[a-z_]*'
                  r'|db_flush[a-z_]*'
                  r'|go_goroutines|go_memstats_heap_inuse_bytes|process_resident_memory_bytes'
                  r'|process_cpu_seconds_total)')


def parse_metrics(txt):
    out = {}
    for line in txt.splitlines():
        if not line or line[0] == '#':
            continue
        name = line.split('{', 1)[0].split(' ', 1)[0]
        if not KEEP.match(name):
            continue
        try:
            val = float(line.rsplit(' ', 1)[1])
        except Exception:
            continue
        out[name] = out.get(name, 0.0) + val
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    ap.add_argument("--manager", required=True, help="comma-separated manager URLs")
    ap.add_argument("--token-file", default="")
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--seconds", type=float, required=True, help="hard upper bound on runtime")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    tok = read_token_file(a.token_file)
    redact = Redactor(tok)
    c = Client(tok, 8.0)
    mgrs = split_endpoints(a.manager)
    end = time.monotonic() + a.seconds
    with open(a.out, "w", encoding="utf-8") as f:
        while time.monotonic() < end:
            rec = {"t": round(time.time(), 3), "nodes": {}}
            for ep in mgrs:
                n = {}
                r = c.call("GET", ep, "/metrics")
                if r.outcome == OK:
                    n["metrics"] = parse_metrics(r.body.decode("utf-8", "replace"))
                else:
                    n["metrics_err"] = {"http": r.status, "err": r.err}
                r = c.call("GET", ep, "/varz")
                j = r.json() if r.outcome == OK else None
                if isinstance(j, dict):
                    n["varz"] = {k: j.get(k) for k in ("goroutine", "mem", "cpu", "connections", "in_msgs",
                                                       "out_msgs", "in_bytes", "out_bytes", "slow_clients",
                                                       "retry_queue")}
                else:
                    n["varz_err"] = {"http": r.status, "err": r.err}
                rec["nodes"][ep] = n
            f.write(redact(json.dumps(rec)) + "\n")
            f.flush()
            time.sleep(max(0.0, min(a.interval, end - time.monotonic())))
    c.close_all()
    return 0


if __name__ == "__main__":
    sys.exit(main())
