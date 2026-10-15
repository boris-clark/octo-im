#!/usr/bin/env python3
"""Aggregate one arm's artifacts, adapted from original/analyze.py (sha256 dcafbaef...2e9a).

Changes: paths come from --state-dir / --metrics / --log; failure classes are
counted from the complete journal instead of the sampled deque; the PR58-only
classes (stale_revision / revision_conflict / managed_channel 409, state:pending
503 budget exhaustion) are gone because PR70 has no such responses; oracle
verdicts are included. Per-node metric deltas are reported separately and are
not summed across topologies.
"""
import argparse
import collections
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from wkload.common import read_journal  # noqa: E402

LOG_MARKERS = ["propose batch until applied timeout", "context deadline exceeded", "panic", "fatal",
               "leader changed", "apply timeout"]
DELTA_KEYS = ["cluster_channel_propose_count_total", "cluster_channel_propose_failed_count_total",
              "cluster_channel_propose_latency_over_500ms_total", "cluster_channel_propose_latency_under_500ms_total",
              "db_commit_count_total", "db_wal_bytes_written", "db_compact_total_count"]


def load_jsonl(p):
    out = []
    if p and os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except Exception:
                        pass
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--metrics", default="", help="poller output JSONL")
    ap.add_argument("--log", action="append", default=[], help="server log file(s) captured by the operator")
    ap.add_argument("--json-out", default="")
    a = ap.parse_args(argv)
    sd = a.state_dir
    summary = {}

    run = load_jsonl(os.path.join(sd, "repro_run.jsonl"))
    final = next((d for d in run if d.get("final")), run[-1] if run else None)
    if final:
        summary["workload"] = {"elapsed": final.get("elapsed"), "kinds": final.get("kinds"),
                               "first_fail": final.get("first_fail"), "first_timeout": final.get("first_timeout"),
                               "stuck_workers": final.get("stuck_workers")}
        print("=== WORKLOAD FINAL (elapsed %ss) ===" % final.get("elapsed"))
        tot = 0
        for k, v in sorted(final["kinds"].items()):
            tot += v["n"]
            print("  %-18s n=%-8d ok=%-8d rejected=%-6d unknown=%-6d not_sent=%-6d p50=%.3f p99=%.3f max=%.3f" % (
                k, v["n"], v["outcome"]["ok"], v["outcome"]["rejected"], v["outcome"]["unknown"],
                v["outcome"]["not_sent"], v["p50"], v["p99"], v["max"]))
        print("  TOTAL requests=%d throughput=%.0f/s" % (tot, tot / max(1.0, final["elapsed"] or 1)))

    jp = os.path.join(sd, "load_journal.jsonl.gz")
    if os.path.exists(jp):
        cls = collections.Counter()
        per_min = collections.defaultdict(collections.Counter)
        t0 = None
        n = 0
        for rec in read_journal(jp):
            n += 1
            t0 = rec["t"] if t0 is None else min(t0, rec["t"])
            if rec["outcome"] != "ok":
                cls[(rec["kind"], rec["outcome"], rec["err"])] += 1
        for rec in read_journal(jp):
            if rec["outcome"] != "ok":
                per_min[int((rec["t"] - t0) // 60)][rec["outcome"]] += 1
        summary["journal_records"] = n
        summary["failure_classes"] = [{"kind": k, "outcome": o, "err": e, "n": c} for (k, o, e), c in cls.most_common()]
        summary["non_ok_per_minute"] = {m: dict(c) for m, c in sorted(per_min.items())}
        print("\n=== NON-OK OUTCOMES (complete journal, %d records) ===" % n)
        for (k, o, e), c in cls.most_common():
            print("  %-18s %-9s %-34s %d" % (k, o, e, c))

    met = load_jsonl(a.metrics)
    if met:
        print("\n=== SERVER METRIC DELTAS PER NODE (first -> last sample) ===")
        summary["metric_deltas"] = {}
        for ep in met[0].get("nodes", {}):
            m0 = next((r["nodes"][ep]["metrics"] for r in met if "metrics" in r["nodes"].get(ep, {})), {})
            m1 = next((r["nodes"][ep]["metrics"] for r in reversed(met) if "metrics" in r["nodes"].get(ep, {})), {})
            d = {k: (m1.get(k, 0) or 0) - (m0.get(k, 0) or 0) for k in DELTA_KEYS}
            gaps = sum(1 for r in met if "metrics" not in r["nodes"].get(ep, {}))
            summary["metric_deltas"][ep] = {"delta": d, "samples_missing": gaps}
            print("  node %s (missing samples %d)" % (ep, gaps))
            for k in DELTA_KEYS:
                print("    %-50s +%d" % (k, d[k]))

    for lp in a.log:
        if os.path.exists(lp):
            with open(lp, encoding="utf-8", errors="replace") as f:
                txt = f.read()
            counts = {m: txt.count(m) for m in LOG_MARKERS}
            summary.setdefault("logs", {})[lp] = counts
            print("\n=== LOG %s ===" % lp)
            for m, c in counts.items():
                print("  %-40s %d" % (m, c))

    for name in sorted(os.listdir(sd)):
        if name.startswith("oracle_") and name.endswith(".json"):
            with open(os.path.join(sd, name), encoding="utf-8") as f:
                o = json.load(f)
            summary.setdefault("oracle", {})[name] = {"verdict": o["verdict"], "fail": o["fail"],
                                                      "read_errors": o["read_errors"]}
            print("\n=== %s: %s fail=%s read_errors=%s" % (name, o["verdict"], o["fail"], o["read_errors"]))

    if a.json_out:
        with open(a.json_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
