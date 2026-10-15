#!/usr/bin/env python3
"""Tail <state-dir>/repro_run.jsonl and print event lines, adapted from
original/monitor_run.py (sha256 b3139d55...d6aa).

Changes: report path is an argument; watched kinds are the PR70 write kinds
(subscriber_add, subscriber_remove, channel_info); the first non-ok outcome of
any kind is announced (not only client timeouts); the wait is bounded by
--max-seconds instead of looping forever.
"""
import argparse
import json
import os
import sys
import time

WRITE_KINDS = ("subscriber_add", "subscriber_remove", "channel_info")
THRESHOLDS = [2.0, 3.0, 4.0, 4.5, 4.9]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--max-seconds", type=float, required=True)
    ap.add_argument("--poll", type=float, default=10.0)
    a = ap.parse_args(argv)
    report = os.path.join(a.state_dir, "repro_run.jsonl")
    end = time.monotonic() + a.max_seconds
    crossed, pos, announced_fail, announced_to, last_beat = set(), 0, False, False, 0.0
    while time.monotonic() < end:
        lines = []
        if os.path.exists(report):
            with open(report, encoding="utf-8") as f:
                f.seek(pos)
                lines = f.readlines()
                pos = f.tell()
        for line in lines:
            try:
                d = json.loads(line)
            except Exception:
                continue
            if "header" in d:
                continue
            k = d.get("kinds", {})
            el = d.get("elapsed")
            tot_to = sum(v.get("timeout", 0) for v in k.values())
            tot_fail = sum(v.get("fail", 0) for v in k.values())
            if d.get("first_fail") and not announced_fail:
                announced_fail = True
                print("FIRST NON-OK elapsed=%s first_fail=%s total_fail=%d" % (el, d["first_fail"], tot_fail), flush=True)
            if d.get("first_timeout") and not announced_to:
                announced_to = True
                print("FIRST CLIENT TIMEOUT elapsed=%s first_timeout=%s total_timeouts=%d" % (el, d["first_timeout"], tot_to), flush=True)
            maxlat = max([k.get(x, {}).get("max", 0) for x in WRITE_KINDS] + [0])
            for th in THRESHOLDS:
                if maxlat >= th and th not in crossed:
                    crossed.add(th)
                    print("LATENCY escalation elapsed=%ss write max=%.2fs (>=%.1fs) timeouts=%d" % (el, maxlat, th, tot_to), flush=True)
            if d.get("final"):
                print("RUN COMPLETE elapsed=%ss fail=%d timeouts=%d outcomes=%s" % (
                    el, tot_fail, tot_to, json.dumps({x: v.get("outcome") for x, v in k.items()})), flush=True)
                return 0
            if time.monotonic() - last_beat > 600:
                last_beat = time.monotonic()
                print("heartbeat: elapsed=%ss fail=%d timeouts=%d" % (el, tot_fail, tot_to), flush=True)
        time.sleep(a.poll)
    print("MONITOR STOPPED at --max-seconds without FINAL record", flush=True)
    return 2


if __name__ == "__main__":
    sys.exit(main())
