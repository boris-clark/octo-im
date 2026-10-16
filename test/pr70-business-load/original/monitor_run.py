#!/usr/bin/env python3
"""Tail the harness report JSONL and emit event lines ONLY on:
 - first timeout reproduced (the key failure signal)
 - reconcile max-latency crossing rising thresholds toward the 5s budget
 - a heartbeat every ~10 min
 - run completion (FINAL) -> then exit
Each printed line becomes a Monitor notification."""
import json, os, sys, time

REPORT = "/tmp/repro_run.jsonl"
thresholds = [2.0, 3.0, 4.0, 4.5, 4.9]
crossed = set()
last_beat = 0.0
pos = 0
announced_timeout = False

# wait for file
for _ in range(60):
    if os.path.exists(REPORT):
        break
    time.sleep(2)

while True:
    try:
        with open(REPORT) as f:
            f.seek(pos)
            lines = f.readlines()
            pos = f.tell()
    except FileNotFoundError:
        time.sleep(3); continue
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except Exception:
            continue
        k = d.get("kinds", {})
        rec = k.get("subscriber_reconcile", {})
        ups = k.get("channel_upsert", {})
        el = d.get("elapsed")
        ft = d.get("first_timeout")
        tot_to = sum(v.get("timeout", 0) for v in k.values())
        maxlat = max([rec.get("max", 0), ups.get("max", 0)])
        if ft and not announced_timeout:
            announced_timeout = True
            print("TIMEOUT REPRODUCED elapsed=%s first_timeout=%s total_timeouts=%d" % (el, ft, tot_to), flush=True)
        for th in thresholds:
            if maxlat >= th and th not in crossed:
                crossed.add(th)
                print("LATENCY escalation elapsed=%ss reconcile/upsert max=%.2fs (>=%.1fs) p99=%.2f timeouts=%d"
                      % (el, maxlat, th, rec.get("p99", 0), tot_to), flush=True)
        if d.get("final"):
            print("RUN COMPLETE elapsed=%ss total_timeouts=%d reconcile=%s upsert=%s"
                  % (el, tot_to, json.dumps({x: rec.get(x) for x in ("n","fail","slow","timeout","p99","max")}),
                     json.dumps({x: ups.get(x) for x in ("n","fail","slow","timeout","p99","max")})), flush=True)
            sys.exit(0)
    now = time.time()
    if now - last_beat > 600:
        last_beat = now
        print("heartbeat: elapsed=%ss reconcile p99=%.2f max=%.2f timeouts=%d fail=%d"
              % (el if 'el' in dir() else '?', rec.get("p99", 0), rec.get("max", 0),
                 tot_to, rec.get("fail", 0)), flush=True)
    time.sleep(10)
