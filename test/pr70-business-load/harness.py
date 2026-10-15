#!/usr/bin/env python3
"""PR70 business-load driver, adapted from the XIN-1968 reproduction harness
(original bytes in original/harness.py, sha256 3d3a7fce...47d).

Phases (state shared through --state-dir):
  seed  create the 378-channel dataset with paged delta subscriber_add (0 -> 378)
  load  42 add/remove + 42 send + 16 channel-info workers for --seconds
  all   seed then load

Every request is journaled with its outcome (ok / rejected / unknown / not_sent).
Nothing is retried; every wait is bounded by the client timeout. See ADAPTATION.md
for the historical-to-PR70 semantic mapping.
"""
import argparse
import base64
import json
import os
import platform
import sys
import threading
import time
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from wkload import HARNESS_VERSION  # noqa: E402
from wkload.common import (OK, REJECTED, UNKNOWN, NOT_SENT, Client, Journal, Redactor,  # noqa: E402
                           read_token_file, split_endpoints, tool_hashes, write_json, read_json)
from wkload.world import (ADD, REMOVE, SubscriberModel, addremove_stream, build_channels,  # noqa: E402
                          seed_members, send_stream, upsert_stream)

PAYLOAD = base64.b64encode(b"repro-load-message").decode()
MEMBER_PAGE = 128


class Metrics:
    """Same counters as the historical Metrics plus explicit outcome accounting."""

    def __init__(self, slow_thresh=1.0):
        self.lock = threading.Lock()
        self.slow_thresh = slow_thresh
        self.by_kind = defaultdict(lambda: {"n": 0, "fail": 0, "slow": 0, "timeout": 0,
                                            "outcome": {OK: 0, REJECTED: 0, UNKNOWN: 0, NOT_SENT: 0},
                                            "err": defaultdict(int), "lat": deque()})
        self.first_timeout = None
        self.first_slow = None
        self.first_fail = None
        self.samples = deque(maxlen=400)

    def record(self, kind, res, redact):
        with self.lock:
            m = self.by_kind[kind]
            m["n"] += 1
            m["outcome"][res.outcome] += 1
            m["lat"].append(res.dt)
            if len(m["lat"]) > 200000:
                m["lat"].popleft()
            if res.outcome != OK:
                m["fail"] += 1
                m["err"][res.err] += 1
                if self.first_fail is None:
                    self.first_fail = (res.t, kind, round(res.dt, 3), res.err)
                body = redact(res.body[:180].decode("utf-8", "replace")) if res.body else ""
                self.samples.append((round(res.t, 3), kind, round(res.dt, 3), res.err, body))
            if res.err == "client_timeout":
                m["timeout"] += 1
                if self.first_timeout is None:
                    self.first_timeout = (res.t, kind, round(res.dt, 3))
            if res.dt >= self.slow_thresh:
                m["slow"] += 1
                if self.first_slow is None:
                    self.first_slow = (res.t, kind, round(res.dt, 3))

    def snapshot(self):
        with self.lock:
            out = {}
            for k, m in self.by_kind.items():
                lat = sorted(m["lat"])

                def pct(p):
                    return lat[min(len(lat) - 1, int(p * len(lat)))] if lat else 0.0
                out[k] = {"n": m["n"], "fail": m["fail"], "slow": m["slow"], "timeout": m["timeout"],
                          "outcome": dict(m["outcome"]), "err": dict(m["err"]),
                          "p50": round(pct(.5), 4), "p95": round(pct(.95), 4), "p99": round(pct(.99), 4),
                          "max": round(lat[-1], 4) if lat else 0.0}
            return out, self.first_timeout, self.first_slow, self.first_fail, list(self.samples)


class Ctx:
    def __init__(self, a):
        self.a = a
        self.token = read_token_file(a.token_file)
        self.redact = Redactor(self.token)
        self.apis = split_endpoints(a.api)
        if not self.apis:
            raise SystemExit("--api is required")
        self.client = Client(self.token, a.budget + 1.0)
        self.channels = build_channels(a.groups, a.children, a.channel_prefix)
        self.stop = threading.Event()
        os.makedirs(a.state_dir, exist_ok=True)

    def endpoint(self, global_wid):
        return self.apis[global_wid % len(self.apis)]


def header(ctx, phase):
    a = ctx.a
    return {
        "harness_version": HARNESS_VERSION, "phase": phase, "tool_sha256": tool_hashes(),
        "python": platform.python_version(), "host": platform.node(), "started_at": time.time(),
        "config": {"api": ctx.apis, "seconds": a.seconds, "groups": a.groups, "children": a.children,
                   "parent_members": a.parent_members, "child_members": a.child_members,
                   "addremove": a.addremove, "send": a.send, "upsert": a.upsert, "budget": a.budget,
                   "client_timeout": a.budget + 1.0, "seed": a.seed, "channel_prefix": a.channel_prefix,
                   "n_channels": len(ctx.channels), "member_page": MEMBER_PAGE,
                   "endpoint_mapping": "global_worker_index %% %d (addremove 0..%d, send %d..%d, upsert %d..%d)" % (
                       len(ctx.apis), a.addremove - 1, a.addremove, a.addremove + a.send - 1,
                       a.addremove + a.send, a.addremove + a.send + a.upsert - 1),
                   "token": "from --token-file" if a.token_file else "none"},
    }


def do_seed(ctx):
    a = ctx.a
    model = SubscriberModel()
    journal = Journal(os.path.join(a.state_dir, "seed_journal.jsonl.gz"), ctx.redact)
    M = Metrics()
    base = ctx.apis[0]
    t0 = time.time()
    print("SEED: %d groups, %d children each, %d channels via %s" % (a.groups, a.children, len(ctx.channels), base), flush=True)
    for cid, ctype, _g in ctx.channels:
        model.init_channel(cid)
        subs = seed_members(ctype, a.parent_members, a.child_members)
        pages = [subs[i:i + MEMBER_PAGE] for i in range(0, len(subs), MEMBER_PAGE)]
        for pi, page in enumerate(pages):
            body = {"channel_id": cid, "channel_type": ctype, "reset": 0, "subscribers": page}
            res = ctx.client.call("POST", base, "/channel/subscriber_add", body, timeout=a.budget + 2.0)
            M.record("seed", res, ctx.redact)
            for u in page:
                model.apply(cid, u, ADD, res.outcome)
            journal.write({"t": round(res.t, 3), "dt": round(res.dt, 4), "kind": "seed", "ep": base, "ch": cid,
                           "ct": ctype, "page": pi, "n": len(page), "outcome": res.outcome, "http": res.status,
                           "err": res.err})
    journal.close()
    snap, _, _, _, samples = M.snapshot()
    ok = snap.get("seed", {}).get("outcome", {}).get(OK, 0)
    total = snap.get("seed", {}).get("n", 0)
    state = {"header": header(ctx, "seed"), "finished_at": time.time(), "elapsed": round(time.time() - t0, 1),
             "metrics": snap, "fail_samples": samples, "model": model.to_json()}
    write_json(os.path.join(a.state_dir, "world_seed.json"), state, ctx.redact)
    print("SEED done in %.1fs: %d/%d pages ok" % (time.time() - t0, ok, total), flush=True)
    return model, ok == total


def addremove_worker(ctx, model, M, journal, wid, gwid):
    a = ctx.a
    base = ctx.endpoint(gwid)
    for cid, ctype, action, u in addremove_stream(a.seed, wid, ctx.channels, a.parent_members, a.child_members):
        if ctx.stop.is_set():
            return
        path = "/channel/subscriber_add" if action == ADD else "/channel/subscriber_remove"
        body = {"channel_id": cid, "channel_type": ctype, "subscribers": [u]}
        if action == ADD:
            body["reset"] = 0
        with model.key_lock(cid, u):
            res = ctx.client.call("POST", base, path, body)
            model.apply(cid, u, action, res.outcome)
        kind = "subscriber_add" if action == ADD else "subscriber_remove"
        M.record(kind, res, ctx.redact)
        journal.write({"t": round(res.t, 3), "dt": round(res.dt, 4), "kind": kind, "w": gwid, "ep": base,
                       "ch": cid, "ct": ctype, "uid": u, "outcome": res.outcome, "http": res.status, "err": res.err})


def send_worker(ctx, M, journal, wid, gwid):
    a = ctx.a
    base = ctx.endpoint(gwid)
    for cid, ctype, frm, n in send_stream(a.seed, wid, ctx.channels, a.parent_members, a.child_members):
        if ctx.stop.is_set():
            return
        cmn = "%s-s%02d-%08d" % (a.run_id, wid, n)
        body = {"header": {"no_persist": 0, "red_dot": 1, "sync_once": 0}, "client_msg_no": cmn,
                "from_uid": frm, "channel_id": cid, "channel_type": ctype, "payload": PAYLOAD}
        res = ctx.client.call("POST", base, "/message/send", body)
        M.record("message_send", res, ctx.redact)
        mid = None
        if res.outcome == OK:
            j = res.json() or {}
            mid = (j.get("data") or {}).get("message_id")
        journal.write({"t": round(res.t, 3), "dt": round(res.dt, 4), "kind": "message_send", "w": gwid, "ep": base,
                       "ch": cid, "ct": ctype, "cmn": cmn, "from": frm, "mid": mid, "outcome": res.outcome,
                       "http": res.status, "err": res.err})


def upsert_worker(ctx, M, journal, wid, gwid):
    a = ctx.a
    base = ctx.endpoint(gwid)
    for cid, ctype, large in upsert_stream(a.seed, wid, ctx.channels):
        if ctx.stop.is_set():
            return
        body = {"channel_id": cid, "channel_type": ctype, "large": large}
        res = ctx.client.call("POST", base, "/channel/info", body)
        M.record("channel_info", res, ctx.redact)
        journal.write({"t": round(res.t, 3), "dt": round(res.dt, 4), "kind": "channel_info", "w": gwid, "ep": base,
                       "ch": cid, "ct": ctype, "large": large, "outcome": res.outcome, "http": res.status,
                       "err": res.err})


def do_load(ctx, model):
    a = ctx.a
    M = Metrics()
    journal = Journal(os.path.join(a.state_dir, "load_journal.jsonl.gz"), ctx.redact)
    threads = []
    g = 0
    for i in range(a.addremove):
        threads.append(threading.Thread(target=addremove_worker, args=(ctx, model, M, journal, i, g), daemon=True))
        g += 1
    for i in range(a.send):
        threads.append(threading.Thread(target=send_worker, args=(ctx, M, journal, i, g), daemon=True))
        g += 1
    for i in range(a.upsert):
        threads.append(threading.Thread(target=upsert_worker, args=(ctx, M, journal, i, g), daemon=True))
        g += 1
    hdr = header(ctx, "load")
    print("START workload: %d add/remove + %d send + %d upsert = %d workers, %ds over %d endpoint(s)" % (
        a.addremove, a.send, a.upsert, len(threads), a.seconds, len(ctx.apis)), flush=True)
    rep = open(os.path.join(a.state_dir, "repro_run.jsonl"), "w", encoding="utf-8")
    rep.write(ctx.redact(json.dumps({"header": hdr})) + "\n")
    start = time.monotonic()
    for t in threads:
        t.start()
    last = 0
    while time.monotonic() - start < a.seconds:
        time.sleep(min(a.report_interval, max(0.05, a.seconds - (time.monotonic() - start))))
        snap, ft, fs, ff, samples = M.snapshot()
        el = time.monotonic() - start
        rep.write(ctx.redact(json.dumps({"t": round(time.time(), 3), "elapsed": round(el, 1), "kinds": snap,
                                         "first_timeout": ft, "first_slow": fs, "first_fail": ff,
                                         "fail_samples": samples[-20:]})) + "\n")
        rep.flush()
        tot = sum(v["n"] for v in snap.values())
        fl = sum(v["fail"] for v in snap.values())
        to = sum(v["timeout"] for v in snap.values())
        print("[%4ds] req=%d (+%d) fail=%d timeout=%d first_fail=%s" % (el, tot, tot - last, fl, to, ff), flush=True)
        last = tot
    ctx.stop.set()
    # Every request is bounded by the client timeout, so each worker exits within it.
    deadline = time.monotonic() + a.budget + 5.0
    stuck = []
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
        if t.is_alive():
            stuck.append(t.name)
    journal.close()
    snap, ft, fs, ff, samples = M.snapshot()
    final = {"final": True, "elapsed": round(time.monotonic() - start, 1), "kinds": snap, "first_timeout": ft,
             "first_slow": fs, "first_fail": ff, "fail_samples": samples, "stuck_workers": stuck,
             "journal_records": journal.count}
    rep.write(ctx.redact(json.dumps(final)) + "\n")
    rep.close()
    state = {"header": hdr, "finished_at": time.time(), "final": final, "model": model.to_json()}
    write_json(os.path.join(a.state_dir, "world_final.json"), state, ctx.redact)
    print("FINAL: %s" % ctx.redact(json.dumps({k: {"n": v["n"], "outcome": v["outcome"]} for k, v in snap.items()})),
          flush=True)
    return not stuck


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
                                 allow_abbrev=False)
    ap.add_argument("--phase", choices=("seed", "load", "all"), default="all")
    ap.add_argument("--api", required=True, help="comma-separated API base URLs, e.g. http://127.0.0.1:25195")
    ap.add_argument("--token-file", default="", help="0600 file holding the manager token (never pass on argv)")
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--run-id", default="r1", help="prefix of every client_msg_no; unique per arm")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--seconds", type=float, default=3600)
    ap.add_argument("--groups", type=int, default=42)
    ap.add_argument("--children", type=int, default=8)
    ap.add_argument("--parent-members", type=int, default=150)
    ap.add_argument("--child-members", type=int, default=20)
    ap.add_argument("--addremove", type=int, default=42)
    ap.add_argument("--send", type=int, default=42)
    ap.add_argument("--upsert", type=int, default=16)
    ap.add_argument("--budget", type=float, default=5.0, help="historical 5s budget; client timeout = budget + 1s")
    ap.add_argument("--channel-prefix", default="repro")
    ap.add_argument("--report-interval", type=float, default=15.0)
    ap.add_argument("--continue-on-seed-failure", action="store_true",
                    help="default: abort before load if any seed page is not ok")
    a = ap.parse_args(argv)
    if "-" in a.run_id or not a.run_id.isalnum():
        ap.error("--run-id must be alphanumeric")
    return a


def main(argv=None):
    a = parse_args(argv)
    ctx = Ctx(a)
    try:
        return run(ctx)
    finally:
        ctx.client.close_all()


def run(ctx):
    a = ctx.a
    if a.phase in ("seed", "all"):
        model, seed_ok = do_seed(ctx)
        if not seed_ok and not a.continue_on_seed_failure:
            print("SEED had non-ok pages; aborting before load (see world_seed.json)", flush=True)
            return 2
        if a.phase == "seed":
            return 0 if seed_ok else 2
    else:
        model = SubscriberModel.from_json(read_json(os.path.join(a.state_dir, "world_seed.json"))["model"])
    return 0 if do_load(ctx, model) else 4


if __name__ == "__main__":
    sys.exit(main())
