#!/usr/bin/env python3
"""Independent consistency oracle for the PR70 business-load harness.

Replaces original/postcheck.py (sha256 219696f6...daa15), whose routes
/channel/subscriber_reconcile and /channel/subscriber_recovery do not exist in
PR70 head or base. All checks read real server state; write receipts are never
used as evidence of state.

Modes (run in this order; --state-dir is shared with harness.py):
  preflight  every API endpoint healthy; every dataset channel has 0 subscribers
             and max_message_seq 0 (detects stale state from earlier runs)
  seedcheck  subscriber sets equal the seeded world model           (0 -> 378)
  post       bounded drain until accepted messages are readable, subscriber
             sets vs final world model, probe send on every channel, full
             message readback/accounting, readback snapshot for restart
  restart    after a graceful restart (performed by the operator): health,
             subscriber sets, probe send on every channel, readback whose
             prefix is identical to the previous snapshot, full accounting
  teardown   subscriber_remove_all on every channel, verify empty sets, probe
             send on every channel, full accounting                 (378 -> 0)

Reads:
  subscribers  GET <manager>/cluster/channels/<id>/<type>/subscribers on every
               manager endpoint; every 200 answer must agree
  messages     POST <api>/channel/messagesync (pull_mode 1, from seq 1, paged,
               streamed page by page) and GET <api>/channel/max_message_seq

Memory is bounded for multi-million message runs: the send ledger is kept as
per-worker byte/short arrays and the restart snapshot as a per-channel
(count, sha256) pair.

Verdict: FAIL if any check failed, INCONCLUSIVE if only read errors occurred,
else PASS. Exit code 0 / 1 / 3. Unknown/rejected write outcomes are counted and
reported, never folded into a pass.
"""
import argparse
import base64
import hashlib
import json
import os
import sys
import time
from array import array
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from wkload import HARNESS_VERSION  # noqa: E402
from wkload.common import (OK, REJECTED, UNKNOWN, NOT_SENT, Client, Redactor, read_journal,  # noqa: E402
                           read_token_file, split_endpoints, tool_hashes, write_json, read_json)
from wkload.world import SubscriberModel, build_channels  # noqa: E402

LOAD_PAYLOAD = base64.b64encode(b"repro-load-message").decode()
PROBE_PAYLOAD = base64.b64encode(b"postcheck-probe").decode()
PAGE = 1000
MAX_PAGES = 100000
CODE = {OK: 1, REJECTED: 2, UNKNOWN: 3, NOT_SENT: 4}
OUTCOME = {v: k for k, v in CODE.items()}


class Ledger:
    """Every message the harness or oracle sent, with its write outcome.

    Load sends use client_msg_no <run>-sWW-NNNNNNNN and are stored as per-worker
    arrays indexed by NNNNNNNN-1 (outcome code, channel index, from-uid number).
    Probe sends are few and kept in a dict.
    """

    def __init__(self, state_dir, run_id, channels):
        self.run_id = run_id
        self.channels = channels
        self.chidx = {c[0]: i for i, c in enumerate(channels)}
        self.out, self.ch, self.frm = {}, {}, {}
        self.probes = {}
        p = os.path.join(state_dir, "load_journal.jsonl.gz")
        if os.path.exists(p):
            for rec in read_journal(p):
                if rec.get("kind") != "message_send":
                    continue
                k = self.load_key(rec["cmn"])
                if k is None:
                    raise SystemExit("journal client_msg_no %r does not match --run-id %r" % (rec["cmn"], run_id))
                w, n = k
                o, c, f = self.out.setdefault(w, bytearray()), self.ch.setdefault(w, array("H")), \
                    self.frm.setdefault(w, array("H"))
                while len(o) < n:
                    o.append(0)
                    c.append(0)
                    f.append(0)
                o[n - 1] = CODE[rec["outcome"]]
                c[n - 1] = self.chidx[rec["ch"]]
                f[n - 1] = int(rec["from"][1:])
        for name in sorted(os.listdir(state_dir)):
            if name.startswith("probes_") and name.endswith(".json"):
                self.probes.update(read_json(os.path.join(state_dir, name)))

    def load_key(self, cmn):
        parts = cmn.split("-") if isinstance(cmn, str) else []
        if len(parts) == 3 and parts[0] == self.run_id and parts[1][:1] == "s" and parts[1][1:].isdigit() \
                and parts[2].isdigit():
            return int(parts[1][1:]), int(parts[2])
        return None

    def lookup(self, cmn):
        """-> (key, outcome, channel_id, from_uid, payload, probe_label) or None."""
        k = self.load_key(cmn)
        if k is not None:
            w, n = k
            o = self.out.get(w)
            if o is None or n < 1 or n > len(o) or o[n - 1] == 0:
                return None
            return (k, OUTCOME[o[n - 1]], self.channels[self.ch[w][n - 1]][0], "u%05d" % self.frm[w][n - 1],
                    LOAD_PAYLOAD, None)
        v = self.probes.get(cmn)
        if v is None:
            return None
        return (cmn, v["outcome"], v["ch"], v["from"], v["payload"], v.get("probe"))

    def entries(self):
        """Yields (key, outcome, channel_id, cmn)."""
        for w, o in self.out.items():
            ch = self.ch[w]
            for i, code in enumerate(o):
                if code:
                    yield (w, i + 1), OUTCOME[code], self.channels[ch[i]][0], "%s-s%02d-%08d" % (self.run_id, w, i + 1)
        for cmn, v in self.probes.items():
            yield cmn, v["outcome"], v["ch"], cmn


class Seen:
    """Compact seen-counter keyed like Ledger (capped at 255)."""

    def __init__(self, ledger):
        self.w = {w: bytearray(len(o)) for w, o in ledger.out.items()}
        self.p = defaultdict(int)

    def bump(self, key):
        if isinstance(key, tuple):
            arr = self.w[key[0]]
            c = arr[key[1] - 1]
            if c < 255:
                arr[key[1] - 1] = c + 1
            return c
        c = self.p[key]
        self.p[key] = c + 1
        return c

    def get(self, key):
        if isinstance(key, tuple):
            return self.w[key[0]][key[1] - 1]
        return self.p.get(key, 0)


class Oracle:
    def __init__(self, a):
        self.a = a
        self.token = read_token_file(a.token_file)
        self.redact = Redactor(self.token)
        self.apis = split_endpoints(a.api)
        self.managers = split_endpoints(a.manager)
        if not self.apis or not self.managers:
            raise SystemExit("--api and --manager are required")
        self.client = Client(self.token, a.read_timeout)
        self.channels = build_channels(a.groups, a.children, a.channel_prefix)
        os.makedirs(a.state_dir, exist_ok=True)
        self.fail = defaultdict(int)
        self.info = defaultdict(int)
        self.read_errors = defaultdict(int)
        self.examples = defaultdict(list)

    # ---------- bookkeeping ----------
    def bad(self, key, example, n=1):
        self.fail[key] += n
        if len(self.examples[key]) < 20:
            self.examples[key].append(example)

    def rerr(self, key, example):
        self.read_errors[key] += 1
        if len(self.examples["read_error:" + key]) < 20:
            self.examples["read_error:" + key].append(example)

    def verdict(self):
        if any(self.fail.values()):
            return "FAIL"
        if any(self.read_errors.values()):
            return "INCONCLUSIVE"
        return "PASS"

    def ledger(self):
        return Ledger(self.a.state_dir, self.a.run_id, self.channels)

    # ---------- reads ----------
    def health(self):
        out = {}
        for ep in self.apis:
            r = self.client.call("GET", ep, "/health")
            j = r.json() or {}
            out[ep] = {"http": r.status, "outcome": r.outcome, "status": j.get("status")}
            if r.outcome != OK or j.get("status") != "ok":
                self.bad("unhealthy_endpoint", {"ep": ep, "http": r.status, "err": r.err})
        return out

    def subscribers(self, cid, ctype):
        """Returns sorted list from the agreeing nodes, or None on read error."""
        answers = {}
        errs = {}
        for ep in self.managers:
            r = self.client.call("GET", ep, "/cluster/channels/%s/%d/subscribers" % (cid, ctype))
            j = r.json() if r.outcome == OK else None
            if isinstance(j, list) and all(isinstance(x, str) for x in j):
                answers[ep] = j
            else:
                errs[ep] = r.err or "bad_body"
        for ep in errs:
            self.info["subscriber_read_node_error:" + ep] += 1
        if not answers:
            self.rerr("subscribers", {"ch": cid, "errors": errs})
            return None
        vals = list(answers.values())
        canon = sorted(vals[0])
        for v in answers.values():
            if sorted(v) != canon:
                self.bad("divergent_subscriber_read", {"ch": cid, "nodes": {k: len(x) for k, x in answers.items()}})
                break
        if len(vals[0]) != len(set(vals[0])):
            self.bad("duplicate_subscriber", {"ch": cid, "n": len(vals[0]), "distinct": len(set(vals[0]))})
        return canon

    def max_seq(self, cid, ctype):
        r = self.client.call("GET", self.apis[0], "/channel/max_message_seq?channel_id=%s&channel_type=%d" % (cid, ctype))
        j = r.json() if r.outcome == OK else None
        if not isinstance(j, dict) or not isinstance(j.get("message_seq"), int):
            self.rerr("max_message_seq", {"ch": cid, "http": r.status, "err": r.err})
            return None
        return j["message_seq"]

    def pages(self, cid, ctype, start=1, quiet=False):
        """Streams message pages from `start`. Yields lists; yields None once on read error.

        quiet=True is used by drain polls: a failed poll is counted in info, not as
        a verdict read error, because the final readback is always repeated."""
        s = start
        for _ in range(MAX_PAGES):
            body = {"login_uid": "u00000", "channel_id": cid, "channel_type": ctype, "start_message_seq": s,
                    "end_message_seq": 0, "limit": PAGE, "pull_mode": 1}
            r = self.client.call("POST", self.apis[0], "/channel/messagesync", body)
            j = r.json() if r.outcome == OK else None
            if not isinstance(j, dict) or not isinstance(j.get("messages"), list):
                if quiet:
                    self.info["drain_read_error"] += 1
                else:
                    self.rerr("messagesync", {"ch": cid, "start": s, "http": r.status, "err": r.err})
                yield None
                return
            msgs = j["messages"]
            if msgs:
                yield msgs
            if not msgs or not j.get("more"):
                return
            s = msgs[-1].get("message_seq", s) + 1
        self.rerr("messagesync_page_limit", {"ch": cid})
        yield None

    # ---------- checks ----------
    def check_subscribers(self, model, label):
        exp_total = 0
        for cid, ctype, _g in self.channels:
            actual = self.subscribers(cid, ctype)
            if actual is None:
                continue
            act = set(actual)
            conf = model.confirmed.get(cid, set())
            unc = model.uncertain.get(cid, set())
            must = conf - unc
            allowed = conf | unc
            exp_total += len(must)
            missing = must - act
            extra = act - allowed
            if missing:
                self.bad("missing_subscriber", {"ch": cid, "n": len(missing), "uids": sorted(missing)[:10]}, len(missing))
            if extra:
                self.bad("unexpected_subscriber", {"ch": cid, "n": len(extra), "uids": sorted(extra)[:10]}, len(extra))
            self.info["uncertain_uid_present"] += len(unc & act)
            self.info["uncertain_uid_absent"] += len(unc - act)
            self.info["subscribers_read_total"] += len(act)
        self.info["subscribers_expected_certain_" + label] = exp_total

    def scan_channel(self, cid, ctype, ledger, seen, snap=None, probe_label=None):
        """Full streamed readback + checks for one channel.

        Returns {"n", "h"} (count and sha256 over "seq,cmn,mid" lines) or None on read error.
        snap: previous {"n", "h"}; the first n messages must hash identically and
        anything after them must be this run's probes (or late unknown sends)."""
        h_all = hashlib.sha256()
        h_pre = None
        count = 0
        prev = None
        last_n_by_worker = {}
        snap_n = snap["n"] if snap else None
        if snap_n == 0:
            h_pre = hashlib.sha256().hexdigest()
        for page in self.pages(cid, ctype):
            if page is None:
                return None
            for m in page:
                seq, cmn, mid = m.get("message_seq"), m.get("client_msg_no"), m.get("message_id")
                count += 1
                if prev is None:
                    if seq != 1:
                        self.bad("seq_gap", {"ch": cid, "first": seq})
                elif seq <= prev:
                    self.bad("non_increasing_seq", {"ch": cid, "prev": prev, "seq": seq})
                elif seq != prev + 1:
                    self.bad("seq_gap", {"ch": cid, "prev": prev, "seq": seq})
                prev = seq
                h_all.update(("%s,%s,%s\n" % (seq, cmn, mid)).encode())
                if snap_n is not None and count == snap_n:
                    h_pre = h_all.hexdigest()
                ent = ledger.lookup(cmn)
                if snap_n is not None and count > snap_n:
                    if ent is not None and ent[5] == probe_label:
                        pass
                    elif ent is not None and ent[1] == UNKNOWN:
                        self.info["late_unknown_after_snapshot"] += 1
                    else:
                        self.bad("new_message_after_restart", {"ch": cid, "cmn": cmn, "seq": seq})
                if ent is None:
                    self.bad("unexpected_message", {"ch": cid, "cmn": cmn, "seq": seq})
                    continue
                key, outcome, ech, efrom, epayload, _pl = ent
                if seen.bump(key):
                    self.bad("duplicate_message", {"ch": cid, "cmn": cmn, "seq": seq})
                    continue
                if ech != cid:
                    self.bad("message_wrong_channel", {"cmn": cmn, "expected": ech, "got": cid})
                if m.get("payload") != epayload or m.get("from_uid") != efrom:
                    self.bad("message_content_mismatch", {"ch": cid, "cmn": cmn})
                if outcome == REJECTED:
                    self.bad("rejected_but_persisted", {"ch": cid, "cmn": cmn})
                elif outcome == NOT_SENT:
                    self.bad("not_sent_but_persisted", {"ch": cid, "cmn": cmn})
                elif outcome == UNKNOWN:
                    self.info["unknown_send_persisted"] += 1
                if isinstance(key, tuple):
                    w, n = key
                    if last_n_by_worker.get(w, 0) > n:
                        self.info["same_worker_order_inversion"] += 1
                    last_n_by_worker[w] = n
        ms = self.max_seq(cid, ctype)
        if ms is not None and ms != (prev or 0):
            self.bad("max_seq_mismatch", {"ch": cid, "max_message_seq": ms, "readback_last": prev or 0})
        if snap is not None and (count < snap_n or h_pre != snap["h"]):
            self.bad("restart_readback_changed", {"ch": cid, "before": snap_n, "after": count})
        self.info["messages_read_total"] += count
        return {"n": count, "h": h_all.hexdigest()}

    def account(self, label, snap=None, probe_label=None):
        """Scan every channel, then report accepted messages never read. Returns new snapshot."""
        ledger = self.ledger()
        seen = Seen(ledger)
        new_snap = {}
        read_ok = set()
        for cid, ctype, _g in self.channels:
            ch_snap = None
            if snap is not None:
                ch_snap = snap.get(cid)
                if ch_snap is None:  # channel was unreadable when the snapshot was taken
                    self.rerr("snapshot_missing_channel", {"ch": cid})
            r = self.scan_channel(cid, ctype, ledger, seen, ch_snap, probe_label)
            if r is not None:
                new_snap[cid] = r
                read_ok.add(cid)
        accepted = 0
        for key, outcome, ch, cmn in ledger.entries():
            if outcome == OK:
                accepted += 1
                if ch in read_ok and seen.get(key) == 0:
                    self.bad("missing_accepted_message", {"ch": ch, "cmn": cmn})
            elif outcome == UNKNOWN and seen.get(key) == 0:
                self.info["unknown_send_absent"] += 1
        self.info["messages_accepted_in_ledger_" + label] = accepted
        return new_snap

    def drain(self, ledger, only=None):
        """Bounded wait until every accepted message is readable. Incremental reads."""
        seen = Seen(ledger)
        want = defaultdict(int)
        for key, outcome, ch, _cmn in ledger.entries():
            if outcome == OK and (only is None or key in only):
                want[ch] += 1
        cursor = {cid: 1 for cid, _t, _g in self.channels}
        t0 = time.monotonic()
        polls = 0
        while True:
            polls += 1
            for cid, ctype, _g in self.channels:
                if want[cid] <= 0:
                    continue
                for page in self.pages(cid, ctype, cursor[cid], quiet=True):
                    if page is None:
                        break
                    for m in page:
                        ent = ledger.lookup(m.get("client_msg_no"))
                        if ent is not None and ent[1] == OK and (only is None or ent[0] in only) \
                                and seen.bump(ent[0]) == 0:
                            want[cid] -= 1
                    cursor[cid] = page[-1]["message_seq"] + 1
            remaining = sum(max(0, v) for v in want.values())
            el = time.monotonic() - t0
            if remaining == 0 or el >= self.a.drain_timeout:
                return {"drained": remaining == 0, "secs": round(el, 1), "polls": polls,
                        "accepted_not_yet_readable": remaining}
            time.sleep(self.a.drain_interval)

    def probe(self, label):
        """One send per channel, recorded in a probe ledger, then a bounded visibility wait.

        Visibility is judged by the caller's final accounting (missing_accepted_message),
        so a failed read makes the result INCONCLUSIVE, never PASS."""
        led = {}
        for i, (cid, ctype, _g) in enumerate(self.channels):
            cmn = "%s-%sprobe-%04d" % (self.a.run_id, label, i)
            body = {"header": {"no_persist": 0, "red_dot": 1, "sync_once": 0}, "client_msg_no": cmn,
                    "from_uid": "u00000", "channel_id": cid, "channel_type": ctype, "payload": PROBE_PAYLOAD}
            r = self.client.call("POST", self.apis[0], "/message/send", body)
            led[cmn] = {"ch": cid, "outcome": r.outcome, "from": "u00000", "payload": PROBE_PAYLOAD, "probe": label}
            if r.outcome != OK:
                self.bad("probe_send_not_ok", {"ch": cid, "http": r.status, "err": r.err})
        write_json(os.path.join(self.a.state_dir, "probes_%s.json" % label), led, self.redact)
        d = self.drain(self.ledger(), only=set(led))
        return {"sent_ok": sum(1 for v in led.values() if v["outcome"] == OK), "n": len(led), "visibility": d}

    def snapshot_path(self):
        return os.path.join(self.a.state_dir, "readback_snapshot.json")


def model_from(state_dir, name):
    return SubscriberModel.from_json(read_json(os.path.join(state_dir, name))["model"])


def run(a):
    o = Oracle(a)
    try:
        return check(o, a)
    finally:
        o.client.close_all()


def check(o, a):
    rep = {"harness_version": HARNESS_VERSION, "tool_sha256": tool_hashes(), "mode": a.mode,
           "started_at": time.time(), "n_channels": len(o.channels), "api": o.apis, "manager": o.managers}
    rep["health"] = o.health()

    if a.mode == "preflight":
        for cid, ctype, _g in o.channels:
            s = o.subscribers(cid, ctype)
            if s:
                o.bad("stale_state_subscribers", {"ch": cid, "n": len(s)})
            ms = o.max_seq(cid, ctype)
            if ms:
                o.bad("stale_state_messages", {"ch": cid, "max_message_seq": ms})

    elif a.mode == "seedcheck":
        o.check_subscribers(model_from(a.state_dir, "world_seed.json"), "seed")

    elif a.mode == "post":
        rep["drain"] = o.drain(o.ledger())
        o.check_subscribers(model_from(a.state_dir, "world_final.json"), "post")
        rep["probe"] = o.probe("post")
        write_json(o.snapshot_path(), o.account("post"), o.redact)

    elif a.mode == "restart":
        label = "restart%d" % a.restart_index
        snap = read_json(o.snapshot_path())
        o.check_subscribers(model_from(a.state_dir, "world_final.json"), label)
        rep["probe"] = o.probe(label)
        # the next restart (T3 leader after follower) compares against this state
        write_json(o.snapshot_path(), o.account(label, snap=snap, probe_label=label), o.redact)

    elif a.mode == "teardown":
        model = model_from(a.state_dir, "world_final.json")
        out = defaultdict(int)
        for cid, ctype, _g in o.channels:
            r = o.client.call("POST", o.apis[0], "/channel/subscriber_remove_all",
                              {"channel_id": cid, "channel_type": ctype}, timeout=a.write_timeout)
            out[r.outcome] += 1
            model.remove_all(cid, r.outcome)
            if r.outcome != OK:
                o.bad("teardown_not_ok", {"ch": cid, "http": r.status, "err": r.err})
        rep["teardown_outcomes"] = dict(out)
        write_json(os.path.join(a.state_dir, "world_teardown.json"), {"model": model.to_json()}, o.redact)
        for cid, ctype, _g in o.channels:
            s = o.subscribers(cid, ctype)
            if s:
                o.bad("not_empty_after_teardown", {"ch": cid, "n": len(s)}, len(s))
        rep["probe"] = o.probe("teardown")
        o.account("teardown")

    rep["verdict"] = o.verdict()
    rep["fail"] = dict(o.fail)
    rep["read_errors"] = dict(o.read_errors)
    rep["info"] = dict(o.info)
    rep["examples"] = {k: v for k, v in o.examples.items()}
    rep["finished_at"] = time.time()
    name = a.mode if a.mode != "restart" else "restart%d" % a.restart_index
    write_json(os.path.join(a.state_dir, "oracle_%s.json" % name), rep, o.redact)
    print(o.redact(json.dumps({"mode": name, "verdict": rep["verdict"], "fail": rep["fail"],
                               "read_errors": rep["read_errors"]})), flush=True)
    return {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 3}[rep["verdict"]]


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
                                 allow_abbrev=False)
    ap.add_argument("mode", choices=("preflight", "seedcheck", "post", "restart", "teardown"))
    ap.add_argument("--api", required=True)
    ap.add_argument("--manager", required=True, help="comma-separated manager URLs (same node order as --api)")
    ap.add_argument("--token-file", default="")
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--run-id", default="r1", help="must equal the harness --run-id of this arm")
    ap.add_argument("--groups", type=int, default=42)
    ap.add_argument("--children", type=int, default=8)
    ap.add_argument("--channel-prefix", default="repro")
    ap.add_argument("--read-timeout", type=float, default=8.0)
    ap.add_argument("--write-timeout", type=float, default=8.0)
    ap.add_argument("--drain-timeout", type=float, default=600.0)
    ap.add_argument("--drain-interval", type=float, default=5.0)
    ap.add_argument("--restart-index", type=int, default=1)
    a = ap.parse_args(argv)
    if not a.run_id.isalnum():
        ap.error("--run-id must be alphanumeric")
    return a


if __name__ == "__main__":
    sys.exit(run(parse_args()))
