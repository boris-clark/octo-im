#!/usr/bin/env python3
"""Post-workload checks for XIN-1968: correctness (all 378 channels readable),
0->378->0 teardown (reconcile every channel to empty membership), recovery drain,
and (invoked separately after a container restart) persistence re-check.

Uses a high revision base so every reconcile is monotonically newer than anything
the 60-min workload left behind."""
import base64, json, sys, time, http.client

BASE_H, BASE_P = "127.0.0.1", 15001
REVBASE = 50_000_000

def req(method, path, obj=None, timeout=8):
    c = http.client.HTTPConnection(BASE_H, BASE_P, timeout=timeout)
    try:
        body = json.dumps(obj).encode() if obj is not None else None
        h = {"Content-Type": "application/json"} if obj is not None else {}
        c.request(method, path, body=body, headers=h)
        r = c.getresponse(); return r.status, r.read()
    finally:
        c.close()

def channels():
    out = []
    for gi in range(1, 43):
        g = "repro_g%03d" % gi
        out.append((g, 2, g))
        for ci in range(1, 9):
            out.append(("%s____t%d" % (g, ci), 5, g))
    return out

def sid():
    import random
    return "".join("%02x" % random.randint(0, 255) for _ in range(32))

def probe_readable(chs):
    ok = 0; bad = []
    payload = base64.b64encode(b"postcheck-probe").decode()
    for cid, ct, g in chs:
        st, b = req("POST", "/message/send",
                    {"from_uid": "u00000", "channel_id": cid, "channel_type": ct, "payload": payload})
        if st == 200:
            ok += 1
        else:
            bad.append((cid, st, b[:120].decode("utf-8", "replace")))
    return ok, bad

def recovery_status(cid, ct):
    st, b = req("GET", "/channel/subscriber_recovery?channel_id=%s&channel_type=%d" % (cid, ct))
    if st != 200:
        return None
    try:
        return json.loads(b).get("data", {})
    except Exception:
        return None

def teardown_to_zero(chs):
    """0<-378: reconcile every channel to empty membership (revision bump)."""
    ok = 0; bad = []
    rev = REVBASE
    for cid, ct, g in chs:
        rev += 1
        op = "teardown-%s-%d" % (cid, rev)
        snap = {"channel_id": cid, "channel_type": ct, "operation_id": op, "revision": rev,
                "snapshot_id": "", "page_index": 0, "page_count": 0, "range_start": "", "range_end": "",
                "subscribers": [], "denylist": [], "ban": 0, "large": 0, "disband": 0}
        st, b = req("POST", "/channel/subscriber_reconcile", snap, timeout=8)
        if st == 200:
            ok += 1
        else:
            bad.append((cid, st, b[:140].decode("utf-8", "replace")))
    return ok, bad

def global_recovery():
    st, b = req("GET", "/channel/subscriber_recovery")
    if st != 200:
        return None
    return json.loads(b).get("data", {})

def drain_wait(timeout_s=120):
    t0 = time.time(); peak = 0; last = None
    while time.time() - t0 < timeout_s:
        d = global_recovery() or {}
        pend = d.get("pending_units", 0) or 0
        rt = d.get("runtime", {}) or {}
        inflight = (rt.get("target_inflight", 0) or 0) + (rt.get("target_waiting", 0) or 0)
        peak = max(peak, pend, inflight)
        last = {"pending_units": pend, "runtime": rt}
        if pend == 0 and inflight == 0:
            return True, round(time.time() - t0, 1), peak, last
        time.sleep(2)
    return False, round(time.time() - t0, 1), peak, last

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "post"
    chs = channels()
    out = {"mode": mode, "t": round(time.time(), 3), "n_channels": len(chs)}
    st, b = req("GET", "/health"); out["health"] = b.decode()[:40]
    if mode == "post":
        ok, bad = probe_readable(chs)
        out["readable_ok"] = ok; out["readable_bad"] = bad[:20]
        # sample applied-revision state
        samp = {}
        for cid, ct, g in [chs[0], chs[1], chs[9], chs[200], chs[-1]]:
            samp[cid] = recovery_status(cid, ct)
        out["recovery_sample"] = samp
        drained, secs, peak, last = drain_wait(60)
        out["pre_teardown_drain"] = {"drained": drained, "secs": secs, "peak": peak, "last": last}
        # 378 -> 0
        tok, tbad = teardown_to_zero(chs)
        out["teardown_ok"] = tok; out["teardown_bad"] = tbad[:20]
        drained, secs, peak, last = drain_wait(180)
        out["teardown_drain"] = {"drained": drained, "secs": secs, "peak": peak, "last": last}
        ok2, bad2 = probe_readable(chs)
        out["readable_after_teardown_ok"] = ok2; out["readable_after_teardown_bad"] = bad2[:20]
    elif mode == "restart":
        ok, bad = probe_readable(chs)
        out["readable_ok"] = ok; out["readable_bad"] = bad[:20]
        samp = {}
        for cid, ct, g in [chs[0], chs[1], chs[9], chs[200], chs[-1]]:
            samp[cid] = recovery_status(cid, ct)
        out["recovery_sample"] = samp
        drained, secs, peak, last = drain_wait(120)
        out["post_restart_drain"] = {"drained": drained, "secs": secs, "peak": peak, "last": last}
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
