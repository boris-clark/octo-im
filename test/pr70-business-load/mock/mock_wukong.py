#!/usr/bin/env python3
"""Disposable in-process mock of the PR70 HTTP subset used by the harness.

Self-test fixture only. It models the PR70 request/response shapes read from
internal/api/channel.go, internal/api/message.go and
pkg/cluster/cluster/api_channel.go at bbc2be61 / a9021588 (identical there):

  API     GET  /health                          {"status":"ok"}
          POST /channel/subscriber_add          {"status":200}
          POST /channel/subscriber_remove       {"status":200}
          POST /channel/subscriber_remove_all   {"status":200}
          POST /channel/info                    {"status":200}
          POST /message/send                    {"status":200,"data":{"message_id","client_msg_no"}}
          POST /channel/messagesync             {"start_message_seq","end_message_seq","more","messages":[...]}
          GET  /channel/max_message_seq         {"message_seq":N}
          errors: HTTP 400 {"msg","status":400}; missing/wrong token: HTTP 401
  Manager GET  /metrics                         (no auth)
          GET  /cluster/channels/<id>/<type>/subscribers   JSON array of uids

Fault switches (dict) drive the deterministic red controls:
  drop_add_uid        uid silently not stored by subscriber_add (still 200)
  phantom_uid         uid silently added to every channel on subscriber_add
  dup_subscriber_read subscriber read returns the first uid twice
  diverge_node        node index whose subscriber read omits one uid
  drop_message_n      n-th accepted send (1-based) is acknowledged but never stored
  dup_message_n       n-th accepted send is stored twice
  seq_gap_n           n-th accepted send skips one seq
  store_then_500_send_n  n-th send is stored but answered with HTTP 500
  http500_every       every k-th write returns HTTP 500 after applying nothing
  apply_then_500_every every k-th subscriber write applies, then returns HTTP 500
  slow_every / slow_secs  every k-th write sleeps slow_secs before answering
  visibility_delay    seconds before an accepted message becomes readable
  lose_on_restart     messages per channel dropped when the mock is restarted
  stale_channel       channel pre-populated before the run
  unhealthy           /health returns 500
  messagesync_500     message reads fail with HTTP 500
"""
import argparse
import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SUB_READ = re.compile(r"^/cluster/channels/([^/]+)/(\d+)/subscribers$")


class State:
    def __init__(self, token="", faults=None):
        self.token = token
        self.faults = dict(faults or {})
        self.mu = threading.Lock()
        self.subs = {}
        self.info = {}
        self.msgs = {}
        self.seq = {}
        self.sends = 0
        self.writes = 0
        self.next_mid = 1000
        self.proposals = 0
        st = self.faults.get("stale_channel")
        if st:
            self.subs[(st, 2)] = {"u99999"}

    def _tick_write(self):
        self.writes += 1
        return self.writes

    def restart(self):
        k = int(self.faults.get("lose_on_restart", 0))
        if k:
            with self.mu:
                for key, lst in self.msgs.items():
                    if lst:
                        del lst[-k:]


def make_handler(state, node_index, role):
    f = state.faults

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _send(self, code, obj):
            b = json.dumps(obj).encode() if not isinstance(obj, bytes) else obj
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def _auth(self):
            if state.token and self.headers.get("token") != state.token:
                self.send_response(401)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return False
            return True

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n) or b"{}")

        def _err(self, msg, code=400):
            self._send(code, {"msg": msg, "status": code})

        def _write_faults(self, sub_write):
            """Returns (skip_apply, error_after_apply)."""
            with state.mu:
                n = state._tick_write()
            if f.get("slow_every") and n % int(f["slow_every"]) == 0:
                time.sleep(float(f.get("slow_secs", 3)))
            if f.get("http500_every") and n % int(f["http500_every"]) == 0:
                return True, False
            if sub_write and f.get("apply_then_500_every") and n % int(f["apply_then_500_every"]) == 0:
                return False, True
            return False, False

        def do_GET(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            if role == "manager" and u.path == "/metrics":
                txt = "cluster_channel_propose_count_total %d\ncluster_channel_propose_failed_count_total 0\n" % state.proposals
                b = txt.encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            if not self._auth():
                return
            if role == "api" and u.path == "/health":
                if f.get("unhealthy"):
                    return self._send(500, {"status": "error", "message": "cluster not ready"})
                return self._send(200, {"status": "ok"})
            if u.path == "/varz":
                return self._send(200, {"goroutine": 1, "mem": 1, "cpu": 0.0, "connections": 0})
            if role == "api" and u.path == "/channel/max_message_seq":
                key = (q.get("channel_id", ""), int(q.get("channel_type", "0")))
                with state.mu:
                    return self._send(200, {"message_seq": state.seq.get(key, 0)})
            m = SUB_READ.match(u.path)
            if role == "manager" and m:
                key = (m.group(1), int(m.group(2)))
                with state.mu:
                    uids = sorted(state.subs.get(key, set()))
                if f.get("dup_subscriber_read") and uids:
                    uids = [uids[0]] + uids
                if f.get("diverge_node") is not None and int(f["diverge_node"]) == node_index and uids:
                    uids = uids[1:]
                return self._send(200, uids)
            self._send(404, {"msg": "404 page not found"})

        def do_POST(self):
            u = urlparse(self.path)
            try:
                b = self._body()  # always consume the body so keep-alive framing stays intact
            except Exception:
                b = None
            if not self._auth():
                return
            if role != "api":
                return self._send(404, {"msg": "404 page not found"})
            if b is None:
                return self._err("数据格式有误！")
            key = (b.get("channel_id", ""), int(b.get("channel_type", 0) or 0))
            p = u.path
            if p in ("/channel/subscriber_add", "/channel/subscriber_remove"):
                subs = [s for s in (b.get("subscribers") or []) if s.strip()]
                if not key[0] or not subs:
                    return self._err("订阅者不能为空！")
                skip, err_after = self._write_faults(True)
                if skip:
                    return self._err("injected 500", 500)
                with state.mu:
                    state.proposals += 1
                    cur = state.subs.setdefault(key, set())
                    if p.endswith("_add"):
                        if b.get("reset") == 1:
                            cur.clear()
                        for s in subs:
                            if s != f.get("drop_add_uid"):
                                cur.add(s)
                        if f.get("phantom_uid"):
                            cur.add(f["phantom_uid"])
                    else:
                        for s in subs:
                            cur.discard(s)
                if err_after:
                    return self._err("injected 500 after apply", 500)
                return self._send(200, {"status": 200})
            if p == "/channel/subscriber_remove_all":
                skip, _ = self._write_faults(False)
                if skip:
                    return self._err("injected 500", 500)
                with state.mu:
                    state.subs[key] = set()
                return self._send(200, {"status": 200})
            if p == "/channel/info":
                skip, _ = self._write_faults(False)
                if skip:
                    return self._err("injected 500", 500)
                with state.mu:
                    state.info[key] = {"large": b.get("large", 0)}
                return self._send(200, {"status": 200})
            if p == "/message/send":
                if not b.get("payload"):
                    return self._err("payload不能为空！")
                skip, _ = self._write_faults(False)
                if skip:
                    return self._err("injected 500", 500)
                with state.mu:
                    state.sends += 1
                    n = state.sends
                    state.next_mid += 1
                    mid = state.next_mid
                    copies = 0 if f.get("drop_message_n") == n else (2 if f.get("dup_message_n") == n else 1)
                    for _ in range(copies):
                        s = state.seq.get(key, 0) + (2 if f.get("seq_gap_n") == n else 1)
                        state.seq[key] = s
                        state.msgs.setdefault(key, []).append({
                            "message_seq": s, "client_msg_no": b.get("client_msg_no", ""), "message_id": mid,
                            "from_uid": b.get("from_uid", ""), "channel_id": key[0], "channel_type": key[1],
                            "payload": b["payload"], "visible_at": time.time() + float(f.get("visibility_delay", 0))})
                if f.get("store_then_500_send_n") == n:
                    return self._err("injected 500 after store", 500)
                return self._send(200, {"status": 200, "data": {"message_id": mid, "client_msg_no": b.get("client_msg_no")}})
            if p == "/channel/messagesync":
                if f.get("messagesync_500"):
                    return self._err("injected read failure", 500)
                start, end, limit = int(b.get("start_message_seq", 0)), int(b.get("end_message_seq", 0)), int(b.get("limit", 0))
                if b.get("pull_mode") != 1 or start == 0:
                    return self._err("mock only implements pull_mode=1 from start>=1")
                now = time.time()
                with state.mu:
                    allm = [m for m in state.msgs.get(key, []) if m["visible_at"] <= now]
                    lst = [dict((k, v) for k, v in m.items() if k != "visible_at") for m in allm
                           if m["message_seq"] >= start and (end == 0 or m["message_seq"] < end)]
                lst = lst[:limit]
                return self._send(200, {"start_message_seq": start, "end_message_seq": end,
                                        "more": 1 if len(lst) >= limit else 0, "messages": lst})
            self._send(404, {"msg": "404 page not found"})

    return H


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass  # clients that time out and close are expected in the failure controls


class MockCluster:
    """N mock nodes sharing one State (a single logical store, like a healthy cluster)."""

    def __init__(self, nodes=1, token="", faults=None, state=None, ports=None):
        self.state = state or State(token, faults)
        self.servers = []
        self.threads = []
        ports = ports or [(0, 0)] * nodes
        for i in range(nodes):
            for role, port in (("api", ports[i][0]), ("manager", ports[i][1])):
                s = _Server(("127.0.0.1", port), make_handler(self.state, i, role))
                self.servers.append((i, role, s))
                t = threading.Thread(target=s.serve_forever, daemon=True)
                t.start()
                self.threads.append(t)

    def urls(self, role):
        return ",".join("http://127.0.0.1:%d" % s.server_address[1] for _i, r, s in self.servers if r == role)

    def ports(self):
        by = {}
        for i, r, s in self.servers:
            by.setdefault(i, {})[r] = s.server_address[1]
        return [(by[i]["api"], by[i]["manager"]) for i in sorted(by)]

    def stop(self):
        for _i, _r, s in self.servers:
            s.shutdown()
            s.server_close()

    def graceful_restart(self):
        """Stop every node, apply restart faults, start again on the same ports with the same store."""
        ports = self.ports()
        self.stop()
        self.state.restart()
        return MockCluster(len(ports), state=self.state, ports=ports)


def main():
    ap = argparse.ArgumentParser(description="run the mock for manual smoke tests", allow_abbrev=False)
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--token-file", default="")
    ap.add_argument("--faults", default="{}")
    a = ap.parse_args()
    tok = open(a.token_file).read().strip() if a.token_file else ""
    c = MockCluster(a.nodes, tok, json.loads(a.faults))
    print(json.dumps({"api": c.urls("api"), "manager": c.urls("manager")}), flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        c.stop()


if __name__ == "__main__":
    main()
