#!/usr/bin/env python3
"""Disposable in-process mock of the PR70 HTTP subset used by the harness.

Self-test fixture only. It models the request/response shapes and the leader
forwarding read from PR70 source at bbc2be61 / a9021588 (identical there):

  API     GET  /health                          {"status":"ok"}
          POST /channel/subscriber_add          {"status":200}       forwarded to slot leader
          POST /channel/subscriber_remove       {"status":200}       forwarded to slot leader
          POST /channel/subscriber_remove_all   {"status":200}       forwarded to slot leader
          POST /channel/info                    {"status":200}       forwarded to slot leader;
                                                 ban/disband/send_ban/allow_stranger reset to 0
          POST /message/send                    {"status":200,"data":{"message_id","client_msg_no"}}
          POST /channel/messagesync             {..., "more", "messages"}  forwarded to leader;
                                                 creates the channel cluster config
          GET  /channel/max_message_seq         {"message_seq":N}    forwarded to leader;
                                                 0 while no channel config is loaded
          errors: HTTP 400 {"msg","status":400}; missing/wrong token: HTTP 401
  API and manager (ServerAPI is mounted on both, internal/api/server_http.go:147,
  server_manager.go:119):
          GET  /cluster/node                    bare NodeConfig object (api_node.go nodeGet,
                                                 model.go NodeConfig): "id", "role", "online",
                                                 ..., membership "status":3 (joined) - NOT an
                                                 HTTP status envelope
          GET  /cluster/nodes                   {"total", "data":[NodeConfig, ...]}
          GET  /cluster/channels/<id>/<type>/subscribers   JSON array; non-leader forwards
                                                 to leader api_server_addr (api_channel.go:242)
          GET  /cluster/channels/<id>/<type>/config   leader: 200 bare ChannelClusterConfig
                                                 with "learder_id" (and "status" when non-zero);
                                                 non-leader: 400 "slot leader is not current node"
  Manager GET  /metrics (no auth), /varz

Leader of a channel: node (crc32(channel_id) % 64) % nodes, or fault leader_override.
Every node keeps its own replica view; a read is served from the view of the node
that handles it, so replica divergence on a non-leader is invisible through the
public routes, exactly as on PR70.

Fault switches (dict) drive the deterministic red/limitation controls:
  drop_add_uid        uid silently not stored by subscriber_add (still 200)
  phantom_uid         uid silently added to every channel on subscriber_add
  dup_subscriber_read subscriber read returns the first uid twice
  diverge_node        node index whose replica view omits the first uid of every channel
  stale_route_node    node index that serves subscriber reads locally instead of forwarding
  leader_override     node index that leads every channel
  drop_message_n      n-th accepted send (1-based) is acknowledged but never stored
  dup_message_n       n-th accepted send is stored twice
  seq_gap_n           n-th accepted send skips one seq
  seq_gap_mid_channel the first send into a channel that already holds a message skips one seq
  store_then_500_send_n  n-th send is stored but answered with HTTP 500
  http500_every       every k-th write returns HTTP 500 after applying nothing
  apply_then_500_every every k-th subscriber write applies, then returns HTTP 500
  slow_every / slow_secs  every k-th write sleeps slow_secs before answering
  trickle_secs        responses to writes are sent one byte at a time over this many seconds
  visibility_delay    seconds before an accepted message becomes readable
  lose_on_restart     messages per channel dropped when the mock is restarted
  stale_channel       channel pre-populated with a subscriber before the run
  stale_message_channel  channel pre-populated with a message whose config is not loaded
  unhealthy           /health returns 500
  messagesync_500     message reads fail with HTTP 500
  node_status         membership "status" served in NodeConfig (default 3 = joined;
                      None omits it like omitempty for 0)
  node_status_400     /cluster/node answers HTTP 400 {"msg","status":400}
  node_app_error      /cluster/node answers HTTP 200 {"msg","status":500} (application error)
  node_malformed      /cluster/node answers HTTP 200 with a non-JSON body
  nodes_malformed_entry  /cluster/nodes "data" holds an extra entry: "no_id" or "zero_id"
  channel_config_status  "status" served in ChannelClusterConfig (1 = candidate)
  channel_config_app_error  leader answers the channel config read with HTTP 200
                      {"msg","status":500}
"""
import argparse
import http.client
import json
import re
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SUB_READ = re.compile(r"^/cluster/channels/([^/]+)/(\d+)/subscribers$")
CFG_READ = re.compile(r"^/cluster/channels/([^/]+)/(\d+)/config$")
LEADER_ROUTED_POST = ("/channel/subscriber_add", "/channel/subscriber_remove", "/channel/subscriber_remove_all",
                      "/channel/info", "/channel/messagesync")


class State:
    def __init__(self, token="", faults=None, nodes=1):
        self.token = token
        self.faults = dict(faults or {})
        self.nodes = nodes
        self.api_urls = []
        self.forward_conns = []
        self.mu = threading.Lock()
        self.subs = {}
        self.info = {}
        self.msgs = {}
        self.seq = {}
        self.configs = set()
        self.sends = 0
        self.writes = 0
        self.next_mid = 1000
        self.proposals = 0
        st = self.faults.get("stale_channel")
        if st:
            self.subs[(st, 2)] = {"u99999"}
        sm = self.faults.get("stale_message_channel")
        if sm:
            key = (sm, 2)
            self.seq[key] = 1
            self.msgs[key] = [{"message_seq": 1, "client_msg_no": "old-run-1", "message_id": 1, "from_uid": "x",
                               "channel_id": sm, "channel_type": 2, "payload": "eA==", "visible_at": 0}]

    def leader(self, cid):
        if self.faults.get("leader_override") is not None:
            return int(self.faults["leader_override"])
        return (zlib.crc32(cid.encode()) % 64) % self.nodes

    def node_config(self, k):
        """NodeConfig as served by GET /cluster/node and in GET /cluster/nodes "data".

        Field names follow pkg/cluster/cluster/model.go NodeConfig; "status" is the
        cluster membership state (types.NodeStatus: 1 will-join, 2 joining, 3 joined),
        omitted when 0."""
        n = {"id": k + 1, "is_leader": 1 if k == 0 else 0, "role": 1, "cluster_addr": "tcp://127.0.0.1:%d" % (11110 + k),
             "api_server_addr": self.api_urls[k] if k < len(self.api_urls) else "", "online": 1,
             "allow_vote": 1, "slot_count": 64, "term": 1, "slot_leader_count": 64 // self.nodes,
             "uptime": "1m0s", "app_version": "mock", "config_version": 7}
        st = self.faults.get("node_status", 3)
        if st:
            n["status"] = st
            n["status_format"] = {1: "将加入", 2: "加入中", 3: "已加入"}.get(st, "")
        return n

    def view(self, node, key):
        """Subscriber set as stored on `node`'s local replica."""
        uids = sorted(self.subs.get(key, set()))
        if self.faults.get("diverge_node") is not None and int(self.faults["diverge_node"]) == node and uids:
            uids = uids[1:]
        return uids

    def _tick_write(self):
        self.writes += 1
        return self.writes

    def restart(self):
        k = int(self.faults.get("lose_on_restart", 0))
        with self.mu:
            self.configs = set()  # configs are loaded lazily again after a restart
            if k:
                for lst in self.msgs.values():
                    if lst:
                        del lst[-k:]


def make_handler(state, node_index, role):
    f = state.faults
    _tl = threading.local()

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _send(self, code, obj, trickle=0.0):
            b = json.dumps(obj).encode() if not isinstance(obj, bytes) else obj
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            if trickle and len(b) > 1:
                step = trickle / len(b)
                for i in range(len(b)):
                    self.wfile.write(b[i:i + 1])
                    self.wfile.flush()
                    time.sleep(step)
            else:
                self.wfile.write(b)

        def _auth(self):
            if state.token and self.headers.get("token") != state.token:
                self.send_response(401)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return False
            return True

        def _raw_body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(n) if n else b""

        def _err(self, msg, code=400):
            self._send(code, {"msg": msg, "status": code})

        def _forward(self, leader, method, path, raw):
            """c.Forward / c.ForwardWithBody: relay the request to the leader's API address.

            Keep-alive connections are pooled per handler thread, like a long-lived HTTP client."""
            pool = getattr(_tl, "pool", None)
            if pool is None:
                pool = _tl.pool = {}
            base = state.api_urls[leader]
            hdr = {"token": self.headers.get("token") or ""}
            if raw:
                hdr["Content-Type"] = "application/json"
            for attempt in (0, 1):  # one reconnect only when a pooled idle connection was closed
                c = pool.get(base)
                if c is None:
                    u = urlparse(base)
                    c = pool[base] = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
                    with state.mu:
                        state.forward_conns.append(c)
                fresh = c.sock is None
                try:
                    c.request(method, path, body=raw or None, headers=hdr)
                    r = c.getresponse()
                    body = r.read()
                    return self._send(r.status, body)
                except Exception as e:
                    c.close()
                    pool.pop(base, None)
                    if fresh or attempt:
                        return self._err("forward to node %d failed: %s" % (leader + 1, e))

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

        def _ok(self):
            self._send(200, {"status": 200}, float(f.get("trickle_secs", 0)))

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
            if u.path == "/cluster/node":
                if f.get("node_status_400"):
                    return self._err("injected node error")
                if f.get("node_app_error"):
                    return self._send(200, {"msg": "injected application error", "status": 500})
                if f.get("node_malformed"):
                    return self._send(200, b"<html>not json</html>")
                return self._send(200, state.node_config(node_index))
            if u.path == "/cluster/nodes":
                data = [state.node_config(k) for k in range(state.nodes)]
                if f.get("nodes_malformed_entry") == "no_id":
                    data.append({"status": 3, "online": 1})
                elif f.get("nodes_malformed_entry") == "zero_id":
                    data.append({"id": 0, "status": 3, "online": 1})
                return self._send(200, {"total": len(data), "data": data})
            if role == "api" and u.path == "/channel/max_message_seq":
                key = (q.get("channel_id", ""), int(q.get("channel_type", "0")))
                leader = state.leader(key[0])
                if leader != node_index:
                    return self._forward(leader, "GET", self.path, b"")
                with state.mu:  # LeaderOfChannelForRead: 0 while no config is loaded
                    return self._send(200, {"message_seq": state.seq.get(key, 0) if key in state.configs else 0})
            m = SUB_READ.match(u.path)
            if m:
                key = (m.group(1), int(m.group(2)))
                leader = state.leader(key[0])
                stale = f.get("stale_route_node") is not None and int(f["stale_route_node"]) == node_index
                if leader != node_index and not stale:
                    return self._forward(leader, "GET", u.path, b"")
                with state.mu:
                    uids = state.view(node_index, key)
                if f.get("dup_subscriber_read") and uids:
                    uids = [uids[0]] + uids
                return self._send(200, uids)
            m = CFG_READ.match(u.path)
            if m:
                key = (m.group(1), int(m.group(2)))
                if state.leader(key[0]) != node_index:
                    return self._err("slot leader is not current node")
                if f.get("channel_config_app_error"):
                    return self._send(200, {"msg": "injected application error", "status": 500})
                with state.mu:
                    if key not in state.configs:
                        return self._err("not found")
                cfg = {"channel_id": key[0], "channel_type": key[1], "learder_id": node_index + 1,
                       "replicas": [k + 1 for k in range(state.nodes)]}
                if f.get("channel_config_status"):
                    cfg["status"] = int(f["channel_config_status"])
                return self._send(200, cfg)
            self._send(404, {"msg": "404 page not found"})

        def do_POST(self):
            u = urlparse(self.path)
            raw = self._raw_body()  # always consume the body so keep-alive framing stays intact
            if not self._auth():
                return
            if role != "api":
                return self._send(404, {"msg": "404 page not found"})
            try:
                b = json.loads(raw or b"{}")
            except Exception:
                return self._err("数据格式有误！")
            key = (b.get("channel_id", ""), int(b.get("channel_type", 0) or 0))
            p = u.path
            if p in LEADER_ROUTED_POST and key[0]:
                leader = state.leader(key[0])
                if leader != node_index:
                    return self._forward(leader, "POST", p, raw)
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
                return self._ok()
            if p == "/channel/subscriber_remove_all":
                skip, _ = self._write_faults(False)
                if skip:
                    return self._err("injected 500", 500)
                with state.mu:
                    state.subs[key] = set()
                return self._ok()
            if p == "/channel/info":
                skip, _ = self._write_faults(False)
                if skip:
                    return self._err("injected 500", 500)
                with state.mu:  # full channel-info upsert, as channelInfoReq.ToChannelInfo
                    state.info[key] = {"large": b.get("large", 0) == 1, "ban": b.get("ban", 0) == 1,
                                       "disband": b.get("disband", 0) == 1, "send_ban": b.get("send_ban", 0) == 1,
                                       "allow_stranger": b.get("allow_stranger", 0) == 1}
                return self._ok()
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
                    state.configs.add(key)
                    copies = 0 if f.get("drop_message_n") == n else (2 if f.get("dup_message_n") == n else 1)
                    gap = f.get("seq_gap_n") == n
                    if f.get("seq_gap_mid_channel") and not state.faults.get("_mid_gap_done") and state.seq.get(key, 0) >= 1:
                        gap = state.faults["_mid_gap_done"] = True
                    for _ in range(copies):
                        s = state.seq.get(key, 0) + (2 if gap else 1)
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
                    state.configs.add(key)  # LeaderOfChannel -> GetOrCreateChannelClusterConfigFromSlotLeader
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
    request_queue_size = 1024  # the default backlog of 5 refuses bursts of forwarded connections

    def handle_error(self, request, client_address):
        pass  # clients that time out and close are expected in the failure controls


class MockCluster:
    """N mock nodes with per-node replica views and leader forwarding over real HTTP."""

    def __init__(self, nodes=1, token="", faults=None, state=None, ports=None):
        self.state = state or State(token, faults, nodes)
        self.state.nodes = nodes
        self.servers = []
        ports = ports or [(0, 0)] * nodes
        for i in range(nodes):
            for role, port in (("api", ports[i][0]), ("manager", ports[i][1])):
                s = _Server(("127.0.0.1", port), make_handler(self.state, i, role))
                self.servers.append((i, role, s))
        self.state.api_urls = ["http://127.0.0.1:%d" % s.server_address[1] for _i, r, s in self.servers if r == "api"]
        for _i, _r, s in self.servers:
            threading.Thread(target=s.serve_forever, daemon=True).start()

    def urls(self, role):
        return ",".join("http://127.0.0.1:%d" % s.server_address[1] for _i, r, s in self.servers if r == role)

    def ports(self):
        by = {}
        for i, r, s in self.servers:
            by.setdefault(i, {})[r] = s.server_address[1]
        return [(by[i]["api"], by[i]["manager"]) for i in sorted(by)]

    def stop_node(self, index):
        """Take one node fully offline (both ports); other nodes keep forwarding to it."""
        keep = []
        for i, r, s in self.servers:
            if i == index:
                s.shutdown()
                s.server_close()
            else:
                keep.append((i, r, s))
        self.servers = keep

    def stop(self):
        for _i, _r, s in self.servers:
            s.shutdown()
            s.server_close()
        self.servers = []
        with self.state.mu:
            conns, self.state.forward_conns = self.state.forward_conns, []
        for c in conns:
            c.close()

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
