"""HTTP client, outcome model, token handling and journaling shared by every tool.

Outcome model (applies to every write the harness issues):

  ok        HTTP 200 with a JSON body whose optional "status" field is 200.
  rejected  The server answered with any other HTTP status (or a non-200
            "status" field). The write may still have been partially applied
            server-side (for example AddSubscribers succeeded and the later
            conversation update failed), so it is NOT proof of no effect.
  unknown   The request may have reached the server but no complete response
            was read (client timeout, reset, broken pipe). Effect unknown.
  not_sent  TCP connect failed before any byte was sent. Proven no effect.

Nothing is retried. Every outcome is journaled and counted.
"""
import gzip
import hashlib
import http.client
import json
import os
import queue
import socket
import stat
import threading
import time
from urllib.parse import urlparse

OK, REJECTED, UNKNOWN, NOT_SENT = "ok", "rejected", "unknown", "not_sent"
OUTCOMES = (OK, REJECTED, UNKNOWN, NOT_SENT)


class TokenError(Exception):
    pass


def read_token_file(path):
    """Read the manager token from a private file. Refuses group/world readable files."""
    if not path:
        return ""
    st = os.stat(path)
    if st.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise TokenError("token file %s must not be group/world accessible (chmod 600)" % path)
    with open(path, "r", encoding="utf-8") as f:
        tok = f.read().strip()
    if not tok:
        raise TokenError("token file %s is empty" % path)
    return tok


class Redactor:
    """Replaces the secret in any text before it is written anywhere."""

    def __init__(self, secret):
        self.secret = secret or ""

    def __call__(self, text):
        if not isinstance(text, str):
            text = str(text)
        if self.secret and self.secret in text:
            text = text.replace(self.secret, "<redacted>")
        return text


def split_endpoints(value):
    eps = [v.strip().rstrip("/") for v in (value or "").split(",") if v.strip()]
    for e in eps:
        u = urlparse(e)
        if u.scheme != "http" or not u.hostname or not u.port:
            raise ValueError("endpoint must look like http://host:port, got %r" % e)
    return eps


def classify_error(status, body, exc):
    """Error taxonomy for PR70 (no subscriber_recovery budget / state:pending 503 exists)."""
    if exc is not None:
        name = type(exc).__name__
        msg = str(exc).lower()
        if isinstance(exc, (socket.timeout, TimeoutError)) or "timed out" in msg:
            return "client_timeout"
        if isinstance(exc, ConnectionRefusedError):
            return "conn_refused"
        if isinstance(exc, (ConnectionResetError, BrokenPipeError)) or "reset" in msg:
            return "conn_reset"
        if isinstance(exc, http.client.RemoteDisconnected):
            return "remote_disconnected"
        return "exc_" + name
    if status == 200:
        return "server_status_field"
    text = body.decode("utf-8", "replace").lower() if isinstance(body, bytes) else str(body).lower()
    if "propose" in text and "timeout" in text:
        return "http_%d_propose_timeout" % status
    if "deadline" in text:
        return "http_%d_context_deadline" % status
    if "timeout" in text:
        return "http_%d_timeout" % status
    if "leader" in text:
        return "http_%d_leader" % status
    return "http_%d" % status


class Result:
    __slots__ = ("status", "body", "dt", "outcome", "err", "t")

    def __init__(self, status, body, dt, outcome, err, t):
        self.status, self.body, self.dt, self.outcome, self.err, self.t = status, body, dt, outcome, err, t

    def json(self):
        try:
            return json.loads(self.body.decode("utf-8"))
        except Exception:
            return None


class Client:
    """Thread-local keep-alive connections; one bounded attempt per call; no retries."""

    def __init__(self, token, timeout):
        self.token = token
        self.timeout = float(timeout)
        self._tl = threading.local()
        self._all = []
        self._all_mu = threading.Lock()

    def _conn(self, base):
        conns = getattr(self._tl, "conns", None)
        if conns is None:
            conns = self._tl.conns = {}
        c = conns.get(base)
        if c is None:
            u = urlparse(base)
            c = conns[base] = http.client.HTTPConnection(u.hostname, u.port, timeout=self.timeout)
            with self._all_mu:
                self._all.append(c)
        return c

    def close_all(self):
        with self._all_mu:
            conns, self._all = self._all, []
        for c in conns:
            try:
                c.close()
            except Exception:
                pass

    def _drop(self, base):
        c = getattr(self._tl, "conns", {}).pop(base, None)
        if c is not None:
            try:
                c.close()
            except Exception:
                pass

    def call(self, method, base, path, obj=None, timeout=None):
        t_wall = time.time()
        t0 = time.monotonic()
        c = self._conn(base)
        c.timeout = float(timeout or self.timeout)
        headers = {}
        data = None
        if obj is not None:
            data = json.dumps(obj).encode()
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["token"] = self.token
        if c.sock is None:
            try:
                c.connect()
                c.sock.settimeout(c.timeout)
            except Exception as e:  # nothing was sent
                self._drop(base)
                return Result(None, b"", time.monotonic() - t0, NOT_SENT, classify_error(None, b"", e), t_wall)
        else:
            c.sock.settimeout(c.timeout)
        try:
            c.request(method, path, body=data, headers=headers)
            r = c.getresponse()
            body = r.read()
        except Exception as e:
            self._drop(base)
            return Result(None, b"", time.monotonic() - t0, UNKNOWN, classify_error(None, b"", e), t_wall)
        dt = time.monotonic() - t0
        if r.status != 200:
            return Result(r.status, body, dt, REJECTED, classify_error(r.status, body, None), t_wall)
        try:
            j = json.loads(body.decode("utf-8")) if body else None
        except Exception:
            j = None
        if isinstance(j, dict) and "status" in j and isinstance(j["status"], int) and j["status"] != 200:
            return Result(r.status, body, dt, REJECTED, classify_error(200, body, None), t_wall)
        return Result(r.status, body, dt, OK, None, t_wall)


class Journal:
    """Single-writer gzip JSONL journal. Every request outcome is appended, nothing sampled."""

    def __init__(self, path, redact):
        self.path = path
        self.redact = redact
        self.q = queue.Queue(maxsize=200000)
        self.f = gzip.open(path, "wt", compresslevel=1, encoding="utf-8")
        self.count = 0
        self.t = threading.Thread(target=self._run, name="journal", daemon=True)
        self.t.start()

    def write(self, rec):
        self.q.put(rec)

    def _run(self):
        while True:
            rec = self.q.get()
            if rec is None:
                break
            self.f.write(self.redact(json.dumps(rec, separators=(",", ":"))) + "\n")
            self.count += 1

    def close(self):
        self.q.put(None)
        self.t.join()
        self.f.close()


def read_journal(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 16), b""):
            h.update(b)
    return h.hexdigest()


def tool_hashes():
    """SHA-256 of every harness source file, recorded in each run header."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = {}
    for rel in ("harness.py", "oracle.py", "poller.py", "monitor_run.py", "analyze.py",
                "wkload/__init__.py", "wkload/common.py", "wkload/world.py"):
        p = os.path.join(root, rel)
        if os.path.exists(p):
            out[rel] = file_sha256(p)
    return out


def write_json(path, obj, redact=None):
    txt = json.dumps(obj, indent=2, sort_keys=True)
    if redact is not None:
        txt = redact(txt)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(txt + "\n")
    os.replace(tmp, path)


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
