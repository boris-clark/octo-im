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
import subprocess
import sys
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
        # The timeout is one wall-clock deadline for the whole request (connect, send,
        # headers and body), not a per-socket-operation idle timeout: before every
        # blocking step the socket timeout is set to the time remaining.
        deadline = t0 + c.timeout

        def arm():
            left = deadline - time.monotonic()
            if left <= 0:
                raise socket.timeout("whole-request deadline exceeded")
            c.sock.settimeout(left)

        if c.sock is None:
            try:
                c.connect()
                arm()
            except Exception as e:  # nothing was sent
                self._drop(base)
                return Result(None, b"", time.monotonic() - t0, NOT_SENT, classify_error(None, b"", e), t_wall)
        try:
            arm()
            c.request(method, path, body=data, headers=headers)
            arm()
            r = c.getresponse()
            chunks = []
            while True:
                arm()
                chunk = r.read1(65536)  # returns what is available, so a trickling body cannot extend the deadline
                if not chunk:
                    break
                chunks.append(chunk)
            arm()
            chunks.append(r.read())  # zero-length final read marks the response closed for keep-alive reuse
            body = b"".join(chunks)
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


class JournalError(OSError):
    pass


def _journal_writer(path, sock, result):
    """Isolated I/O can be terminated and collected if the filesystem stalls."""
    count = 0
    try:
        with sock, sock.makefile("rb") as stream, gzip.open(path, "wt", compresslevel=1, encoding="utf-8") as f:
            while True:
                line = stream.readline(1048577)
                if not line:
                    break
                if len(line) > 1048576 or not line.endswith(b"\n"):
                    raise ValueError("incomplete or oversized journal record")
                f.write(line.decode("utf-8"))
                count += 1
        result.sendall(json.dumps([True, count, None]).encode())
    except BaseException as exc:
        result.sendall(json.dumps([False, count, type(exc).__name__]).encode())
    finally:
        result.close()


class Journal:
    """Bounded single-writer gzip process with direct socket backpressure.

    A bounded OS socket buffer replaces an in-process queue. Send: 5 seconds;
    close: 10 seconds then terminate and collect the child. No feeder thread.
    """
    def __init__(self, path, redact, evidence=False):
        self.path, self.redact = path, redact
        self.mu = threading.Lock()
        self.summary = None
        if evidence:
            from .evidence import Summary
            self.summary = Summary()
        self.count, self.closed, self.error = 0, False, None
        self.sock, child_sock = socket.socketpair()
        self.sock.settimeout(5.0)
        self.result, child = socket.socketpair()
        self.t = subprocess.Popen([sys.executable, "-I", os.path.abspath(__file__), path,
                                   str(child_sock.fileno()), str(child.fileno())],
                                  pass_fds=(child_sock.fileno(), child.fileno()))
        child_sock.close()
        child.close()

    def write(self, rec):
        line = (self.redact(json.dumps(rec, separators=(",", ":"))) + "\n").encode()
        with self.mu:
            if self.closed or self.error or self.t.poll() is not None:
                raise JournalError("journal writer closed or failed")
            try:
                self.sock.sendall(line)
            except OSError as exc:
                self.error = "journal send failed: " + type(exc).__name__
                raise JournalError(self.error) from exc
            if self.summary is not None:
                self.summary.record(rec)

    def close(self):
        if self.closed:
            if self.error:
                raise JournalError(self.error)
            return
        self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_WR)
            try:
                self.t.wait(timeout=10.0)
            except subprocess.TimeoutExpired as exc:
                raise JournalError("journal close deadline exceeded") from exc
            raw = self.result.recv(4096)
            if not raw:
                raise JournalError("journal final receipt missing")
            ok, self.count, err = json.loads(raw)
            if not ok or self.t.returncode != 0 or self.error:
                raise JournalError(self.error or "journal write/finalization failed: %s" % err)
        except (OSError, EOFError) as exc:
            self.error = str(exc)
            raise JournalError(self.error) from exc
        finally:
            self.sock.close()
            if self.t.poll() is None:
                self.t.terminate()
                try:
                    self.t.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    self.t.kill()
                    self.t.wait(timeout=2.0)
            self.result.close()


def read_journal(path):
    """Stream bounded input: 1 MiB/record, 64 GiB decoded, 30 minute scan limit.

    Reads require a completed gzip trailer; truncated and invalid JSON fail closed.
    Local regular files are required; named pipes/devices are rejected.
    """
    if not stat.S_ISREG(os.stat(path).st_mode):
        raise ValueError("journal must be a regular file")
    total = 0
    deadline = time.monotonic() + 1800
    with gzip.open(path, "rt", encoding="utf-8") as f:
        while True:
            line = f.readline(1048577)
            if not line:
                break
            total += len(line)
            if len(line) > 1048576 or total > 64 * 1024 ** 3 or time.monotonic() > deadline:
                raise ValueError("journal evidence size/time bound exceeded")
            if line.strip():
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
                "wkload/__init__.py", "wkload/common.py", "wkload/world.py", "wkload/evidence.py"):
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
    st = os.stat(path)
    if not stat.S_ISREG(st.st_mode) or st.st_size > 256 * 1024 ** 2:
        raise ValueError("JSON evidence must be a regular file <=256 MiB")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    _journal_writer(sys.argv[1], socket.socket(fileno=int(sys.argv[2])),
                    socket.socket(fileno=int(sys.argv[3])))
