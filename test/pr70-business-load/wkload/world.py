"""Dataset, deterministic per-worker operation streams and the recorded world model.

Dataset (unchanged from XIN-1968): G parent groups (channel_type 2, id
<prefix>_gNNN) each with C child threads (channel_type 5, id <group>____t<k>).
Defaults 42 x (1 + 8) = 378 channels; seed members parent u00000..u00149,
child u00000..u00019.

Determinism: every worker owns random.Random("<seed>:<role>:<wid>") and its
operation stream is a pure function of (seed, role, wid, dataset). No shared or
global RNG is used anywhere. Cross-worker interleaving and the number of
operations completed in a fixed wall-clock window still depend on server
latency; that is inherent to a closed-loop concurrent workload.
"""
import base64
import random
import re
import threading
import zlib
from array import array

from .common import OK, NOT_SENT

ADD, REMOVE = "add", "remove"
UID_SPACE = 100000


def uid(n):
    return "u%05d" % n


def build_channels(ngroups, nchildren, prefix="repro"):
    chans = []
    for gi in range(1, ngroups + 1):
        g = "%s_g%03d" % (prefix, gi)
        chans.append((g, 2, g))
        for ci in range(1, nchildren + 1):
            chans.append(("%s____t%d" % (g, ci), 5, g))
    return chans


def seed_members(ctype, parent_members, child_members):
    n = parent_members if ctype == 2 else child_members
    return [uid(i) for i in range(n)]


def worker_rng(seed, role, wid):
    return random.Random("%s:%s:%d" % (seed, role, wid))


def addremove_stream(seed, wid, channels, parent_members, child_members):
    """Infinite deterministic stream of (cid, ctype, action, uid).

    add    -> random uid in u00000..u99999 (as historical).
    remove -> a uid from the channel's seed members plus the uids this worker
              previously added to that channel. It may already be absent
              (removed by another worker); the delta remove is still issued and
              the server must treat it as a no-op.

    The remove pool is held as an int array and indexed with randrange(len),
    which consumes the RNG exactly like Random.choice on a list did in 6255599a,
    so the stream is byte-identical while memory stays at 4 bytes per add.
    """
    r = worker_rng(seed, "addremove", wid)
    added = {}
    while True:
        cid, ctype, _g = r.choice(channels)
        if r.random() < 0.5:
            n = r.randrange(UID_SPACE)
            added.setdefault(cid, array("l")).append(n)
            yield cid, ctype, ADD, uid(n)
        else:
            nseed = parent_members if ctype == 2 else child_members
            lst = added.get(cid, ())
            k = r.randrange(nseed + len(lst))
            yield cid, ctype, REMOVE, uid(k if k < nseed else lst[k - nseed])


def send_stream(seed, wid, channels, parent_members, child_members):
    """Infinite deterministic stream of (cid, ctype, from_uid, n)."""
    r = worker_rng(seed, "send", wid)
    n = 0
    while True:
        cid, ctype, _g = r.choice(channels)
        frm = r.choice(seed_members(ctype, parent_members, child_members))
        n += 1
        yield cid, ctype, frm, n


def upsert_stream(seed, wid, channels):
    """Infinite deterministic stream of (cid, ctype, large)."""
    r = worker_rng(seed, "upsert", wid)
    while True:
        cid, ctype, _g = r.choice(channels)
        yield cid, ctype, r.randint(0, 1)


# Possible-state encoding per (channel, uid), one byte:
#   POSS_P / POSS_A  states the uid may be in right now, given every write so far
#   FLOAT_ADD / FLOAT_REM  an ambiguous write in that direction may still land later
#   TOUCHED  the harness wrote this key at least once (coverage denominator)
# 0 means never touched, which is certainly absent.
POSS_P, POSS_A, FLOAT_ADD, FLOAT_REM, TOUCHED = 1, 2, 4, 8, 16
DEFAULT = POSS_A

APPLIED, NO_EFFECT, AMBIGUOUS = "applied", "no_effect", "ambiguous"


def effect_of(outcome, http_status):
    """Map a write outcome to what it proves about server state.

    applied    HTTP 200: the write was applied on the slot leader before the reply.
    no_effect  not_sent (TCP connect failed) or HTTP 401 (the manager-token
               middleware aborts before any handler runs, server_http.go:50-61).
    ambiguous  every other outcome: a 4xx/5xx can follow a committed store step
               (subscriber_add returns 400 after AddSubscribers when the
               conversation/tag update fails), a forwarded request can still be
               applied by the leader after the forwarding node answered, and an
               unknown outcome may land at any later time.
    """
    if outcome == OK:
        return APPLIED
    if outcome == NOT_SENT or http_status == 401:
        return NO_EFFECT
    return AMBIGUOUS


def possible(code):
    """Final states the uid may be in: current possibilities plus any floating write."""
    code = code or DEFAULT
    out = set()
    if code & POSS_P or code & FLOAT_ADD:
        out.add("P")
    if code & POSS_A or code & FLOAT_REM:
        out.add("A")
    return out


class SubscriberModel:
    """Recorded world model: per (channel, uid) possible states.

    Transitions for a write in direction d (add -> P, remove -> A):
      applied    POSS := {d}; floating writes are kept (they may still land after it)
      no_effect  unchanged (prior known state preserved)
      ambiguous  POSS := POSS + {d}; FLOAT += d
    remove_all applies the same rule to every touched uid of the channel.
    A uid is required present when its only possible state is P, required absent
    when it is only A, and is unconstrained only when both are possible. Writes
    to the same (channel, uid) are serialised by the harness, so the order of
    applied writes is known exactly.

    Storage: one bytearray of UID_SPACE bytes per touched channel, indexed by the
    uid number (all harness uids are u00000..u99999): 378 x 100 KB = ~38 MB at
    most, independent of the number of operations.
    """

    STRIPES = 4096

    def __init__(self):
        self.keys = {}
        self._mu = threading.Lock()
        self._stripes = [threading.Lock() for _ in range(self.STRIPES)]

    def key_lock(self, cid, u):
        return self._stripes[hash((cid, u)) % self.STRIPES]

    def _table(self, cid):
        t = self.keys.get(cid)
        if t is None:
            t = self.keys[cid] = bytearray(UID_SPACE)
        return t

    def init_channel(self, cid):
        with self._mu:
            self._table(cid)

    @staticmethod
    def _next(code, direction, effect):
        if effect == NO_EFFECT:
            return code
        cur = code or DEFAULT
        bit = POSS_P if direction == ADD else POSS_A
        if effect == APPLIED:
            return TOUCHED | bit | (cur & (FLOAT_ADD | FLOAT_REM))
        return TOUCHED | (cur & 15) | bit | (FLOAT_ADD if direction == ADD else FLOAT_REM)

    def apply(self, cid, u, action, effect):
        i = int(u[1:])
        with self._mu:
            t = self._table(cid)
            t[i] = self._next(t[i], action, effect)

    def remove_all(self, cid, effect):
        with self._mu:
            t = self._table(cid)
            for i in self.touched(cid):
                t[i] = self._next(t[i], REMOVE, effect)

    def touched(self, cid):
        t = self.keys.get(cid)
        if t is None:
            return []
        return [m.start() for m in _NONZERO.finditer(t)]

    def code(self, cid, u):
        t = self.keys.get(cid)
        if t is None or not (len(u) == 6 and u[0] == "u" and u[1:].isdigit()):
            return 0
        return t[int(u[1:])]

    def items(self, cid):
        """(uid, code) for every touched uid of the channel."""
        t = self.keys.get(cid)
        return [(uid(i), t[i]) for i in self.touched(cid)]

    def to_json(self):
        with self._mu:
            return {"version": 3, "encoding": "per channel zlib+base64 bytearray[100000] indexed by uid number; "
                                             "bit0=maybe_present bit1=maybe_absent bit2=floating_add "
                                             "bit3=floating_remove bit4=touched; 0=never touched (absent)",
                    "channels": {c: base64.b64encode(zlib.compress(bytes(t), 6)).decode()
                                 for c, t in sorted(self.keys.items())}}

    @classmethod
    def from_json(cls, d):
        if d.get("version") != 3:
            raise ValueError("world model version %r is not supported" % d.get("version"))
        m = cls()
        for c, blob in d["channels"].items():
            m.keys[c] = bytearray(zlib.decompress(base64.b64decode(blob)))
        return m


_NONZERO = re.compile(b"[^\\x00]")
