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
import random
import threading

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
    """
    r = worker_rng(seed, "addremove", wid)
    added = {}
    while True:
        cid, ctype, _g = r.choice(channels)
        if r.random() < 0.5:
            u = uid(r.randrange(UID_SPACE))
            added.setdefault(cid, []).append(u)
            yield cid, ctype, ADD, u
        else:
            pool = seed_members(ctype, parent_members, child_members) + added.get(cid, [])
            yield cid, ctype, REMOVE, r.choice(pool)


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


class SubscriberModel:
    """Recorded world model of channel -> subscriber set.

    confirmed[cid]  uids whose last harness write was acknowledged (ok) as add.
    uncertain[cid]  uids with at least one write whose effect is not known
                    (rejected or unknown outcome). Sticky: a later ok write cannot
                    clear it because the earlier request may still apply late.
    Writes to the same (channel, uid) are serialised by the harness, so the
    model is exact for every uid that never had an unknown/rejected write.
    """

    STRIPES = 4096

    def __init__(self):
        self.confirmed = {}
        self.uncertain = {}
        self._mu = threading.Lock()
        self._stripes = [threading.Lock() for _ in range(self.STRIPES)]

    def key_lock(self, cid, u):
        return self._stripes[hash((cid, u)) % self.STRIPES]

    def init_channel(self, cid):
        with self._mu:
            self.confirmed.setdefault(cid, set())
            self.uncertain.setdefault(cid, set())

    def apply(self, cid, u, action, outcome):
        with self._mu:
            conf = self.confirmed.setdefault(cid, set())
            unc = self.uncertain.setdefault(cid, set())
            if outcome == OK:
                if action == ADD:
                    conf.add(u)
                else:
                    conf.discard(u)
            elif outcome == NOT_SENT:
                pass
            else:
                conf.discard(u)
                unc.add(u)

    def remove_all(self, cid, outcome):
        with self._mu:
            if outcome == OK:
                self.confirmed[cid] = set()
                self.uncertain[cid] = set()
            elif outcome != NOT_SENT:
                self.uncertain.setdefault(cid, set()).update(self.confirmed.get(cid, set()))
                self.confirmed[cid] = set()

    def to_json(self):
        with self._mu:
            return {
                "confirmed": {c: sorted(s) for c, s in self.confirmed.items()},
                "uncertain": {c: sorted(s) for c, s in self.uncertain.items()},
            }

    @classmethod
    def from_json(cls, d):
        m = cls()
        for c, s in d["confirmed"].items():
            m.confirmed[c] = set(s)
        for c, s in d["uncertain"].items():
            m.uncertain[c] = set(s)
        return m
