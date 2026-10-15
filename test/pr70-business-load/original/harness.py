#!/usr/bin/env python3
"""
Faithful-equivalent reproduction harness for XIN-1968.

Replays octo-server's imreconcile call pattern directly against a single-node
octo-im (WukongIM) built from PR#58, over the required 42-group / 378-channel
dataset. Not 王留超's original harness (unavailable in this workspace); this is
the smallest faithful equivalent permitted by the issue.

Managed-group semantics mirrored from octo-server/pkg/imreconcile:
  - group channel  : channel_type=2, id=repro_g<NN>
  - child thread    : channel_type=5, id=<group>____t<k>   (four-underscore cut)
  - every membership/flag mutation bumps a per-group business revision and
    delivers a FULL authoritative snapshot to POST /channel/subscriber_reconcile
    (not deltas), paged at memberPageSize=128 / channelPageSize=16, with headers
    token + Idempotency-Key, under a 5s foreground budget (client timeout 6s).
"""
import argparse, base64, json, random, threading, time, http.client, socket
from urllib.parse import urlparse
from collections import defaultdict, deque
socket_timeout = socket.timeout

def now(): return time.time()

_tl = threading.local()
def _conn(base):
    u = urlparse(base)
    key = (u.hostname, u.port or 80)
    c = getattr(_tl, "conns", None)
    if c is None:
        c = _tl.conns = {}
    if key not in c:
        c[key] = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=10)
    return c[key]

class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.by_kind = defaultdict(lambda: {"n":0,"fail":0,"slow":0,"timeout":0,"lat":deque()})
        self.first_timeout = None
        self.first_slow = None
        self.samples = deque(maxlen=400)   # (t, kind, dt, reason) for non-200 / timeouts
    def record(self, kind, dt, ok, timed_out, slow_thresh=1.0, reason=None):
        with self.lock:
            m = self.by_kind[kind]
            m["n"] += 1
            m["lat"].append(dt)
            if len(m["lat"]) > 200000: m["lat"].popleft()
            if not ok:
                m["fail"] += 1
                if reason is not None:
                    self.samples.append((round(now(),3), kind, round(dt,3), reason[:180]))
            if timed_out:
                m["timeout"] += 1
                if self.first_timeout is None: self.first_timeout = (now(), kind, dt)
            if dt >= slow_thresh:
                m["slow"] += 1
                if self.first_slow is None: self.first_slow = (now(), kind, dt)
    def snapshot(self):
        with self.lock:
            out = {}
            for k,m in self.by_kind.items():
                lat = sorted(m["lat"])
                def pct(p):
                    if not lat: return 0.0
                    i = min(len(lat)-1, int(p*len(lat)))
                    return lat[i]
                out[k] = {"n":m["n"],"fail":m["fail"],"slow":m["slow"],"timeout":m["timeout"],
                          "p50":round(pct(.5),4),"p95":round(pct(.95),4),"p99":round(pct(.99),4),
                          "max":round(lat[-1],4) if lat else 0.0}
            return out, self.first_timeout, self.first_slow, list(self.samples)

def _txt(b):
    try: return b.decode("utf-8","replace")
    except Exception: return str(b)

def http_post(url, obj, token, timeout, base=None):
    data = json.dumps(obj).encode()
    u = urlparse(url)
    path = u.path
    headers = {"Content-Type":"application/json",
               "Idempotency-Key": obj.get("operation_id", "x")}
    if token: headers["token"] = token
    t0 = now()
    conn = _conn(base or ("http://%s:%d" % (u.hostname, u.port or 80)))
    conn.timeout = timeout
    try:
        conn.request("POST", path, body=data, headers=headers)
        r = conn.getresponse()
        body = r.read()
        return now()-t0, (r.status==200), False, (b"HTTP %d " % r.status)+body[:200] if r.status!=200 else body[:200]
    except Exception as e:
        dt = now()-t0
        try: conn.close()
        except Exception: pass
        try: _tl.conns.pop((u.hostname, u.port or 80), None)
        except Exception: pass
        timed = isinstance(e, (TimeoutError, socket_timeout)) or "timed out" in str(e).lower() or dt>=timeout*0.95
        return dt, False, timed, str(e).encode()

class World:
    """Dataset + per-group revision + per-channel membership (mirrors SQL state)."""
    def __init__(self, ngroups, nchildren, parent_members, child_members):
        self.groups = []
        self.channels = []          # list of (chan_id, chan_type, group)
        self.members = {}           # chan_id -> list[str]
        self.rev = defaultdict(int) # group -> business revision
        self.revlock = threading.Lock()
        self.mlock = defaultdict(threading.Lock)
        for gi in range(1, ngroups+1):
            g = "repro_g%03d" % gi
            self.groups.append(g)
            self.channels.append((g,2,g))
            self.members[g] = ["u%05d" % u for u in range(parent_members)]
            for ci in range(1, nchildren+1):
                cid = "%s____t%d" % (g, ci)
                self.channels.append((cid,5,g))
                self.members[cid] = ["u%05d" % u for u in range(child_members)]
    def bump(self, group):
        with self.revlock:
            self.rev[group]+=1
            return self.rev[group]

def _sid():  # 32-byte digest as 64 hex chars (required for paged snapshots)
    return "".join("%02x" % random.randint(0,255) for _ in range(32))

def snapshots_for(cid, ctype, subs, rev, opid, ban=0,large=0,disband=0, page=128):
    subs = sorted(subs)
    n = len(subs)
    if n <= page:  # unpaged: page_count=0, snapshot_id/ranges empty
        return [{"channel_id":cid,"channel_type":ctype,"operation_id":opid,"revision":rev,
                 "snapshot_id":"","page_index":0,"page_count":0,"range_start":"","range_end":"",
                 "subscribers":subs,"denylist":[],"ban":ban,"large":large,"disband":disband}]
    pages = [subs[i:i+page] for i in range(0,n,page)]
    pc = len(pages); sid = _sid(); out=[]
    for i,chunk in enumerate(pages):
        rs = "" if i==0 else pages[i][0]
        re = "" if i==pc-1 else pages[i+1][0]
        out.append({"channel_id":cid,"channel_type":ctype,"operation_id":"%s-p%d"%(opid,i),
                    "revision":rev,"snapshot_id":sid,"page_index":i,"page_count":pc,
                    "range_start":rs,"range_end":re,
                    "subscribers":chunk,"denylist":[],"ban":ban,"large":large,"disband":disband})
    return out

STOP = threading.Event()

def reconcile_channel(base, token, cid, ctype, group, world, M, kind, budget, ban=0,large=0,disband=0):
    with world.mlock[cid]:
        subs = list(world.members[cid])
    rev = world.bump(group)
    opid = "op-%s-%d-%d" % (cid, rev, random.randint(0,1<<30))
    for snap in snapshots_for(cid,ctype,subs,rev,opid,ban,large,disband):
        dt,ok,timed,body = http_post(base+"/channel/subscriber_reconcile", snap, token, budget+1.0)
        M.record(kind, dt, ok, timed, reason=None if ok else _txt(body))
        if STOP.is_set(): break

def addremove_worker(base, token, world, M, budget, wid=0):
    r = random.Random(1000+wid)
    while not STOP.is_set():
        cid,ctype,group = r.choice(world.channels)
        with world.mlock[cid]:
            mem = world.members[cid]
            if r.random()<0.5:
                u="u%05d"%r.randint(0,99999)
                if u not in mem: mem.append(u)
            elif mem:
                mem.pop(r.randrange(len(mem)))
        reconcile_channel(base,token,cid,ctype,group,world,M,"subscriber_reconcile",budget)

def send_worker(base, token, world, M, budget, wid=0):
    r = random.Random(2000+wid)
    payload = base64.b64encode(b"repro-load-message").decode()
    while not STOP.is_set():
        cid,ctype,group = r.choice(world.channels)
        mem = world.members[cid]
        frm = r.choice(mem) if mem else "u00000"
        body={"header":{"no_persist":0,"red_dot":1,"sync_once":0},"from_uid":frm,
              "channel_id":cid,"channel_type":ctype,"payload":payload}
        dt,ok,timed,rb = http_post(base+"/message/send", body, token, budget+1.0)
        M.record("message_send", dt, ok, timed, reason=None if ok else _txt(rb))

def upsert_worker(base, token, world, M, budget, wid=0):
    r = random.Random(3000+wid)
    while not STOP.is_set():
        cid,ctype,group = r.choice(world.channels)
        large = r.randint(0,1)
        reconcile_channel(base,token,cid,ctype,group,world,M,"channel_upsert",budget,large=large)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--base",default="http://127.0.0.1:5001")
    ap.add_argument("--token",default="")
    ap.add_argument("--seconds",type=int,default=3600)
    ap.add_argument("--groups",type=int,default=42)
    ap.add_argument("--children",type=int,default=8)
    ap.add_argument("--parent-members",type=int,default=150)
    ap.add_argument("--child-members",type=int,default=20)
    ap.add_argument("--addremove",type=int,default=42)
    ap.add_argument("--send",type=int,default=42)
    ap.add_argument("--upsert",type=int,default=16)
    ap.add_argument("--budget",type=float,default=5.0)
    ap.add_argument("--report",default="/tmp/repro_run.jsonl")
    ap.add_argument("--seed-only",action="store_true")
    a=ap.parse_args()
    random.seed(42)
    world=World(a.groups,a.children,a.parent_members,a.child_members)
    M=Metrics()
    base=a.base; token=a.token
    print("SEED: %d groups, %d children each, %d channels" % (a.groups,a.children,len(world.channels)),flush=True)
    # ---- seed: create every channel with initial authoritative snapshot ----
    seedM=Metrics(); t0=now(); seeded=0
    for cid,ctype,group in world.channels:
        # create/update channel then deliver first revision snapshot (authority opt-in)
        subs=list(world.members[cid])
        rev=world.bump(group)
        opid="seed-%s-%d"%(cid,rev)
        for snap in snapshots_for(cid,ctype,subs,rev,opid):
            dt,ok,timed,body=http_post(base+"/channel/subscriber_reconcile",snap,token,a.budget+2.0)
            seedM.record("seed",dt,ok,timed)
            if not ok and seeded<5: print("SEED-ERR",cid,body[:200],flush=True)
        seeded+=1
    ss,_,_,_=seedM.snapshot()
    print("SEED done in %.1fs: %s"%(now()-t0, json.dumps(ss)),flush=True)
    if a.seed_only: return
    # ---- workload ----
    threads=[]
    for i in range(a.addremove): threads.append(threading.Thread(target=addremove_worker,args=(base,token,world,M,a.budget,i),daemon=True))
    for i in range(a.send):      threads.append(threading.Thread(target=send_worker,args=(base,token,world,M,a.budget,i),daemon=True))
    for i in range(a.upsert):    threads.append(threading.Thread(target=upsert_worker,args=(base,token,world,M,a.budget,i),daemon=True))
    print("START workload: %d add/remove + %d send + %d upsert = %d workers, %ds"%(a.addremove,a.send,a.upsert,len(threads),a.seconds),flush=True)
    start=now()
    for t in threads: t.start()
    rep=open(a.report,"w")
    last=None
    while now()-start < a.seconds and not STOP.is_set():
        time.sleep(15)
        snap,ft,fs,samples=M.snapshot()
        line={"t":round(now(),3),"elapsed":round(now()-start,1),"kinds":snap,
              "first_timeout":ft,"first_slow":fs,"fail_samples":samples[-20:]}
        rep.write(json.dumps(line)+"\n"); rep.flush()
        tot=sum(v["n"] for v in snap.values()); to=sum(v["timeout"] for v in snap.values()); fl=sum(v["fail"] for v in snap.values())
        d=tot-(last or 0); last=tot
        print("[%4ds] req=%d (+%d, %.0f/s) fail=%d timeout=%d ft=%s"%(now()-start,tot,d,d/15.0,fl,to,ft),flush=True)
    STOP.set()
    time.sleep(2)
    snap,ft,fs,samples=M.snapshot()
    final={"final":True,"elapsed":round(now()-start,1),"kinds":snap,"first_timeout":ft,"first_slow":fs,"fail_samples":samples}
    rep.write(json.dumps(final)+"\n"); rep.close()
    print("FINAL: %s"%json.dumps(final),flush=True)

if __name__=="__main__": main()
