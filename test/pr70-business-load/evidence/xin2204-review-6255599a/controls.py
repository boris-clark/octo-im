"""Reviewer-authored independent controls. Imports the clean export of 6255599a read-only."""
import contextlib, io, json, os, shutil, socket, sys, tempfile
ROOT = sys.argv[1]
sys.path.insert(0, ROOT)
import harness, oracle  # noqa
from mock.mock_wukong import MockCluster  # noqa
from wkload import world  # noqa

TOKEN = "rv-secret-1234"
SMALL = ["--groups", "2", "--children", "2"]

def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p

def env(nodes, faults):
    tmp = tempfile.mkdtemp(prefix="rv-")
    tok = os.path.join(tmp, "t"); open(tok, "w").write(TOKEN); os.chmod(tok, 0o600)
    return tmp, tok, os.path.join(tmp, "st"), MockCluster(nodes, TOKEN, faults)

def H(c, tok, st, *x):
    with contextlib.redirect_stdout(io.StringIO()):
        return harness.main(["--api", c.urls("api"), "--token-file", tok, "--state-dir", st] + SMALL + list(x))

def O(mode, api, mgr, tok, st, *x):
    a = oracle.parse_args([mode, "--api", api, "--manager", mgr, "--token-file", tok, "--state-dir", st,
                           "--drain-timeout", "3", "--drain-interval", "0.2"] + SMALL + list(x))
    with contextlib.redirect_stdout(io.StringIO()):
        rc = oracle.run(a)
    return rc, json.load(open(os.path.join(st, "oracle_%s.json" % mode)))

res = {}
# X1: node 1 diverges AND node 1 manager is unreachable -> what verdict?
tmp, tok, st, c = env(3, {"diverge_node": 1})
assert H(c, tok, st, "--phase", "seed") == 0
mg = c.urls("manager").split(",")
rc, rep = O("seedcheck", c.urls("api"), mg[0] + "," + mg[2], tok, st)
res["X0_diverged_node_omitted_from_manager_list"] = (rc, rep["verdict"], rep["fail"], rep["read_errors"])
mg[1] = "http://127.0.0.1:%d" % free_port()
rc, rep = O("seedcheck", c.urls("api"), ",".join(mg), tok, st)
res["X1_diverged_node_unreachable"] = (rc, rep["verdict"], rep["fail"], rep["read_errors"],
                                       {k: v for k, v in rep["info"].items() if "node_error" in k})
c.stop(); shutil.rmtree(tmp)

# X2: confirmed seed uid, then a rejected add (no effect) on it, then server loses it -> verdict?
tmp, tok, st, c = env(1, {})
assert H(c, tok, st, "--phase", "seed") == 0
wp = os.path.join(st, "world_seed.json"); w = json.load(open(wp))
cid = "repro_g001"
m = world.SubscriberModel.from_json(w["model"])
m.apply(cid, "u00001", world.ADD, "rejected")       # a rejected ADD cannot remove a present uid
w["model"] = m.to_json(); json.dump(w, open(wp, "w"))
with c.state.mu:
    c.state.subs[(cid, 2)].discard("u00001")          # product loses the uid
rc, rep = O("seedcheck", c.urls("api"), c.urls("manager"), tok, st)
res["X2_loss_masked_by_rejected_add"] = (rc, rep["verdict"], rep["fail"], rep["info"].get("uncertain_uid_absent"))
c.stop(); shutil.rmtree(tmp)

# X3: messagesync without "more" field (pagination contract mismatch) -> verdict?
tmp, tok, st, c = env(1, {})
assert H(c, tok, st, "--phase", "seed") == 0
assert H(c, tok, st, "--phase", "load", "--addremove", "1", "--send", "2", "--upsert", "0", "--seconds", "1") == 0
oracle.PAGE = 2
import mock.mock_wukong as mw
orig = mw.make_handler
rc, rep = O("post", c.urls("api"), c.urls("manager"), tok, st)
res["X3a_page2_with_more"] = (rc, rep["verdict"], rep["fail"])
c.stop(); shutil.rmtree(tmp)
print(json.dumps(res, indent=1, default=str))
