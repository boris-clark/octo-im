"""Self-tests and deterministic red/green negative controls.

Disposable local mocks only (mock/mock_wukong.py on 127.0.0.1, ephemeral ports).
No deployed service is contacted. Run:  python3 -m unittest discover -s tests -v
"""
import contextlib
import glob
import gzip
import io
import itertools
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import analyze  # noqa: E402
import harness  # noqa: E402
import monitor_run  # noqa: E402
import oracle  # noqa: E402
import poller  # noqa: E402
from mock.mock_wukong import MockCluster  # noqa: E402
from wkload import common, world  # noqa: E402

TOKEN = "selftest-secret-7f3a9c"
# Reduced scale for unit speed: 2 groups x (1 + 2 children) = 6 channels.
# Parent members stay 150 so the 128-member seed paging is exercised.
SMALL = ["--groups", "2", "--children", "2"]
SMALL_LOAD = ["--addremove", "3", "--send", "3", "--upsert", "1", "--seconds", "1.5", "--report-interval", "0.5"]


def _read(p, mode="r"):
    with open(p, mode) as f:
        return f.read()


def _json(p):
    return json.loads(_read(p))


def quiet(fn, *a):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a)


class Base(unittest.TestCase):
    nodes = 1
    faults = {}

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pr70bl-")
        self.tok = os.path.join(self.tmp, "token")
        with open(self.tok, "w") as f:
            f.write(TOKEN)
        os.chmod(self.tok, 0o600)
        self.st = os.path.join(self.tmp, "state")
        self.cluster = MockCluster(self.nodes, TOKEN, dict(self.faults))

    def tearDown(self):
        self.cluster.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def h(self, *extra, budget="5"):
        argv = ["--api", self.cluster.urls("api"), "--token-file", self.tok, "--state-dir", self.st,
                "--budget", budget] + SMALL + list(extra)
        return quiet(harness.main, argv)

    def o(self, mode, *extra, api=None, manager=None):
        argv = [mode, "--api", api or self.cluster.urls("api"), "--manager", manager or self.cluster.urls("manager"),
                "--token-file", self.tok, "--state-dir", self.st, "--drain-timeout", "3",
                "--drain-interval", "0.2"] + SMALL + list(extra)
        rc = quiet(oracle.run, oracle.parse_args(argv))
        name = mode if mode != "restart" else "restart" + (extra[extra.index("--restart-index") + 1]
                                                          if "--restart-index" in extra else "1")
        with open(os.path.join(self.st, "oracle_%s.json" % name)) as f:
            return rc, json.load(f)

    def full_green(self, budget="5"):
        rc, rep = self.o("preflight")
        self.assertEqual((rc, rep["verdict"]), (0, "PASS"), rep)
        self.assertEqual(self.h("--phase", "seed", budget=budget), 0)
        rc, rep = self.o("seedcheck")
        self.assertEqual(rep["verdict"], "PASS", rep)
        self.assertEqual(self.h("--phase", "load", *SMALL_LOAD, budget=budget), 0)
        return self.o("post")


class TestDefaultsAndProvenance(unittest.TestCase):
    def test_defaults_preserve_historical_shape(self):
        a = harness.parse_args(["--api", "http://127.0.0.1:1", "--state-dir", "/nonexistent"])
        self.assertEqual((a.groups, a.children, a.parent_members, a.child_members), (42, 8, 150, 20))
        self.assertEqual((a.addremove, a.send, a.upsert, a.seconds, a.budget, a.seed), (42, 42, 16, 3600, 5.0, 42))
        self.assertEqual(len(world.build_channels(a.groups, a.children)), 378)
        chans = world.build_channels(42, 8)
        self.assertEqual(chans[0], ("repro_g001", 2, "repro_g001"))
        self.assertEqual(chans[1], ("repro_g001____t1", 5, "repro_g001"))
        self.assertEqual(chans[-1], ("repro_g042____t8", 5, "repro_g042"))

    def test_no_token_argv_option(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                harness.parse_args(["--api", "http://127.0.0.1:1", "--state-dir", "x", "--token", "t"])

    def test_originals_are_verbatim(self):
        sums = _read(os.path.join(ROOT, "original", "SHA256SUMS")).split("\n")
        expected = {"harness.py": "3d3a7fce5ab07e89c5a08e2c9775861c07e6c4b3189d4baf0c4ef7ad302cc47d",
                    "postcheck.py": "219696f66e1207994e445dd28f43f7685bc0621283b66a26b3de7eae13ffdcaa",
                    "poller.py": "cd739d912f1df49b887b6b6c0e31eb1de1803ecd43b0bb1cb548dcfb5479ea0c",
                    "monitor_run.py": "b3139d55e01f13d250bd4d11b18507e22f24627d28a00e5fabeb896c4870d6aa",
                    "analyze.py": "dcafbaef1a9e4d18be1317d269366ca537b0a4fd23186f1666b098cc59914f2e"}
        for name, h in expected.items():
            self.assertEqual(common.file_sha256(os.path.join(ROOT, "original", name)), h, name)
            self.assertIn("%s  %s" % (h, name), sums)

    def test_adapted_tools_never_call_removed_routes(self):
        for p in ["harness.py", "oracle.py", "poller.py", "monitor_run.py", "analyze.py"] + glob.glob(
                os.path.join(ROOT, "wkload", "*.py")):
            src = _read(os.path.join(ROOT, p))
            self.assertIsNone(re.search(r"[\"']/channel/subscriber_(reconcile|recovery)", src), p)


class TestDeterminism(unittest.TestCase):
    def take(self, gen, n=2000):
        return list(itertools.islice(gen, n))

    def test_streams_identical_for_same_seed(self):
        ch = world.build_channels(42, 8)
        for wid in (0, 7, 41):
            self.assertEqual(self.take(world.addremove_stream(42, wid, ch, 150, 20)),
                             self.take(world.addremove_stream(42, wid, ch, 150, 20)))
            self.assertEqual(self.take(world.send_stream(42, wid, ch, 150, 20)),
                             self.take(world.send_stream(42, wid, ch, 150, 20)))
        self.assertEqual(self.take(world.upsert_stream(42, 3, ch)), self.take(world.upsert_stream(42, 3, ch)))

    def test_streams_differ_by_seed_and_worker(self):
        ch = world.build_channels(42, 8)
        self.assertNotEqual(self.take(world.addremove_stream(42, 0, ch, 150, 20)),
                            self.take(world.addremove_stream(43, 0, ch, 150, 20)))
        self.assertNotEqual(self.take(world.send_stream(42, 0, ch, 150, 20)),
                            self.take(world.send_stream(42, 1, ch, 150, 20)))

    def test_stream_stable_across_processes(self):
        code = ("import sys,itertools,json;sys.path.insert(0,%r);from wkload import world;"
                "ch=world.build_channels(42,8);"
                "print(json.dumps(list(itertools.islice(world.addremove_stream(42,5,ch,150,20),300))))") % ROOT
        outs = {subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True,
                               env={"PYTHONHASHSEED": s}).stdout for s in ("1", "2")}
        self.assertEqual(len(outs), 1)

    def test_addremove_stream_identical_to_6255599a(self):
        """The compact remove pool consumes the RNG exactly like the reviewed implementation."""
        def v1(seed, wid, channels, pm, cm):
            r = world.worker_rng(seed, "addremove", wid)
            added = {}
            while True:
                cid, ctype, _g = r.choice(channels)
                if r.random() < 0.5:
                    u = world.uid(r.randrange(world.UID_SPACE))
                    added.setdefault(cid, []).append(u)
                    yield cid, ctype, world.ADD, u
                else:
                    pool = world.seed_members(ctype, pm, cm) + added.get(cid, [])
                    yield cid, ctype, world.REMOVE, r.choice(pool)
        ch = world.build_channels(42, 8)
        for wid in (0, 13, 41):
            self.assertEqual(self.take(world.addremove_stream(42, wid, ch, 150, 20), 20000),
                             self.take(v1(42, wid, ch, 150, 20), 20000))
        small = world.build_channels(1, 1)  # dense: most removes hit previously added uids
        self.assertEqual(self.take(world.addremove_stream(7, 3, small, 150, 20), 20000),
                         self.take(v1(7, 3, small, 150, 20), 20000))

    def test_no_shared_global_rng(self):
        pat = re.compile(r"\brandom\.(random|randint|choice|randrange|seed|shuffle|sample)\(")
        for p in ["harness.py", "oracle.py"] + glob.glob(os.path.join(ROOT, "wkload", "*.py")):
            self.assertIsNone(pat.search(_read(os.path.join(ROOT, p))), p)


class TestToken(Base):
    def test_rejects_group_readable_token(self):
        os.chmod(self.tok, 0o644)
        with self.assertRaises(common.TokenError):
            common.read_token_file(self.tok)

    def test_token_never_written(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["verdict"], "PASS")
        for p in glob.glob(os.path.join(self.st, "*")):
            data = gzip.decompress(_read(p, "rb")) if p.endswith(".gz") else _read(p, "rb")
            self.assertNotIn(TOKEN.encode(), data, p)
        # subprocess stdout/stderr of a real CLI run
        r = subprocess.run([sys.executable, "-I", os.path.join(ROOT, "harness.py"), "--phase", "seed", "--api",
                            self.cluster.urls("api"), "--token-file", self.tok, "--state-dir",
                            os.path.join(self.tmp, "cli")] + SMALL, capture_output=True, text=True, timeout=120)
        self.assertNotIn(TOKEN, r.stdout + r.stderr)

    def test_wrong_token_aborts_before_load(self):
        with open(self.tok, "w") as f:
            f.write("wrong-token")
        self.assertEqual(self.h("--phase", "all", *SMALL_LOAD), 2)
        self.assertFalse(os.path.exists(os.path.join(self.st, "load_journal.jsonl.gz")))
        seed = _json(os.path.join(self.st, "world_seed.json"))
        self.assertEqual(seed["metrics"]["seed"]["outcome"]["rejected"], seed["metrics"]["seed"]["n"])
        self.assertEqual(seed["metrics"]["seed"]["err"], {"http_401": seed["metrics"]["seed"]["n"]})


class TestGreenSingle(Base):
    def test_full_sequence_with_two_restarts(self):
        rc, rep = self.full_green()
        self.assertEqual((rc, rep["verdict"]), (0, "PASS"), rep)
        self.assertGreater(rep["info"]["messages_accepted_in_ledger_post"], 0)
        self.assertTrue(rep["drain"]["drained"])
        self.cluster = self.cluster.graceful_restart()
        rc, rep = self.o("restart", "--restart-index", "1")
        self.assertEqual(rep["verdict"], "PASS", rep)
        self.cluster = self.cluster.graceful_restart()
        rc, rep = self.o("restart", "--restart-index", "2")
        self.assertEqual(rep["verdict"], "PASS", rep)
        rc, rep = self.o("teardown")
        self.assertEqual(rep["verdict"], "PASS", rep)
        self.assertEqual(rep["teardown_outcomes"], {"ok": 6})

    def test_accounting_is_complete(self):
        self.full_green()
        final = _json(os.path.join(self.st, "world_final.json"))["final"]
        recs = list(common.read_journal(os.path.join(self.st, "load_journal.jsonl.gz")))
        self.assertEqual(final["journal_records"], len(recs))
        for kind, v in final["kinds"].items():
            self.assertEqual(v["n"], sum(v["outcome"].values()))
            self.assertEqual(v["n"], sum(1 for r in recs if r["kind"] == kind))
        self.assertEqual(final["stuck_workers"], [])

    def test_seed_pages_at_128(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        recs = list(common.read_journal(os.path.join(self.st, "seed_journal.jsonl.gz")))
        parent = [r for r in recs if r["ch"] == "repro_g001"]
        self.assertEqual([r["n"] for r in parent], [128, 22])
        self.assertEqual(len(recs), 2 * 2 + 4)


class TestGreenThreeNode(Base):
    nodes = 3

    def test_three_endpoints_and_full_sequence(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["verdict"], "PASS", rep)
        eps = {r["ep"] for r in common.read_journal(os.path.join(self.st, "load_journal.jsonl.gz"))}
        self.assertEqual(eps, set(self.cluster.urls("api").split(",")))
        self.cluster = self.cluster.graceful_restart()
        self.assertEqual(self.o("restart")[1]["verdict"], "PASS")
        self.assertEqual(self.o("teardown")[1]["verdict"], "PASS")


class TestObservabilityTools(Base):
    nodes = 3

    def test_poller_monitor_analyze(self):
        self.full_green()
        out = os.path.join(self.tmp, "metrics.jsonl")
        self.assertEqual(quiet(poller.main, ["--manager", self.cluster.urls("manager"), "--token-file", self.tok,
                                             "--interval", "0.2", "--seconds", "0.7", "--out", out]), 0)
        recs = [json.loads(x) for x in _read(out).splitlines()]
        self.assertGreaterEqual(len(recs), 2)
        self.assertEqual(len(recs[0]["nodes"]), 3)
        self.assertIn("cluster_channel_propose_count_total", list(recs[0]["nodes"].values())[0]["metrics"])
        self.assertEqual(quiet(monitor_run.main, ["--state-dir", self.st, "--max-seconds", "5", "--poll", "0.1"]), 0)
        log = os.path.join(self.tmp, "wk.log")
        with open(log, "w") as f:
            f.write("x propose batch until applied timeout\ny propose batch until applied timeout\n")
        js = os.path.join(self.tmp, "summary.json")
        self.assertEqual(quiet(analyze.main, ["--state-dir", self.st, "--metrics", out, "--log", log, "--json-out", js]), 0)
        summ = _json(js)
        self.assertEqual(summ["logs"][log]["propose batch until applied timeout"], 2)
        self.assertEqual(summ["oracle"]["oracle_post.json"]["verdict"], "PASS")
        self.assertEqual(len(summ["metric_deltas"]), 3)
        for p in (out, js):
            self.assertNotIn(TOKEN, _read(p))

    def test_monitor_is_bounded(self):
        os.makedirs(self.st)
        self.assertEqual(quiet(monitor_run.main, ["--state-dir", self.st, "--max-seconds", "0.3", "--poll", "0.1"]), 2)


# ---------------- red controls: each must FAIL with the specific signal ----------------

class TestRedStaleState(Base):
    faults = {"stale_channel": "repro_g001"}

    def test(self):
        rc, rep = self.o("preflight")
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"))
        self.assertEqual(rep["fail"], {"stale_state_subscribers": 1})


class TestRedMissingSubscriber(Base):
    faults = {"drop_add_uid": "u00005"}

    def test(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        rc, rep = self.o("seedcheck")
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertEqual(rep["fail"], {"missing_subscriber": 6})


class TestRedUnexpectedMember(Base):
    faults = {"phantom_uid": "zz_phantom"}

    def test(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        rc, rep = self.o("seedcheck")
        self.assertEqual(rep["fail"], {"unexpected_subscriber": 6})


class TestRedDuplicateSubscriber(Base):
    faults = {"dup_subscriber_read": True}

    def test(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        rc, rep = self.o("seedcheck")
        self.assertEqual(rep["fail"], {"duplicate_subscriber": 6})


# ---------------- B1: topology coverage, fail-closed node reads, forwarding limits ----------------

def free_port():
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class TestB1OmittedManagerIsInconclusive(Base):
    """Reviewer X0: diverged node left out of --manager used to PASS."""
    nodes = 3
    faults = {"diverge_node": 1, "leader_override": 0}

    def test(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        mg = self.cluster.urls("manager").split(",")
        rc, rep = self.o("seedcheck", manager=mg[0] + "," + mg[2])
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))
        self.assertIn("endpoint_mapping_incomplete", rep["read_errors"])

    def test_node_omitted_from_both_lists(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        ap, mg = self.cluster.urls("api").split(","), self.cluster.urls("manager").split(",")
        rc, rep = self.o("seedcheck", api=ap[0] + "," + ap[2], manager=mg[0] + "," + mg[2])
        self.assertEqual(rep["verdict"], "INCONCLUSIVE")
        self.assertIn("topology_incomplete", rep["read_errors"])

    def test_swapped_manager_order(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        mg = self.cluster.urls("manager").split(",")
        rc, rep = self.o("seedcheck", manager=",".join([mg[1], mg[0], mg[2]]))
        self.assertEqual(rep["verdict"], "INCONCLUSIVE")
        self.assertIn("endpoint_mapping_mismatch", rep["read_errors"])


class TestB1UnreachableNodeIsInconclusive(Base):
    """Reviewer X1: diverged node whose manager is unreachable used to PASS (info only)."""
    nodes = 3
    faults = {"diverge_node": 1, "leader_override": 0}

    def test_unreachable_manager(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        mg = self.cluster.urls("manager").split(",")
        mg[1] = "http://127.0.0.1:%d" % free_port()
        rc, rep = self.o("seedcheck", manager=",".join(mg))
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))
        self.assertEqual(rep["read_errors"]["subscriber_read_node_error"], 6)

    def test_node_down(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        self.cluster.stop_node(1)
        ap = ",".join(self.cluster.state.api_urls)
        mg = self.cluster.urls("manager").split(",")
        mg.insert(1, "http://127.0.0.1:%d" % free_port())
        rc, rep = self.o("seedcheck", api=ap, manager=",".join(mg))
        # a fully stopped node also fails /health, which is a FAIL; never PASS
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"))
        self.assertEqual(rep["fail"], {"unhealthy_endpoint": 1})
        self.assertIn("node_identity", rep["read_errors"])
        self.assertIn("subscriber_read_node_error", rep["read_errors"])


class TestB1LeaderDivergenceFails(Base):
    """A node that leads channels and holds wrong state is caught through every route."""
    nodes = 3
    faults = {"diverge_node": 1}

    def test(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        rc, rep = self.o("seedcheck")
        self.assertEqual(rep["verdict"], "FAIL")
        led = rep["leaders"]["slot_leader_counts"].get("2", 0)
        self.assertGreater(led, 0)
        self.assertEqual(rep["fail"], {"missing_subscriber": led})
        self.assertEqual(sum(rep["leaders"]["slot_leader_counts"].values()), 6)


class TestB1ReplicaLocalDivergenceIsAnObservationLimit(Base):
    """Follower-only divergence is invisible through PR70 public reads: PASS, and the
    report says so instead of claiming replica verification."""
    nodes = 3
    faults = {"diverge_node": 1, "leader_override": 0}

    def test(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        rc, rep = self.o("seedcheck")
        self.assertEqual(rep["verdict"], "PASS", rep)
        self.assertEqual(rep["leaders"]["slot_leader_counts"], {"1": 6})
        self.assertTrue(any("not observable" in x for x in rep["observation_limits"]))


class TestB1StaleRouteMismatchFails(Base):
    """A node that answers locally instead of forwarding is caught as a forwarding mismatch."""
    nodes = 3
    faults = {"diverge_node": 1, "leader_override": 0, "stale_route_node": 1}

    def test(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        rc, rep = self.o("seedcheck")
        self.assertEqual(rep["fail"], {"forwarded_read_mismatch": 6})


class TestB1LeadersRecordedAcrossRestart(Base):
    nodes = 3

    def test(self):
        self.full_green()
        self.cluster.state.faults["leader_override"] = 2  # every slot moves to node 3 across the restart
        self.cluster = self.cluster.graceful_restart()
        rc, rep = self.o("restart")
        self.assertEqual(rep["verdict"], "PASS", rep)
        self.assertEqual(rep["leaders"]["previous_label"], "post")
        self.assertEqual(rep["leaders"]["slot_leader_counts"], {"3": 6})
        moved = sum(1 for c in _json(os.path.join(self.st, "leaders_post.json"))["leaders"].values()
                    if c["slot_leader"] != 3)
        self.assertEqual(rep["leaders"]["slot_leader_changed_vs_previous"], moved)
        self.assertTrue(os.path.exists(os.path.join(self.st, "leaders_restart1.json")))


class TestMockForwardsLikeProduct(Base):
    nodes = 3

    def test(self):
        from wkload.common import Client
        c = Client(TOKEN, 5)
        st = self.cluster.state
        apis = st.api_urls
        cid = next(x for x in ("a%d" % i for i in range(100)) if st.leader(x) == 2)
        r = c.call("POST", apis[0], "/channel/subscriber_add", {"channel_id": cid, "channel_type": 2, "subscribers": ["u1"]})
        self.assertEqual(r.outcome, "ok")
        self.assertEqual(st.subs[(cid, 2)], {"u1"})
        r = c.call("GET", apis[0], "/cluster/channels/%s/2/config" % cid)
        self.assertEqual((r.status, r.json()["msg"]), (400, "slot leader is not current node"))
        r = c.call("GET", apis[1], "/cluster/channels/%s/2/subscribers" % cid)
        self.assertEqual(r.json(), ["u1"])
        r = c.call("POST", apis[0], "/channel/info", {"channel_id": cid, "channel_type": 2, "large": 1})
        self.assertEqual(st.info[(cid, 2)], {"large": True, "ban": False, "disband": False, "send_ban": False,
                                            "allow_stranger": False})
        c.close_all()


class TestRedMissingMessage(Base):
    faults = {"drop_message_n": 3}

    def test(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertEqual(rep["fail"], {"missing_accepted_message": 1})
        self.assertFalse(rep["drain"]["drained"])


class TestRedDuplicateMessage(Base):
    faults = {"dup_message_n": 3}

    def test(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["fail"], {"duplicate_message": 1, "duplicate_message_id": 1})


class TestRedSeqGap(Base):
    faults = {"seq_gap_n": 3}

    def test(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["fail"], {"seq_gap": 1})


class TestRedSeqGapMidChannel(Base):
    """Gap after a channel's first message: exercises the contiguity check, not the first-seq check."""
    faults = {"seq_gap_mid_channel": True}

    def test(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["fail"], {"seq_gap": 1})
        self.assertIn("prev", rep["examples"]["seq_gap"][0])


class TestRedRejectedButPersisted(Base):
    faults = {"store_then_500_send_n": 4}

    def test(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["fail"], {"rejected_but_persisted": 1})


class TestRedUnexpectedMessage(Base):
    def test(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["verdict"], "PASS")
        st = self.cluster.state
        with st.mu:
            key = ("repro_g002", 2)
            s = st.seq.get(key, 0) + 1
            st.seq[key] = s
            st.msgs.setdefault(key, []).append({"message_seq": s, "client_msg_no": "foreign-1", "message_id": 1,
                                                "from_uid": "x", "channel_id": key[0], "channel_type": 2,
                                                "payload": "eA==", "visible_at": 0})
        self.cluster = self.cluster.graceful_restart()
        rc, rep = self.o("restart")
        self.assertEqual(rep["fail"], {"new_message_after_restart": 1, "unexpected_message": 1})


class TestRedRestartLoss(Base):
    faults = {"lose_on_restart": 1}

    def test(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["verdict"], "PASS", rep)
        self.cluster = self.cluster.graceful_restart()
        rc, rep = self.o("restart")
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertEqual(rep["fail"]["restart_readback_changed"], 6)
        self.assertIn("missing_accepted_message", rep["fail"])


class TestRedUnhealthy(Base):
    faults = {"unhealthy": True}

    def test(self):
        rc, rep = self.o("preflight")
        self.assertEqual(rep["fail"], {"unhealthy_endpoint": 1})


class TestReadFailureIsInconclusiveNotPass(Base):
    faults = {"messagesync_500": True}

    def test(self):
        rc, rep = self.o("preflight")
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))
        self.assertEqual(rep["read_errors"], {"messagesync": 6})
        self.assertEqual(self.h("--phase", "seed"), 0)
        self.assertEqual(self.h("--phase", "load", *SMALL_LOAD), 0)
        rc, rep = self.o("post")
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))
        self.assertEqual(rep["read_errors"], {"messagesync": 6})


class TestN3StaleMessagesWithUnloadedConfig(Base):
    """max_message_seq answers 0 while the channel config is not loaded; readback still finds it."""
    faults = {"stale_message_channel": "repro_g002"}

    def test(self):
        rc, rep = self.o("preflight")
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertEqual(rep["fail"]["stale_state_messages"], 2)  # readback, then max seq once the config is loaded
        self.assertEqual(rep["examples"]["stale_state_messages"][0], {"ch": "repro_g002", "messages": 1})


# ---------------- client failures: outcomes retained and accounted, no false verdict ----------------

class TestClientTimeoutsAccounted(Base):
    faults = {"slow_every": 9, "slow_secs": 2.0}

    def test(self):
        # load client timeout = budget + 1 = 1.2s < 2.0s server delay; seed timeout = budget + 2 = 2.2s
        rc, rep = self.full_green(budget="0.2")
        final = _json(os.path.join(self.st, "world_final.json"))["final"]
        unknown = sum(v["outcome"]["unknown"] for v in final["kinds"].values())
        self.assertGreater(unknown, 0)
        # broad ambiguity cannot masquerade as full consistency
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"), rep)
        self.assertEqual(rep["fail"], {})
        self.assertEqual(rep["scope_errors"], {"ambiguous_ratio_above_threshold": 1})
        cov = rep["subscriber_coverage"]["post"]
        self.assertGreater(cov["ambiguous_ratio"], 0.01)
        # Fixture-only widened model scope accepts exactly the possible states.
        # Do not use teardown for this assertion: late load adds are a teardown
        # defect even when their model state remains ambiguous (B3).
        a = oracle.parse_args(["post", "--api", self.cluster.urls("api"),
                               "--manager", self.cluster.urls("manager"), "--token-file", self.tok,
                               "--state-dir", self.st, "--max-ambiguous-ratio", "1"] + SMALL)
        o = oracle.Oracle(a)
        try:
            o.check_subscribers(oracle.model_from(self.st, "world_final.json"), "post")
            self.assertEqual((o.verdict(), dict(o.fail)), ("PASS", {}))
        finally:
            o.client.close_all()


class TestN4WholeRequestDeadline(Base):
    faults = {"trickle_secs": 3.0}

    def test(self):
        from wkload.common import Client
        c = Client(TOKEN, 1.0)
        r = c.call("POST", self.cluster.urls("api"), "/channel/info",
                   {"channel_id": "x1", "channel_type": 2, "large": 1})
        c.close_all()
        self.assertEqual((r.outcome, r.err), ("unknown", "client_timeout"))
        self.assertLess(r.dt, 1.5)


class TestRejectedWritesAccounted(Base):
    faults = {"http500_every": 11, "apply_then_500_every": 13}

    def test(self):
        self.assertEqual(self.o("preflight")[1]["verdict"], "PASS")
        self.h("--phase", "seed", "--continue-on-seed-failure")
        self.assertEqual(self.h("--phase", "load", *SMALL_LOAD), 0)
        rc, rep = self.o("post", "--max-ambiguous-ratio", "1.0")
        final = _json(os.path.join(self.st, "world_final.json"))["final"]
        rejected = sum(v["outcome"]["rejected"] for v in final["kinds"].values())
        self.assertGreater(rejected, 0)
        errs = set()
        for v in final["kinds"].values():
            errs |= set(v["err"])
        self.assertEqual(errs, {"http_500"})
        # applied-then-500 writes are ambiguous in the model, so no false missing/unexpected verdict;
        # the only allowed failure is an oracle probe send that itself drew an injected 500
        self.assertLessEqual(set(rep["fail"]), {"probe_send_not_ok"})
        self.assertGreater(rep["subscriber_coverage"]["post"]["ambiguous_keys"], 0)


# ---------------- B2: possible-state model per (channel, uid) ----------------

class TestB2Transitions(unittest.TestCase):
    """Pure model transitions, including the reviewer reproducer and ordering cases."""

    def poss(self, *writes, seed_present=False):
        m = world.SubscriberModel()
        if seed_present:
            m.apply("c", "u00001", world.ADD, world.APPLIED)
        for action, outcome, status in writes:
            m.apply("c", "u00001", action, world.effect_of(outcome, status))
        return world.possible(m.code("c", "u00001"))

    def test_effect_mapping(self):
        self.assertEqual(world.effect_of("ok", 200), world.APPLIED)
        self.assertEqual(world.effect_of("not_sent", None), world.NO_EFFECT)
        self.assertEqual(world.effect_of("rejected", 401), world.NO_EFFECT)
        for o, st in (("rejected", 400), ("rejected", 500), ("unknown", None)):
            self.assertEqual(world.effect_of(o, st), world.AMBIGUOUS)

    def test_table(self):
        A, R = world.ADD, world.REMOVE
        cases = [
            # (seed_present, writes, expected possible states)
            (True, [(A, "rejected", 400)], {"P"}),                     # reviewer X2: add cannot remove
            (True, [(A, "unknown", None)], {"P"}),
            (True, [(R, "rejected", 400)], {"P", "A"}),
            (True, [(R, "not_sent", None)], {"P"}),                    # definite non-admission
            (True, [(R, "rejected", 401)], {"P"}),
            (False, [(R, "unknown", None)], {"A"}),                    # remove cannot add
            (False, [(A, "unknown", None)], {"P", "A"}),
            (False, [(A, "rejected", 401)], {"A"}),
            (False, [(A, "unknown", None), (R, "ok", 200)], {"P", "A"}),   # floating add may land after
            (False, [(A, "unknown", None), (A, "ok", 200)], {"P"}),        # same direction narrows
            (True, [(R, "unknown", None), (A, "ok", 200)], {"P", "A"}),
            (True, [(R, "unknown", None), (R, "ok", 200)], {"A"}),
            (False, [(A, "ok", 200), (R, "ok", 200)], {"A"}),
            (True, [(R, "ok", 200), (A, "rejected", 500)], {"P", "A"}),
        ]
        for seed_present, writes, want in cases:
            self.assertEqual(self.poss(*writes, seed_present=seed_present), want, (seed_present, writes))

    def test_remove_all(self):
        m = world.SubscriberModel()
        for u in ("u00001", "u00002"):
            m.apply("c", u, world.ADD, world.APPLIED)
        m.apply("c", "u00002", world.REMOVE, world.AMBIGUOUS)
        m.remove_all("c", world.APPLIED)
        self.assertEqual(world.possible(m.code("c", "u00001")), {"A"})
        self.assertEqual(world.possible(m.code("c", "u00002")), {"A"})  # floating remove only adds A
        m2 = world.SubscriberModel()
        m2.apply("c", "u00001", world.ADD, world.APPLIED)
        m2.remove_all("c", world.AMBIGUOUS)
        self.assertEqual(world.possible(m2.code("c", "u00001")), {"P", "A"})

    def test_storage_roundtrip_and_bound(self):
        m = world.SubscriberModel()
        m.apply("c", "u00001", world.ADD, world.APPLIED)
        m.apply("c", "u00001", world.REMOVE, world.APPLIED)
        m.apply("c", "u99999", world.ADD, world.AMBIGUOUS)
        self.assertEqual([u for u, _ in m.items("c")], ["u00001", "u99999"])  # touched-but-absent still counted
        m2 = world.SubscriberModel.from_json(json.loads(json.dumps(m.to_json())))
        self.assertEqual(m2.items("c"), m.items("c"))
        self.assertEqual(len(m.keys["c"]), world.UID_SPACE)
        self.assertEqual(m.code("c", "zz_phantom"), 0)


class TestB2OracleControls(Base):
    """End-to-end through seedcheck: model transitions written by the harness API, state mutated in the mock."""

    def mutate(self, writes, drop=(), add=(), scope=None):
        self.assertEqual(self.h("--phase", "seed"), 0)
        wp = os.path.join(self.st, "world_seed.json")
        w = _json(wp)
        m = world.SubscriberModel.from_json(w["model"])
        for uid, action, outcome, status in writes:
            m.apply("repro_g001", uid, action, world.effect_of(outcome, status))
        w["model"] = m.to_json()
        with open(wp, "w") as f:
            json.dump(w, f)
        with self.cluster.state.mu:
            s = self.cluster.state.subs[("repro_g001", 2)]
            for u in drop:
                s.discard(u)
            for u in add:
                s.add(u)
        return self.o("seedcheck", *(["--max-ambiguous-ratio", scope] if scope is not None else []))

    def test_reviewer_x2_rejected_add_then_loss_fails(self):
        rc, rep = self.mutate([("u00001", world.ADD, "rejected", 400)], drop=["u00001"])
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"))
        self.assertEqual(rep["fail"], {"missing_subscriber": 1})

    def test_unknown_add_then_loss_fails(self):
        rc, rep = self.mutate([("u00001", world.ADD, "unknown", None)], drop=["u00001"])
        self.assertEqual(rep["fail"], {"missing_subscriber": 1})

    def test_not_sent_remove_then_loss_fails(self):
        rc, rep = self.mutate([("u00001", world.REMOVE, "not_sent", None)], drop=["u00001"])
        self.assertEqual(rep["fail"], {"missing_subscriber": 1})

    def test_ambiguous_remove_then_absent_passes_with_scope(self):
        rc, rep = self.mutate([("u00001", world.REMOVE, "unknown", None)], drop=["u00001"], scope="1")
        self.assertEqual(rep["verdict"], "PASS", rep)
        cov = rep["subscriber_coverage"]["seed"]
        self.assertEqual((cov["ambiguous_keys"], cov["ambiguous_absent"]), (1, 1))

    def test_unknown_remove_cannot_create_member(self):
        rc, rep = self.mutate([("u77777", world.REMOVE, "unknown", None)], add=["u77777"])
        self.assertEqual(rep["fail"], {"unexpected_subscriber": 1})

    def test_401_add_is_no_effect(self):
        rc, rep = self.mutate([("u77777", world.ADD, "rejected", 401)], add=["u77777"])
        self.assertEqual(rep["fail"], {"unexpected_subscriber": 1})

    def test_ordering_floating_add_then_confirmed_remove(self):
        rc, rep = self.mutate([("u77777", world.ADD, "unknown", None), ("u77777", world.REMOVE, "ok", 200)],
                              add=["u77777"], scope="1")
        self.assertEqual(rep["verdict"], "PASS", rep)

    def test_ordering_floating_add_then_confirmed_add(self):
        rc, rep = self.mutate([("u77777", world.ADD, "unknown", None), ("u77777", world.ADD, "ok", 200)])
        self.assertEqual(rep["fail"], {"missing_subscriber": 1})

    def test_scope_threshold(self):
        writes = [("u%05d" % i, world.REMOVE, "unknown", None) for i in range(10)]
        rc, rep = self.mutate(writes)
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))
        self.assertEqual(rep["scope_errors"], {"ambiguous_ratio_above_threshold": 1})


class TestConnectionRefusedIsNotSent(unittest.TestCase):
    def test(self):
        import socket
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        r = common.Client("", 0.5).call("POST", "http://127.0.0.1:%d" % port, "/x", {})
        self.assertEqual((r.outcome, r.err), (common.NOT_SENT, "conn_refused"))


class TestB3P1Correction(Base):
    def prepare_ambiguous_final(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        self.assertEqual(self.h("--phase", "load", "--addremove", "1", "--send", "1",
                                "--upsert", "0", "--seconds", "0.1"), 0)
        wp = os.path.join(self.st, "world_final.json")
        w = _json(wp)
        m = world.SubscriberModel.from_json(w["model"])
        m.apply("repro_g001", "u77777", world.ADD, world.AMBIGUOUS)
        w["model"] = m.to_json()
        with open(wp, "w") as f:
            json.dump(w, f)

    def test_reviewer_z2_teardown_surviving_ambiguous_uid_fails(self):
        self.prepare_ambiguous_final()
        self.assertEqual(self.o("post", "--max-ambiguous-ratio", "1")[1]["verdict"], "PASS")
        original = oracle.Oracle.check_subscribers

        def inject(o, model, label):
            if label == "teardown":
                with self.cluster.state.mu:
                    self.cluster.state.subs.setdefault(("repro_g001", 2), set()).add("u77777")
            return original(o, model, label)

        # Explicitly widened scope is fixture-only: even it cannot excuse a survivor.
        with mock.patch.object(oracle.Oracle, "check_subscribers", inject):
            rc, rep = self.o("teardown", "--max-ambiguous-ratio", "1")
        self.assertEqual(rep["teardown_outcomes"], {"ok": 6})
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"), rep)
        self.assertEqual(rep["fail"], {"not_empty_after_teardown": 1})

    def test_one_ambiguous_key_is_inconclusive_by_default(self):
        self.prepare_ambiguous_final()
        rc, rep = self.o("post")
        cov = rep["subscriber_coverage"]["post"]
        self.assertEqual(cov["ambiguous_keys"], 1)
        self.assertLess(cov["ambiguous_ratio"], 0.01)
        self.assertEqual(cov["max_ambiguous_ratio"], 0)
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"), rep)

    def test_empty_teardown_keeps_ambiguity_scope(self):
        self.prepare_ambiguous_final()
        rc, rep = self.o("teardown")
        self.assertEqual(rep["teardown_outcomes"], {"ok": 6})
        self.assertEqual(rep["fail"], {})
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"), rep)
        self.assertEqual(rep["subscriber_coverage"]["teardown"]["ambiguous_present"], 0)

    def test_load_write_denominator_and_ratio(self):
        self.prepare_ambiguous_final()
        rows = [
            {"kind": "subscriber_add", "outcome": "ok"},
            {"kind": "subscriber_add", "outcome": "unknown"},
            {"kind": "subscriber_remove", "outcome": "rejected"},
            {"kind": "message_send", "outcome": "not_sent"},
            {"kind": "channel_info", "outcome": "unknown"},
            {"kind": "seed", "outcome": "unknown"},  # never part of load
        ]
        with gzip.open(os.path.join(self.st, "load_journal.jsonl.gz"), "wt") as f:
            for row in rows:
                # Valid message-ledger fields for the one not-sent send.
                if row["kind"] == "message_send":
                    row.update(cmn="r1-s00-00000001", ch="repro_g001", **{"from": "u00000"})
                f.write(json.dumps(row) + "\n")
        # seedcheck reads this synthetic load accounting without message readback.
        shutil.copyfile(os.path.join(self.st, "world_final.json"), os.path.join(self.st, "world_seed.json"))
        rc, rep = self.o("seedcheck")
        cov = rep["subscriber_coverage"]["seed"]
        self.assertEqual(rep["load_write_accounting"]["outcomes"],
                         {"ok": 1, "rejected": 1, "unknown": 2, "not_sent": 1})
        self.assertEqual(rep["load_write_accounting"]["total"], 5)
        self.assertEqual(rep["load_write_accounting"]["non_ok_ratio"], 0.8)
        self.assertEqual(cov["non_ok_load_writes"], 4)
        self.assertEqual(cov["ambiguous_keys"], 1)
        self.assertEqual(cov["ambiguous_keys_per_non_ok_load_write"], 0.25)
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))

    def test_zero_load_denominator_is_null(self):
        self.prepare_ambiguous_final()
        with gzip.open(os.path.join(self.st, "load_journal.jsonl.gz"), "wt") as f:
            f.write(json.dumps({"kind": "subscriber_add", "outcome": "ok"}) + "\n")
        shutil.copyfile(os.path.join(self.st, "world_final.json"), os.path.join(self.st, "world_seed.json"))
        rc, rep = self.o("seedcheck")
        cov = rep["subscriber_coverage"]["seed"]
        self.assertEqual(cov["non_ok_load_writes"], 0)
        self.assertEqual(rep["load_write_accounting"]["non_ok_ratio"], 0)
        self.assertIsNone(cov["ambiguous_keys_per_non_ok_load_write"])
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))

    def test_no_load_journal_reports_absence_and_undefined_ratio(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        rc, rep = self.o("seedcheck")
        acct = rep["load_write_accounting"]
        self.assertFalse(acct["journal_present"])
        self.assertEqual((acct["total"], acct["non_ok"]), (0, 0))
        self.assertIsNone(acct["non_ok_ratio"])
        self.assertIsNone(rep["subscriber_coverage"]["seed"]["ambiguous_keys_per_non_ok_load_write"])
        self.assertEqual((rc, rep["verdict"]), (0, "PASS"))

    def test_non_ok_remove_all_fails(self):
        self.prepare_ambiguous_final()
        original = common.Client.call

        def reject(client, method, base, path, *args, **kwargs):
            if path == "/channel/subscriber_remove_all":
                return common.Result(500, b"{}", 0, common.REJECTED, "http_500", 0)
            return original(client, method, base, path, *args, **kwargs)

        with mock.patch.object(common.Client, "call", reject):
            rc, rep = self.o("teardown")
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"))
        self.assertEqual(rep["fail"]["teardown_not_ok"], 6)

    def test_unreadable_teardown_cannot_pass(self):
        self.prepare_ambiguous_final()
        original = common.Client.call

        def unreadable(client, method, base, path, *args, **kwargs):
            if path.endswith("/subscribers"):
                return common.Result(500, b"{}", 0, common.REJECTED, "http_500", 0)
            return original(client, method, base, path, *args, **kwargs)

        with mock.patch.object(common.Client, "call", unreadable):
            rc, rep = self.o("teardown")
        self.assertEqual(rep["teardown_outcomes"], {"ok": 6})
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))
        self.assertEqual(rep["read_errors"]["subscriber_read_node_error"], 6)


class _FixedResponder:
    """Disposable 127.0.0.1 server answering every request with one (HTTP status, body)."""

    def __init__(self, code, body):
        import http.server
        import threading

        class H(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_GET(self):
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = "http://127.0.0.1:%d" % self.srv.server_address[1]
        self.t = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self.t.start()

    def get(self, envelope):
        c = common.Client("", 2.0)
        try:
            return c.call("GET", self.url, "/x", envelope=envelope)
        finally:
            c.close_all()

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()
        self.t.join(5)


# Real PR70 GET /cluster/node body (NodeConfig, pkg/cluster/cluster/model.go): "status" is
# cluster membership (types.NodeStatus 3 = joined), not an HTTP status.
REAL_NODE = {"id": 1, "is_leader": 1, "role": 1, "cluster_addr": "tcp://10.0.0.1:11110",
             "api_server_addr": "http://10.0.0.1:5001", "online": 1, "allow_vote": 1, "slot_count": 64,
             "term": 1, "slot_leader_count": 64, "uptime": "3m", "app_version": "v2", "config_version": 9,
             "status": 3, "status_format": "\u5df2\u52a0\u5165"}


class TestResponseClassification(unittest.TestCase):
    """XIN-2231: HTTP status is authoritative; body "status" is an application status only on envelope routes."""

    def check(self, code, body, envelope, outcome, err=None):
        srv = _FixedResponder(code, body if isinstance(body, bytes) else json.dumps(body).encode())
        try:
            r = srv.get(envelope)
        finally:
            srv.close()
        self.assertEqual((r.status, r.outcome), (code, outcome))
        if err is not None:
            self.assertEqual(r.err, err)
        return r

    def test_real_node_shape_is_ok(self):
        for st in (1, 2, 3):
            r = self.check(200, dict(REAL_NODE, status=st), False, common.OK)
            self.assertEqual((r.json()["id"], r.json()["status"]), (1, st))
        omitted = dict(REAL_NODE)
        del omitted["status"], omitted["status_format"]
        self.check(200, omitted, False, common.OK)

    def test_real_nodes_shape_is_ok(self):
        body = {"total": 3, "data": [dict(REAL_NODE, id=k, status=3) for k in (1, 2, 3)]}
        self.check(200, body, False, common.OK)

    def test_envelope_routes_keep_application_errors(self):
        self.check(200, {"status": 200}, True, common.OK)
        self.check(200, {"status": 200, "data": {"message_id": 7}}, True, common.OK)
        self.check(200, {"status": 404}, True, common.REJECTED, "server_status_field")
        self.check(200, {"msg": "bad", "status": 500}, True, common.REJECTED, "server_status_field")

    def test_genuine_http_errors_rejected_on_both_route_kinds(self):
        for env in (True, False):
            self.check(400, {"msg": "slot leader is not current node", "status": 400}, env, common.REJECTED,
                       "http_400_leader")
            self.check(500, {"msg": "x", "status": 500}, env, common.REJECTED, "http_500")
            self.check(401, b"", env, common.REJECTED, "http_401")
            self.check(400, dict(REAL_NODE), env, common.REJECTED)


class TestRealNodeResponseShape(Base):
    """XIN-2231 PRE regression: real /cluster/node(s) bodies carry membership "status": 3."""

    def check_topology(self, rep, n):
        self.assertEqual(rep["verdict"], "PASS", rep)
        self.assertEqual(rep["read_errors"], {})
        topo = rep["topology"]
        self.assertEqual(sorted(topo["api"].values()), list(range(1, n + 1)))
        self.assertEqual(sorted(topo["manager"].values()), list(range(1, n + 1)))
        self.assertEqual(topo["cluster_nodes"], list(range(1, n + 1)))
        self.assertEqual(topo["membership_status"], {str(k): 3 for k in range(1, n + 1)})
        self.assertEqual(rep["leaders"]["channels_identified"], 6)

    def test_mock_serves_real_shape(self):
        c = common.Client(TOKEN, 2.0)
        try:
            j = c.call("GET", self.cluster.urls("manager").split(",")[0], "/cluster/node", envelope=False).json()
        finally:
            c.close_all()
        self.assertEqual((j["id"], j["status"], j["online"]), (1, 3, 1))

    def test_preflight_single_node(self):
        rc, rep = self.o("preflight")
        self.assertEqual(rc, 0)
        self.check_topology(rep, 1)

    def test_full_sequence_single_node(self):
        rc, rep = self.full_green()
        self.assertEqual(rc, 0)
        self.check_topology(rep, 1)


class TestRealNodeResponseShapeThreeNode(TestRealNodeResponseShape):
    nodes = 3

    def test_preflight_single_node(self):
        rc, rep = self.o("preflight")
        self.assertEqual(rc, 0)
        self.check_topology(rep, 3)

    def test_full_sequence_single_node(self):
        rc, rep = self.full_green()
        self.assertEqual(rc, 0)
        self.check_topology(rep, 3)


class TestMembershipStatusValuesAreData(Base):
    """will-join (1), joining (2) and omitted (0) membership values are recorded, never read as HTTP status."""

    def test(self):
        for st, rec in ((1, 1), (2, 2), (None, None)):
            self.cluster.state.faults["node_status"] = st
            rc, rep = self.o("preflight")
            self.assertEqual((rc, rep["verdict"]), (0, "PASS"), rep)
            self.assertEqual(rep["topology"]["membership_status"], {"1": rec})


class TestCandidateChannelConfigIsLeaderClaim(Base):
    """ChannelClusterConfig "status": 1 (candidate) is endpoint data on the leader's 200 answer."""
    nodes = 3
    faults = {"channel_config_status": 1}

    def test(self):
        rc, rep = self.o("preflight")
        self.assertEqual((rc, rep["verdict"]), (0, "PASS"), rep)
        self.assertEqual(rep["leaders"]["channels_identified"], 6)


class TestNodeReadErrorsFailClosed(Base):
    """Genuine HTTP errors, application error objects and malformed bodies stay INCONCLUSIVE."""
    nodes = 3

    def run_pre(self, **faults):
        self.cluster.state.faults.update(faults)
        rc, rep = self.o("preflight")
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"), rep)
        return rep

    def test_http_400(self):
        rep = self.run_pre(node_status_400=True)
        self.assertEqual(rep["read_errors"]["node_identity"], 6)
        self.assertIn("topology_incomplete", rep["read_errors"])
        self.assertEqual({e["http"] for e in rep["examples"]["read_error:node_identity"]}, {400})

    def test_http_200_application_error(self):
        rep = self.run_pre(node_app_error=True)
        self.assertEqual(rep["read_errors"]["node_identity"], 6)
        self.assertIn("topology_incomplete", rep["read_errors"])

    def test_malformed_body(self):
        rep = self.run_pre(node_malformed=True)
        self.assertEqual(rep["read_errors"]["node_identity"], 6)
        self.assertEqual({e["err"] for e in rep["examples"]["read_error:node_identity"]}, {"malformed_node_body"})

    def test_nodes_entry_without_id(self):
        rep = self.run_pre(nodes_malformed_entry="no_id")
        self.assertEqual(rep["read_errors"], {"cluster_nodes_unreadable": 1})

    def test_nodes_entry_with_invalid_id(self):
        rep = self.run_pre(nodes_malformed_entry="zero_id")
        self.assertEqual(rep["read_errors"], {"cluster_nodes_unreadable": 1})

    def test_channel_config_application_error(self):
        rep = self.run_pre(channel_config_app_error=True)
        self.assertEqual(rep["read_errors"], {"leader_identification": 6})

    def test_unreachable_node_endpoint(self):
        ap = self.cluster.urls("api").split(",")
        ap[2] = "http://127.0.0.1:%d" % free_port()
        rc, rep = self.o("preflight", api=",".join(ap))
        self.assertNotEqual(rep["verdict"], "PASS")
        self.assertIn("node_identity", rep["read_errors"])


if __name__ == "__main__":
    unittest.main()
