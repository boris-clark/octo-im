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

    def o(self, mode, *extra):
        argv = [mode, "--api", self.cluster.urls("api"), "--manager", self.cluster.urls("manager"),
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


class TestRedDivergentNode(Base):
    nodes = 3
    faults = {"diverge_node": 1}

    def test(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        rc, rep = self.o("seedcheck")
        self.assertEqual(rep["fail"], {"divergent_subscriber_read": 6})


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
        self.assertEqual(rep["fail"], {"duplicate_message": 1})


class TestRedSeqGap(Base):
    faults = {"seq_gap_n": 3}

    def test(self):
        rc, rep = self.full_green()
        self.assertEqual(rep["fail"], {"seq_gap": 1})


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
        rc, rep = self.full_green()
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"))
        self.assertEqual(rep["read_errors"], {"messagesync": 6})


# ---------------- client failures: outcomes retained and accounted, no false verdict ----------------

class TestClientTimeoutsAccounted(Base):
    faults = {"slow_every": 9, "slow_secs": 2.0}

    def test(self):
        # load client timeout = budget + 1 = 1.2s < 2.0s server delay; seed timeout = budget + 2 = 2.2s
        rc, rep = self.full_green(budget="0.2")
        final = _json(os.path.join(self.st, "world_final.json"))["final"]
        unknown = sum(v["outcome"]["unknown"] for v in final["kinds"].values())
        self.assertGreater(unknown, 0)
        self.assertEqual(rep["verdict"], "PASS", rep)
        model = _json(os.path.join(self.st, "world_final.json"))["model"]
        self.assertGreater(sum(len(v) for v in model["uncertain"].values()), 0)


class TestRejectedWritesAccounted(Base):
    faults = {"http500_every": 11, "apply_then_500_every": 13}

    def test(self):
        self.assertEqual(self.o("preflight")[1]["verdict"], "PASS")
        self.h("--phase", "seed", "--continue-on-seed-failure")
        self.assertEqual(self.h("--phase", "load", *SMALL_LOAD), 0)
        rc, rep = self.o("post")
        final = _json(os.path.join(self.st, "world_final.json"))["final"]
        rejected = sum(v["outcome"]["rejected"] for v in final["kinds"].values())
        self.assertGreater(rejected, 0)
        errs = set()
        for v in final["kinds"].values():
            errs |= set(v["err"])
        self.assertEqual(errs, {"http_500"})
        # applied-then-500 writes are uncertain in the model, so no false missing/unexpected verdict
        self.assertNotIn("missing_subscriber", rep["fail"])
        self.assertNotIn("unexpected_subscriber", rep["fail"])
        self.assertNotIn("rejected_but_persisted", rep["fail"])


class TestConnectionRefusedIsNotSent(unittest.TestCase):
    def test(self):
        import socket
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        r = common.Client("", 0.5).call("POST", "http://127.0.0.1:%d" % port, "/x", {})
        self.assertEqual((r.outcome, r.err), (common.NOT_SENT, "conn_refused"))


if __name__ == "__main__":
    unittest.main()
