"""B4/B5 mock-only loss/corruption regressions."""
import gzip
import json
import os
from unittest import mock
from test_harness import Base, _json
from wkload import common


class TestEvidence(Base):
    def prepare(self):
        self.assertEqual(self.h("--phase", "seed"), 0)
        self.assertEqual(self.h("--phase", "load", "--addremove", "0", "--send", "2",
                                "--upsert", "1", "--seconds", "0.15"), 0)
        self.jp = os.path.join(self.st, "load_journal.jsonl.gz")
        self.rows = list(common.read_journal(self.jp))
        self.assertGreater(sum(r["kind"] == "message_send" for r in self.rows), 2)

    def replace(self, rows):
        with gzip.open(self.jp, "wt") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    def lose_messages(self):
        with self.cluster.state.mu:
            self.cluster.state.msgs.clear()
            self.cluster.state.seq.clear()

    def test_missing_journal_with_loss(self):
        self.prepare(); os.unlink(self.jp); self.lose_messages()
        rc, rep = self.o("post")
        self.assertEqual((rc, rep["verdict"]), (3, "INCONCLUSIVE"), rep)
        self.assertIn("load_evidence", rep["read_errors"])
        self.assertNotIn("messages_accepted_in_ledger_post", rep["info"])

    def test_empty_journal_with_loss(self):
        self.prepare(); self.replace([]); self.lose_messages()
        self.assertEqual(self.o("post")[1]["verdict"], "INCONCLUSIVE")

    def test_valid_partial_journal_with_loss(self):
        self.prepare(); self.replace(self.rows[:len(self.rows)//2]); self.lose_messages()
        self.assertEqual(self.o("post")[1]["verdict"], "INCONCLUSIVE")

    def test_truncated_journal_reports_error(self):
        self.prepare()
        with open(self.jp, "r+b") as f:
            f.truncate(os.path.getsize(self.jp)//2)
        self.assertEqual(self.o("post")[1]["verdict"], "INCONCLUSIVE")

    def test_counter_mismatch(self):
        self.prepare()
        p = os.path.join(self.st, "world_final.json"); w = _json(p)
        w["final"]["kinds"]["message_send"]["n"] += 1
        common.write_json(p, w)
        self.assertEqual(self.o("post")[1]["verdict"], "INCONCLUSIVE")

    def test_same_count_identity_mismatch(self):
        self.prepare()
        next(r for r in self.rows if r["kind"] == "message_send")["cmn"] = "r1-s00-99999999"
        self.replace(self.rows)
        self.assertEqual(self.o("post")[1]["verdict"], "INCONCLUSIVE")

    def test_missing_final_all_later_modes(self):
        self.prepare(); os.unlink(os.path.join(self.st, "world_final.json"))
        for mode in ("post", "restart", "teardown"):
            self.assertEqual(self.o(mode)[1]["verdict"], "INCONCLUSIVE")
        self.assertFalse(os.path.exists(os.path.join(self.st, "world_teardown.json")))

    def test_incomplete_writer_finalization(self):
        self.prepare()
        p = os.path.join(self.st, "world_final.json"); w = _json(p)
        w["final"]["journal_complete"] = False
        common.write_json(p, w)
        self.assertEqual(self.o("post")[1]["verdict"], "INCONCLUSIVE")

    def test_duplicate_client_identity(self):
        self.prepare()
        sends = [r for r in self.rows if r["kind"] == "message_send"]
        sends[1]["cmn"] = sends[0]["cmn"]
        self.replace(self.rows)
        self.assertEqual(self.o("post")[1]["verdict"], "INCONCLUSIVE")

    def corrupt(self, which):
        self.prepare()
        with self.cluster.state.mu:
            msgs = [m for ms in self.cluster.state.msgs.values() for m in ms]
            if which == "changed": msgs[0]["message_id"] += 99999999
            elif which == "duplicate":
                for m in msgs: m["message_id"] = 987654321
            else: msgs[0]["message_id"] = which

    def test_changed_message_id(self):
        self.corrupt("changed")
        rc, rep = self.o("post")
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertIn("message_id_mismatch", rep["fail"])

    def test_duplicate_message_ids(self):
        self.corrupt("duplicate")
        rc, rep = self.o("post")
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertGreater(rep["fail"].get("duplicate_message_id", 0), 0)

    def test_invalid_message_id(self):
        self.corrupt(0)
        self.assertIn("invalid_message_id", self.o("post")[1]["fail"])

    def test_restart_changed_id(self):
        self.prepare()
        self.assertEqual(self.o("post")[1]["verdict"], "PASS")
        self.cluster = self.cluster.graceful_restart()
        with self.cluster.state.mu:
            next(iter(self.cluster.state.msgs.values()))[0]["message_id"] += 99999999
        rep = self.o("restart")[1]
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertIn("message_id_mismatch", rep["fail"])
        self.assertIn("restart_readback_changed", rep["fail"])

    def test_receipt_hash_mismatch(self):
        self.prepare()
        next(r for r in self.rows if r['kind'] == 'message_send')['mid'] += 99999999
        self.replace(self.rows)
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')

    def test_per_kind_outcome_mismatch(self):
        self.prepare()
        p = os.path.join(self.st, 'world_final.json'); w = _json(p)
        w['final']['kinds']['message_send']['outcome']['ok'] -= 1
        w['final']['kinds']['message_send']['outcome']['unknown'] += 1
        common.write_json(p, w)
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')

    def test_writer_record_count_mismatch(self):
        self.prepare()
        p = os.path.join(self.st, 'world_final.json'); w = _json(p)
        w['final']['journal_records'] -= 1
        common.write_json(p, w)
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')

    def test_invalid_json_shape(self):
        self.prepare(); self.replace([None])
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')

    def test_malformed_json(self):
        self.prepare()
        with gzip.open(self.jp, 'wt') as f: f.write('{invalid-json')
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')

    def test_oversized_record(self):
        self.prepare()
        with gzip.open(self.jp, 'wt') as f: f.write('x' * 1048577)
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')

    def test_probe_id_corruption_after_restart(self):
        self.prepare()
        self.assertEqual(self.o('post')[1]['verdict'], 'PASS')
        self.cluster = self.cluster.graceful_restart()
        with self.cluster.state.mu:
            msgs = [m for ms in self.cluster.state.msgs.values() for m in ms]
            next(m for m in msgs if 'postprobe' in m['client_msg_no'])['message_id'] += 99999999
        self.assertIn('message_id_mismatch', self.o('restart')[1]['fail'])

    def test_empty_completed_load_is_not_accepted(self):
        self.assertEqual(self.h('--phase', 'seed'), 0)
        self.assertEqual(self.h('--phase', 'load', '--addremove', '0', '--send', '0',
                                '--upsert', '0', '--seconds', '0.05'), 0)
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')


class TestJournalFailures(Base):
    def test_writer_crash_propagates_and_is_collected(self):
        j = common.Journal(os.path.join(self.tmp, 'journal.gz'), common.Redactor('fixture'))
        j.t.terminate(); j.t.wait(timeout=2)
        with self.assertRaises(common.JournalError):
            j.write({'kind': 'anything'})
        with self.assertRaises(common.JournalError):
            j.close()
        self.assertTrue(j.closed)

    def test_writer_open_failure(self):
        j = common.Journal(os.path.join(self.tmp, 'missing', 'journal.gz'), common.Redactor('fixture'))
        with self.assertRaises(common.JournalError):
            j.close()
        self.assertTrue(j.closed)

    def test_failed_writer_invalidates_final_and_exit(self):
        self.assertEqual(self.h('--phase', 'seed'), 0)
        original = common.Journal.write
        def fail(journal, rec):
            if rec['kind'] == 'message_send':
                raise common.JournalError('injected disk failure')
            return original(journal, rec)
        with mock.patch.object(common.Journal, 'write', fail):
            self.assertEqual(self.h('--phase', 'load', '--addremove', '0', '--send', '1',
                                    '--upsert', '0', '--seconds', '0.1'), 4)
        f = _json(os.path.join(self.st, 'world_final.json'))['final']
        self.assertFalse(f['journal_complete'])
        self.assertEqual(f['worker_errors'], ['JournalError'])
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')

    def test_close_failure_invalidates_final_and_exit(self):
        self.assertEqual(self.h('--phase', 'seed'), 0)
        original = common.Journal.close
        def fail(journal):
            original(journal)
            raise common.JournalError('injected final flush failure')
        with mock.patch.object(common.Journal, 'close', fail):
            self.assertEqual(self.h('--phase', 'load', '--addremove', '0', '--send', '1',
                                    '--upsert', '0', '--seconds', '0.1'), 4)
        f = _json(os.path.join(self.st, 'world_final.json'))['final']
        self.assertFalse(f['journal_complete'])
        self.assertIn('flush', f['journal_error'])
        self.assertEqual(self.o('post')[1]['verdict'], 'INCONCLUSIVE')


    def test_stalled_writer_close_is_bounded_and_collected(self):
        import signal
        import time
        j = common.Journal(os.path.join(self.tmp, 'stalled.gz'), common.Redactor('fixture'))
        os.kill(j.t.pid, signal.SIGSTOP)
        started = time.monotonic()
        with self.assertRaises(common.JournalError):
            j.close()
        self.assertLess(time.monotonic() - started, 16)
        self.assertIsNotNone(j.t.returncode)

    def test_seed_failure_collects_writer(self):
        captured = []
        def fail(journal, rec):
            captured.append(journal)
            raise common.JournalError('seed journal failure')
        with mock.patch.object(common.Journal, 'write', fail):
            with self.assertRaises(common.JournalError):
                self.h('--phase', 'seed')
        self.assertTrue(captured[0].closed)
        self.assertIsNotNone(captured[0].t.returncode)
