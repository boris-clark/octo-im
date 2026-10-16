#!/usr/bin/env python3
"""Mutation controls: prove the shipped tests detect the regressions that matter.

Each mutation is applied to a disposable copy of this directory and must make
its target tests FAIL ("killed"). The unmutated copy must pass the same tests
first. Mutations reintroduce the XIN-2204 blockers (fail-open node reads,
missing topology check, sticky-unchecked ambiguity, rejected-as-no-effect,
no scope threshold), the XIN-2231 body-status/HTTP-status confusion and other
oracle checks.

usage: mutation_controls.py [--json-out PATH]
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MUTATIONS = [
    ("M1_B1_node_read_error_fail_open", "oracle.py",
     'self.rerr("subscriber_read_node_error"', '(lambda *a: None)("subscriber_read_node_error"',
     ["test_harness.TestB1UnreachableNodeIsInconclusive.test_unreachable_manager"]),
    ("M2_B1_topology_check_removed", "oracle.py",
     'out = {"api": {}, "manager": {}, "cluster_nodes": None, "membership_status": {}}',
     'out = {"api": {}, "manager": {}, "cluster_nodes": None, "membership_status": {}}\n        return out',
     ["test_harness.TestB1OmittedManagerIsInconclusive"]),
    ("M3_B1_forwarding_mismatch_ignored", "oracle.py",
     "if sorted(v) != canon:", "if False:",
     ["test_harness.TestB1StaleRouteMismatchFails"]),
    ("M4_B2_sticky_unchecked_ambiguity", "wkload/world.py",
     "    code = code or DEFAULT\n",
     "    code = code or DEFAULT\n    if code & (FLOAT_ADD | FLOAT_REM):\n        return {\"P\", \"A\"}\n",
     ["test_harness.TestB2OracleControls.test_reviewer_x2_rejected_add_then_loss_fails",
      "test_harness.TestB2Transitions"]),
    ("M5_B2_rejected_treated_as_no_effect", "wkload/world.py",
     "if outcome == NOT_SENT or http_status == 401:",
     "if outcome == NOT_SENT or http_status == 401 or outcome == \"rejected\":",
     ["test_harness.TestB2Transitions.test_effect_mapping", "test_harness.TestRejectedWritesAccounted"]),
    ("M6_B2_scope_threshold_removed", "oracle.py",
     "if ratio > self.a.max_ambiguous_ratio:", "if False:",
     ["test_harness.TestB2OracleControls.test_scope_threshold", "test_harness.TestClientTimeoutsAccounted"]),
    ("M7_seq_gap_check_removed", "oracle.py",
     "elif seq != prev + 1:", "elif False:",
     ["test_harness.TestRedSeqGapMidChannel"]),
    ("M8_missing_accepted_message_ignored", "oracle.py",
     'self.bad("missing_accepted_message"', '(lambda *a: None)("missing_accepted_message"',
     ["test_harness.TestRedMissingMessage", "test_harness.TestRedRestartLoss"]),
    ("M9_N3_preflight_readback_removed", "oracle.py",
     "                for page in o.pages(cid, ctype):\n                    if page is None:\n                        break\n"
     "                    n += len(page)\n",
     "                pass\n",
     ["test_harness.TestN3StaleMessagesWithUnloadedConfig"]),
    ("M10_N4_per_socket_timeout", "wkload/common.py",
     "chunk = r.read1(65536)", "chunk = r.read(65536)",
     ["test_harness.TestN4WholeRequestDeadline"]),
    ("M11_B3_strict_teardown_removed", "oracle.py",
     'if label == "teardown" and act:', 'if False:',
     ["test_harness.TestB3P1Correction.test_reviewer_z2_teardown_surviving_ambiguous_uid_fails"]),
    ("M12_P1_nonzero_default_restored", "oracle.py",
     'type=float, default=0.0,', 'type=float, default=0.01,',
     ["test_harness.TestB3P1Correction.test_one_ambiguous_key_is_inconclusive_by_default"]),
    ("M13_P1_unknown_writes_excluded_from_denominator", "oracle.py",
     'if k != OK)', 'if k not in (OK, UNKNOWN))',
     ["test_harness.TestB3P1Correction.test_load_write_denominator_and_ratio"]),
    ("M14_B4_journal_hash_not_reconciled", "wkload/evidence.py",
     "s != f['producer_evidence']", "False",
     ["test_evidence.TestEvidence.test_receipt_hash_mismatch"]),
    ("M15_B4_counters_not_reconciled", "wkload/evidence.py",
     "if counts != s['counts'] or any(v['n'] != sum(v['outcome'].values()) for v in f['kinds'].values()):",
     "if False:", ["test_evidence.TestEvidence.test_counter_mismatch"]),
    ("M16_B4_finalization_ignored", "wkload/evidence.py",
     "f.get('journal_complete') is not True", "False",
     ["test_evidence.TestEvidence.test_incomplete_writer_finalization"]),
    ("M17_B5_receipt_equality_removed", "oracle.py",
     "if outcome == OK and mid != expected_mid:", "if False:",
     ["test_evidence.TestEvidence.test_changed_message_id", "test_evidence.TestEvidence.test_restart_changed_id"]),
    ("M18_B5_duplicate_id_ignored", "oracle.py",
     "elif seen.duplicate_mid(mid):", "elif False:",
     ["test_evidence.TestEvidence.test_duplicate_message_ids"]),
    ("M19_B5_invalid_id_ignored", "oracle.py",
     "if not valid_mid(mid):", "if False:",
     ["test_evidence.TestEvidence.test_invalid_message_id"]),
    ("M20_B4_writer_failure_suppressed", "harness.py",
     '"journal_complete": not journal_error and not stuck and not worker_errors,',
     '"journal_complete": True,',
     ["test_evidence.TestJournalFailures.test_failed_writer_invalidates_final_and_exit",
      "test_evidence.TestJournalFailures.test_close_failure_invalidates_final_and_exit"]),
    ("M21_body_status_treated_as_http_status", "wkload/common.py",
     "        if not envelope:\n            return Result(r.status, body, dt, OK, None, t_wall)\n", "",
     ["test_harness.TestRealNodeResponseShape", "test_harness.TestResponseClassification"]),
    ("M22_node_identity_shape_unchecked", "oracle.py",
     "if isinstance(v, int) and not isinstance(v, bool) and v > 0:", "if v is not None:",
     ["test_harness.TestNodeReadErrorsFailClosed.test_nodes_entry_with_invalid_id"]),
    ("M23_channel_config_app_error_claims_leader", "oracle.py",
     'if isinstance(j, dict) and "msg" not in j:', "if r.outcome == OK:",
     ["test_harness.TestNodeReadErrorsFailClosed.test_channel_config_application_error"]),

]


def copy_tree(dst):
    shutil.copytree(ROOT, dst, ignore=shutil.ignore_patterns("__pycache__", "evidence"))


def run_tests(tree, ids):
    boot = "import sys, unittest; sys.path.insert(0, '.'); unittest.main(module=None, argv=['unittest'] + sys.argv[1:])"
    r = subprocess.run([sys.executable, "-I", "-c", boot] + ids, cwd=os.path.join(tree, "tests"),
                       capture_output=True, text=True, timeout=900)
    tail = [ln for ln in r.stderr.splitlines() if ln.startswith(("Ran ", "OK", "FAILED"))]
    if "ImportError" in r.stderr or "ModuleNotFoundError" in r.stderr:
        tail.append("IMPORT_ERROR")
    return r.returncode, " | ".join(tail)


def main():
    ap = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    ap.add_argument("--json-out", default="")
    a = ap.parse_args()
    results = []
    work = tempfile.mkdtemp(prefix="pr70bl-mut-")
    try:
        base = os.path.join(work, "baseline")
        copy_tree(base)
        all_ids = sorted({t for m in MUTATIONS for t in m[4]})
        rc, tail = run_tests(base, all_ids)
        results.append({"id": "M0_baseline_unmutated", "rc": rc, "tests": all_ids, "summary": tail,
                        "result": "pass" if rc == 0 else "BROKEN"})
        print("M0 baseline rc=%d %s" % (rc, tail), flush=True)
        for mid, rel, old, new, ids in MUTATIONS:
            tree = os.path.join(work, mid)
            copy_tree(tree)
            p = os.path.join(tree, rel)
            src = open(p, encoding="utf-8").read()
            if src.count(old) != 1:
                results.append({"id": mid, "result": "MUTATION_NOT_APPLICABLE", "occurrences": src.count(old)})
                print("%s NOT APPLICABLE (%d occurrences)" % (mid, src.count(old)), flush=True)
                continue
            with open(p, "w", encoding="utf-8") as f:
                f.write(src.replace(old, new))
            rc, tail = run_tests(tree, ids)
            res = "SURVIVED" if rc == 0 else ("INVALID_IMPORT_ERROR" if "IMPORT_ERROR" in tail else "killed")
            results.append({"id": mid, "file": rel, "tests": ids, "rc": rc, "summary": tail, "result": res})
            print("%s %s rc=%d %s" % (mid, res, rc, tail), flush=True)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    ok = results[0]["result"] == "pass" and all(r["result"] == "killed" for r in results[1:])
    if a.json_out:
        with open(a.json_out, "w", encoding="utf-8") as f:
            json.dump({"all_killed": ok, "results": results}, f, indent=1)
    print("MUTATION CONTROLS %s" % ("PASS (all killed)" if ok else "FAIL"), flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
