#!/usr/bin/env python3
"""Full default-shape dry run against the disposable mock (never a real service).

378 channels, 42 + 42 + 16 workers, 150/20 seed members, default seed. Only the
load duration is shortened (argument, default 20 s). Runs the exact CLI
commands an operator would run (subprocesses), including two graceful mock
restarts and teardown, and prints each oracle verdict.

usage: fullscale_dryrun.py <work-dir> [seconds] [nodes]
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from mock.mock_wukong import MockCluster  # noqa: E402


def main():
    work = sys.argv[1]
    secs = sys.argv[2] if len(sys.argv) > 2 else "20"
    nodes = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    os.makedirs(work, exist_ok=True)
    tok = os.path.join(work, "token")
    secret = "dryrun-" + os.urandom(8).hex()
    with open(tok, "w") as f:
        f.write(secret)
    os.chmod(tok, 0o600)
    st = os.path.join(work, "state")
    c = MockCluster(nodes, secret, {"visibility_delay": 0.2})
    api, mgr = c.urls("api"), c.urls("manager")
    results = []

    def run(*args):
        cmd = [sys.executable, "-I"] + [os.path.join(ROOT, args[0])] + list(args[1:])
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        assert secret not in r.stdout + r.stderr
        tail = (r.stdout.strip().splitlines() or [""])[-1]
        results.append({"cmd": " ".join(args[:2]), "rc": r.returncode, "last": tail[:400]})
        print("rc=%d %s :: %s" % (r.returncode, " ".join(args[:2]), tail[:300]), flush=True)
        if r.returncode not in (0,) and args[0] == "harness.py":
            print(r.stderr[-2000:])
        return r.returncode

    common = ["--api", api, "--token-file", tok, "--state-dir", st, "--run-id", "dry1"]
    oc = common + ["--manager", mgr, "--drain-timeout", "60", "--drain-interval", "1"]
    run("oracle.py", "preflight", *oc)
    run("harness.py", "--phase", "seed", *common)
    run("oracle.py", "seedcheck", *oc)
    run("harness.py", "--phase", "load", "--seconds", secs, "--report-interval", "5", *common)
    run("oracle.py", "post", *oc)
    c = c.graceful_restart()
    run("oracle.py", "restart", "--restart-index", "1", *oc)
    c = c.graceful_restart()
    run("oracle.py", "restart", "--restart-index", "2", *oc)
    run("oracle.py", "teardown", *oc)
    run("analyze.py", "--state-dir", st, "--json-out", os.path.join(work, "summary.json"))
    c.stop()
    leaked = []
    for root, _d, files in os.walk(work):
        for n in files:
            p = os.path.join(root, n)
            if n == "token":
                continue
            data = open(p, "rb").read()
            if p.endswith(".gz"):
                import gzip
                data = gzip.decompress(data)
            if secret.encode() in data:
                leaked.append(p)
    print(json.dumps({"results": results, "token_leaks": leaked}, indent=1))
    return 0 if all(r["rc"] == 0 for r in results) and not leaked else 1


if __name__ == "__main__":
    sys.exit(main())
