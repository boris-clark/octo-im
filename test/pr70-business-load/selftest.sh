#!/bin/sh
# Self-test: unit tests + red/green negative controls + full default-shape dry
# runs (single node and three nodes) against disposable local mocks only.
# Usage: ./selftest.sh [dryrun-seconds]
set -eu
cd "$(dirname "$0")"
SECS="${1:-20}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pr70bl-selftest.XXXXXX")"
umask 077
echo "== python: $(python3 -I -c 'import sys; print(sys.version.split()[0])')"
echo "== verbatim originals"
(cd original && shasum -a 256 -c SHA256SUMS)
echo "== unit tests and negative controls"
python3 -I -m unittest discover -s tests -v
echo "== full default-shape dry run, 1 node, ${SECS}s load"
python3 -I mock/fullscale_dryrun.py "$WORK/n1" "$SECS" 1
echo "== full default-shape dry run, 3 nodes, ${SECS}s load"
python3 -I mock/fullscale_dryrun.py "$WORK/n3" "$SECS" 3
echo "== SELFTEST PASS (work dir $WORK)"
