#!/usr/bin/env python3
"""Create disposable fixtures with pinned old code, then test the new reader."""
import argparse
import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import time

BASE = 'bdfbce6951787f58706e1ce15f0cfb1bf02252df'
REPO = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix='issue87-compat-'))
    if args.output:
        output.mkdir(parents=True, exist_ok=False)
    (output / 'logs').mkdir()
    env = dict(os.environ, GOCACHE=str(output / 'go-cache'))

    def run(name, command, cwd, extra_env=None):
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        tick = time.monotonic()
        log = output / 'logs' / (name + '.log')
        with log.open('wb') as stream:
            result = subprocess.run(command, cwd=cwd, env=dict(env, **(extra_env or {})), stdout=stream, stderr=subprocess.STDOUT)
        record = dict(name=name, command=command, cwd=str(cwd), started=started,
                      elapsed_seconds=time.monotonic()-tick, exit=result.returncode,
                      log=str(log), sha256=hashlib.sha256(log.read_bytes()).hexdigest())
        with (output / 'commands.jsonl').open('a') as stream:
            stream.write(json.dumps(record)+'\n')
        print(json.dumps(record), flush=True)
        if result.returncode:
            raise SystemExit(result.returncode)

    baseline = output / 'baseline'
    baseline.mkdir()
    archive = subprocess.check_output(['git', 'archive', BASE], cwd=REPO)
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(baseline)
    driver = Path('experiments/issue87-compat/oldfixture/main.go')
    (baseline / driver).parent.mkdir(parents=True)
    (baseline / driver).write_bytes((REPO / driver).read_bytes())
    binary = output / 'oldfixture'
    run('old-build', ['go', 'build', '-p', '1', '-ldflags', '-X main.sourceRevision='+BASE,
                      '-o', str(binary), './experiments/issue87-compat/oldfixture'], baseline)
    manifest = dict(baseline=BASE, source_archive_sha256=hashlib.sha256(archive).hexdigest(),
                    driver_sha256=hashlib.sha256((baseline/driver).read_bytes()).hexdigest(),
                    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    (output / 'old-source.json').write_text(json.dumps(manifest, indent=2)+'\n')
    fixtures = output / 'fixtures'
    fixtures.mkdir()
    for name, profile in [('node1','normal'), ('node2','normal'), ('node3','normal'),
                          ('single','single'), ('lagging','lagging'), ('divergent','divergent'), ('malformed','malformed'),('noquorum','normal')]:
        root = fixtures / name
        root.mkdir()
        run('old-create-'+name, [str(binary), '-root', str(root), '-profile', profile,
                                '-report', str(fixtures/(name+'.json'))], baseline)
    run('new-activate-read-write-reopen', ['go', 'test', '-p', '1', './pkg/cluster/channel',
              '-run', '^TestPinnedOldStore', '-count=1', '-v', '-timeout=90s'], REPO,
              {'ISSUE87_OLD_FIXTURES':str(fixtures)})
    print('Evidence: '+str(output), flush=True)


if __name__ == '__main__':
    main()
