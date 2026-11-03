#!/usr/bin/env python3
"""Run one bounded check and retain its exact command, output and exit status."""
import argparse, datetime, hashlib, json, pathlib, subprocess, time
p = argparse.ArgumentParser()
p.add_argument('--evidence', required=True)
p.add_argument('--name', required=True)
p.add_argument('command', nargs=argparse.REMAINDER)
a = p.parse_args()
command = a.command[1:] if a.command[:1] == ['--'] else a.command
out = pathlib.Path(a.evidence).resolve()
(out / 'logs').mkdir(parents=True, exist_ok=True)
log = out / 'logs' / (a.name + '.log')
start = datetime.datetime.now(datetime.timezone.utc).isoformat()
t = time.monotonic()
with log.open('wb') as f:
    result = subprocess.run(command, stdout=f, stderr=subprocess.STDOUT)
record = dict(name=a.name, command=command, cwd=str(pathlib.Path.cwd()), started=start,
              elapsed_seconds=time.monotonic()-t, exit=result.returncode,
              log=str(log), sha256=hashlib.sha256(log.read_bytes()).hexdigest())
with (out / 'commands.jsonl').open('a') as f:
    f.write(json.dumps(record)+'\n')
print(json.dumps(record), flush=True)
raise SystemExit(result.returncode)
