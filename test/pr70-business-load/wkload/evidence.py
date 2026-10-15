"""Bounded producer/journal reconciliation and disk-backed message identities."""
import hashlib
import json
import os
import sqlite3
import tempfile
from collections import defaultdict
from .common import OK, OUTCOMES, read_journal, read_json

KINDS = ('subscriber_add', 'subscriber_remove', 'message_send', 'channel_info')


class EvidenceError(ValueError):
    pass


class Summary:
    """Constant-space per-worker hash chains include every send identity/receipt.

    Each producer has a sequential stream; inter-worker ordering is irrelevant.
    """
    def __init__(self):
        self.counts = defaultdict(lambda: {k: 0 for k in OUTCOMES})
        self.hashes = {}
        self.total = 0

    def record(self, rec):
        kind, outcome = rec['kind'], rec['outcome']
        if kind not in KINDS or outcome not in OUTCOMES:
            raise EvidenceError('invalid load kind/outcome')
        w = rec['w']
        if type(w) is not int or not 0 <= w < 10000:
            raise EvidenceError('invalid load worker')
        self.counts[kind][outcome] += 1
        self.total += 1
        self.hashes.setdefault(w, hashlib.sha256()).update(
            (json.dumps(rec, sort_keys=True, separators=(',', ':')) + '\n').encode())

    def snapshot(self):
        return {'total': self.total, 'counts': dict(self.counts),
                'worker_sha256': {str(w): h.hexdigest() for w, h in self.hashes.items()}}


def valid_mid(mid):
    return type(mid) is int and 0 < mid <= 9223372036854775807


class DiskLedger:
    """Disposable on-disk identities; 8 MiB cache; no sequence-indexed allocations."""
    def __init__(self, state_dir, run_id, channels):
        self.run_id, self.channels = run_id, channels
        self.chidx = {c[0]: i for i, c in enumerate(channels)}
        self.tmp = tempfile.TemporaryDirectory(prefix='pr70-ledger-')
        self.db = sqlite3.connect(os.path.join(self.tmp.name, 'ledger.sqlite'))
        self.db.execute('PRAGMA cache_size=-8192')
        self.db.execute('PRAGMA temp_store=FILE')
        self.db.execute('PRAGMA max_page_count=4194304')  # 16 GiB at default 4 KiB pages
        self.db.execute('PRAGMA synchronous=OFF')
        self.db.execute('CREATE TABLE sends(cmn TEXT PRIMARY KEY, outcome TEXT, ch TEXT, frm TEXT, '
                        'payload TEXT, probe TEXT, mid INTEGER)')
        self.db.execute('CREATE UNIQUE INDEX receipt_ids ON sends(mid) WHERE mid IS NOT NULL')
        self.db.execute('CREATE TABLE seen(cmn TEXT PRIMARY KEY, n INTEGER)')
        self.db.execute('CREATE TABLE stored_ids(mid INTEGER PRIMARY KEY)')
        try:
            final = read_json(os.path.join(state_dir, 'world_final.json'))
            f = final['final']
            if f.get('final') is not True or f.get('journal_complete') is not True or \
                    f.get('stuck_workers') != [] or f.get('worker_errors') != []:
                raise EvidenceError('load not finalized successfully')
            if final['header']['config']['run_id'] != run_id:
                raise EvidenceError('final load run identity mismatch')
            summary, last = Summary(), {}
            for rec in read_journal(os.path.join(state_dir, 'load_journal.jsonl.gz')):
                summary.record(rec)
                if rec.get('ch') not in self.chidx:
                    raise EvidenceError('journal channel outside dataset')
                if rec['kind'] != 'message_send':
                    continue
                k = self.load_key(rec['cmn'])
                if k is None or k[1] != last.get(k[0], 0) + 1:
                    raise EvidenceError('duplicate, missing or invalid send client number')
                last[k[0]] = k[1]
                self.insert(rec['cmn'], rec['outcome'], rec['ch'], rec['from'],
                            final['header']['load_payload'], None, rec.get('mid'))
            s = summary.snapshot()
            if not s['total'] or s['total'] != f['journal_records'] or s != f['producer_evidence']:
                raise EvidenceError('journal incomplete or producer identity/hash mismatch')
            counts = {k: v['outcome'] for k, v in f['kinds'].items()}
            if counts != s['counts'] or any(v['n'] != sum(v['outcome'].values()) for v in f['kinds'].values()):
                raise EvidenceError('final per-kind/outcome counters mismatch')
            self.db.commit()
            self.evidence = s
            self.refresh_probes(state_dir)
        except BaseException:
            self.close()
            raise

    def insert(self, cmn, outcome, ch, frm, payload, probe, mid):
        if outcome == OK and not valid_mid(mid):
            raise EvidenceError('invalid acknowledged receipt message ID')
        if mid is not None and not valid_mid(mid):
            raise EvidenceError('invalid receipt message ID')
        self.db.execute('INSERT INTO sends VALUES(?,?,?,?,?,?,?)', (cmn, outcome, ch, frm, payload, probe, mid))

    def refresh_probes(self, state_dir):
        for name in sorted(os.listdir(state_dir)):
            if name.startswith('probes_') and name.endswith('.json'):
                for cmn, v in read_json(os.path.join(state_dir, name)).items():
                    if self.db.execute('SELECT 1 FROM sends WHERE cmn=?', (cmn,)).fetchone():
                        continue
                    self.insert(cmn, v['outcome'], v['ch'], v['from'], v['payload'], v.get('probe'), v.get('mid'))
        self.db.commit()

    def load_key(self, cmn):
        parts = cmn.split('-') if isinstance(cmn, str) and len(cmn) < 128 else []
        if len(parts) == 3 and parts[0] == self.run_id and parts[1][:1] == 's' and \
                parts[1][1:].isdigit() and parts[2].isdigit():
            w, n = int(parts[1][1:]), int(parts[2])
            if 0 <= w < 10000 and 0 < n <= 9223372036854775807:
                return w, n
        return None

    def lookup(self, cmn):
        if not isinstance(cmn, str):
            return None
        v = self.db.execute('SELECT outcome,ch,frm,payload,probe,mid FROM sends WHERE cmn=?', (cmn,)).fetchone()
        return (cmn,) + v if v else None

    def entries(self):
        for cmn, outcome, ch in self.db.execute('SELECT cmn,outcome,ch FROM sends'):
            yield cmn, outcome, ch, cmn

    def close(self):
        self.db.close()
        self.tmp.cleanup()


class DiskSeen:
    def __init__(self, ledger):
        self.db = ledger.db
        self.db.execute('DELETE FROM seen')
        self.db.execute('DELETE FROM stored_ids')

    def bump(self, key):
        n = self.get(key)
        self.db.execute('INSERT OR REPLACE INTO seen VALUES(?,?)', (key, min(255, n + 1)))
        return n

    def get(self, key):
        v = self.db.execute('SELECT n FROM seen WHERE cmn=?', (key,)).fetchone()
        return v[0] if v else 0

    def duplicate_mid(self, mid):
        try:
            self.db.execute('INSERT INTO stored_ids VALUES(?)', (mid,))
            return False
        except sqlite3.IntegrityError:
            return True
