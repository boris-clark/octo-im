#!/usr/bin/env python3
"""Instrumentation poller for XIN-1968: scrape WukongIM /metrics (manager port),
/varz (api port) and the subscriber-recovery runtime every INTERVAL seconds into
a JSONL, plus a compact set of Raft/Pebble/WAL series. Timestamp-correlated."""
import json, re, sys, time, http.client

API_HOST, API_PORT = "127.0.0.1", 15001
MGR_HOST, MGR_PORT = "127.0.0.1", 15300
INTERVAL = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0
OUT = sys.argv[2] if len(sys.argv) > 2 else "/tmp/wk_metrics.jsonl"

# series we care about (prefix match on the metric name before '{')
KEEP = re.compile(r'^(cluster_channel_propose_(count|failed_count|latency_(over|under)_500ms)_total'
                  r'|cluster_(channel|slot)_propose_latency_(sum|count)'
                  r'|cluster_channel_active_count'
                  r'|db_commit_count_total'
                  r'|db_wal_(bytes_written|size|physical_size|files_count|fsync[a-z_]*)'
                  r'|db_compact_(num_in_progress|total_count|estimated_debt|in_progress_bytes)'
                  r'|db_alllevel_(bytes_compacted|compact_score)'
                  r'|db_memtable[a-z_]*'
                  r'|db_flush[a-z_]*'
                  r'|go_goroutines|go_memstats_heap_inuse_bytes|process_resident_memory_bytes'
                  r'|process_cpu_seconds_total)')

def get(host, port, path):
    c = http.client.HTTPConnection(host, port, timeout=8)
    try:
        c.request("GET", path); r = c.getresponse(); return r.status, r.read()
    finally:
        c.close()

def parse_metrics(txt):
    out = {}
    for line in txt.splitlines():
        if not line or line[0] == '#':
            continue
        name = line.split('{', 1)[0].split(' ', 1)[0]
        if not KEEP.match(name):
            continue
        try:
            val = float(line.rsplit(' ', 1)[1])
        except Exception:
            continue
        key = name
        if '{' in line:  # keep label-less aggregate; sum duplicates
            key = name
        out[key] = out.get(key, 0.0) + val
    return out

def main():
    f = open(OUT, "w")
    while True:
        rec = {"t": round(time.time(), 3)}
        try:
            st, b = get(MGR_HOST, MGR_PORT, "/metrics")
            rec["metrics"] = parse_metrics(b.decode("utf-8", "replace")) if st == 200 else {"http": st}
        except Exception as e:
            rec["metrics_err"] = str(e)
        try:
            st, b = get(API_HOST, API_PORT, "/varz")
            if st == 200:
                v = json.loads(b)
                rec["varz"] = {k: v.get(k) for k in ("goroutine","mem","cpu","connections",
                              "in_msgs","out_msgs","in_bytes","out_bytes","slow_clients","retry_queue")}
        except Exception as e:
            rec["varz_err"] = str(e)
        try:
            st, b = get(API_HOST, API_PORT, "/channel/subscriber_recovery")
            if st == 200:
                d = json.loads(b).get("data", {})
                rec["recovery"] = {"pending_units": d.get("pending_units"), "runtime": d.get("runtime")}
        except Exception as e:
            rec["recovery_err"] = str(e)
        f.write(json.dumps(rec) + "\n"); f.flush()
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
