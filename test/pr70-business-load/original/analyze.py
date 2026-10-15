#!/usr/bin/env python3
"""Aggregate all XIN-1968 artifacts into final evidence numbers."""
import json, re, collections, sys

RUN="/tmp/repro_run.jsonl"; MET="/tmp/wk_metrics.jsonl"; STATS="/tmp/wk_stats.jsonl"; LOG="/tmp/wk_full.log"

def load_jsonl(p):
    out=[]
    try:
        for line in open(p):
            line=line.strip()
            if line:
                try: out.append(json.loads(line))
                except: pass
    except FileNotFoundError: pass
    return out

def classify(reason):
    r=reason
    if 'stale_revision' in r: return 'fence_stale_revision_409'
    if 'revision_conflict' in r: return 'fence_revision_conflict_409'
    if 'managed_channel' in r: return 'fence_managed_channel_409'
    if 'did not complete' in r or ('503' in r and 'pending' in r): return 'BUDGET_EXHAUSTED_503'
    if 'HTTP 503' in r: return 'other_503'
    if 'deadline' in r.lower(): return 'context_deadline'
    if 'timed out' in r.lower() or 'timeout' in r.lower(): return 'client_socket_timeout'
    if 'backlog_full' in r or '429' in r: return 'backlog_full_429'
    m=re.search(r'HTTP \d+', r); return (m.group(0) if m else r[:30])

def main():
    run=load_jsonl(RUN); met=load_jsonl(MET); stats=load_jsonl(STATS)
    final=None
    for d in run:
        if d.get('final'): final=d
    if final is None and run: final=run[-1]
    print("=== WORKLOAD FINAL (elapsed %ss) ==="%final.get('elapsed'))
    for k,v in final['kinds'].items():
        print("  %-20s n=%-8d fail=%-6d slow=%-6d timeout=%-4d p50=%.3f p95=%.3f p99=%.3f max=%.3f"%(
            k,v['n'],v['fail'],v['slow'],v['timeout'],v['p50'],v['p95'],v['p99'],v['max']))
    tot=sum(v['n'] for v in final['kinds'].values())
    print("  TOTAL requests=%d  throughput=%.0f/s"%(tot, tot/max(1,final['elapsed'])))
    # failure classification across all samples
    cat=collections.Counter(); worst={}
    for d in run:
        for s in d.get('fail_samples',[]):
            t,kind,dt,reason=s; c=classify(reason); cat[c]+=1
            if c not in worst or dt>worst[c][0]: worst[c]=(dt,reason[:160])
    print("\n=== FAILURE SAMPLE CLASSIFICATION (sampled, maxlen deque) ===")
    for c,n in cat.most_common(): print("  %-28s %d   worst_dt=%.2fs"%(c,n,worst[c][0]))
    print("  (exemplars)")
    for c,(dt,r) in worst.items(): print("   [%s] %s"%(c,r))
    # metrics deltas
    if met:
        m0=met[0].get('metrics',{}); m1=met[-1].get('metrics',{})
        def d(k): return (m1.get(k,0) or 0)-(m0.get(k,0) or 0)
        print("\n=== SERVER METRIC DELTAS (start->end) ===")
        for k in ['cluster_channel_propose_count_total','cluster_channel_propose_failed_count_total',
                  'cluster_channel_propose_latency_over_500ms_total','cluster_channel_propose_latency_under_500ms_total',
                  'db_commit_count_total','db_wal_bytes_written','db_compact_total_count']:
            print("  %-48s +%d (end=%s)"%(k, d(k), m1.get(k)))
        over=d('cluster_channel_propose_latency_over_500ms_total'); under=d('cluster_channel_propose_latency_under_500ms_total')
        if over+under>0: print("  propose >500ms share: %.3f%%"%(100.0*over/(over+under)))
        # recovery pending peak + drain
        peak=0; last_nonzero_t=None; first_t=met[0]['t']
        for r in met:
            rec=r.get('recovery') or {}; p=rec.get('pending_units') or 0
            rt=rec.get('runtime') or {}; infl=(rt.get('target_inflight') or 0)
            peak=max(peak,p,infl)
            if p>0: last_nonzero_t=r['t']
        print("  recovery pending/inflight PEAK=%d"%peak)
        print("  goroutine end=%s"%(met[-1].get('varz',{}) or {}).get('goroutine'))
    # resource curve
    cpu=[]; mem=[]
    for line in open(STATS) if True else []:
        pass
    try:
        for line in open(STATS):
            parts=line.split()
            if len(parts)>=2 and parts[1].endswith('%'):
                cpu.append(float(parts[1].rstrip('%')))
            # mem like 700.8MiB
            m=re.search(r'([\d.]+)MiB',line)
            if m: mem.append(float(m.group(1)))
            m2=re.search(r'([\d.]+)GiB / 4GiB',line)
            if m2: mem.append(float(m2.group(1))*1024)
    except FileNotFoundError: pass
    if cpu: print("\n=== RESOURCE (docker stats) ===\n  CPU%% avg=%.0f max=%.0f (limit 200)  MEM MiB avg=%.0f max=%.0f (limit 4096)"%(
        sum(cpu)/len(cpu),max(cpu),sum(mem)/len(mem) if mem else 0,max(mem) if mem else 0))
    # log timeout counts
    try:
        txt=open(LOG).read()
        print("\n=== SERVER LOG ===")
        print("  'propose batch until applied timeout' count = %d"%txt.count('propose batch until applied timeout'))
        for msg in ['did not complete','context deadline','apply timeout','recovery']:
            print("  '%s' lines = %d"%(msg, txt.count(msg)))
    except FileNotFoundError: pass

if __name__=="__main__": main()
