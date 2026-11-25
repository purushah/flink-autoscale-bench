#!/usr/bin/env python3
"""Cluster signal campaign: what the rule believed at its first evaluation in a range of seconds after the
rise, against the truth from the harness samples. From 60 s on the one-minute window lies wholly inside
the overload. Use 60 to 480 for the eight-minute overload: with 60 to 125, as first run, two of the
eight default-signal jobs have no evaluation in the range (theirs come at 163 and 164 s, at 5 and 6
subtasks), and the paper said "six of eight runs" until 24 Nov (ISSUES.md #185).
Written by a reviewer in round 3 and checked in round 4; not a planned analysis.
   python3 beliefs.py <campaign dir, e.g. results-cluster/signal> <scenario> <from s> <to s>"""
import json, os, sys, csv, datetime, collections, bisect
ROOT=sys.argv[1]
scen=sys.argv[2]; lo=float(sys.argv[3]); hi=float(sys.argv[4])
names={'bench-builtin':'builtin','bench-builtin-max70':'builtin+max70','bench-builtin-lagfix':'builtin+lagfix','bench-builtin-max70-lagfix':'builtin+max70+lagfix'}
agg=collections.defaultdict(list)
for rep in sorted(os.listdir(os.path.join(ROOT,scen))):
    p=os.path.join(ROOT,scen,rep)
    meta=json.load(open(os.path.join(p,'meta.json')))
    t0=meta['t0_epoch']; rise=meta['events'][0]
    samp=collections.defaultdict(list)
    for r in csv.DictReader(open(os.path.join(p,'samples.csv'))):
        try: samp[r['job']].append((float(r['t']), float(r['end_offset'])-float(r['position_offset']), r['p_aggregate']))
        except: pass
    load=[(float(r['t']),float(r['target_rate'])) for r in csv.DictReader(open(os.path.join(p,'load.csv')))]
    for line in open(os.path.join(p,'decisions.jsonl')):
        try: d=json.loads(line)
        except: continue
        if d.get('event')!='decision': continue
        ts=d['ts']
        dt=datetime.datetime.strptime(ts[:19],'%Y-%m-%dT%H:%M:%S').replace(tzinfo=datetime.timezone.utc).timestamp()+float('0.'+ts[20:].rstrip('Z')[:6])
        t=dt-t0; ta=t-rise
        if ta<lo or ta>hi: continue
        j=names[d['job'].split("name='")[1].split("'")[0]]
        for v in d['vertices'].values():
            if v['name']!='Aggregate': continue
            s=samp[j]; i=min(range(len(s)), key=lambda k: abs(s[k][0]-t))
            tb=s[i][1]
            agg[j].append((rep,round(ta),v['current'],v['demand'],v['backlog'],tb))
for j in agg:
    xs=agg[j]
    # first evaluation per rep in the range, while still at <= 6 subtasks
    first={}
    for x in xs:
        if x[0] not in first: first[x[0]]=x
    dem=[100*x[3]/4800 for x in first.values()]; bl=[100*x[4]/x[5] for x in first.values() if x[5]>0]
    print(j,'n',len(first),'demand believed %% of 4800: %.0f..%.0f (mean %.0f)'%(min(dem),max(dem),sum(dem)/len(dem)),'backlog believed %% of true: %.0f..%.0f (mean %.0f)'%(min(bl),max(bl),sum(bl)/len(bl)))
    for x in first.values(): print('     ',x[0],'t+%d'%x[1],'p',x[2],'demand %.0f'%x[3],'backlog %.0f'%x[4],'true %.0f'%x[5])
