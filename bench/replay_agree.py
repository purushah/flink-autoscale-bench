import json, collections, sys
def summarize(path):
    rows=[json.loads(l) for l in open(path)]
    st=collections.Counter(r['status'] for r in rows)
    agree=collections.Counter(); dev=collections.Counter(); app=collections.Counter(); bycat=collections.defaultdict(collections.Counter); detail=collections.Counter()
    for r in rows:
        if r['status']!='DECIDED': continue
        for v in r['vertices'].values():
            if v['choice'] is None: continue
            c=int(v['choice'][1:]); rc=v['rule_choice']; cur=v['current']
            cat='up' if rc>cur else 'down' if rc<cur else 'hold'
            a=(c==rc); agree[a]+=1; bycat[cat][a]+=1; app[v['target']==rc]+=1
            if not a:
                dev[(cat, 'higher' if c>rc else 'lower')]+=1
                detail[(v['name'][:9], f"rule {cur}->{rc}", f"model {c}", v['offered'], f"trig {v['rule_trigger']} size {v['rule_size']}")]+=1
    n=sum(agree.values())
    print(path.split('/')[-1], len(rows), dict(st), [x for x in collections.Counter((r.get('reason') or '')[:50] for r in rows if r['status']!='DECIDED').most_common(3)])
    print(f"  choice == rule: {agree[True]}/{n} = {agree[True]/max(n,1):.3f}; applied == rule: {app[True]}/{n}")
    print('  by rule category:', {k:(v[True], v[True]+v[False]) for k,v in bycat.items()})
    print('  deviations:', dict(dev))
    for k,c in detail.most_common(12): print('   ', c, k)
    lat=[r['latency_ms'] for r in rows if r.get('latency_ms')]
    if lat: print('  latency ms mean %.0f max %d' % (sum(lat)/len(lat), max(lat)))
for p in sys.argv[1:]: summarize(p)
