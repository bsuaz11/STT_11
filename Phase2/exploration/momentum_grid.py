"""Exploration on the Week 2 tape only: does a big k-tick move continue, after costs?

    python Phase2/exploration/momentum_grid.py homework4/session_week02_*.jsonl.gz
"""
import gzip,json,collections,sys,pickle,os
import numpy as np
path=sys.argv[1]
snap=collections.defaultdict(dict); ev=[]
for l in gzip.open(path,'rt'):
    if '"book_snapshot"' not in l and '"session_event"' not in l: continue
    r=json.loads(l); m=r['msg']; t=m.get('type')
    if t=='book_snapshot':
        b,a=m['bids'],m['asks']
        snap[m['symbol']][r['tick']]=(b[0][0] if b else np.nan,a[0][0] if a else np.nan,m['mid_price'])
    elif m['event']=='CALENDAR_EVENT': ev.append(r['tick'])
ticks=sorted({t for s in snap.values() for t in s}); t0,t1=ticks[0],ticks[-1]
def arr(s):
    A=np.full((t1-t0+1,3),np.nan)
    for t,v in s.items(): A[t-t0]=v
    return A
fee=30
print('events',[e-t0 for e in ev])
for k in (5,10,20,30,60):
 for th in (40,60,80,120,180):
  for h in (10,20,30,60,120):
    pnl=[];evn=[]
    for sym,s in snap.items():
        if sym=='ARENA10':continue
        A=arr(s);bid,ask,mid=A[:,0],A[:,1],A[:,2]
        i=k;n=len(mid)
        while i<n-h-2:
            if not(mid[i]>0 and mid[i-k]>0): i+=1;continue
            r=(mid[i]/mid[i-k]-1)*1e4
            if abs(r)>=th:
                if r>0: e,x=ask[i+1],bid[i+1+h]; g=(x/e-1)*1e4
                else: e,x=bid[i+1],ask[i+1+h]; g=(e/x-1)*1e4
                if np.isfinite(g):
                    pnl.append(g-2*fee); evn.append(any(0<=i+t0-t<=60 for t in ev))
                i+=h+1
            else:i+=1
    if len(pnl)>=5:
        p=np.array(pnl);E=np.array(evn)
        print(f'k={k:3d} th={th:4d} h={h:4d} n={len(p):4d} net={p.mean():+7.1f} hit={np.mean(p>0):.2f} t={p.mean()/(p.std(ddof=1)/len(p)**.5):+5.1f} | event n={E.sum():3d} net={p[E].mean() if E.any() else 0:+7.1f} | quiet n={(~E).sum():3d} net={p[~E].mean() if (~E).any() else 0:+7.1f}')
