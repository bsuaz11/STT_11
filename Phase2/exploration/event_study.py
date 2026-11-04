"""Exploration that shaped the hypothesis: the book around every print on a tape.

    python Phase2/exploration/event_study.py <tape.jsonl.gz> ...
"""
import gzip,json,collections,sys
import numpy as np
for path in sys.argv[1:]:
    snap=collections.defaultdict(dict); ev=[]; tr=[]
    for l in gzip.open(path,'rt'):
        if '"book_snapshot"' in l or '"session_event"' in l or '"trade_execution"' in l:
            r=json.loads(l); m=r['msg']; t=m.get('type')
            if t=='book_snapshot':
                b,a=m['bids'],m['asks']
                snap[m['symbol']][r['tick']]=(b[0][0] if b else None,a[0][0] if a else None,m['mid_price'],m.get('ref_price'),sum(x[1] for x in b),sum(x[1] for x in a))
            elif t=='session_event' and m['event']=='CALENDAR_EVENT':
                d=m['data']; ev.append((r['tick'],d.get('kind'),d['pct'],d.get('ramp_ticks'),d.get('overshoot'),d.get('affected')))
            elif t=='trade_execution' and tr is not None:
                tr.append((r['tick'],m['symbol'],m['quantity'],m['price'],m.get('fee'),m.get('maker_rebate')))
    t=[x for x in tr if x[4]]; print(path,'fee/notional',np.median([x[4]/(x[2]*x[3]) for x in t]),'rebate/notional',np.median([(x[5] or 0)/(x[2]*x[3]) for x in t]))
    for e in ev:
        print('EVENT',e[:5],'n_affected',len(e[5]))
        for sym in e[5][:1 if len(e[5])>1 else 1]:
            if sym=='ARENA10':continue
            s=snap[sym]; base=None
            for k in range(-3,0):
                if e[0]+k in s and s[e[0]+k][2]: base=s[e[0]+k][2]
            print(' ',sym,'base',base,'  rel: mid% bid% ask% ref% bidsz asksz')
            for k in (-1,0,1,2,3,4,5,6,8,10,12,15,18,20,23,25,30,40,60):
                v=s.get(e[0]+k)
                if v:
                    f=lambda x:(f'{(x/base-1)*100:+6.2f}' if x else '  None')
                    print(f'   t{k:+3d} {f(v[2])} {f(v[0])} {f(v[1])} {f(v[3])} {v[4]:6.0f} {v[5]:6.0f}')
