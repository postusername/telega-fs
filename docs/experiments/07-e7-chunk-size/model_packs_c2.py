# Оценка заливки C2 с пакетами (E7, 07-e7-chunk-size.md): python3 -I model_packs_c2.py
import os
import csv, math, random
MiB=1<<20; KiB=1024
rows=[(int(r['size']),int(r['count'])) for r in csv.DictReader(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../../../tools/bench/corpora/c2-sizes.csv')))]
SEG=65536
def enc(p): return 40+p+16*((p+SEG-1)//SEG)
AR=12/60.0; BR=224*MiB/60.0; ALB=8*MiB
tot_files=sum(c for s,c in rows); tot_bytes=sum(s*c for s,c in rows)
print('C2 files',tot_files,'GiB %.1f'%(tot_bytes/2**30))
def model(S,P,thr=4*MiB,inline=4*KiB):
    packed_bytes=0; el=[];sing=[]
    pos_docs=0
    nel=0;nsing=0;B=0
    for s,c in rows:
        if s<=inline: continue
        if s<=thr:
            packed_bytes+=s*c
        else:
            full,rem=divmod(s,S)
            for _ in [0]:
                # chunks per file: full of size S and one remainder
                nfull=full; nrem=1 if rem else 0
            # classify
            if S<=ALB: nel+=c*nfull
            else: nsing+=c*nfull
            if nrem:
                if rem<=ALB: nel+=c
                else: nsing+=c
            B+=c*(nfull*enc(S)+(enc(rem) if rem else 0))
    # packs: count ~ packed_bytes / (P*fill); use fill 0.85*P (files up to thr=4MiB into P)
    fill=0.85 if P/thr>=3 else (0.75 if P/thr>=2 else 0.5)
    npacks=packed_bytes/(P*fill)
    B+=packed_bytes+npacks*(40+16*math.ceil(P*fill/SEG))
    if P<=ALB: nel+=npacks
    else: nsing+=npacks
    A=2*math.ceil(nel/10)+nsing
    t=max(A/AR,B/BR)
    return nel+nsing, A, B/2**30, t/3600, max(A/AR,0)/3600, B/BR/3600
for S,P in ((8,8),(16,16),(32,16),(64,16)):
    d,A,Bg,th,ta,tb=model(S*MiB,P*MiB)
    t_commit=max(Bg*2**30/BR, A/(AR-3/60.0))/3600
    print('S=%d P=%d with commits every 60 s: %.2f h'%(S,P,t_commit))
    print('S=%d P=%d: docs %.0f actions %.0f GiB %.1f time %.2f h (actions %.2f h, bytes %.2f h)  ideal albums'%(S,P,d,A,Bg,th,ta,tb))
