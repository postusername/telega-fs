# Оценка заливки набора C3 с пакетами (E7, 07-e7-chunk-size.md): python3 -I model_packs_c3.py
import os
import csv, statistics as st
MiB=1<<20
sizes=[int(r['size_bytes']) for r in csv.DictReader(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../../../tools/bench/corpora/c3-sizes.csv')))]
SEG=65536
def enc(p): return 40+p+16*((p+SEG-1)//SEG)
AR=12/60.0; BR=224*MiB/60.0   # actions/s, bytes/s
ALB=8*MiB
def small_set(limit=1024*MiB):
    out=[];s=0
    for x in sizes:
        if s+x>limit: break
        s+=x; out.append(x)
    return out
def chunks_of(size,S):
    n=(size+S-1)//S
    return [min(S,size-i*S) for i in range(n)]
def group_inorder(docs):
    # docs: list of plain sizes in emission order -> list of jobs (list of plain sizes)
    jobs=[];cur=[]
    for d in docs:
        if d<=ALB:
            cur.append(d)
            if len(cur)==10: jobs.append(cur);cur=[]
        else:
            if cur: jobs.append(cur);cur=[]
            jobs.append([d])
    if cur: jobs.append(cur)
    return jobs
def group_ideal(docs):
    el=[d for d in docs if d<=ALB]; big=[d for d in docs if d>ALB]
    jobs=[el[i:i+10] for i in range(0,len(el),10)]+[[d] for d in big]
    return jobs
def cost(jobs):
    A=sum(2 if len(j)>1 else 1 for j in jobs); B=sum(enc(d) for j in jobs for d in j)
    t=max(A/AR,B/BR)
    return A,B,t
def docs_nopack(files,S):
    return [c for f in files for c in chunks_of(f,S)]
def docs_pack(files,S,P,thr=4*MiB):
    out=[];cur=0
    for f in files:
        if f<=thr:
            if cur and cur+f>P:
                out.append(cur);cur=0
            cur+=f
        else:
            out+=chunks_of(f,S)
    if cur: out.append(cur)
    return out
small=small_set()
print('small files',len(small),sum(small)/MiB,'<=4MiB:',sum(1 for x in small if x<=4*MiB),sum(x for x in small if x<=4*MiB)/MiB)
big={S:[S]*(1024*MiB//S) for S in (8*MiB,16*MiB,32*MiB,64*MiB)}
def report(name,docs,bigdocs,grouper):
    jobs=grouper(docs); A,B,t=cost(jobs)
    jb=grouper(bigdocs); Ab,Bb,tb=cost(jb)
    plain=sum(docs)/MiB
    print('%-34s small: docs %3d actions %3d time %5.0fs  %.2f MiB/s | big %.0fs %.2f | total 2GiB: %.2f MiB/s, msgs %d (%.0f/GiB)'%(name,len(docs),A,t,plain/t,tb,1024/tb,(plain+1024)/(t+tb),len(docs)+len(bigdocs),(len(docs)+len(bigdocs))/((plain+1024)/1024)))
    return plain/t,(plain+1024)/(t+tb)
print('--- validation vs measured (no packs, in-order)')
for S in (8,16,32,64):
    report('nopack S=%d'%S,docs_nopack(small,S*MiB),big[S*MiB],group_inorder)
print('--- with packs (threshold 4 MiB), in-order albums')
for S,P in ((8,8),(8,4),(16,16),(16,8),(32,16),(64,16)):
    report('S=%d pack_target=%d'%(S,P),docs_pack(small,S*MiB,P*MiB),big[S*MiB],group_inorder)
print('--- with packs, ideal album grouping')
for S,P in ((8,8),(16,16),(16,8),(32,16),(64,16)):
    report('S=%d pack_target=%d'%(S,P),docs_pack(small,S*MiB,P*MiB),big[S*MiB],group_ideal)
print('--- all 500 files (2.7 GiB) with packs, in-order')
allf=sizes
for S,P in ((8,8),(16,16),(32,16),(64,16)):
    d=docs_pack(allf,S*MiB,P*MiB); j=group_inorder(d); A,B,t=cost(j); print('S=%d P=%d docs %d actions %d %.2f MiB/s'%(S,P,len(d),A,sum(d)/MiB/t))
# distribution of files
import collections
print('share of bytes >4MiB (all 500):', sum(x for x in sizes if x>4*MiB)/sum(sizes), 'files >4MiB',sum(1 for x in sizes if x>4*MiB))
print('files >8MiB',sum(1 for x in small if x>8*MiB),'in small set of',len(small))
