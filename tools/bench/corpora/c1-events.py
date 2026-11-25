#!/usr/bin/env python3
"""События набора C1 «Исходники» для tgfs-sim.

Читает историю git репозитория TDLib (`git -C td ...`), рабочую копию не трогает:
только `rev-list` и `ls-tree`. Десять коммитов берутся из истории первого родителя
`ba976cb4a` с шагом 200: `ba976cb4a~0, ~200, ..., ~1800`.

Выход — CSV `step,op,id,size`:
  step  номер переключения (0 — первая распаковка рабочей копии);
  op    `w` — файл записан целиком (создан или содержимое изменилось), `d` — удалён;
  id    номер пути (имён в CSV нет), стабилен внутри одного файла;
  size  размер файла, для `d` — 0.
Как и настоящий `git checkout`, переключение переписывает только файлы, у которых изменился
blob (смена режима без смены содержимого — это chmod, не запись), и удаляет исчезнувшие.
Символические ссылки считаются файлами длиной в цель ссылки, подмодули пропускаются.

Использование: c1-events.py <каталог td> <каталог вывода>
Получаются c1.csv (только ba976cb4a), c1-switch.csv (от старого коммита к новому) и
c1-switch-rev.csv (от нового к старому).
"""
import subprocess
import sys

HEAD = "ba976cb4aba53a82eff425db2f85e2741774c944"
STEP = 200
COUNT = 10


def git(repo, *args):
    return subprocess.run(["git", "-C", repo, *args], check=True, stdout=subprocess.PIPE).stdout


def tree(repo, commit):
    """path -> (oid, size) для всех файлов и ссылок коммита."""
    out = git(repo, "ls-tree", "-r", "-l", "-z", commit)
    files = {}
    for rec in out.split(b"\0"):
        if not rec:
            continue
        meta, path = rec.split(b"\t", 1)
        mode, kind, oid, size = meta.split()
        if kind != b"blob":
            continue  # подмодули (commit)
        files[path] = (oid, int(size))
    return files


def main():
    repo, outdir = sys.argv[1], sys.argv[2]
    history = git(repo, "rev-list", "--first-parent", HEAD).decode().split()
    commits = [history[i * STEP] for i in range(COUNT)]  # от новейшего к старому
    trees = [tree(repo, c) for c in commits]
    ids = {}

    def pid(path):
        return ids.setdefault(path, len(ids) + 1)

    def write(name, seq):
        rows = []
        prev = {}
        for step, t in enumerate(seq):
            for path in sorted(t):
                oid, size = t[path]
                if prev.get(path, (None,))[0] != oid:
                    rows.append((step, "w", pid(path), size))
            for path in sorted(prev):
                if path not in t:
                    rows.append((step, "d", pid(path), 0))
            prev = t
        with open(f"{outdir}/{name}", "w") as f:
            f.write("step,op,id,size\n")
            for r in rows:
                f.write("%d,%s,%d,%d\n" % r)
        per = {}
        for s, op, _, size in rows:
            c = per.setdefault(s, [0, 0, 0, 0])
            c[0 if op == "w" else 2] += 1
            c[1 if op == "w" else 3] += size
        print(name, {s: tuple(v) for s, v in per.items()})

    write("c1.csv", [trees[0]])
    write("c1-switch.csv", list(reversed(trees)))
    write("c1-switch-rev.csv", trees)
    print("commits:", [c[:9] for c in commits])


main()
