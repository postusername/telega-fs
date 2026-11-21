#!/usr/bin/env python3
"""Таблицы отчёта E9 из сырых CSV tgfs-sim: медиана [мин..макс] по seed.

Использование: summarize-e9.py <каталог с e9_sim_*.csv> <таблица> [аргументы]
Таблицы: msgs, dead, age, gap, speed, fsync, inline, time (см. main)."""
import csv
import statistics as st
import sys

KEYS = ["dataset", "scenario", "config", "threshold", "target", "max_age_s", "inline_max",
        "inline_budget", "limits", "step_gap_s", "files_per_s", "fsync_prob"]


def load(path):
    groups = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            key = tuple(row[k] for k in KEYS)
            groups.setdefault(key, []).append(row)
    return groups


def med(rows, col):
    return st.median(float(r[col]) for r in rows)


def rng(rows, col):
    v = [float(r[col]) for r in rows]
    return min(v), max(v)


def fmt(x, nd=0):
    if nd == 0:
        return f"{x:,.0f}".replace(",", " ")
    return f"{x:.{nd}f}".replace(".", ",")


def cfg(groups, **want):
    for key, rows in groups.items():
        d = dict(zip(KEYS, key))
        if all(d[k] == str(v) for k, v in want.items()):
            return rows
    raise KeyError(want)


def main():
    d, table = sys.argv[1], sys.argv[2]
    args = sys.argv[3:]
    if table == "msgs":  # <файл> <age>: лимиты 10.2 включены, время и без них
        g = load(f"{d}/e9_sim_{args[0]}.csv")
        age = args[1]

        def row_of(name, thr=None, target=None, lim="on"):
            for key, rows in g.items():
                if (key[2] == name and key[8] == lim and (name != "packs" or
                        (key[3] == thr and key[4] == target and key[5] == age))):
                    return rows
            raise KeyError((name, thr, target, lim))

        base = med(row_of("nopack-noinline"), "docs_total")
        print("| pack_threshold | pack_target | чанки | пакеты | блобы индекса | pin+unpin | документов | действий | к «файл = чанк» | время с лимитами, с | без лимитов, с |")
        print("|---|---|---|---|---|---|---|---|---|---|---|")

        def line(label_a, label_b, r, r_off):
            docs = med(r, "docs_total")
            print(f"| {label_a} | {label_b} | {fmt(med(r,'docs_chunk'))} | {fmt(med(r,'docs_pack'))} | {fmt(med(r,'docs_blob_delta')+med(r,'docs_blob_snapshot'))} | {fmt(med(r,'pins')+med(r,'unpins'))} | {fmt(docs)} | {fmt(med(r,'actions_total'))} | {fmt(docs/base*100,1)} % | {fmt(med(r,'t_done_s'))} | {fmt(med(r_off,'t_done_s'))} |")

        for name, text in (("nopack-noinline", "каждый файл — чанк"), ("nopack", "инлайн без пакетов")):
            line(text, "", row_of(name), row_of(name, lim="off"))
        for key, r in g.items():
            if key[2] == "packs" and key[8] == "on" and key[5] == age:
                line(key[3], key[4], r, row_of("packs", key[3], key[4], "off"))
    elif table == "dead":  # <файл> <age>
        g = load(f"{d}/e9_sim_{args[0]}.csv")
        print("| pack_threshold | pack_target | пакетов (залито) | пакетов жив | доля мёртвого | [мин..макс] | уплотняемых | мёртвое при закрытии, МиБ | пакетов целиком мёртвых |")
        print("|---|---|---|---|---|---|---|---|---|")
        for key, r in g.items():
            if key[2] == "packs" and key[8] == "on" and key[5] == args[1]:
                lo, hi = rng(r, "dead_share")
                print(f"| {key[3]} | {key[4]} | {fmt(med(r,'packs'))} | {fmt(med(r,'packs_alive'))} | {fmt(med(r,'dead_share')*100,1)} % | {fmt(lo*100,1)}..{fmt(hi*100,1)} | {fmt(med(r,'packs_compactable'))} | {fmt(med(r,'dead_at_close_bytes')/1048576,1)} | {fmt(med(r,'packs_all_dead'))} |")
    elif table == "age":  # <файл> <порог>
        g = load(f"{d}/e9_sim_{args[0]}.csv")
        print("| pack_target | pack_max_age, с | пакетов | по размеру | по возрасту | по fsync | при размонтировании | недобранных (возраст+fsync) | то же с размонтированием |")
        print("|---|---|---|---|---|---|---|---|---|")
        for key, r in g.items():
            if key[2] == "packs" and key[8] == "on" and key[3] == args[1]:
                lo, hi = rng(r, "undersized_share")
                print(f"| {key[4]} | {key[5]} | {fmt(med(r,'packs'))} | {fmt(med(r,'packs_by_size'))} | {fmt(med(r,'packs_by_age'))} | {fmt(med(r,'packs_by_fsync'))} | {fmt(med(r,'packs_by_unmount'))} | {fmt(med(r,'undersized_share')*100)} % [{fmt(lo*100)}..{fmt(hi*100)}] | {fmt(med(r,'undersized_share_with_unmount')*100)} % |")
    elif table in ("gap", "speed", "fsync"):  # матрица: age x параметр, по target
        col = {"gap": "step_gap_s", "speed": "files_per_s", "fsync": "fsync_prob"}[table]
        g = load(f"{d}/e9_sim_{args[0]}.csv")
        vals = sorted({k[KEYS.index(col)] for k in g}, key=float)
        for target in ("4MiB", "16MiB", "64MiB"):
            print(f"\n**pack_target {target}**: доля недобранных (возраст+fsync), %; строки — pack_max_age, столбцы — {col}\n")
            print("| pack_max_age, с | " + " | ".join(vals) + " |")
            print("|---|" + "---|" * len(vals))
            for age in ("10", "30", "120"):
                cells = []
                for v in vals:
                    r = cfg(g, config="packs", target=target, max_age_s=age, **{col: v})
                    lo, hi = rng(r, "undersized_share")
                    cells.append(f"{fmt(med(r,'undersized_share')*100)} ({fmt(med(r,'packs'))})")
                print(f"| {age} | " + " | ".join(cells) + " |")
    elif table == "inline":  # <c1|c2>
        print("| inline_budget | inline_max | инлайн-файлов | инлайн, МиБ | файлов в пакетах | документов | из них пакетов | блобы индекса, МиБ | время с лимитами, с |")
        print("|---|---|---|---|---|---|---|---|---|")
        for budget in ("4MiB", "16MiB", "64MiB"):
            g = load(f"{d}/e9_sim_{args[0]}_inline_{budget}.csv")
            for key, r in g.items():
                print(f"| {budget} | {key[6]} | {fmt(med(r,'files_inline'))} | {fmt(med(r,'inline_bytes')/1048576,1)} | {fmt(med(r,'files_pack'))} | {fmt(med(r,'docs_total'))} | {fmt(med(r,'docs_pack'))} | {fmt(med(r,'bytes_blobs')/1048576,1)} | {fmt(med(r,'t_done_s'))} |")
    elif table == "commit":  # <c1|c2>
        print("| commit_interval, с | pack_threshold | коммитов | из них снимков | документов | действий | время с лимитами, с |")
        print("|---|---|---|---|---|---|---|")
        for ci in (30, 60, 300, 600):
            g = load(f"{d}/e9_sim_{args[0]}_commit_{ci}.csv")
            for key, r in g.items():
                print(f"| {ci} | {key[3]} | {fmt(med(r,'commits'))} | {fmt(med(r,'docs_blob_snapshot'))} | {fmt(med(r,'docs_total'))} | {fmt(med(r,'actions_total'))} | {fmt(med(r,'t_done_s'))} |")
    else:
        raise SystemExit("unknown table")


main()
