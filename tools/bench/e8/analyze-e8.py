#!/usr/bin/env python3
"""E8: сводка измерений индекса, расчёт времени заливки и критерия решения.

Использование: python3 -I analyze-e8.py <каталог CSV> [--tag-suffix text]
Читает e8_build_*.csv, e8_snapshots_*.csv, e8_deltas_*.csv (tgfs-e8 index) и
e8_sim_*_summary.csv (run-e8.sh); пишет e8_summary.csv, e8_sim_saving.csv и печатает таблицы.

Время заливки НЕ замер в Telegram, а расчёт по данным E5:
  upload_s = blob / 14 МиБ/с          (один поток, E5: 13,6 МиБ/с при параллельности 1)
  bucket_s = blob / 3,733 МиБ/с       (send_bytes_rate 224 МиБ/мин: сколько байтовый бакет
                                       «занят» блобом; во столько же блоб задержит чанки
                                       при насыщенной очереди)
"""
import csv
import glob
import os
import statistics
import sys
from collections import defaultdict

MIB = 1024 * 1024
RATE = 14 * MIB
BUCKET = 224 * MIB / 60
INTERVAL = 60.0
LIMIT = 0.10 * INTERVAL  # 10 % от commit_interval
MAX_CHAIN = 16
RATIO = 0.25
FIXED = 1.1  # CAS 0,3 + документ 0,2 + pin 0,3 + unpin 0,3 (модель tgfs-sim, E9)
W_TYPICAL = 100  # правка 100 инлайн-файлов (как в E1)
BASE_DELTA = (600, 0.005)  # дельта без инлайна (rename/touch, E1): блоб, локально с


def parse_bytes(label):
    if label == "-":
        return 0
    for suf, mul in (("KiB", 1024), ("MiB", MIB)):
        if label.endswith(suf):
            return int(label[:-3]) * mul
    return int(label)


def med(xs):
    return statistics.median(xs)


def read_all(d, kind, suffix):
    rows = []
    for p in sorted(glob.glob(os.path.join(d, f"e8_{kind}_*.csv"))):
        name = os.path.basename(p)
        if kind in ("build", "snapshots", "deltas") and name.startswith(f"e8_{kind}_"):
            tag = name[len(f"e8_{kind}_") : -4]
            is_text = tag.startswith("text-")
            if is_text != bool(suffix):
                continue
        with open(p, newline="") as f:
            rows.extend(csv.DictReader(f))
    return rows


def key(r):
    return (r["dataset"], r["content"], r["inline_max"], r["inline_budget"])


def main():
    d = sys.argv[1]
    suffix = "text" if "--tag-suffix" in sys.argv else ""
    build = {key(r): r for r in read_all(d, "build", suffix)}
    snaps = defaultdict(list)
    for r in read_all(d, "snapshots", suffix):
        snaps[key(r)].append(r)
    deltas = defaultdict(list)
    for r in read_all(d, "deltas", suffix):
        deltas[key(r) + (int(r["files_rewritten"]),)].append(r)

    out = []
    base_snap = {}
    for k in build:
        if k[2] == "0":
            base_snap[k[0]] = med([int(r["blob_bytes"]) for r in snaps[k]])
    for k, b in build.items():
        ds, content, im, bd = k
        sn = snaps[k]
        blob = [int(r["blob_bytes"]) for r in sn]
        local = [
            (int(r["vacuum_ns"]) + int(r["compress_ns"]) + int(r["encrypt_ns"])) / 1e9
            for r in sn
        ]
        open_s = med([int(r["open_ns"]) / 1e9 for r in sn])
        S = med(blob)
        local_s = med(local)
        inline_bytes = int(b["inline_bytes"])
        budget = parse_bytes(bd)
        # Оценка снимка при заполненном бюджете: добавка на каждый инлайн-байт — по этому же набору.
        full = budget > 0 and inline_bytes < 0.97 * budget and inline_bytes > 0
        per_byte = (S - base_snap.get(ds, S)) / inline_bytes if inline_bytes else 0
        S_full = S + (budget - inline_bytes) * per_byte if full else S
        # Типичная дельта: правка 100 инлайн-файлов (или всех, если их меньше).
        dkeys = [x for x in deltas if x[:4] == k]
        dsel = None
        if dkeys:
            want = min(W_TYPICAL, int(b["inline_files"]))
            dsel = min(dkeys, key=lambda x: abs(x[4] - want))
        dd = deltas[dsel] if dsel else []
        if dd:
            dblob = med([int(r["blob_bytes"]) for r in dd])
            dloc = med(
                [
                    (int(r["take_locked_ns"]) + int(r["compress_ns"]) + int(r["encrypt_ns"])) / 1e9
                    for r in dd
                ]
            )
        else:
            dblob, dloc = BASE_DELTA

        def avg(Sx, extra):
            kk = min(MAX_CHAIN, int(RATIO * Sx / dblob) + 1)
            sc = local_s + Sx / RATE + extra
            dc = dloc + dblob / RATE + extra
            sb = local_s + Sx / BUCKET
            db_ = dloc + dblob / BUCKET
            return kk, (sc + kk * dc) / (kk + 1), (sb + kk * db_) / (kk + 1)

        kk, A, B = avg(S, 0)
        kkf, Af, Bf = avg(S_full, 0)
        row = {
            "dataset": ds, "content": content, "inline_max": im, "inline_budget": bd,
            "files": b["files"], "inline_files": b["inline_files"],
            "inline_mib": f"{inline_bytes / MIB:.2f}", "inline_overflow": b["inline_overflow"],
            "budget_full": "yes" if (budget and inline_bytes >= 0.97 * budget) else "no",
            "snap_raw_mib": f"{med([int(r['raw_bytes']) for r in sn]) / MIB:.2f}",
            "snap_blob_mib": f"{S / MIB:.2f}",
            "snap_blob_min_mib": f"{min(blob) / MIB:.2f}", "snap_blob_max_mib": f"{max(blob) / MIB:.2f}",
            "vacuum_s": f"{med([int(r['vacuum_ns']) / 1e9 for r in sn]):.2f}",
            "zstd_s": f"{med([int(r['compress_ns']) / 1e9 for r in sn]):.2f}",
            "encrypt_s": f"{med([int(r['encrypt_ns']) / 1e9 for r in sn]):.3f}",
            "local_build_s": f"{local_s:.2f}",
            "local_build_min_s": f"{min(local):.2f}", "local_build_max_s": f"{max(local):.2f}",
            "mount_open_s": f"{open_s:.2f}",
            "delta_files": dsel[4] if dsel else 0,
            "delta_blob_kib": f"{dblob / 1024:.1f}",
            "snap_upload_s_calc": f"{S / RATE:.1f}", "snap_bucket_s_calc": f"{S / BUCKET:.1f}",
            "chain_len": kk, "avg_commit_s_calc": f"{A:.2f}", "avg_commit_bucket_s_calc": f"{B:.2f}",
            "ok_10pct": "yes" if A <= LIMIT else "no",
            "ok_10pct_bucket": "yes" if B <= LIMIT else "no",
            "snap_full_est_mib": f"{S_full / MIB:.2f}",
            "avg_commit_full_s_calc": f"{Af:.2f}", "avg_commit_full_bucket_s_calc": f"{Bf:.2f}",
            "ok_10pct_full": "yes" if Af <= LIMIT else "no",
            "ok_10pct_full_bucket": "yes" if Bf <= LIMIT else "no",
            "download_snap_20mbs_s_calc": f"{S / (20e6) + open_s:.1f}",
        }
        for w in (1, 100, 1000):
            x = deltas.get(k + (w,))
            row[f"delta{w}_blob_kib"] = f"{med([int(r['blob_bytes']) for r in x]) / 1024:.1f}" if x else ""
            row[f"delta{w}_patch_kib"] = f"{med([int(r['patch_bytes']) for r in x]) / 1024:.1f}" if x else ""
        out.append(row)

    if out:
        name = "e8_summary_text.csv" if suffix else "e8_summary.csv"
        with open(os.path.join(d, name), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(out[0]))
            w.writeheader()
            w.writerows(out)

    # --- сообщения (tgfs-sim) ---
    saving = []
    sim = defaultdict(dict)  # (dataset, scenario, budget) -> inline_max -> metric -> median
    for p in sorted(glob.glob(os.path.join(d, "e8_sim_*_summary.csv"))):
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                k = (r["dataset"], r["scenario"], r["inline_budget"], r["fsync_prob"])
                sim[k].setdefault(r["inline_max"], {})[r["metric"]] = (
                    float(r["median"]), float(r["min"]), float(r["max"]))
    for k, per in sorted(sim.items()):
        base = per.get("0")
        for im, m in per.items():
            row = {"dataset": k[0], "scenario": k[1] + ("-fsync" if k[3] != "0" else ""),
                   "inline_budget": k[2], "inline_max": im}
            for met in ("docs_total", "actions_total", "packs", "files_inline", "inline_bytes",
                        "dead_share", "undersized_share", "bytes_blobs", "bytes_packs"):
                row[met] = m[met][0]
            for met in ("docs_total", "actions_total"):
                row[f"saved_{met}"] = base[met][0] - m[met][0]
                row[f"saved_{met}_min"] = base[met][2] - m[met][2]
                row[f"saved_{met}_max"] = base[met][1] - m[met][1]
            saving.append(row)
    if saving and not suffix:
        with open(os.path.join(d, "e8_sim_saving.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(saving[0]))
            w.writeheader()
            w.writerows(saving)
    print(f"{len(out)} index rows, {len(saving)} sim rows")


if __name__ == "__main__":
    main()
