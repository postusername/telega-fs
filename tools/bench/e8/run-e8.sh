#!/usr/bin/env bash
# Офлайн-часть E8: сколько сообщений экономит инлайн по сравнению с пакетами.
# Все прогоны tgfs-sim, из которых сделан отчёт. Размеры снимков и дельт индекса даёт
# `tgfs-e8 index` (run-e8-index.sh), а не симулятор: модель блобов индекса в tgfs-sim грубая.
# Использование: tools/bench/e8/run-e8.sh <tgfs-sim> <tgfs-e8> <каталог вывода> <рабочий каталог>
# Пакеты по итогам E9: pack_threshold 4 МиБ, pack_target 16 МиБ, pack_max_age 30 с; коммиты с
# отсрочкой при заливке (5 · commit_interval).
set -euo pipefail

sim=${1:?usage: run-e8.sh <tgfs-sim> <tgfs-e8> <out dir> <work dir>}
e8=${2:?usage: run-e8.sh <tgfs-sim> <tgfs-e8> <out dir> <work dir>}
out=${3:?usage: run-e8.sh <tgfs-sim> <tgfs-e8> <out dir> <work dir>}
work=${4:?usage: run-e8.sh <tgfs-sim> <tgfs-e8> <out dir> <work dir>}
corpora=$(dirname "$0")/../corpora
mkdir -p "$out" "$work"

packs=(--thresholds 4MiB --targets 16MiB --ages 30 --limits on --commit-defer 5)
inline_max=0,1KiB,2KiB,4KiB,8KiB,16KiB

# Размеры C4 для симулятора: только файлы до pack_threshold (позиционные от инлайна не зависят
# и только удлиняют прогон). Те же размеры, что и в индексе (tgfs-e8 index).
for n in 100000 1000000; do
    "$e8" --work "$work" --out "$work" --c2-sizes "$corpora/c2-sizes.csv" hist \
        --dataset "c4-$n" --file "$work/c4-$n-small.csv" --small-only
done

run() { # <имя> <аргументы...>
    local name=$1
    shift
    echo "== $name"
    "$sim" "$@" --out "$out/e8_sim_$name.csv" --summary "$out/e8_sim_${name}_summary.csv"
}

for budget in 4MiB 16MiB 64MiB; do
    flags=(--inline-max "$inline_max" --inline-budget "$budget" "${packs[@]}")
    run "c1_$budget" --events "$corpora/c1.csv" --dataset c1 --scenario unpack --seeds 5 "${flags[@]}"
    run "c1_switch_$budget" --events "$corpora/c1-switch.csv" --dataset c1-switch \
        --scenario switch --seeds 5 --step-gap 60 "${flags[@]}"
    run "c1_switch_rev_$budget" --events "$corpora/c1-switch-rev.csv" --dataset c1-switch-rev \
        --scenario switch --seeds 5 --step-gap 60 "${flags[@]}"
    # fsync после 2 % файлов: инлайн-файл не закрывает пакет
    run "c1_fsync_$budget" --events "$corpora/c1-switch.csv" --dataset c1-switch \
        --scenario fsync --seeds 5 --step-gap 60 --fsync-prob 0.02 "${flags[@]}"
    run "c2_$budget" --hist "$corpora/c2-sizes.csv" --dataset c2 --scenario unpack --seeds 3 \
        "${flags[@]}"
    run "c4_100000_$budget" --hist "$work/c4-100000-small.csv" --dataset c4-100000 \
        --scenario unpack --seeds 3 "${flags[@]}"
    run "c4_1000000_$budget" --hist "$work/c4-1000000-small.csv" --dataset c4-1000000 \
        --scenario unpack --seeds 3 "${flags[@]}"
done
