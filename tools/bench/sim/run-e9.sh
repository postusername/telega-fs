#!/usr/bin/env bash
# Офлайн-часть E9 (SPEC 17.12): все прогоны tgfs-sim, из которых сделан отчёт.
# Использование: tools/bench/sim/run-e9.sh <tgfs-sim> <каталог вывода>
# Корпуса: tools/bench/corpora/{c1.csv,c1-switch.csv,c1-switch-rev.csv,c2-sizes.csv}.
set -euo pipefail

sim=${1:?usage: run-e9.sh <tgfs-sim> <out dir>}
out=${2:?usage: run-e9.sh <tgfs-sim> <out dir>}
corpora=$(dirname "$0")/../corpora
mkdir -p "$out"

grid=(--thresholds 64KiB,256KiB,1MiB,4MiB --targets 4MiB,16MiB,64MiB --ages 10,30,120 --baselines
      --limits both)
fast=(--thresholds 1MiB --targets 4MiB,16MiB,64MiB --ages 10,30,120 --limits on)

run() { # <имя> <аргументы...>
    local name=$1
    shift
    echo "== $name"
    "$sim" "$@" --out "$out/e9_sim_$name.csv" --summary "$out/e9_sim_${name}_summary.csv"
}

# Основная сетка 17.12. C1 и C2 — распаковка набора; C1-switch — распаковка старейшего из десяти
# коммитов и девять переключений с паузой 60 с; rev — то же в обратном порядке (есть удаления).
run c1 --events "$corpora/c1.csv" --dataset c1 --scenario unpack --seeds 5 "${grid[@]}"
run c1_switch --events "$corpora/c1-switch.csv" --dataset c1-switch --scenario switch \
    --seeds 5 --step-gap 60 "${grid[@]}"
run c1_switch_rev --events "$corpora/c1-switch-rev.csv" --dataset c1-switch-rev \
    --scenario switch --seeds 5 --step-gap 60 "${grid[@]}"
run c2 --hist "$corpora/c2-sizes.csv" --dataset c2 --scenario unpack --seeds 3 "${grid[@]}"

# Чувствительность pack_max_age: пауза между переключениями, скорость писателя, fsync.
run c1_gap --events "$corpora/c1-switch.csv" --dataset c1-switch --scenario gap --seeds 5 \
    --step-gap 0,10,30,60,120,300 "${fast[@]}"
run c1_speed --events "$corpora/c1-switch.csv" --dataset c1-switch --scenario speed --seeds 5 \
    --step-gap 60 --files-per-s 50,200,1000 "${fast[@]}"
run c1_unpack_speed --events "$corpora/c1.csv" --dataset c1 --scenario speed --seeds 5 \
    --files-per-s 20,50,200,1000 "${fast[@]}"
run c1_fsync --events "$corpora/c1-switch.csv" --dataset c1-switch --scenario fsync --seeds 5 \
    --step-gap 60 --fsync-prob 0,0.005,0.02 "${fast[@]}"
run c1_seal_lag --events "$corpora/c1-switch.csv" --dataset c1-switch --scenario seal-lag \
    --seeds 5 --step-gap 60 --seal-lag "${fast[@]}"

# Стоимость коммитов в лимите действий: commit_interval 30, 60 (по умолчанию), 300 и 600 с.
for ci in 30 60 300 600; do
    run "c2_commit_$ci" --hist "$corpora/c2-sizes.csv" --dataset c2 --scenario commit \
        --seeds 3 --commit-interval "$ci" --thresholds 1MiB,4MiB --targets 16MiB --ages 30 \
        --limits on
    run "c1_commit_$ci" --events "$corpora/c1-switch.csv" --dataset c1-switch --scenario commit \
        --seeds 5 --step-gap 60 --commit-interval "$ci" --thresholds 1MiB --targets 16MiB \
        --ages 30 --limits on
done

# Связь с E8 (инлайн, значения E8 ещё не выбраны): inline_max 0..16 КиБ, бюджет 4/16/64 МиБ.
for budget in 4MiB 16MiB 64MiB; do
    run "c1_inline_$budget" --events "$corpora/c1-switch.csv" --dataset c1-switch \
        --scenario inline --seeds 5 --step-gap 60 --inline-max 0,1KiB,4KiB,16KiB \
        --inline-budget "$budget" --thresholds 1MiB --targets 16MiB --ages 30 --limits on
    run "c2_inline_$budget" --hist "$corpora/c2-sizes.csv" --dataset c2 --scenario inline \
        --seeds 3 --inline-max 0,1KiB,4KiB,16KiB --inline-budget "$budget" \
        --thresholds 1MiB --targets 16MiB --ages 30 --limits on
done
