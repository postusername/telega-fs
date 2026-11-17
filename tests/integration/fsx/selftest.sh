#!/usr/bin/env bash
# Самопроверка tgfs-fsx на обычной ФС (SPEC 14): инструмент сам не должен давать ложных
# расхождений, должен ловить настоящие и продолжать после контрольной точки детерминированно.
# usage: selftest.sh MODE FSX WORKDIR
# Временные файлы — в mktemp-каталоге внутри WORKDIR; при неудаче каталог остаётся для разбора.
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "usage: $0 MODE FSX WORKDIR" >&2
    exit 2
fi
mode=$1
fsx=$2
mkdir -p "$3"
work=$(mktemp -d "$3/$mode.XXXXXX")

die() {
    echo "FAIL ($mode): $*" >&2
    echo "work dir kept: $work" >&2
    exit 1
}

# Запустить fsx и проверить точный код выхода; вывод в $work/out.txt.
expect_exit() {
    local want=$1
    shift
    local got=0
    "$fsx" "$@" >"$work/out.txt" 2>&1 || got=$?
    if [[ $got -ne $want ]]; then
        cat "$work/out.txt" >&2
        die "expected exit $want, got $got: tgfs-fsx $*"
    fi
}

expect_output() {
    grep -q -- "$1" "$work/out.txt" || { cat "$work/out.txt" >&2; die "no '$1' in output"; }
}

# Испортить один байт файла мимо fsx (или дописать байт, если файл пуст).
corrupt() {
    local file=$1 size
    size=$(stat -c %s "$file")
    if [[ $size -eq 0 ]]; then
        printf 'x' >>"$file"
    else
        local off=$((size / 2)) old
        old=$(od -An -tu1 -j "$off" -N1 "$file" | tr -d ' ')
        printf "\\$(printf '%03o' $(((old + 1) % 256)))" |
            dd of="$file" bs=1 seek="$off" conv=notrunc status=none
    fi
}

case $mode in
    small)
        "$fsx" -q -N 20000 -S 1 "$work/f"
        ;;
    large)
        # Длина больше 16 МиБ, чтобы операции пересекали границы чанков.
        "$fsx" -q -N 3000 -S 3 -l 40m -o 1m --full-check 500 "$work/f"
        ;;
    determinism)
        "$fsx" -q -N 5000 -S 7 "$work/a"
        "$fsx" -q -N 5000 -S 7 "$work/b"
        cmp "$work/a" "$work/b" || die "same seed produced different files"
        ;;
    resume)
        "$fsx" -q -N 10000 -S 5 "$work/straight"
        expect_exit 0 -N 10000 -S 5 --checkpoint 5000 "$work/f"
        expect_output "checkpoint after op 5000"
        [[ -f "$work/f.fsxstate" ]] || die "state file not written"
        # Повтор тех же опций допустим, расходящиеся с состоянием — ошибка использования.
        expect_exit 2 --resume -S 6 "$work/f"
        expect_exit 0 --resume -N 10000 -S 5 "$work/f"
        expect_output "resuming at op 5001"
        [[ ! -e "$work/f.fsxstate" ]] || die "state file not removed after resume"
        cmp "$work/straight" "$work/f" || die "checkpoint/resume diverged from a straight run"
        # Отдельный путь состояния и несколько контрольных точек подряд.
        expect_exit 0 -N 10000 -S 5 --checkpoint 3000 --state "$work/st" "$work/g"
        expect_exit 0 --resume --checkpoint 7000 --state "$work/st" "$work/g"
        expect_exit 0 --resume --state "$work/st" "$work/g"
        cmp "$work/straight" "$work/g" || die "two checkpoints diverged from a straight run"
        ;;
    detects)
        expect_exit 1 -N 3000 -S 1 --inject-corruption 1000 "$work/f"
        expect_output "injected corruption"
        expect_output "MISMATCH"
        expect_output "last 100 operations"
        expect_output "FAILED at op"
        [[ -f "$work/f.fsxgood" && -f "$work/f.fsxbad" ]] || die "dumps not written"
        if cmp -s "$work/f.fsxgood" "$work/f.fsxbad"; then
            die "good and bad dumps are identical"
        fi
        # Порча файла между контрольной точкой и продолжением ловится сверкой при --resume.
        expect_exit 0 -N 4000 -S 2 --checkpoint 2000 "$work/g"
        corrupt "$work/g"
        expect_exit 1 --resume "$work/g"
        expect_output "on resume"
        ;;
    usage)
        expect_exit 2 "$work/f" "$work/g"
        expect_exit 2 -N
        expect_exit 2 -l 10x "$work/f"
        expect_exit 2 --bogus "$work/f"
        expect_exit 2 -N 100 --checkpoint 100 "$work/f"
        expect_exit 2 --resume "$work/missing"
        expect_exit 0 --help
        expect_output "usage: tgfs-fsx"
        ;;
    *)
        echo "unknown mode $mode" >&2
        exit 2
        ;;
esac

rm -rf "$work"
echo "ok ($mode)"
