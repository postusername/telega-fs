#!/usr/bin/env bash
# clang-tidy по compile_commands.json (SPEC 15.5).
# usage: run-clang-tidy.sh <build-dir> [file.cpp...]
# Без списка файлов проверяются все .cpp из src/ и tests/. Любое предупреждение — ошибка.
# Если задан CLANG_TIDY_CACHE_DIR, чистые результаты кэшируются по ключу из
# tools/ci/tidy-cache-key.py (препроцессированный TU, флаги, .clang-tidy, версия clang-tidy):
# файл с известным чистым ключом не проверяется повторно. Ошибки не кэшируются.
set -euo pipefail

CLANG_TIDY=${CLANG_TIDY:-clang-tidy-18}

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <build-dir> [file.cpp...]" >&2
    exit 2
fi
build_dir=$(realpath "$1")
shift

cd "$(git rev-parse --show-toplevel)"

if [[ ! -f "$build_dir/compile_commands.json" ]]; then
    echo "$build_dir/compile_commands.json not found, configure the build first" >&2
    exit 2
fi

if [[ $# -gt 0 ]]; then
    # Пути относительно корня репозитория. Заголовки проверяются через .cpp (HeaderFilterRegex),
    # поэтому остальные файлы пропускаются; отсутствующий .cpp — ошибка, а не тихий пропуск.
    files=()
    for f in "$@"; do
        [[ "$f" == *.cpp ]] || continue
        if [[ ! -f "$f" ]]; then
            echo "file not found: $f" >&2
            exit 2
        fi
        files+=("$f")
    done
else
    mapfile -t files < <(git ls-files -- 'src/*.cpp' 'tests/*.cpp')
fi

if [[ ${#files[@]} -eq 0 ]]; then
    echo "no .cpp files to check"
    exit 0
fi

"$CLANG_TIDY" --version
tidy_version=$("$CLANG_TIDY" --version)
cache_dir=${CLANG_TIDY_CACHE_DIR:-}
if [[ -n "$cache_dir" ]]; then
    mkdir -p "$cache_dir"
    cache_dir=$(realpath "$cache_dir")
fi
key_script="$(pwd)/tools/ci/tidy-cache-key.py"
# Заголовки генерируются при сборке, поэтому таргеты должны быть хотя бы сконфигурированы.
# Файлы проверяются параллельно (по одному процессу на файл), вывод каждого файла собирается
# отдельно и печатается целиком, чтобы сообщения разных файлов не перемешивались.
jobs=${CLANG_TIDY_JOBS:-$(nproc)}
out_dir=$(mktemp -d)
trap 'rm -rf "$out_dir"' EXIT
export CLANG_TIDY build_dir out_dir cache_dir key_script tidy_version
status=0
printf '%s\0' "${files[@]}" | xargs -0 -n 1 -P "$jobs" sh -c '
    log="$out_dir/$(printf "%s" "$1" | tr "/" "_").log"
    key=
    if [ -n "$cache_dir" ]; then
        key=$(python3 "$key_script" "$build_dir" "$1" "$tidy_version") || key=
        if [ -n "$key" ] && [ -f "$cache_dir/$key" ]; then
            touch "$log.cached"
            exit 0
        fi
    fi
    if "$CLANG_TIDY" -p "$build_dir" --quiet --warnings-as-errors="*" "$1" > "$log" 2>&1; then
        if [ -n "$key" ]; then
            touch "$cache_dir/$key"
        fi
    else
        touch "$log.failed"
    fi
' sh || status=$?
for log in "$out_dir"/*.log; do
    [[ -s "$log" ]] && cat "$log"
done
failed=$(find "$out_dir" -name '*.failed' | wc -l)
if [[ $failed -ne 0 || $status -ne 0 ]]; then
    echo "clang-tidy failed: $failed of ${#files[@]} files" >&2
    exit 1
fi
cached=$(find "$out_dir" -name '*.cached' | wc -l)
echo "clang-tidy ok: ${#files[@]} files, $cached from cache ($jobs jobs)"
