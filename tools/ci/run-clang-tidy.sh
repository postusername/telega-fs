#!/usr/bin/env bash
# clang-tidy по compile_commands.json (SPEC 15.5).
# usage: run-clang-tidy.sh <build-dir> [file.cpp...]
# Без списка файлов проверяются все .cpp из src/ и tests/. Любое предупреждение — ошибка.
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
# Заголовки генерируются при сборке, поэтому таргеты должны быть хотя бы сконфигурированы.
"$CLANG_TIDY" -p "$build_dir" --quiet --warnings-as-errors='*' "${files[@]}"
echo "clang-tidy ok: ${#files[@]} files"
