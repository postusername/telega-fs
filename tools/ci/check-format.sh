#!/usr/bin/env bash
# Проверка форматирования всех C/C++ файлов из git (SPEC 15.5).
set -euo pipefail

CLANG_FORMAT=${CLANG_FORMAT:-clang-format-18}
cd "$(git rev-parse --show-toplevel)"

mapfile -t files < <(git ls-files -- '*.c' '*.cpp' '*.h' '*.hpp')
if [[ ${#files[@]} -eq 0 ]]; then
    echo "no C/C++ files to check"
    exit 0
fi

"$CLANG_FORMAT" --version
"$CLANG_FORMAT" --dry-run --Werror "${files[@]}"
echo "formatting ok: ${#files[@]} files"
