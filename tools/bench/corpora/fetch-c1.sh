#!/usr/bin/env bash
# Распаковать корпус C1 «Исходники» (SPEC 17.3): TDLib на коммите ba976cb4a.
# Использование: fetch-c1.sh <каталог TDLib с .git> <пустой целевой каталог>
set -euo pipefail

td_repo=${1:?usage: fetch-c1.sh <td git repo> <empty target dir>}
target=${2:?usage: fetch-c1.sh <td git repo> <empty target dir>}
commit=ba976cb4aba53a82eff425db2f85e2741774c944

mkdir -p "$target"
if [ -n "$(ls -A "$target")" ]; then
    echo "fetch-c1: $target is not empty" >&2
    exit 1
fi
git -C "$td_repo" archive "$commit" | tar -x -C "$target"
files=$(find "$target" -type f | wc -l)
bytes=$(find "$target" -type f -printf '%s\n' | awk '{s += $1} END {print s}')
echo "C1: $files files, $bytes bytes"
