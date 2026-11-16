#!/usr/bin/env bash
# Запуск `telega-fs --version`: проверяются и код выхода, и вывод. Под санитайзерами
# отчёт об ошибке меняет код выхода, поэтому PASS_REGULAR_EXPRESSION здесь не годится.
# usage: check-version.sh <binary> <expected-tdlib-version>
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <binary> <expected-tdlib-version>" >&2
    exit 2
fi

rc=0
out=$("$1" --version) || rc=$?
printf '%s\n' "$out"
if [[ $rc -ne 0 ]]; then
    echo "telega-fs --version exited with $rc" >&2
    exit "$rc"
fi
if ! grep -qxF "TDLib $2" <<<"$out"; then
    echo "expected line 'TDLib $2' not found" >&2
    exit 1
fi
