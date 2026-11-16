#!/usr/bin/env bash
# SPEC 15.4: бинарник динамически линкуется только с разрешёнными системными библиотеками.
# usage: check-linkage.sh [--sanitizer] <binary>
# --sanitizer дополнительно разрешает системные библиотеки, которые тянет рантайм санитайзеров.
set -euo pipefail

sanitizer=0
if [[ "${1:-}" == --sanitizer ]]; then
    sanitizer=1
    shift
fi
if [[ $# -ne 1 ]]; then
    echo "usage: $0 [--sanitizer] <binary>" >&2
    exit 2
fi

# libdl, libpthread и librt — части glibc: до 2.34 это отдельные .so, TDLib просит -ldl.
libs='libfuse3|libsodium|libzstd|libssl|libcrypto|libz|libc|libm|libdl|libpthread|librt'
libs+='|libstdc\+\+|libgcc_s|ld-linux-[a-z0-9_-]+'
if [[ $sanitizer -eq 1 ]]; then
    libs+='|libasan|libubsan|libtsan|libresolv'
fi
allowed="^(${libs})\.so(\.[0-9]+)*$"

needed=$(readelf -d "$1" | sed -n 's/.*(NEEDED).*\[\(.*\)\]/\1/p')
if [[ -z "$needed" ]]; then
    echo "no NEEDED entries found in $1" >&2
    exit 1
fi

status=0
while read -r lib; do
    if [[ "$lib" =~ $allowed ]]; then
        echo "ok:          $lib"
    else
        echo "not allowed: $lib" >&2
        status=1
    fi
done <<<"$needed"
exit "$status"
