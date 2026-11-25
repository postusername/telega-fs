#!/usr/bin/env python3
# Ключ кэша результатов clang-tidy для одного файла.
# usage: tidy-cache-key.py <build-dir> <file.cpp> <clang-tidy-version-text>
#
# Ключ — SHA-256 от препроцессированного TU, флагов компиляции (без пути выходного файла),
# содержимого .clang-tidy, версии clang-tidy и версии самого ключа. Препроцессированный TU
# включает все заголовки, поэтому правка любого включённого заголовка меняет ключ.
# Печатает ключ в stdout; при любой ошибке завершается с ненулевым кодом (тогда файл
# проверяется без кэша).
import hashlib
import json
import os
import shlex
import subprocess
import sys

KEY_VERSION = b"tidy-cache-v1 --warnings-as-errors=* --quiet\n"


def main() -> int:
    build_dir, src, tidy_version = sys.argv[1], sys.argv[2], sys.argv[3]
    src_abs = os.path.realpath(src)
    with open(os.path.join(build_dir, "compile_commands.json"), encoding="utf-8") as f:
        entries = json.load(f)
    entry = None
    for e in entries:
        path = e["file"]
        if not os.path.isabs(path):
            path = os.path.join(e["directory"], path)
        if os.path.realpath(path) == src_abs:
            entry = e
            break
    if entry is None:
        print(f"no compile command for {src}", file=sys.stderr)
        return 1
    args = entry.get("arguments") or shlex.split(entry["command"])

    # Флаги без выходного файла и без -c: они не влияют на результат clang-tidy.
    flags = []
    skip = False
    for a in args:
        if skip:
            skip = False
            continue
        if a == "-o":
            skip = True
            continue
        if a == "-c" or a.startswith("-o"):
            continue
        flags.append(a)

    pre = subprocess.run(flags + ["-E", "-o", "-"], cwd=entry["directory"],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=True).stdout

    h = hashlib.sha256()
    h.update(KEY_VERSION)
    h.update(tidy_version.encode())
    h.update(b"\0")
    h.update("\0".join(flags).encode())
    h.update(b"\0")
    root = subprocess.run(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE,
                          check=True, text=True).stdout.strip()
    with open(os.path.join(root, ".clang-tidy"), "rb") as f:
        h.update(f.read())
    h.update(b"\0")
    h.update(pre)
    print(h.hexdigest())
    return 0


if __name__ == "__main__":
    sys.exit(main())
