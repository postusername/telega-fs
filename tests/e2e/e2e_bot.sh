#!/usr/bin/env bash
# E2E telega-fs на живом Telegram в режиме бота (SPEC 14). Запускается вручную, не на каждый push.
#
#   tests/e2e/e2e_bot.sh                                  # из корня репозитория, бинарники из build/w-e2e
#   TGFS_BIN=... BENCH_BIN=... tests/e2e/e2e_bot.sh
#   cmake -B build/w-e2e -DTELEGA_FS_BUILD_TOOLS=ON -DTELEGA_FS_E2E=ON ... &&
#     TELEGA_FS_E2E_RUN=1 ctest --test-dir build/w-e2e -L e2e --output-on-failure
#
# Что делает (SPEC 14, 17.2):
#   1. tgfs-bench e2e setup: пользователь создаёт приватный канал и добавляет бота администратором
#      (post, edit, delete). Больше пользователь ничего не делает (0 сообщений).
#   2. Машина A (изолированные XDG-каталоги): login --bot, init --chat, mount, запись 200 МиБ
#      случайных данных и мелких файлов (инлайн, пакеты, каталоги), fsync, второй fsync сразу
#      после первого (CAS-повтор), размонтирование.
#   3. A ещё раз: повторное монтирование, сверка, запись файла без fsync и размонтирование по
#      SIGTERM (финальный коммит).
#   4. Машина B (пустые каталоги, тот же config.toml): login --bot заново (keyring без ФС),
#      mount (master_key из головы), чтение всего с пустым кэшем, сверка sha256.
#   5. A: logout, login, mount, сверка.
#   6. tgfs-bench e2e inspect: документы в канале называются <16 hex>.bin|.idx, подписей нет.
#   7. tgfs-bench e2e teardown (если не E2E_KEEP_CHANNEL=1).
#
# Креды: файл E2E_CREDS (по умолчанию cred.secret в корне репозитория; ключи api-id, api-hash,
# bot-token) или переменные TELEGA_API_ID, TELEGA_API_HASH, TELEGA_BOT_TOKEN. Секреты не
# печатаются. Все данные прогона лежат в E2E_WORK (по умолчанию mktemp в $TMPDIR), id канала —
# в E2E_CHAT_FILE (вне репозитория). Репозиторий скрипт не меняет.
#
# Переменные: TGFS_BIN, BENCH_BIN, E2E_CREDS, E2E_WORK, E2E_CHAT_FILE, E2E_SIZE_MIB (200),
# E2E_KEEP_CHANNEL=1 (не удалять канал), E2E_KEEP_SESSIONS=1 (не делать logout A и B в конце),
# E2E_REUSE_CHANNEL=1 (канал уже создан, E2E_CHAT_FILE существует; на нём не должно быть
# закреплённых сообщений), E2E_SYNC_TIMEOUT (sync_timeout в config.toml, по умолчанию 600s),
# E2E_BENCH_ARGS (дополнительные глобальные опции tgfs-bench, например --state-dir).
#
# Время: около 15 минут. Монтирование A после init ждёт до 90 с, пока кэш TDLib покажет голову
# (раздел «Кэш TDLib» в docs/experiments/10-m2-e2e.md), заливка 200 МиБ упирается в send_rate.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

# Из ctest без явного разрешения тест пропускается (код 77), чтобы `ctest -LE long` не ходил в сеть.
if [[ "${E2E_CTEST:-}" == "1" && "${TELEGA_FS_E2E_RUN:-}" != "1" ]]; then
    echo "E2E skipped: set TELEGA_FS_E2E_RUN=1 to run it against live Telegram"
    exit 77
fi

TGFS_BIN="${TGFS_BIN:-$repo/build/w-e2e/src/telega-fs}"
BENCH_BIN="${BENCH_BIN:-$repo/build/w-e2e/tools/bench/tdbench/tgfs-bench}"
E2E_CREDS="${E2E_CREDS:-$repo/cred.secret}"
E2E_SIZE_MIB="${E2E_SIZE_MIB:-200}"
E2E_SYNC_TIMEOUT="${E2E_SYNC_TIMEOUT:-600s}"
E2E_CHAT_FILE="${E2E_CHAT_FILE:-${XDG_DATA_HOME:-$HOME/.local/share}/telega-fs-bench/e2e-chat}"

for bin in "$TGFS_BIN" "$BENCH_BIN"; do
    [[ -x "$bin" ]] || { echo "no executable: $bin" >&2; exit 2; }
done

cred() { # значение ключа из файла кредов, ничего не печатает
    [[ -r "$E2E_CREDS" ]] || return 0
    sed -n "s/^$1=//p" "$E2E_CREDS" | head -n1
}
TELEGA_API_ID="${TELEGA_API_ID:-$(cred api-id)}"
TELEGA_API_HASH="${TELEGA_API_HASH:-$(cred api-hash)}"
bot_token="${TELEGA_BOT_TOKEN:-$(cred bot-token)}"
unset TELEGA_BOT_TOKEN
[[ -n "$TELEGA_API_ID" && -n "$TELEGA_API_HASH" && -n "$bot_token" ]] ||
    { echo "need api-id, api-hash and bot-token (E2E_CREDS or environment)" >&2; exit 2; }
export TELEGA_API_ID TELEGA_API_HASH

W="${E2E_WORK:-$(mktemp -d "${TMPDIR:-/tmp}/tgfs-e2e.XXXXXX")}"
mkdir -p "$W/logs"
chmod 700 "$W"
echo "[e2e] work dir: $W"

pids=()
mounts=()
channel_created=0

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
log() { echo "[e2e] $*"; }
fail() { echo "[e2e] FAIL: $*" >&2; exit 1; }

# Команда на «машине» A или B: свои XDG-каталоги.
on() {
    local m=$1
    shift
    XDG_CONFIG_HOME="$W/$m/config" XDG_DATA_HOME="$W/$m/data" XDG_CACHE_HOME="$W/$m/cache" "$@"
}
tgfs() { local m=$1; shift; on "$m" "$TGFS_BIN" "$@"; }
bench() { "$BENCH_BIN" ${E2E_BENCH_ARGS:-} --creds "$E2E_CREDS" "$@"; }

cleanup() {
    local rc=$?
    set +e
    for m in "${mounts[@]:-}"; do
        [[ -n "$m" ]] && mountpoint -q "$m" 2>/dev/null && fusermount3 -u "$m" 2>/dev/null
    done
    for p in "${pids[@]:-}"; do
        [[ -n "$p" ]] || continue
        for _ in $(seq 1 60); do kill -0 "$p" 2>/dev/null || break; sleep 1; done
    done
    # Сессии бота «машин» A и B закрываются (logOut), чтобы не копить авторизации бота.
    if [[ "${E2E_KEEP_SESSIONS:-0}" != 1 ]]; then
        for m in A B; do
            [[ -e "$W/$m/config/telega-fs/config.toml" ]] || continue
            on "$m" timeout 120 "$TGFS_BIN" logout >"$W/logs/final-logout-$m.out" 2>&1 \
                </dev/null || true
        done
    fi
    if [[ "$channel_created" == 1 && "${E2E_KEEP_CHANNEL:-0}" != 1 ]]; then
        log "teardown: deleting the channel"
        bench e2e --chat-file "$E2E_CHAT_FILE" teardown >"$W/logs/teardown.out" 2>&1 ||
            echo "[e2e] teardown failed, delete the channel by hand (see $W/logs/teardown.out)" >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

# --- config.toml и keyfile ---

write_config() { # машина A: профиль бота с keyfile; api_id и api_hash идут через окружение
    local m=$1
    mkdir -p "$W/$m/config/telega-fs" "$W/$m/data" "$W/$m/cache"
    chmod 700 "$W/$m/data"
    cat >"$W/$m/config/telega-fs/config.toml" <<TOML
[profiles.main]
store = "telegram"
auth = "bot"
keyfile = "$W/keyfile"
sync_timeout = "$E2E_SYNC_TIMEOUT"
TOML
    chmod 600 "$W/$m/config/telega-fs/config.toml"
}
head -c 32 /dev/urandom >"$W/keyfile"
chmod 600 "$W/keyfile"

# --- вспомогательное ---

# Смонтировать, запомнить pid демона в $W/<m>.pid. Использование: mount_on A MNT [опции].
mount_on() {
    local m=$1 mnt=$2
    shift 2
    mkdir -p "$mnt"
    local out
    out="$(tgfs "$m" mount "$@" "$mnt" 2>&1)" || { echo "$out" >&2; fail "mount on $m"; }
    echo "$out" >>"$W/logs/mount-$m.out"
    local pid
    pid="$(sed -n 's/.*mounted (pid \([0-9]*\),.*/\1/p' <<<"$out")"
    [[ -n "$pid" ]] || fail "cannot find the pid of the mount daemon on $m"
    echo "$pid" >"$W/$m.pid"
    pids+=("$pid")
    mounts+=("$mnt")
}

# Ждёт завершения демона по pid. Использование: wait_pid PID ТАЙМАУТ_С.
wait_pid() {
    local pid=$1 limit=$2 t0
    t0="$(now)"
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1
        awk -v a="$t0" -v b="$(now)" -v l="$limit" 'BEGIN { exit !(b - a > l) }' &&
            fail "the daemon $pid did not exit in ${limit}s"
    done
    return 0
}

daemon_log() { echo "$W/$1/data/telega-fs/main/telega-fs.log"; }

# Размонтировать fusermount3 и дождаться финального коммита (демон вышел, в логе unmounted).
unmount_wait() {
    local m=$1 mnt=$2 t0
    t0="$(now)"
    local pid
    pid="$(cat "$W/$m.pid")"
    fusermount3 -u "$mnt" || fail "fusermount3 -u on $m"
    wait_pid "$pid" 900
    LAST_UNMOUNT_S="$(elapsed "$t0")"
    grep -q 'unmounted$' "$(daemon_log "$m")" || fail "no 'unmounted' in the log of $m"
    if grep -q 'final commit failed\|flush on unmount failed' "$(daemon_log "$m")"; then
        fail "the final commit on $m failed (see its log)"
    fi
}

make_manifest() { # make_manifest ROOT OUT: sha256 файлов, режимы и размеры
    (
        cd "$1"
        find . -type f -printf '%P\n' | LC_ALL=C sort | xargs -d '\n' sha256sum
        find . -mindepth 1 -printf '%y %m %P\n' | LC_ALL=C sort
        find . -type f -printf '%m %s %P\n' | LC_ALL=C sort
    ) >"$2"
}

verify_tree() { # verify_tree МАШИНА MNT ИМЯ stage1|full: содержимое смонтированной ФС совпадает с эталоном
    local mnt=$2 name=$3 expected="$W/manifest-$4.txt"
    make_manifest "$mnt" "$W/manifest-$name.txt" || fail "cannot read the tree on $1 ($name)"
    if ! diff -q "$expected" "$W/manifest-$name.txt" >/dev/null; then
        diff "$expected" "$W/manifest-$name.txt" | head -n 20 >&2
        fail "tree mismatch on $1 ($name)"
    fi
    log "tree on $1 matches ($name)"
}

# --- 1. канал ---

if [[ "${E2E_REUSE_CHANNEL:-0}" == 1 ]]; then
    [[ -r "$E2E_CHAT_FILE" ]] || fail "E2E_REUSE_CHANNEL=1 but E2E_CHAT_FILE does not exist"
else
    log "step 1: creating the channel (user account: setup only, no messages)"
    t=$(now)
    bench e2e --chat-file "$E2E_CHAT_FILE" setup >"$W/logs/setup.out" 2>&1 ||
        { tail -n 5 "$W/logs/setup.out" >&2; fail "channel setup"; }
    channel_created=1
    log "channel ready in $(elapsed "$t") s"
fi
chat_id="$(cat "$E2E_CHAT_FILE")"

# --- 2. данные-эталон ---

log "step 2: generating test data ($E2E_SIZE_MIB MiB + small files)"
src="$W/src"
mkdir -p "$src/small" "$src/dir/sub/deeper" "$src/medium"
head -c "$((E2E_SIZE_MIB * 1024 * 1024))" /dev/urandom >"$src/big.bin"
head -c $((5 * 1024 * 1024 + 17)) /dev/urandom >"$src/five.bin"   # больше pack_threshold, свой чанк
for n in 1 2 3 4 5 6 7 8 9 10 11 12; do                           # инлайн: до 4 КиБ
    head -c $((n * 300)) /dev/urandom >"$src/small/s$n.dat"
done
for n in 1 2 3 4 5 6; do                                          # пакеты: 5 КиБ .. 3 МиБ
    head -c $((n * n * 90 * 1024)) /dev/urandom >"$src/medium/m$n.dat"
done
: >"$src/empty.txt"
echo "hello" >"$src/dir/hello.txt"
head -c 7000 /dev/urandom >"$src/dir/sub/deeper/seven.dat"
printf 'name with spaces and \xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82' >"$src/dir/sub/имя файла.txt"
chmod 640 "$src/dir/hello.txt"
chmod 755 "$src/dir"

# Эталоны: после первого сеанса A (со second.txt и newdir, без late.txt) и итоговый (с late.txt).
printf 'second commit\n' >"$src/second.txt"
mkdir -p "$src/newdir"
make_manifest "$src" "$W/manifest-stage1.txt"
printf 'late file, no fsync\n' >"$W/late.txt"
cp "$W/late.txt" "$src/late.txt"
make_manifest "$src" "$W/manifest-full.txt"
rm -f "$src/late.txt"

# --- 3. машина A: login, init ---

log "step 3: machine A: login, init"
write_config A
printf '%s\n' "$bot_token" | tgfs A login --bot >"$W/logs/login-A.out" 2>&1 ||
    { tail -n 5 "$W/logs/login-A.out" >&2; fail "login --bot on A"; }
grep -q '^Logged in as bot' "$W/logs/login-A.out" || fail "login output on A has no 'Logged in as bot'"
tgfs A init --chat "$chat_id" >"$W/logs/init-A.out" 2>&1 ||
    { tail -n 8 "$W/logs/init-A.out" >&2; fail "init on A"; }
grep -q "^chat_id *= *$chat_id" "$W/A/config/telega-fs/config.toml" || fail "chat_id was not saved in config.toml of A"
grep -q 'api_hash' "$W/A/config/telega-fs/config.toml" && fail "api_hash leaked into config.toml"
log "init ok, chat_id saved in config.toml"
[[ "$(stat -c %a "$W/A/data/telega-fs/main/keyring")" == 600 ]] || fail "keyring is not 0600"

# --- 4. A: запись ---

log "step 4: machine A: mount, write, fsync, unmount"
mnt_a="$W/A/mnt"
t_mount=$(now)
mount_on A "$mnt_a"
T_MOUNT_A="$(elapsed "$t_mount")"
log "mounted in $T_MOUNT_A s"

t=$(now)
cp -r "$src/." "$mnt_a/"
rm -f "$mnt_a/second.txt"
rmdir "$mnt_a/newdir"
T_WRITE="$(elapsed "$t")"
log "written to the mount in $T_WRITE s (page cache and overlay)"

t=$(now)
sync "$mnt_a/big.bin" || fail "fsync of the big file failed (EIO?)"
T_FSYNC1="$(elapsed "$t")"
log "fsync #1 (upload of everything + commit): $T_FSYNC1 s"

# CAS-повтор (SPEC 7.1 шаг 4): второй коммит вскоре после первого, голова может быть из кэша TDLib.
t=$(now)
cp "$src/second.txt" "$mnt_a/second.txt"
sync "$mnt_a/second.txt" || fail "fsync #2 failed (EIO?)"
T_FSYNC2="$(elapsed "$t")"
log "fsync #2 right after #1: $T_FSYNC2 s"

t=$(now)
mkdir "$mnt_a/newdir"
sync "$mnt_a" || fail "fsyncdir failed (EIO?)"
T_FSYNC3="$(elapsed "$t")"
log "fsyncdir #3: $T_FSYNC3 s"

# Чтение только что залитого в том же процессе (TDLib считает такой файл локальным, а жёсткая
# ссылка уже удалена). Страницы файла выбрасываются из page cache, чтобы читать через Store.
python3 -I -c 'import os,sys; fd=os.open(sys.argv[1], os.O_RDONLY); os.posix_fadvise(fd,0,0,os.POSIX_FADV_DONTNEED); os.close(fd)' "$mnt_a/big.bin"
t=$(now)
sha_big_a_same="$(sha256sum "$mnt_a/big.bin" | cut -d' ' -f1)"
T_READ_SAME="$(elapsed "$t")"
[[ "$sha_big_a_same" == "$(sha256sum "$src/big.bin" | cut -d' ' -f1)" ]] \
    || fail "sha256 of the big file read back in the same process differs"
log "big file read back in the same process after upload: $T_READ_SAME s, sha256 matches"

unmount_wait A "$mnt_a"
T_UNMOUNT1="$LAST_UNMOUNT_S"
log "unmounted (final commit): $T_UNMOUNT1 s"

# --- 5. A: повторное монтирование и SIGTERM ---

log "step 5: machine A: remount, verify, write without fsync, SIGTERM"
t=$(now)
mount_on A "$mnt_a"
T_REMOUNT="$(elapsed "$t")"
log "remounted in $T_REMOUNT s"
verify_tree A "$mnt_a" A-remount stage1
cp "$W/late.txt" "$mnt_a/late.txt"
pid_a="$(cat "$W/A.pid")"
t=$(now)
kill -TERM "$pid_a"
wait_pid "$pid_a" 900
T_SIGTERM="$(elapsed "$t")"
mountpoint -q "$mnt_a" && fail "the mount point is still mounted after SIGTERM"
grep -q 'unmounted$' "$(daemon_log A)" || fail "no 'unmounted' after SIGTERM"
grep -q 'final commit failed\|flush on unmount failed' "$(daemon_log A)" && fail "final commit after SIGTERM failed"
log "SIGTERM: unmounted with the final commit in $T_SIGTERM s"

# --- 6. машина B ---

log "step 6: machine B: empty directories, login, mount, read everything"
mkdir -p "$W/B/config/telega-fs" "$W/B/data" "$W/B/cache"
chmod 700 "$W/B/data"
cp "$W/A/config/telega-fs/config.toml" "$W/B/config/telega-fs/config.toml"
chmod 600 "$W/B/config/telega-fs/config.toml"
printf '%s\n' "$bot_token" | tgfs B login --bot >"$W/logs/login-B.out" 2>&1 ||
    { tail -n 5 "$W/logs/login-B.out" >&2; fail "login --bot on B"; }
mnt_b="$W/B/mnt"
t=$(now)
mount_on B "$mnt_b"
T_MOUNT_B="$(elapsed "$t")"
log "mounted on B (master_key from the channel head) in $T_MOUNT_B s"

t=$(now)
sha_big_b="$(sha256sum "$mnt_b/big.bin" | cut -d' ' -f1)"
T_READ_BIG="$(elapsed "$t")"
sha_big_src="$(sha256sum "$src/big.bin" | cut -d' ' -f1)"
[[ "$sha_big_b" == "$sha_big_src" ]] || fail "sha256 of the big file differs on B"
log "big file read on B with an empty cache: $T_READ_BIG s, sha256 matches"
verify_tree B "$mnt_b" B full
unmount_wait B "$mnt_b"
log "unmounted B"

# --- 7. A: logout, login ---

log "step 7: machine A: logout, login, mount, verify"
tgfs A logout >"$W/logs/logout-A.out" 2>&1 || { tail -n 5 "$W/logs/logout-A.out" >&2; fail "logout on A"; }
[[ ! -e "$W/A/data/telega-fs/main/tdlib" ]] || fail "the TDLib session was not removed by logout"
[[ -e "$W/A/data/telega-fs/main/keyring" ]] || fail "logout removed the keyring"
printf '%s\n' "$bot_token" | tgfs A login --bot >"$W/logs/login-A2.out" 2>&1 ||
    { tail -n 5 "$W/logs/login-A2.out" >&2; fail "login --bot on A after logout"; }
mount_on A "$mnt_a"
verify_tree A "$mnt_a" A-relogin full
unmount_wait A "$mnt_a"

# --- 8. что в канале ---

log "step 8: inspect the channel"
bench e2e --chat-file "$E2E_CHAT_FILE" inspect >"$W/logs/inspect.out" 2>&1 || true
cat "$W/logs/inspect.out" | grep -v '^\[' | sed 's/^/[e2e]   /'
grep -q '^INSPECT OK' "$W/logs/inspect.out" || fail "the channel holds documents with unexpected names or captions"

# --- итоги ---

leaks=0
for f in "$W"/logs/*.out "$W"/A/data/telega-fs/main/*.log "$W"/B/data/telega-fs/main/*.log; do
    [[ -r "$f" ]] || continue
    if grep -qF -- "$bot_token" "$f" || grep -qF -- "$TELEGA_API_HASH" "$f"; then
        leaks=$((leaks + 1))
        echo "[e2e] secret found in a log: ${f##*/}" >&2
    fi
done
[[ "$leaks" == 0 ]] || fail "secrets leaked into logs"

floods=0
for m in A B; do
    n="$(grep -ci 'flood_wait\|retry after' "$W/$m/data/telega-fs/main/telega-fs.log" 2>/dev/null || true)"
    floods=$((floods + ${n:-0}))
done
errors=0
for m in A B; do
    n="$(grep -c '\[error\]' "$(daemon_log "$m")" 2>/dev/null || true)"
    errors=$((errors + ${n:-0}))
done

cat <<SUMMARY
[e2e] ---- summary ----
[e2e] data: ${E2E_SIZE_MIB} MiB random + small files
[e2e] mount A: ${T_MOUNT_A} s, remount A: ${T_REMOUNT} s, mount B: ${T_MOUNT_B} s
[e2e] write to the mount: ${T_WRITE} s
[e2e] fsync #1 (upload + commit): ${T_FSYNC1} s, #2: ${T_FSYNC2} s, fsyncdir: ${T_FSYNC3} s
[e2e] unmount: ${T_UNMOUNT1} s, SIGTERM: ${T_SIGTERM} s
[e2e] cold read of the big file on B: ${T_READ_BIG} s
[e2e] FLOOD_WAIT lines in logs: ${floods}, [error] lines in daemon logs: ${errors}
[e2e] E2E PASS (work dir kept: $W)
SUMMARY
