#!/usr/bin/env bash
# E2E telega-fs на живом Telegram в режиме бота (SPEC 14, 16, веха M4: обслуживание). Запускается
# вручную. Дополняет e2e_bot.sh (M2) и e2e_m3.sh (M3): управляющий сокет на смонтированной ФС,
# дедупликация, потеря головы с `mount --force-local`, fsck, gc, gc --compact.
#
#   tests/e2e/e2e_m4.sh [ЭТАП...]        # без аргументов: все неготовые этапы по порядку
#   tests/e2e/e2e_m4.sh status           # какие этапы готовы, проверки и результаты
#   tests/e2e/e2e_m4.sh teardown         # удалить канал (final делает это сам только при всех PASS;
#                                        при FAIL канал остаётся для разбора)
#   E2E_SHORT=1 tests/e2e/e2e_m4.sh channel gen a_init a_write drop_head refuse force_local fresh_ro
#                                        # короткий прогон на ~20 МиБ (свой E2E_WORK и канал)
#   E2E_REDO="a_init a_write ..." tests/e2e/e2e_m4.sh   # повтор этапов; gen (src) не нужен
#
# Бинарники: TGFS_BIN, BENCH_BIN (по умолчанию build/w-e2e4). Креды: cred.secret (api-id, api-hash,
# bot-token) или E2E_CREDS, либо TELEGA_API_ID, TELEGA_API_HASH, TELEGA_BOT_TOKEN. Секреты не
# печатаются и в логи не попадают (в конце лог-файлы проверяются на токен и api_hash).
#
# Нагрузку несёт бот. Аккаунт пользователя (SPEC 17.2): только создание канала с добавлением бота и
# его удаление в конце (`tgfs-bench e2e setup|teardown`), ни одного сообщения. «Потерю головы»
# делает тоже бот: `tgfs-bench e2e drop-head` (unpinAllChatMessages и deleteMessages).
#
# Этапы (каждый возобновляемый: маркер готовности в $E2E_WORK/state/<этап>.done):
#   channel     tgfs-bench e2e setup: канал и бот-администратор
#   gen         данные-эталон: два файла 50-100 МиБ, файл 5 МиБ, инлайн, пакеты, каталоги
#   a_init      машина A: login --bot, init --chat
#   a_write     A: mount, запись, status (до, во время, после), status --json, sync, дерево после
#               sync, запись после sync, размонтирование; inspect: база документов в канале
#   a_dedup     A: cp большого файла второй раз, sync: новых чанков ~0 (status и inspect), sha256
#   drop_head   бот снимает все закрепы и удаляет сообщение головы
#   refuse      A: mount отказывает (подсказка --ro и --force-local), mount --ro читает дерево
#   force_local A: mount --force-local, дерево, размонтирование: новая голова — снимок
#   fresh_ro    машина B1 (пустые каталоги, тот же канал, --ro): дерево совпадает
#   fsck1       A (размонтирована): fsck, fsck --verify-data, gc
#   compact     A: пачка мелких файлов своим пакетом, удаление почти всех, gc --compact (с
#               retain_generations = 1: старые пакеты реально уходят из канала), fsck --verify-data,
#               B2 (--ro): дерево совпадает
#   final       inspect, секреты в логах, FLOOD_WAIT, сводка, logout, удаление канала
#
# Коды выхода: 0 все PASS; 1 есть FAIL (канал оставлен); 2 неверное окружение; 3 остановка по
# правилам SPEC 17.2 (FLOOD_WAIT больше часа или блокировка бота; state/STOP-FLOOD); 4 жёсткая
# ошибка этапа (этап не отмечен готовым, прогон можно повторить); 130/143 прерван.
#
# Переменные (по умолчанию в скобках):
#   E2E_SHORT (0)                 1: большие файлы 12 и 8 МиБ, меньше мелких; свои E2E_WORK и канал
#   E2E_WORK (build/e2e-m4-work)  рабочий каталог (нужно ~1 ГиБ)
#   E2E_CHAT_FILE                 файл с id канала (~/.local/share/telega-fs-bench/e2e-m4-chat)
#   E2E_STORE (telegram)          local: профили на LocalDirStore (только для проверки синтаксиса)
#   E2E_BIG1_MIB (80), E2E_BIG2_MIB (55), E2E_PACK_FILES (300), E2E_CMP_FILES (40)
#   E2E_CHUNK_MIB (8)             chunk_size профиля
#   E2E_TRACE_ARGS (-v)           глобальные опции telega-fs; E2E_TRACE_ENV
#                                 (TELEGA_FS_TDLIB_LOG_VERBOSITY=2) — переменные демонов
#   E2E_SYNC_TIMEOUT (600s)       sync_timeout профиля
#   E2E_REDO="этап ..."           сбросить готовность этих этапов
#   E2E_KEEP_CHANNEL=1            не удалять канал в final; E2E_KEEP_SESSIONS=1 не делать logout
#   E2E_REUSE_CHANNEL=1           канал уже создан (E2E_CHAT_FILE существует)
#   E2E_BENCH_ARGS                доп. глобальные опции tgfs-bench (например --state-dir)
#
# Время живого прогона: около часа. Окно 90 с на кэш TDLib ждётся при каждом монтировании после
# потери головы (refuse: отказ и --ro, force_local) и после init.

set -Eeuo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

if [[ "${E2E_CTEST:-}" == "1" && "${TELEGA_FS_E2E_RUN:-}" != "1" ]]; then
    echo "E2E skipped: set TELEGA_FS_E2E_RUN=1 to run it against live Telegram"
    exit 77
fi

EXIT_CHECKS=1 EXIT_ENV=2 EXIT_STOP=3 EXIT_STAGE=4

E2E_SHORT="${E2E_SHORT:-0}"
TGFS_BIN="${TGFS_BIN:-$repo/build/w-e2e4/src/telega-fs}"
BENCH_BIN="${BENCH_BIN:-$repo/build/w-e2e4/tools/bench/tdbench/tgfs-bench}"
E2E_CREDS="${E2E_CREDS:-$repo/cred.secret}"
E2E_STORE="${E2E_STORE:-telegram}"
if [[ "$E2E_SHORT" == 1 ]]; then
    E2E_BIG1_MIB="${E2E_BIG1_MIB:-12}"
    E2E_BIG2_MIB="${E2E_BIG2_MIB:-8}"
    E2E_PACK_FILES="${E2E_PACK_FILES:-40}"
    E2E_CMP_FILES="${E2E_CMP_FILES:-30}"
    sfx="-short"
else
    E2E_BIG1_MIB="${E2E_BIG1_MIB:-80}"
    E2E_BIG2_MIB="${E2E_BIG2_MIB:-55}"
    E2E_PACK_FILES="${E2E_PACK_FILES:-300}"
    E2E_CMP_FILES="${E2E_CMP_FILES:-40}"
    sfx=""
fi
E2E_CHUNK_MIB="${E2E_CHUNK_MIB:-8}"
E2E_TRACE_ARGS="${E2E_TRACE_ARGS--v}"
E2E_TRACE_ENV="${E2E_TRACE_ENV-TELEGA_FS_TDLIB_LOG_VERBOSITY=2}"
E2E_SYNC_TIMEOUT="${E2E_SYNC_TIMEOUT:-600s}"
E2E_CHAT_FILE="${E2E_CHAT_FILE:-${XDG_DATA_HOME:-$HOME/.local/share}/telega-fs-bench/e2e-m4${sfx}-chat}"
W="${E2E_WORK:-$repo/build/e2e-m4${sfx}-work}"

STAGES=(channel gen a_init a_write a_dedup drop_head refuse force_local fresh_ro fsck1 compact final)
[[ "$E2E_STORE" == local || "$E2E_STORE" == telegram ]] || { echo "E2E_STORE: telegram or local" >&2; exit "$EXIT_ENV"; }
FLOOD_LIMIT_S=3600          # SPEC 17.2: FLOOD_WAIT больше часа останавливает серию
SEED_PREFIX="tgfs-e2e-m4"   # seed детерминированного генератора данных

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
log() { echo "[e2e] $(date +%H:%M:%S) $*"; }
die() { echo "[e2e] ERROR: $*" >&2; exit "$EXIT_STAGE"; }
env_die() { echo "[e2e] $*" >&2; exit "$EXIT_ENV"; }
fcmp() { awk -v a="$1" -v b="$3" "BEGIN { exit !(a $2 b) }"; }
mib() { awk -v b="$1" 'BEGIN { printf "%.1f", b / 1048576 }'; }

# --- подготовка окружения -------------------------------------------------------------------

sub="${1:-}"
for bin in "$TGFS_BIN" "$BENCH_BIN"; do
    [[ -x "$bin" ]] || env_die "no executable: $bin"
done
for tool in openssl fusermount3 sha256sum awk find jq; do
    command -v "$tool" >/dev/null || env_die "need $tool"
done

cred() { # значение ключа из файла кредов, ничего не печатает
    [[ -r "$E2E_CREDS" ]] || return 0
    sed -n "s/^$1=//p" "$E2E_CREDS" | head -n1
}
bot_token=""
if [[ "$E2E_STORE" == telegram ]]; then
    TELEGA_API_ID="${TELEGA_API_ID:-$(cred api-id)}"
    TELEGA_API_HASH="${TELEGA_API_HASH:-$(cred api-hash)}"
    bot_token="${TELEGA_BOT_TOKEN:-$(cred bot-token)}"
    unset TELEGA_BOT_TOKEN
    [[ -n "$TELEGA_API_ID" && -n "$TELEGA_API_HASH" && -n "$bot_token" ]] ||
        env_die "need api-id, api-hash and bot-token (E2E_CREDS or environment)"
    export TELEGA_API_ID TELEGA_API_HASH
    unset TELEGA_FS_FAULTS
fi

mkdir -p "$W/logs" "$W/state" "$W/samples"
chmod 700 "$W"
touch "$W/results.env" "$W/checks.txt"
# shellcheck disable=SC1091
. "$W/results.env"
log "work dir: $W (store: $E2E_STORE, short: $E2E_SHORT)"

# --- результаты и проверки ------------------------------------------------------------------

res() { # res ИМЯ ЗНАЧЕНИЕ: переменная + запись в results.env (при возобновлении подхватывается)
    printf -v "$1" '%s' "$2"
    printf '%s=%q\n' "$1" "$2" >>"$W/results.env"
}

CUR_STAGE=""
verdict() { # verdict ИМЯ PASS|FAIL|SKIP ПОДРОБНОСТИ
    printf '%s|%s|%s|%s\n' "$CUR_STAGE" "$1" "$2" "$3" >>"$W/checks.txt"
    log "check $2: $1 ($3)"
}
check_if() { # check_if ИМЯ УСЛОВИЕ(0|1) ПОДРОБНОСТИ
    if [[ "$2" == 1 ]]; then verdict "$1" PASS "$3"; else verdict "$1" FAIL "$3"; fi
}
b2i() { if "$@"; then echo 1; else echo 0; fi; }   # b2i команда...: код 0 -> 1

# --- «машины», монтирование, демоны ----------------------------------------------------------

daemon_pids=()
mounts=()
sampler_pids=()
declare -A SAMPLER_OF=()
declare -A LOG_OFFSET=()

on() { # команда на «машине» (A, B1, B2): свои XDG-каталоги
    local m=$1
    shift
    XDG_CONFIG_HOME="$W/$m/config" XDG_DATA_HOME="$W/$m/data" XDG_CACHE_HOME="$W/$m/cache" "$@"
}
# shellcheck disable=SC2086
tgfs() { local m=$1; shift; on "$m" env $E2E_TRACE_ENV "$TGFS_BIN" $E2E_TRACE_ARGS "$@"; }
bench() { "$BENCH_BIN" ${E2E_BENCH_ARGS:-} --creds "$E2E_CREDS" "$@" </dev/null; }

daemon_log() { echo "$W/$1/data/telega-fs/main/telega-fs.log"; }
data_dir() { echo "$W/$1/data/telega-fs/main"; }

cleanup() {
    local rc=$?
    set +e
    trap - ERR
    trap '' INT TERM
    for p in "${sampler_pids[@]:-}"; do
        [[ -n "$p" ]] && kill "$p" 2>/dev/null
    done
    if [[ -e "$W/state/STOP-FLOOD" ]]; then
        for p in "${daemon_pids[@]:-}"; do [[ -n "$p" ]] && kill -KILL "$p" 2>/dev/null; done
        for m in "${mounts[@]:-}"; do
            [[ -n "$m" ]] && fusermount3 -uz "$m" 2>/dev/null
        done
        rc=$EXIT_STOP
    else
        for m in "${mounts[@]:-}"; do
            [[ -n "$m" ]] && mountpoint -q "$m" 2>/dev/null && fusermount3 -u "$m" 2>/dev/null
        done
        for p in "${daemon_pids[@]:-}"; do
            [[ -n "$p" ]] || continue
            for _ in $(seq 1 600); do kill -0 "$p" 2>/dev/null || break; sleep 1; done
        done
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'echo "[e2e] ERROR: command failed at line $LINENO (exit $?)" >&2; exit $EXIT_STAGE' ERR
trap 'exit 130' INT
trap 'exit 143' TERM

write_config() { # write_config МАШИНА: профиль main (бот или local)
    local m=$1
    mkdir -p "$W/$m/config/telega-fs" "$W/$m/data" "$W/$m/cache"
    chmod 700 "$W/$m/data"
    {
        echo "[profiles.main]"
        if [[ "$E2E_STORE" == local ]]; then
            echo "store = \"local\""
            echo "local_dir = \"$W/channel\""
        else
            echo "store = \"telegram\""
            echo "auth = \"bot\""
        fi
        echo "keyfile = \"$W/keyfile\""
        echo "chunk_size = \"${E2E_CHUNK_MIB}MiB\""
        echo "sync_timeout = \"$E2E_SYNC_TIMEOUT\""
    } >"$W/$m/config/telega-fs/config.toml"
    chmod 600 "$W/$m/config/telega-fs/config.toml"
}
if [[ ! -e "$W/keyfile" ]]; then
    head -c 32 /dev/urandom >"$W/keyfile"
    chmod 600 "$W/keyfile"
fi

login_bot() { # login_bot МАШИНА
    local m=$1
    [[ "$E2E_STORE" == local ]] && return 0
    printf '%s\n' "$bot_token" | tgfs "$m" login --bot >"$W/logs/login-$m.out" 2>&1 ||
        { tail -n 5 "$W/logs/login-$m.out" >&2; die "login --bot on $m"; }
    grep -q '^Logged in as bot' "$W/logs/login-$m.out" || die "login output on $m has no 'Logged in as bot'"
}

logout_machine() { # закрыть сессию бота (токен остаётся рабочим)
    local m=$1
    [[ "$E2E_STORE" == telegram && -e "$W/$m/config/telega-fs/config.toml" ]] || return 0
    [[ "${E2E_KEEP_SESSIONS:-0}" == 1 ]] && return 0
    on "$m" timeout 120 "$TGFS_BIN" logout >"$W/logs/logout-$m.out" 2>&1 </dev/null || true
}

# Чистая «машина» B: новые каталоги, логин, тот же config.toml (с chat_id), что у A.
fresh_machine() {
    local m=$1
    [[ -n "$m" && "$m" != A ]] || die "fresh_machine: refusing to wipe '$m'"
    logout_machine "$m"
    [[ -e "$W/$m/mnt" ]] && mountpoint -q "$W/$m/mnt" && die "$W/$m/mnt is still mounted"
    rm -rf "${W:?}/$m"
    mkdir -p "$W/$m/config/telega-fs" "$W/$m/data" "$W/$m/cache"
    chmod 700 "$W/$m/data"
    cp "$W/A/config/telega-fs/config.toml" "$W/$m/config/telega-fs/config.toml"
    chmod 600 "$W/$m/config/telega-fs/config.toml"
    login_bot "$m"
    if [[ "$E2E_STORE" == local ]]; then
        mkdir -p "$(data_dir "$m")"
        cp "$(data_dir A)/keyring" "$(data_dir "$m")/keyring" 2>/dev/null || true
    fi
}

# Смонтировать, запомнить pid демона. mount_on МАШИНА MNT [опции mount].
mount_on() {
    local m=$1 mnt=$2
    shift 2
    mkdir -p "$mnt"
    LOG_OFFSET[$m]=0
    [[ -r "$(daemon_log "$m")" ]] && LOG_OFFSET[$m]=$(stat -c %s "$(daemon_log "$m")")
    local out
    # shellcheck disable=SC2086
    out="$(timeout 1200 env XDG_CONFIG_HOME="$W/$m/config" XDG_DATA_HOME="$W/$m/data" \
        XDG_CACHE_HOME="$W/$m/cache" $E2E_TRACE_ENV "$TGFS_BIN" $E2E_TRACE_ARGS mount "$@" "$mnt" 2>&1)" ||
        { echo "$out" >&2; die "mount on $m"; }
    echo "$out" >>"$W/logs/mount-$m.out"
    local pid
    pid="$(sed -n 's/.*mounted (pid \([0-9]*\),.*/\1/p' <<<"$out")"
    [[ -n "$pid" ]] || die "cannot find the pid of the mount daemon on $m"
    echo "$pid" >"$W/$m.pid"
    daemon_pids+=("$pid")
    mounts+=("$mnt")
    sample_start "$CUR_STAGE-$m" "$m" "$pid"
    SAMPLER_OF[$m]=$SAMPLER_PID
}

wait_pid() { # wait_pid PID ТАЙМАУТ_С: ждать завершения демона
    local pid=$1 limit=$2 t0
    t0="$(now)"
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1
        fcmp "$(elapsed "$t0")" '>' "$limit" && die "the daemon $pid did not exit in ${limit}s"
    done
    return 0
}

save_logs() {
    local m=$1 tag
    tag="$CUR_STAGE-$m"
    cp "$(daemon_log "$m")" "$W/logs/$tag.telega-fs.log" 2>/dev/null || true
    if [[ -s "$(data_dir "$m")/td.log" ]]; then
        cp "$(data_dir "$m")/td.log" "$W/logs/$tag.td.log" 2>/dev/null || true
    fi
    return 0
}

# Размонтировать и дождаться финального коммита. Время — в LAST_UNMOUNT_S.
unmount_wait() {
    local m=$1 mnt=$2 t0 pid
    t0="$(now)"
    pid="$(cat "$W/$m.pid")"
    fusermount3 -u "$mnt" || die "fusermount3 -u on $m"
    wait_pid "$pid" 900
    wait "${SAMPLER_OF[$m]:-}" 2>/dev/null || true
    LAST_UNMOUNT_S="$(elapsed "$t0")"
    save_logs "$m"
    local fresh
    fresh="$(tail -c "+$((LOG_OFFSET[$m] + 1))" "$(daemon_log "$m")")"
    [[ "$fresh" == *unmounted ]] || [[ "$fresh" == *unmounted$'\n'* ]] || die "no 'unmounted' in the log of $m"
    if grep -q 'final commit failed\|flush on unmount failed' <<<"$fresh"; then
        die "the final commit on $m failed (see its log)"
    fi
}

# --- FLOOD_WAIT и счётчики по логу демона -----------------------------------------------------

flood_lines() {
    local f
    f="$(daemon_log "$1")"
    [[ -r "$f" ]] || { echo 0; return; }
    grep -aci 'flood_wait\|retry after' "$f" || true
}
flood_max_s() {
    local f
    f="$(daemon_log "$1")"
    [[ -r "$f" ]] || { echo 0; return; }
    { grep -aoiE 'FLOOD_(PREMIUM_)?WAIT[_ ]+[0-9]+|retry after [0-9]+' "$f" || true; } |
        { grep -oE '[0-9]+$' || true; } | sort -n | tail -n1 | awk '{ print $1 + 0 }'
}
blocked_in_log() {
    local f
    f="$(daemon_log "$1")"
    [[ -r "$f" ]] && grep -aqE 'USER_DEACTIVATED|AUTH_KEY_UNREGISTERED|SESSION_REVOKED|bot was blocked|bot was kicked|USER_BANNED_IN_CHANNEL' "$f"
}

# Сэмплер: пока жив демон, раз в секунду VmHWM/VmRSS в samples/ИМЯ.csv; раз в 5 с лог на
# FLOOD_WAIT больше часа и блокировку (SPEC 17.2): тогда убивает демона и пишет state/STOP-FLOOD.
sample_start() {
    local name=$1 m=$2 pid=$3
    local csv="$W/samples/$name.csv"
    echo "t_s,vmhwm_kib,vmrss_kib" >"$csv"
    (
        trap - EXIT INT TERM ERR
        set +e +o pipefail
        local t0 i=0 hwm rss fm
        t0="$(now)"
        while kill -0 "$pid" 2>/dev/null; do
            read -r hwm rss < <(awk '/^VmHWM:/ { h = $2 } /^VmRSS:/ { r = $2 } END { print h + 0, r + 0 }' \
                "/proc/$pid/status" 2>/dev/null || echo "0 0")
            echo "$(elapsed "$t0"),$hwm,$rss" >>"$csv"
            if ((i % 5 == 0)); then
                fm="$(flood_max_s "$m")"
                if ((fm > FLOOD_LIMIT_S)) || blocked_in_log "$m"; then
                    echo "FLOOD_WAIT ${fm}s or blocked on $m at $(date -Is)" >"$W/state/STOP-FLOOD"
                    kill -KILL "$pid" 2>/dev/null
                    exit 0
                fi
            fi
            i=$((i + 1))
            sleep 1
        done
    ) &
    sampler_pids+=("$!")
    SAMPLER_PID=$!
}

# --- данные ----------------------------------------------------------------------------------

# Детерминированные псевдослучайные байты: AES-128-CTR по нулям, ключ = sha256(seed).
gen_bytes() {
    local key
    key="$(printf '%s' "$SEED_PREFIX-$1" | sha256sum | cut -c1-32)"
    (
        set +o pipefail
        openssl enc -aes-128-ctr -K "$key" -iv 00000000000000000000000000000000 </dev/zero 2>/dev/null |
            head -c "$2"
    )
}

make_manifest() { # make_manifest КОРЕНЬ ВЫХОД: sha256 файлов, режимы и размеры
    (
        cd "$1"
        find . -type f -printf '%P\n' | LC_ALL=C sort | xargs -d '\n' -r sha256sum
        find . -mindepth 1 -printf '%y %m %P\n' | LC_ALL=C sort
        find . -type f -printf '%m %s %P\n' | LC_ALL=C sort
    ) >"$2"
}

# verify_tree МАШИНА MNT ИМЯ: дерево смонтированной ФС совпадает с ожидаемым ($W/exp)
verify_tree() {
    local mnt=$2 name=$3 nfiles
    make_manifest "$W/exp" "$W/manifest-exp.txt"
    make_manifest "$mnt" "$W/manifest-$name.txt" || die "cannot read the tree on $1 ($name)"
    nfiles="$(find "$W/exp" -type f | wc -l)"
    if diff -q "$W/manifest-exp.txt" "$W/manifest-$name.txt" >/dev/null; then
        verdict "tree-$name" PASS "sha256, modes and sizes of all $nfiles files match"
    else
        diff "$W/manifest-exp.txt" "$W/manifest-$name.txt" | head -n 20 >&2 || true
        verdict "tree-$name" FAIL "the tree on $1 differs from the expected one (see manifest-$name.txt)"
    fi
}

stage_gen() {
    local src="$W/src"
    rm -rf "$src" "$W/exp" "$W/cmp" "$W/late"
    mkdir -p "$src/small" "$src/dir/sub/deeper" "$src/medium" "$src/pack" "$W/late" "$W/cmp"
    log "generating $E2E_BIG1_MIB + $E2E_BIG2_MIB MiB files, small files (inline, packs), directories"
    gen_bytes big1 "$((E2E_BIG1_MIB * 1048576))" >"$src/big1.bin"
    gen_bytes big2 "$((E2E_BIG2_MIB * 1048576))" >"$src/big2.bin"
    gen_bytes five $((5 * 1048576 + 17)) >"$src/five.bin"        # больше pack_threshold: свой чанк
    local n
    for n in 1 2 3 4 5 6 7 8 9 10 11 12; do                        # инлайн: до 4 КиБ
        gen_bytes "s$n" $((n * 300)) >"$src/small/s$n.dat"
    done
    for n in 1 2 3 4 5 6; do                                       # пакеты: 5 КиБ .. 3 МиБ
        gen_bytes "m$n" $((n * n * 90 * 1024)) >"$src/medium/m$n.dat"
    done
    for n in $(seq 1 "$E2E_PACK_FILES"); do                        # мелкие 5-200 КиБ: пакеты
        mkdir -p "$src/pack/d$((n % 6))"
        gen_bytes "p$n" $(((5 + (n * 7919) % 196) * 1024)) >"$src/pack/d$((n % 6))/f$n.dat"
    done
    : >"$src/empty.txt"
    echo "hello" >"$src/dir/hello.txt"
    gen_bytes seven 7000 >"$src/dir/sub/deeper/seven.dat"
    printf 'name with spaces and \xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82' >"$src/dir/sub/имя файла.txt"
    chmod 640 "$src/dir/hello.txt"
    chmod 755 "$src/dir"
    # Пачка для gc --compact (этап compact): пишется отдельно, чтобы лечь в свой пакет.
    for n in $(seq 1 "$E2E_CMP_FILES"); do
        gen_bytes "c$n" $(((100 + (n * 37) % 100) * 1024)) >"$W/cmp/k$n.dat"
    done
    printf 'written after sync\n' >"$W/late/after_sync.txt"
    printf 'marker for the commit before gc\n' >"$W/late/marker.txt"
    # Ожидаемое дерево: жёсткие ссылки на src (место не занимают); этапы дополняют его по ходу.
    cp -al "$src" "$W/exp"
    local inline_max=4096 pack_thr=$((4 * 1048576)) chunk_b=$((E2E_CHUNK_MIB * 1048576))
    res FILES_NOT_INLINE "$(find "$src" -type f -size +"${inline_max}"c | wc -l)"
    res FILES_PACKED "$(find "$src" -type f -size +"${inline_max}"c -size -"${pack_thr}"c | wc -l)"
    res LARGE_CHUNKS "$(find "$src" -type f -size +"$((pack_thr - 1))"c -printf '%s\n' |
        awk -v c="$chunk_b" '{ n += int(($1 + c - 1) / c) } END { print n + 0 }')"
    res BIG1_CHUNKS "$(((E2E_BIG1_MIB * 1048576 + chunk_b - 1) / chunk_b))"
    res SRC_FILES "$(find "$src" -type f | wc -l)"
    res SRC_BYTES "$(find "$src" -type f -printf '%s\n' | awk '{ s += $1 } END { print s + 0 }')"
}

stage_channel() {
    if [[ "$E2E_STORE" == local ]]; then
        mkdir -p "$W/channel"
        return 0
    fi
    if [[ "${E2E_REUSE_CHANNEL:-0}" == 1 ]]; then
        [[ -r "$E2E_CHAT_FILE" ]] || die "E2E_REUSE_CHANNEL=1 but E2E_CHAT_FILE does not exist"
        touch "$W/state/channel-reused"
        return 0
    fi
    rm -f "$W/state/channel-deleted" "$W/state/channel-reused"
    log "creating the channel (user account: setup only, no messages, SPEC 17.2)"
    bench e2e --chat-file "$E2E_CHAT_FILE" setup >"$W/logs/setup.out" 2>&1 ||
        { tail -n 5 "$W/logs/setup.out" >&2; die "channel setup (see logs/setup.out)"; }
}

stage_a_init() {
    logout_machine A
    [[ -e "$W/A/mnt" ]] && mountpoint -q "$W/A/mnt" && die "$W/A/mnt is still mounted"
    rm -rf "${W:?}/A"
    write_config A
    login_bot A
    if [[ "$E2E_STORE" == telegram ]]; then
        local chat_id
        chat_id="$(cat "$E2E_CHAT_FILE")"
        tgfs A init --chat "$chat_id" >"$W/logs/init-A.out" 2>&1 ||
            { tail -n 8 "$W/logs/init-A.out" >&2; die "init on A"; }
        grep -q "^chat_id *= *$chat_id" "$W/A/config/telega-fs/config.toml" ||
            die "chat_id was not saved in config.toml of A"
        grep -q 'api_hash' "$W/A/config/telega-fs/config.toml" && die "api_hash leaked into config.toml"
    else
        tgfs A init >"$W/logs/init-A.out" 2>&1 || { tail -n 8 "$W/logs/init-A.out" >&2; die "init on A"; }
    fi
    log "init ok"
}

# --- status и sync -----------------------------------------------------------------------------

# status_json МАШИНА ИМЯ: status --json в $W/status-ИМЯ.json; при ошибке жёсткий отказ
status_json() {
    tgfs "$1" status --json >"$W/status-$2.json" 2>"$W/logs/status-$2.err" ||
        { cat "$W/logs/status-$2.err" >&2; die "status --json ($2)"; }
    jq -e . "$W/status-$2.json" >/dev/null || die "status --json ($2) is not valid JSON"
}
sj() { jq -r ".$2" "$W/status-$1.json"; }   # sj ИМЯ ПОЛЕ

inspect_run() { # inspect_run ИМЯ: вывод в logs/inspect-ИМЯ.out, код 0 при INSPECT OK
    bench e2e --chat-file "$E2E_CHAT_FILE" inspect >"$W/logs/inspect-$1.out" 2>&1 || true
    grep -v '^\[' "$W/logs/inspect-$1.out" | sed 's/^/[e2e]   /'
    grep -q '^INSPECT OK' "$W/logs/inspect-$1.out"
}
docs_bin() { sed -n 's/^documents \.bin: //p' "$W/logs/inspect-$1.out" | head -n1; }

stage_a_write() {
    local mnt="$W/A/mnt" t
    t=$(now)
    mount_on A "$mnt"
    res T_MOUNT_A "$(elapsed "$t")"
    log "mounted A in $T_MOUNT_A s"

    status_json A before
    res GEN_BEFORE "$(sj before generation)"
    tgfs A status >"$W/logs/status-before.txt" 2>&1 || die "status (text)"
    check_if status-before "$([[ "$(sj before mounted)" == true && "$(sj before dirty_chunks)" == 0 &&
        "$(sj before dirty_bytes)" == 0 && "$(sj before index_dirty)" == false ]] && echo 1 || echo 0)" \
        "mounted, generation $GEN_BEFORE, no dirty data"
    check_if status-text "$(b2i grep -q 'state: *mounted' "$W/logs/status-before.txt")" \
        "text output has the state line"

    t=$(now)
    cp -r "$W/src/." "$mnt/" || die "cp to the mount failed"
    res T_WRITE_BASE "$(elapsed "$t")"
    status_json A during
    res DIRTY_DURING "$(sj during dirty_bytes)"
    res UPLOADED_DURING "$(sj during uploaded_chunks)"
    log "status after cp: generation $(sj during generation), dirty chunks $(sj during dirty_chunks) / $(mib "$DIRTY_DURING") MiB, uploaded $UPLOADED_DURING, upload queue $(sj during upload_queued)+$(sj during upload_active), index_dirty $(sj during index_dirty)"
    check_if status-after-write "$([[ "$(sj during index_dirty)" == true || "$DIRTY_DURING" -gt 0 ||
        "$UPLOADED_DURING" -gt 0 ]] && echo 1 || echo 0)" \
        "status sees the write: index_dirty $(sj during index_dirty), dirty $DIRTY_DURING B, uploaded chunks $UPLOADED_DURING"

    t=$(now)
    tgfs A sync >"$W/logs/sync-1.out" 2>&1 || { cat "$W/logs/sync-1.out" >&2; die "sync failed"; }
    res T_SYNC_BASE "$(elapsed "$t")"
    status_json A after
    res GEN_AFTER "$(sj after generation)"
    log "sync returned in $T_SYNC_BASE s: $(cat "$W/logs/sync-1.out"); generation $GEN_BEFORE -> $GEN_AFTER"
    check_if sync-generation "$((GEN_AFTER > GEN_BEFORE ? 1 : 0))" "generation $GEN_BEFORE -> $GEN_AFTER after sync"
    check_if sync-clean "$([[ "$(sj after dirty_chunks)" == 0 && "$(sj after dirty_bytes)" == 0 &&
        "$(sj after index_dirty)" == false && "$(sj after upload_failed)" == 0 &&
        "$(sj after upload_queued)" == 0 && "$(sj after upload_active)" == 0 ]] && echo 1 || echo 0)" \
        "no dirty chunks/bytes, index committed, upload queue empty (dirty $(sj after dirty_chunks), queue $(sj after upload_queued), failed $(sj after upload_failed))"
    check_if status-errors-empty "$([[ "$(jq '.errors | length' "$W/status-after.json")" == 0 ]] && echo 1 || echo 0)" \
        "no errors in status: $(jq -c .errors "$W/status-after.json")"

    # sync не ломает смонтированную ФС: дерево читается целиком, запись после sync работает.
    cp "$W/late/after_sync.txt" "$mnt/after_sync.txt" || die "write after sync failed"
    ln -f "$W/late/after_sync.txt" "$W/exp/after_sync.txt"
    verify_tree A "$mnt" after-sync
    tgfs A sync >"$W/logs/sync-2.out" 2>&1 || { cat "$W/logs/sync-2.out" >&2; die "second sync failed"; }
    status_json A after2
    check_if sync-again "$(($(sj after2 generation) > GEN_AFTER ? 1 : 0))" \
        "second sync after one more write: generation $GEN_AFTER -> $(sj after2 generation)"
    unmount_wait A "$mnt"
    res T_UNMOUNT_BASE "$LAST_UNMOUNT_S"

    if [[ "$E2E_STORE" == telegram ]]; then
        local ok=0
        inspect_run base && ok=1
        check_if inspect-names "$ok" "document names <16 hex>.bin|.idx, no captions"
        res DOCS_BIN_BASE "$(docs_bin base)"
        check_if small-files-fewer-docs "$((DOCS_BIN_BASE < FILES_NOT_INLINE ? 1 : 0))" \
            ".bin documents: $DOCS_BIN_BASE, files outside inline: $FILES_NOT_INLINE"
    fi
}

stage_a_dedup() {
    local mnt="$W/A/mnt" t
    mount_on A "$mnt"
    status_json A d0
    local ded0 up0 ded1 up1 sha_a sha_b sha_src
    ded0="$(sj d0 deduped_chunks)"
    up0="$(sj d0 uploaded_chunks)"
    t=$(now)
    cp "$mnt/big1.bin" "$mnt/big1_copy.bin" || die "cp of the big file failed"
    ln -f "$W/src/big1.bin" "$W/exp/big1_copy.bin"
    tgfs A sync >"$W/logs/sync-dedup.out" 2>&1 || { cat "$W/logs/sync-dedup.out" >&2; die "sync after the copy failed"; }
    res T_DEDUP_COPY "$(elapsed "$t")"
    status_json A d1
    ded1="$(sj d1 deduped_chunks)"
    up1="$(sj d1 uploaded_chunks)"
    res DEDUP_NEW "$((ded1 - ded0))"
    res DEDUP_UPLOADED "$((up1 - up0))"
    log "copy of $E2E_BIG1_MIB MiB + sync in $T_DEDUP_COPY s: deduplicated $((ded1 - ded0)) of $BIG1_CHUNKS chunks, uploaded $((up1 - up0))"
    check_if dedup-status "$((DEDUP_NEW >= BIG1_CHUNKS ? 1 : 0))" \
        "deduplicated_chunks +$DEDUP_NEW (the file has $BIG1_CHUNKS chunks)"
    check_if dedup-uploaded "$((DEDUP_UPLOADED <= 1 ? 1 : 0))" \
        "uploaded_chunks +$DEDUP_UPLOADED after copying a file of $BIG1_CHUNKS chunks (tolerance 1)"
    sha_a="$(sha256sum "$mnt/big1.bin" | cut -d' ' -f1)"
    sha_b="$(sha256sum "$mnt/big1_copy.bin" | cut -d' ' -f1)"
    sha_src="$(sha256sum "$W/src/big1.bin" | cut -d' ' -f1)"
    check_if dedup-sha "$([[ "$sha_a" == "$sha_b" && "$sha_a" == "$sha_src" ]] && echo 1 || echo 0)" \
        "sha256 of the original and the copy match the source"
    unmount_wait A "$mnt"
    if [[ "$E2E_STORE" == telegram ]]; then
        local ok=0
        inspect_run dedup && ok=1
        res DOCS_BIN_DEDUP "$(docs_bin dedup)"
        local delta=$((DOCS_BIN_DEDUP - DOCS_BIN_BASE))
        res DOCS_BIN_DELTA "$delta"
        check_if dedup-channel "$((delta <= 1 && ok == 1 ? 1 : 0))" \
            "new .bin documents in the channel after the copy: $delta (before $DOCS_BIN_BASE, after $DOCS_BIN_DEDUP; the copy has $BIG1_CHUNKS chunks)"
    fi
}

# --- потеря головы -------------------------------------------------------------------------------

stage_drop_head() {
    status_json A pre-drop
    res GEN_PRE_DROP "$(sj pre-drop generation)"
    res HEAD_PRE_DROP "$(sj pre-drop head_msg_id)"
    if [[ "$E2E_STORE" == telegram ]]; then
        bench e2e --chat-file "$E2E_CHAT_FILE" drop-head >"$W/logs/drop-head.out" 2>&1 ||
            { cat "$W/logs/drop-head.out" >&2; die "drop-head (the bot unpins and deletes the head)"; }
        sed 's/^/[e2e]   /' "$W/logs/drop-head.out"
        check_if head-dropped "$(b2i grep -q '^after: pinned message none, head message deleted' "$W/logs/drop-head.out")" \
            "the bot unpinned everything and deleted the head (generation $GEN_PRE_DROP)"
    else
        verdict head-dropped SKIP "store = local: use E2E_STORE=telegram for the real scenario"
    fi
}

# Монтирование должно отказать: код выхода не 0, в тексте подсказка про --ro и --force-local.
stage_refuse() {
    local t out rc=0
    t=$(now)
    mkdir -p "$W/A/mnt"
    # shellcheck disable=SC2086
    out="$(timeout 600 env XDG_CONFIG_HOME="$W/A/config" XDG_DATA_HOME="$W/A/data" \
        XDG_CACHE_HOME="$W/A/cache" $E2E_TRACE_ENV "$TGFS_BIN" $E2E_TRACE_ARGS mount "$W/A/mnt" 2>&1)" || rc=$?
    echo "$out" >"$W/logs/mount-refused.out"
    res T_REFUSE "$(elapsed "$t")"
    mountpoint -q "$W/A/mnt" && die "the refused mount left a mountpoint"
    echo "$out" | tail -n 6 | sed 's/^/[e2e]   /'
    check_if mount-refused "$((rc != 0 ? 1 : 0))" "exit code $rc after $T_REFUSE s (the 90 s TDLib window of SPEC 7.2 included)"
    check_if mount-hint "$([[ "$out" == *"--ro"* && "$out" == *"--force-local"* ]] && echo 1 || echo 0)" \
        "the message mentions --ro and --force-local"

    # --ro читает данные при потерянной голове.
    mount_on A "$W/A/mnt" --ro
    status_json A ro
    check_if ro-mounted "$([[ "$(sj ro read_only)" == true && "$(sj ro generation)" == "$GEN_PRE_DROP" ]] && echo 1 || echo 0)" \
        "mounted read-only, generation $(sj ro generation)"
    verify_tree A "$W/A/mnt" ro-lost-head
    unmount_wait A "$W/A/mnt"
}

stage_force_local() {
    local mnt="$W/A/mnt" t
    t=$(now)
    mount_on A "$mnt" --force-local
    res T_FORCE_MOUNT "$(elapsed "$t")"
    status_json A force
    check_if force-pending-snapshot "$([[ "$(sj force force_snapshot)" == true && "$(sj force generation)" == "$GEN_PRE_DROP" ]] && echo 1 || echo 0)" \
        "the next commit is a snapshot, generation still $(sj force generation)"
    verify_tree A "$mnt" force-local
    unmount_wait A "$mnt"
    status_json A forced
    local g
    g="$(sj forced generation)"
    res GEN_FORCED "$g"
    res HEAD_FORCED "$(sj forced head_msg_id)"
    check_if force-new-generation "$((g == GEN_PRE_DROP + 1 ? 1 : 0))" \
        "generation $GEN_PRE_DROP -> $g"
    check_if force-head-snapshot "$([[ "$(sj forced chain_length)" == 1 && "$(sj forced force_snapshot)" == false &&
        "$(sj forced index_dirty)" == false && "$HEAD_FORCED" != "$HEAD_PRE_DROP" ]] && echo 1 || echo 0)" \
        "the new head is a snapshot: chain of $(sj forced chain_length) element(s), nothing pending, new head message differs from the lost one"
    if [[ "$E2E_STORE" == telegram ]]; then
        local ok=0
        inspect_run force && ok=1
        check_if inspect-names-force "$ok" "document names are fine after the forced commit"
    fi
}

stage_fresh_ro() {
    fresh_machine B1
    local mnt="$W/B1/mnt" t
    t=$(now)
    mount_on B1 "$mnt" --ro
    res T_MOUNT_B1 "$(elapsed "$t")"
    status_json B1 b1
    check_if b1-ro "$([[ "$(sj b1 read_only)" == true && "$(sj b1 generation)" == "$GEN_FORCED" ]] && echo 1 || echo 0)" \
        "another machine, empty directories, --ro: generation $(sj b1 generation)"
    t=$(now)
    verify_tree B1 "$mnt" B1
    res T_VERIFY_B1 "$(elapsed "$t")"
    res FLOOD_B1 "$(flood_lines B1)"
    unmount_wait B1 "$mnt"
    logout_machine B1
    rm -rf "${W:?}/B1/cache"
}

# --- обслуживание ---------------------------------------------------------------------------------

# maint_cmd ИМЯ КОД_ОЖИДАЕМЫЙ КОМАНДА...: вывод в logs/ИМЯ.out
maint_cmd() {
    local name=$1 want=$2 rc=0
    shift 2
    local t
    t=$(now)
    tgfs A "$@" >"$W/logs/$name.out" 2>&1 || rc=$?
    res "T_${name//[^A-Za-z0-9]/_}" "$(elapsed "$t")"
    sed 's/^/[e2e]   /' "$W/logs/$name.out" | grep -v '^\[e2e\]   \[' | tail -n 8 || true
    check_if "$name" "$((rc == want ? 1 : 0))" "telega-fs $* exited with $rc (expected $want)"
}

stage_fsck1() {
    maint_cmd fsck 0 fsck
    grep -q '^OK' "$W/logs/fsck.out" || verdict fsck-text FAIL "no 'OK' in the fsck output"
    maint_cmd fsck-verify 0 fsck --verify-data
    check_if fsck-verified "$(b2i grep -q 'chunks verified' "$W/logs/fsck-verify.out")" \
        "fsck --verify-data checked the chunk tags: $(grep -o '[0-9]* chunks verified' "$W/logs/fsck-verify.out" | head -n1)"
    maint_cmd gc 0 gc
}

stage_compact() {
    local mnt="$W/A/mnt" n keep=3
    # retain_generations = 1: после уплотнения старые пакеты и блобы реально удаляются из канала,
    # и fsck --verify-data ниже доказывает, что живые данные целы.
    grep -q '^retain_generations' "$W/A/config/telega-fs/config.toml" ||
        echo "retain_generations = 1" >>"$W/A/config/telega-fs/config.toml"
    # 1. Пачка мелких файлов своим пакетом, sync (пакет закрывается и коммитится).
    mount_on A "$mnt"
    mkdir -p "$mnt/cmp" "$W/exp/cmp"
    cp -r "$W/cmp/." "$mnt/cmp/" || die "cp of the compaction batch failed"
    cp -al "$W/cmp/." "$W/exp/cmp/" 2>/dev/null || true
    tgfs A sync >"$W/logs/sync-cmp1.out" 2>&1 || { cat "$W/logs/sync-cmp1.out" >&2; die "sync of the batch failed"; }
    # 2. Удаляем все, кроме нескольких: пакет почти мёртв (живых < compact_ratio = 25 %), commit.
    for n in $(seq $((keep + 1)) "$E2E_CMP_FILES"); do
        rm "$mnt/cmp/k$n.dat" || die "rm of k$n failed"
        rm -f "$W/exp/cmp/k$n.dat"
    done
    tgfs A sync >"$W/logs/sync-cmp2.out" 2>&1 || { cat "$W/logs/sync-cmp2.out" >&2; die "sync after the removal failed"; }
    unmount_wait A "$mnt"
    # 3. Уплотнение без FUSE.
    maint_cmd gc-compact 0 gc --compact
    local line packs
    line="$(grep '^compact:' "$W/logs/gc-compact.out" || true)"
    packs="$(sed -n 's/^compact: \([0-9]*\) pack(s).*/\1/p' <<<"$line")"
    res COMPACT_PACKS "${packs:-0}"
    check_if compact-did-work "$([[ "${packs:-0}" -ge 1 && "$line" == *committed* && "$line" != *"nothing to commit"* ]] && echo 1 || echo 0)" \
        "$line"
    # 4. Ещё один коммит: на нём шаг 9 (retain_generations = 1) удаляет мёртвые пакеты из канала.
    mount_on A "$mnt"
    cp "$W/late/marker.txt" "$mnt/marker.txt" || die "write of the marker failed"
    ln -f "$W/late/marker.txt" "$W/exp/marker.txt"
    tgfs A sync >"$W/logs/sync-cmp3.out" 2>&1 || { cat "$W/logs/sync-cmp3.out" >&2; die "sync of the marker failed"; }
    unmount_wait A "$mnt"
    maint_cmd gc-after 0 gc
    # 5. Целостность после удаления старых пакетов.
    maint_cmd fsck-verify-after 0 fsck --verify-data
    check_if fsck-after-compact "$(b2i grep -q '^OK' "$W/logs/fsck-verify-after.out")" \
        "no problems after the compaction and the removal of old packs: $(grep -o '[0-9]* chunks verified' "$W/logs/fsck-verify-after.out" | head -n1)"
    # 6. Другая машина читает оставшееся.
    fresh_machine B2
    mount_on B2 "$W/B2/mnt" --ro
    verify_tree B2 "$W/B2/mnt" B2-compact
    res FLOOD_B2 "$(flood_lines B2)"
    unmount_wait B2 "$W/B2/mnt"
    logout_machine B2
    rm -rf "${W:?}/B2/cache"
}

# --- сводка ----------------------------------------------------------------------------------

longest_flood() {
    local m v max=0
    for m in A B1 B2; do
        [[ -r "$(daemon_log "$m")" ]] || continue
        v="$(flood_max_s "$m")"
        ((v > max)) && max=$v
    done
    echo "$max"
}

print_summary() {
    local fails passes skips total_flood=0 m n t=0 s v
    fails=$(grep -c '|FAIL|' "$W/checks.txt" || true)
    passes=$(grep -c '|PASS|' "$W/checks.txt" || true)
    skips=$(grep -c '|SKIP|' "$W/checks.txt" || true)
    for m in A B1 B2; do
        [[ -r "$(daemon_log "$m")" ]] || continue
        n="$(flood_lines "$m")"
        total_flood=$((total_flood + n))
    done
    for s in "${STAGES[@]}"; do
        v="T_STAGE_$s"
        [[ -n "${!v:-}" ]] && t="$(awk -v a="$t" -v b="${!v}" 'BEGIN { printf "%.1f", a + b }')"
    done
    {
        echo "[e2e] ---- summary ($(date -Is), store: $E2E_STORE) ----"
        echo "[e2e] env: telega-fs $(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo '?') + uncommitted M4; $(uname -sr); fs of the work dir: $(stat -f -c %T "$W")"
        echo "[e2e] data: ${SRC_FILES:-?} files / $(mib "${SRC_BYTES:-0}") MiB in the base set (big files $E2E_BIG1_MIB and $E2E_BIG2_MIB MiB), chunk_size ${E2E_CHUNK_MIB} MiB; seed prefix '$SEED_PREFIX'"
        echo "[e2e] stage time, s:$(for s in "${STAGES[@]}"; do v="T_STAGE_$s"; [[ -n "${!v:-}" ]] && printf ' %s %s;' "$s" "${!v}"; done) total $t"
        echo "[e2e] sync: first ${T_SYNC_BASE:-n/a} s (generation ${GEN_BEFORE:-?} -> ${GEN_AFTER:-?}); dedup copy: ${DEDUP_NEW:-n/a} chunks deduplicated, ${DEDUP_UPLOADED:-n/a} uploaded, ${DOCS_BIN_DELTA:-n/a} new .bin documents in the channel"
        echo "[e2e] lost head: generation ${GEN_PRE_DROP:-?}; mount refused after ${T_REFUSE:-n/a} s; --force-local mount ${T_FORCE_MOUNT:-n/a} s; new generation ${GEN_FORCED:-?}"
        echo "[e2e] compaction: ${COMPACT_PACKS:-n/a} pack(s); documents .bin: base ${DOCS_BIN_BASE:-n/a}, after dedup ${DOCS_BIN_DEDUP:-n/a}, at the end ${DOCS_BIN_END:-n/a}; last server message id in the channel ${LAST_MSG_ID:-n/a} (upper bound of messages sent by the bot incl. deleted)"
        echo "[e2e] FLOOD_WAIT lines in daemon logs: $total_flood; longest wait $(longest_flood) s; [error] lines ${DAEMON_ERRORS:-n/a}"
        echo "[e2e] checks: $passes PASS, $fails FAIL, $skips SKIP"
        awk -F'|' '$3 == "FAIL" { printf "[e2e]   FAIL %s: %s (stage %s)\n", $2, $4, $1 }' "$W/checks.txt"
    } | tee "$W/summary.txt"
}

stage_final() {
    if [[ "$E2E_STORE" == telegram ]]; then
        local ok=0
        inspect_run end && ok=1
        check_if inspect-names-end "$ok" "document names <16 hex>.bin|.idx, no captions (after everything)"
        res DOCS_BIN_END "$(docs_bin end)"
        res LAST_MSG_ID "$(sed -n 's/^head server id: //p' "$W/logs/inspect-end.out")"
    fi
    local leaks=0 f
    for f in "$W"/logs/* "$W"/*/data/telega-fs/main/*.log; do
        [[ -r "$f" ]] || continue
        if [[ -n "$bot_token" ]] && { grep -aqF -- "$bot_token" "$f" || grep -aqF -- "$TELEGA_API_HASH" "$f"; }; then
            leaks=$((leaks + 1))
            echo "[e2e] secret found in a log: ${f##*/}" >&2
        fi
    done
    check_if no-secrets-in-logs "$((leaks == 0 ? 1 : 0))" "$leaks log files with a token or api_hash"
    local errors=0 m n
    for m in A B1 B2; do
        [[ -r "$(daemon_log "$m")" ]] || continue
        n="$(grep -ac '\[error\]' "$(daemon_log "$m")" || true)"
        errors=$((errors + ${n:-0}))
    done
    res DAEMON_ERRORS "$errors"
    log "[error] lines in daemon logs: $errors"
}

# --- запуск этапов --------------------------------------------------------------------------

done_file() { echo "$W/state/$1.done"; }

run_stage() {
    local name=$1 t0
    if [[ -e "$(done_file "$name")" ]]; then
        log "stage $name: done earlier, skipping"
        return 0
    fi
    CUR_STAGE="$name"
    grep -v "^$name|" "$W/checks.txt" >"$W/checks.txt.new" || true
    mv "$W/checks.txt.new" "$W/checks.txt"
    log "=== stage $name ==="
    t0=$(now)
    "stage_$name"
    res "T_STAGE_$name" "$(elapsed "$t0")"
    touch "$(done_file "$name")"
    log "=== stage $name done in $(elapsed "$t0") s ==="
}

teardown_channel() {
    [[ "$E2E_STORE" == telegram && -r "$E2E_CHAT_FILE" ]] || return 0
    log "teardown: deleting the channel (user account: delete only)"
    bench e2e --chat-file "$E2E_CHAT_FILE" teardown >"$W/logs/teardown.out" 2>&1 ||
        { echo "[e2e] teardown failed, delete the channel by hand (see $W/logs/teardown.out)" >&2; return 1; }
}

show_status() {
    local s
    for s in "${STAGES[@]}"; do
        printf '%-12s %s\n' "$s" "$([[ -e "$(done_file "$s")" ]] && echo done || echo pending)"
    done
    [[ -e "$W/state/STOP-FLOOD" ]] && echo "STOP-FLOOD: $(cat "$W/state/STOP-FLOOD")"
    awk -F'|' '{ printf "%-5s %-24s %s\n", $3, $2, $4 }' "$W/checks.txt"
}

case "$sub" in
    status) show_status; exit 0 ;;
    teardown) teardown_channel; exit $? ;;
esac

for s in ${E2E_REDO:-}; do rm -f "$(done_file "$s")"; done
if [[ -e "$W/state/STOP-FLOOD" ]]; then
    echo "[e2e] a previous run stopped on FLOOD_WAIT or a block: $(cat "$W/state/STOP-FLOOD")" >&2
    echo "[e2e] the user decides whether to continue; remove $W/state/STOP-FLOOD to resume" >&2
    exit "$EXIT_STOP"
fi

wanted=("$@")
if [[ ${#wanted[@]} -eq 0 ]]; then
    wanted=("${STAGES[@]}")
fi
for s in "${wanted[@]}"; do
    printf '%s\n' "${STAGES[@]}" | grep -qx "$s" || env_die "unknown stage: $s (stages: ${STAGES[*]})"
done
if [[ $# -gt 0 ]]; then
    for s in "$@"; do rm -f "$(done_file "$s")"; done
fi

for s in "${STAGES[@]}"; do
    printf '%s\n' "${wanted[@]}" | grep -qx "$s" || continue
    run_stage "$s"
done

if [[ -e "$(done_file final)" ]]; then
    print_summary
    if [[ "${E2E_KEEP_SESSIONS:-0}" != 1 ]]; then
        logout_machine A
    fi
    # Канал удаляется только если все проверки PASS: при FAIL данные нужны для разбора.
    if grep -q '|FAIL|' "$W/checks.txt"; then
        echo "[e2e] the channel is kept because of FAIL checks; delete it with: $0 teardown"
    elif [[ "${E2E_KEEP_CHANNEL:-0}" != 1 && ! -e "$W/state/channel-deleted" &&
        ! -e "$W/state/channel-reused" ]]; then
        teardown_channel && touch "$W/state/channel-deleted" || true
    fi
    if grep -q '|FAIL|' "$W/checks.txt"; then
        echo "[e2e] E2E FAIL (work dir kept: $W)"
        exit "$EXIT_CHECKS"
    fi
    echo "[e2e] E2E PASS (work dir kept: $W)"
fi
exit 0
