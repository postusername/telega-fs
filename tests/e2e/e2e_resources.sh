#!/usr/bin/env bash
# Ресурсный E2E telega-fs на живом Telegram в режиме бота: скорость и память на больших данных.
# Запускается вручную. Дополняет e2e_basic.sh (базовый путь данных): здесь холодное чтение с
# опережающим чтением, видео, файл 10 ГиБ при staging_limit 2 ГиБ и кэш чтения с лимитом.
#
# Что проверяет:
#   - холодное последовательное чтение 200 МиБ с «машины» с пустым кэшем не медленнее порога
#     (по умолчанию 6 МиБ/с), sha256 и дерево совпадают с эталоном;
#   - перемотка видео (около 300 МиБ, +faststart) ffmpeg -ss в трёх точках скачивает не больше
#     3 * (readahead_max + 2 * chunk_size) + заголовок, то есть малую долю файла;
#   - мелкие файлы лежат в пакетах: документов в канале заметно меньше, чем файлов;
#   - копирование потока 10 ГиБ при staging_limit 2 ГиБ: пик VmHWM демона меньше 256 МиБ, staging
#     не выше лимита;
#   - холодное чтение этого файла целиком при cache_limit 4 ГиБ: sha256 совпадает, каталог кэша
#     TDLib не выше cache_limit плюс чанк;
#   - в конце: имена документов <16 hex>.bin|.idx без подписей, секреты не попали в логи.
#
# Что нужно:
#   - собранные telega-fs и tgfs-bench (-DTELEGA_FS_BUILD_TOOLS=ON); по умолчанию
#     build/dev/src/telega-fs и build/dev/tools/bench/tdbench/tgfs-bench, иначе TGFS_BIN и BENCH_BIN;
#   - fusermount3, openssl, sha256sum, awk, find, ffmpeg и ffprobe с libx264;
#   - место: около 8 ГиБ в E2E_WORK (по умолчанию build/e2e-resources-work): src 0,6 ГиБ, staging
#     до 2 ГиБ, кэш чтения до 4 ГиБ;
#   - креды Telegram: файл E2E_CREDS (по умолчанию cred.secret в корне репозитория, строки
#     api-id=..., api-hash=..., bot-token=...) либо TELEGA_API_ID, TELEGA_API_HASH,
#     TELEGA_BOT_TOKEN. Секреты не печатаются и в логи не попадают (в конце логи проверяются на
#     токен и api_hash);
#   - канал. Два варианта:
#       E2E_CHAT_ID=-100... (рекомендуется): ваш готовый приватный канал, где бот администратор
#         с правами публиковать, редактировать и удалять сообщения. Аккаунт пользователя не нужен,
#         канал не создаётся и не удаляется. Канал должен быть пустым (без закрепа и документов):
#         иначе этап channel откажется работать, чтобы не затереть чужую ФС (E2E_WIPE_FIRST=1
#         разрешает боту убрать документы telega-fs и закреп). Если все проверки PASS, этап final
#         удаляет ботом сообщения telega-fs в канале (E2E_KEEP_CHANNEL=1 оставляет); при FAIL они
#         остаются для разбора, убрать: tests/e2e/e2e_resources.sh teardown.
#         Проверка пустоты занимает около 1-2 минут и растёт с числом сообщений
#         (закреп ждётся до минуты, затем бот отправляет и удаляет текст-зонд, его id задаёт
#         верхнюю границу, и перебирает id 1..зонд пачками по 100 с паузой 10 с: на канале, где
#         id дошли до N, это около N/10 с сверх минуты). Бот чистит канал только если
#         этот E2E_WORK сам проверил его пустым (маркер state/channel-claimed с тем же id); с другим
#         E2E_CHAT_ID рабочий каталог не берётся. Закреплено что-то кроме головы telega-fs: уборка
#         отказывает и ничего не удаляет;
#       без E2E_CHAT_ID: канал создаёт и удаляет аккаунт пользователя в tgfs-bench (`tgfs-bench e2e
#         setup|teardown`, вход пользователя: `tgfs-bench --qr login`, см. tools/bench).
#         Это только настройка канала, ни одного сообщения с этого аккаунта.
#
# Время и трафик: 1,5-2 часа живого прогона. gen около 5 минут (ffmpeg), a_write около 5 минут,
# b1_read и b2_video около 10 минут, a_huge около 50 минут (упор в скорость отправки), b3_read
# 20-30 минут, остальное около 10 минут (паузы кэша TDLib после init и перед inspect). Трафик:
# около 11 ГиБ исходящего (в основном поток 10 ГиБ) и около 10,5 ГиБ входящего. Чтобы прогнать
# быстрее и дешевле, уменьшите E2E_HUGE_GIB; проверка staging_limit имеет смысл при значении
# больше staging_limit.
#
# Запуск:
#   tests/e2e/e2e_resources.sh [ЭТАП...]   # без аргументов: все неготовые этапы по порядку
#   tests/e2e/e2e_resources.sh status      # какие этапы готовы, проверки и результаты
#   tests/e2e/e2e_resources.sh teardown    # удалить канал (или, с E2E_CHAT_ID, сообщения в нём);
#                                          # final делает это сам только при всех PASS
#   E2E_CHAT_ID=-1001234567890 tests/e2e/e2e_resources.sh
#   tests/e2e/e2e_resources.sh channel gen a_init a_write    # только эти этапы (заново)
#   E2E_REDO="channel a_init a_write b1_read b2_video inspect1 a_huge b3_read final" \
#     tests/e2e/e2e_resources.sh             # повтор в новом канале; gen (данные в $E2E_WORK/src) не нужен
#   TELEGA_FS_E2E_RUN=1 ctest --test-dir build/dev -L e2e -R e2e_resources   # то же через ctest
#
# Этапы (каждый возобновляемый: маркер готовности в $E2E_WORK/state/<этап>.done):
#   channel   канал: проверка пустоты своего (E2E_CHAT_ID) или создание аккаунтом пользователя
#   gen       данные-эталон: 200 МиБ, мелкие файлы, видео ~300 МиБ (ffmpeg, +faststart), манифест
#   a_init    машина A: login --bot, init --chat
#   a_write   A: монтирование, запись всего, fsync, размонтирование
#   b1_read   машина B1 (пустые каталоги): холодное чтение 200 МиБ (>= 6 МиБ/с), сверка дерева
#   b2_video  машина B2 (пустой кэш): ffmpeg -ss в трёх точках, скачано < 15 % файла
#   inspect1  tgfs-bench e2e inspect: имена документов, мелкие файлы лежат в пакетах
#   a_huge    A: копирование 10 ГиБ потоком при staging_limit 2 ГиБ: VmHWM < 256 МиБ, staging <= лимита
#   b3_read   машина B3 (пустой кэш, cache_limit 4 ГиБ): холодное чтение всего файла, sha256,
#             tdfiles/ <= cache_limit + чанк
#   final     inspect, секреты в логах, FLOOD_WAIT, сводка, logout, удаление канала или уборка в нём
#
# Коды выхода:
#   0  все этапы прошли и все проверки PASS
#   1  все этапы выполнены, но есть проверки FAIL (список в сводке)
#   2  неверные аргументы или окружение (нет бинарников, кредов, ffmpeg, канал не пуст); ничего не
#      запускалось
#   3  остановка по правилу безопасности: FLOOD_WAIT больше часа или бот заблокирован/сессия убита.
#      Демон убит, канал и данные оставлены, решение за пользователем (файл state/STOP-FLOOD)
#   4  жёсткая ошибка этапа (не смонтировалось, команда упала); этап не отмечен готовым, прогон
#      можно повторить той же командой
#   130/143  прерван Ctrl-C / SIGTERM (монтирования убираются в trap)
# Проверки порогов (скорость, память, доля скачанного, sha256) «мягкие»: FAIL записывается, но
# остальные этапы идут дальше, чтобы длинный прогон дал все данные. Итог в коде 1.
#
# Переменные (по умолчанию в скобках):
#   E2E_CHAT_ID                    id готового канала (см. выше); без него канал создаёт аккаунт
#                                  пользователя
#   E2E_WIPE_FIRST=1               с E2E_CHAT_ID: если в канале есть документы или закреп, бот
#                                  сначала удаляет документы telega-fs и закреп (иначе отказ)
#   E2E_SCAN_MAX_ID               потолок серверного id зонда при проверке пустоты и уборке
#                                 (100000); id выше потолка: проверка и уборка не выполняются
#                                 (ошибка)
#   E2E_WORK (build/e2e-resources-work)  рабочий каталог; для возобновления тот же
#   E2E_CHAT_FILE                  файл с id канала (~/.local/share/telega-fs-bench/e2e-resources-chat);
#                                  при E2E_CHAT_ID это $E2E_WORK/chat-id
#   E2E_STORE (telegram)           local: профили на LocalDirStore (без сети, для проверки скрипта)
#   E2E_SIZE_MIB (200), E2E_VIDEO_MIB (300), E2E_HUGE_GIB (10)
#   E2E_STAGING_LIMIT (2GiB), E2E_CACHE_LIMIT (4GiB)     staging_limit и cache_limit в профилях
#   E2E_MIN_READ_MIBS (6)          порог холодного последовательного чтения, МиБ/с
#   E2E_READAHEAD_MAX_MIB (32)     readahead_max профиля (пишется в config.toml машин)
#   E2E_VIDEO_HEADER_MIB (4)       запас на заголовок видео (moov при +faststart стоит до mdat)
#                                  и блобы индекса в tdfiles/. Порог скачанного видео:
#                                  3 * (readahead_max + 2 * chunk_size) + заголовок, то есть
#                                  при значениях по умолчанию 3 * 48 + 4 = 148 МиБ
#   E2E_TRACE_ARGS                 глобальные опции telega-fs для машин B1..B3 (например -v:
#                                  debug-лог демона)
#   E2E_TRACE_CONFIG               строки, добавляемые в профиль [profiles.main] машин B (через
#                                  \n, например 'trace_scheduler = true'); для отладочной трассировки
#   E2E_TRACE_ENV                  переменные окружения демонов B, 'VAR=1 VAR2=x'
#   E2E_CHUNK_MIB (8)              chunk_size профиля (допуск tdfiles/: лимит + чанк)
#   E2E_SYNC_TIMEOUT (1800s)       sync_timeout профиля
#   E2E_REDO="этап ..."            сбросить готовность этих этапов (и перезапустить)
#   E2E_KEEP_CHANNEL=1             не удалять канал (или сообщения в нём) в final; E2E_KEEP_SESSIONS=1
#                                  не делать logout
#   E2E_REUSE_CHANNEL=1            без E2E_CHAT_ID: канал уже создан, E2E_CHAT_FILE существует
#                                  (этап channel ничего не создаёт, канал не удаляется)
#   E2E_BENCH_ARGS                 доп. глобальные опции tgfs-bench (например --state-dir)
#
# Правила безопасности: нагрузку несёт бот. Аккаунт пользователя участвует только в создании и
# удалении канала и не отправляет сообщений (при E2E_CHAT_ID не участвует вообще). FLOOD_WAIT
# дольше часа или блокировка бота останавливают прогон (код 3).

set -Eeuo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
# shellcheck disable=SC1091
. "$here/lib.sh"

# Из ctest без явного разрешения тест пропускается (код 77), чтобы `ctest -LE long` не ходил в сеть.
if [[ "${E2E_CTEST:-}" == "1" && "${TELEGA_FS_E2E_RUN:-}" != "1" ]]; then
    echo "E2E skipped: set TELEGA_FS_E2E_RUN=1 to run it against live Telegram"
    exit 77
fi

EXIT_CHECKS=1 EXIT_ENV=2 EXIT_STOP=3 EXIT_STAGE=4

TGFS_BIN="${TGFS_BIN:-$repo/build/dev/src/telega-fs}"
BENCH_BIN="${BENCH_BIN:-$repo/build/dev/tools/bench/tdbench/tgfs-bench}"
E2E_CREDS="${E2E_CREDS:-$repo/cred.secret}"
E2E_STORE="${E2E_STORE:-telegram}"
E2E_SIZE_MIB="${E2E_SIZE_MIB:-200}"
E2E_VIDEO_MIB="${E2E_VIDEO_MIB:-300}"
E2E_HUGE_GIB="${E2E_HUGE_GIB:-10}"
E2E_STAGING_LIMIT="${E2E_STAGING_LIMIT:-2GiB}"
E2E_CACHE_LIMIT="${E2E_CACHE_LIMIT:-4GiB}"
E2E_MIN_READ_MIBS="${E2E_MIN_READ_MIBS:-6}"
E2E_READAHEAD_MAX_MIB="${E2E_READAHEAD_MAX_MIB:-32}"
E2E_VIDEO_HEADER_MIB="${E2E_VIDEO_HEADER_MIB:-4}"
E2E_TRACE_ARGS="${E2E_TRACE_ARGS:-}"
E2E_TRACE_CONFIG="${E2E_TRACE_CONFIG:-}"
E2E_TRACE_ENV="${E2E_TRACE_ENV:-}"
E2E_CHUNK_MIB="${E2E_CHUNK_MIB:-8}"
E2E_SYNC_TIMEOUT="${E2E_SYNC_TIMEOUT:-1800s}"
E2E_CHAT_FILE="${E2E_CHAT_FILE:-${XDG_DATA_HOME:-$HOME/.local/share}/telega-fs-bench/e2e-resources-chat}"
W="${E2E_WORK:-$repo/build/e2e-resources-work}"

STAGES=(channel gen a_init a_write b1_read b2_video inspect1 a_huge b3_read final)
[[ "$E2E_STORE" == local || "$E2E_STORE" == telegram ]] || { echo "E2E_STORE: telegram or local" >&2; exit "$EXIT_ENV"; }
E2E_CHAT_ID="${E2E_CHAT_ID:-}"
if [[ -n "$E2E_CHAT_ID" && ! "$E2E_CHAT_ID" =~ ^-100[0-9]+$ ]]; then
    echo "E2E_CHAT_ID must be the id of a channel: -100 and digits, for example -1001234567890" >&2
    exit "$EXIT_ENV"
fi
FLOOD_LIMIT_S=3600          # FLOOD_WAIT больше часа останавливает серию
SEED_PREFIX="tgfs-e2e-resources"   # seed детерминированного генератора данных (пишется в отчёт)

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
log() { echo "[e2e] $(date +%H:%M:%S) $*"; }
die() { echo "[e2e] ERROR: $*" >&2; exit "$EXIT_STAGE"; }
env_die() { echo "[e2e] $*" >&2; exit "$EXIT_ENV"; }
fcmp() { awk -v a="$1" -v b="$3" "BEGIN { exit !(a $2 b) }"; }   # fcmp 3.5 '>=' 6
rate() { awk -v b="$1" -v s="$2" 'BEGIN { printf "%.2f", (s > 0 ? b / 1048576 / s : 0) }'; }  # МиБ/с

to_bytes() { # 2GiB -> байты
    awk -v s="$1" 'BEGIN {
        n = s + 0; u = s; sub(/^[0-9.]+/, "", u)
        m = 1
        if (u == "KiB") m = 1024; else if (u == "MiB") m = 1048576; else if (u == "GiB") m = 1073741824
        printf "%.0f", n * m }'
}
STAGING_LIMIT_B="$(to_bytes "$E2E_STAGING_LIMIT")"
CACHE_LIMIT_B="$(to_bytes "$E2E_CACHE_LIMIT")"
CHUNK_B=$((E2E_CHUNK_MIB * 1048576))
VIDEO_LIMIT_B=$(((3 * (E2E_READAHEAD_MAX_MIB + 2 * E2E_CHUNK_MIB) + E2E_VIDEO_HEADER_MIB) * 1048576))
HUGE_B=$((E2E_HUGE_GIB * 1073741824))

# --- подготовка окружения -------------------------------------------------------------------

sub="${1:-}"
for bin in "$TGFS_BIN" "$BENCH_BIN"; do
    [[ -x "$bin" ]] || env_die "no executable: $bin (build with -DTELEGA_FS_BUILD_TOOLS=ON or set TGFS_BIN and BENCH_BIN)"
done
for tool in openssl fusermount3 sha256sum awk find; do
    command -v "$tool" >/dev/null || env_die "need $tool"
done
if [[ "$sub" != status && "$sub" != teardown ]]; then
    command -v ffmpeg >/dev/null && command -v ffprobe >/dev/null || env_die "need ffmpeg and ffprobe"
    [[ "$(ffmpeg -hide_banner -encoders 2>/dev/null)" == *libx264* ]] || env_die "ffmpeg without libx264"
fi

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
        env_die "need api-id, api-hash and bot-token: a file E2E_CREDS (default: cred.secret in the repository root; lines api-id=..., api-hash=..., bot-token=...) or TELEGA_API_ID, TELEGA_API_HASH, TELEGA_BOT_TOKEN"
    export TELEGA_API_ID TELEGA_API_HASH
    unset TELEGA_FS_FAULTS      # инъекция сбоев только для LocalDirStore
fi

mkdir -p "$W/logs" "$W/state" "$W/samples"
chmod 700 "$W"
touch "$W/results.env" "$W/checks.txt"
bench_creds=""   # путь появляется ниже, после установки trap
# shellcheck disable=SC1091
. "$W/results.env"
log "work dir: $W (store: $E2E_STORE)"

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
check_if() { # check_if ИМЯ УСЛОВИЕ_ВЫПОЛНЕНО(0|1) ПОДРОБНОСТИ
    if [[ "$2" == 1 ]]; then verdict "$1" PASS "$3"; else verdict "$1" FAIL "$3"; fi
}

# --- «машины», монтирование, демоны ----------------------------------------------------------

mounts=()
daemon_pids=()
sampler_pids=()
declare -A SAMPLER_OF=()
declare -A LOG_OFFSET=()   # размер лога демона на момент монтирования: лог дописывается между запусками

# Команда на «машине» A, B1, B2, B3: свои XDG-каталоги.
on() {
    local m=$1
    shift
    XDG_CONFIG_HOME="$W/$m/config" XDG_DATA_HOME="$W/$m/data" XDG_CACHE_HOME="$W/$m/cache" "$@"
}
tgfs() { local m=$1; shift; on "$m" "$TGFS_BIN" "$@"; }
bench() { "$BENCH_BIN" ${E2E_BENCH_ARGS:-} --creds "$bench_creds" "$@" </dev/null; }

daemon_log() { echo "$W/$1/data/telega-fs/main/telega-fs.log"; }
data_dir() { echo "$W/$1/data/telega-fs/main"; }
tdfiles_dir() { echo "$W/$1/cache/telega-fs/main/tdfiles"; }

cleanup() {
    local rc=$?
    set +e
    trap - ERR
    [[ "$bench_creds" == "$W/bench-creds" ]] && rm -f "$bench_creds"
    trap '' INT TERM
    for p in "${sampler_pids[@]:-}"; do
        [[ -n "$p" ]] && kill "$p" 2>/dev/null
    done
    if [[ -e "$W/state/STOP-FLOOD" ]]; then
        for p in "${daemon_pids[@]:-}"; do [[ -n "$p" ]] && kill -KILL "$p" 2>/dev/null; done
        for m in "${mounts[@]:-}"; do
            [[ -n "$m" ]] && fusermount3 -uz "$m" 2>/dev/null   # у убитого демона mountpoint -q не работает
        done
        rc=$EXIT_STOP
    else
        for m in "${mounts[@]:-}"; do
            [[ -n "$m" ]] && mountpoint -q "$m" 2>/dev/null && fusermount3 -u "$m" 2>/dev/null
        done
        # Демон делает финальный коммит: ждём его (до 30 минут), чтобы не бросить данные в staging.
        for p in "${daemon_pids[@]:-}"; do
            [[ -n "$p" ]] || continue
            for _ in $(seq 1 1800); do kill -0 "$p" 2>/dev/null || break; sleep 1; done
        done
    fi
    exit "$rc"
}
trap cleanup EXIT
# Неожиданный сбой команды: код 4 (этап не готов, повтор возможен), а не код самой команды.
trap 'echo "[e2e] ERROR: command failed at line $LINENO (exit $?)" >&2; exit $EXIT_STAGE' ERR
trap 'exit 130' INT
trap 'exit 143' TERM

# tgfs-bench читает креды только из файла: собираем его из найденных значений (0600, убирается при
# выходе). Свой канал: id кладётся в файл для подкоманд `tgfs-bench e2e`.
bench_creds="$E2E_CREDS"
if [[ "$E2E_STORE" == telegram ]]; then
    bench_creds="$W/bench-creds"
    (umask 077; printf 'api-id=%s\napi-hash=%s\nbot-token=%s\n' "$TELEGA_API_ID" "$TELEGA_API_HASH" "$bot_token" >"$bench_creds")
fi
chan_guard_start
if [[ -n "$E2E_CHAT_ID" ]]; then
    (umask 077; printf '%s\n' "$E2E_CHAT_ID" >"$W/chat-id")
    E2E_CHAT_FILE="$W/chat-id"
fi

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
        echo "staging_limit = \"$E2E_STAGING_LIMIT\""
        echo "cache_limit = \"$E2E_CACHE_LIMIT\""
        echo "readahead_max = \"${E2E_READAHEAD_MAX_MIB}MiB\""
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
    if [[ "$E2E_STORE" == local ]]; then
        return 0
    fi
    printf '%s\n' "$bot_token" | tgfs "$m" login --bot >"$W/logs/login-$m.out" 2>&1 ||
        { tail -n 5 "$W/logs/login-$m.out" >&2; die "login --bot on $m"; }
    grep -q '^Logged in as bot' "$W/logs/login-$m.out" || die "login output on $m has no 'Logged in as bot'"
}

logout_machine() { # logout_machine МАШИНА: закрыть сессию бота (токен остаётся рабочим)
    local m=$1
    [[ "$E2E_STORE" == telegram && -e "$W/$m/config/telega-fs/config.toml" ]] || return 0
    [[ "${E2E_KEEP_SESSIONS:-0}" == 1 ]] && return 0
    on "$m" timeout 120 "$TGFS_BIN" logout >"$W/logs/logout-$m.out" 2>&1 </dev/null || true
}

# Чистая «машина» B: новые каталоги, логин. Прежняя (если есть) закрывается и удаляется.
fresh_machine() {
    local m=$1
    logout_machine "$m"
    rm -rf "${W:?}/$m"
    # Тот же config.toml, что у A (там после init лежит chat_id), но пустые каталоги данных и кэша.
    mkdir -p "$W/$m/config/telega-fs" "$W/$m/data" "$W/$m/cache"
    chmod 700 "$W/$m/data"
    cp "$W/A/config/telega-fs/config.toml" "$W/$m/config/telega-fs/config.toml"
    if [[ -n "$E2E_TRACE_CONFIG" ]]; then   # отладочные ключи профиля только на машинах B
        printf '%b\n' "$E2E_TRACE_CONFIG" >>"$W/$m/config/telega-fs/config.toml"
    fi
    chmod 600 "$W/$m/config/telega-fs/config.toml"
    login_bot "$m"
    if [[ "$E2E_STORE" == local ]]; then
        # Профиль LocalDirStore без keyring не смонтировать: копия keyring машины A заменяет
        # master_key из головы канала (в режиме бота B берёт его из головы).
        mkdir -p "$(data_dir "$m")"
        cp "$(data_dir A)/keyring" "$(data_dir "$m")/keyring" 2>/dev/null || true
    fi
}

# Смонтировать, запомнить pid демона в $W/<m>.pid. Использование: mount_on A MNT [опции].
mount_on() {
    local m=$1 mnt=$2
    shift 2
    mkdir -p "$mnt"
    LOG_OFFSET[$m]=0
    # Размер лога до этого монтирования (старые строки с ошибками прошлых запусков не считаются).
    [[ -r "$(daemon_log "$m")" ]] && LOG_OFFSET[$m]=$(stat -c %s "$(daemon_log "$m")")
    local out trace_args="" trace_env=""
    if [[ "$m" == B* ]]; then               # трассировка (E2E_TRACE_*) только на машинах B
        trace_args="$E2E_TRACE_ARGS"
        trace_env="$E2E_TRACE_ENV"
    fi
    # mount идёт фоном под timeout, чтобы pid запускающего процесса был виден в pid-файле (lib.sh
    # отказывается чистить канал, пока жив процесс этого прогона).
    local mrc=0 mfile="$W/logs/mount-$m.cur" mp
    # shellcheck disable=SC2086
    timeout 1200 env XDG_CONFIG_HOME="$W/$m/config" XDG_DATA_HOME="$W/$m/data" \
        XDG_CACHE_HOME="$W/$m/cache" $trace_env "$TGFS_BIN" $trace_args mount "$@" "$mnt" \
        >"$mfile" 2>&1 &
    mp=$!
    echo "$mp" >"$W/$m.mount.pid"
    wait "$mp" || mrc=$?
    rm -f "$W/$m.mount.pid"
    out="$(cat "$mfile")"
    rm -f "$mfile"
    [[ "$mrc" == 0 ]] || { echo "$out" >&2; die "mount on $m"; }
    echo "$out" >>"$W/logs/mount-$m.out"
    local pid
    pid="$(sed -n 's/.*mounted (pid \([0-9]*\),.*/\1/p' <<<"$out")"
    [[ -n "$pid" ]] || die "cannot find the pid of the mount daemon on $m"
    echo "$pid" >"$W/$m.pid"
    daemon_pids+=("$pid")
    mounts+=("$mnt")
    # Сэмплер живёт, пока жив демон: память, staging, tdfiles/ и охрана от FLOOD_WAIT > часа.
    sample_start "$CUR_STAGE-$m" "$m" "$pid"
    SAMPLER_OF[$m]=$SAMPLER_PID
}

# Ждёт завершения демона по pid. Использование: wait_pid PID ТАЙМАУТ_С.
wait_pid() {
    local pid=$1 limit=$2 t0
    t0="$(now)"
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1
        fcmp "$(elapsed "$t0")" '>' "$limit" && die "the daemon $pid did not exit in ${limit}s"
    done
    return 0
}

# Копия логов демона и TDLib машины в logs/ (для разбора: машину B потом удаляют и пересоздают).
save_logs() {
    local m=$1 tag
    tag="$CUR_STAGE-$m"
    cp "$(daemon_log "$m")" "$W/logs/$tag.telega-fs.log" 2>/dev/null || true
    if [[ -s "$(data_dir "$m")/td.log" ]]; then
        cp "$(data_dir "$m")/td.log" "$W/logs/$tag.td.log" 2>/dev/null || true
    fi
    return 0
}

# Размонтировать fusermount3 и дождаться финального коммита. Время — в LAST_UNMOUNT_S.
unmount_wait() {
    local m=$1 mnt=$2 t0 pid
    t0="$(now)"
    pid="$(cat "$W/$m.pid")"
    fusermount3 -u "$mnt" || die "fusermount3 -u on $m"
    wait_pid "$pid" 1800
    wait "${SAMPLER_OF[$m]:-}" 2>/dev/null || true
    LAST_UNMOUNT_S="$(elapsed "$t0")"
    save_logs "$m"
    local fresh
    fresh="$(tail -c "+$((LOG_OFFSET[$m] + 1))" "$(daemon_log "$m")")"   # только строки этого монтирования
    [[ "$fresh" == *unmounted ]] || [[ "$fresh" == *unmounted$'\n'* ]] || die "no 'unmounted' in the log of $m"
    if grep -q 'final commit failed\|flush on unmount failed' <<<"$fresh"; then
        die "the final commit on $m failed (see its log)"
    fi
}

# --- FLOOD_WAIT и счётчики по логу демона -----------------------------------------------------

flood_lines() { # число строк про FLOOD_WAIT в логе машины
    local f
    f="$(daemon_log "$1")"
    [[ -r "$f" ]] || { echo 0; return; }
    grep -aci 'flood_wait\|retry after' "$f" || true
}
flood_max_s() { # наибольшее ожидание в секундах по логу
    local f
    f="$(daemon_log "$1")"
    [[ -r "$f" ]] || { echo 0; return; }
    { grep -aoiE 'FLOOD_(PREMIUM_)?WAIT[_ ]+[0-9]+|retry after [0-9]+' "$f" || true; } |
        { grep -oE '[0-9]+$' || true; } | sort -n | tail -n1 | awk '{ print $1 + 0 }'
}
blocked_in_log() { # бот заблокирован, сессия отозвана
    local f
    f="$(daemon_log "$1")"
    [[ -r "$f" ]] && grep -aqE 'USER_DEACTIVATED|AUTH_KEY_UNREGISTERED|SESSION_REVOKED|bot was blocked|bot was kicked|USER_BANNED_IN_CHANNEL' "$f"
}

# --- сэмплер: раз в секунду VmHWM/VmRSS демона, staging+tmp, tdfiles/ -------------------------

dir_len_sum() { # сумма длин файлов (логических) в каталогах
    find "$@" -type f -printf '%s\n' 2>/dev/null | awk '{ s += $1 } END { print s + 0 }'
}
dir_blocks_sum() { # сумма st_blocks * 512 (занятое место разреженных файлов)
    find "$@" -type f -printf '%b\n' 2>/dev/null | awk '{ s += $1 * 512 } END { print s + 0 }'
}

# sample_start ИМЯ МАШИНА PID: пишет $W/samples/ИМЯ.csv, пока жив демон. Заодно раз в 5 с смотрит
# лог на FLOOD_WAIT больше часа и блокировку: тогда убивает демона и пишет state/STOP-FLOOD.
sample_start() {
    local name=$1 m=$2 pid=$3
    local csv="$W/samples/$name.csv"
    local d td
    d="$(data_dir "$m")"
    td="$(tdfiles_dir "$m")"
    echo "t_s,vmhwm_kib,vmrss_kib,staging_tmp_bytes,tdfiles_blocks_bytes" >"$csv"
    (
        trap - EXIT INT TERM ERR
        set +e +o pipefail     # сэмплер не должен падать из-за исчезнувшего файла
        local t0 i=0 hwm rss st tdb fm
        t0="$(now)"
        while kill -0 "$pid" 2>/dev/null; do
            read -r hwm rss < <(awk '/^VmHWM:/ { h = $2 } /^VmRSS:/ { r = $2 } END { print h + 0, r + 0 }' \
                "/proc/$pid/status" 2>/dev/null || echo "0 0")
            st="$(dir_len_sum "$d/staging" "$d/tmp")"
            tdb="$(dir_blocks_sum "$td")"
            echo "$(elapsed "$t0"),$hwm,$rss,$st,$tdb" >>"$csv"
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
sample_max() { # sample_max ИМЯ КОЛОНКА(2..5): максимум
    awk -F, -v c="$2" 'NR > 1 && $c + 0 > m { m = $c + 0 } END { print m + 0 }' "$W/samples/$1.csv"
}
mib() { awk -v b="$1" 'BEGIN { printf "%.1f", b / 1048576 }'; }

# --- данные ----------------------------------------------------------------------------------

# Детерминированные псевдослучайные байты: AES-128-CTR по нулям, ключ = sha256(seed). Seed
# фиксирован (SEED_PREFIX + имя), поэтому набор воспроизводим. gen_bytes ИМЯ БАЙТЫ [> файл|поток].
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
        find . -type f -printf '%P\n' | LC_ALL=C sort | xargs -d '\n' sha256sum
        find . -mindepth 1 -printf '%y %m %P\n' | LC_ALL=C sort
        find . -type f -printf '%m %s %P\n' | LC_ALL=C sort
    ) >"$2"
}

verify_tree() { # verify_tree МАШИНА MNT ИМЯ: дерево смонтированной ФС совпадает с эталоном
    local mnt=$2 name=$3
    make_manifest "$mnt" "$W/manifest-$name.txt" || die "cannot read the tree on $1 ($name)"
    if diff -q "$W/manifest.txt" "$W/manifest-$name.txt" >/dev/null; then
        verdict "tree-$name" PASS "sha256, modes and sizes of all files match"
    else
        diff "$W/manifest.txt" "$W/manifest-$name.txt" | head -n 20 >&2 || true
        verdict "tree-$name" FAIL "the tree on $1 differs from the source (see manifest-$name.txt)"
    fi
}

stage_gen() {
    local src="$W/src"
    rm -rf "$src"
    mkdir -p "$src/small" "$src/dir/sub/deeper" "$src/medium" "$src/pack"
    log "generating $E2E_SIZE_MIB MiB, small files (inline, packs), directories"
    gen_bytes big "$((E2E_SIZE_MIB * 1048576))" >"$src/big.bin"
    gen_bytes five $((5 * 1048576 + 17)) >"$src/five.bin"       # больше pack_threshold: свой чанк
    local n
    for n in 1 2 3 4 5 6 7 8 9 10 11 12; do                       # инлайн: до 4 КиБ
        gen_bytes "s$n" $((n * 300)) >"$src/small/s$n.dat"
    done
    for n in 1 2 3 4 5 6; do                                      # пакеты: 5 КиБ .. 3 МиБ
        gen_bytes "m$n" $((n * n * 90 * 1024)) >"$src/medium/m$n.dat"
    done
    for n in $(seq 1 300); do                                     # 300 файлов 5-200 КиБ: пакеты
        mkdir -p "$src/pack/d$((n % 6))"
        gen_bytes "p$n" $(((5 + (n * 7919) % 196) * 1024)) >"$src/pack/d$((n % 6))/f$n.dat"
    done
    : >"$src/empty.txt"
    echo "hello" >"$src/dir/hello.txt"
    gen_bytes seven 7000 >"$src/dir/sub/deeper/seven.dat"
    printf 'name with spaces and \xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82' >"$src/dir/sub/имя файла.txt"
    chmod 640 "$src/dir/hello.txt"
    chmod 755 "$src/dir"

    # Видео: testsrc2 с шумом, CBR 8 Мбит/с (около 1,03 МБ/с), +faststart (moov в начале).
    # Длительность считается так, чтобы получить E2E_VIDEO_MIB.
    local dur
    dur=$(awk -v m="$E2E_VIDEO_MIB" 'BEGIN { printf "%d", m * 1048576 / 1030000 + 1 }')
    log "generating the video ($dur s, about $E2E_VIDEO_MIB MiB) with ffmpeg"
    local t
    t=$(now)
    ffmpeg -nostdin -hide_banner -loglevel error -y -f lavfi -i "testsrc2=size=1280x720:rate=30" \
        -vf "noise=alls=40:allf=t+u" -t "$dur" -c:v libx264 -preset veryfast -b:v 8M -minrate 8M \
        -maxrate 8M -bufsize 16M -x264-params nal-hrd=cbr:force-cfr=1 -threads 4 -pix_fmt yuv420p \
        -movflags +faststart -f mp4 "$src/video.mp4.tmp" 2>"$W/logs/ffmpeg-gen.out" ||
        { tail -n 5 "$W/logs/ffmpeg-gen.out" >&2; die "ffmpeg could not generate the video"; }
    mv "$src/video.mp4.tmp" "$src/video.mp4"
    local vbytes vdur
    vbytes=$(stat -c %s "$src/video.mp4")
    vdur="$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$src/video.mp4")"
    res VIDEO_BYTES "$vbytes"
    res VIDEO_DURATION_S "${vdur%.*}"
    res T_GEN_VIDEO "$(elapsed "$t")"
    log "video: $(mib "$vbytes") MiB, ${vdur%.*} s"
    # +faststart: moov должен идти раньше mdat.
    local first
    first="$(ffprobe -v trace "$src/video.mp4" 2>&1 | grep -oE "type:'(moov|mdat)'" | head -n1)"
    [[ "$first" == "type:'moov'" ]] || die "the video is not faststart (first atom: $first)"
    fcmp "$vbytes" '>' $((E2E_VIDEO_MIB * 1048576 * 8 / 10)) ||
        log "warning: the video is much smaller than requested"

    make_manifest "$src" "$W/manifest.txt"
    # Для проверки «мелкие файлы в пакетах» (inspect1): сколько файлов вне инлайна, сколько из них
    # мелких (в пакеты) и сколько чанков дают крупные (>= pack_threshold = 4 МиБ).
    local inline_max=4096 pack_thr=$((4 * 1048576))
    res FILES_NOT_INLINE "$(find "$src" -type f -size +"${inline_max}"c | wc -l)"
    res FILES_PACKED "$(find "$src" -type f -size +"${inline_max}"c -size -"${pack_thr}"c | wc -l)"
    res LARGE_CHUNKS "$(find "$src" -type f -size +"$((pack_thr - 1))"c -printf '%s\n' |
        awk -v c="$CHUNK_B" '{ n += int(($1 + c - 1) / c) } END { print n + 0 }')"
    res SRC_FILES "$(find "$src" -type f | wc -l)"
    res SRC_BYTES "$(find "$src" -type f -printf '%s\n' | awk '{ s += $1 } END { print s + 0 }')"
}

stage_channel() {
    if [[ "$E2E_STORE" == local ]]; then
        mkdir -p "$W/channel"
        return 0
    fi
    if [[ -n "$E2E_CHAT_ID" ]]; then
        log "checking that your channel is empty (the bot only, no user account); takes a few minutes"
        ensure_channel_empty
        rm -f "$W/state/channel-deleted" "$W/state/channel-reused"
        return 0
    fi
    if [[ "${E2E_REUSE_CHANNEL:-0}" == 1 ]]; then
        [[ -r "$E2E_CHAT_FILE" ]] || die "E2E_REUSE_CHANNEL=1 but E2E_CHAT_FILE does not exist"
        touch "$W/state/channel-reused"     # чужой канал final не удаляет
        return 0
    fi
    rm -f "$W/state/channel-deleted" "$W/state/channel-reused"
    log "creating the channel (user account: setup only, no messages)"
    # Если сессия пользователя бенча не авторизована, setup падает: вход только вручную.
    bench e2e --chat-file "$E2E_CHAT_FILE" setup >"$W/logs/setup.out" 2>&1 ||
        {
            tail -n 5 "$W/logs/setup.out" >&2
            echo "[e2e] the user account must be logged in to tgfs-bench (tgfs-bench --qr login);" >&2
            echo "[e2e] or set E2E_CHAT_ID to a channel you made yourself, the bot is its administrator" >&2
            die "channel setup (see logs/setup.out)"
        }
}

stage_a_init() {
    logout_machine A
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

stage_a_write() {
    local mnt="$W/A/mnt" t
    t=$(now)
    mount_on A "$mnt"
    res T_MOUNT_A "$(elapsed "$t")"
    log "mounted A in $T_MOUNT_A s"
    t=$(now)
    cp -r "$W/src/." "$mnt/" || die "cp to the mount failed"
    res T_WRITE_BASE "$(elapsed "$t")"
    t=$(now)
    sync "$mnt/big.bin" || die "fsync of the big file failed (EIO?)"
    sync "$mnt/video.mp4" || die "fsync of the video failed (EIO?)"
    sync "$mnt" || die "fsyncdir failed (EIO?)"
    res T_FSYNC_BASE "$(elapsed "$t")"
    log "written in $T_WRITE_BASE s, fsync (upload + commit) $T_FSYNC_BASE s"
    unmount_wait A "$mnt"
    res T_UNMOUNT_BASE "$LAST_UNMOUNT_S"
}

stage_b1_read() {
    fresh_machine B1
    local mnt="$W/B1/mnt" t sha_src sha_b
    t=$(now)
    mount_on B1 "$mnt"
    res T_MOUNT_B1 "$(elapsed "$t")"
    log "mounted B1 (empty directories) in $T_MOUNT_B1 s"
    local f0
    f0="$(flood_lines B1)"
    # Холодное последовательное чтение (sha256sum читает по 128 КиБ, как пользователь).
    t=$(now)
    sha_b="$(sha256sum "$mnt/big.bin" | cut -d' ' -f1)"
    res T_READ_BIG "$(elapsed "$t")"
    res READ_BIG_MIBS "$(rate "$((E2E_SIZE_MIB * 1048576))" "$T_READ_BIG")"
    sha_src="$(sha256sum "$W/src/big.bin" | cut -d' ' -f1)"
    check_if sha-big-B1 "$([[ "$sha_b" == "$sha_src" ]] && echo 1 || echo 0)" \
        "sha256 of the $E2E_SIZE_MIB MiB file on B1"
    check_if cold-read-speed "$(fcmp "$READ_BIG_MIBS" '>=' "$E2E_MIN_READ_MIBS" && echo 1 || echo 0)" \
        "$READ_BIG_MIBS MiB/s in $T_READ_BIG s, threshold $E2E_MIN_READ_MIBS MiB/s"
    verify_tree B1 "$mnt" B1
    res FLOOD_B1 "$(($(flood_lines B1) - f0))"
    unmount_wait B1 "$mnt"
    logout_machine B1
    rm -rf "${W:?}/B1/cache"
}

# Скачано на машине M: tdfiles/ по st_blocks (Telegram) или read_range_bytes из stats (local).
downloaded_bytes() {
    if [[ "$E2E_STORE" == local ]]; then
        awk '$1 == "read_range_bytes" { print $2 }' "$W/channel/stats" 2>/dev/null || echo 0
    else
        dir_blocks_sum "$(tdfiles_dir "$1")"
    fi
}

stage_b2_video() {
    fresh_machine B2
    local mnt="$W/B2/mnt" t vf="$W/B2/mnt/video.mp4"
    t=$(now)
    mount_on B2 "$mnt"
    res T_MOUNT_B2 "$(elapsed "$t")"
    local dur=$VIDEO_DURATION_S pct pos i=0 total=0
    # Точки: 15 %, 50 %, 85 % длительности; ffmpeg ищет через moov в начале файла (+faststart).
    local f0
    f0="$(flood_lines B2)"
    for pct in 15 50 85; do
        pos=$((dur * pct / 100))
        i=$((i + 1))
        t=$(now)
        timeout 900 ffmpeg -nostdin -hide_banner -loglevel error -ss "$pos" -t 5 -i "$vf" -f null - \
            >"$W/logs/ffmpeg-seek-$i.out" 2>&1 ||
            { tail -n 5 "$W/logs/ffmpeg-seek-$i.out" >&2; die "ffmpeg -ss $pos on B2"; }
        res "T_SEEK_$i" "$(elapsed "$t")"
        sleep 2
        total="$(downloaded_bytes B2)"
        res "VIDEO_DL_$i" "$total"
        log "seek #$i to ${pos}s: $(elapsed "$t") s, downloaded so far $(mib "$total") MiB"
    done
    res VIDEO_DL_PCT "$(awk -v d="$total" -v s="$VIDEO_BYTES" 'BEGIN { printf "%.2f", d * 100 / s }')"
    check_if video-downloaded "$((total <= VIDEO_LIMIT_B ? 1 : 0))" \
        "$(mib "$total") MiB ($VIDEO_DL_PCT % of the $(mib "$VIDEO_BYTES") MiB file), limit 3 * (readahead_max $E2E_READAHEAD_MAX_MIB + 2 * chunk $E2E_CHUNK_MIB) + header $E2E_VIDEO_HEADER_MIB = $(mib "$VIDEO_LIMIT_B") MiB"
    res FLOOD_B2 "$(($(flood_lines B2) - f0))"
    unmount_wait B2 "$mnt"
    logout_machine B2
    rm -rf "${W:?}/B2/cache"
}

run_inspect() { # run_inspect ИМЯ: вывод в logs/inspect-ИМЯ.out, возвращает код inspect
    bench e2e --chat-file "$E2E_CHAT_FILE" inspect >"$W/logs/inspect-$1.out" 2>&1 || true
    grep -v '^\[' "$W/logs/inspect-$1.out" | sed 's/^/[e2e]   /'
    grep -q '^INSPECT OK' "$W/logs/inspect-$1.out"
}

stage_inspect1() {
    if [[ "$E2E_STORE" == local ]]; then
        verdict inspect-names SKIP "store = local"
        verdict small-files-in-packs SKIP "store = local"
        return 0
    fi
    local ok=0
    run_inspect 1 && ok=1
    check_if inspect-names "$ok" "document names <16 hex>.bin|.idx, no captions"
    local bins
    bins="$(sed -n 's/^documents \.bin: //p' "$W/logs/inspect-1.out")"
    bins="${bins:-0}"
    res DOCS_BIN_BASE "$bins"
    res DOCS_IDX_BASE "$(sed -n 's/^documents \.idx: //p' "$W/logs/inspect-1.out" | head -n1)"
    check_if small-files-fewer-docs "$((bins < FILES_NOT_INLINE ? 1 : 0))" \
        ".bin documents: $bins, files outside inline: $FILES_NOT_INLINE"
    # Документы без чанков крупных файлов: это пакеты. Их должно быть заметно меньше мелких файлов.
    local pack_docs=$((bins - LARGE_CHUNKS))
    res DOCS_PACKS_EST "$pack_docs"
    check_if packs "$((pack_docs >= 1 && pack_docs * 4 < FILES_PACKED ? 1 : 0))" \
        "about $pack_docs pack documents ($bins .bin minus $LARGE_CHUNKS chunks of large files) for $FILES_PACKED packed files"
}

stage_a_huge() {
    local mnt="$W/A/mnt" t dd_rc
    local hf="$W/huge.sha256"
    rm -f "$hf"
    res HUGE_OK 0
    t=$(now)
    mount_on A "$mnt"
    res T_REMOUNT_A "$(elapsed "$t")"
    local f0
    f0="$(flood_lines A)"
    log "copying $E2E_HUGE_GIB GiB as a stream (staging_limit $E2E_STAGING_LIMIT), the sampler writes samples/a_huge-A.csv every second"
    t=$(now)
    dd_rc=0
    gen_bytes huge "$HUGE_B" | tee >(sha256sum | cut -d' ' -f1 >"$hf") |
        dd of="$mnt/huge.bin" bs=4M iflag=fullblock status=none || dd_rc=$?
    for _ in $(seq 1 60); do [[ -s "$hf" ]] && break; sleep 1; done
    res T_COPY_HUGE "$(elapsed "$t")"
    local rc_sync=0
    if [[ "$dd_rc" == 0 ]]; then
        t=$(now)
        sync "$mnt/huge.bin" || rc_sync=$?
        res T_FSYNC_HUGE "$(elapsed "$t")"
    fi
    if [[ -e "$W/state/STOP-FLOOD" ]]; then
        die "stopped: $(cat "$W/state/STOP-FLOOD")"
    fi
    res HUGE_SHA "$(cat "$hf" 2>/dev/null || true)"
    res HWM_KIB "$(sample_max a_huge-A 2)"
    res RSS_KIB "$(sample_max a_huge-A 3)"
    res STAGING_MAX "$(sample_max a_huge-A 4)"
    res FLOOD_HUGE_A "$(($(flood_lines A) - f0))"
    log "copy: ${T_COPY_HUGE} s (cp-equivalent), fsync ${T_FSYNC_HUGE:-n/a} s; VmHWM $(mib "$((HWM_KIB * 1024))") MiB, staging peak $(mib "$STAGING_MAX") MiB"
    check_if huge-copy "$((dd_rc == 0 && rc_sync == 0 ? 1 : 0))" \
        "dd exit $dd_rc, fsync exit $rc_sync"
    check_if huge-vmhwm "$((HWM_KIB * 1024 < 256 * 1048576 ? 1 : 0))" \
        "VmHWM $(mib "$((HWM_KIB * 1024))") MiB (limit 256 MiB), VmRSS peak $(mib "$((RSS_KIB * 1024))") MiB"
    check_if huge-staging "$((STAGING_MAX <= STAGING_LIMIT_B ? 1 : 0))" \
        "staging+tmp peak $(mib "$STAGING_MAX") MiB, limit $(mib "$STAGING_LIMIT_B") MiB"
    unmount_wait A "$mnt"
    res T_UNMOUNT_HUGE "$LAST_UNMOUNT_S"
    if [[ "$dd_rc" == 0 && "$rc_sync" == 0 ]]; then
        res HUGE_OK 1
    fi
}

stage_b3_read() {
    if [[ "${HUGE_OK:-0}" != 1 ]]; then
        verdict huge-read SKIP "the copy of the huge file did not finish (a_huge)"
        return 0
    fi
    fresh_machine B3
    local mnt="$W/B3/mnt" t sha_b
    t=$(now)
    mount_on B3 "$mnt"
    res T_MOUNT_B3 "$(elapsed "$t")"
    local f0
    f0="$(flood_lines B3)"
    log "cold read of the whole file on B3 (cache_limit $E2E_CACHE_LIMIT), the sampler writes samples/b3_read-B3.csv"
    t=$(now)
    sha_b="$(sha256sum "$mnt/huge.bin" | cut -d' ' -f1)" || sha_b=""
    res T_READ_HUGE "$(elapsed "$t")"
    if [[ -e "$W/state/STOP-FLOOD" ]]; then
        die "stopped: $(cat "$W/state/STOP-FLOOD")"
    fi
    res READ_HUGE_MIBS "$(rate "$HUGE_B" "$T_READ_HUGE")"
    res TDFILES_MAX "$(sample_max b3_read-B3 5)"
    res HWM_READ_KIB "$(sample_max b3_read-B3 2)"
    res FLOOD_B3 "$(($(flood_lines B3) - f0))"
    check_if sha-huge-B3 "$([[ -n "$sha_b" && "$sha_b" == "$HUGE_SHA" ]] && echo 1 || echo 0)" \
        "sha256 of $E2E_HUGE_GIB GiB on B3, $READ_HUGE_MIBS MiB/s in $T_READ_HUGE s"
    if [[ "$E2E_STORE" == local ]]; then
        verdict tdfiles-limit SKIP "store = local has no tdfiles/"
    else
        check_if tdfiles-limit "$((TDFILES_MAX <= CACHE_LIMIT_B + CHUNK_B ? 1 : 0))" \
            "tdfiles/ peak $(mib "$TDFILES_MAX") MiB (st_blocks), cache_limit $(mib "$CACHE_LIMIT_B") MiB + chunk $E2E_CHUNK_MIB MiB"
    fi
    log "B3 VmHWM during the read: $(mib "$((HWM_READ_KIB * 1024))") MiB"
    unmount_wait B3 "$mnt"
    logout_machine B3
    rm -rf "${W:?}/B3/cache"
}

# --- сводка ----------------------------------------------------------------------------------

longest_flood() { # наибольшее ожидание по логам всех машин, с
    local m v max=0
    for m in A B1 B2 B3; do
        [[ -r "$(daemon_log "$m")" ]] || continue
        v="$(flood_max_s "$m")"
        ((v > max)) && max=$v
    done
    echo "$max"
}

print_env() { # окружение прогона для отчёта
    local dirty=""
    [[ -n "$(git -C "$repo" status --porcelain --untracked-files=no 2>/dev/null)" ]] && dirty=" + uncommitted changes"
    echo "[e2e] env: telega-fs $(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo '?')$dirty, branch $(git -C "$repo" rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?'); $(uname -sr); CPU $(sed -n 's/^model name[ \t]*: //p' /proc/cpuinfo | head -n1); $(ffmpeg -version 2>/dev/null | head -n1 | cut -d' ' -f1-3); fs of the work dir: $(stat -f -c %T "$W")"
    echo "[e2e] profile: staging_limit $E2E_STAGING_LIMIT, cache_limit $E2E_CACHE_LIMIT, chunk_size ${E2E_CHUNK_MIB} MiB, other settings are the defaults"
}

print_summary() {
    local fails passes skips total_flood=0 m n
    fails=$(grep -c '|FAIL|' "$W/checks.txt" || true)
    passes=$(grep -c '|PASS|' "$W/checks.txt" || true)
    skips=$(grep -c '|SKIP|' "$W/checks.txt" || true)
    for m in A B1 B2 B3; do
        [[ -r "$(daemon_log "$m")" ]] || continue
        n="$(flood_lines "$m")"
        total_flood=$((total_flood + n))
    done
    {
        echo "[e2e] ---- summary ($(date -Is), store: $E2E_STORE) ----"
        print_env
        echo "[e2e] data: $E2E_SIZE_MIB MiB + $SRC_FILES files / $(mib "${SRC_BYTES:-0}") MiB in the base set, video $(mib "${VIDEO_BYTES:-0}") MiB (${VIDEO_DURATION_S:-?} s), stream $E2E_HUGE_GIB GiB; seed prefix '$SEED_PREFIX'"
        echo "[e2e] time, s: gen video ${T_GEN_VIDEO:-n/a}; mount A ${T_MOUNT_A:-n/a}; write to mount ${T_WRITE_BASE:-n/a}; fsync (upload+commit) ${T_FSYNC_BASE:-n/a}; unmount ${T_UNMOUNT_BASE:-n/a}"
        echo "[e2e]   mount B1 ${T_MOUNT_B1:-n/a}; mount B2 ${T_MOUNT_B2:-n/a}; remount A ${T_REMOUNT_A:-n/a}; mount B3 ${T_MOUNT_B3:-n/a}"
        echo "[e2e] cold read $E2E_SIZE_MIB MiB on B1: ${T_READ_BIG:-n/a} s = ${READ_BIG_MIBS:-n/a} MiB/s (threshold $E2E_MIN_READ_MIBS)"
        echo "[e2e] video seeks on B2, s: ${T_SEEK_1:-n/a} / ${T_SEEK_2:-n/a} / ${T_SEEK_3:-n/a}; downloaded ${VIDEO_DL_PCT:-n/a} % of the file (limit $(mib "$VIDEO_LIMIT_B") MiB)"
        echo "[e2e] documents in the channel after the base set: .bin ${DOCS_BIN_BASE:-n/a}, .idx ${DOCS_IDX_BASE:-n/a}; files outside inline ${FILES_NOT_INLINE:-n/a}; packed ${FILES_PACKED:-n/a}; chunks of large files ${LARGE_CHUNKS:-n/a}; estimated packs ${DOCS_PACKS_EST:-n/a}"
        echo "[e2e] $E2E_HUGE_GIB GiB copy: ${T_COPY_HUGE:-n/a} s, fsync ${T_FSYNC_HUGE:-n/a} s, unmount ${T_UNMOUNT_HUGE:-n/a} s; VmHWM ${HWM_KIB:+$(mib "$((HWM_KIB * 1024))")} MiB, VmRSS peak ${RSS_KIB:+$(mib "$((RSS_KIB * 1024))")} MiB, staging+tmp peak ${STAGING_MAX:+$(mib "$STAGING_MAX")} MiB (limit $E2E_STAGING_LIMIT)"
        echo "[e2e] cold read of $E2E_HUGE_GIB GiB on B3: ${T_READ_HUGE:-n/a} s = ${READ_HUGE_MIBS:-n/a} MiB/s; tdfiles/ peak ${TDFILES_MAX:+$(mib "$TDFILES_MAX")} MiB (cache_limit $E2E_CACHE_LIMIT + chunk $E2E_CHUNK_MIB MiB)"
        echo "[e2e] documents at the end: .bin ${DOCS_BIN_END:-n/a}, .idx ${DOCS_IDX_END:-n/a}"
        echo "[e2e] FLOOD_WAIT lines in daemon logs: $total_flood in all (copy of the huge file on A ${FLOOD_HUGE_A:-n/a}, B1 ${FLOOD_B1:-n/a}, B2 ${FLOOD_B2:-n/a}, B3 ${FLOOD_B3:-n/a}); longest wait $(longest_flood) s; [error] lines ${DAEMON_ERRORS:-n/a}"
        echo "[e2e] checks: $passes PASS, $fails FAIL, $skips SKIP"
        awk -F'|' '$3 == "FAIL" { printf "[e2e]   FAIL %s: %s (stage %s)\n", $2, $4, $1 }' "$W/checks.txt"
    } | tee "$W/summary.txt"
}

stage_final() {
    if [[ "$E2E_STORE" == telegram ]]; then
        local ok=0
        run_inspect end && ok=1
        check_if inspect-names-end "$ok" "document names <16 hex>.bin|.idx, no captions (after everything)"
        res DOCS_BIN_END "$(sed -n 's/^documents \.bin: //p' "$W/logs/inspect-end.out")"
        res DOCS_IDX_END "$(sed -n 's/^documents \.idx: //p' "$W/logs/inspect-end.out" | head -n1)"
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
    for m in A B1 B2 B3; do
        [[ -r "$(daemon_log "$m")" ]] || continue
        n="$(grep -ac '\[error\]' "$(daemon_log "$m")" || true)"
        errors=$((errors + ${n:-0}))
    done
    res DAEMON_ERRORS "$errors"
    # Строки [error] не провал сами по себе (повтор после транзиентной ошибки), но видны в сводке.
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
    # Результаты прежнего неудачного запуска этого этапа не копятся.
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
    if [[ -n "$E2E_CHAT_ID" ]]; then
        # Свой канал остаётся: бот удаляет только сообщения telega-fs, и только если этот рабочий
        # каталог проверил канал пустым (маркер state/channel-claimed с тем же id).
        chan_cleanup ||
            { echo "[e2e] the cleanup did not complete: delete the pin and the .bin/.idx documents by hand (see $W/logs/)" >&2; return 1; }
        return 0
    fi
    log "teardown: deleting the channel (user account: delete only)"
    bench e2e --chat-file "$E2E_CHAT_FILE" teardown >"$W/logs/teardown.out" 2>&1 ||
        { echo "[e2e] teardown failed, delete the channel by hand (see $W/logs/teardown.out)" >&2; return 1; }
}

show_status() {
    local s
    for s in "${STAGES[@]}"; do
        printf '%-10s %s\n' "$s" "$([[ -e "$(done_file "$s")" ]] && echo done || echo pending)"
    done
    [[ -e "$W/state/STOP-FLOOD" ]] && echo "STOP-FLOOD: $(cat "$W/state/STOP-FLOOD")"
    awk -F'|' '{ printf "%-5s %-24s %s\n", $3, $2, $4 }' "$W/checks.txt"
}

case "$sub" in
    status) show_status; exit 0 ;;
    teardown) if teardown_channel; then exit 0; else exit 1; fi ;;
esac

# Сброс готовности: E2E_REDO и явный список этапов в аргументах.
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
# Явно названные этапы запускаются заново.
if [[ $# -gt 0 ]]; then
    for s in "$@"; do rm -f "$(done_file "$s")"; done
fi

for s in "${STAGES[@]}"; do
    printf '%s\n' "${wanted[@]}" | grep -qx "$s" || continue
    run_stage "$s"
done

if [[ -e "$(done_file final)" ]]; then
    cleanup_failed=0
    print_summary
    if [[ "${E2E_KEEP_SESSIONS:-0}" != 1 ]]; then
        logout_machine A
    fi
    # Канал удаляется только если все проверки PASS: при FAIL данные канала нужны для разбора.
    # Удалить вручную: tests/e2e/e2e_resources.sh teardown.
    if grep -q '|FAIL|' "$W/checks.txt"; then
        echo "[e2e] the channel (its data) is kept because of FAIL checks; clean it with: $0 teardown"
    elif [[ "${E2E_KEEP_CHANNEL:-0}" != 1 && ! -e "$W/state/channel-deleted" &&
        ! -e "$W/state/channel-reused" ]]; then
        if teardown_channel; then
            touch "$W/state/channel-deleted"
        elif [[ -n "$E2E_CHAT_ID" ]]; then
            cleanup_failed=1    # свой канал не очищен: об этом сообщаем кодом выхода
        fi
    fi
    if grep -q '|FAIL|' "$W/checks.txt"; then
        echo "[e2e] E2E FAIL (work dir kept: $W)"
        exit "$EXIT_CHECKS"
    fi
    if [[ "$cleanup_failed" == 1 ]]; then
        echo "[e2e] E2E FAIL: all checks passed, but the channel cleanup did not complete (work dir kept: $W)"
        exit "$EXIT_CHECKS"
    fi
    echo "[e2e] E2E PASS (work dir kept: $W)"
fi
exit 0
