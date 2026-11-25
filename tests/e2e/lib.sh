# Общие функции E2E-скриптов для работы с готовым каналом (E2E_CHAT_ID). Не запускается сам:
# подключается из e2e_basic.sh, e2e_resources.sh и e2e_maint.sh (`. "$here/lib.sh"`).
#
# Что ждёт от скрипта: переменные W (рабочий каталог), E2E_CHAT_ID, E2E_CHAT_FILE; функции
# bench (tgfs-bench с кредами), log, die (жёсткая ошибка), env_die (неверное окружение).
# Необязательно: E2E_SCAN_MAX_ID (потолок серверного id текста-зонда, по умолчанию 100000 в
# tgfs-bench), E2E_WIPE_FIRST=1, TGFS_BIN (для поиска демонов).
#
# Работает только бот. Канал считается «нашим» (его можно чистить), если check-empty нашёл его
# пустым или E2E_WIPE_FIRST=1 убрал из него всё, что принадлежит telega-fs, и повторный check-empty
# это подтвердил. Тогда пишется маркер $W/state/channel-claimed с id канала. Уборка (wipe) идёт
# только при маркере с тем же id, чтобы случайно не почистить чужой канал.

CHAN_SCAN_ARGS=()
if [[ -n "${E2E_SCAN_MAX_ID:-}" ]]; then
    CHAN_SCAN_ARGS=(--scan-max-id "$E2E_SCAN_MAX_ID")
fi

chan_marker() { echo "$W/state/channel-claimed"; }

# Вызывается до любых действий: рабочий каталог, который уже вёл другой канал, не берём.
chan_guard_start() {
    [[ -n "$E2E_CHAT_ID" ]] || return 0
    local m
    m="$(chan_marker)"
    if [[ -r "$m" && "$(cat "$m")" != "$E2E_CHAT_ID" ]]; then
        env_die "the work dir $W was used with another channel (state/channel-claimed differs from E2E_CHAT_ID); use a new E2E_WORK or the same E2E_CHAT_ID"
    fi
}

chan_claim() {
    mkdir -p "$W/state"
    printf '%s\n' "$E2E_CHAT_ID" >"$(chan_marker)"
}

# 0: канал проверен этим рабочим каталогом; иначе сообщение и 1.
chan_require_claimed() {
    local m
    m="$(chan_marker)"
    if [[ ! -r "$m" ]]; then
        echo "[e2e] refusing to clean the channel: this run has not checked E2E_CHAT_ID as empty (no state/channel-claimed)" >&2
        return 1
    fi
    if [[ "$(cat "$m")" != "$E2E_CHAT_ID" ]]; then
        echo "[e2e] refusing to clean the channel: state/channel-claimed holds another channel id than E2E_CHAT_ID" >&2
        return 1
    fi
}

# chan_check_empty ИМЯ_ЛОГА: код tgfs-bench e2e check-empty (0 пусто, 3 не пусто, иначе не удалось).
# Вывод в logs/ИМЯ.out, итог печатается. «Пусто» значит: нет закрепа и нет документов (текст и
# прочее не считаются). Идёт 1-2 минуты и дольше на больших каналах: закреп ждётся до минуты,
# затем бот шлёт и удаляет текст-зонд и перебирает id 1..id зонда пачками по 100 с паузой 10 с.
chan_check_empty() {
    local name=$1 rc=0
    bench e2e --chat-file "$E2E_CHAT_FILE" check-empty ${CHAN_SCAN_ARGS[@]+"${CHAN_SCAN_ARGS[@]}"} \
        >"$W/logs/$name.out" 2>&1 || rc=$?
    { grep -v '^\[' "$W/logs/$name.out" || true; } | sed 's/^/[e2e]   /'
    return "$rc"
}

# chan_wipe ИМЯ_ЛОГА: tgfs-bench e2e wipe (0 удалено, 1 часть не удалена, 2 ошибка или потолок
# перебора, 3 закреплено чужое; в двух последних случаях ничего не удалено).
chan_wipe() {
    local name=$1 rc=0
    bench e2e --chat-file "$E2E_CHAT_FILE" wipe ${CHAN_SCAN_ARGS[@]+"${CHAN_SCAN_ARGS[@]}"} \
        >"$W/logs/$name.out" 2>&1 || rc=$?
    { grep -E '^(wipe:|scan:|REFUSED|CANNOT)' "$W/logs/$name.out" || true; } | sed 's/^/[e2e]   /'
    if [[ "$rc" != 0 ]]; then
        tail -n 3 "$W/logs/$name.out" >&2
    fi
    return "$rc"
}

# chan_wipe_and_verify ТЕГ: wipe, затем повторный check-empty с итогом. 0 только если и то и то
# прошло и канал пуст; иначе предупреждение и 1.
chan_wipe_and_verify() {
    local tag=$1 wrc=0 crc=0
    chan_wipe "wipe-$tag" || wrc=$?
    chan_check_empty "check-empty-after-$tag" || crc=$?
    if [[ "$wrc" == 0 && "$crc" == 0 ]]; then
        log "the channel is empty after the cleanup"
        return 0
    fi
    echo "[e2e] WARNING: the channel is not clean after the cleanup (wipe exit $wrc, check exit $crc); see $W/logs/wipe-$tag.out and check-empty-after-$tag.out" >&2
    return 1
}

# Шаг «канал» для своего канала: отказ, если он не пуст (иначе прогон затёр бы чужую ФС).
# E2E_WIPE_FIRST=1 разрешает сначала убрать документы и закреп telega-fs; после этого повторный
# check-empty обязан показать пустоту. Успех пишет маркер channel-claimed.
ensure_channel_empty() {
    local rc=0
    chan_check_empty check-empty || rc=$?
    if [[ "$rc" == 3 && "${E2E_WIPE_FIRST:-0}" == 1 ]]; then
        log "E2E_WIPE_FIRST=1: the bot deletes the documents and the pin of telega-fs"
        chan_wipe_and_verify first ||
            env_die "the channel is still not empty after the cleanup (foreign documents or a foreign pin); use another channel"
    elif [[ "$rc" == 3 ]]; then
        env_die "the channel is not empty; use another channel or set E2E_WIPE_FIRST=1 (the bot deletes telega-fs documents and unpins its head)"
    elif [[ "$rc" != 0 ]]; then
        tail -n 5 "$W/logs/check-empty.out" >&2
        die "cannot check the channel (is the bot an administrator with post, edit and delete rights? is E2E_SCAN_MAX_ID too low?)"
    fi
    chan_claim
}

# Pid живых процессов telega-fs этого прогона: из pid-файлов $W/*.pid (демоны и запускающие mount
# процессы) и по /proc/*/cmdline: argv[0] называется как $TGFS_BIN, а в командной строке есть
# каталог $W (точка монтирования). Не pgrep -f: строка поиска попала бы в саму команду.
chan_live_daemons() {
    local f pid d argv0 cmd bin
    bin="$(basename "${TGFS_BIN:-telega-fs}")"
    {
        for f in "$W"/*.pid; do
            [[ -r "$f" ]] || continue
            pid="$(cat "$f" 2>/dev/null)"
            if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
                echo "$pid"
            fi
        done
        for d in /proc/[0-9]*; do
            pid="${d#/proc/}"
            [[ "$pid" != "$$" && -r "$d/cmdline" ]] || continue
            argv0=""
            IFS= read -r -d '' argv0 <"$d/cmdline" 2>/dev/null || true
            [[ "$(basename "$argv0")" == "$bin" ]] || continue
            cmd="$(tr '\0' ' ' <"$d/cmdline" 2>/dev/null)"
            [[ "$cmd" == *"$W/"* ]] && echo "$pid"
        done
    } | sort -un
}

# Отказ (1), если демоны этого прогона живы: они могли бы закоммитить в канал после уборки.
chan_require_no_daemons() {
    local pids
    pids="$(chan_live_daemons | tr '\n' ' ')"
    if [[ -n "${pids// /}" ]]; then
        echo "[e2e] refusing to clean the channel: telega-fs process(es) of this run are still alive (pid $pids): unmount with fusermount3 -u and wait for the final commit, or kill them, then clean again" >&2
        return 1
    fi
}

# Для уборки из того же процесса, который запускал демонов (cleanup в e2e_basic.sh): SIGKILL всем
# живым демонам этого прогона, затем fusermount3 -uz по точкам монтирования.
chan_kill_daemons() {
    local pid m
    for pid in $(chan_live_daemons); do
        kill -KILL "$pid" 2>/dev/null
    done
    for _ in $(seq 1 10); do
        [[ -z "$(chan_live_daemons)" ]] && break
        sleep 1
    done
    for m in "$W"/*/mnt; do
        [[ -d "$m" ]] && mountpoint -q "$m" 2>/dev/null && fusermount3 -uz "$m" 2>/dev/null
    done
    return 0
}

# Уборка в конце: только в нашем (проверенном) канале и когда демонов этого прогона нет; сам
# канал остаётся.
chan_cleanup() {
    chan_require_claimed || return 1
    chan_require_no_daemons || return 1
    log "cleanup: the bot deletes the telega-fs messages in your channel (the channel stays)"
    chan_wipe_and_verify end
}
