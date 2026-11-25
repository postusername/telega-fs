#include "meta/commit.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <set>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "common/files.h"
#include "crypto/index_blob.h"
#include "meta/index_ops.h"
#include "store/local_dir_store.h"

namespace telega::meta {

namespace {

using SteadyClock = std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::nanoseconds;

class RealClock final : public Clock {
  public:
    time_point now() override { return SteadyClock::now(); }
    void wait(std::condition_variable& cv, std::unique_lock<std::mutex>& lock,
              std::optional<time_point> deadline) override {
        if (deadline) {
            cv.wait_until(lock, *deadline);
        } else {
            cv.wait(lock);
        }
    }
};

// Удаление сообщений и строк уборкой идёт пачками (deleteMessages по 100).
constexpr size_t kDeleteBatch = 100;

nanoseconds since(SteadyClock::time_point start) {
    return duration_cast<nanoseconds>(SteadyClock::now() - start);
}

int64_t wall_ns() {
    return duration_cast<nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void remove_quietly(const std::filesystem::path& path) {
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}

BlobKind to_history_kind(crypto::BlobKind kind) {
    return kind == crypto::BlobKind::snapshot ? BlobKind::snapshot : BlobKind::delta;
}

const char* kind_name(crypto::BlobKind kind) {
    return kind == crypto::BlobKind::snapshot ? "snapshot" : "delta";
}

// Голова канала совпадает с ожидаемой (CAS-проверка, шаг 4)?
// foreign: ожидается чужое сообщение (потеря головы), сверяется только msg_id.
bool head_matches(const std::optional<store::HeadInfo>& head, std::optional<int64_t> msg_id,
                  uint64_t generation, bool foreign = false) {
    if (!msg_id) {
        return !head.has_value();
    }
    if (!head || head->msg_id != *msg_id) {
        return false;
    }
    if (foreign) {
        return true;
    }
    try {
        return crypto::parse_index_header(head->header).generation == generation;
    } catch (const crypto::CorruptError&) {
        return false;
    }
}

}  // namespace

Clock& steady_clock() {
    static RealClock clock;
    return clock;
}

// ---------------------------------------------------------------------------------------------
// Тело блоба

Bytes encode_delta_body(const std::vector<int64_t>& chain, ByteSpan patchset) {
    if (chain.size() > UINT32_MAX) {
        throw std::invalid_argument("delta chain is too long");
    }
    Bytes out(4);
    put_u32_le(out, static_cast<uint32_t>(chain.size()));
    out.reserve(4 + chain.size() * 8 + patchset.size());
    for (const int64_t id : chain) {
        append_u64_le(out, static_cast<uint64_t>(id));
    }
    out.insert(out.end(), patchset.begin(), patchset.end());
    return out;
}

DeltaBody decode_delta_body(ByteSpan body) {
    if (body.size() < 4) {
        throw crypto::CorruptError("delta body: too short");
    }
    const uint64_t n = get_u32_le(body);
    if ((body.size() - 4) / 8 < n) {
        throw crypto::CorruptError("delta body: chain list is truncated");
    }
    DeltaBody out;
    out.chain.reserve(static_cast<size_t>(n));
    for (uint64_t i = 0; i < n; ++i) {
        out.chain.push_back(static_cast<int64_t>(get_u64_le(body.subspan(4 + i * 8, 8))));
    }
    const auto rest = body.subspan(static_cast<size_t>(4 + n * 8));
    out.patchset.assign(rest.begin(), rest.end());
    return out;
}

std::filesystem::path snapshot_tmp_path(const std::filesystem::path& tmp_dir, uint64_t generation) {
    return tmp_dir / ("index-" + std::to_string(generation) + ".sqlite");
}

std::filesystem::path snapshot_raw_path(const std::filesystem::path& tmp_dir, uint64_t generation) {
    auto path = snapshot_tmp_path(tmp_dir, generation);
    path += ".raw";
    return path;
}

void install_dirty_hook(MetaStore& meta, StateStore& state, std::function<void()> extra) {
    meta.set_dirty_hook([&state, extra = std::move(extra)] {
        state.set_int(state_key::index_dirty, 1);
        if (extra) {
            extra();
        }
    });
}

// ---------------------------------------------------------------------------------------------
// CommitManager: жизненный цикл

CommitManager::CommitManager(MetaStore& meta, StateStore& state, store::Store& store, BlobKeys keys,
                             CommitConfig config)
    : meta_(meta),
      state_(state),
      store_(store),
      keys_(std::move(keys)),
      config_(std::move(config)),
      clock_(config_.clock != nullptr ? *config_.clock : steady_clock()) {
    read_only_.store(config_.read_only);
    const auto g = state_.get_int_or(state_key::base_generation, 0);
    base_generation_.store(static_cast<uint64_t>(g));
    std::filesystem::create_directories(config_.tmp_dir);
    const bool changed = index_changed();
    {
        const std::lock_guard lock(mu_);
        stats_committed_gen_ = static_cast<uint64_t>(g);
        dirty_ = changed;
        last_commit_ = clock_.now();
        backoff_ = config_.retry_min;
    }
    force_next_snapshot_.store(state_.get_int_or(state_key::force_snapshot, 0) != 0);
    install_dirty_hook(meta_, state_, [this] { on_dirty(); });
    meta_.set_rows_hook(
        static_cast<int64_t>(std::min<uint64_t>(config_.patch_take_rows, INT64_MAX)),
        [this] { notify_rows(); });
}

CommitManager::~CommitManager() {
    stop();
    // Хуки без уведомления: MetaStore может пережить этот объект.
    meta_.set_rows_hook(0, {});
    install_dirty_hook(meta_, state_);
}

void CommitManager::start() {
    const std::lock_guard lock(mu_);
    if (thread_.joinable() || stopping_) {
        return;
    }
    thread_ = std::thread([this] { background_loop(); });
}

void CommitManager::stop() {
    {
        const std::lock_guard lock(mu_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
    // Дождаться коммита, начатого из другого потока (commit_now).
    const std::lock_guard wait_commit(commit_mu_);
}

void CommitManager::on_dirty() {
    {
        const std::lock_guard lock(mu_);
        dirty_ = true;
    }
    cv_.notify_all();
}

void CommitManager::notify_rows() {
    // Пустая критическая секция: уведомление не должно проскочить между проверкой условия
    // фоновым потоком и его засыпанием.
    { const std::lock_guard lock(mu_); }
    cv_.notify_all();
}

bool CommitManager::take_due() const {
    return config_.patch_take_rows > 0 && !read_only_.load() && !unconfirmed_pin_.load() &&
           !force_next_snapshot_.load() &&
           static_cast<uint64_t>(meta_.rows_since_take()) >= config_.patch_take_rows;
}

void CommitManager::chunk_uploaded() {
    {
        const std::lock_guard lock(mu_);
        ++chunks_since_commit_;
        reprobe_.store(true);  // очередь могла опустеть: пробу не ограничивать по частоте
    }
    // Всегда будим: очередь могла опустеть, и отложенный коммит по интервалу пора делать.
    cv_.notify_all();
}

void CommitManager::set_upload_probe(std::function<bool()> busy) {
    {
        const std::lock_guard lock(probe_mu_);
        upload_probe_ = std::move(busy);
    }
    reprobe_.store(true);
    cv_.notify_all();
}

bool CommitManager::uploads_busy() {
    const std::lock_guard lock(probe_mu_);
    return upload_probe_ && upload_probe_();
}

uint64_t CommitManager::base_generation() const { return base_generation_.load(); }

void CommitManager::poke() { cv_.notify_all(); }

void CommitManager::set_cas_override(int64_t head_msg_id, uint64_t head_generation) {
    const std::lock_guard lock(commit_mu_);
    cas_override_ = CasExpect{head_msg_id, head_generation, false};
    const std::lock_guard force(force_mu_);
    force_epoch_.fetch_add(1);
    force_next_snapshot_ = true;
}

void CommitManager::set_cas_override_lost_head(std::optional<int64_t> foreign_msg_id) {
    const std::lock_guard lock(commit_mu_);
    cas_override_ = CasExpect{foreign_msg_id, 0, foreign_msg_id.has_value()};
    const std::lock_guard force(force_mu_);
    force_epoch_.fetch_add(1);
    force_next_snapshot_ = true;
}

bool CommitManager::wait_for_generation(uint64_t generation, std::chrono::milliseconds timeout) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return stats_committed_gen_ >= generation; });
}

CommitStats CommitManager::stats() const {
    const std::lock_guard lock(mu_);
    return stats_;
}

bool CommitManager::index_changed() {
    return meta_.dirty_since_take() || state_.get_int_or(state_key::index_dirty, 0) != 0;
}

void CommitManager::background_loop() {
    std::unique_lock lock(mu_);
    // Проба очереди заливки дорога (Vfs::status() считает очередь целиком), а цикл просыпается
    // от каждой записи в индекс. В окне отсрочки проба идёт не чаще раза в defer_recheck, если
    // её не потребовала заливка чанка или смена пробника (reprobe_). Результат пробы живёт только
    // в своём окне отсрочки: вне его (коммит сделан, интервал не вышел) он сбрасывается, иначе
    // новое окно пропустило бы первую пробу. Только этот поток.
    std::optional<Clock::time_point> probed_at;
    while (!stopping_) {
        const auto now = clock_.now();
        std::optional<Clock::time_point> deadline;
        bool due = false;
        bool take = false;
        bool deferring = false;
        if (read_only_.load()) {
            due = false;  // после CAS-конфликта коммитить нельзя: ждём остановки
        } else if (retry_at_) {
            due = now >= *retry_at_;
            deadline = *retry_at_;
        } else {
            if (dirty_) {
                const auto interval_deadline = last_commit_ + config_.commit_interval;
                if (chunks_since_commit_ >= config_.commit_every_chunks) {
                    due = true;  // без отсрочки
                } else if (now < interval_deadline) {
                    deadline = interval_deadline;
                } else {
                    // Интервал вышел. Пока очередь заливки не пуста, откладываем, но
                    // не дольше commit_defer_factor · commit_interval с прошлого коммита.
                    const auto hard_deadline =
                        last_commit_ + config_.commit_interval * config_.commit_defer_factor;
                    if (now >= hard_deadline) {
                        due = true;
                    } else if (!reprobe_.exchange(false) && probed_at &&
                               now < *probed_at + config_.defer_recheck) {
                        // Недавняя проба показала «занято»: ждать до следующей.
                        deferring = true;
                        deadline = std::min(hard_deadline, *probed_at + config_.defer_recheck);
                    } else {
                        lock.unlock();  // пробник заглядывает в Vfs: без mu_
                        const bool busy = uploads_busy();
                        lock.lock();
                        probed_at = clock_.now();
                        if (busy) {
                            deferring = true;
                            deadline = std::min(hard_deadline, *probed_at + config_.defer_recheck);
                            spdlog::debug("commit: deferred, the upload queue is not empty");
                        } else {
                            due = true;
                        }
                    }
                }
            }
            take = !due && take_due();
        }
        if (!deferring) {
            probed_at.reset();  // проба относится только к своему окну отсрочки
        }

        if (take) {
            // Промежуточный забор: забрать патч, не делая коммит. Ошибка уже в логе и
            // в retry_at_ (record_failure), следующая попытка — коммит-снимок.
            lock.unlock();
            try {
                take_intermediate();
            } catch (const std::exception& e) {
                spdlog::debug("intermediate patch take: {}", e.what());
            }
            lock.lock();
            continue;
        }
        if (!due) {
            clock_.wait(cv_, lock, deadline);
            continue;
        }
        lock.unlock();
        bool crashed = false;
        try {
            commit(false);
        } catch (const store::CrashInjected&) {
            crashed = true;  // тест имитирует падение процесса: поток просто умирает
        } catch (const std::exception& e) {
            // Уже в логе и в retry_at_ (record_failure): фон повторит после задержки.
            spdlog::debug("background commit: {}", e.what());
        }
        lock.lock();
        if (crashed) {
            return;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Коммит

void CommitManager::commit_now() { commit(false); }

std::optional<TakeStats> CommitManager::take_intermediate() {
    if (read_only_.load() || unconfirmed_pin_.load() || force_next_snapshot_.load()) {
        return std::nullopt;
    }
    TakeStats stats;
    try {
        const std::lock_guard queue(patch_queue_mu_);
        try {
            const Bytes patch = meta_.with_lock([&] { return meta_.take_patchset(&stats); });
            if (patch.empty()) {
                return std::nullopt;
            }
            append_patch(patch);
            // В pending теперь лежит ещё не закоммиченный патч (в том числе из одних служебных
            // записей): без index_dirty = 1 коммит бы не запустился, а монтирование приняло бы
            // pending за локальные изменения при чистом индексе.
            state_.set_int(state_key::index_dirty, 1);
        } catch (...) {
            // Патч забран из сессии, но не сохранён (или не сохранён флаг): дельта его потеряла
            // бы. Флаг ставится, пока держится очередь: коммит, решивший сделать дельту до
            // очереди, перепроверяет его под ней (commit_attempt) и уйдёт на снимок.
            mark_force_snapshot();
            throw;
        }
    } catch (const std::exception& e) {
        const std::string what = std::string("intermediate patch take: ") + e.what();
        record_failure(what);
        throw CommitError(what);
    }
    on_dirty();
    {
        const std::lock_guard lock(mu_);
        ++stats_.intermediate_takes;
    }
    return stats;
}

CommitResult CommitManager::commit(bool force_snapshot) {
    // Параллельные вызовы сливаются: если после входа сюда началась и удачно закончилась
    // другая попытка, она забрала и наши изменения (забор патча был позже нашего входа).
    const uint64_t entry = attempts_started_.load();
    const std::lock_guard lock(commit_mu_);
    if (!force_snapshot && last_success_attempt_.load() > entry) {
        return {};
    }
    bool restart_as_snapshot = false;
    CommitResult result = commit_attempt(force_snapshot, restart_as_snapshot);
    if (restart_as_snapshot) {
        // Под очередью патчей выяснилось, что нужен снимок; ничего не забрано, попытка заново.
        result = commit_attempt(true, restart_as_snapshot);
    }
    return result;
}

bool CommitManager::sleep_for(std::chrono::milliseconds d) {
    std::unique_lock lock(mu_);
    const auto until = clock_.now() + d;
    while (!stopping_ && clock_.now() < until) {
        clock_.wait(cv_, lock, until);
    }
    return !stopping_;
}

// Голова, которую мог показать кэш TDLib после нашего pin: нет закреплённого
// (404) или более старый блоб этой же ФС. Чужой блоб или более новое поколение — не кэш.
bool CommitManager::is_stale_own_head(const std::optional<store::HeadInfo>& head,
                                      const CasExpect& expect) const {
    if (!expect.msg_id || expect.foreign) {
        // Первый коммит после init или --force-local при потере головы: ждали «головы нет» или
        // чужое сообщение, своего pin до этого не было. Окно 90 с монтирование уже выдержало.
        return false;
    }
    if (!head) {
        return true;
    }
    if (head->msg_id >= *expect.msg_id) {
        return false;
    }
    try {
        const crypto::IndexHeader h = crypto::parse_index_header(head->header);
        return h.fs_uuid == keys_.fs_uuid && h.generation < expect.generation;
    } catch (const crypto::CorruptError&) {
        return false;
    }
}

bool CommitManager::matches(const std::optional<store::HeadInfo>& head, const CasExpect& expect) {
    return head_matches(head, expect.msg_id, expect.generation, expect.foreign);
}

bool CommitManager::cas_with_retry(const std::optional<store::HeadInfo>& first,
                                   const CasExpect& expect) {
    if (!last_pin_ || !is_stale_own_head(first, expect)) {
        return false;
    }
    const auto until = *last_pin_ + config_.cas_retry_window;
    std::optional<store::HeadInfo> head = first;
    while (clock_.now() < until) {
        const auto left =
            std::chrono::duration_cast<std::chrono::milliseconds>(until - clock_.now());
        spdlog::warn(
            "commit: the channel head is older than our last pin (TDLib cache?), checking again "
            "in {} s",
            std::min(config_.cas_retry_period, left).count() / 1000);
        if (!sleep_for(std::min(config_.cas_retry_period, left))) {
            throw std::runtime_error("commit interrupted by shutdown while waiting for the head");
        }
        head = store_.read_head();
        if (matches(head, expect)) {
            return true;
        }
        if (!is_stale_own_head(head, expect)) {
            return false;
        }
    }
    return false;
}

// Голова для разбора неподтверждённого pin. Пока с попытки pin не прошло
// cas_retry_window, ответ «нет головы» или «старая голова» — неизвестность (кэш TDLib):
// чтение повторяется раз в cas_retry_period, блоб не удаляется. Окно нужно, только если pin мог
// пройти; после permanent-ошибки set_head — одно чтение. Возвращает последний ответ.
std::optional<store::HeadInfo> CommitManager::read_head_for_unconfirmed(const Uploaded& up) {
    std::optional<store::HeadInfo> head = store_.read_head();
    if (!up.pin_may_have_passed) {
        return head;
    }
    const auto until = last_pin_.value_or(clock_.now()) + config_.cas_retry_window;
    const auto unknown = [&] { return !head || head->msg_id < up.msg_id; };
    while (unknown() && clock_.now() < until) {
        const auto left =
            std::chrono::duration_cast<std::chrono::milliseconds>(until - clock_.now());
        const auto pause = std::min(config_.cas_retry_period, left);
        spdlog::warn(
            "commit: the head is {} our unconfirmed pin (TDLib cache?), checking again in {} s",
            head ? "older than" : "missing after", pause.count() / 1000);
        if (!sleep_for(pause)) {
            throw std::runtime_error("commit interrupted by shutdown while waiting for the head");
        }
        head = store_.read_head();
    }
    return head;
}

void CommitManager::record_failure(const std::string& what) {
    spdlog::error("commit failed: {}", what);
    {
        const std::lock_guard lock(mu_);
        ++stats_.failures;
        stats_.last_error = what;
        dirty_ = true;
        retry_at_ = clock_.now() + backoff_;
        backoff_ = std::min(backoff_ * 2, config_.retry_max);
    }
    cv_.notify_all();
}

void CommitManager::set_unconfirmed(const Uploaded& up) {
    unconfirmed_ = up;
    unconfirmed_pin_.store(true);
}

void CommitManager::clear_unconfirmed() {
    unconfirmed_.reset();
    unconfirmed_pin_.store(false);
}

void CommitManager::append_patch(const Bytes& patch) {
    if (config_.before_append_pending) {
        config_.before_append_pending();
    }
    state_.append_pending(patch);
}

void CommitManager::mark_force_snapshot() {
    const std::lock_guard force(force_mu_);
    force_epoch_.fetch_add(1);
    force_next_snapshot_.store(true);
    try {
        state_.set_int(state_key::force_snapshot, 1);
    } catch (const std::exception& e) {
        // В памяти флаг стоит; после перезапуска снимок всё равно будет (clean_shutdown = 0).
        spdlog::error("cannot persist force_snapshot: {}", e.what());
    }
}

CommitResult CommitManager::commit_attempt(bool force_snapshot, bool& restart_as_snapshot) {
    const auto started = SteadyClock::now();
    if (read_only_.load()) {
        if (force_snapshot || index_changed()) {
            throw CommitError("file system is read-only, changes cannot be committed");
        }
        return {};
    }
    const uint64_t attempt = ++attempts_started_;

    // Прошлая попытка упала на set_head: pin мог состояться (ответ потерян). Голова скажет.
    //
    // Пока голова недоступна, патч здесь не забирается: сессия растёт в памяти, и записи после
    // неподтверждённой попытки живут только в ней. Забирать их в pending_patchsets нельзя:
    // монтирование узнаёт свой блоб в голове по равенству patchset и склейки pending (G_r = G_l +
    // 1), лишние патчи сломали бы узнавание. Поэтому и фоновый промежуточный забор при
    // неподтверждённом pin не делается (take_due, unconfirmed_pin_). Потеря сессии при
    // размонтировании закрыта иначе: неудачный финальный коммит ставит force_snapshot = 1
    // (MountedFs::unmount), а крах даёт clean_shutdown = 0. Поэтому отдельная пометка «в сессии
    // есть незабранное» в этой ветке не нужна.
    if (unconfirmed_) {
        try {
            const auto head = read_head_for_unconfirmed(*unconfirmed_);
            if (head && head->msg_id == unconfirmed_->msg_id) {
                spdlog::warn("commit: generation {} turned out to be pinned, finishing it",
                             unconfirmed_->generation);
                finish_commit(*unconfirmed_);
                // Это тот же успех шага 8, что и в основном пути: CAS-подмена (--force-local)
                // относилась к этой попытке и теперь указывает на старую голову.
                cas_override_.reset();
                remove_quietly(snapshot_tmp_path(config_.tmp_dir, unconfirmed_->generation));
            } else {
                const std::vector<int64_t> orphan{unconfirmed_->msg_id};
                store_.delete_messages(orphan);
            }
            clear_unconfirmed();
        } catch (const store::CrashInjected&) {
            throw;  // имитация падения процесса: как и в основном пути, ничего не убираем
        } catch (const std::exception& e) {
            // Не только StoreError: finish_commit пишет в state.sqlite, и SqliteError (диск
            // заполнен) без record_failure не сдвинул бы retry_at_: фоновый цикл крутился бы
            // без задержки.
            record_failure(e.what());
            throw CommitError(std::string("commit failed: ") + e.what());
        }
    }

    const bool forced = force_snapshot || force_next_snapshot_ ||
                        state_.get_int_or(state_key::force_snapshot, 0) != 0;
    if (!force_snapshot && !index_changed()) {
        last_success_attempt_.store(attempt);
        return {};
    }
    {
        const std::lock_guard lock(mu_);
        dirty_ = false;  // записи после забора патча снова поставят флаг через хук
    }

    const auto g = static_cast<uint64_t>(state_.get_int_or(state_key::base_generation, 0));
    const int64_t h = state_.get_int_or(state_key::head_msg_id, 0);
    const Bytes head_hash = state_.get_blob(state_key::head_hash).value_or(Bytes{});
    const std::vector<ChainElement> chain = state_.list_chain();

    // Шаг 1: вид блоба (снимок или дельта).
    crypto::BlobKind kind = crypto::BlobKind::delta;
    if (forced || chain.empty() || h == 0 || cas_override_) {
        kind = crypto::BlobKind::snapshot;
    } else {
        int64_t delta_bytes = 0;
        for (size_t i = 1; i < chain.size(); ++i) {
            delta_bytes += chain[i].blob_size;
        }
        if (chain.size() - 1 >= config_.max_chain ||
            static_cast<double>(delta_bytes) >
                config_.checkpoint_ratio * static_cast<double>(chain.front().blob_size)) {
            kind = crypto::BlobKind::snapshot;
        }
    }

    CommitResult result;
    CommitMetrics& m = result.metrics;
    m.kind = kind;
    Uploaded up;
    up.kind = kind;
    up.generation = g + 1;
    m.generation = up.generation;

    const auto snap_path = snapshot_tmp_path(config_.tmp_dir, up.generation);
    const auto snap_raw_path = snapshot_raw_path(config_.tmp_dir, up.generation);
    const auto blob_path = config_.tmp_dir / ("blob-" + std::to_string(up.generation) + ".idx");
    bool snapshot_taken = false;  // изменения ушли из сессии: неудача → force_snapshot
    Bytes raw;

    try {
        // Шаг 2: под блокировкой MetaStore только забор патча или VACUUM INTO.
        if (kind == crypto::BlobKind::delta) {
            std::vector<PendingPatchset> pending;
            bool to_snapshot = false;
            if (config_.before_patch_queue) {
                config_.before_patch_queue();
            }
            {
                // Очередь патчей захвачена до блокировки MetaStore и отпускается после записи
                // патча: другой забор (промежуточный) не обгонит наш при записи в pending.
                const std::lock_guard queue(patch_queue_mu_);
                // Вид блоба решался до очереди. Промежуточный забор, не сохранивший патч, ставит
                // force_snapshot, пока держит очередь (take_intermediate), поэтому здесь флаг
                // виден: дельта без потерянного патча расходилась бы с локальным индексом.
                to_snapshot = force_next_snapshot_.load();
                if (!to_snapshot) {
                    // pending меняют только заборы (под очередью) и шаг 8 (под commit_mu_, он у
                    // нас), поэтому «патчи уже есть» можно узнать до захвата MetaStore.
                    const bool had_pending = state_.max_pending_seq() != 0;
                    Bytes patch;
                    meta_.with_lock([&] {
                        patch = meta_.take_patchset(&m.take);
                        up.take_ns = wall_ns();  // для committed_take_ns
                        if (!patch.empty() || had_pending) {
                            // Записи после забора попадут в следующий блоб: чанк, умерший в них,
                            // ещё жив в поколении G+1 (dead_since_gen).
                            base_generation_.store(up.generation);
                        } else {
                            state_.set_int(state_key::index_dirty, 0);  // все записи откатились
                        }
                    });
                    if (!patch.empty()) {
                        try {
                            append_patch(patch);
                        } catch (...) {
                            mark_force_snapshot();  // патч забран из сессии, но не сохранён
                            throw;
                        }
                    }
                    pending = state_.list_pending();
                }
            }
            if (to_snapshot) {
                // Ничего не забрано: commit() повторит попытку снимком.
                spdlog::debug("commit: force_snapshot was set after the blob kind was chosen");
                restart_as_snapshot = true;
                return result;
            }
            if (pending.empty()) {
                // Все записи откатились: в индексе ничего не изменилось.
                last_success_attempt_.store(attempt);
                return result;
            }
            std::vector<Bytes> patches;
            patches.reserve(pending.size());
            for (const auto& p : pending) {
                patches.push_back(p.patchset);
            }
            up.max_pending_seq = pending.back().seq;
            m.pending_merged = pending.size();
            const Bytes patchset = MetaStore::concat_patchsets(patches);
            m.patch_bytes = patchset.size();
            std::vector<int64_t> chain_ids;
            chain_ids.reserve(chain.size());
            for (const auto& el : chain) {
                chain_ids.push_back(el.msg_id);
            }
            raw = encode_delta_body(chain_ids, patchset);
        } else {
            remove_quietly(snap_path);
            {
                // Читательское соединение снимка и соединение копии открываются заранее, вне
                // блокировок. Деструктор закрывает читательскую транзакцию и убирает
                // .raw на любом пути выхода.
                SnapshotCopy copy{meta_.path(), snap_raw_path, snap_path, config_.snapshot_hook};
                {
                    // Очередь патчей держится на время фиксации среза и чтения max_pending_seq:
                    // забранный до среза патч должен успеть лечь в pending (его строки уже в
                    // срезе), забранный после — не входит в снимок и переживёт шаг 8.
                    const std::lock_guard queue(patch_queue_mu_);
                    const auto t = SteadyClock::now();
                    meta_.with_lock([&] {
                        meta_.cut_snapshot(copy.reader(), &m.take);
                        up.take_ns = wall_ns();  // для committed_take_ns
                        // С этого момента сессия пересоздана без патча: сбой снимка означает
                        // force_snapshot.
                        snapshot_taken = true;
                        base_generation_.store(up.generation);
                        // Метки, поставленные до этой точки (они и под очередью патчей), срез
                        // уже покрывает; поставленные позже снимок не сбросит.
                        up.force_epoch = force_epoch_.load();
                    });
                    up.max_pending_seq = state_.max_pending_seq();
                    m.snapshot_hold = since(t);
                }
                const SnapshotCopyStats copied = copy.copy();
                m.snapshot_backup = copied.backup;
                m.vacuum = copied.vacuum;
            }
            raw = read_file(snap_path);
        }
        m.raw_bytes = raw.size();

        // Шаг 3: сжать, зашифровать.
        auto t = SteadyClock::now();
        const Bytes compressed = crypto::zstd_compress(
            raw, kind == crypto::BlobKind::snapshot ? kSnapshotZstdLevel : kDeltaZstdLevel);
        m.compress = since(t);
        m.compressed_bytes = compressed.size();
        raw = Bytes{};

        const CasExpect expect = cas_override_.value_or(
            CasExpect{h != 0 ? std::optional<int64_t>{h} : std::nullopt, g, false});
        crypto::IndexHeader header;
        header.kind = kind;
        header.fs_uuid = keys_.fs_uuid;
        header.generation = up.generation;
        if (kind == crypto::BlobKind::delta) {
            header.snapshot_gen = up.generation - chain.size();
            header.snapshot_msg_id = chain.front().msg_id;
            if (head_hash.size() != header.prev_hash.size()) {
                throw std::runtime_error("state.sqlite: head_hash is missing or malformed");
            }
            std::copy(head_hash.begin(), head_hash.end(), header.prev_hash.begin());
        } else {
            header.snapshot_gen = up.generation;
            header.snapshot_msg_id = 0;
        }
        // Чужое сообщение не бывшая голова: prev_msg_id = 0, как и при пустой голове.
        header.prev_msg_id = expect.foreign ? 0 : expect.msg_id.value_or(0);
        header.kdf = keys_.kdf;
        header.wrapped_master = keys_.wrapped_master;
        t = SteadyClock::now();
        const Bytes blob = crypto::seal_index_blob(keys_.k_index, header, compressed);
        m.encrypt = since(t);
        m.blob_bytes = blob.size();
        up.hash = crypto::blake2b256(blob);
        up.blob_size = static_cast<int64_t>(blob.size());
        write_file_atomic(blob_path, blob);

        // Шаг 4: CAS по голове канала.
        t = SteadyClock::now();
        const auto head = store_.read_head();
        if (!matches(head, expect) && !cas_with_retry(head, expect)) {
            read_only_.store(true);
            const std::string what = "the channel head changed under us (expected generation " +
                                     std::to_string(expect.generation) +
                                     "): someone else modified the file system; "
                                     "switching to read-only";
            spdlog::error("commit: {}", what);
            if (snapshot_taken) {
                mark_force_snapshot();
            }
            remove_quietly(blob_path);
            remove_quietly(snap_path);
            {
                const std::lock_guard lock(mu_);
                ++stats_.failures;
                stats_.last_error = what;
            }
            cv_.notify_all();
            throw CommitError(what);
        }

        // Шаги 5–7: залить, закрепить (точка коммита), открепить старую голову.
        up.msg_id = store_.put_index(blob_path);
        m.msg_id = up.msg_id;
        remove_quietly(blob_path);
        try {
            // Чужое сообщение не открепляем (--force-local).
            store_.set_head(up.msg_id, expect.foreign ? std::nullopt : expect.msg_id);
            last_pin_ = clock_.now();
        } catch (const store::StoreError& e) {
            // Окончательная ошибка: pin не прошёл, ждать кэш TDLib незачем. Иначе pin
            // мог состояться, а кэш TDLib мог его не показать.
            up.pin_may_have_passed = e.kind() != store::StoreErrorKind::permanent;
            if (up.pin_may_have_passed) {
                last_pin_ = clock_.now();
            }
            set_unconfirmed(up);  // ответ мог потеряться после pin: разберёт следующая попытка
            throw;
        }
        m.upload = since(t);
    } catch (const store::CrashInjected&) {
        throw;  // имитация падения процесса: ничего не убираем, как и настоящий сбой
    } catch (const CommitError&) {
        throw;
    } catch (const std::exception& e) {
        if (snapshot_taken) {
            mark_force_snapshot();  // изменения до VACUUM INTO ушли из сессии
        }
        remove_quietly(blob_path);
        if (!unconfirmed_) {
            remove_quietly(snap_path);
        }
        const std::string what = std::string(kind_name(kind)) + " generation " +
                                 std::to_string(up.generation) + ": " + e.what();
        record_failure(what);
        throw CommitError("commit failed: " + what);
    }

    // Шаг 8. Pin уже сделан: если state.sqlite не записался, следующая попытка (или
    // монтирование) увидит свой блоб в голове и закончит шаг 8.
    try {
        finish_commit(up);
    } catch (const store::CrashInjected&) {
        throw;
    } catch (const std::exception& e) {
        set_unconfirmed(up);
        const std::string what = "generation " + std::to_string(up.generation) +
                                 " is pinned but local state was not updated: " + e.what();
        record_failure(what);
        throw CommitError("commit failed: " + what);
    }
    cas_override_.reset();
    remove_quietly(snap_path);
    last_success_attempt_.store(attempt);

    // Шаг 9.
    try {
        if (!config_.skip_gc) {
            gc(up.generation, m);
        }
    } catch (const store::CrashInjected&) {
        throw;
    } catch (const std::exception& e) {
        spdlog::warn("gc after commit {}: {}", up.generation, e.what());
    }
    m.total = since(started);
    spdlog::info("commit: generation {} ({}, blob {} bytes, msg {})", up.generation,
                 kind_name(kind), m.blob_bytes, up.msg_id);
    {
        const std::lock_guard lock(mu_);
        ++stats_.commits;
        ++(kind == crypto::BlobKind::snapshot ? stats_.snapshots : stats_.deltas);
        stats_.last = m;
        stats_.last_error.clear();
        retry_at_.reset();
        backoff_ = config_.retry_min;
        last_commit_ = clock_.now();
        chunks_since_commit_ = 0;
    }
    cv_.notify_all();
    result.committed = true;
    return result;
}

void CommitManager::finish_commit(const Uploaded& up) {
    // Флаг force_snapshot снимок сбрасывает, только если с его среза флаг не ставили заново
    // (иначе потерянный после среза патч остался бы без снимка). force_mu_ держится до конца:
    // постановка флага не вклинится между проверкой эпохи и записью.
    const std::lock_guard force(force_mu_);
    const bool clear_force =
        up.kind == crypto::BlobKind::snapshot && force_epoch_.load() == up.force_epoch;
    // Одна транзакция state.sqlite под мьютексом MetaStore: index_dirty = dirty_since_take()
    // не теряет записей, сделанных во время коммита (их хук уже поставил index_dirty = 1).
    meta_.with_lock([&] {
        // Повтор после сбоя в середине шага 8: транзакция state.sqlite уже прошла.
        const bool already =
            state_.get_int_or(state_key::base_generation, 0) == static_cast<int64_t>(up.generation);
        state_.transaction([&] {
            if (already) {
                return;
            }
            state_.set_int(state_key::base_generation, static_cast<int64_t>(up.generation));
            state_.set_int(state_key::head_msg_id, up.msg_id);
            state_.set_blob(state_key::head_hash, up.hash);
            if (up.take_ns != 0) {
                state_.set_int(state_key::committed_take_ns, up.take_ns);
            }
            // Только включённые патчи: промежуточный забор мог дописать новые, пока шёл коммит.
            state_.delete_pending_upto(up.max_pending_seq);
            if (up.kind == crypto::BlobKind::delta) {
                state_.append_chain(up.msg_id, up.blob_size);
            } else {
                state_.reset_chain_to_snapshot(up.msg_id, up.blob_size);
                if (clear_force) {
                    state_.set_int(state_key::force_snapshot, 0);
                }
            }
            // Оставшиеся патчи ещё не закоммичены. Записи после забора уже поставили флаг сами.
            const bool uncommitted = meta_.dirty_since_take() || state_.max_pending_seq() != 0;
            state_.set_int(state_key::index_dirty, uncommitted ? 1 : 0);
        });
        base_generation_.store(std::max(base_generation_.load(), up.generation));
    });
    if (clear_force) {
        force_next_snapshot_.store(false);
    }
    {
        const std::lock_guard lock(mu_);
        stats_committed_gen_ = up.generation;
    }
    cv_.notify_all();
    // Строка index_history уйдёт со следующим блобом; dirty не ставит.
    meta_.write(
        [&](Db& db) {
            Stmt st{db, "SELECT 1 FROM index_history WHERE generation = ?"};
            st.bind(1, static_cast<int64_t>(up.generation));
            if (!st.step()) {
                add_history(db, static_cast<int64_t>(up.generation), up.msg_id,
                            to_history_kind(up.kind));
            }
        },
        WriteKind::housekeeping);
}

// ---------------------------------------------------------------------------------------------
// Отложенная уборка

void CommitManager::gc(uint64_t generation, CommitMetrics& metrics) {
    metrics.gc_chunks_deleted = gc_chunks(generation);
    metrics.gc_blobs_deleted = gc_blobs(generation);
}

CommitManager::GcResult CommitManager::collect_garbage() {
    const std::lock_guard lock(commit_mu_);
    CommitMetrics metrics;
    try {
        gc(static_cast<uint64_t>(state_.get_int_or(state_key::base_generation, 0)), metrics);
    } catch (const store::CrashInjected&) {
        throw;
    } catch (const std::exception& e) {
        throw CommitError(std::string("gc failed: ") + e.what());
    }
    return {metrics.gc_chunks_deleted, metrics.gc_blobs_deleted};
}

// Порядок для чанка: (1) запись в upload_journal (chunk_id, msg_id) — долговечное намерение;
// (2) удаление строки chunks (housekeeping); (3) deleteMessages; (4) удаление записи журнала.
// Сначала строка, потом сообщение: при сбое между ними индекс не ссылается на удалённое
// сообщение (иначе дедупликация могла бы воскресить чанк без данных). Сообщение, оставшееся
// после сбоя, удалит уборка сирот при монтировании по записи журнала, а
// после ошибки Store — следующий проход уборки (gc_retry_).
size_t CommitManager::gc_chunks(uint64_t generation) {
    if (!gc_retry_.empty()) {
        delete_victims(std::exchange(gc_retry_, {}));
    }
    if (generation < config_.retain_generations) {
        return 0;
    }
    const auto max_dead = static_cast<int64_t>(generation - config_.retain_generations);
    size_t deleted = 0;
    for (;;) {
        const auto candidates = meta_.read([&](Db& db) {
            return list_gc_candidates(db, max_dead, static_cast<int64_t>(kDeleteBatch));
        });
        if (candidates.empty()) {
            break;
        }
        const int64_t now = wall_ns();
        state_.transaction([&] {
            for (const Chunk& c : candidates) {
                state_.remove_upload(c.chunk_id);
                state_.upload_started(c.chunk_id, now);
                state_.upload_finished(c.chunk_id, c.msg_id);
            }
        });
        std::vector<GcVictim> victims;
        meta_.write(
            [&](Db& db) {
                victims.clear();
                for (const Chunk& c : candidates) {
                    // Между чтением и записью чанк мог ожить (дедупликация): перепроверить.
                    const auto cur = get_chunk(db, c.chunk_id);
                    if (cur && cur->refcount == 0 && cur->dead_since_gen &&
                        *cur->dead_since_gen <= max_dead) {
                        delete_chunk_row(db, c.chunk_id);
                        victims.push_back({c.chunk_id, cur->msg_id});
                    }
                }
            },
            WriteKind::housekeeping);
        if (victims.size() != candidates.size()) {
            state_.transaction([&] {
                for (const Chunk& c : candidates) {
                    if (std::none_of(victims.begin(), victims.end(),
                                     [&](const GcVictim& v) { return v.chunk_id == c.chunk_id; })) {
                        state_.remove_upload(c.chunk_id);
                    }
                }
            });
        }
        if (victims.empty()) {
            break;
        }
        deleted += victims.size();
        delete_victims(std::move(victims));
        if (candidates.size() < kDeleteBatch) {
            break;
        }
    }
    return deleted;
}

void CommitManager::delete_victims(std::vector<GcVictim> victims) {
    for (size_t at = 0; at < victims.size(); at += kDeleteBatch) {
        const size_t end = std::min(victims.size(), at + kDeleteBatch);
        std::vector<int64_t> ids;
        ids.reserve(end - at);
        for (size_t i = at; i < end; ++i) {
            ids.push_back(victims[i].msg_id);
        }
        try {
            store_.delete_messages(ids);
        } catch (const store::StoreError& e) {
            spdlog::warn("gc: cannot delete {} chunk messages, will retry: {}", ids.size(),
                         e.what());
            gc_retry_.insert(gc_retry_.end(), victims.begin() + static_cast<std::ptrdiff_t>(at),
                             victims.end());
            return;
        }
        state_.transaction([&] {
            for (size_t i = at; i < end; ++i) {
                state_.remove_upload(victims[i].chunk_id);
            }
        });
    }
}

// Старые цепочки блобов: снимок и все его дельты, когда G ≥ g_last + retain_generations.
// Текущая цепочка (state.chain) не трогается никогда. Сначала сообщения, потом строки
// index_history: при сбое между ними строки остаются, и удаление повторится (повторное
// удаление отсутствующих сообщений — не ошибка).
size_t CommitManager::gc_blobs(uint64_t generation) {
    const auto chain = state_.list_chain();
    if (chain.empty() || generation < config_.retain_generations) {
        return 0;
    }
    std::set<int64_t> current;
    for (const auto& el : chain) {
        current.insert(el.msg_id);
    }
    const auto current_snapshot_gen =
        static_cast<int64_t>(generation) - static_cast<int64_t>(chain.size()) + 1;
    const auto history = meta_.read([](Db& db) { return list_history(db); });

    // Разбить на цепочки: каждая начинается со снимка.
    std::vector<std::vector<HistoryEntry>> groups;
    for (const auto& e : history) {
        if (e.generation >= current_snapshot_gen) {
            break;
        }
        if (groups.empty() || e.kind == BlobKind::snapshot) {
            groups.emplace_back();
        }
        groups.back().push_back(e);
    }
    size_t deleted = 0;
    for (const auto& group : groups) {
        const int64_t g_last = group.back().generation;
        if (static_cast<int64_t>(generation) <
            g_last + static_cast<int64_t>(config_.retain_generations)) {
            break;
        }
        std::vector<int64_t> ids;
        for (const auto& e : group) {
            if (!current.contains(e.msg_id)) {
                ids.push_back(e.msg_id);
            }
        }
        for (size_t at = 0; at < ids.size(); at += kDeleteBatch) {
            const size_t end = std::min(ids.size(), at + kDeleteBatch);
            const std::vector<int64_t> batch(ids.begin() + static_cast<std::ptrdiff_t>(at),
                                             ids.begin() + static_cast<std::ptrdiff_t>(end));
            try {
                store_.delete_messages(batch);
            } catch (const store::StoreError& e) {
                spdlog::warn("gc: cannot delete old index blobs, will retry: {}", e.what());
                return deleted;
            }
        }
        meta_.write([&](Db& db) { delete_history_upto(db, g_last); }, WriteKind::housekeeping);
        deleted += ids.size();
    }
    return deleted;
}

}  // namespace telega::meta
