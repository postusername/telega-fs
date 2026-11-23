#include "store/download_scheduler.h"

#include <fcntl.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <utility>

#include "common/files.h"

namespace telega::store {

namespace {

using Clock = DownloadScheduler::Clock;
using td::Json;
using td::TdError;
using td::TdException;

constexpr int kReadAheadMaxPrio = 16;
constexpr int kUserMaxPrio = 32;
constexpr int kUserMinPrio = 17;
constexpr int kBlobPrio = 32;

int64_t get_i64(const Json& j, const char* key, int64_t fallback = 0) {
    if (j.is_object()) {
        const auto it = j.find(key);
        if (it != j.end() && it->is_number_integer()) {
            return it->get<int64_t>();
        }
    }
    return fallback;
}

bool get_bool(const Json& j, const char* key) {
    if (j.is_object()) {
        const auto it = j.find(key);
        if (it != j.end() && it->is_boolean()) {
            return it->get<bool>();
        }
    }
    return false;
}

std::string get_str(const Json& j, const char* key) {
    if (j.is_object()) {
        const auto it = j.find(key);
        if (it != j.end() && it->is_string()) {
            return it->get<std::string>();
        }
    }
    return {};
}

const Json& get_sub(const Json& j, const char* key) {
    static const Json kNull;
    if (j.is_object()) {
        const auto it = j.find(key);
        if (it != j.end()) {
            return *it;
        }
    }
    return kNull;
}

}  // namespace

// Ожидающий пользовательский запрос. Живёт на стеке потока в read(); рабочий поток пишет в него
// ошибку под mu_.
struct DownloadScheduler::Wait {
    uint64_t seq = 0;
    int64_t a = 0;
    int64_t b = 0;
    ReadKind kind = ReadKind::user;
    std::optional<TdError> td_error;  // downloadFile не удался
    bool canceled = false;            // скачивание оборвалось

    [[nodiscard]] bool failed() const { return td_error.has_value() || canceled; }
};

struct DownloadScheduler::Plan {
    std::vector<Action> actions;
    std::vector<std::string> stale;
    std::vector<std::pair<std::string, std::string>> touches;
};

int DownloadScheduler::user_prio(size_t rank) {
    return static_cast<int>(
        std::max<int64_t>(kUserMinPrio, kUserMaxPrio - static_cast<int64_t>(rank)));
}

int DownloadScheduler::ra_prio(uint32_t distance) {
    return static_cast<int>(std::clamp<int64_t>(kReadAheadMaxPrio - static_cast<int64_t>(distance),
                                                1, kReadAheadMaxPrio));
}

DownloadScheduler::DownloadScheduler(td::TdClient& client, SchedulerOptions options,
                                     SchedulerHooks hooks)
    : client_(client), opts_(options), hooks_(std::move(hooks)) {
    sub_ = client_.subscribe("updateFile", [this](const Json& u) { on_update(u); });
    worker_ = std::thread([this] { loop(); });
}

DownloadScheduler::~DownloadScheduler() {
    sub_.reset();  // отписка ждёт завершения идущего обработчика
    {
        const std::lock_guard<std::mutex> lock(mu_);
        stop_ = true;
    }
    worker_cv_.notify_all();
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

SchedulerStats DownloadScheduler::stats() const {
    const std::lock_guard<std::mutex> lock(mu_);
    SchedulerStats s = stats_;
    s.waiting = waiting_;
    s.plans = plans_;
    s.plans_since_event = plans_ - plans_at_event_;
    return s;
}

size_t DownloadScheduler::tracked_files() const {
    const std::lock_guard<std::mutex> lock(mu_);
    return files_.size();
}

DownloadScheduler::FileState& DownloadScheduler::track(const FileRef& file) {
    FileState& f = files_[file.id];
    f.id = file.id;
    if (file.size > 0) {
        f.size = static_cast<int64_t>(file.size);
    }
    if (!file.remote_id.empty() && f.remote_id.empty()) {
        f.remote_id = file.remote_id;
    }
    f.last_use = Clock::now();
    return f;
}

void DownloadScheduler::set_path(FileState& f, const std::string& path) {
    if (!f.path.empty() && f.path != path) {
        f.stale_paths.push_back(f.path);
    }
    f.path = path;
    // Файл получил путь, который сейчас вытесняется (планировщик не знал его при begin_evict):
    // читать его нельзя, пока вытеснение не закончено (end_evict снимет метку).
    if (!path.empty() && evicting_paths_.contains(path)) {
        f.evicting = true;
    }
}

void DownloadScheduler::clear_ra(FileState& f) {
    f.ra.clear();
    f.ra_reqs.clear();
}

void DownloadScheduler::rebuild_ra(FileState& f) {
    f.ra.clear();
    for (const RaRequest& r : f.ra_reqs) {
        f.ra.add(r.a, r.b);
    }
}

std::chrono::milliseconds DownloadScheduler::call_budget(Clock::time_point deadline) const {
    const auto left =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    return std::clamp(left, std::chrono::milliseconds{0}, opts_.call_timeout);
}

// ---- Апдейты ----------------------------------------------------------------------------

void DownloadScheduler::apply_file(const Json& file, bool from_reply) {
    const auto it = files_.find(static_cast<int32_t>(get_i64(file, "id", -1)));
    if (it == files_.end()) {
        return;
    }
    FileState& f = it->second;
    const Json& local = get_sub(file, "local");
    const std::string path = get_str(local, "path");
    // Путь из ответа на downloadFile может быть старше уже обработанных апдейтов: берём его, только
    // если своего ещё нет.
    if (!path.empty() && (!from_reply || f.path.empty())) {
        set_path(f, path);
    }
    const std::string remote = get_str(get_sub(file, "remote"), "id");
    if (!remote.empty()) {
        f.remote_id = remote;
    }
    if (f.size == 0) {
        f.size = std::max<int64_t>(get_i64(file, "size"), 0);
    }
    const bool completed = get_bool(local, "is_downloading_completed");
    const int64_t off = get_i64(local, "download_offset");
    const int64_t prefix = get_i64(local, "downloaded_prefix_size");
    {
        UpdateRec rec;
        rec.at = Clock::now();
        rec.reply = from_reply;
        rec.download_offset = off;
        rec.prefix = prefix;
        rec.size = get_i64(file, "size");
        rec.active = get_bool(local, "is_downloading_active");
        rec.completed = completed;
        rec.path_name = std::filesystem::path(path).filename().string();
        spdlog::debug(
            "download trace: file {} {} offset {} prefix {} size {} active {} completed {} path {}",
            f.id, from_reply ? "reply" : "update", rec.download_offset, rec.prefix, rec.size,
            rec.active, rec.completed, rec.path_name);
        f.history.push_back(std::move(rec));
        if (f.history.size() > kUpdateHistory) {
            f.history.pop_front();
        }
    }
    if (completed && f.size > 0) {
        f.ready.add(0, f.size);
    } else if (prefix > 0) {
        f.ready.add(off, f.size > 0 ? std::min(f.size, off + prefix) : off + prefix);
    }
    const int64_t total = f.ready.total();
    if (f.active && total > f.last_total) {
        f.active->last_progress = Clock::now();
    }
    f.last_total = total;
    // «Не качается» при непокрытом окне — признак обрыва (SPEC 10.2), но только апдейт именно об
    // этом окне. По исходникам TDLib 1.8.70 (FileManager.cpp: download() выставляет download_offset
    // и сбрасывает апдейт до ответа на downloadFile; on_download_ok/on_download_error_impl после
    // последнего прогресса сбрасывают отдельный updateFile с is_downloading_active = false) такой
    // финальный апдейт всегда идёт после прогресса и несёт итоговый downloaded_prefix_size, а
    // апдейт о прежнем окне несёт его download_offset. Запоздалый апдейт о прежнем окне
    // опознаётся двумя признаками: он пришёл раньше, чем мы начали отправлять downloadFile нового
    // окна (sent ещё пуст), либо несёт другой download_offset. Ответа на downloadFile ждать нельзя:
    // он и первый апдейт об оборванном окне приходят подряд, а поток вызывающего просыпается позже.
    // TDLib сообщил, что качает: прежний «не качается» (например, апдейт о сбросе локального
    // расположения перед скачиванием, FileManager::on_failed_check_local_location) устарел. Иначе
    // здоровое скачивание, которое ещё не дало первую часть, через failure_guard считалось бы
    // оборванным и отменялось.
    if (f.active && get_bool(local, "is_downloading_active") && off == f.active->off) {
        f.inactive_seen = {};
    }
    if (!from_reply && f.active && f.active->sent != Clock::time_point{} &&
        !get_bool(local, "is_downloading_active") && !completed && off == f.active->off &&
        !f.ready.covers(f.active->off, f.end_of_active())) {
        f.inactive_seen = Clock::now();
    }
    cv_.notify_all();
    worker_cv_.notify_one();
}

void DownloadScheduler::on_update(const Json& update) {
    const Json& file = get_sub(update, "file");
    if (!file.is_object()) {
        return;
    }
    const std::lock_guard<std::mutex> lock(mu_);
    apply_file(file, /*from_reply=*/false);
}

// ---- Диагностика ------------------------------------------------------------------------

namespace {

std::string intervals_str(const IntervalSet& set, size_t max_items = 8) {
    std::string out = "{";
    size_t n = 0;
    for (const auto& [a, b] : set.intervals()) {
        if (n++ == max_items) {
            out += " ...";
            break;
        }
        out += (n > 1 ? " [" : "[") + std::to_string(a) + "," + std::to_string(b) + ")";
    }
    return out + "}";
}

}  // namespace

std::string DownloadScheduler::describe_locked(const FileState& f) const {
    const auto now = Clock::now();
    const auto ago = [&](Clock::time_point t) {
        return t == Clock::time_point{}
                   ? std::string{"never"}
                   : std::to_string(
                         std::chrono::duration_cast<std::chrono::milliseconds>(now - t).count()) +
                         " ms ago";
    };
    std::string out = "file " + std::to_string(f.id) + " size " + std::to_string(f.size) +
                      " path " + std::filesystem::path(f.path).filename().string();
    std::error_code ec;
    std::string on_disk = "no path";
    if (!f.path.empty()) {
        on_disk = std::filesystem::exists(f.path, ec) ? "yes" : "NO";
    }
    out += " (on disk: " + on_disk + ")";
    out += "; ready " + intervals_str(f.ready) + " ra " + intervals_str(f.ra);
    if (f.active) {
        out += "; active window [" + std::to_string(f.active->off) + ", +" +
               std::to_string(f.active->lim) + ") prio " + std::to_string(f.active->prio) +
               (f.active->user ? " user" : " readahead") + " sent " + ago(f.active->sent) +
               ", progress " + ago(f.active->last_progress);
    } else {
        out += "; no active window";
    }
    out += "; evicting " + std::string{f.evicting ? "yes" : "no"} + ", users " +
           std::to_string(f.users) + ", stalls " + std::to_string(f.stalls) + ", retry_at " +
           (f.retry_at > now ? "in the future" : "past") + ", inactive_seen " +
           ago(f.inactive_seen);
    out += "; waiters:";
    for (const Wait* w : f.waiters) {
        out += " [" + std::to_string(w->a) + "," + std::to_string(w->b) + ")" +
               (w->failed() ? " failed" : "");
    }
    out += "; scheduler: " + std::to_string(files_.size()) + " file(s), waiting " +
           std::to_string(waiting_) + ", evicting paths " + std::to_string(evicting_paths_.size());
    size_t active = 0;
    for (const auto& [id, other] : files_) {
        active += other.active ? 1 : 0;
    }
    out += ", active windows " + std::to_string(active) + "/" +
           std::to_string(opts_.download_concurrency);
    out += "; last updates:";
    for (const UpdateRec& r : f.history) {
        out += "\n  " + ago(r.at) + (r.reply ? " reply" : " update") + " offset " +
               std::to_string(r.download_offset) + " prefix " + std::to_string(r.prefix) +
               " size " + std::to_string(r.size) + " active " + (r.active ? "1" : "0") +
               " completed " + (r.completed ? "1" : "0") + " path " + r.path_name;
    }
    return out;
}

std::string DownloadScheduler::describe(int32_t file_id) const {
    const std::lock_guard<std::mutex> lock(mu_);
    const auto it = files_.find(file_id);
    return it == files_.end() ? "file " + std::to_string(file_id) + " is unknown"
                              : describe_locked(it->second);
}

void DownloadScheduler::log_wait_timeout(int32_t file_id, const std::string& dump) {
    std::string tdlib = "no answer";
    try {
        const Json file =
            client_.call(Json{{"@type", "getFile"}, {"file_id", file_id}}, std::chrono::seconds{3});
        const Json& local = get_sub(file, "local");
        tdlib = "local.path " + std::filesystem::path(get_str(local, "path")).filename().string() +
                " active " + (get_bool(local, "is_downloading_active") ? "1" : "0") +
                " completed " + (get_bool(local, "is_downloading_completed") ? "1" : "0") +
                " download_offset " + std::to_string(get_i64(local, "download_offset")) +
                " prefix " + std::to_string(get_i64(local, "downloaded_prefix_size")) + " size " +
                std::to_string(get_i64(file, "size"));
    } catch (const std::exception& e) {
        tdlib = std::string{"getFile failed: "} + e.what();
    }
    spdlog::warn("TelegramStore: the read timed out {}; getFile now: {}", dump, tdlib);
}

// ---- Чтение -----------------------------------------------------------------------------

bool DownloadScheduler::probe_ready(int32_t file_id, int64_t a, int64_t b,
                                    Clock::time_point deadline) {
    int64_t size = 0;
    const auto budget = call_budget(deadline);
    if (budget <= std::chrono::milliseconds{0}) {
        return false;  // срок чтения вышел: дальше ждать нечего, read() сам выдаст таймаут
    }
    try {
        const Json r = client_.call(
            Json{{"@type", "getFileDownloadedPrefixSize"}, {"file_id", file_id}, {"offset", a}},
            budget);
        size = get_i64(r, "size");
    } catch (const TdException&) {
        return false;  // нет данных: обычный путь
    }
    if (size < b - a) {
        return false;
    }
    const std::lock_guard<std::mutex> lock(mu_);
    const auto it = files_.find(file_id);
    if (it != files_.end()) {
        it->second.ready.add(a, b);
    }
    ++stats_.prefix_hits;
    return true;
}

std::string DownloadScheduler::refresh_path(int32_t file_id, Clock::time_point deadline) {
    const auto budget = call_budget(deadline);
    if (budget <= std::chrono::milliseconds{0}) {
        throw StoreError(StoreErrorKind::transient,
                         "TelegramStore: getFile: timed out waiting for the download");
    }
    const Json file = client_.call(Json{{"@type", "getFile"}, {"file_id", file_id}}, budget);
    const std::string path = get_str(get_sub(file, "local"), "path");
    if (!path.empty()) {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = files_.find(file_id);
        if (it != files_.end()) {
            set_path(it->second, path);
        }
    }
    return path;
}

std::string DownloadScheduler::pread_range(int32_t file_id, int64_t offset,
                                           std::span<std::byte> out, Clock::time_point deadline) {
    std::string path;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = files_.find(file_id);
        if (it != files_.end()) {
            path = it->second.path;
        }
    }
    if (path.empty()) {
        path = refresh_path(file_id, deadline);
    }
    // ENOENT сразу после завершения скачивания — не ошибка: временный файл переименован, путь
    // берётся заново (SPEC 8.2).
    constexpr int kAttempts = 4;
    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        try {
            const Fd fd = Fd::open(path, O_RDONLY);
            if (pread_full(fd.get(), out, static_cast<uint64_t>(offset)) < out.size()) {
                throw StoreError(StoreErrorKind::permanent,
                                 "TelegramStore: the downloaded file is shorter than expected");
            }
            return path;
        } catch (const SysError& e) {
            if (e.err() != ENOENT) {
                throw StoreError(
                    StoreErrorKind::permanent,
                    std::string{"TelegramStore: cannot read the downloaded file: "} + e.what());
            }
        }
        const std::string fresh = path.empty() ? std::string{} : refresh_path(file_id, deadline);
        if (!fresh.empty() && fresh != path) {
            path = fresh;
            continue;
        }
        break;
    }
    // Путь прежний, а файла нет: его удалили за спиной TDLib. Готовые диапазоны в памяти
    // недействительны, следующий read() заново спросит getFileDownloadedPrefixSize.
    {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = files_.find(file_id);
        if (it != files_.end()) {
            FileState& f = it->second;
            f.ready.clear();
            // Путь устарел вместе с файлом: новый придёт с ответом и апдейтами downloadFile.
            if (!f.path.empty()) {
                f.stale_paths.push_back(f.path);
                f.path.clear();
            }
        }
    }
    throw LocalFileGone(StoreErrorKind::transient,
                        "TelegramStore: the local copy of the file has gone");
}

bool DownloadScheduler::window_serves(const FileState& f, const Wait& w) {
    if (!f.active) {
        return false;
    }
    // Окно качает от своего offset подряд: оно обслуживает ждущего, если все ещё не готовые куски
    // его диапазона лежат внутри окна. Готовое начало диапазона (его дошло скачать readahead) не
    // мешает: окно, начатое с первого непокрытого байта, иначе никогда не «покрывало» бы ждущего, и
    // планировщик заново выдавал бы тот же downloadFile на каждом проходе (так тысячи вызовов в
    // секунду отменяли друг друга и скачивание не шло).
    const int64_t end = f.end_of_active();
    const auto gaps = f.ready.missing(w.a, w.b);
    return std::ranges::all_of(
        gaps, [&](const auto& gap) { return gap.first >= f.active->off && gap.second <= end; });
}

bool DownloadScheduler::satisfied(const FileState& f, int64_t a, int64_t b) {
    return f.ready.covers(a, b) && !f.path.empty() && !f.evicting;
}

bool DownloadScheduler::unsatisfied(const FileState& f, const Wait& w) {
    return !w.failed() && !satisfied(f, w.a, w.b);
}

void DownloadScheduler::read(const FileRef& file, uint64_t offset, std::span<std::byte> out,
                             ReadKind kind, Clock::time_point deadline) {
    std::string diag;
    try {
        read_impl(file, offset, out, kind, deadline, diag);
    } catch (const StoreError&) {
        if (!diag.empty()) {
            log_wait_timeout(file.id, diag);
        }
        throw;
    }
}

void DownloadScheduler::read_impl(const FileRef& file, uint64_t offset, std::span<std::byte> out,
                                  ReadKind kind, Clock::time_point deadline, std::string& diag) {
    if (out.empty()) {
        return;
    }
    const auto a = static_cast<int64_t>(offset);
    const auto b = a + static_cast<int64_t>(out.size());
    const auto timed_out = [] {
        return StoreError(StoreErrorKind::transient,
                          "TelegramStore: downloadFile: timed out waiting for the download");
    };
    const auto stopped = [] {
        return StoreError(StoreErrorKind::transient, "TelegramStore: the store is stopping");
    };
    {
        // Счётчик растёт до ожидания вытеснения: с читателем состояние файла не забывается
        // (idle_forget), и ссылка на него ниже не повисает.
        const std::lock_guard<std::mutex> lock(mu_);
        ++track(file).users;
    }
    // Счётчик пользователей снимается на любом выходе: файл с читателем не вытесняется из кэша и
    // не забывается.
    struct UserGuard {
        DownloadScheduler& self;
        int32_t id;
        ~UserGuard() {
            const std::lock_guard<std::mutex> lock(self.mu_);
            const auto it = self.files_.find(id);
            if (it != self.files_.end()) {
                --it->second.users;
                it->second.last_use = Clock::now();
            }
        }
    };
    const UserGuard guard{*this, file.id};
    {
        std::unique_lock<std::mutex> lock(mu_);
        const FileState& f = files_.at(file.id);
        if (!cv_.wait_until(lock, deadline, [&] { return !f.evicting || stop_; })) {
            diag = "waiting for the end of an eviction: " + describe_locked(f);
            throw timed_out();
        }
        if (stop_) {
            throw stopped();
        }
    }

    // Ждёт скачивания диапазона; возвращается, когда он готов, путь известен и файл не вытесняется.
    const auto await_download = [&] {
        Wait w;
        w.a = a;
        w.b = b;
        w.kind = kind;
        std::unique_lock<std::mutex> lock(mu_);
        FileState& f = files_.at(file.id);
        w.seq = ++wait_seq_;
        f.waiters.push_back(&w);
        ++waiting_;
        plans_at_event_ = plans_;
        f.last_demand = Clock::now();
        worker_cv_.notify_one();
        const auto leave = [&] {
            std::erase(f.waiters, &w);
            --waiting_;
            f.last_demand = Clock::now();
            worker_cv_.notify_one();  // ранги и приоритеты пересчитываются
        };
        for (;;) {
            if (w.td_error) {
                const TdError error = *w.td_error;
                leave();
                throw TdException(error);
            }
            if (w.canceled) {
                leave();
                throw DownloadCanceled(
                    StoreErrorKind::transient,
                    "TelegramStore: downloadFile: the download has failed or was canceled");
            }
            if (f.ready.covers(a, b) && !f.evicting) {
                if (!f.path.empty()) {
                    leave();
                    break;
                }
                // Путь ещё не пришёл апдейтом: спросить TDLib.
                lock.unlock();
                try {
                    (void)refresh_path(file.id, deadline);
                } catch (...) {
                    lock.lock();
                    leave();
                    throw;
                }
                lock.lock();
                if (satisfied(f, a, b)) {
                    leave();
                    break;
                }
            }
            if (stop_) {
                leave();
                throw stopped();
            }
            if (cv_.wait_until(lock, deadline) == std::cv_status::timeout && !satisfied(f, a, b) &&
                !w.failed()) {
                diag = "waiting for the window [" + std::to_string(a) + ", " + std::to_string(b) +
                       "): " + describe_locked(f);
                leave();
                throw timed_out();
            }
        }
    };
    // TDLib считает диапазон готовым, а локального файла нет (SPEC 10.2): так бывает с файлом,
    // который отправил этот процесс (local.path — временная ссылка, уже удалённая), и с файлом,
    // удалённым за спиной TDLib. getFileDownloadedPrefixSize и getFile состояние диска не
    // проверяют (FileManager.cpp 1.8.70: get_file_view().downloaded_prefix() смотрит только на
    // запись о файле), а download() сначала зовёт check_local_location_async и при отсутствии
    // файла забывает локальное расположение и качает с сервера. Поэтому ENOENT при pread — не
    // ошибка: диапазон считается неготовым и идёт обычным окном downloadFile (skip_probe — не
    // верить getFileDownloadedPrefixSize ещё раз, он снова скажет «готово»).
    bool skip_probe = false;
    std::string path;
    for (int gone_rounds = 0;; ++gone_rounds) {
        for (;;) {
            const bool ready = !skip_probe && probe_ready(file.id, a, b, deadline);
            skip_probe = false;
            if (!ready) {
                await_download();
                break;
            }
            // TDLib считает диапазон готовым. Перед pread нужен путь и барьер вытеснения: файл,
            // которого планировщик не знал при begin_evict, узнаёт вытесняемый путь только здесь
            // (set_path).
            std::unique_lock<std::mutex> lock(mu_);
            FileState& f = files_.at(file.id);
            if (f.evicting) {
                if (!cv_.wait_until(lock, deadline, [&] { return !f.evicting || stop_; })) {
                    diag = "waiting for the end of an eviction: " + describe_locked(f);
                    throw timed_out();
                }
                if (stop_) {
                    throw stopped();
                }
                continue;  // после вытеснения диапазон спрашивается заново
            }
            if (!f.path.empty()) {
                break;
            }
            lock.unlock();
            (void)refresh_path(file.id, deadline);
            lock.lock();
            if (!f.evicting) {
                break;
            }
        }
        try {
            path = pread_range(file.id, a, out, deadline);
            break;
        } catch (const LocalFileGone& e) {
            if (gone_rounds >= 1) {
                throw;  // и после downloadFile файла нет: решает повтор вызывающего
            }
            spdlog::debug("TelegramStore: {}, downloading the range", e.what());
            skip_probe = true;
        }
    }

    std::string remote;
    std::vector<std::string> stale;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = files_.find(file.id);
        if (it != files_.end()) {
            remote = it->second.remote_id;
            stale.swap(it->second.stale_paths);
        }
    }
    if (hooks_.on_forget) {
        for (const std::string& p : stale) {
            hooks_.on_forget(p);
        }
    }
    if (hooks_.on_touch) {
        hooks_.on_touch(path, remote);
    }
}

// ---- Readahead --------------------------------------------------------------------------

void DownloadScheduler::prefetch(const FileRef& file, uint64_t offset, uint64_t len,
                                 uint32_t distance) {
    auto a = static_cast<int64_t>(offset);
    auto b = static_cast<int64_t>(offset + len);
    if (file.size > 0) {
        b = std::min<int64_t>(b, static_cast<int64_t>(file.size));
    }
    if (b <= a) {
        return;
    }
    // Уже скачанное начало диапазона не запрашивается (SPEC 8.2: проверка готового — первый шаг).
    try {
        const Json r = client_.call(
            Json{{"@type", "getFileDownloadedPrefixSize"}, {"file_id", file.id}, {"offset", a}},
            opts_.call_timeout);
        a += std::min<int64_t>(get_i64(r, "size"), b - a);
    } catch (const TdException& e) {
        // Нет данных: запрашиваем весь диапазон.
        spdlog::debug("TelegramStore: prefix size is unknown, prefetching all: {}", e.what());
    }
    if (b <= a) {
        return;
    }
    const std::lock_guard<std::mutex> lock(mu_);
    if (stop_) {
        return;
    }
    FileState& f = track(file);
    if (f.evicting) {
        return;
    }
    const auto now = Clock::now();
    for (const auto& [x, y] : f.ready.missing(a, b)) {
        f.ra.add(x, y);
        // Запрос, целиком лежащий внутри нового, заменяется им (и берёт его срок).
        const int64_t lo = x;
        const int64_t hi = y;
        std::erase_if(f.ra_reqs, [&](const RaRequest& r) { return r.a >= lo && r.b <= hi; });
        f.ra_reqs.push_back({lo, hi, now});
    }
    f.ra_distance = distance;
    plans_at_event_ = plans_;
    worker_cv_.notify_one();
}

// ---- Вытеснение из кэша -----------------------------------------------------------------

bool DownloadScheduler::begin_evict(const std::string& path) {
    const std::lock_guard<std::mutex> lock(mu_);
    if (evicting_paths_.contains(path)) {
        return false;
    }
    // Один путь могут знать несколько file_id (после getMessages с новым file_id): занятость
    // любого из них — отказ, а метку получают все.
    for (const auto& [id, f] : files_) {
        if (f.path == path && (f.users > 0 || f.active || !f.waiters.empty() || f.evicting)) {
            return false;
        }
    }
    for (auto& [id, f] : files_) {
        if (f.path == path) {
            f.evicting = true;
            f.ready.clear();
            clear_ra(f);
        }
    }
    // Файл, которого планировщик не знает, может попасть к читателю с этим же путём до конца
    // вытеснения: его пометит set_path.
    evicting_paths_.insert(path);
    return true;
}

void DownloadScheduler::end_evict(const std::string& path, bool deleted) {
    const std::lock_guard<std::mutex> lock(mu_);
    evicting_paths_.erase(path);
    for (auto& [id, f] : files_) {
        if (f.path != path) {
            continue;
        }
        f.evicting = false;
        if (deleted) {
            f.ready.clear();
            f.path.clear();
        }
    }
    cv_.notify_all();
    worker_cv_.notify_one();
}

// ---- Планирование -----------------------------------------------------------------------

std::vector<std::pair<int64_t, int64_t>> DownloadScheduler::remaining(const FileState& f) {
    std::vector<std::pair<int64_t, int64_t>> out;
    for (const auto& [a, b] : f.ra.intervals()) {
        for (const auto& gap : f.ready.missing(a, b)) {
            out.push_back(gap);
        }
    }
    return out;
}

void DownloadScheduler::set_active(FileState& f, int64_t off, int64_t lim, int prio, bool user,
                                   Clock::time_point now) {
    Active a;
    a.off = off;
    a.lim = lim;
    a.prio = prio;
    a.user = user;
    a.last_progress = now;
    a.progress_mark = f.ready.total();
    f.active = a;
    f.inactive_seen = {};
}

void DownloadScheduler::fail_waiters(FileState& f, const std::optional<TdError>& td_error) {
    for (Wait* w : f.waiters) {
        if (!unsatisfied(f, *w)) {
            continue;
        }
        if (td_error) {
            w->td_error = *td_error;
        } else {
            w->canceled = true;
        }
    }
    cv_.notify_all();
}

DownloadScheduler::Plan DownloadScheduler::plan(Clock::time_point now,
                                                Clock::time_point& next_wake) {
    Plan out;
    next_wake = now + std::chrono::seconds(1);
    const auto soon = [&](Clock::time_point t) { next_wake = std::min(next_wake, t); };
    const Clock::time_point pause = hooks_.pause_until ? hooks_.pause_until() : Clock::time_point{};
    const bool paused = pause > now;
    if (paused) {
        soon(pause);
    }
    const size_t cap = std::max<uint32_t>(opts_.download_concurrency, 1);
    const auto part = static_cast<int64_t>(opts_.part_tolerance);

    // 0. Уборка: забытые пути, просроченный readahead, давно простаивающие файлы.
    for (auto it = files_.begin(); it != files_.end();) {
        FileState& f = it->second;
        for (std::string& p : f.stale_paths) {
            out.stale.push_back(std::move(p));
        }
        f.stale_paths.clear();
        if (!f.ra_reqs.empty()) {
            // Срок отсчитывается от каждого запроса readahead, а не от последней подсказки файла.
            const size_t before = f.ra_reqs.size();
            std::erase_if(f.ra_reqs,
                          [&](const RaRequest& r) { return now - r.stamp > opts_.readahead_ttl; });
            if (f.ra_reqs.size() != before) {
                rebuild_ra(f);
            }
            for (const RaRequest& r : f.ra_reqs) {
                soon(r.stamp + opts_.readahead_ttl);
            }
        }
        const bool idle =
            f.users == 0 && f.waiters.empty() && !f.active && f.ra.empty() && !f.evicting;
        if (idle && now - f.last_use > opts_.idle_forget) {
            it = files_.erase(it);
            continue;
        }
        if (idle) {
            soon(f.last_use + opts_.idle_forget);
        }
        ++it;
    }

    // 1. Завершение окна, обрыв, остановка без прогресса.
    for (auto& [id, f] : files_) {
        if (!f.active) {
            continue;
        }
        const int64_t end = f.end_of_active();
        if (f.ready.covers(f.active->off, end)) {
            out.touches.emplace_back(f.path, f.remote_id);
            f.active.reset();
            f.stalls = 0;
            continue;
        }
        // Обрыв: TDLib сообщил «не качается» после последнего прогресса и с тех пор молчит.
        if (f.active->sent != Clock::time_point{} && f.inactive_seen > f.active->sent &&
            f.inactive_seen > f.active->last_progress) {
            const auto at = f.inactive_seen + opts_.failure_guard;
            if (now >= at) {
                spdlog::debug("TelegramStore: the download of file {} stopped without completing",
                              id);
                fail_waiters(f, std::nullopt);
                f.active.reset();
                clear_ra(f);
                f.inactive_seen = {};
                continue;
            }
            soon(at);
        }
        const auto stall_at = f.active->last_progress + opts_.stall_timeout;
        if (now >= stall_at) {
            if (++f.stalls > opts_.max_stalls) {
                spdlog::debug("download trace: file {} stalled {} times, giving up", id, f.stalls);
                fail_waiters(f, std::nullopt);
                f.active.reset();
                clear_ra(f);
                f.stalls = 0;
            } else if (!paused) {
                spdlog::debug(
                    "download trace: file {} made no progress for {} ms, repeating", id,
                    std::chrono::duration_cast<std::chrono::milliseconds>(opts_.stall_timeout)
                        .count());
                out.actions.push_back({id, f.active->off, f.active->lim, f.active->prio});
                f.active->last_progress = now;
                f.active->sent = {};
                f.inactive_seen = {};
                ++stats_.stalls;
                ++stats_.calls;
            }
            continue;
        }
        soon(stall_at);
    }

    // 2. Ранги: у файла один ранг по его самому старому ждущему запросу (пользовательское чтение
    // от 32 вниз, но не ниже 17; голова и блобы индекса — 32).
    struct Need {
        FileState* f = nullptr;
        uint64_t seq = 0;
        bool blob = false;
        int prio = 0;
    };
    std::vector<Need> needs;
    for (auto& [id, f] : files_) {
        if (f.evicting) {
            continue;  // читатель ждёт конца вытеснения
        }
        const Wait* oldest = nullptr;
        for (const Wait* w : f.waiters) {
            if (unsatisfied(f, *w) && (oldest == nullptr || w->seq < oldest->seq)) {
                oldest = w;
            }
        }
        if (oldest != nullptr) {
            needs.push_back({&f, oldest->seq, oldest->kind == ReadKind::blob, 0});
        }
    }
    std::sort(needs.begin(), needs.end(),
              [](const Need& x, const Need& y) { return x.seq < y.seq; });
    size_t rank = 0;
    for (Need& n : needs) {
        n.prio = n.blob ? kBlobPrio : user_prio(rank++);
    }

    size_t total = 0;
    for (const auto& [id, f] : files_) {
        total += f.active ? 1 : 0;
    }

    // 3. Пользовательские запросы по возрасту.
    std::vector<const FileState*> served;
    for (const Need& need : needs) {
        FileState& f = *need.f;
        served.push_back(&f);
        bool serves = false;
        const Wait* uncovered = nullptr;
        std::vector<const Wait*> sorted(f.waiters.begin(), f.waiters.end());
        std::sort(sorted.begin(), sorted.end(),
                  [](const Wait* x, const Wait* y) { return x->seq < y->seq; });
        for (const Wait* w : sorted) {
            if (!unsatisfied(f, *w)) {
                continue;
            }
            const bool covered = window_serves(f, *w);
            if (covered) {
                serves = true;
            } else if (uncovered == nullptr) {
                uncovered = w;
            }
        }
        if (serves) {
            // Окно обслуживает ждущего: приоритет по возрасту. Остальные ждут очереди файла.
            if ((!f.active->user || f.active->prio != need.prio) && !paused) {
                out.actions.push_back({f.id, f.active->off, f.active->lim, need.prio});
                f.active->prio = need.prio;
                f.active->user = true;
                ++stats_.reprioritized;
                ++stats_.calls;
            }
            continue;
        }
        if (uncovered == nullptr) {
            continue;  // готово, ждём путь
        }
        if (paused) {
            continue;  // проснёмся по концу паузы (soon(pause) выше), retry_at тут не нужен
        }
        if (now < f.retry_at) {
            soon(f.retry_at);
            continue;
        }
        if (!f.active && total >= cap) {
            continue;  // слот освободится при завершении другого окна
        }
        const auto gaps = f.ready.missing(uncovered->a, uncovered->b);
        if (gaps.empty()) {
            continue;
        }
        const int64_t off = gaps.front().first;
        int64_t end = uncovered->b;
        if (f.size > 0) {
            end = std::min(end, f.size);
        }
        // Вытесняемое окно: его непрочитанный остаток за запросом захватывается этим же вызовом.
        const bool displaced = f.active.has_value();
        if (displaced) {
            const int64_t r_begin = f.ready.end_of(f.active->off);
            const int64_t r_end = f.end_of_active();
            if (r_begin < r_end && r_end > end && r_begin <= end + part) {
                end = r_end;
            }
        }
        // Смежный readahead, который ещё не стартовал, тоже входит в окно.
        for (const auto& [a, b] : remaining(f)) {
            if (a <= end + part && b > end && b > off) {
                end = b;
            }
        }
        if (f.size > 0) {
            end = std::min(end, f.size);
        }
        // Окно уже именно такое: повторять тот же downloadFile незачем (его повтор по остановке без
        // прогресса делает шаг 1).
        if (f.active && f.active->off == off && f.active->lim == end - off) {
            if (!f.active->user || f.active->prio != need.prio) {
                out.actions.push_back({f.id, off, end - off, need.prio});
                f.active->prio = need.prio;
                f.active->user = true;
                ++stats_.reprioritized;
                ++stats_.calls;
            }
            continue;
        }
        out.actions.push_back({f.id, off, end - off, need.prio});
        set_active(f, off, end - off, need.prio, /*user=*/true, now);
        ++stats_.starts;
        ++stats_.calls;
        if (displaced) {
            ++stats_.displaced;
        } else {
            ++total;
        }
    }

    // 4. Файлы без ждущих: понижение окна, продление, старт readahead.
    size_t ra_active = 0;
    for (const auto& [id, f] : files_) {
        if (!f.active) {
            continue;
        }
        bool user_served = false;
        for (const Wait* w : f.waiters) {
            if (unsatisfied(f, *w) && window_serves(f, *w)) {
                user_served = true;
            }
        }
        ra_active += user_served ? 0 : 1;
    }
    struct Start {
        uint32_t distance = 0;
        FileState* f = nullptr;
    };
    std::vector<Start> starts;
    for (auto& [id, f] : files_) {
        if (std::find(served.begin(), served.end(), &f) != served.end() || f.evicting) {
            continue;
        }
        if (!f.active) {
            if (!f.ra.empty() && !remaining(f).empty()) {
                starts.push_back({f.ra_distance, &f});
            }
            continue;
        }
        const bool wanted = !remaining(f).empty();
        const int want_prio = wanted ? ra_prio(f.ra_distance) : 1;
        if (f.active->user) {
            const auto demote_at = f.last_demand + opts_.demote_delay;
            if (now >= demote_at) {
                if (!paused) {
                    if (f.active->prio != want_prio) {
                        out.actions.push_back({id, f.active->off, f.active->lim, want_prio});
                        ++stats_.reprioritized;
                        ++stats_.calls;
                    }
                    f.active->prio = want_prio;
                    f.active->user = false;
                }
            } else {
                soon(demote_at);
            }
        } else if (f.active->prio != want_prio && !paused) {
            out.actions.push_back({id, f.active->off, f.active->lim, want_prio});
            f.active->prio = want_prio;
            ++stats_.reprioritized;
            ++stats_.calls;
        }
        // Продление окна: кусок readahead, который начинается не дальше конца окна (тот же offset,
        // больший limit). Допуска на часть здесь нет: несмежный кусок ждёт очереди файла.
        if (!paused && now >= f.retry_at && f.active->lim != 0) {
            const int64_t end = f.end_of_active();
            for (const auto& [a, b0] : remaining(f)) {
                const int64_t b = f.size > 0 ? std::min(b0, f.size) : b0;
                if (a >= f.active->off && a <= end && b > end) {
                    const int64_t lim = b - f.active->off;
                    out.actions.push_back({id, f.active->off, lim, f.active->prio});
                    f.active->lim = lim;
                    ++stats_.extended;
                    ++stats_.calls;
                    break;
                }
            }
        }
    }
    std::sort(starts.begin(), starts.end(),
              [](const Start& x, const Start& y) { return x.distance < y.distance; });
    const size_t ra_cap = std::max<size_t>(1, cap / 2);
    for (const Start& s : starts) {
        if (paused || total >= cap || ra_active >= ra_cap) {
            break;
        }
        FileState& f = *s.f;
        if (now < f.retry_at) {
            soon(f.retry_at);
            continue;
        }
        const auto rem = remaining(f);
        if (rem.empty()) {
            continue;
        }
        const auto [a, b] = rem.front();
        const int prio = ra_prio(f.ra_distance);
        out.actions.push_back({f.id, a, b - a, prio});
        set_active(f, a, b - a, prio, /*user=*/false, now);
        ++stats_.starts;
        ++stats_.calls;
        ++total;
        ++ra_active;
    }

    size_t active_now = 0;
    for (const auto& [id, f] : files_) {
        active_now += f.active ? 1 : 0;
    }
    stats_.active_now = active_now;
    stats_.max_active = std::max<uint64_t>(stats_.max_active, active_now);
    ++plans_;
    return out;
}

// ---- Рабочий поток ----------------------------------------------------------------------

void DownloadScheduler::loop() {
    std::unique_lock<std::mutex> lock(mu_);
    while (!stop_) {
        Clock::time_point next;
        const Plan p = plan(Clock::now(), next);
        if (p.actions.empty() && p.stale.empty() && p.touches.empty()) {
            worker_cv_.wait_until(lock, next);
            continue;
        }
        lock.unlock();
        if (hooks_.on_forget) {
            for (const std::string& path : p.stale) {
                hooks_.on_forget(path);
            }
        }
        if (hooks_.on_touch) {
            for (const auto& [path, remote] : p.touches) {
                if (!path.empty()) {
                    hooks_.on_touch(path, remote);
                }
            }
        }
        for (const Action& a : p.actions) {
            issue(a);
        }
        lock.lock();
    }
}

void DownloadScheduler::issue(const Action& action) {
    spdlog::debug("download trace: downloadFile file {} offset {} limit {} priority {}",
                  action.file_id, action.off, action.lim, action.prio);
    {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = files_.find(action.file_id);
        if (it != files_.end()) {
            std::optional<Active>& active = it->second.active;
            if (active.has_value() && active->off == action.off &&
                active->sent == Clock::time_point{}) {
                active->sent = Clock::now();
            }
        }
    }
    try {
        const Json reply = client_.call(Json{{"@type", "downloadFile"},
                                             {"file_id", action.file_id},
                                             {"priority", action.prio},
                                             {"offset", action.off},
                                             {"limit", action.lim},
                                             {"synchronous", false}},
                                        opts_.call_timeout);
        const std::lock_guard<std::mutex> lock(mu_);
        spdlog::debug("download trace: downloadFile of file {} answered", action.file_id);
        apply_file(reply, /*from_reply=*/true);
    } catch (const TdException& e) {
        const TdError& error = e.error();
        if (error.is_rate_limit() && hooks_.on_flood) {
            hooks_.on_flood(error);
        }
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = files_.find(action.file_id);
        if (it == files_.end()) {
            return;
        }
        FileState& f = it->second;
        if (f.active && f.active->off == action.off) {
            f.active.reset();
        }
        if (error.is_rate_limit()) {
            // Пауза общая: окно будет запрошено заново после неё.
            f.retry_at = Clock::now() + error.retry_after.value_or(std::chrono::seconds{1});
            worker_cv_.notify_one();
            return;
        }
        f.retry_at = Clock::now() + std::chrono::milliseconds{200};
        clear_ra(f);
        fail_waiters(f, error);
    }
}

}  // namespace telega::store
