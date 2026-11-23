#include "store/telegram_store.h"

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cinttypes>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <exception>
#include <filesystem>
#include <mutex>
#include <random>
#include <semaphore>
#include <set>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/files.h"
#include "store/download_scheduler.h"
#include "store/read_cache.h"
#include "td/send_tracker.h"
#include "td/td_client.h"
#include "td/td_error.h"

namespace telega::store {

namespace {

namespace fs = std::filesystem;
using std::chrono::milliseconds;
using std::chrono::seconds;
using td::Json;
using td::TdError;
using td::TdException;

using Clock = std::chrono::steady_clock;

constexpr size_t kAlbumMax = 10;
constexpr size_t kDeleteBatch = 100;
constexpr size_t kInfoCacheMax = 65'536;
constexpr size_t kKnownIdsMax = 1U << 20;
constexpr seconds kDefaultRetryAfter{60};  // 429 без retry_after
// Всего попыток скачивания одного диапазона: повторы после отмены и исчезнувшего локального файла
// (SPEC 10.2).
constexpr int kMaxDownloadRounds = 5;
constexpr uint64_t kSegmentSize = 65'536;  // SPEC 5
constexpr uint64_t kChunkHeaderSize = 40;
constexpr uint64_t kSegmentTag = 16;

// ---- Ошибки ------------------------------------------------------------------------------

// Ошибки TDLib про файлы содержат полный локальный путь (в нём имя пользователя ОС): пути в
// кавычках заменяются на <path>, чтобы они не попали ни в лог, ни в исключение (SPEC 11).
std::string scrub(std::string text) {
    size_t pos = 0;
    while ((pos = text.find('"', pos)) != std::string::npos) {
        const size_t end = text.find('"', pos + 1);
        if (end == std::string::npos) {
            break;
        }
        if (text.find('/', pos) < end) {
            text.replace(pos + 1, end - pos - 1, "<path>");
            pos += std::string_view{"\"<path>\""}.size();
        } else {
            pos = end + 1;
        }
    }
    return text;
}

bool contains_ci(std::string_view haystack, std::string_view needle) {
    const auto* const it = std::search(haystack.begin(), haystack.end(), needle.begin(),
                                       needle.end(), [](char a, char b) {
                                           return std::tolower(static_cast<unsigned char>(a)) ==
                                                  std::tolower(static_cast<unsigned char>(b));
                                       });
    return it != haystack.end();
}

// Отмена скачивания: TDLib отменяет downloadFile, если пришёл новый запрос на тот же файл (код 200)
// или если первые запросы сеанса не прошли (код 400); повтор проходит (SPEC 10.2, E2E, E7).
bool is_download_canceled(const TdError& e) {
    return contains_ci(e.message, "File download has failed or was canceled") ||
           contains_ci(e.message, "Canceled by another downloadFile request");
}

StoreErrorKind kind_of(const TdError& e) {
    if (e.is_rate_limit() || e.is_timeout() || is_download_canceled(e)) {
        return StoreErrorKind::transient;
    }
    if (e.code >= 500) {
        return StoreErrorKind::transient;
    }
    // Закрытый клиент и остановленный рантайм не оживут сами, неожиданный ответ — тоже.
    return StoreErrorKind::permanent;
}

StoreError store_error(const TdError& e, std::string_view what) {
    return {kind_of(e), "TelegramStore: " + std::string{what} + ": " + scrub(e.to_string())};
}

StoreError store_error(const TdException& e, std::string_view what) {
    return store_error(e.error(), what);
}

[[noreturn]] void throw_store(StoreErrorKind kind, const std::string& what) {
    throw StoreError(kind, "TelegramStore: " + what);
}

// Повторяемые сбои скачивания (DownloadCanceled, LocalFileGone) объявлены в download_scheduler.h:
// чтение повторяет их с обновлением file_id (SPEC 8.2, 10.2).
//
// Повтор не поможет, хотя ошибка не permanent по виду TDLib (проверка диапазона до скачивания).
class NoRetryError : public StoreError {
  public:
    using StoreError::StoreError;
};

std::chrono::seconds retry_after_of(const TdError& e) {
    return e.retry_after.value_or(kDefaultRetryAfter);
}

// ---- JSON --------------------------------------------------------------------------------

int64_t get_int(const Json& j, const char* key, int64_t fallback = 0) {
    if (j.is_object()) {
        const auto it = j.find(key);
        if (it != j.end() && it->is_number_integer()) {
            return it->get<int64_t>();
        }
    }
    return fallback;
}

std::string get_string(const Json& j, const char* key) {
    if (j.is_object()) {
        const auto it = j.find(key);
        if (it != j.end() && it->is_string()) {
            return it->get<std::string>();
        }
    }
    return {};
}

const Json& get_obj(const Json& j, const char* key) {
    static const Json kNull;
    if (j.is_object()) {
        const auto it = j.find(key);
        if (it != j.end()) {
            return *it;
        }
    }
    return kNull;
}

struct FileInfo {
    int32_t id = 0;
    uint64_t size = 0;  // 0 — неизвестен
    std::string remote_id;
};

FileInfo parse_file(const Json& file) {
    FileInfo info;
    info.id = static_cast<int32_t>(get_int(file, "id"));
    int64_t size = get_int(file, "size");
    if (size <= 0) {
        size = get_int(file, "expected_size");
    }
    info.size = size > 0 ? static_cast<uint64_t>(size) : 0;
    info.remote_id = get_string(get_obj(file, "remote"), "id");
    return info;
}

// Файл документа из сообщения; nullopt, если сообщение не документ.
std::optional<FileInfo> document_file(const Json& message) {
    const Json& content = get_obj(message, "content");
    if (get_string(content, "@type") != "messageDocument") {
        return std::nullopt;
    }
    const Json& file = get_obj(get_obj(content, "document"), "document");
    if (!file.is_object()) {
        return std::nullopt;
    }
    FileInfo info = parse_file(file);
    if (info.id == 0) {
        return std::nullopt;
    }
    return info;
}

Json input_document(const fs::path& file) {
    return Json{{"@type", "inputMessageDocument"},
                {"document",
                 {{"@type", "inputDocument"},
                  {"document", {{"@type", "inputFileLocal"}, {"path", file.string()}}},
                  {"disable_content_type_detection", true}}}};
}

Json send_options() {
    return Json{{"@type", "messageSendOptions"}, {"disable_notification", true}};
}

// ---- Файлы -------------------------------------------------------------------------------

// Причина, по которой файл нельзя отправлять; nullopt — пригоден (SPEC 10.2). Пути в тексте нет.
std::optional<std::string> file_problem(const fs::path& path, std::optional<uint64_t> expected) {
    std::error_code ec;
    const auto status = fs::status(path, ec);
    if (ec || !fs::exists(status)) {
        return "file does not exist";
    }
    if (!fs::is_regular_file(status)) {
        return "not a regular file";
    }
    const auto size = fs::file_size(path, ec);
    if (ec) {
        return "cannot read the file size";
    }
    if (size == 0) {
        return "file is empty";
    }
    if (::access(path.c_str(), R_OK) != 0) {
        return "file is not readable";
    }
    if (expected && size != *expected) {
        return "file size changed";
    }
    return std::nullopt;
}

std::string random_hex16() {
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::array<char, 17> buf{};
    std::snprintf(buf.data(), buf.size(), "%016" PRIx64, static_cast<uint64_t>(rng()));
    return std::string{buf.data(), 16};
}

// Документ в канале получает имя файла, по которому его отправили (SPEC 4: `<16 hex>.bin`), а
// файл чанка может называться как угодно и выдавать, например, номер чанка. Поэтому рядом с ним
// создаётся жёсткая ссылка со случайным именем (копия, если ссылки не поддерживаются). Симлинк не
// годится: TDLib разворачивает путь через realpath.
fs::path make_named_link(const fs::path& source, const char* extension) {
    fs::path dir = source.parent_path();
    if (dir.empty()) {
        dir = ".";
    }
    std::error_code ec;
    for (int attempt = 0; attempt < 8; ++attempt) {
        const fs::path link = dir / (random_hex16() + extension);
        fs::create_hard_link(source, link, ec);
        if (!ec) {
            return link;
        }
        if (ec != std::errc::file_exists) {
            if (fs::copy_file(source, link, fs::copy_options::none, ec)) {
                return link;
            }
            break;
        }
    }
    throw_store(StoreErrorKind::permanent,
                "cannot create a document with a random name next to the file: " + ec.message());
}

// ---- Семафор с RAII ---------------------------------------------------------------------

class SlotGuard {
  public:
    explicit SlotGuard(std::counting_semaphore<>& sem) : sem_(&sem) { sem_->acquire(); }
    SlotGuard(const SlotGuard&) = delete;
    SlotGuard& operator=(const SlotGuard&) = delete;
    ~SlotGuard() {
        if (sem_ != nullptr) {
            sem_->release();
        }
    }

  private:
    std::counting_semaphore<>* sem_;
};

// Документ, который put_chunk/put_index отдал на отправку.
struct UploadJob {
    fs::path link;  // файл со случайным именем, который уходит в TDLib
    uint64_t size = 0;
    Clock::time_point queued_at;
    bool done = false;
    std::optional<ChunkRef> ref;
    std::optional<StoreError> error;
};

struct DocOutcome {
    UploadJob* job = nullptr;
    bool ok = false;
    Json message;   // итоговое сообщение, если ok
    TdError error;  // иначе ошибка
};

}  // namespace

// ---- Impl --------------------------------------------------------------------------------

struct TelegramStore::Impl {
    Impl(td::TdClient& c, RateLimiter& l, TelegramStoreOptions o)
        : client(c),
          limiter(l),
          options(std::move(o)),
          tracker(c),
          upload_slots(
              static_cast<std::ptrdiff_t>(std::max<uint32_t>(options.upload_concurrency, 1))) {
        // Порог альбомов считается по размеру зашифрованного файла (SPEC 5).
        const uint64_t t = options.album_max_doc_size;
        album_threshold =
            t + kChunkHeaderSize + kSegmentTag * ((t + kSegmentSize - 1) / kSegmentSize);
        if (!options.tdfiles_dir.empty()) {
            ReadCacheOptions co;
            co.state_db = options.state_db;
            co.dir = options.tdfiles_dir;
            co.limit = options.cache_limit;
            cache = std::make_unique<ReadCache>(std::move(co));
        }
        SchedulerOptions so;
        so.download_concurrency = std::max<uint32_t>(options.download_concurrency, 1);
        so.call_timeout = options.call_timeout;
        so.stall_timeout = options.download_stall;
        so.failure_guard = options.download_failure_guard;
        SchedulerHooks hooks;
        hooks.pause_until = [this] {
            const std::lock_guard<std::mutex> lock(read_pause_mu);
            return read_pause_until;
        };
        hooks.on_flood = [this](const TdError& e) { start_read_pause(e); };
        hooks.on_touch = [this](const std::string& path, const std::string& remote) {
            touch_cache(path, remote);
        };
        hooks.on_forget = [this](const std::string& path) {
            if (cache) {
                cache->forget(path);
            }
        };
        sched = std::make_unique<DownloadScheduler>(client, so, std::move(hooks));
        pf_thread = std::thread([this] { prefetch_loop(); });
    }

    ~Impl() {
        {
            const std::lock_guard<std::mutex> lock(pf_mu);
            pf_stop = true;
        }
        pf_cv.notify_all();
        {
            const std::lock_guard<std::mutex> lock(maint_mu);
            maint_stop = true;
        }
        maint_cv.notify_all();
        if (pf_thread.joinable()) {
            pf_thread.join();
        }
        if (maint_thread.joinable()) {
            maint_thread.join();
        }
        sched.reset();  // его потоки зовут хуки, которые обращаются к остальным членам
        if (cache) {
            cache->flush();
        }
    }
    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    td::TdClient& client;
    RateLimiter& limiter;
    const TelegramStoreOptions options;
    td::SendTracker tracker;
    std::counting_semaphore<> upload_slots;
    uint64_t album_threshold = 0;

    // Скачивания идут через планировщик (SPEC 8.2): TDLib отменяет прежний downloadFile файла,
    // если пришёл новый запрос на тот же файл с другими offset и limit, поэтому по файлу активно
    // одно окно, а число одновременных окон ограничивает download_concurrency. Объявлен после
    // cache и пауз: его потоки зовут хуки, которые к ним обращаются (см. ~Impl).
    std::unique_ptr<ReadCache> cache;
    std::unique_ptr<DownloadScheduler> sched;

    // Очередь сбора альбомов.
    std::mutex q_mu;
    std::condition_variable q_cv;
    std::deque<UploadJob*> queue;
    bool gathering = false;

    // Общая пауза чтения после FLOOD_WAIT на getMessages и скачиваниях.
    std::mutex read_pause_mu;
    Clock::time_point read_pause_until;

    // getChat после старта (SPEC 2).
    std::mutex chat_mu;
    bool chat_known = false;

    // Кэши процесса.
    struct CachedFile {
        int32_t file_id = 0;  // 0 — известен только размер
        uint64_t size = 0;
    };
    std::mutex cache_mu;
    std::unordered_map<int64_t, CachedFile> info_cache;
    std::unordered_set<int64_t> known_ids;  // сообщения, которые TDLib знает в этом процессе

    // Подсказки readahead (SPEC 8.2): prefetch() только ставит их в очередь, фоновый поток
    // получает file_id (getRemoteFile или getMessages) и отдаёт диапазон планировщику.
    struct PrefetchJob {
        ChunkRef ref;
        uint64_t offset = 0;
        uint64_t len = 0;
        uint32_t distance = 0;
        Clock::time_point queued;
    };
    std::mutex pf_mu;
    std::condition_variable pf_cv;
    std::deque<PrefetchJob> pf_queue;
    bool pf_stop = false;
    bool pf_busy = false;  // фоновый поток обрабатывает подсказку
    std::condition_variable pf_idle_cv;
    std::thread pf_thread;

    // Кэш чтения: фоновый поток сбрасывает учёт раз в cache_flush и вытесняет по запросу.
    std::mutex maint_mu;
    std::condition_variable maint_cv;
    bool maint_stop = false;
    bool maint_wanted = false;  // сумма выше лимита
    bool maint_started = false;
    std::thread maint_thread;

    // ---- Вспомогательное ----

    [[nodiscard]] static Clock::time_point deadline_in(milliseconds d) { return Clock::now() + d; }

    [[nodiscard]] milliseconds call_timeout_until(Clock::time_point deadline) const {
        const auto left = std::chrono::duration_cast<milliseconds>(deadline - Clock::now());
        return std::min(options.call_timeout, left);
    }

    void note_known(int64_t id) {
        const std::lock_guard<std::mutex> lock(cache_mu);
        if (known_ids.size() >= kKnownIdsMax) {
            known_ids.clear();  // только оптимизация: лишний getMessages перед удалением
        }
        known_ids.insert(id);
    }
    void forget_known(const std::vector<int64_t>& ids) {
        const std::lock_guard<std::mutex> lock(cache_mu);
        for (const int64_t id : ids) {
            known_ids.erase(id);
            info_cache.erase(id);
        }
    }
    bool is_known(int64_t id) {
        const std::lock_guard<std::mutex> lock(cache_mu);
        return known_ids.contains(id);
    }
    void cache_put(int64_t msg_id, int32_t file_id, uint64_t size) {
        const std::lock_guard<std::mutex> lock(cache_mu);
        if (info_cache.size() >= kInfoCacheMax) {
            info_cache.clear();
        }
        info_cache[msg_id] = CachedFile{file_id, size};
    }
    std::optional<CachedFile> cache_get(int64_t msg_id) {
        const std::lock_guard<std::mutex> lock(cache_mu);
        const auto it = info_cache.find(msg_id);
        if (it == info_cache.end()) {
            return std::nullopt;
        }
        return it->second;
    }
    void cache_drop(int64_t msg_id) {
        const std::lock_guard<std::mutex> lock(cache_mu);
        info_cache.erase(msg_id);
    }

    // ---- Чтение: запросы с паузой FLOOD_WAIT ----

    void wait_read_pause(Clock::time_point deadline) {
        Clock::time_point until;
        {
            const std::lock_guard<std::mutex> lock(read_pause_mu);
            until = read_pause_until;
        }
        if (until <= Clock::now()) {
            return;
        }
        if (until > deadline) {
            throw TdException(
                TdError::make(TdError::kTimeout, "FLOOD_WAIT longer than the timeout"));
        }
        std::this_thread::sleep_until(until);
    }

    void start_read_pause(const TdError& e) {
        const auto base = retry_after_of(e);
        // Джиттер до 10 % (SPEC 10.2), чтобы читатели не просыпались одновременно.
        thread_local std::mt19937_64 rng{std::random_device{}()};
        const double jitter = std::uniform_real_distribution<double>(0.0, 0.10)(rng);
        const auto until =
            Clock::now() +
            std::chrono::duration_cast<Clock::duration>(
                std::chrono::duration<double>(static_cast<double>(base.count()) * (1.0 + jitter)));
        spdlog::warn("TelegramStore: FLOOD_WAIT {} s on a read, waiting", base.count());
        const std::lock_guard<std::mutex> lock(read_pause_mu);
        read_pause_until = std::max(read_pause_until, until);
    }

    // Запрос на чтение: FLOOD_WAIT ждётся и повторяется до deadline, остальные ошибки TDLib
    // выходят как TdException.
    Json read_call_td(const Json& request, Clock::time_point deadline) {
        for (;;) {
            wait_read_pause(deadline);
            if (call_timeout_until(deadline) <= milliseconds{0}) {
                throw TdException(TdError::make(TdError::kTimeout, "timed out before the request"));
            }
            try {
                return client.call(request, call_timeout_until(deadline));
            } catch (const TdException& e) {
                if (!e.error().is_rate_limit()) {
                    throw;
                }
                start_read_pause(e.error());
            }
        }
    }

    Json read_call(const Json& request, Clock::time_point deadline, std::string_view what) {
        try {
            return read_call_td(request, deadline);
        } catch (const TdException& e) {
            if (is_download_canceled(e.error())) {
                throw DownloadCanceled(StoreErrorKind::transient, store_error(e, what).what());
            }
            throw store_error(e, what);
        }
    }

    // getChat первым запросом по каналу (SPEC 2). open() делает это сам, остальные методы —
    // лениво, если open() не звали.
    void ensure_chat(Clock::time_point deadline) {
        const std::lock_guard<std::mutex> lock(chat_mu);
        if (chat_known) {
            return;
        }
        read_call(Json{{"@type", "getChat"}, {"chat_id", options.chat_id}}, deadline, "getChat");
        chat_known = true;
    }

    // ---- Действия через лимитер (pin, unpin, удаление) ----

    // FLOOD_WAIT ставит паузу лимитера и повторяется до deadline; остальные ошибки TDLib выходят
    // как TdException.
    Json run_action_td(const Json& request, Clock::time_point deadline) {
        for (;;) {
            if (!limiter.acquire(1, 0, deadline)) {
                throw StoreError(StoreErrorKind::transient,
                                 "TelegramStore: rate limiter did not release the action in time");
            }
            try {
                return client.call(request, options.call_timeout);
            } catch (const TdException& e) {
                if (!e.error().is_rate_limit()) {
                    throw;
                }
                limiter.on_flood_wait(retry_after_of(e.error()));
            }
        }
    }

    // ---- Отправка ----

    // Задания, которым в deliver() уже отдан итог. После finish() вызывающий put_chunk просыпается
    // и уничтожает свой UploadJob, поэтому обращаться к нему по указателю больше нельзя (и в catch
    // тоже): помнить об этом можно только по самому указателю.
    using FinishedSet = std::unordered_set<const UploadJob*>;

    void finish(UploadJob& job, std::optional<ChunkRef> ref, std::optional<StoreError> error,
                FinishedSet& finished) {
        finished.insert(&job);
        {
            const std::lock_guard<std::mutex> lock(q_mu);
            job.ref = std::move(ref);
            job.error = std::move(error);
            job.done = true;
        }
        q_cv.notify_all();
    }

    void finish_ok(UploadJob& job, const Json& message, FinishedSet& finished) {
        const int64_t id = get_int(message, "id");
        if (id == 0) {
            finish(job, std::nullopt,
                   StoreError(StoreErrorKind::permanent,
                              "TelegramStore: send result has no message id"),
                   finished);
            return;
        }
        ChunkRef ref;
        ref.msg_id = id;
        if (const auto file = document_file(message)) {
            ref.remote_file_id = file->remote_id;
        }
        note_known(id);
        cache_put(id, 0, job.size);  // ожидаемый размер файла для проверки getRemoteFile
        finish(job, std::move(ref), std::nullopt, finished);
    }

    void finish_td_error(UploadJob& job, const TdError& error, FinishedSet& finished) {
        finish(job, std::nullopt, store_error(error, "send"), finished);
    }

    // ---- Потерянный итог отправки (SPEC 10.2) ----

    // Отправка, итог которой не пришёл вовремя. Документ мог всё же дойти, и тогда в канале
    // остался бы дубль, о котором не знает upload_journal. temp_id == 0: ответ на sendMessage не
    // пришёл, id неизвестен, документ опознаётся по имени файла со случайным именем.
    struct Abandoned {
        int64_t chat_id = 0;
        int64_t temp_id = 0;
        std::string name;
        Clock::time_point since;
    };
    std::mutex abandoned_mu;
    std::vector<Abandoned> abandoned;

    // Таймаут итога растёт с размером документа: заливка 8 МиБ и 100 КиБ не может ждать одинаково.
    [[nodiscard]] milliseconds send_timeout_for(uint64_t bytes) const {
        const uint64_t rate = std::max<uint64_t>(options.send_min_rate, 1);
        return options.send_timeout + milliseconds{static_cast<int64_t>(bytes * 1000 / rate)};
    }

    // Удаление без исключений: чистка после сбоя не должна подменять собой исходную ошибку.
    void best_effort_delete(const std::vector<int64_t>& ids) {
        if (ids.empty()) {
            return;
        }
        try {
            run_action_td(Json{{"@type", "deleteMessages"},
                               {"chat_id", options.chat_id},
                               {"message_ids", ids},
                               {"revoke", true}},
                          deadline_in(options.call_timeout));
        } catch (const TdException& e) {
            spdlog::warn("TelegramStore: cleanup of abandoned sends failed: {}",
                         scrub(e.error().to_string()));
        } catch (const std::exception& e) {
            spdlog::warn("TelegramStore: cleanup of abandoned sends failed: {}", e.what());
        }
    }

    // Итог отправки не пришёл за send_timeout: временные сообщения удаляются (TDLib отменяет
    // заливку), а поздно пришедший итог ловится при следующей уборке и удаляет настоящее сообщение.
    void abandon_sends(std::vector<Abandoned> sends) {
        std::vector<int64_t> temps;
        for (Abandoned& a : sends) {
            a.since = Clock::now();
            if (a.temp_id != 0) {
                temps.push_back(a.temp_id);
            }
        }
        spdlog::warn("TelegramStore: no send result in time, deleting {} temporary message(s)",
                     temps.size());
        best_effort_delete(temps);
        {
            const std::lock_guard<std::mutex> lock(abandoned_mu);
            abandoned.insert(abandoned.end(), std::make_move_iterator(sends.begin()),
                             std::make_move_iterator(sends.end()));
        }
        sweep_abandoned();
    }

    // Итоги, пришедшие после таймаута: успешный означает настоящее сообщение, которого никто не
    // ждёт, его удаляем. Вызывается перед каждой отправкой и удалением.
    void sweep_abandoned() {
        constexpr auto kTtl = std::chrono::hours{1};
        std::vector<Abandoned> pending;
        {
            const std::lock_guard<std::mutex> lock(abandoned_mu);
            if (abandoned.empty()) {
                return;
            }
            pending.swap(abandoned);
        }
        std::vector<Abandoned> keep;
        std::vector<int64_t> orphans;
        for (Abandoned& a : pending) {
            bool resolved = false;
            try {
                if (a.temp_id != 0) {
                    const Json message = tracker.wait(a.chat_id, a.temp_id, milliseconds{0});
                    orphans.push_back(get_int(message, "id"));
                    resolved = true;
                } else if (const auto message = tracker.take_document(a.name)) {
                    orphans.push_back(get_int(*message, "id"));
                    resolved = true;
                }
            } catch (const TdException& e) {
                // Таймаут: итога всё ещё нет. Остальное (отправка не удалась, клиент закрыт):
                // сообщения нет, убирать нечего.
                resolved = !e.error().is_timeout();
            }
            if (!resolved && Clock::now() - a.since < kTtl) {
                keep.push_back(std::move(a));
            }
        }
        if (!keep.empty()) {
            const std::lock_guard<std::mutex> lock(abandoned_mu);
            abandoned.insert(abandoned.end(), std::make_move_iterator(keep.begin()),
                             std::make_move_iterator(keep.end()));
        }
        std::erase(orphans, 0);
        if (!orphans.empty()) {
            spdlog::warn("TelegramStore: deleting {} message(s) whose send result came late",
                         orphans.size());
            best_effort_delete(orphans);
            forget_known(orphans);
        }
    }

    // Один раунд отправки одного документа.
    DocOutcome attempt_single(UploadJob& job) {
        const std::string name = job.link.filename().string();
        Json sent;
        try {
            sent = client.call(Json{{"@type", "sendMessage"},
                                    {"chat_id", options.chat_id},
                                    {"options", send_options()},
                                    {"input_message_content", input_document(job.link)}},
                               options.call_timeout);
        } catch (const TdException& e) {
            if (e.error().is_timeout()) {
                abandon_sends({Abandoned{options.chat_id, 0, name, {}}});  // ответ потерян
            }
            return DocOutcome{&job, false, {}, e.error()};
        }
        const int64_t chat_id = get_int(sent, "chat_id");
        const int64_t temp_id = get_int(sent, "id");
        if (chat_id == 0 || temp_id == 0) {
            return DocOutcome{
                &job,
                false,
                {},
                TdError::make(TdError::kUnexpected, "sendMessage returned no message")};
        }
        try {
            return DocOutcome{
                &job, true, tracker.wait(chat_id, temp_id, send_timeout_for(job.size)), {}};
        } catch (const TdException& e) {
            if (e.error().is_timeout()) {
                abandon_sends({Abandoned{chat_id, temp_id, name, {}}});
            }
            return DocOutcome{&job, false, {}, e.error()};
        }
    }

    struct AlbumAttempt {
        std::optional<TdError> call_error;  // sendMessageAlbum не принят: ничего не отправлено
        std::vector<DocOutcome> docs;
    };

    // Один раунд отправки альбома: итог каждого документа свой (SPEC 10.2).
    AlbumAttempt attempt_album(const std::vector<UploadJob*>& jobs) {
        AlbumAttempt result;
        Json contents = Json::array();
        uint64_t bytes = 0;
        for (const UploadJob* job : jobs) {
            contents.push_back(input_document(job->link));
            bytes += job->size;
        }
        const auto album_names = [&] {
            std::vector<Abandoned> names;
            names.reserve(jobs.size());
            for (const UploadJob* job : jobs) {
                names.push_back(Abandoned{options.chat_id, 0, job->link.filename().string(), {}});
            }
            return names;
        };
        Json reply;
        try {
            reply = client.call(Json{{"@type", "sendMessageAlbum"},
                                     {"chat_id", options.chat_id},
                                     {"options", send_options()},
                                     {"input_message_contents", std::move(contents)}},
                                options.call_timeout);
        } catch (const TdException& e) {
            if (e.error().is_timeout()) {
                abandon_sends(album_names());  // ответ потерян: документы опознаются по имени
            }
            result.call_error = e.error();
            return result;
        }
        const Json& messages = get_obj(reply, "messages");
        if (!messages.is_array() || messages.size() != jobs.size()) {
            if (messages.is_array() && !messages.empty()) {
                std::vector<Abandoned> sent;
                for (const Json& m : messages) {
                    sent.push_back(Abandoned{get_int(m, "chat_id"), get_int(m, "id"), {}, {}});
                }
                abandon_sends(std::move(sent));
            }
            result.call_error = TdError::make(
                TdError::kUnexpected, "sendMessageAlbum returned a wrong number of messages");
            return result;
        }
        const auto deadline = Clock::now() + send_timeout_for(bytes);
        std::vector<Abandoned> timed_out;
        for (size_t i = 0; i < jobs.size(); ++i) {
            const Json& temp = messages[i];
            const int64_t chat_id = get_int(temp, "chat_id");
            const int64_t temp_id = get_int(temp, "id");
            const auto left = std::max(
                std::chrono::duration_cast<milliseconds>(deadline - Clock::now()), milliseconds{0});
            try {
                result.docs.push_back(
                    DocOutcome{jobs[i], true, tracker.wait(chat_id, temp_id, left), {}});
            } catch (const TdException& e) {
                if (e.error().is_timeout()) {
                    timed_out.push_back(
                        Abandoned{chat_id, temp_id, jobs[i]->link.filename().string(), {}});
                }
                result.docs.push_back(DocOutcome{jobs[i], false, {}, e.error()});
            }
        }
        if (!timed_out.empty()) {
            abandon_sends(std::move(timed_out));
        }
        return result;
    }

    // Доставка задания (SPEC 10.2): проверка файлов, лимитер, отправка альбомом (2+ документа) или
    // по одному, повторы при FLOOD_WAIT. Каждое задание получает итог в finish().
    void deliver(std::vector<UploadJob*> todo) {
        const std::vector<UploadJob*> all = todo;
        FinishedSet finished;
        bool force_single = false;
        try {
            while (!todo.empty()) {
                std::erase_if(todo, [&](UploadJob* job) {
                    const auto problem = file_problem(job->link, job->size);
                    if (!problem) {
                        return false;
                    }
                    finish(*job, std::nullopt,
                           StoreError(StoreErrorKind::permanent,
                                      "TelegramStore: document file is unusable: " + *problem),
                           finished);
                    return true;
                });
                if (todo.empty()) {
                    break;
                }
                const bool album = todo.size() >= 2 && !force_single;
                const std::vector<UploadJob*> batch =
                    album ? todo : std::vector<UploadJob*>{todo.front()};
                uint64_t bytes = 0;
                for (const UploadJob* job : batch) {
                    bytes += job->size;
                }
                if (!limiter.acquire(album ? kAlbumActions : 1.0, bytes)) {
                    for (UploadJob* job : todo) {
                        finish(*job, std::nullopt,
                               StoreError(StoreErrorKind::transient,
                                          "TelegramStore: rate limiter stopped"),
                               finished);
                    }
                    return;
                }

                std::optional<TdError> call_error;
                std::vector<DocOutcome> docs;
                if (album) {
                    AlbumAttempt attempt = attempt_album(batch);
                    call_error = std::move(attempt.call_error);
                    docs = std::move(attempt.docs);
                } else {
                    docs.push_back(attempt_single(*batch.front()));
                }

                std::vector<UploadJob*> retry;
                seconds flood{0};
                bool flooded = false;
                bool any_ok = false;
                const auto note_flood = [&](const TdError& e) {
                    flood = std::max(flood, retry_after_of(e));
                    flooded = true;
                };
                if (call_error.has_value()) {
                    const TdError& rejected = *call_error;
                    if (rejected.is_rate_limit()) {
                        // 429 у альбома относится ко всем документам: повтор целиком.
                        note_flood(rejected);
                        retry = batch;
                    } else if (album && rejected.code == 400) {
                        // Альбом отвергнут целиком, хотя файлы прошли проверку: отправляем по
                        // одному, чтобы каждый документ получил свой вердикт.
                        spdlog::warn(
                            "TelegramStore: sendMessageAlbum rejected ({}), sending one by one",
                            scrub(rejected.to_string()));
                        force_single = true;
                        retry = batch;
                    } else {
                        for (UploadJob* job : batch) {
                            finish_td_error(*job, rejected, finished);
                        }
                    }
                } else {
                    for (const DocOutcome& doc : docs) {
                        if (doc.ok) {
                            finish_ok(*doc.job, doc.message, finished);
                            any_ok = true;
                        } else if (doc.error.is_rate_limit()) {
                            note_flood(doc.error);
                            retry.push_back(doc.job);
                        } else {
                            finish_td_error(*doc.job, doc.error, finished);
                        }
                    }
                }
                if (flooded) {
                    spdlog::warn("TelegramStore: FLOOD_WAIT {} s on send, {} document(s) to retry",
                                 flood.count(), retry.size());
                    limiter.on_flood_wait(flood);
                }
                // Часть альбома дошла: остальные повторяются по одному, иначе будут дубли (E6).
                if (album && any_ok && !retry.empty()) {
                    force_single = true;
                }
                if (album) {
                    todo = std::move(retry);
                } else {
                    todo.erase(todo.begin());
                    todo.insert(todo.begin(), retry.begin(), retry.end());
                }
            }
        } catch (const std::exception& e) {
            // Только незавершённые задания: завершённые вызывающий мог уже уничтожить.
            for (UploadJob* job : all) {
                if (!finished.contains(job)) {
                    finish(*job, std::nullopt,
                           StoreError(
                               StoreErrorKind::permanent,
                               std::string{"TelegramStore: internal error on send: "} + e.what()),
                           finished);
                }
            }
        }
    }

    // Ждёт попутчиков для альбома. Лидером становится задание в голове очереди: оно ждёт до
    // album_gather от своего появления либо набора 10 штук, занимает слот отправки и забирает до
    // 10 заданий.
    void run_queued(UploadJob& job) {
        std::unique_lock<std::mutex> lock(q_mu);
        job.queued_at = Clock::now();
        queue.push_back(&job);
        q_cv.notify_all();
        while (!job.done) {
            if (!gathering && queue.front() == &job) {
                gathering = true;
                q_cv.wait_until(lock, job.queued_at + options.album_gather,
                                [&] { return queue.size() >= kAlbumMax; });
                lock.unlock();
                upload_slots.acquire();
                std::vector<UploadJob*> batch;
                {
                    const std::lock_guard<std::mutex> relock(q_mu);
                    while (!queue.empty() && batch.size() < kAlbumMax) {
                        batch.push_back(queue.front());
                        queue.pop_front();
                    }
                    gathering = false;
                }
                q_cv.notify_all();
                deliver(std::move(batch));
                upload_slots.release();
                lock.lock();
            } else {
                q_cv.wait(lock);
            }
        }
    }

    ChunkRef upload(const fs::path& file, const char* extension, const char* what) {
        const auto problem = file_problem(file, std::nullopt);
        if (problem) {
            throw_store(StoreErrorKind::permanent, std::string{what} + ": " + *problem);
        }
        std::error_code ec;
        UploadJob job;
        job.size = fs::file_size(file, ec);
        if (ec) {
            throw_store(StoreErrorKind::permanent,
                        std::string{what} + ": cannot read the file size");
        }
        ensure_chat(deadline_in(options.read_timeout));
        sweep_abandoned();
        job.link = make_named_link(file, extension);
        struct LinkCleanup {
            const fs::path& link;
            ~LinkCleanup() {
                std::error_code remove_ec;
                fs::remove(link, remove_ec);
            }
        } const cleanup{job.link};
        spdlog::debug("TelegramStore: {} of {} bytes", what, job.size);

        if (job.size <= album_threshold) {
            run_queued(job);
        } else {
            const SlotGuard slot(upload_slots);
            deliver({&job});
        }
        if (job.ref) {
            return std::move(*job.ref);
        }
        throw job.error ? StoreError(*job.error)
                        : StoreError(StoreErrorKind::permanent, "TelegramStore: no send result");
    }

    // ---- Скачивание ----

    // Чтение диапазона через планировщик (SPEC 8.2): уже скачанное читается pread без downloadFile,
    // иначе ждёт окно скачивания. deadline — от постановки запроса. Ошибки TDLib превращаются в
    // StoreError как у read_call; отмена и исчезнувший локальный файл — DownloadCanceled и
    // LocalFileGone (их повторяет retry_download).
    void download_and_read(const FileInfo& file, uint64_t offset, std::span<std::byte> out,
                           ReadKind kind, Clock::time_point deadline) const {
        uint64_t limit = out.size();
        if (file.size > 0) {
            limit = std::min<uint64_t>(limit, file.size - offset);
        }
        try {
            sched->read(FileRef{file.id, file.size, file.remote_id}, offset, out.first(limit), kind,
                        deadline);
        } catch (const TdException& e) {
            if (is_download_canceled(e.error())) {
                throw DownloadCanceled(StoreErrorKind::transient,
                                       store_error(e, "downloadFile").what());
            }
            throw store_error(e, "downloadFile");
        } catch (const DownloadCanceled&) {
            throw;
        } catch (const LocalFileGone&) {
            throw;
        } catch (const StoreError& e) {
            // Дедлайн (read_timeout от входа в read_range) вышел: повтор уже не успеет.
            if (e.kind() == StoreErrorKind::transient && Clock::now() >= deadline) {
                throw NoRetryError(StoreErrorKind::transient, e.what());
            }
            throw;
        }
    }

    // Скачивание с повторами (SPEC 8.2, 10.2). resolve(refresh) даёт файл для попытки: при
    // refresh == true file_id берётся заново из getMessages, а кэш процесса сбрасывается; run(file)
    // качает и читает. Отмена downloadFile (transient) и исчезнувший локальный файл повторяются с
    // обновлением file_id, но всего попыток не больше kMaxDownloadRounds и не дольше deadline:
    // «File download has failed or was canceled» TDLib отдаёт на любой неуспех, а не только на
    // отмену. Прочие ошибки скачивания повторяются один раз. Ошибки resolve не повторяются.
    template <class Resolve, class Run>
    void retry_download(int64_t msg_id, Clock::time_point deadline, Resolve&& resolve, Run&& run) {
        constexpr milliseconds kCancelPause{50};
        constexpr milliseconds kCancelPauseMax{500};
        bool refresh = false;
        bool failed_once = false;
        for (int round = 1;; ++round) {
            const FileInfo file = resolve(refresh);
            try {
                run(file);
                return;
            } catch (const DownloadCanceled& e) {
                const auto pause = std::min(kCancelPause * round, kCancelPauseMax);
                if (round >= kMaxDownloadRounds || Clock::now() + pause >= deadline) {
                    throw;
                }
                spdlog::debug("TelegramStore: download canceled ({}), retrying", e.what());
                std::this_thread::sleep_for(pause);
            } catch (const LocalFileGone& e) {
                if (round >= kMaxDownloadRounds) {
                    throw_store(StoreErrorKind::permanent,
                                "the local copy of the file keeps disappearing");
                }
                spdlog::debug("TelegramStore: {}, downloading again", e.what());
            } catch (const NoRetryError&) {
                throw;
            } catch (const StoreError& e) {
                if (e.kind() == StoreErrorKind::not_found || failed_once ||
                    round >= kMaxDownloadRounds) {
                    throw;
                }
                failed_once = true;
                // После любой ошибки скачивания file_id берётся заново из сообщения (SPEC 8.2).
                spdlog::debug("TelegramStore: download failed ({}), refreshing the file id",
                              e.what());
            }
            cache_drop(msg_id);
            refresh = true;
        }
    }

    FileInfo lookup_message_file(int64_t msg_id, Clock::time_point deadline) {
        const Json reply = read_call(Json{{"@type", "getMessages"},
                                          {"chat_id", options.chat_id},
                                          {"message_ids", Json::array({msg_id})}},
                                     deadline, "getMessages");
        const Json& messages = get_obj(reply, "messages");
        if (!messages.is_array() || messages.empty()) {
            throw_store(StoreErrorKind::permanent, "getMessages returned no result");
        }
        if (messages[0].is_null()) {
            throw StoreError(
                StoreErrorKind::not_found,
                "TelegramStore: message " + std::to_string(msg_id) + " does not exist");
        }
        const auto file = document_file(messages[0]);
        if (!file) {
            throw_store(StoreErrorKind::permanent,
                        "message " + std::to_string(msg_id) + " is not a document");
        }
        note_known(msg_id);
        cache_put(msg_id, file->id, file->size);
        return *file;
    }

    // file_id по SPEC 8.2: кэш процесса, затем getRemoteFile (если размер правдоподобен),
    // иначе getMessages.
    FileInfo resolve_file(const ChunkRef& ref, Clock::time_point deadline, bool force_messages) {
        uint64_t expected = 0;
        if (!force_messages) {
            if (const auto cached = cache_get(ref.msg_id)) {
                if (cached->file_id != 0) {
                    return FileInfo{cached->file_id, cached->size, {}};
                }
                expected = cached->size;
            }
            if (!ref.remote_file_id.empty()) {
                try {
                    const Json reply =
                        read_call(Json{{"@type", "getRemoteFile"},
                                       {"remote_file_id", ref.remote_file_id},
                                       {"file_type", {{"@type", "fileTypeDocument"}}}},
                                  deadline, "getRemoteFile");
                    const FileInfo info = parse_file(reply);
                    // На пустой БД размер 0 и скачивание конца файла падает (E4): годится только
                    // файл с известным размером, равным ожидаемому, если он нам известен.
                    if (info.id != 0 && info.size > 0 && (expected == 0 || info.size == expected)) {
                        cache_put(ref.msg_id, info.id, info.size);
                        return info;
                    }
                } catch (const StoreError& e) {
                    spdlog::debug(
                        "TelegramStore: getRemoteFile failed, falling back to getMessages: {}",
                        e.what());
                }
            }
        }
        return lookup_message_file(ref.msg_id, deadline);
    }

    // ---- Readahead и кэш чтения (SPEC 8.2) ----

    void queue_prefetch(const ChunkRef& ref, uint64_t offset, uint64_t len, uint32_t distance) {
        if (len == 0) {
            return;
        }
        {
            const std::lock_guard<std::mutex> lock(pf_mu);
            if (pf_stop) {
                return;
            }
            for (const PrefetchJob& j : pf_queue) {
                if (j.ref.msg_id == ref.msg_id && j.offset == offset && j.len == len) {
                    return;  // та же подсказка уже ждёт
                }
            }
            while (pf_queue.size() >= std::max<size_t>(options.prefetch_queue_max, 1)) {
                pf_queue.pop_front();  // старые подсказки устаревают первыми
            }
            pf_queue.push_back({ref, offset, len, distance, Clock::now()});
        }
        pf_cv.notify_one();
    }

    void prefetch_loop() {
        std::unique_lock<std::mutex> lock(pf_mu);
        while (!pf_stop) {
            if (pf_queue.empty()) {
                pf_cv.wait(lock);
                continue;
            }
            const PrefetchJob job = std::move(pf_queue.front());
            pf_queue.pop_front();
            if (Clock::now() - job.queued > options.prefetch_ttl) {
                continue;
            }
            pf_busy = true;
            lock.unlock();
            try {
                run_prefetch(job);
            } catch (const std::exception& e) {
                spdlog::debug("TelegramStore: prefetch skipped: {}", e.what());
            }
            lock.lock();
            pf_busy = false;
            pf_idle_cv.notify_all();
        }
    }

    void run_prefetch(const PrefetchJob& job) {
        const auto deadline = deadline_in(options.call_timeout);
        ensure_chat(deadline);
        const FileInfo file = resolve_file(job.ref, deadline, false);
        uint64_t len = job.len;
        if (file.size > 0) {
            if (job.offset >= file.size) {
                return;
            }
            len = std::min<uint64_t>(len, file.size - job.offset);
        }
        sched->prefetch(FileRef{file.id, file.size, file.remote_id}, job.offset, len, job.distance);
    }

    void touch_cache(const std::string& path, const std::string& remote_id) {
        if (!cache) {
            return;
        }
        cache->note(path, remote_id);
        if (cache->over_limit()) {
            {
                const std::lock_guard<std::mutex> lock(maint_mu);
                maint_wanted = true;
            }
            maint_cv.notify_one();
        }
    }

    // Вытеснение через TDLib блокирует его файловый актор на время unlink (FileLoadManager::
    // unlink_file делает unlink синхронно): все скачивания стоят, пока удаляется файл. Медленные
    // вызовы пишутся в лог, чтобы такую остановку можно было опознать.
    static void log_slow_eviction_call(Clock::time_point t0, Clock::time_point t1,
                                       Clock::time_point t2) {
        const auto ms = [](Clock::time_point a, Clock::time_point b) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count();
        };
        const auto total = ms(t0, t2);
        if (total >= 1000) {
            spdlog::warn(
                "TelegramStore: cache eviction through TDLib took {} ms (getRemoteFile {} ms, "
                "deleteFile {} ms): TDLib downloads stand still meanwhile",
                total, ms(t0, t1), ms(t1, t2));
        } else {
            spdlog::debug("TelegramStore: cache eviction call took {} ms (getRemoteFile {} ms)",
                          total, ms(t0, t1));
        }
    }

    // Удаление файла кэша: getRemoteFile и deleteFile, а если TDLib файл не знает или его
    // local.path другой — прямо с диска (SPEC 8.2).
    bool delete_cached(const ReadCacheEntry& e) {
        bool via_tdlib = false;
        if (!e.remote_file_id.empty()) {
            try {
                const auto t0 = Clock::now();
                const Json reply = client.call(Json{{"@type", "getRemoteFile"},
                                                    {"remote_file_id", e.remote_file_id},
                                                    {"file_type", {{"@type", "fileTypeDocument"}}}},
                                               options.call_timeout);
                const auto t1 = Clock::now();
                const FileInfo info = parse_file(reply);
                if (info.id != 0 && get_string(get_obj(reply, "local"), "path") == e.path) {
                    client.call(Json{{"@type", "deleteFile"}, {"file_id", info.id}},
                                options.call_timeout);
                    via_tdlib = true;
                    log_slow_eviction_call(t0, t1, Clock::now());
                }
            } catch (const TdException& ex) {
                spdlog::debug("TelegramStore: cache eviction through TDLib failed: {}",
                              scrub(ex.error().to_string()));
            }
        }
        std::error_code ec;
        const bool exists = fs::exists(e.path, ec);
        if (exists || !via_tdlib) {
            fs::remove(e.path, ec);
            if (ec && fs::exists(e.path)) {
                spdlog::warn("TelegramStore: cannot delete a cache file: {}", ec.message());
                return false;
            }
        }
        return true;
    }

    void run_eviction() {
        if (!cache || !cache->over_limit()) {
            return;
        }
        // Пометки вытеснения снимаются на любом выходе, в том числе по исключению (не только
        // TdException): иначе файл навсегда остался бы evicting и читатели ждали бы до таймаута.
        struct Claims {
            DownloadScheduler& sched;
            std::vector<std::string> open;
            explicit Claims(DownloadScheduler& s) : sched(s) {}
            Claims(const Claims&) = delete;
            Claims& operator=(const Claims&) = delete;
            ~Claims() {
                for (const std::string& path : open) {
                    sched.end_evict(path, false);
                }
            }
            void release(const std::string& path, bool deleted) {
                std::erase(open, path);
                sched.end_evict(path, deleted);
            }
        };
        Claims claims(*sched);
        const auto victims = cache->choose_victims([&](const ReadCacheEntry& e) {
            if (!sched->begin_evict(e.path)) {
                return true;  // файл с активным скачиванием или читающим pread не удаляется
            }
            claims.open.push_back(e.path);  // дальше его удаляет цикл ниже и снимает пометку
            return false;
        });
        for (const ReadCacheEntry& v : victims) {
            if (options.eviction_test_hook) {
                options.eviction_test_hook(v.path);
            }
            const bool gone = delete_cached(v);
            claims.release(v.path, gone);
            if (gone) {
                cache->forget(v.path);
            }
        }
        if (!victims.empty()) {
            spdlog::debug("TelegramStore: evicted {} file(s) from the read cache", victims.size());
        }
    }
    std::mutex maint_run_mu;  // одно вытеснение за раз

    void maintain() {
        const std::lock_guard<std::mutex> lock(maint_run_mu);
        run_eviction();
        if (cache) {
            cache->flush();
        }
    }

    void maintenance_loop() {
        std::unique_lock<std::mutex> lock(maint_mu);
        while (!maint_stop) {
            maint_cv.wait_for(lock, options.cache_flush,
                              [&] { return maint_stop || maint_wanted; });
            if (maint_stop) {
                break;
            }
            maint_wanted = false;
            lock.unlock();
            try {
                maintain();
            } catch (const std::exception& e) {
                spdlog::warn("TelegramStore: read cache maintenance failed: {}", e.what());
            }
            lock.lock();
        }
    }

    // ---- Голова ----

    std::optional<HeadInfo> read_head() {
        const auto deadline = deadline_in(options.read_timeout);
        ensure_chat(deadline);
        Json message;
        try {
            message = read_call_td(
                Json{{"@type", "getChatPinnedMessage"}, {"chat_id", options.chat_id}}, deadline);
        } catch (const TdException& e) {
            if (e.error().code == 404) {
                return std::nullopt;  // закреплённых нет (SPEC 2)
            }
            throw store_error(e, "getChatPinnedMessage");
        }
        const int64_t id = get_int(message, "id");
        const auto file = document_file(message);
        if (id == 0 || !file) {
            throw_store(StoreErrorKind::permanent, "the pinned message is not a document");
        }
        note_known(id);
        cache_put(id, file->id, file->size);
        const size_t want =
            file->size > 0 ? std::min<uint64_t>(kHeadHeaderSize, file->size) : kHeadHeaderSize;
        HeadInfo head;
        head.msg_id = id;
        retry_download(
            id, deadline,
            [&](bool refresh) { return refresh ? lookup_message_file(id, deadline) : *file; },
            [&](const FileInfo& target) {
                head.header.assign(want, std::byte{0});
                download_and_read(target, 0, head.header, ReadKind::blob, deadline);
            });
        return head;
    }
};

// ---- Публичный интерфейс -----------------------------------------------------------------

TelegramStore::TelegramStore(td::TdClient& client, RateLimiter& limiter,
                             TelegramStoreOptions options)
    : impl_(std::make_unique<Impl>(client, limiter, std::move(options))) {}

TelegramStore::~TelegramStore() = default;

void TelegramStore::open() {
    Impl& s = *impl_;
    const auto deadline = Impl::deadline_in(s.options.read_timeout);
    const std::string where = "channel check";
    const Json chat = s.read_call(Json{{"@type", "getChat"}, {"chat_id", s.options.chat_id}},
                                  deadline, "getChat");
    {
        const std::lock_guard<std::mutex> lock(s.chat_mu);
        s.chat_known = true;
    }
    const Json& type = get_obj(chat, "type");
    if (get_string(type, "@type") != "chatTypeSupergroup" ||
        !get_obj(type, "is_channel").is_boolean() || !get_obj(type, "is_channel").get<bool>()) {
        throw_store(StoreErrorKind::permanent, where + ": the chat is not a channel");
    }
    if (get_int(chat, "message_auto_delete_time") != 0) {
        throw_store(StoreErrorKind::permanent,
                    where + ": auto-delete of messages is enabled in the channel, turn it off");
    }
    const Json me = s.read_call(Json{{"@type", "getMe"}}, deadline, "getMe");
    const int64_t user_id = get_int(me, "id");
    const Json member =
        s.read_call(Json{{"@type", "getChatMember"},
                         {"chat_id", s.options.chat_id},
                         {"member_id", {{"@type", "messageSenderUser"}, {"user_id", user_id}}}},
                    deadline, "getChatMember");
    const Json& status = get_obj(member, "status");
    const std::string status_type = get_string(status, "@type");
    if (status_type == "chatMemberStatusCreator") {
        return;
    }
    if (status_type != "chatMemberStatusAdministrator") {
        throw_store(StoreErrorKind::permanent,
                    where + ": the account is not an administrator of the channel");
    }
    const Json& rights = get_obj(status, "rights");
    std::string missing;
    for (const auto& [key, label] : {std::pair{"can_post_messages", "post messages"},
                                     std::pair{"can_edit_messages", "edit messages"},
                                     std::pair{"can_delete_messages", "delete messages"}}) {
        const Json& flag = get_obj(rights, key);
        if (!flag.is_boolean() || !flag.get<bool>()) {
            missing += (missing.empty() ? "" : ", ") + std::string{label};
        }
    }
    if (!missing.empty()) {
        throw_store(StoreErrorKind::permanent,
                    where + ": the administrator lacks the right to " + missing);
    }
}

ChunkRef TelegramStore::put_chunk(const std::filesystem::path& encrypted_file) {
    return impl_->upload(encrypted_file, ".bin", "put_chunk");
}

int64_t TelegramStore::put_index(const std::filesystem::path& blob) {
    return impl_->upload(blob, ".idx", "put_index").msg_id;
}

void TelegramStore::read_range(const ChunkRef& ref, uint64_t offset, std::span<std::byte> out) {
    read_range_until(ref, offset, out, Deadline::max());
}

void TelegramStore::read_range_until(const ChunkRef& ref, uint64_t offset, std::span<std::byte> out,
                                     Deadline until) {
    if (out.empty()) {
        return;
    }
    Impl& s = *impl_;
    // Срок вызывающего (ChunkReader: один на всё чтение чанка) и собственный read_timeout: берётся
    // ближайший. Повторы после transient и ожидание лимитера делят этот срок.
    const auto deadline = std::min(until, Impl::deadline_in(s.options.read_timeout));
    s.ensure_chat(deadline);
    s.retry_download(
        ref.msg_id, deadline, [&](bool refresh) { return s.resolve_file(ref, deadline, refresh); },
        [&](const FileInfo& file) {
            if (file.size > 0 && (offset > file.size || out.size() > file.size - offset)) {
                throw NoRetryError(StoreErrorKind::permanent,
                                   "TelegramStore: read_range beyond the end of message " +
                                       std::to_string(ref.msg_id));
            }
            s.download_and_read(file, offset, out, ReadKind::user, deadline);
        });
}

void TelegramStore::delete_messages(std::span<const int64_t> msg_ids) {
    Impl& s = *impl_;
    s.sweep_abandoned();
    std::vector<int64_t> ids;
    {
        std::unordered_set<int64_t> seen;
        for (const int64_t id : msg_ids) {
            if (seen.insert(id).second) {
                ids.push_back(id);
            }
        }
    }
    for (size_t at = 0; at < ids.size(); at += kDeleteBatch) {
        std::vector<int64_t> batch(
            ids.begin() + static_cast<std::ptrdiff_t>(at),
            ids.begin() + static_cast<std::ptrdiff_t>(std::min(ids.size(), at + kDeleteBatch)));
        const auto deadline = Impl::deadline_in(s.options.send_timeout);
        s.ensure_chat(deadline);
        // После перезапуска процесса TDLib не знает старые сообщения и deleteMessages на них
        // не срабатывает (E2, E5): сначала getMessages. Исчезнувшие (null) удалять уже нечего.
        std::vector<int64_t> unknown;
        for (const int64_t id : batch) {
            if (!s.is_known(id)) {
                unknown.push_back(id);
            }
        }
        if (!unknown.empty()) {
            const Json reply = s.read_call(Json{{"@type", "getMessages"},
                                                {"chat_id", s.options.chat_id},
                                                {"message_ids", unknown}},
                                           deadline, "getMessages");
            const Json& messages = get_obj(reply, "messages");
            if (!messages.is_array() || messages.size() != unknown.size()) {
                throw_store(StoreErrorKind::permanent,
                            "getMessages returned a wrong number of results");
            }
            std::unordered_set<int64_t> gone;
            for (size_t i = 0; i < unknown.size(); ++i) {
                if (messages[i].is_null()) {
                    gone.insert(unknown[i]);
                } else {
                    s.note_known(unknown[i]);
                }
            }
            std::erase_if(batch, [&](int64_t id) { return gone.contains(id); });
        }
        if (batch.empty()) {
            continue;
        }
        try {
            s.run_action_td(Json{{"@type", "deleteMessages"},
                                 {"chat_id", s.options.chat_id},
                                 {"message_ids", batch},
                                 {"revoke", true}},
                            deadline);
        } catch (const TdException& e) {
            if (e.error().code != 400 || !contains_ci(e.error().message, "not found")) {
                throw store_error(e, "deleteMessages");
            }
            spdlog::debug("TelegramStore: deleteMessages: messages are already gone");
        }
        s.forget_known(batch);
    }
}

std::optional<HeadInfo> TelegramStore::read_head() { return impl_->read_head(); }

void TelegramStore::prefetch(const ChunkRef& ref, uint64_t offset, uint64_t len,
                             uint32_t distance) {
    impl_->queue_prefetch(ref, offset, len, distance);
}

void TelegramStore::start_read_cache() {
    Impl& s = *impl_;
    if (!s.cache) {
        return;
    }
    {
        const std::lock_guard<std::mutex> lock(s.maint_mu);
        if (s.maint_started) {
            return;
        }
        s.maint_started = true;
    }
    const ReconcileReport r = s.cache->reconcile();
    spdlog::info("read cache: {} file(s) kept, {} adopted, {} dropped, {} MiB in use", r.kept,
                 r.adopted, r.dropped, s.cache->total_bytes() >> 20U);
    s.maint_thread = std::thread([&s] { s.maintenance_loop(); });
    if (s.cache->over_limit()) {
        {
            const std::lock_guard<std::mutex> lock(s.maint_mu);
            s.maint_wanted = true;
        }
        s.maint_cv.notify_one();
    }
}

void TelegramStore::maintain_read_cache() { impl_->maintain(); }

bool TelegramStore::wait_prefetch_idle(std::chrono::milliseconds timeout) {
    Impl& s = *impl_;
    std::unique_lock<std::mutex> lock(s.pf_mu);
    return s.pf_idle_cv.wait_for(lock, timeout, [&] { return s.pf_queue.empty() && !s.pf_busy; });
}

SchedulerStats TelegramStore::download_stats() const { return impl_->sched->stats(); }

const ReadCache* TelegramStore::read_cache() const noexcept { return impl_->cache.get(); }

Bytes TelegramStore::read_index(int64_t msg_id) {
    Impl& s = *impl_;
    const auto deadline = Impl::deadline_in(s.options.read_timeout);
    s.ensure_chat(deadline);
    Bytes data;
    s.retry_download(
        msg_id, deadline, [&](bool) { return s.lookup_message_file(msg_id, deadline); },
        [&](const FileInfo& file) {
            if (file.size == 0) {
                throw NoRetryError(StoreErrorKind::permanent,
                                   "TelegramStore: the index blob has an unknown size");
            }
            data.assign(file.size, std::byte{0});
            s.download_and_read(file, 0, data, ReadKind::blob, deadline);
        });
    return data;
}

void TelegramStore::set_head(int64_t new_msg_id, std::optional<int64_t> old_msg_id) {
    Impl& s = *impl_;
    const auto deadline = Impl::deadline_in(s.options.send_timeout);
    s.ensure_chat(deadline);
    try {
        s.run_action_td(Json{{"@type", "pinChatMessage"},
                             {"chat_id", s.options.chat_id},
                             {"message_id", new_msg_id},
                             {"disable_notification", true},
                             {"only_for_self", false}},
                        deadline);
    } catch (const TdException& e) {
        throw store_error(e, "pinChatMessage");
    }
    if (!old_msg_id || *old_msg_id == new_msg_id) {
        return;
    }
    // Ошибка unpin не страшна: головой считается самое новое закреплённое (SPEC 7.1 шаг 7).
    try {
        if (!s.is_known(*old_msg_id)) {
            // После перезапуска процесса сообщение сначала надо загрузить, иначе 400 (E2).
            const Json reply = s.read_call(Json{{"@type", "getMessages"},
                                                {"chat_id", s.options.chat_id},
                                                {"message_ids", Json::array({*old_msg_id})}},
                                           deadline, "getMessages");
            const Json& messages = get_obj(reply, "messages");
            if (!messages.is_array() || messages.empty() || messages[0].is_null()) {
                spdlog::warn("TelegramStore: the old head {} is gone, nothing to unpin",
                             *old_msg_id);
                return;
            }
            s.note_known(*old_msg_id);
        }
        s.run_action_td(Json{{"@type", "unpinChatMessage"},
                             {"chat_id", s.options.chat_id},
                             {"message_id", *old_msg_id}},
                        deadline);
    } catch (const TdException& e) {
        spdlog::warn("TelegramStore: unpin of the old head failed: {}",
                     scrub(e.error().to_string()));
    } catch (const StoreError& e) {
        spdlog::warn("TelegramStore: unpin of the old head failed: {}", e.what());
    }
}

}  // namespace telega::store
