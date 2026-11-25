#pragma once

// Планировщик скачиваний TelegramStore. Все скачивания одного файла TDLib (чанка, пакета, блоба
// индекса) идут через его очередь, потому что новый downloadFile того же файла с другими
// offset/limit отменяет прежний.
//
// Правила (E10 подтвердил поведение TDLib):
// - по файлу активно не больше одного скачивания (offset, limit, priority), downloadFile всегда с
//   synchronous = false. Ход приходит в updateFile: готов префикс
//   [download_offset, download_offset + downloaded_prefix_size);
// - read() сначала спрашивает getFileDownloadedPrefixSize: готовый диапазон читается pread без
//   downloadFile. Если активное окно покрывает диапазон, read() ждёт его; иначе пользовательский
//   запрос вытесняет окно readahead и захватывает его непрочитанный остаток за запросом;
// - приоритеты: пользовательское чтение 32 вниз по возрасту (не ниже 17), голова и блобы индекса
//   32, readahead 16 вниз по расстоянию (не ниже 1). Приоритет ждущего пересчитывается повторным
//   downloadFile с теми же offset и limit; он не отменяет скачивание;
// - prefetch() ставит диапазон в очередь файла: смежный с активным окном продлевает его
//   (тот же offset, больший limit, пауза не нужна), прочие стартуют, когда файл свободен;
// - одновременно активных скачиваний по разным файлам не больше download_concurrency, readahead
//   занимает не больше половины из них. Пользовательские запросы допускаются по возрасту.
//
// Потоки. Обработчик updateFile работает в потоке приёма TdClient и только перекладывает данные и
// будит ожидающих. Все downloadFile делает один рабочий поток планировщика, блокирующие вызовы
// TDLib (getFileDownloadedPrefixSize, getFile) делают потоки вызывающих. Ни один вызов TDLib не
// делается под мьютексом.
//
// Неочевидные решения:
// - ранг пользовательского запроса считается по файлам: у файла один ранг — по его самому
//   старому ждущему запросу; следующие запросы того же файла ждут в его очереди, пока старый
//   не обслужен;
// - окно, которое больше никого не обслуживает (пользователь дочитал, а readahead его
//   «заимствовал»), через demote_delay понижается до приоритета readahead повторным вызовом;
// - оборвавшееся скачивание (updateFile «не качается» при непокрытом окне) отдаётся ждущим как
//   DownloadCanceled (transient), его повторяет вызывающий с новым file_id;
//   скачивание без прогресса stall_timeout повторяется тем же вызовом до max_stalls раз;
// - ответ на пользовательский запрос в очереди дольше read_timeout — transient (отсчёт от
//   постановки запроса, без сети); ожидание вытеснения, getFileDownloadedPrefixSize и
//   getFile ограничены тем же дедлайном;
// - вытеснение (begin_evict/end_evict) помечает все file_id с вытесняемым путём и запоминает сам
//   путь, поэтому чтение файла, которого планировщик не знал, тоже ждёт конца вытеснения, как
//   только узнаёт путь (барьер стоит и перед pread готового диапазона);
// - срок readahead_ttl считается от каждого запроса readahead, а не от последней подсказки файла;
// - при паузе FLOOD_WAIT рабочий поток спит до её конца или до события.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "store/interval_set.h"
#include "store/store.h"
#include "td/td_client.h"
#include "td/td_error.h"

namespace telega::store {

// Отмена или обрыв скачивания: transient, вызывающий повторяет с обновлением file_id.
class DownloadCanceled : public StoreError {
  public:
    using StoreError::StoreError;
};

// TDLib считает файл скачанным, а локального файла нет. Повторяется downloadFile.
class LocalFileGone : public StoreError {
  public:
    using StoreError::StoreError;
};

enum class ReadKind : uint8_t {
    user,  // пользовательское чтение, приоритет 32..17 по возрасту
    blob,  // голова и блобы индекса, приоритет 32
};

struct SchedulerOptions {
    using ms = std::chrono::milliseconds;
    uint32_t download_concurrency = 4;
    ms call_timeout{60'000};  // один запрос к TDLib
    ms stall_timeout{10'000};  // активное скачивание без прогресса: повторить downloadFile
    // Апдейт «не качается» принимается за обрыв, только если он пришёл после начала нашего
    // downloadFile по этому окну и несёт его download_offset (иначе это апдейт о прежнем окне), и
    // с тех пор не было прогресса failure_guard.
    ms failure_guard{300};
    ms demote_delay{300};  // окно без ждущих понижается до приоритета readahead
    ms readahead_ttl{
        15'000};  // запрос readahead (каждый отдельно), который не стартовал, забывается
    ms idle_forget{60'000};  // состояние простаивающего файла забывается
    uint64_t part_tolerance = 256U << 10U;  // допуск «сразу за запросом» (часть TDLib)
    int max_stalls = 3;
};

// Файл, которым занимается планировщик.
struct FileRef {
    int32_t id = 0;
    uint64_t size = 0;      // 0 — неизвестен
    std::string remote_id;  // может быть пустым
};

struct SchedulerHooks {
    using Clock = std::chrono::steady_clock;
    // Общая пауза после FLOOD_WAIT: новые downloadFile не уходят, пока она не кончится.
    std::function<Clock::time_point()> pause_until;
    std::function<void(const td::TdError&)> on_flood;
    // Файл кэша чтения скачивался или читался: учёт ведёт TelegramStore. Вызывается без
    // мьютексов планировщика.
    std::function<void(const std::string& path, const std::string& remote_id)> on_touch;
    // Путь файла перестал существовать (переименование при завершении скачивания, удаление).
    std::function<void(const std::string& path)> on_forget;
};

struct SchedulerStats {
    uint64_t calls = 0;  // всего downloadFile
    uint64_t starts = 0;  // из них с новым окном (старт и вытеснение)
    uint64_t displaced = 0;  // вытеснений активного окна
    uint64_t extended = 0;  // продлений окна (тот же offset, больший limit)
    uint64_t reprioritized = 0;  // смен приоритета (тот же offset и limit)
    uint64_t prefix_hits = 0;    // чтения, закрытые getFileDownloadedPrefixSize
    uint64_t stalls = 0;  // повторов после остановки без прогресса
    uint64_t active_now = 0;
    uint64_t max_active = 0;  // наибольшее число одновременно активных окон
    uint64_t waiting = 0;  // читателей, ждущих окно сейчас
    uint64_t plans = 0;    // завершённых проходов планировщика
    // Проходов планировщика после последней постановки запроса (ждущего чтения или readahead).
    // Проход идёт под мьютексом целиком, поэтому tests ждут plans_since_event >= 1 и затем
    // проверяют, что планировщик на запрос не ответил новым downloadFile.
    uint64_t plans_since_event = 0;
};

class DownloadScheduler {
  public:
    using Clock = std::chrono::steady_clock;

    DownloadScheduler(td::TdClient& client, SchedulerOptions options, SchedulerHooks hooks);
    DownloadScheduler(const DownloadScheduler&) = delete;
    DownloadScheduler& operator=(const DownloadScheduler&) = delete;
    ~DownloadScheduler();

    // Блокирует, пока [offset, offset + out.size()) файла не окажется на диске, и читает его в
    // out. deadline отсчитывается от постановки запроса. Исключения: StoreError transient
    // (дедлайн, остановка), DownloadCanceled, LocalFileGone, td::TdException (ошибка downloadFile
    // или getFile), StoreError permanent (чтение с диска не удалось).
    void read(const FileRef& file, uint64_t offset, std::span<std::byte> out, ReadKind kind,
              Clock::time_point deadline);
    // Состояние файла в планировщике одной строкой (окно, ready, ждущие, последние апдейты): то же
    // выводится в лог при таймауте ожидания окна. Для диагностики и тестов.
    [[nodiscard]] std::string describe(int32_t file_id) const;

    // Подсказка readahead: [offset, offset + len) файла. Может блокировать на локальный вызов
    // getFileDownloadedPrefixSize, поэтому зовётся из фонового потока. distance — расстояние в
    // чанках от позиции чтения. Ошибки глотаются.
    void prefetch(const FileRef& file, uint64_t offset, uint64_t len, uint32_t distance);

    // Вытеснение из кэша чтения. begin_evict сообщает, что файл по этому пути можно
    // удалять: false — он занят (скачивается, читается или ждёт чтения; под одним путём могут быть
    // известны несколько file_id, занят любой — отказ). Пока вытеснение не закончено (end_evict),
    // чтение любого file_id, чей путь совпал с вытесняемым, ждёт: помечаются все известные
    // планировщику файлы с этим путём, а путь запоминается и для файлов, которых планировщик
    // пока не знает (метка ставится, когда они получат путь).
    bool begin_evict(const std::string& path);
    // deleted — файл удалён с диска: готовые диапазоны файлов с этим путём забываются.
    void end_evict(const std::string& path, bool deleted);

    [[nodiscard]] SchedulerStats stats() const;
    // Для тестов: сколько файлов планировщик помнит.
    [[nodiscard]] size_t tracked_files() const;

  private:
    struct Wait;
    struct Plan;
    struct Active {
        int64_t off = 0;
        int64_t lim = 0;  // 0 — до конца файла
        int prio = 0;
        bool user = false;
        // Момент, когда рабочий поток начал отправлять downloadFile по этому окну (пусто — запрос
        // ещё не ушёл). Апдейты «не качается» раньше него принадлежат прежнему окну и обрывом не
        // считаются.
        Clock::time_point sent;
        Clock::time_point last_progress;
        int64_t progress_mark = 0;
    };
    struct RaRequest {
        int64_t a = 0;
        int64_t b = 0;
        Clock::time_point stamp;
    };
    // Запись об апдейте или ответе TDLib по файлу: для дампа состояния при таймауте ожидания окна.
    struct UpdateRec {
        Clock::time_point at;
        bool reply = false;  // ответ на downloadFile, а не updateFile
        int64_t download_offset = 0;
        int64_t prefix = 0;
        int64_t size = 0;
        bool active = false;
        bool completed = false;
        std::string path_name;  // только имя файла: пути в логи не пишутся
    };
    static constexpr size_t kUpdateHistory = 16;
    struct FileState {
        int32_t id = 0;
        std::deque<UpdateRec> history;
        int64_t size = 0;  // 0 — неизвестен
        std::string path;
        std::string remote_id;
        std::vector<std::string> stale_paths;  // прежние пути, о которых надо сообщить кэшу
        IntervalSet ready;
        IntervalSet ra;  // запрошенный readahead (без готового считается remaining)
        // Запросы readahead со временем постановки: ra — их объединение, срок readahead_ttl
        // считается от каждого запроса.
        std::vector<RaRequest> ra_reqs;
        uint32_t ra_distance = 0;
        std::optional<Active> active;
        std::vector<Wait*> waiters;
        uint32_t users = 0;  // потоки в read()
        bool evicting = false;
        Clock::time_point last_use;
        Clock::time_point last_demand;
        Clock::time_point inactive_seen;  // последний апдейт «не качается» при непокрытом окне
        int stalls = 0;
        Clock::time_point retry_at;
        int64_t last_total = 0;

        // Конец активного окна; размер неизвестен и limit 0 — без предела.
        [[nodiscard]] int64_t end_of_active() const {
            if (!active) {
                return 0;
            }
            int64_t end = active->lim == 0 ? INT64_MAX : active->off + active->lim;
            if (size > 0) {
                end = std::min(end, size);
            }
            return end;
        }
    };
    struct Action {
        int32_t file_id = 0;
        int64_t off = 0;
        int64_t lim = 0;
        int prio = 0;
    };

    static int user_prio(size_t rank);
    static int ra_prio(uint32_t distance);

    // read() без обёртки диагностики; diag получает дамп состояния, если чтение упёрлось в дедлайн.
    void read_impl(const FileRef& file, uint64_t offset, std::span<std::byte> out, ReadKind kind,
                   Clock::time_point deadline, std::string& diag);
    [[nodiscard]] std::string describe_locked(const FileState& f) const;  // под mu_
    // Дамп в лог (warning) вместе с ответом getFile в этот момент; без мьютексов планировщика.
    void log_wait_timeout(int32_t file_id, const std::string& dump);
    FileState& track(const FileRef& file);  // под mu_
    // под mu_: новый путь файла; прежний уходит в stale_paths, вытесняемый путь ставит барьер
    void set_path(FileState& f, const std::string& path);
    static void clear_ra(FileState& f);
    static void rebuild_ra(FileState& f);
    void on_update(const td::Json& update);
    // под mu_; from_reply — ответ на downloadFile, а не апдейт (его путь мог устареть)
    void apply_file(const td::Json& file, bool from_reply);
    // Активное окно обслуживает ждущего: все непокрытые куски его диапазона внутри окна.
    [[nodiscard]] static bool window_serves(const FileState& f, const Wait& w);
    [[nodiscard]] static bool satisfied(const FileState& f, int64_t a, int64_t b);
    [[nodiscard]] static bool unsatisfied(const FileState& f, const Wait& w);
    [[nodiscard]] static std::vector<std::pair<int64_t, int64_t>> remaining(const FileState& f);
    static void set_active(FileState& f, int64_t off, int64_t lim, int prio, bool user,
                           Clock::time_point now);
    void fail_waiters(FileState& f, const std::optional<td::TdError>& td_error);
    Plan plan(Clock::time_point now, Clock::time_point& next_wake);
    void loop();
    void issue(const Action& action);
    // true, если диапазон уже скачан (getFileDownloadedPrefixSize), и тогда он добавлен в ready.
    // Вызов ограничен дедлайном чтения: не дождались — false (дальше обычный путь с тем же
    // дедлайном).
    bool probe_ready(int32_t file_id, int64_t a, int64_t b, Clock::time_point deadline);
    // Читает out с диска; возвращает путь, по которому прочитано.
    std::string pread_range(int32_t file_id, int64_t offset, std::span<std::byte> out,
                            Clock::time_point deadline);
    // getFile не дольше call_timeout и не дольше дедлайна чтения; не успели — transient StoreError.
    std::string refresh_path(int32_t file_id, Clock::time_point deadline);
    // Сколько можно ждать одного вызова TDLib: call_timeout, но не дальше дедлайна чтения.
    [[nodiscard]] std::chrono::milliseconds call_budget(Clock::time_point deadline) const;

    td::TdClient& client_;
    const SchedulerOptions opts_;
    const SchedulerHooks hooks_;

    mutable std::mutex mu_;
    std::condition_variable cv_;         // ждущие запросы
    std::condition_variable worker_cv_;  // рабочий поток
    std::map<int32_t, FileState> files_;
    uint64_t wait_seq_ = 0;
    uint64_t plans_ = 0;
    uint64_t plans_at_event_ = 0;
    uint64_t waiting_ = 0;
    std::set<std::string> evicting_paths_;  // пути под вытеснением (begin_evict .. end_evict)
    bool stop_ = false;
    SchedulerStats stats_;
    td::Subscription sub_;
    std::thread worker_;
};

}  // namespace telega::store
