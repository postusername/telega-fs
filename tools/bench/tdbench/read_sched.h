#pragma once

// Прототип планировщика скачиваний и readahead по SPEC 8.2 (E10, SPEC 17.13). Компактный
// самостоятельный класс для tgfs-bench: боевую версию в TelegramStore напишут по выводам E10,
// поэтому здесь важна ясность поведения, а не универсальность.
//
// Правила (SPEC 8.2, 10.2):
//   - по файлу активно не больше одного скачивания (offset, limit, priority), `downloadFile` с
//     synchronous = false; ход приходит в `updateFile`: готов префикс
//     [download_offset, download_offset + downloaded_prefix_size);
//   - запрос диапазона сначала проверяет готовое (память, затем getFileDownloadedPrefixSize);
//   - если активное скачивание покрывает диапазон, запрос ждёт, приоритет повышается повторным
//     downloadFile с теми же offset и limit;
//   - иначе пользовательский запрос вытесняет активное скачивание readahead: limit захватывает и
//     непрочитанный остаток вытесненного окна, если он идёт сразу за запросом;
//   - запрос readahead, не покрытый активным скачиванием, встаёт в очередь файла и стартует,
//     когда очередь свободна; смежный с активным окном продлевает его повторным downloadFile с тем
//     же offset и бóльшим limit (отступление от 8.2, которое проверяет E10);
//   - приоритеты: пользовательский запрос от 32 вниз по возрасту (не ниже 17), readahead от 16
//     вниз по расстоянию в чанках (не ниже 1);
//   - всего активных скачиваний не больше download_concurrency, readahead — не больше половины.
//
// updateFile приходит в потоке приёма TdClient и только перекладывает данные и будит ожидающих;
// все вызовы TDLib делает рабочий поток планировщика.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "td/td_client.h"
#include "td/td_error.h"

namespace telega::bench {

// Объединение полуинтервалов [a, b).
class IntervalSet {
  public:
    void add(int64_t a, int64_t b) {
        if (b <= a) {
            return;
        }
        auto it = m_.upper_bound(a);
        if (it != m_.begin()) {
            auto prev = std::prev(it);
            if (prev->second >= a) {
                a = prev->first;
                b = std::max(b, prev->second);
                it = m_.erase(prev);
            }
        }
        while (it != m_.end() && it->first <= b) {
            b = std::max(b, it->second);
            it = m_.erase(it);
        }
        m_[a] = b;
    }
    [[nodiscard]] bool covers(int64_t a, int64_t b) const {
        if (b <= a) {
            return true;
        }
        return end_of(a) >= b;
    }
    // Конец непрерывного куска, в котором лежит a; a, если a не покрыта.
    [[nodiscard]] int64_t end_of(int64_t a) const {
        auto it = m_.upper_bound(a);
        if (it == m_.begin()) {
            return a;
        }
        --it;
        return it->second > a ? it->second : a;
    }
    // Непокрытые куски [a, b).
    [[nodiscard]] std::vector<std::pair<int64_t, int64_t>> missing(int64_t a, int64_t b) const {
        std::vector<std::pair<int64_t, int64_t>> gaps;
        int64_t pos = a;
        while (pos < b) {
            const int64_t e = end_of(pos);
            if (e > pos) {
                pos = e;
                continue;
            }
            auto it = m_.upper_bound(pos);
            const int64_t next = it == m_.end() ? b : std::min(b, it->first);
            gaps.emplace_back(pos, next);
            pos = next;
        }
        return gaps;
    }
    [[nodiscard]] const std::map<int64_t, int64_t>& intervals() const { return m_; }
    [[nodiscard]] int64_t total() const {
        int64_t t = 0;
        for (const auto& [a, b] : m_) {
            t += b - a;
        }
        return t;
    }
    [[nodiscard]] bool empty() const { return m_.empty(); }
    void clear() { m_.clear(); }

  private:
    std::map<int64_t, int64_t> m_;
};

struct SchedConfig {
    int download_concurrency = 4;
    std::chrono::milliseconds stall{8000};  // нет прогресса — повторить downloadFile
    std::chrono::milliseconds read_timeout{120'000};
    std::chrono::milliseconds idle_demote{
        300};  // после стольких мс без запросов приоритет readahead
    int64_t part = 256 * 1024;  // часть TDLib, допуск «сразу за запросом»
};

struct SchedStats {
    int64_t downloads = 0;  // вызовов downloadFile с новым диапазоном (старт и вытеснение)
    int64_t displaced = 0;  // из них вытеснений активного скачивания
    int64_t absorbed = 0;  // из них захватов остатка или смежного readahead
    int64_t extended = 0;  // продлений окна readahead (тот же offset, больше limit)
    int64_t raised = 0;  // повышений приоритета до пользовательского
    int64_t reprio = 0;  // прочих смен приоритета
    int64_t lost = 0;    // повторов после остановки без прогресса
    int64_t errors = 0;  // ошибки вызовов downloadFile
    int64_t prefix_checks = 0;  // getFileDownloadedPrefixSize
    int64_t prefix_hits = 0;    // из них «уже скачано»
    int64_t ready_hits = 0;     // запросы, закрытые данными из памяти
    int64_t calls = 0;          // всего вызовов downloadFile
};

class ReadScheduler {
  public:
    using Clock = std::chrono::steady_clock;

    ReadScheduler(td::TdClient& client, SchedConfig cfg) : client_(client), cfg_(cfg) {
        sub_ = client_.subscribe("updateFile", [this](const td::Json& u) { on_update(u); });
        worker_ = std::thread([this] { loop(); });
    }
    ReadScheduler(const ReadScheduler&) = delete;
    ReadScheduler& operator=(const ReadScheduler&) = delete;
    ~ReadScheduler() {
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

    void add_file(int32_t file_id, int64_t size) {
        const std::lock_guard<std::mutex> lock(mu_);
        FileState& f = files_[file_id];
        f.id = file_id;
        f.size = size;
    }

    // Блокирует, пока диапазон [a, b) файла не будет на диске. Возвращает local.path.
    // Бросает std::runtime_error при таймауте или исчерпании повторов, TdException при лимите.
    std::string ensure(int32_t file_id, int64_t a, int64_t b) {
        {
            std::unique_lock<std::mutex> lock(mu_);
            FileState& f = file(file_id);
            b = std::min(b, f.size);
            if (f.ready.covers(a, b) && !f.path.empty()) {
                ++stats_.ready_hits;
                return f.path;
            }
        }
        // Проверка готового у TDLib (SPEC 8.2): диапазон мог скачаться раньше.
        try {
            const td::Json r = client_.call(td::Json{
                {"@type", "getFileDownloadedPrefixSize"}, {"file_id", file_id}, {"offset", a}});
            const int64_t size = r.value("size", int64_t{0});
            bool hit = false;
            {
                const std::lock_guard<std::mutex> lock(mu_);
                ++stats_.prefix_checks;
                if (size >= b - a) {
                    ++stats_.prefix_hits;
                    file(file_id).ready.add(a, a + size);
                    hit = true;
                }
            }
            if (hit) {
                std::string path = path_of(file_id);
                if (path.empty()) {
                    const td::Json fj =
                        client_.call(td::Json{{"@type", "getFile"}, {"file_id", file_id}});
                    path = fj["local"].value("path", std::string{});
                    const std::lock_guard<std::mutex> lock(mu_);
                    file(file_id).path = path;
                }
                if (!path.empty()) {
                    return path;
                }
            }
        } catch (const td::TdException&) {
            // нет данных — обычный путь
        }
        std::unique_lock<std::mutex> lock(mu_);
        FileState& f = file(file_id);
        const uint64_t seq = ++demand_seq_;
        f.demands.push_back(Demand{seq, a, b});
        f.last_demand = Clock::now();
        worker_cv_.notify_one();
        const auto deadline = Clock::now() + cfg_.read_timeout;
        const auto drop = [&] {
            f.demands.erase(std::remove_if(f.demands.begin(), f.demands.end(),
                                           [&](const Demand& d) { return d.seq == seq; }),
                            f.demands.end());
        };
        for (;;) {
            if (f.ready.covers(a, b) && !f.path.empty()) {
                drop();
                f.last_demand = Clock::now();
                return f.path;
            }
            if (flood_) {
                drop();
                throw td::TdException(*flood_);
            }
            if (!f.error.empty()) {
                drop();
                throw std::runtime_error(f.error);
            }
            if (stop_ || Clock::now() > deadline) {
                drop();
                throw std::runtime_error("read timeout");
            }
            cv_.wait_for(lock, std::chrono::milliseconds{200});
        }
    }

    // Просит скачать диапазон заранее. rank — расстояние в чанках от позиции чтения.
    void prefetch(int32_t file_id, int64_t a, int64_t b, int rank) {
        const std::lock_guard<std::mutex> lock(mu_);
        FileState& f = file(file_id);
        b = std::min(b, f.size);
        for (const auto& [x, y] : f.ready.missing(a, b)) {
            f.ra.add(x, y);
        }
        f.ra_rank = rank;
        worker_cv_.notify_one();
    }

    [[nodiscard]] SchedStats stats() {
        const std::lock_guard<std::mutex> lock(mu_);
        return stats_;
    }
    [[nodiscard]] std::string path_of(int32_t file_id) {
        const std::lock_guard<std::mutex> lock(mu_);
        return file(file_id).path;
    }

  private:
    struct Demand {
        uint64_t seq = 0;
        int64_t a = 0;
        int64_t b = 0;
    };
    struct Active {
        int64_t off = 0;
        int64_t lim = 0;  // 0 — до конца файла
        int prio = 0;
        bool user = false;
        Clock::time_point issued;
        Clock::time_point last_progress;
        int64_t progress_mark = 0;
    };
    struct FileState {
        int32_t id = 0;
        int64_t size = 0;
        std::string path;
        IntervalSet ready;
        IntervalSet ra;  // запрошенный readahead (кроме уже готового)
        int ra_rank = 0;
        std::optional<Active> active;
        std::vector<Demand> demands;
        Clock::time_point last_demand{};
        int attempts = 0;
        Clock::time_point retry_at{};
        std::string error;
        bool suspect = false;
        int64_t last_total = 0;

        [[nodiscard]] int64_t end_of_active() const {
            return active->lim == 0 ? size : std::min(size, active->off + active->lim);
        }
    };
    struct Action {
        int32_t file_id = 0;
        int64_t off = 0;
        int64_t lim = 0;
        int prio = 0;
    };

    FileState& file(int32_t id) {
        auto it = files_.find(id);
        if (it == files_.end()) {
            throw std::runtime_error("file is not registered in the scheduler");
        }
        return it->second;
    }

    static int user_prio(size_t age_index) {
        return static_cast<int>(std::max<int64_t>(17, 32 - static_cast<int64_t>(age_index)));
    }
    static int ra_prio(int rank) { return std::clamp(16 - rank, 1, 16); }

    void on_update(const td::Json& u) {
        if (!u.contains("file") || !u["file"].contains("local")) {
            return;
        }
        const td::Json& file_json = u["file"];
        const td::Json& l = file_json["local"];
        const std::lock_guard<std::mutex> lock(mu_);
        auto it = files_.find(file_json.value("id", int32_t{-1}));
        if (it == files_.end()) {
            return;
        }
        FileState& f = it->second;
        const std::string path = l.value("path", std::string{});
        if (!path.empty()) {
            f.path = path;
        }
        const bool completed = l.value("is_downloading_completed", false);
        const int64_t off = l.value("download_offset", int64_t{0});
        const int64_t prefix = l.value("downloaded_prefix_size", int64_t{0});
        if (completed) {
            f.ready.add(0, f.size);
        } else if (prefix > 0) {
            f.ready.add(off, std::min(f.size, off + prefix));
        }
        const int64_t total = f.ready.total();
        if (f.active) {
            if (total > f.last_total) {
                f.active->last_progress = Clock::now();
            }
            // Остановка без прогресса: апдейт «не качается», хотя окно не готово. Старые апдейты
            // (до нашего вызова) отсекаются задержкой 300 мс от вызова.
            if (!l.value("is_downloading_active", false) && !completed &&
                !f.ready.covers(f.active->off, f.end_of_active()) &&
                Clock::now() - f.active->issued > std::chrono::milliseconds{300}) {
                f.suspect = true;
            }
        }
        f.last_total = total;
        cv_.notify_all();
        worker_cv_.notify_one();
    }

    void loop() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!stop_) {
            worker_cv_.wait_for(lock, std::chrono::milliseconds{100});
            if (stop_) {
                break;
            }
            std::vector<Action> actions = plan(Clock::now());
            if (actions.empty()) {
                continue;
            }
            lock.unlock();
            for (const Action& a : actions) {
                issue(a);
            }
            lock.lock();
        }
    }

    // Один вызов downloadFile. Ошибка снимает активное скачивание файла, повтор — на следующем
    // проходе.
    void issue(const Action& a) {
        try {
            client_.call(td::Json{{"@type", "downloadFile"},
                                  {"file_id", a.file_id},
                                  {"priority", a.prio},
                                  {"offset", a.off},
                                  {"limit", a.lim},
                                  {"synchronous", false}},
                         std::chrono::seconds{30});
        } catch (const td::TdException& e) {
            const std::lock_guard<std::mutex> lock(mu_);
            ++stats_.errors;
            FileState& f = file(a.file_id);
            if (e.error().is_rate_limit()) {
                flood_ = e.error();
            }
            if (f.active && f.active->off == a.off) {
                f.active.reset();
            }
            ++f.attempts;
            f.retry_at = Clock::now() + std::chrono::seconds{std::min(f.attempts, 5)};
            if (f.attempts > 5) {
                f.error = "downloadFile failed repeatedly: " + e.error().to_string();
            }
            cv_.notify_all();
        }
    }

    [[nodiscard]] std::vector<std::pair<int64_t, int64_t>> remaining(FileState& f) const {
        std::vector<std::pair<int64_t, int64_t>> out;
        for (const auto& [a, b] : f.ra.intervals()) {
            for (const auto& gap : f.ready.missing(a, b)) {
                out.push_back(gap);
            }
        }
        return out;
    }

    void set_active(FileState& f, int64_t off, int64_t lim, int prio, bool user,
                    Clock::time_point now) {
        Active a;
        a.off = off;
        a.lim = lim;
        a.prio = prio;
        a.user = user;
        a.issued = now;
        a.last_progress = now;
        f.active = a;
    }

    // Под mu_. Решает, какие вызовы downloadFile нужны сейчас.
    std::vector<Action> plan(Clock::time_point now) {
        std::vector<Action> out;
        // Пользовательские запросы по возрасту: самый старый — 32, следующие на единицу ниже.
        struct Want {
            Demand demand;
            int prio = 0;
        };
        std::vector<std::pair<uint64_t, FileState*>> order;
        for (auto& [id, f] : files_) {
            for (const Demand& d : f.demands) {
                if (!f.ready.covers(d.a, d.b)) {
                    order.emplace_back(d.seq, &f);
                }
            }
        }
        std::sort(order.begin(), order.end());
        std::map<FileState*, Want> wants;
        for (size_t i = 0; i < order.size(); ++i) {
            FileState* f = order[i].second;
            if (wants.contains(f)) {
                continue;
            }
            for (const Demand& d : f->demands) {
                if (d.seq == order[i].first) {
                    wants[f] = Want{d, user_prio(i)};
                }
            }
        }
        int total_active = 0;
        int ra_active = 0;
        for (auto& [id, f] : files_) {
            if (f.active && f.ready.covers(f.active->off, f.end_of_active())) {
                f.active.reset();
                f.attempts = 0;
            }
            if (f.active) {
                ++total_active;
                ra_active += f.active->user || wants.contains(&f) ? 0 : 1;
            }
        }
        struct Candidate {
            int rank;
            FileState* f;
        };
        std::vector<Candidate> starts;
        for (auto& [id, f] : files_) {
            if (now < f.retry_at || !f.error.empty()) {
                continue;
            }
            // Остановка без прогресса: тот же downloadFile ещё раз.
            if (f.active && (f.suspect || now - f.active->last_progress > cfg_.stall)) {
                out.push_back({id, f.active->off, f.active->lim, f.active->prio});
                ++stats_.lost;
                ++stats_.calls;
                f.active->last_progress = now;
                f.active->issued = now;
                f.suspect = false;
                if (++f.attempts > 5) {
                    f.error = "download stalled repeatedly";
                }
                continue;
            }
            const auto want_it = wants.find(&f);
            if (want_it != wants.end()) {
                const Demand& d = want_it->second.demand;
                const int prio = want_it->second.prio;
                if (f.active && f.active->off <= d.a && f.end_of_active() >= d.b) {
                    // Покрыт активным скачиванием: повысить приоритет.
                    if (!f.active->user || f.active->prio != prio) {
                        out.push_back({id, f.active->off, f.active->lim, prio});
                        ++stats_.raised;
                        ++stats_.calls;
                        f.active->prio = prio;
                        f.active->user = true;
                    }
                } else if (f.active && f.active->user) {
                    // Занято другим пользовательским запросом того же файла: ждём очереди.
                } else {
                    const auto gaps = f.ready.missing(d.a, d.b);
                    const int64_t off = gaps.empty() ? d.a : gaps.front().first;
                    int64_t end = d.b;
                    bool absorbed = false;
                    if (f.active) {
                        // Остаток вытесняемого окна, если он идёт сразу за запросом.
                        const int64_t r_begin = f.ready.end_of(f.active->off);
                        const int64_t r_end = f.end_of_active();
                        if (r_begin < r_end && r_begin >= off && r_begin <= d.b + cfg_.part) {
                            end = std::max(end, r_end);
                            absorbed = true;
                        }
                    } else {
                        for (const auto& [a, b] : remaining(f)) {
                            if (a <= d.b + cfg_.part && b > end && b > off) {
                                end = std::max(end, b);
                                absorbed = true;
                            }
                        }
                    }
                    end = std::min(end, f.size);
                    ++stats_.downloads;
                    ++stats_.calls;
                    stats_.displaced += f.active ? 1 : 0;
                    stats_.absorbed += absorbed ? 1 : 0;
                    if (!f.active) {
                        ++total_active;
                    }
                    out.push_back({id, off, end - off, prio});
                    set_active(f, off, end - off, prio, true, now);
                }
                continue;
            }
            // Запросов нет.
            if (f.active) {
                const int want_prio = ra_prio(f.ra_rank);
                if (f.active->user && now - f.last_demand > cfg_.idle_demote) {
                    f.active->user = false;
                    if (f.active->prio != want_prio) {
                        out.push_back({id, f.active->off, f.active->lim, want_prio});
                        ++stats_.reprio;
                        ++stats_.calls;
                        f.active->prio = want_prio;
                    }
                    ++ra_active;
                } else if (!f.active->user && f.active->prio != want_prio) {
                    out.push_back({id, f.active->off, f.active->lim, want_prio});
                    ++stats_.reprio;
                    ++stats_.calls;
                    f.active->prio = want_prio;
                }
                // Продление окна: смежный кусок readahead.
                const int64_t end = f.end_of_active();
                for (const auto& [a, b] : remaining(f)) {
                    if (a <= end + cfg_.part && b > end && a >= f.active->off) {
                        const int64_t lim = b - f.active->off;
                        out.push_back({id, f.active->off, lim, f.active->prio});
                        ++stats_.extended;
                        ++stats_.calls;
                        f.active->lim = lim;
                        break;
                    }
                }
                continue;
            }
            const auto rem = remaining(f);
            if (!rem.empty()) {
                starts.push_back({f.ra_rank, &f});
            }
        }
        std::sort(starts.begin(), starts.end(),
                  [](const Candidate& x, const Candidate& y) { return x.rank < y.rank; });
        for (const Candidate& c : starts) {
            if (total_active >= cfg_.download_concurrency ||
                ra_active >= std::max(1, cfg_.download_concurrency / 2)) {
                break;
            }
            FileState& f = *c.f;
            const auto rem = remaining(f);
            const auto [a, b] = rem.front();
            const int prio = ra_prio(f.ra_rank);
            out.push_back({f.id, a, b - a, prio});
            set_active(f, a, b - a, prio, false, now);
            ++stats_.downloads;
            ++stats_.calls;
            ++total_active;
            ++ra_active;
        }
        return out;
    }

    td::TdClient& client_;
    const SchedConfig cfg_;
    std::mutex mu_;
    std::condition_variable cv_;         // ожидающие запросы
    std::condition_variable worker_cv_;  // рабочий поток
    std::map<int32_t, FileState> files_;
    SchedStats stats_;
    std::optional<td::TdError> flood_;
    uint64_t demand_seq_ = 0;
    bool stop_ = false;
    std::thread worker_;
    td::Subscription sub_;
};

}  // namespace telega::bench
