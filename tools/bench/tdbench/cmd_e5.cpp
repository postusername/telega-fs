// e5: лимиты отправки в режиме бота (SPEC 17.8). Канал создаёт пользователь (setup, сообщений
// от пользователя нет), всю нагрузку несёт бот в отдельном каталоге сессии <state-dir>/bot-e5.
//
//   tgfs-bench e5 [--probe] setup      пользователь создаёт канал и добавляет бота администратором
//   tgfs-bench e5 [--probe] run        серии отправки: возобновляемо, запускать в фоне
//   tgfs-bench e5 [--probe] status     состояние серий и счётчики
//   tgfs-bench e5 [--probe] teardown   удалить канал (пользователь) и каталоги сессии бота
//
// Серии (порядок выбран так, чтобы FLOOD_WAIT одной не портил следующую):
//   - «волны» по 16 МиБ при параллельности 1, 2, 4: P документов сразу раз в 20 с, не больше
//     12 действий в минуту, лимита они не достигают, зато видно скорость заливки при P
//     одновременных загрузках (для upload_concurrency);
//   - «лестницы»: темп 5, 10, 15, 18, 20, 30, 60 документов в минуту по 10 минут. Ступень
//     обрывается на первом FLOOD_WAIT, серия на этом заканчивается. Если темп не выдержала
//     пропускная способность канала (меньше 80 % заданного), ступень помечается
//     bandwidth_bound, и серия дальше не идёт. 1 КиБ при параллельности 1, 2, 4, потом 16 МиБ,
//     потом 64 МиБ;
//   - «возврат»: сразу после первой лестницы (1 КиБ, параллельность 1) ждёт 30 минут после
//     FLOOD_WAIT и 10 минут держит наибольший устойчивый темп этой лестницы.
// После каждой серии бот удаляет её документы пачками по 100, между сериями пауза не меньше
// 10 минут. Всё, что нужно для отчёта, пишется в CSV (без id канала и username); id канала
// лежит только в <state-dir>/e5.json.
//
// Код отправки (send_load.*) общий с будущим E6: задание может быть альбомом до 10 документов.

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <csignal>
#include <ctime>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "commands.h"
#include "exp_common.h"
#include "send_load.h"

namespace telega::bench {

namespace {

namespace fs = std::filesystem;
using std::chrono::milliseconds;
using std::chrono::seconds;
using WallClock = std::chrono::system_clock;

constexpr int64_t kServerIdShift = 20;
constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * kKiB;
constexpr uint64_t kSeed = 0xE5E5E5E5ULL;
constexpr int64_t kMaxFloodWaitSeconds = 3600;  // SPEC 17.2: больше часа — остановиться
constexpr double kBandwidthShare = 0.8;  // ниже этой доли заданного темпа — упёрлись в канал
constexpr int kMaxStepAttempts = 3;  // повторов ступени, сорванной сетью (disturbed)
constexpr int kDeleteBatch = 100;

std::atomic<bool> g_stop{false};

extern "C" void e5_on_stop_signal(int /*sig*/) { g_stop.store(true); }

void install_signal_handlers() {
    struct sigaction sa {};
    sa.sa_handler = e5_on_stop_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

// Остановка по сигналу: идущие загрузки закончены, состояние сохранено.
class Interrupted : public std::runtime_error {
  public:
    Interrupted() : std::runtime_error("interrupted by signal") {}
};

// Клиент TDLib закрыт или рантайм остановлен: сеанс надо открыть заново.
class SessionError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

void sleep_interruptible(milliseconds duration) {
    const auto end = ExpClock::now() + duration;
    while (ExpClock::now() < end) {
        if (g_stop.load()) {
            throw Interrupted();
        }
        const auto left = std::chrono::duration_cast<milliseconds>(end - ExpClock::now());
        std::this_thread::sleep_for(std::min(left, milliseconds{500}));
    }
    if (g_stop.load()) {
        throw Interrupted();
    }
}

void log_line(const std::string& text) { std::cerr << iso_utc_now() << " " << text << "\n"; }

int64_t epoch_now() { return static_cast<int64_t>(std::time(nullptr)); }

int64_t server_id(int64_t message_id) { return message_id >> kServerIdShift; }

double seconds_between(WallClock::time_point a, WallClock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

std::string size_name(uint64_t size) {
    if (size >= kMiB) {
        return std::to_string(size / kMiB) + "MiB";
    }
    return std::to_string(size / kKiB) + "KiB";
}

std::string date_stamp() {
    const std::string iso = iso_utc_now();
    return iso.substr(0, 4) + iso.substr(5, 2) + iso.substr(8, 2);
}

// ---- Параметры и имена ------------------------------------------------------------------

struct E5Options {
    bool probe = false;
    fs::path out;
    std::string rights = "only:post,edit,delete";
    int step_seconds = 600;          // длительность ступени
    int series_pause_seconds = 600;  // пауза между сериями (после удаления)
    int recovery_wait_seconds = 1800;  // «возврат»: пауза после окончания FLOOD_WAIT
    int wave_cycles = 15;
    int wave_period_seconds = 20;
    int delete_interval_ms = 6000;  // между пачками удаления и чтения (не больше 10 в минуту)
};

struct Names {
    std::string prefix;  // состояние канала <state-dir>/e5.json (общее для пробы и прогона)
    std::string run_state;  // состояние серий <state-dir>/<run_state>.json
    fs::path out;
    fs::path session;
    fs::path lock;
    fs::path docs;
};

Names names_for(const BenchOptions& options, const E5Options& e5) {
    Names n;
    n.prefix = "e5";
    n.run_state = e5.probe ? "e5p-run" : "e5-run";
    n.out = e5.probe ? e5.out / "probe" : e5.out;
    n.session = options.state_dir / "bot-e5";
    n.lock = options.state_dir / "e5-run.lock";
    n.docs = options.state_dir / "e5-docs";
    return n;
}

// Один процесс на каталог сессии бота: две копии TDLib на одной БД нельзя.
class SessionLock {
  public:
    explicit SessionLock(const fs::path& path) {
        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
        if (fd_ < 0) {
            throw std::runtime_error("cannot open lock file " + path.string());
        }
        if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("another e5 process uses this bot session (lock " +
                                     path.string() + ")");
        }
    }
    SessionLock(const SessionLock&) = delete;
    SessionLock& operator=(const SessionLock&) = delete;
    ~SessionLock() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

  private:
    int fd_ = -1;
};

// ---- План серий ---------------------------------------------------------------------------

enum class SeriesKind : uint8_t { kWave, kLadder, kRecovery };

const char* kind_name(SeriesKind k) {
    switch (k) {
        case SeriesKind::kWave:
            return "wave";
        case SeriesKind::kLadder:
            return "ladder";
        case SeriesKind::kRecovery:
            return "recovery";
    }
    return "?";
}

struct SeriesSpec {
    std::string id;
    SeriesKind kind = SeriesKind::kLadder;
    uint64_t size = 0;
    int parallel = 1;
    std::vector<int> rates;  // ладдер: документов в минуту по ступеням
    std::string base;  // возврат: id лестницы, чей темп проверяется
    int cycles = 0;    // волна: число циклов
};

std::string series_id(const char* kind, uint64_t size, int parallel) {
    return std::string{kind} + "-" + size_name(size) + "-p" + std::to_string(parallel);
}

std::vector<SeriesSpec> build_plan(const E5Options& e5) {
    const std::vector<int> ladder{5, 10, 15, 18, 20, 30, 60};
    std::vector<SeriesSpec> plan;
    if (e5.probe) {
        plan.push_back({series_id("wave", 16 * kMiB, 2),
                        SeriesKind::kWave,
                        16 * kMiB,
                        2,
                        {},
                        "",
                        e5.wave_cycles});
        plan.push_back({series_id("ladder", kKiB, 1), SeriesKind::kLadder, kKiB, 1, {5}, "", 0});
        plan.push_back(
            {series_id("ladder", 16 * kMiB, 1), SeriesKind::kLadder, 16 * kMiB, 1, {1}, "", 0});
        return plan;
    }
    for (const int p : {1, 2, 4}) {
        plan.push_back({series_id("wave", 16 * kMiB, p),
                        SeriesKind::kWave,
                        16 * kMiB,
                        p,
                        {},
                        "",
                        e5.wave_cycles});
    }
    plan.push_back({series_id("ladder", kKiB, 1), SeriesKind::kLadder, kKiB, 1, ladder, "", 0});
    plan.push_back({series_id("recovery", kKiB, 1),
                    SeriesKind::kRecovery,
                    kKiB,
                    1,
                    {},
                    series_id("ladder", kKiB, 1),
                    0});
    for (const uint64_t size : {kKiB, 16 * kMiB, 64 * kMiB}) {
        for (const int p : {1, 2, 4}) {
            if (size == kKiB && p == 1) {
                continue;  // уже в плане выше
            }
            plan.push_back(
                {series_id("ladder", size, p), SeriesKind::kLadder, size, p, ladder, "", 0});
        }
    }
    return plan;
}

// ---- Состояние серий ----------------------------------------------------------------------

class RunState {
  public:
    RunState(const BenchOptions& options, std::string name)
        : options_(options), name_(std::move(name)) {
        if (has_state(options_, name_)) {
            j_ = load_state(options_, name_);
            // Номера документов после возможного падения пропускаем: документы уникальны.
            j_["next_doc_no"] = j_.value("next_doc_no", int64_t{0}) + 1000;
        } else {
            j_ = td::Json{
                {"series", td::Json::object()}, {"quiet_until", 0}, {"hold_until", 0},
                {"last_flood_end", 0},          {"next_doc_no", 0}, {"created", iso_utc_now()}};
        }
        save();
    }

    td::Json& series(const std::string& id) { return j_["series"][id]; }
    [[nodiscard]] bool has_series(const std::string& id) const { return j_["series"].contains(id); }
    [[nodiscard]] bool is_done(const std::string& id) const {
        return has_series(id) && j_["series"][id].is_object() &&
               j_["series"][id].value("status", std::string{}) == "done";
    }
    td::Json& root() { return j_; }

    uint64_t take_doc_no() {
        const auto n = j_["next_doc_no"].get<int64_t>();
        j_["next_doc_no"] = n + 1;
        if (n % 50 == 0) {
            save();
        }
        return static_cast<uint64_t>(n);
    }

    void save() {
        j_["updated"] = iso_utc_now();
        save_state(options_, name_, j_);
    }

  private:
    const BenchOptions& options_;
    std::string name_;
    td::Json j_;
};

// ---- CSV ----------------------------------------------------------------------------------

std::vector<std::string> parse_csv_line(const std::string& line) {
    std::vector<std::string> out;
    std::string cur;
    bool quoted = false;
    for (size_t i = 0; i < line.size(); ++i) {
        const char c = line[i];
        if (quoted) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    cur += '"';
                    ++i;
                } else {
                    quoted = false;
                }
            } else {
                cur += c;
            }
        } else if (c == '"') {
            quoted = true;
        } else if (c == ',') {
            out.push_back(cur);
            cur.clear();
        } else {
            cur += c;
        }
    }
    out.push_back(cur);
    return out;
}

// Читает CSV с заголовком в вектор строк-словарей (для status и возобновления).
std::vector<std::map<std::string, std::string>> read_csv(const fs::path& path) {
    std::vector<std::map<std::string, std::string>> rows;
    std::ifstream in(path);
    std::string line;
    if (!std::getline(in, line)) {
        return rows;
    }
    const std::vector<std::string> header = parse_csv_line(line);
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const std::vector<std::string> fields = parse_csv_line(line);
        std::map<std::string, std::string> row;
        for (size_t i = 0; i < header.size() && i < fields.size(); ++i) {
            row[header[i]] = fields[i];
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

// ---- Ограничения Telegram вне ступеней (удаление, чтение) ---------------------------------

// Ожидание FLOOD_WAIT и повтор при сетевых ошибках для вызовов, которые не входят в измеряемые
// ступени (удаление, getMessages, getChat). Прочие ошибки возвращаются вызывающему.
class Guard {
  public:
    explicit Guard(const fs::path& out)
        : flood_(out / "e5_flood.csv",
                 {"time_utc", "series", "phase", "size_bytes", "parallel", "rate_per_min", "step",
                  "attempt", "offset_in_step_s", "retry_after_s", "wait_s", "launched_last_60s",
                  "completed_last_60s", "in_flight", "what"}),
          net_(out / "e5_net_errors.csv",
               {"time_utc", "series", "phase", "what", "error", "backoff_s"}) {}

    void set_series(const std::string& id) { series_ = id; }
    Csv& flood() { return flood_; }
    Csv& net() { return net_; }

    template <class F>
    auto run(const std::string& what, F&& f) -> decltype(f()) {
        for (int transient = 0;;) {
            if (g_stop.load()) {
                throw Interrupted();
            }
            try {
                return f();
            } catch (const td::TdException& e) {
                const td::TdError& error = e.error();
                if (error.code == 401 ||
                    error.message.find("USER_DEACTIVATED") != std::string::npos ||
                    error.message.find("AUTH_KEY") != std::string::npos) {
                    throw StopExperiment("account problem at '" + what + "': " + error.to_string());
                }
                if (error.code == td::TdError::kClosed || error.code == td::TdError::kStopped) {
                    throw SessionError("client closed at '" + what + "'");
                }
                if (error.is_rate_limit()) {
                    const int64_t s = error.retry_after ? error.retry_after->count() : 60;
                    const int64_t wait_s = s + s / 5 + 5;
                    flood_.row({iso_utc_now(), series_, "housekeeping", "", "", "", "", "", "",
                                std::to_string(s), std::to_string(wait_s), "", "", "", what});
                    if (s > kMaxFloodWaitSeconds) {
                        throw StopExperiment("FLOOD_WAIT of " + std::to_string(s) + " s at '" +
                                             what + "': stopping, see e5_flood.csv");
                    }
                    log_line("FLOOD_WAIT " + std::to_string(s) + " s at '" + what + "': waiting " +
                             std::to_string(wait_s) + " s");
                    sleep_interruptible(seconds{wait_s});
                } else if (error.is_local() || error.code >= 500) {
                    const int64_t backoff = std::min<int64_t>(300, 15LL << std::min(transient, 4));
                    ++transient;
                    net_.row({iso_utc_now(), series_, "housekeeping", what, error.to_string(),
                              std::to_string(backoff)});
                    log_line("network/server error at '" + what + "': " + error.to_string() +
                             "; retry in " + std::to_string(backoff) + " s");
                    sleep_interruptible(seconds{backoff});
                } else {
                    throw;
                }
            }
        }
    }

  private:
    Csv flood_;
    Csv net_;
    std::string series_;
};

// ---- Ступень ------------------------------------------------------------------------------

struct StepOutcome {
    std::string verdict;  // clean | flood | bandwidth_bound | disturbed | interrupted
    int launched = 0;
    int64_t flood_retry_s = 0;
    WallClock::time_point flood_at;
    double actual_rate = 0;  // запущено документов в минуту
};

struct StepStats {
    int planned = 0;
    int launched = 0;
    int ok = 0;
    int flood = 0;
    int net = 0;
    int timeouts = 0;
    int aborted = 0;
    uint64_t bytes_ok = 0;
    std::vector<double> speeds;     // МиБ/с одной загрузки
    std::vector<double> durations;  // с
    double slot_wait_s = 0;
    int64_t max_retry_s = 0;
    std::optional<WallClock::time_point> flood_at;
};

class Runner {
  public:
    Runner(BenchEnv& env, const BenchOptions& options, const E5Options& e5, const Names& names)
        : env_(env),
          options_(options),
          e5_(e5),
          names_(names),
          setup_(load_state(options, names.prefix)),
          chat_id_(setup_.at("chat_id").get<int64_t>()),
          state_(options, names.run_state),
          plan_(build_plan(e5)),
          guard_(names.out),
          uploads_(names.out / "e5_uploads.csv",
                   {"series", "attempt", "step", "rate_per_min", "size_bytes", "parallel", "doc_no",
                    "doc_index", "start_utc", "end_utc", "duration_s", "mib_per_s", "prepare_ms",
                    "in_flight_at_start", "in_flight_at_end", "result", "retry_after_s", "error",
                    "server_id"}),
          steps_(names.out / "e5_steps.csv", {"series",
                                              "kind",
                                              "size_bytes",
                                              "parallel",
                                              "step",
                                              "attempt",
                                              "rate_set_per_min",
                                              "start_utc",
                                              "end_utc",
                                              "window_s",
                                              "planned",
                                              "launched",
                                              "ok",
                                              "flood",
                                              "net_errors",
                                              "timeouts",
                                              "aborted",
                                              "actual_rate_per_min",
                                              "bytes_ok",
                                              "aggregate_mib_s",
                                              "speed_median_mib_s",
                                              "speed_min_mib_s",
                                              "speed_max_mib_s",
                                              "duration_median_s",
                                              "duration_max_s",
                                              "slot_wait_s",
                                              "flood_retry_s",
                                              "verdict"}),
          waves_(names.out / "e5_waves.csv",
                 {"series", "attempt", "cycle", "parallel", "docs", "ok", "bytes_ok", "wall_s",
                  "aggregate_mib_s", "speed_min_mib_s", "speed_max_mib_s", "verdict"}),
          series_csv_(names.out / "e5_series.csv",
                      {"series", "kind", "size_bytes", "parallel", "result", "max_clean_rate",
                       "flood_rate", "flood_retry_s", "bandwidth_bound_rate", "steps_run",
                       "started_utc", "ended_utc", "deleted", "note"}),
          progress_(names.out / "e5_progress.csv",
                    {"time_utc", "series", "step", "rate_per_min", "elapsed_s", "launched", "ok",
                     "flood", "errors", "in_flight", "actual_rate_per_min"}),
          deleted_csv_(names.out / "e5_deleted.csv",
                       {"time_utc", "series", "server_id", "result", "error"}) {
        if (!setup_.value("bot_added", false)) {
            throw std::runtime_error("run `e5 setup` first (the bot is not added yet)");
        }
        fs::create_directories(names.docs);
        load_pending();
    }

    // 0: всё готово или остановка по сигналу, 1: ошибка, 2: остановка, решает человек.
    int run() {
        log_line("e5 starts: " + std::to_string(plan_.size()) + " series in the plan, " +
                 std::to_string(pending_.size()) + " documents wait for deletion");
        int failures = 0;
        for (;;) {
            const auto before = progress_marker();
            try {
                run_session();
                finish();
                return 0;
            } catch (const Interrupted&) {
                log_line("interrupted by signal; state is saved, rerun `e5 run` to resume");
                close_session();
                state_.save();
                return 0;
            } catch (const StopExperiment& e) {
                log_line(std::string{"STOPPED: "} + e.what());
                close_session();
                state_.save();
                return 2;
            } catch (const td::TdException& e) {
                log_line("STOPPED on a non-retryable error: " + e.error().to_string());
                close_session();
                state_.save();
                return 2;
            } catch (const std::exception& e) {
                close_session();
                failures = progress_marker() > before ? 1 : failures + 1;
                log_line(std::string{"session error: "} + e.what() + " (failure " +
                         std::to_string(failures) + " of 10)");
                if (failures >= 10) {
                    state_.save();
                    return 1;
                }
                try {
                    sleep_interruptible(seconds{std::min(600, 60 * failures)});
                } catch (const Interrupted&) {
                    state_.save();
                    return 0;
                }
            }
        }
    }

  private:
    // Растёт с каждой завершённой ступенью или серией: отличает «сеанс падает на пустом месте»
    // от «сеанс упал после полезной работы».
    [[nodiscard]] int64_t progress_marker() const { return steps_done_; }

    void close_session() {
        load_.reset();
        bot_.reset();
    }

    void finish() {
        bump_counter(options_, "e5.bot_messages_sent", session_sends_);
        session_sends_ = 0;
        close_session();
        int done = 0;
        for (const auto& spec : plan_) {
            if (state_.is_done(spec.id)) {
                ++done;
            }
        }
        log_line("e5 done: " + std::to_string(done) + " of " + std::to_string(plan_.size()) +
                 " series finished");
    }

    void run_session() {
        bot_ = env_.open_at(Account::kBot, names_.session, std::cerr);
        std::string error;
        if (!wait_chat_known(bot_->client(), chat_id_, seconds{120}, &error)) {
            throw std::runtime_error("bot cannot get the channel: " + error);
        }
        sent_this_session_.clear();
        for (const auto& spec : plan_) {
            if (state_.is_done(spec.id)) {
                continue;
            }
            run_series(spec);
        }
    }

    // ---- серия ----

    static void wait_until_epoch(int64_t epoch, const std::string& why) {
        int64_t last_log = 0;
        while (epoch_now() < epoch) {
            if (g_stop.load()) {
                throw Interrupted();
            }
            if (epoch_now() - last_log >= 300) {
                last_log = epoch_now();
                log_line(why + ": " + std::to_string(epoch - epoch_now()) + " s left");
            }
            sleep_interruptible(seconds{std::min<int64_t>(epoch - epoch_now(), 30)});
        }
    }

    void run_series(const SeriesSpec& spec) {
        td::Json& st = state_.series(spec.id);
        guard_.set_series(spec.id);
        if (st.is_null()) {
            st =
                td::Json{{"status", "pending"}, {"next_step", 0}, {"attempts", td::Json::object()}};
        }
        std::vector<int> rates = spec.rates;
        if (spec.kind == SeriesKind::kRecovery && !st.contains("rates")) {
            const td::Json& base = state_.series(spec.base);
            const int sustained = base.value("max_clean_rate", 0);
            if (base.value("result", std::string{}) != "flood" || sustained <= 0) {
                st["status"] = "done";
                st["result"] = "skipped";
                st["note"] = "base series has no FLOOD_WAIT after a clean step";
                series_row(spec, st);
                state_.save();
                log_line("series " + spec.id + " skipped: " + st["note"].get<std::string>());
                return;
            }
            st["rates"] = td::Json::array({sustained});
        }
        if (st.contains("rates")) {
            rates = st["rates"].get<std::vector<int>>();
        }

        if (!st.value("finished", false)) {
            // Пауза между сериями (после удаления прошлой) и, для «возврата», 30 минут после
            // окончания последнего FLOOD_WAIT.
            int64_t until = state_.root().value("quiet_until", int64_t{0});
            if (spec.kind == SeriesKind::kRecovery) {
                until = std::max<int64_t>(until, state_.root().value("last_flood_end", int64_t{0}) +
                                                     e5_.recovery_wait_seconds);
            }
            wait_until_epoch(until, "pause before series " + spec.id);
            if (!st.contains("started")) {
                st["started"] = iso_utc_now();
            }
            st["status"] = "running";
            state_.save();
            log_line("series " + spec.id + " starts (" + kind_name(spec.kind) + ", size " +
                     size_name(spec.size) + ", parallel " + std::to_string(spec.parallel) + ")");
            SendLoad::Options lo;
            lo.parallelism = spec.parallel;
            load_ = std::make_unique<SendLoad>(*bot_, chat_id_, lo);
            if (spec.kind == SeriesKind::kWave) {
                run_wave_series(spec, st);
            } else {
                run_ladder_series(spec, st, rates);
            }
            load_.reset();
            st["finished"] = true;
            state_.save();
        }
        finalize_series(spec, st);
    }

    void run_ladder_series(const SeriesSpec& spec, td::Json& st, const std::vector<int>& rates) {
        for (auto i = static_cast<size_t>(st.value("next_step", 0)); i < rates.size(); ++i) {
            StepOutcome out;
            for (;;) {
                td::Json& attempts = st["attempts"][std::to_string(i)];
                const int attempt = attempts.is_null() ? 1 : attempts.get<int>() + 1;
                attempts = attempt;
                state_.save();
                out = run_step(spec, static_cast<int>(i), rates[i], attempt);
                if (out.verdict == "disturbed" && attempt < kMaxStepAttempts) {
                    log_line("step " + std::to_string(rates[i]) +
                             "/min was disturbed (network?), repeating it");
                    continue;
                }
                break;
            }
            st["steps_run"] = st.value("steps_run", 0) + 1;
            if (out.verdict == "clean") {
                st["max_clean_rate"] = rates[i];
                st["next_step"] = i + 1;
                state_.save();
                continue;
            }
            if (out.verdict == "flood") {
                end_by_flood(st, rates[i], out);
            } else if (out.verdict == "bandwidth_bound") {
                st["result"] = "bandwidth_bound";
                st["bandwidth_bound_rate"] = rates[i];
                st["bandwidth_bound_actual"] = out.actual_rate;
            } else {
                st["result"] = "disturbed";
                st["note"] = "step " + std::to_string(rates[i]) + "/min disturbed " +
                             std::to_string(kMaxStepAttempts) + " times";
            }
            return;
        }
        st["result"] = "completed";
    }

    void end_by_flood(td::Json& st, int rate, const StepOutcome& out) {
        st["result"] = "flood";
        st["flood_rate"] = rate;
        st["flood_retry_s"] = out.flood_retry_s;
        const int64_t flood_epoch =
            std::chrono::duration_cast<seconds>(out.flood_at.time_since_epoch()).count();
        state_.root()["last_flood_end"] = flood_epoch + out.flood_retry_s;
        // Выждать с запасом (20 % и 5 с), как FloodGuard; удаление и следующая серия после этого.
        state_.root()["hold_until"] = flood_epoch + out.flood_retry_s + out.flood_retry_s / 5 + 5;
        state_.save();
    }

    void finalize_series(const SeriesSpec& spec, td::Json& st) {
        wait_until_epoch(state_.root().value("hold_until", int64_t{0}),
                         "waiting out FLOOD_WAIT of " + spec.id);
        const int deleted = delete_pending(spec.id);
        st["status"] = "done";
        st["ended"] = iso_utc_now();
        state_.root()["quiet_until"] = epoch_now() + e5_.series_pause_seconds;
        series_row(spec, st, deleted);
        bump_counter(options_, "e5.bot_messages_sent", session_sends_);
        session_sends_ = 0;
        state_.save();
        log_line("series " + spec.id + " done: " + st.value("result", std::string{"?"}) +
                 ", max clean rate " + std::to_string(st.value("max_clean_rate", 0)) +
                 "/min, deleted " + std::to_string(deleted));
    }

    void series_row(const SeriesSpec& spec, const td::Json& st, int deleted = 0) {
        series_csv_.row(
            {spec.id, kind_name(spec.kind), std::to_string(spec.size),
             std::to_string(spec.parallel), st.value("result", std::string{"?"}),
             st.contains("max_clean_rate") ? std::to_string(st["max_clean_rate"].get<int>()) : "",
             st.contains("flood_rate") ? std::to_string(st["flood_rate"].get<int>()) : "",
             st.contains("flood_retry_s") ? std::to_string(st["flood_retry_s"].get<int64_t>()) : "",
             st.contains("bandwidth_bound_rate")
                 ? std::to_string(st["bandwidth_bound_rate"].get<int>())
                 : "",
             std::to_string(st.value("steps_run", 0)), st.value("started", std::string{}),
             iso_utc_now(), std::to_string(deleted), st.value("note", std::string{})});
    }

    // ---- общий разбор итогов ----

    struct StepContext {
        const SeriesSpec* spec = nullptr;
        int step = 0;
        int rate = 0;
        int attempt = 0;
        StepStats stats;
        std::deque<WallClock::time_point> launches;
        std::deque<WallClock::time_point> completions;
        WallClock::time_point start_wall;
        ExpClock::time_point t0;
        std::optional<int> in_flight_at_flood;
        std::string flood_what;
    };

    static int count_since(const std::deque<WallClock::time_point>& times, WallClock::time_point t,
                           double window_s) {
        return static_cast<int>(std::ranges::count_if(times, [&](WallClock::time_point x) {
            const double d = seconds_between(x, t);
            return d >= 0 && d <= window_s;
        }));
    }

    // Забирает завершённые задания, пишет строки загрузок, обновляет счётчики ступени. Бросает
    // StopExperiment для ошибок аккаунта и других неповторяемых ошибок.
    void collect(StepContext& ctx) {
        for (const SendResult& r : load_->take_finished()) {
            consume(ctx, r);
        }
    }

    void consume(StepContext& ctx, const SendResult& r) {
        StepStats& s = ctx.stats;
        for (size_t i = 0; i < r.docs.size(); ++i) {
            const DocOutcome& d = r.docs[i];
            const double speed = d.kind == SendKind::kOk && d.done_ms > 0
                                     ? static_cast<double>(r.sizes[i]) / static_cast<double>(kMiB) /
                                           (d.done_ms / 1000.0)
                                     : 0;
            const int64_t sid = d.message_id != 0 ? server_id(d.message_id) : 0;
            uploads_.row({ctx.spec->id, std::to_string(ctx.attempt), std::to_string(ctx.step),
                          std::to_string(ctx.rate), std::to_string(r.sizes[i]),
                          std::to_string(ctx.spec->parallel), std::to_string(r.job.id),
                          std::to_string(i), iso_utc_ms(r.start_wall), iso_utc_ms(r.end_wall),
                          fmt(d.done_ms / 1000.0, 3), d.kind == SendKind::kOk ? fmt(speed, 2) : "",
                          fmt(r.prepare_ms, 0), std::to_string(r.in_flight_at_start),
                          std::to_string(r.in_flight_at_end), send_kind_name(d.kind),
                          d.retry_after_s != 0 ? std::to_string(d.retry_after_s) : "", d.error,
                          sid != 0 ? std::to_string(sid) : ""});
            switch (d.kind) {
                case SendKind::kOk:
                    ++s.ok;
                    ++session_sends_;
                    s.bytes_ok += r.sizes[i];
                    s.speeds.push_back(speed);
                    s.durations.push_back(d.done_ms / 1000.0);
                    ctx.completions.push_back(r.end_wall);
                    pending_.insert(sid);
                    sent_this_session_.insert(sid);
                    break;
                case SendKind::kFlood: {
                    ++s.flood;
                    s.max_retry_s = std::max(s.max_retry_s, d.retry_after_s);
                    if (!s.flood_at) {
                        s.flood_at = r.end_wall;
                        record_flood(ctx, r, d);
                    }
                    break;
                }
                case SendKind::kNet:
                case SendKind::kTimeout: {
                    ++(d.kind == SendKind::kNet ? s.net : s.timeouts);
                    guard_.net().row({iso_utc_ms(r.end_wall), ctx.spec->id, "step",
                                      send_kind_name(d.kind), d.error, ""});
                    log_line(std::string{"upload "} + send_kind_name(d.kind) + ": " + d.error);
                    break;
                }
                case SendKind::kAborted:
                    ++s.aborted;
                    if (!g_stop.load()) {
                        throw SessionError("client closed during a send: " + d.error);
                    }
                    break;
                case SendKind::kAccount:
                    throw StopExperiment("account problem on a send: " + d.error);
                case SendKind::kFatal:
                    throw StopExperiment("non-retryable error on a send: " + d.error);
            }
        }
    }

    void record_flood(const StepContext& ctx, const SendResult& r, const DocOutcome& d) {
        const double offset = seconds_between(ctx.start_wall, r.end_wall);
        guard_.flood().row(
            {iso_utc_ms(r.end_wall), ctx.spec->id, "step", std::to_string(ctx.spec->size),
             std::to_string(ctx.spec->parallel), std::to_string(ctx.rate), std::to_string(ctx.step),
             std::to_string(ctx.attempt), fmt(offset, 1), std::to_string(d.retry_after_s),
             std::to_string(d.retry_after_s + d.retry_after_s / 5 + 5),
             std::to_string(count_since(ctx.launches, r.end_wall, 60)),
             std::to_string(count_since(ctx.completions, r.end_wall, 60)),
             std::to_string(r.in_flight_at_end), "send " + d.error});
        log_line("FLOOD_WAIT " + std::to_string(d.retry_after_s) + " s at " +
                 std::to_string(ctx.rate) + "/min after " + fmt(offset, 0) + " s (" +
                 std::to_string(count_since(ctx.launches, r.end_wall, 60)) +
                 " launches in the last minute)");
    }

    // Ждёт завершения всех идущих заданий. После сигнала даёт им 2 минуты, потом прерывает.
    void drain(StepContext& ctx) {
        std::optional<ExpClock::time_point> abort_at;
        while (load_->in_flight() > 0 || load_->wait_event(milliseconds{0})) {
            if (g_stop.load()) {
                if (!abort_at) {
                    abort_at = ExpClock::now() + seconds{120};
                } else if (ExpClock::now() > *abort_at) {
                    load_->abort();
                }
            }
            load_->wait_event(milliseconds{200});
            collect(ctx);
        }
        collect(ctx);
    }

    void progress_tick(const StepContext& ctx, double elapsed_s) {
        const StepStats& s = ctx.stats;
        const double rate = elapsed_s > 0 ? s.launched * 60.0 / elapsed_s : 0;
        const Summary sp = summarize(s.speeds);
        log_line("[" + ctx.spec->id + "] step " + std::to_string(ctx.step) + " (" +
                 std::to_string(ctx.rate) + "/min, attempt " + std::to_string(ctx.attempt) +
                 "): " + fmt(elapsed_s, 0) + " s, launched " + std::to_string(s.launched) +
                 ", ok " + std::to_string(s.ok) + ", FLOOD " + std::to_string(s.flood) +
                 ", errors " + std::to_string(s.net + s.timeouts) + ", in flight " +
                 std::to_string(load_->in_flight()) + ", " + fmt(rate, 1) + "/min, upload " +
                 (s.speeds.empty() ? std::string{"-"} : fmt(sp.median, 1) + " MiB/s median"));
        progress_.row({iso_utc_now(), ctx.spec->id, std::to_string(ctx.step),
                       std::to_string(ctx.rate), fmt(elapsed_s, 0), std::to_string(s.launched),
                       std::to_string(s.ok), std::to_string(s.flood),
                       std::to_string(s.net + s.timeouts), std::to_string(load_->in_flight()),
                       fmt(rate, 2)});
    }

    SendJob make_job(const SeriesSpec& spec, uint64_t doc_no) {
        SendJob job;
        job.id = doc_no;
        const uint64_t size = spec.size;
        const fs::path dir = names_.docs;
        const uint64_t seed = kSeed ^ (doc_no * 0x9E3779B97F4A7C15ULL);
        job.prepare = [dir, seed, size] {
            return std::vector<fs::path>{make_seeded_document(dir, seed, size)};
        };
        return job;
    }

    // ---- ступень лестницы ----

    StepOutcome run_step(const SeriesSpec& spec, int step, int rate, int attempt) {
        StepContext ctx;
        ctx.spec = &spec;
        ctx.step = step;
        ctx.rate = rate;
        ctx.attempt = attempt;
        const double interval_s = 60.0 / rate;
        const double window_s = e5_.step_seconds;
        ctx.stats.planned = static_cast<int>(std::ceil(window_s / interval_s - 1e-9));
        ctx.start_wall = WallClock::now();
        ctx.t0 = ExpClock::now();
        log_line("[" + spec.id + "] step " + std::to_string(step) + ": " + std::to_string(rate) +
                 " docs/min for " + std::to_string(e5_.step_seconds) + " s (attempt " +
                 std::to_string(attempt) + ")");

        const auto interval = std::chrono::duration_cast<ExpClock::duration>(
            std::chrono::duration<double>(interval_s));
        const auto window =
            std::chrono::duration_cast<ExpClock::duration>(std::chrono::duration<double>(window_s));
        ExpClock::time_point next_due = ctx.t0;
        ExpClock::time_point last_tick = ctx.t0;
        bool blocked = false;
        bool early_bound = false;
        StepStats& s = ctx.stats;
        std::string verdict;

        for (;;) {
            collect(ctx);
            if (g_stop.load()) {
                verdict = "interrupted";
                break;
            }
            if (s.flood > 0) {
                break;
            }
            const auto now = ExpClock::now();
            const double elapsed_s = std::chrono::duration<double>(now - ctx.t0).count();
            if (now - last_tick >= seconds{60}) {
                last_tick = now;
                progress_tick(ctx, elapsed_s);
            }
            if (now - ctx.t0 >= window) {
                break;
            }
            // Ранний выход: упёрлись в канал связи, ждать все 10 минут незачем.
            if (elapsed_s >= 180 && s.slot_wait_s >= 0.05 * elapsed_s &&
                s.launched < kBandwidthShare * (std::floor(elapsed_s / interval_s) + 1)) {
                early_bound = true;
                break;
            }
            if (next_due - ctx.t0 < window && now >= next_due) {
                if (!load_->has_free_slot()) {
                    blocked = true;
                    load_->wait_event(milliseconds{100});
                    continue;
                }
                const double late_s = std::chrono::duration<double>(now - next_due).count();
                if (blocked) {
                    s.slot_wait_s += late_s;
                    blocked = false;
                }
                ctx.launches.push_back(WallClock::now());
                ++s.launched;
                load_->launch(make_job(spec, state_.take_doc_no()));
                next_due = now - next_due > interval ? now + interval : next_due + interval;
            } else {
                const auto target = next_due - ctx.t0 < window ? next_due : ctx.t0 + window;
                const auto wait = std::min<ExpClock::duration>(target - now, milliseconds{200});
                load_->wait_event(std::max<milliseconds>(
                    std::chrono::duration_cast<milliseconds>(wait), milliseconds{1}));
            }
        }
        drain(ctx);
        const WallClock::time_point end_wall = WallClock::now();
        const double total_s = std::chrono::duration<double>(ExpClock::now() - ctx.t0).count();

        StepOutcome out;
        out.launched = s.launched;
        out.actual_rate = total_s > 0 ? s.launched * 60.0 / std::min(total_s, window_s) : 0;
        if (verdict.empty()) {
            if (s.flood > 0) {
                verdict = "flood";
                out.flood_retry_s = s.max_retry_s;
                out.flood_at = s.flood_at.value_or(WallClock::now());
            } else if (early_bound ||
                       s.launched < kBandwidthShare * static_cast<double>(s.planned)) {
                verdict = s.slot_wait_s >= 0.05 * std::min(total_s, window_s) ? "bandwidth_bound"
                                                                              : "disturbed";
            } else {
                verdict = "clean";
            }
        }
        out.verdict = verdict;
        write_step_row(ctx, end_wall, total_s, verdict, out);
        ++steps_done_;
        log_line("[" + spec.id + "] step " + std::to_string(step) + " (" + std::to_string(rate) +
                 "/min) ended: " + verdict + ", launched " + std::to_string(s.launched) + "/" +
                 std::to_string(s.planned) + ", ok " + std::to_string(s.ok));
        if (verdict == "interrupted") {
            throw Interrupted();
        }
        if (verdict == "flood" && out.flood_retry_s > kMaxFloodWaitSeconds) {
            throw StopExperiment("FLOOD_WAIT of " + std::to_string(out.flood_retry_s) +
                                 " s: stopping, see e5_flood.csv");
        }
        return out;
    }

    void write_step_row(const StepContext& ctx, WallClock::time_point end_wall, double total_s,
                        const std::string& verdict, const StepOutcome& out) {
        const StepStats& s = ctx.stats;
        const Summary sp = summarize(s.speeds);
        const Summary du = summarize(s.durations);
        const double agg =
            total_s > 0 ? static_cast<double>(s.bytes_ok) / static_cast<double>(kMiB) / total_s : 0;
        steps_.row({ctx.spec->id,
                    kind_name(ctx.spec->kind),
                    std::to_string(ctx.spec->size),
                    std::to_string(ctx.spec->parallel),
                    std::to_string(ctx.step),
                    std::to_string(ctx.attempt),
                    std::to_string(ctx.rate),
                    iso_utc_ms(ctx.start_wall),
                    iso_utc_ms(end_wall),
                    fmt(total_s, 1),
                    std::to_string(s.planned),
                    std::to_string(s.launched),
                    std::to_string(s.ok),
                    std::to_string(s.flood),
                    std::to_string(s.net),
                    std::to_string(s.timeouts),
                    std::to_string(s.aborted),
                    fmt(out.actual_rate, 2),
                    std::to_string(s.bytes_ok),
                    fmt(agg, 2),
                    fmt(sp.median, 2),
                    fmt(sp.min, 2),
                    fmt(sp.max, 2),
                    fmt(du.median, 2),
                    fmt(du.max, 2),
                    fmt(s.slot_wait_s, 1),
                    std::to_string(s.max_retry_s),
                    verdict});
    }

    // ---- волны: P документов сразу ----

    void run_wave_series(const SeriesSpec& spec, td::Json& st) {
        const int attempt = st["attempts"].value("wave", 0) + 1;
        st["attempts"]["wave"] = attempt;
        state_.save();
        StepContext ctx;
        ctx.spec = &spec;
        ctx.step = 0;
        ctx.rate = spec.parallel * 60 / e5_.wave_period_seconds;
        ctx.attempt = attempt;
        ctx.stats.planned = spec.cycles * spec.parallel;
        ctx.start_wall = WallClock::now();
        ctx.t0 = ExpClock::now();
        std::string verdict = "clean";
        StepStats& s = ctx.stats;
        for (int cycle = 1; cycle <= spec.cycles; ++cycle) {
            const auto cycle_start = ExpClock::now();
            std::vector<fs::path> files;
            files.reserve(static_cast<size_t>(spec.parallel));
            for (int k = 0; k < spec.parallel; ++k) {
                files.push_back(make_seeded_document(
                    names_.docs, kSeed ^ (state_.take_doc_no() * 0x9E3779B97F4A7C15ULL),
                    spec.size));
            }
            const int ok_before = s.ok;
            const uint64_t bytes_before = s.bytes_ok;
            const size_t speeds_before = s.speeds.size();
            const auto t_launch = ExpClock::now();
            for (const auto& f : files) {
                SendJob job;
                job.id = state_.take_doc_no();
                job.files = {f};
                ctx.launches.push_back(WallClock::now());
                ++s.launched;
                load_->launch(std::move(job));
            }
            drain(ctx);
            const double wall_s = std::chrono::duration<double>(ExpClock::now() - t_launch).count();
            const int ok = s.ok - ok_before;
            const uint64_t bytes = s.bytes_ok - bytes_before;
            const std::vector<double> sp(s.speeds.begin() + static_cast<long>(speeds_before),
                                         s.speeds.end());
            const Summary sm = summarize(sp);
            const bool flood = s.flood > 0;
            waves_.row({spec.id, std::to_string(attempt), std::to_string(cycle),
                        std::to_string(spec.parallel), std::to_string(spec.parallel),
                        std::to_string(ok), std::to_string(bytes), fmt(wall_s, 2),
                        fmt(wall_s > 0 ? static_cast<double>(bytes) / kMiB / wall_s : 0, 2),
                        fmt(sm.min, 2), fmt(sm.max, 2), flood ? "flood" : "ok"});
            if (g_stop.load()) {
                verdict = "interrupted";
                break;
            }
            if (flood) {
                verdict = "flood";
                break;
            }
            if (cycle == 1 || cycle % 5 == 0) {
                progress_tick(ctx, std::chrono::duration<double>(ExpClock::now() - ctx.t0).count());
            }
            const auto next = cycle_start + seconds{e5_.wave_period_seconds};
            if (ExpClock::now() < next) {
                sleep_interruptible(
                    std::chrono::duration_cast<milliseconds>(next - ExpClock::now()));
            }
        }
        const WallClock::time_point end_wall = WallClock::now();
        const double total_s = std::chrono::duration<double>(ExpClock::now() - ctx.t0).count();
        StepOutcome out;
        out.verdict = verdict;
        out.launched = s.launched;
        out.actual_rate = total_s > 0 ? s.launched * 60.0 / total_s : 0;
        if (verdict == "flood") {
            out.flood_retry_s = s.max_retry_s;
            out.flood_at = s.flood_at.value_or(WallClock::now());
        }
        write_step_row(ctx, end_wall, total_s, verdict, out);
        ++steps_done_;
        st["steps_run"] = 1;
        if (verdict == "interrupted") {
            throw Interrupted();
        }
        if (verdict == "flood") {
            end_by_flood(st, ctx.rate, out);
            if (out.flood_retry_s > kMaxFloodWaitSeconds) {
                throw StopExperiment("FLOOD_WAIT of " + std::to_string(out.flood_retry_s) +
                                     " s: stopping, see e5_flood.csv");
            }
        } else {
            st["result"] = "completed";
            st["max_clean_rate"] = ctx.rate;
        }
    }

    // ---- удаление ----

    void load_pending() {
        std::set<int64_t> sent;
        for (const auto& row : read_csv(names_.out / "e5_uploads.csv")) {
            if (row.at("result") == "ok" && !row.at("server_id").empty()) {
                sent.insert(std::stoll(row.at("server_id")));
            }
        }
        for (const auto& row : read_csv(names_.out / "e5_deleted.csv")) {
            if (row.at("result") == "ok") {
                sent.erase(std::stoll(row.at("server_id")));
            }
        }
        pending_ = std::move(sent);
    }

    // Удаляет документы ботом пачками по 100, не чаще 10 пачек в минуту. Возвращает число
    // удалённых. Неудаляемое остаётся в pending_ (канал в конце удаляется целиком).
    int delete_pending(const std::string& series) {
        guard_.set_series(series);
        const std::vector<int64_t> ids(pending_.begin(), pending_.end());
        int deleted = 0;
        std::optional<ExpClock::time_point> last_action;
        const auto pace = [&] {
            if (last_action) {
                const auto next = *last_action + milliseconds{e5_.delete_interval_ms};
                if (ExpClock::now() < next) {
                    sleep_interruptible(
                        std::chrono::duration_cast<milliseconds>(next - ExpClock::now()));
                }
            }
            last_action = ExpClock::now();
        };
        for (size_t i = 0; i < ids.size(); i += kDeleteBatch) {
            const std::vector<int64_t> batch(
                ids.begin() + static_cast<long>(i),
                ids.begin() + static_cast<long>(std::min(ids.size(), i + kDeleteBatch)));
            td::Json message_ids = td::Json::array();
            bool known = true;
            for (const int64_t sid : batch) {
                message_ids.push_back(sid << kServerIdShift);
                known = known && sent_this_session_.contains(sid);
            }
            const td::Json request{{"@type", "deleteMessages"},
                                   {"chat_id", chat_id_},
                                   {"message_ids", message_ids},
                                   {"revoke", true}};
            try {
                if (!known) {
                    // После перезапуска процесса TDLib не знает эти сообщения (E2, E3).
                    pace();
                    guard_.run("getMessages", [&] {
                        return bot_->client().call(td::Json{{"@type", "getMessages"},
                                                            {"chat_id", chat_id_},
                                                            {"message_ids", message_ids}},
                                                   seconds{60});
                    });
                }
                pace();
                guard_.run("deleteMessages",
                           [&] { return bot_->client().call(request, seconds{60}); });
                for (const int64_t sid : batch) {
                    deleted_csv_.row({iso_utc_now(), series, std::to_string(sid), "ok", ""});
                    pending_.erase(sid);
                }
                deleted += static_cast<int>(batch.size());
            } catch (const td::TdException& e) {
                for (const int64_t sid : batch) {
                    deleted_csv_.row({iso_utc_now(), series, std::to_string(sid), "failed",
                                      e.error().to_string()});
                }
                log_line("deleteMessages failed for a batch: " + e.error().to_string());
            }
        }
        log_line("deleted " + std::to_string(deleted) + " of " + std::to_string(ids.size()) +
                 " documents after " + series);
        return deleted;
    }

    BenchEnv& env_;
    const BenchOptions& options_;
    const E5Options& e5_;
    const Names& names_;
    td::Json setup_;
    int64_t chat_id_;
    RunState state_;
    std::vector<SeriesSpec> plan_;
    Guard guard_;
    Csv uploads_;
    Csv steps_;
    Csv waves_;
    Csv series_csv_;
    Csv progress_;
    Csv deleted_csv_;

    std::unique_ptr<BenchAccount> bot_;
    std::unique_ptr<SendLoad> load_;
    std::set<int64_t> pending_;  // серверные id доставленного и ещё не удалённого
    std::set<int64_t> sent_this_session_;
    int64_t session_sends_ = 0;
    int64_t steps_done_ = 0;
};

// ---- setup / status / teardown ------------------------------------------------------------

void cmd_setup(BenchEnv& env, const BenchOptions& options, const E5Options& e5) {
    const Names names = names_for(options, e5);
    const SessionLock lock(names.lock);
    td::Json state;
    if (has_state(options, names.prefix)) {
        state = load_state(options, names.prefix);
        if (state.value("bot_added", false)) {
            throw std::runtime_error("channel e5 already set up; run teardown first");
        }
        log_line("resuming setup: the channel exists");
    }
    FloodGuard guard(std::cerr, names.out / "e5_setup_flood.csv", "e5-setup");
    auto user = env.open(Account::kUser, std::cerr);
    auto bot = env.open_at(Account::kBot, names.session, std::cerr);
    if (state.is_null()) {
        const std::string title = "tgfs-bench-e5-" + date_stamp();
        const int64_t chat_id = create_channel(*user, title, guard, std::cerr);
        bump_counter(options, "e5.user_actions", 1);
        state = td::Json{{"chat_id", chat_id},
                         {"title", title},
                         {"created", iso_utc_now()},
                         {"bot_added", false}};
        save_state(options, names.prefix, state);
    }
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    std::string error;
    wait_chat_known(user->client(), chat_id, seconds{60}, &error);
    add_bot_admin(*user, *bot, chat_id, e5.rights, guard, std::cerr);
    bump_counter(options, "e5.user_actions", 2);
    const auto known = wait_chat_known(bot->client(), chat_id, seconds{90}, &error);
    log_line("bot knows the channel: " + (known ? fmt(*known) + " ms" : "NO (" + error + ")"));
    state["bot_added"] = true;
    state["rights"] = e5.rights;
    state["bot_added_at"] = iso_utc_now();
    save_state(options, names.prefix, state);
    std::cout << "setup done: channel created, bot is administrator (" << e5.rights
              << "), the user sent no messages\n";
}

void cmd_status(const BenchOptions& options, const E5Options& e5) {
    const Names names = names_for(options, e5);
    if (has_state(options, names.prefix)) {
        const td::Json state = load_state(options, names.prefix);
        std::cout << "channel: " << state.value("title", std::string{"?"})
                  << ", bot_added=" << state.value("bot_added", false) << "\n";
    } else {
        std::cout << "no channel (run `e5 setup`)\n";
        return;
    }
    if (has_state(options, names.run_state)) {
        const td::Json run = load_state(options, names.run_state);
        const int64_t now = epoch_now();
        std::cout << "run state updated: " << run.value("updated", std::string{"-"})
                  << ", quiet for " << std::max<int64_t>(0, run.value("quiet_until", 0) - now)
                  << " s more, next doc no " << run.value("next_doc_no", 0) << "\n";
        for (const auto& [id, st] : run["series"].items()) {
            if (!st.is_object()) {
                continue;
            }
            std::cout << "  " << id << ": " << st.value("status", std::string{"?"});
            if (st.contains("result")) {
                std::cout << " -> " << st["result"].get<std::string>();
            }
            if (st.contains("max_clean_rate")) {
                std::cout << ", max clean " << st["max_clean_rate"].get<int>() << "/min";
            }
            if (st.contains("flood_rate")) {
                std::cout << ", FLOOD at " << st["flood_rate"].get<int>() << "/min ("
                          << st.value("flood_retry_s", int64_t{0}) << " s)";
            }
            if (st.value("status", std::string{}) == "running") {
                std::cout << ", next step " << st.value("next_step", 0);
            }
            std::cout << "\n";
        }
    } else {
        std::cout << "run has not started\n";
    }
    int64_t ok = 0;
    int64_t other = 0;
    for (const auto& row : read_csv(names.out / "e5_uploads.csv")) {
        ++(row.at("result") == "ok" ? ok : other);
    }
    int64_t deleted = 0;
    for (const auto& row : read_csv(names.out / "e5_deleted.csv")) {
        deleted += row.at("result") == "ok" ? 1 : 0;
    }
    std::cout << "uploads: " << ok << " ok, " << other << " not ok; deleted: " << deleted << "\n"
              << "FLOOD_WAIT events: " << read_csv(names.out / "e5_flood.csv").size() << "\n";
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E5Options& e5) {
    const Names names = names_for(options, e5);
    const SessionLock lock(names.lock);
    const td::Json state = load_state(options, names.prefix);
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    FloodGuard guard(std::cerr, names.out / "e5_setup_flood.csv", "e5-teardown");
    auto user = env.open(Account::kUser, std::cerr);
    std::string error;
    wait_chat_known(user->client(), chat_id, seconds{30}, &error);
    // Право удалить канал приходит с полной информацией о нём (E3).
    try {
        const td::Json chat =
            user->client().call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
        user->client().call(
            td::Json{{"@type", "getSupergroupFullInfo"},
                     {"supergroup_id", chat["type"].value("supergroup_id", int64_t{0})}});
    } catch (const td::TdException& e) {
        std::cerr << "getSupergroupFullInfo: " << e.error().to_string() << "\n";
    }
    for (int attempt_no = 1;; ++attempt_no) {
        try {
            guard.run("deleteChat", [&] {
                return user->client().call(td::Json{{"@type", "deleteChat"}, {"chat_id", chat_id}});
            });
            break;
        } catch (const td::TdException& e) {
            if (attempt_no >= 5 || e.error().code != 400) {
                throw;
            }
            std::this_thread::sleep_for(seconds{3});
            wait_chat_known(user->client(), chat_id, seconds{20}, &error);
        }
    }
    bump_counter(options, "e5.user_actions", 1);
    user.reset();
    remove_fresh_bot_dir(names.session);
    remove_fresh_bot_dir(names.docs);
    fs::remove(options.state_dir / (names.prefix + ".json"));
    fs::remove(options.state_dir / "e5-run.json");
    fs::remove(options.state_dir / "e5p-run.json");
    std::cout << "channel deleted, bot session directory removed\n";
}

}  // namespace

void add_e5_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("e5", "Experiment E5: send limits in bot mode (SPEC 17.8)");
    cmd->require_subcommand(1);
    auto e5 = std::make_shared<E5Options>();
    e5->out = default_out_dir("05-e5-send-limits");
    cmd->add_flag("--probe", e5->probe, "Trial run: short plan, data in probe/");
    cmd->add_option("--out", e5->out, "Directory for CSV data")->capture_default_str();
    cmd->add_option("--rights", e5->rights, "Bot rights set")->capture_default_str();
    auto* step_opt = cmd->add_option("--step-seconds", e5->step_seconds, "Length of one step")
                         ->capture_default_str();
    auto* pause_opt = cmd->add_option("--series-pause-seconds", e5->series_pause_seconds,
                                      "Pause between series, after deletion")
                          ->capture_default_str();
    cmd->add_option("--recovery-wait-seconds", e5->recovery_wait_seconds,
                    "Recovery series: wait after the FLOOD_WAIT ends")
        ->capture_default_str();
    auto* cycles_opt = cmd->add_option("--wave-cycles", e5->wave_cycles, "Cycles of a wave series")
                           ->capture_default_str();
    cmd->add_option("--wave-period-seconds", e5->wave_period_seconds, "Period of a wave cycle")
        ->capture_default_str();
    cmd->add_option("--delete-interval-ms", e5->delete_interval_ms,
                    "Minimal interval between delete batches")
        ->capture_default_str();

    // Значения пробного прогона применяются, если опция не задана явно.
    const auto apply_probe = [e5, step_opt, pause_opt, cycles_opt] {
        if (!e5->probe) {
            return;
        }
        if (step_opt->count() == 0) {
            e5->step_seconds = 120;
        }
        if (pause_opt->count() == 0) {
            e5->series_pause_seconds = 20;
        }
        if (cycles_opt->count() == 0) {
            e5->wave_cycles = 2;
        }
    };

    cmd->add_subcommand("setup", "User creates the channel and adds the bot")
        ->callback([&options, exit_code, e5, apply_probe] {
            apply_probe();
            install_signal_handlers();
            BenchEnv env(options);
            cmd_setup(env, options, *e5);
            *exit_code = 0;
        });
    cmd->add_subcommand("run", "Bot runs the series (resumable, run it in the background)")
        ->callback([&options, exit_code, e5, apply_probe] {
            apply_probe();
            install_signal_handlers();
            BenchOptions own = options;
            own.tdlib_log = options.state_dir / "tdlib-e5.log";
            BenchEnv env(own);
            const Names names = names_for(own, *e5);
            const SessionLock lock(names.lock);
            Runner runner(env, own, *e5, names);
            *exit_code = runner.run();
        });
    cmd->add_subcommand("status", "State of the series and counters")
        ->callback([&options, exit_code, e5, apply_probe] {
            apply_probe();
            cmd_status(options, *e5);
            *exit_code = 0;
        });
    cmd->add_subcommand("teardown", "Delete the channel (user account)")
        ->callback([&options, exit_code, e5, apply_probe] {
            apply_probe();
            install_signal_handlers();
            BenchEnv env(options);
            cmd_teardown(env, options, *e5);
            *exit_code = 0;
        });
}

}  // namespace telega::bench
