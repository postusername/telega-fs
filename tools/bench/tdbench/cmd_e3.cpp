// e3: чтение старых сообщений по id в режиме бота (SPEC 17.6). Канал создаёт пользователь,
// до добавления бота отправляет в него короткие тексты; потом бот (отдельный каталог сессии
// <state-dir>/bot-e3) долго, не быстрее лимитов, доводит канал до 10 000 id, а затем перебирает
// все id через getMessages и сверяет ответы со списком отправленного и удалённого (ledger).
//
//   tgfs-bench e3 [--probe] setup      создать канал, 25 текстов от пользователя, добавить бота
//   tgfs-bench e3 [--probe] fill       заполнение: возобновляемо, переживает обрывы и FLOOD_WAIT
//   tgfs-bench e3 [--probe] scan       перебор id 1..последний+100 (--fresh: с пустой БД бота)
//   tgfs-bench e3 [--probe] status     что записано в ledger
//   tgfs-bench e3 [--probe] teardown   удалить канал (пользователь) и каталоги сессии бота
//
// --probe: пробный прогон на отдельном канале (состояние e3p, сессия bot-e3p, данные в probe/).
// Всё, что нужно для отчёта, пишется в CSV рядом с остальными данными эксперимента. Id канала
// и пользователя лежат только в <state-dir>/e3.json, в CSV их нет.

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <ranges>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "commands.h"
#include "exp_common.h"

namespace telega::bench {

namespace {

namespace fs = std::filesystem;
using std::chrono::milliseconds;
using std::chrono::seconds;

constexpr int64_t kServerIdShift = 20;
constexpr int kUserMessageBudget = 50;  // SPEC 17.2: с аккаунта пользователя за все эксперименты
constexpr uint64_t kSeed = 0xE3E3E3E3ULL;
constexpr int64_t kMaxFloodWaitSeconds = 3600;  // SPEC 17.2: больше часа — остановиться

std::atomic<bool> g_stop{false};

extern "C" void on_stop_signal(int /*sig*/) { g_stop.store(true); }

void install_signal_handlers() {
    struct sigaction sa {};
    sa.sa_handler = on_stop_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

// Остановка по сигналу: текущее действие закончено, процесс закрывает клиентов и выходит.
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

uint64_t mix(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

int64_t server_id(int64_t message_id) { return message_id >> kServerIdShift; }

std::string date_stamp() {
    const std::string iso = iso_utc_now();
    return iso.substr(0, 4) + iso.substr(5, 2) + iso.substr(8, 2);
}

std::string one_line(std::string text) {
    std::ranges::replace(text, '\n', ' ');
    std::ranges::replace(text, '\r', ' ');
    return text;
}

// ---- Параметры и имена ------------------------------------------------------------------

struct E3Options {
    bool probe = false;
    fs::path out;
    std::string rights = "only:post,edit,delete";
    int user_messages = 25;  // до добавления бота
    int user_deletes = 5;  // сколько сообщений пользователя бот пробует удалить
    int user_interval_ms = 13000;  // не больше 5 в минуту (SPEC 17.2)
    int target = 10000;  // заполнять, пока максимальный серверный id не дойдёт сюда
    int commit_every = 60;  // каждые столько сообщений бота: «коммит» (индекс, pin, unpin)
    int delete_every = 100;  // каждые столько сообщений бота: пачка удалений
    int delete_batch = 10;
    int send_interval_ms = 3400;  // 17,6 действия в минуту (потолок 18, SPEC 17.2)
    int pin_interval_ms = 4200;  // после pin, unpin и удалений: не больше 15 в минуту
    int pause_ms = 2000;  // перебор: пауза между пачками getMessages
    int margin = 100;  // перебор: сколько id сверх последнего проверить
    int max_batches = 0;  // перебор: остановиться после N пачек (проверка паузы), 0 — все
    bool fresh = false;
    std::string tag;
};

struct Names {
    std::string prefix;  // состояние <state-dir>/<prefix>.json
    fs::path out;        // CSV
    fs::path session;    // каталог сеанса бота
    fs::path fresh;      // каталог для перебора с пустой БД
    fs::path lock;
    fs::path docs;  // временные файлы отправки
};

Names names_for(const BenchOptions& options, const E3Options& e3) {
    Names n;
    n.prefix = e3.probe ? "e3p" : "e3";
    n.out = e3.probe ? e3.out / "probe" : e3.out;
    n.session = options.state_dir / ("bot-" + n.prefix);
    n.fresh = options.state_dir / ("bot-" + n.prefix + "-fresh");
    n.lock = options.state_dir / (n.prefix + "-fill.lock");
    n.docs = options.state_dir / (n.prefix + "-docs");
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
            throw std::runtime_error("another e3 process uses this bot session (lock " +
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

// ---- Ledger: что реально отправлено и удалено --------------------------------------------

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

struct LedgerEntry {
    std::string kind;    // user_text | bot_doc | bot_text | bot_idx
    std::string detail;  // имя документа или текст
    bool deleted = false;
};

bool is_bot_plain(const std::string& kind) { return kind == "bot_doc" || kind == "bot_text"; }

// Журнал событий: <out>/e3_ledger.csv (дописывается, из него состояние восстанавливается после
// падения). События: send, delete, delete_fail, delete_round, pin, unpin, unpin_fail, service,
// send_uncertain, note.
class Ledger {
  public:
    // writable = false: только чтение (scan, status).
    Ledger(const fs::path& path, bool writable) {
        if (fs::exists(path)) {
            std::ifstream in(path);
            std::string line;
            bool first = true;
            while (std::getline(in, line)) {
                if (first) {
                    first = false;
                    continue;
                }
                apply(parse_csv_line(line));
            }
        }
        if (writable) {
            csv_ = std::make_unique<Csv>(
                path, std::vector<std::string>{"time_utc", "event", "server_id", "kind", "detail"});
        }
    }

    void send(int64_t sid, const std::string& kind, const std::string& detail) {
        row("send", sid, kind, detail);
    }
    void deleted(int64_t sid, const std::string& detail) {
        row("delete", sid, kind_of(sid), detail);
    }
    void delete_failed(int64_t sid, const std::string& detail) {
        row("delete_fail", sid, kind_of(sid), detail);
    }
    void delete_round(int count) { row("delete_round", 0, "-", std::to_string(count)); }
    void pin(int64_t sid) { row("pin", sid, kind_of(sid), "ok"); }
    void unpin(int64_t sid, bool ok, const std::string& detail) {
        row(ok ? "unpin" : "unpin_fail", sid, kind_of(sid), ok ? "ok" : detail);
    }
    void service(int64_t sid, const std::string& type, const std::string& detail) {
        if (!services_.contains(sid)) {
            row("service", sid, type, detail);
        }
    }
    void uncertain(const std::string& kind, const std::string& detail) {
        row("send_uncertain", 0, kind, detail);
    }
    void note(int64_t sid, const std::string& text) { row("note", sid, kind_of(sid), text); }

    [[nodiscard]] const std::map<int64_t, LedgerEntry>& entries() const { return entries_; }
    [[nodiscard]] const std::map<int64_t, std::string>& services() const { return services_; }
    [[nodiscard]] const std::vector<int64_t>& pinned_open() const { return pinned_open_; }
    [[nodiscard]] bool ever_pinned(int64_t sid) const { return pinned_ever_.contains(sid); }
    [[nodiscard]] bool user_delete_tried(int64_t sid) const {
        return user_delete_tried_.contains(sid);
    }
    [[nodiscard]] int64_t max_id() const { return max_id_; }
    [[nodiscard]] int64_t bot_sends() const { return bot_sends_; }
    [[nodiscard]] int64_t idx_sends() const { return idx_sends_; }
    [[nodiscard]] int64_t delete_rounds() const { return delete_rounds_; }
    [[nodiscard]] int64_t actions() const { return actions_; }
    [[nodiscard]] int64_t pins() const { return pins_; }
    [[nodiscard]] int64_t uncertain_sends() const { return uncertain_; }
    [[nodiscard]] int64_t deleted_count() const {
        int64_t n = 0;
        for (const auto& [sid, e] : entries_) {
            n += e.deleted ? 1 : 0;
        }
        return n;
    }
    [[nodiscard]] std::vector<int64_t> user_ids() const {
        std::vector<int64_t> ids;
        for (const auto& [sid, e] : entries_) {
            if (e.kind == "user_text") {
                ids.push_back(sid);
            }
        }
        return ids;
    }

  private:
    [[nodiscard]] std::string kind_of(int64_t sid) const {
        const auto it = entries_.find(sid);
        return it == entries_.end() ? "-" : it->second.kind;
    }

    void row(const std::string& event, int64_t sid, const std::string& kind,
             const std::string& detail) {
        if (csv_) {
            csv_->row({iso_utc_now(), event, std::to_string(sid), kind, one_line(detail)});
        }
        apply({"", event, std::to_string(sid), kind, detail});
    }

    void apply(const std::vector<std::string>& f) {
        if (f.size() < 5) {
            return;
        }
        const std::string& event = f[1];
        const int64_t sid = std::stoll(f[2]);
        const std::string& kind = f[3];
        if (event != "service" && event != "note") {
            ++actions_;
        }
        if (event == "send") {
            entries_[sid] = LedgerEntry{kind, f[4], false};
            max_id_ = std::max(max_id_, sid);
            bot_sends_ += is_bot_plain(kind) ? 1 : 0;
            idx_sends_ += kind == "bot_idx" ? 1 : 0;
        } else if (event == "delete") {
            if (const auto it = entries_.find(sid); it != entries_.end()) {
                it->second.deleted = true;
            }
            if (kind == "user_text") {
                user_delete_tried_.insert(sid);
            }
        } else if (event == "delete_fail") {
            if (kind == "user_text") {
                user_delete_tried_.insert(sid);
            }
        } else if (event == "delete_round") {
            ++delete_rounds_;
        } else if (event == "pin") {
            ++pins_;
            pinned_ever_.insert(sid);
            std::erase(pinned_open_, sid);
            pinned_open_.push_back(sid);
        } else if (event == "unpin" || event == "unpin_fail") {
            std::erase(pinned_open_, sid);
        } else if (event == "service") {
            services_[sid] = kind + (f[4].empty() ? "" : " " + f[4]);
            max_id_ = std::max(max_id_, sid);
        } else if (event == "send_uncertain") {
            ++uncertain_;
        }
    }

    std::map<int64_t, LedgerEntry> entries_;
    std::map<int64_t, std::string> services_;
    std::vector<int64_t> pinned_open_;
    std::set<int64_t> pinned_ever_;
    std::set<int64_t> user_delete_tried_;
    int64_t max_id_ = 0;
    int64_t bot_sends_ = 0;
    int64_t idx_sends_ = 0;
    int64_t delete_rounds_ = 0;
    int64_t actions_ = 0;
    int64_t pins_ = 0;
    int64_t uncertain_ = 0;
    std::unique_ptr<Csv> csv_;
};

// ---- Ограничения Telegram и сеть ---------------------------------------------------------

enum class ErrorClass : uint8_t { kFlood, kTransient, kSession, kStop, kFatal };

ErrorClass classify(const td::TdError& e) {
    if (e.code == 401 || e.message.find("USER_DEACTIVATED") != std::string::npos ||
        e.message.find("AUTH_KEY") != std::string::npos) {
        return ErrorClass::kStop;
    }
    if (e.code == td::TdError::kClosed || e.code == td::TdError::kStopped) {
        return ErrorClass::kSession;
    }
    if (e.is_rate_limit()) {
        return ErrorClass::kFlood;
    }
    if (e.is_local() || e.code >= 500) {
        return ErrorClass::kTransient;
    }
    return ErrorClass::kFatal;
}

// Пауза между действиями, которая растёт после FLOOD_WAIT.
class Pacer {
  public:
    void wait() {
        if (next_) {
            const auto now = ExpClock::now();
            if (*next_ > now) {
                sleep_interruptible(std::chrono::duration_cast<milliseconds>(*next_ - now));
            }
        }
        if (g_stop.load()) {
            throw Interrupted();
        }
        begin_ = ExpClock::now();
    }
    // Интервал считается от начала действия (возврата из wait), а не от его конца: время самой
    // отправки входит в интервал, темп получается заданным, а не медленнее его.
    void done(int interval_ms) {
        next_ = begin_ + milliseconds{static_cast<int64_t>(interval_ms * factor_)};
    }
    void slow_down() { factor_ = std::min(2.0, factor_ * 1.15); }
    void set_factor(double f) { factor_ = std::clamp(f, 1.0, 2.0); }
    [[nodiscard]] double factor() const { return factor_; }

  private:
    std::optional<ExpClock::time_point> next_;
    ExpClock::time_point begin_ = ExpClock::now();
    double factor_ = 1.0;
};

// Выполняет запросы с ожиданием FLOOD_WAIT (с запасом), повтором при обрывах сети и 5xx.
// Остальные ошибки возвращаются вызывающему как TdException.
class Resilience {
  public:
    Resilience(const fs::path& out, Pacer& pacer)
        : pacer_(pacer),
          flood_(out / "e3_flood.csv",
                 {"time_utc", "phase", "what", "retry_after_s", "waited_s", "pace_factor"}),
          net_(out / "e3_net_errors.csv", {"time_utc", "phase", "what", "error", "backoff_s"}) {}

    void set_phase(const std::string& phase) { phase_ = phase; }
    [[nodiscard]] int flood_events() const { return flood_events_; }
    [[nodiscard]] double flood_wait_s() const { return flood_wait_s_; }
    [[nodiscard]] int net_errors() const { return net_errors_; }

    template <class F>
    auto run(const std::string& what, F&& f) -> decltype(f()) {
        for (;;) {
            if (g_stop.load()) {
                throw Interrupted();
            }
            try {
                auto result = f();
                transient_streak_ = 0;
                return result;
            } catch (const td::TdException& e) {
                handle(what, e.error());
            }
        }
    }

    // Обработка ошибки: возвращается, если надо повторить; иначе бросает.
    void handle(const std::string& what, const td::TdError& error) {
        switch (classify(error)) {
            case ErrorClass::kStop:
                throw StopExperiment("account problem at '" + what + "': " + error.to_string());
            case ErrorClass::kSession:
                throw SessionError("client closed at '" + what + "'");
            case ErrorClass::kFatal:
                throw td::TdException(error);
            case ErrorClass::kFlood: {
                const int64_t seconds_to_wait = error.retry_after ? error.retry_after->count() : 60;
                const int64_t wait_s = seconds_to_wait + seconds_to_wait / 5 + 5;
                ++flood_events_;
                flood_wait_s_ += static_cast<double>(seconds_to_wait);
                flood_.row({iso_utc_now(), phase_, what, std::to_string(seconds_to_wait),
                            std::to_string(wait_s), fmt(pacer_.factor(), 2)});
                if (seconds_to_wait > kMaxFloodWaitSeconds) {
                    throw StopExperiment("FLOOD_WAIT of " + std::to_string(seconds_to_wait) +
                                         " s at '" + what + "': stopping, see e3_flood.csv");
                }
                log_line("FLOOD_WAIT " + std::to_string(seconds_to_wait) + " s at '" + what +
                         "': waiting " + std::to_string(wait_s) + " s, pace slowed");
                pacer_.slow_down();
                sleep_interruptible(seconds{wait_s});
                return;
            }
            case ErrorClass::kTransient: {
                ++net_errors_;
                const int64_t backoff =
                    std::min<int64_t>(600, 15LL << std::min(transient_streak_, 5));
                ++transient_streak_;
                net_.row({iso_utc_now(), phase_, what, error.to_string(), std::to_string(backoff)});
                log_line("network/server error at '" + what + "': " + error.to_string() +
                         "; retry in " + std::to_string(backoff) + " s");
                sleep_interruptible(seconds{backoff});
                return;
            }
        }
    }

  private:
    Pacer& pacer_;
    Csv flood_;
    Csv net_;
    std::string phase_ = "fill";
    int flood_events_ = 0;
    double flood_wait_s_ = 0;
    int net_errors_ = 0;
    int transient_streak_ = 0;
};

// ---- Общие запросы -----------------------------------------------------------------------

td::Json text_request(int64_t chat_id, const std::string& text) {
    return td::Json{
        {"@type", "sendMessage"},
        {"chat_id", chat_id},
        {"options", {{"@type", "messageSendOptions"}, {"disable_notification", true}}},
        {"input_message_content",
         {{"@type", "inputMessageText"}, {"text", {{"@type", "formattedText"}, {"text", text}}}}}};
}

td::Json get_messages_request(int64_t chat_id, const std::vector<int64_t>& server_ids) {
    td::Json ids = td::Json::array();
    for (const int64_t sid : server_ids) {
        ids.push_back(sid << kServerIdShift);
    }
    return td::Json{{"@type", "getMessages"}, {"chat_id", chat_id}, {"message_ids", ids}};
}

// Что бот видит в сообщении: тип содержимого и текст или имя файла.
struct Observed {
    bool found = false;
    std::string type = "-";
    std::string text;
    bool outgoing = false;
};

Observed observe(const td::Json& message) {
    Observed o;
    if (message.is_null()) {
        return o;
    }
    o.found = true;
    o.type = message["content"].value("@type", std::string{"?"});
    o.outgoing = message.value("is_outgoing", false);
    if (o.type == "messageText") {
        o.text = message["content"]["text"].value("text", std::string{});
    } else if (o.type == "messageDocument") {
        o.text = message["content"]["document"].value("file_name", std::string{});
    } else if (o.type == "messagePinMessage") {
        o.text = "pinned " + std::to_string(server_id(message["content"].value("message_id", 0LL)));
    }
    return o;
}

// Бот входит по токену в отдельный каталог сессии и узнаёт канал.
std::unique_ptr<BenchAccount> open_bot(BenchEnv& env, const fs::path& dir, int64_t chat_id) {
    auto bot = env.open_at(Account::kBot, dir, std::cerr);
    std::string error;
    if (!wait_chat_known(bot->client(), chat_id, seconds{120}, &error)) {
        throw std::runtime_error("bot cannot get the channel: " + error);
    }
    return bot;
}

// Сколько сообщений пользователь уже отправил во всех экспериментах (счётчики
// *.user_messages_sent).
int user_messages_spent(const BenchOptions& options) {
    const fs::path path = options.state_dir / "counters.json";
    if (!fs::exists(path)) {
        return 0;
    }
    std::ifstream in(path);
    const td::Json counters = td::Json::parse(in);
    int64_t total = 0;
    for (const auto& [key, value] : counters.items()) {
        if (key.ends_with(".user_messages_sent") && value.is_number_integer()) {
            total += value.get<int64_t>();
        }
    }
    return static_cast<int>(total);
}

// ---- setup -------------------------------------------------------------------------------

void cmd_setup(BenchEnv& env, const BenchOptions& options, const E3Options& e3) {
    const Names names = names_for(options, e3);
    const SessionLock lock(names.lock);
    td::Json state;
    if (has_state(options, names.prefix)) {
        state = load_state(options, names.prefix);
        if (state.value("bot_added", false)) {
            throw std::runtime_error("channel " + names.prefix + " already set up; run teardown");
        }
        log_line("resuming setup: channel exists, " +
                 std::to_string(state["user_messages"].size()) + " user messages sent");
    }
    FloodGuard guard(std::cerr, names.out / "e3_flood.csv", "e3-setup");
    auto user = env.open(Account::kUser, std::cerr);
    auto bot = env.open_at(Account::kBot, names.session, std::cerr);
    Ledger ledger(names.out / "e3_ledger.csv", true);

    if (state.is_null()) {
        const std::string title =
            std::string{"tgfs-bench-e3"} + (e3.probe ? "probe" : "") + "-" + date_stamp();
        const int remaining = e3.user_messages;
        if (user_messages_spent(options) + remaining > kUserMessageBudget) {
            throw std::runtime_error("user message budget (50) would be exceeded");
        }
        const int64_t chat_id = create_channel(*user, title, guard, std::cerr);
        bump_counter(options, names.prefix + ".user_actions", 1);
        state = td::Json{{"chat_id", chat_id},       {"title", title},
                         {"created", iso_utc_now()}, {"user_messages", td::Json::array()},
                         {"bot_added", false},       {"fill_done", false}};
        save_state(options, names.prefix, state);
    }
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    std::string error;
    wait_chat_known(user->client(), chat_id, seconds{60}, &error);

    // Сообщения пользователя до добавления бота.
    Pacer pacer;
    for (int n = static_cast<int>(state["user_messages"].size()) + 1; n <= e3.user_messages; ++n) {
        if (user_messages_spent(options) + 1 > kUserMessageBudget) {
            throw std::runtime_error("user message budget (50) exhausted");
        }
        pacer.wait();
        const std::string text = "e3 user " + std::to_string(n);
        const td::Json msg = guard.run("user send", [&] {
            return user->tracker().send(text_request(chat_id, text), seconds{30}, seconds{120});
        });
        pacer.done(e3.user_interval_ms);
        const int64_t sid = server_id(msg.value("id", int64_t{0}));
        ledger.send(sid, "user_text", text);
        bump_counter(options, names.prefix + ".user_messages_sent", 1);
        state["user_messages"].push_back(sid);
        save_state(options, names.prefix, state);
        log_line("user message " + std::to_string(n) + "/" + std::to_string(e3.user_messages) +
                 " sent, server id " + std::to_string(sid));
    }

    add_bot_admin(*user, *bot, chat_id, e3.rights, guard, std::cerr);
    bump_counter(options, names.prefix + ".user_actions", 2);
    const auto known = wait_chat_known(bot->client(), chat_id, seconds{90}, &error);
    log_line("bot knows the channel: " + (known ? fmt(*known) + " ms" : "NO (" + error + ")"));
    state["bot_added"] = true;
    state["rights"] = e3.rights;
    state["bot_added_at"] = iso_utc_now();
    save_state(options, names.prefix, state);
    std::cout << "setup done: " << e3.user_messages << " user messages, bot is administrator ("
              << e3.rights << ")\n";
}

// ---- fill --------------------------------------------------------------------------------

class Filler {
  public:
    Filler(BenchEnv& env, const BenchOptions& options, const E3Options& e3, const Names& names)
        : env_(env),
          options_(options),
          e3_(e3),
          names_(names),
          state_(load_state(options, names.prefix)),
          chat_id_(state_.at("chat_id").get<int64_t>()),
          ledger_(names.out / "e3_ledger.csv", true),
          resilience_(names.out, pacer_),
          progress_(names.out / "e3_fill_progress.csv",
                    {"time_utc", "max_server_id", "bot_sends", "deleted", "pins", "service_seen",
                     "flood_events", "flood_wait_s", "net_errors", "pace_factor", "ids_per_hour"}),
          user_delete_csv_(
              names.out / "e3_user_delete.csv",
              {"time_utc", "server_id", "seen_before", "delete_result", "seen_after"}) {
        if (!state_.value("bot_added", false)) {
            throw std::runtime_error("run `e3 setup` first (the bot is not added yet)");
        }
        pacer_.set_factor(state_.value("pace_factor", 1.0));
        fs::create_directories(names.docs);
    }

    // 0: цель достигнута или остановка по сигналу, 1: ошибка, 2: серия остановлена (решает
    // человек).
    int run() {
        started_ = ExpClock::now();
        log_line("fill starts: ledger has " + std::to_string(ledger_.entries().size()) +
                 " messages, max server id " + std::to_string(ledger_.max_id()) + ", target " +
                 std::to_string(e3_.target) + ", pace factor " + fmt(pacer_.factor(), 2));
        start_max_id_ = ledger_.max_id();
        int failures = 0;
        for (;;) {
            const int64_t actions_before = ledger_.actions();
            try {
                run_session();
                finish();
                return 0;
            } catch (const Interrupted&) {
                log_line("interrupted by signal; state is saved, rerun `e3 fill` to resume");
                persist();
                return 0;
            } catch (const StopExperiment& e) {
                log_line(std::string{"STOPPED: "} + e.what());
                persist();
                return 2;
            } catch (const td::TdException& e) {
                log_line("STOPPED on a non-retryable error: " + e.error().to_string());
                persist();
                return 2;
            } catch (const std::exception& e) {
                close_session();
                failures = ledger_.actions() > actions_before ? 1 : failures + 1;
                log_line(std::string{"session error: "} + e.what() + " (failure " +
                         std::to_string(failures) + " of 10)");
                if (failures >= 10) {
                    persist();
                    return 1;
                }
                try {
                    sleep_interruptible(seconds{std::min(600, 60 * failures)});
                } catch (const Interrupted&) {
                    persist();
                    return 0;
                }
            }
        }
    }

  private:
    void run_session() {
        bot_ = open_bot(env_, names_.session, chat_id_);
        service_sub_ = bot_->client().subscribe("updateNewMessage",
                                                [this](const td::Json& u) { on_new_message(u); });
        resilience_.set_phase("fill");
        recover();
        while (ledger_.max_id() < e3_.target) {
            if (g_stop.load()) {
                throw Interrupted();
            }
            step();
            flush_service();
            progress(false);
        }
        flush_service();
    }

    void finish() {
        progress(true);
        state_["fill_done"] = true;
        state_["fill_done_at"] = iso_utc_now();
        persist();
        bump_counter(options_, names_.prefix + ".bot_messages_sent", session_sends_);
        close_session();
        log_line("fill done: max server id " + std::to_string(ledger_.max_id()) + ", " +
                 std::to_string(ledger_.bot_sends()) + " bot messages, " +
                 std::to_string(ledger_.deleted_count()) + " deleted, " +
                 std::to_string(resilience_.flood_events()) + " FLOOD_WAIT in this run");
    }

    void close_session() {
        service_sub_.reset();
        bot_.reset();
    }

    void persist() {
        state_["pace_factor"] = pacer_.factor();
        state_["updated"] = iso_utc_now();
        state_["max_server_id"] = ledger_.max_id();
        save_state(options_, names_.prefix, state_);
    }

    // Служебные сообщения приходят апдейтами. Обработчик только запоминает.
    void on_new_message(const td::Json& update) {
        const td::Json& m = update["message"];
        if (m.value("chat_id", int64_t{0}) != chat_id_) {
            return;
        }
        const std::string type = m["content"].value("@type", std::string{});
        if (type == "messageText" || type == "messageDocument") {
            return;
        }
        const std::lock_guard<std::mutex> lock(service_mu_);
        service_.push_back(
            {server_id(m.value("id", int64_t{0})), type,
             type == "messagePinMessage"
                 ? "pinned " + std::to_string(server_id(m["content"].value("message_id", 0LL)))
                 : ""});
    }

    void flush_service() {
        std::vector<ServiceMsg> items;
        {
            const std::lock_guard<std::mutex> lock(service_mu_);
            items.swap(service_);
        }
        for (const auto& s : items) {
            ledger_.service(s.sid, s.type, s.detail);
        }
    }

    // После падения: докончить коммит (индекс отправлен, pin нет), снять лишние закрепления.
    void recover() {
        for (const auto& [sid, entry] : std::ranges::reverse_view(ledger_.entries())) {
            if (entry.kind == "bot_idx" && !entry.deleted) {
                if (!ledger_.ever_pinned(sid)) {
                    log_line("recovery: pinning the unfinished commit " + std::to_string(sid));
                    pin(sid);
                }
                break;
            }
        }
        unpin_others(true);
    }

    void step() {
        const auto n = static_cast<uint64_t>(ledger_.actions());
        if (e3_.user_deletes > 0 && next_user_delete()) {
            user_delete_step();
        } else if (ledger_.bot_sends() >= (ledger_.delete_rounds() + 1) * e3_.delete_every) {
            delete_round(n);
        } else if (ledger_.bot_sends() >= (ledger_.idx_sends() + 1) * e3_.commit_every) {
            commit(n);
        } else {
            plain(n);
        }
    }

    // ---- отправка ----

    // Возвращает серверный id или 0, если итог неизвестен (запись send_uncertain).
    int64_t send_message(const td::Json& request, const std::string& kind,
                         const std::string& detail) {
        for (;;) {
            pacer_.wait();
            td::Json temp;
            try {
                temp = bot_->client().call(request, seconds{120});
            } catch (const td::TdException& e) {
                if (e.error().is_timeout()) {
                    ledger_.uncertain(kind, detail);
                    log_line("sendMessage timed out, the outcome is unknown (send_uncertain)");
                    pacer_.done(e3_.send_interval_ms);
                    return 0;
                }
                resilience_.handle("send", e.error());
                continue;
            }
            const int64_t old_id = temp.value("id", int64_t{0});
            const auto deadline = ExpClock::now() + std::chrono::hours{3};
            bool retry = false;
            for (;;) {
                try {
                    const td::Json done = bot_->tracker().wait(chat_id_, old_id, seconds{300});
                    const int64_t sid = server_id(done.value("id", int64_t{0}));
                    ledger_.send(sid, kind, detail);
                    pacer_.done(e3_.send_interval_ms);
                    return sid;
                } catch (const td::TdException& e) {
                    if (e.error().is_timeout()) {
                        if (g_stop.load() || ExpClock::now() > deadline) {
                            ledger_.uncertain(kind, detail);
                            if (g_stop.load()) {
                                throw Interrupted();
                            }
                            return 0;
                        }
                        log_line("waiting for the upload to finish (network?)");
                        continue;
                    }
                    resilience_.handle("send", e.error());  // не ушло, значит можно повторить
                    retry = true;
                    break;
                }
            }
            if (!retry) {
                return 0;
            }
        }
    }

    void plain(uint64_t n) {
        const uint64_t r = mix(kSeed ^ (n * 0xA24BAED4963EE407ULL));
        if (r % 100 < 70) {
            // Мелкий документ 200 Б – 16 КиБ, размер по логарифмической шкале.
            const double u = static_cast<double>(mix(r) % 10000) / 10000.0;
            const auto size = static_cast<size_t>(200.0 * std::pow(16384.0 / 200.0, u));
            const std::string name = random_hex(16) + ".bin";
            send_document(name, size, kSeed + n, "bot_doc");
        } else {
            const size_t extra = 10 + mix(r ^ 1) % 140;
            std::string text = "e3 bot s" + std::to_string(n);
            static constexpr const char* kWords[] = {"alpha", "beta",  "gamma", "delta",
                                                     "omega", "sigma", "kappa", "theta"};
            for (uint64_t i = 0; text.size() < 10 + extra; ++i) {
                text += std::string{" "} + kWords[mix(r + i) % 8];
            }
            send_message(text_request(chat_id_, text), "bot_text", text);
        }
        ++session_sends_;
    }

    int64_t send_document(const std::string& name, size_t size, uint64_t seed,
                          const std::string& kind) {
        const fs::path file = names_.docs / name;
        std::string content;
        fill_seeded(seed, 0, content, size);
        {
            std::ofstream out(file, std::ios::binary | std::ios::trunc);
            out.write(content.data(), static_cast<std::streamsize>(content.size()));
        }
        int64_t sid = 0;
        try {
            sid = send_message(send_document_request(chat_id_, file), kind, name);
        } catch (...) {
            std::error_code ec;
            fs::remove(file, ec);
            throw;
        }
        std::error_code ec;
        fs::remove(file, ec);
        return sid;
    }

    // ---- закрепления ----

    void pin(int64_t sid) {
        pacer_.wait();
        resilience_.run("pin", [&] {
            return bot_->client().call(td::Json{{"@type", "pinChatMessage"},
                                                {"chat_id", chat_id_},
                                                {"message_id", sid << kServerIdShift},
                                                {"disable_notification", true},
                                                {"only_for_self", false}});
        });
        pacer_.done(e3_.pin_interval_ms);
        ledger_.pin(sid);
    }

    // preload: после перезапуска процесса unpin требует, чтобы сообщение было загружено (E2), и
    // его сначала читают getMessages. В обычном коммите оно уже известно процессу.
    void unpin_others(bool preload = false) {
        const std::vector<int64_t> open = ledger_.pinned_open();
        for (size_t i = 0; i + 1 < open.size(); ++i) {
            const int64_t sid = open[i];
            if (preload) {
                pacer_.wait();
                attempt([&] {
                    bot_->client().call(get_messages_request(chat_id_, {sid}), seconds{30});
                });
                pacer_.done(e3_.pin_interval_ms);
            }
            const auto unpin_call = [&] {
                return resilience_.run("unpin", [&] {
                    return bot_->client().call(td::Json{{"@type", "unpinChatMessage"},
                                                        {"chat_id", chat_id_},
                                                        {"message_id", sid << kServerIdShift}});
                });
            };
            pacer_.wait();
            try {
                try {
                    unpin_call();
                } catch (const td::TdException& e) {
                    if (preload || e.error().code != 400) {
                        throw;
                    }
                    // Сообщение закреплено до перезапуска процесса и TDLib его не знает: прочитать
                    // и повторить (E2).
                    pacer_.done(e3_.pin_interval_ms);
                    pacer_.wait();
                    attempt([&] {
                        bot_->client().call(get_messages_request(chat_id_, {sid}), seconds{30});
                    });
                    pacer_.done(e3_.pin_interval_ms);
                    pacer_.wait();
                    unpin_call();
                }
                ledger_.unpin(sid, true, "");
            } catch (const td::TdException& e) {
                ledger_.unpin(sid, false, e.error().to_string());
                log_line("unpin of " + std::to_string(sid) + " failed: " + e.error().to_string());
            }
            pacer_.done(e3_.pin_interval_ms);
        }
    }

    // Коммит как в telega-fs: индекс, pin, unpin прошлого. Два подряд идущих id.
    void commit(uint64_t n) {
        const std::string name = random_hex(16) + ".idx";
        const int64_t sid = send_document(name, 2048, kSeed + n, "bot_idx");
        if (sid == 0) {
            return;
        }
        pin(sid);
        unpin_others();
    }

    // ---- удаления ----

    // Сообщение пользователя, которое пора пробовать удалить: равномерно по всем его сообщениям.
    bool next_user_delete() {
        const std::vector<int64_t> ids = ledger_.user_ids();
        if (ids.empty()) {
            return false;
        }
        const size_t total = ids.size();
        const size_t deletes = std::min<size_t>(static_cast<size_t>(e3_.user_deletes), total);
        for (size_t k = 0; k < deletes; ++k) {
            const int64_t sid = ids[k * total / deletes + total / (2 * deletes)];
            if (!ledger_.user_delete_tried(sid)) {
                user_delete_target_ = sid;
                return true;
            }
        }
        return false;
    }

    // Может ли бот с правом delete удалить чужое (пользователя) сообщение: читаем, удаляем,
    // читаем ещё раз.
    void user_delete_step() {
        const int64_t sid = user_delete_target_;
        const auto read = [&] {
            pacer_.wait();
            const td::Json res = resilience_.run("getMessages(user message)", [&] {
                return bot_->client().call(get_messages_request(chat_id_, {sid}));
            });
            pacer_.done(e3_.pin_interval_ms);
            return !res["messages"][0].is_null();
        };
        const bool before = read();
        std::string result = "ok";
        pacer_.wait();
        try {
            resilience_.run("deleteMessages(user message)", [&] {
                return bot_->client().call(
                    td::Json{{"@type", "deleteMessages"},
                             {"chat_id", chat_id_},
                             {"message_ids", td::Json::array({sid << kServerIdShift})},
                             {"revoke", true}});
            });
        } catch (const td::TdException& e) {
            result = "error " + e.error().to_string();
        }
        pacer_.done(e3_.pin_interval_ms);
        const bool after = read();
        user_delete_csv_.row(
            {iso_utc_now(), std::to_string(sid), before ? "1" : "0", result, after ? "1" : "0"});
        log_line("bot deletes the user message " + std::to_string(sid) + ": seen before " +
                 (before ? "yes" : "no") + ", " + result + ", seen after " +
                 (after ? "yes" : "no"));
        if (result == "ok" && !after) {
            ledger_.deleted(sid, "ok");
        } else {
            ledger_.delete_failed(
                sid, result == "ok" ? "call ok but the message is still returned" : result);
        }
    }

    void delete_round(uint64_t n) {
        std::vector<int64_t> candidates;
        for (const auto& [sid, e] : ledger_.entries()) {
            const auto& open = ledger_.pinned_open();
            if (is_bot_plain(e.kind) && !e.deleted && sid + 30 <= ledger_.max_id() &&
                std::ranges::find(open, sid) == open.end()) {
                candidates.push_back(sid);
            }
        }
        std::vector<int64_t> chosen;
        const size_t want =
            std::min<size_t>(static_cast<size_t>(e3_.delete_batch), candidates.size());
        for (size_t i = 0; i < want; ++i) {
            const size_t j = i + mix(kSeed ^ (n * 131 + i)) % (candidates.size() - i);
            std::swap(candidates[i], candidates[j]);
            chosen.push_back(candidates[i]);
        }
        if (chosen.empty()) {
            ledger_.delete_round(0);
            return;
        }
        // Бот читает то, что собирается удалить: после перезапуска процесса сообщения ему
        // неизвестны, а заодно проверка «отправленное читается».
        pacer_.wait();
        const td::Json res = resilience_.run("getMessages(before delete)", [&] {
            return bot_->client().call(get_messages_request(chat_id_, chosen));
        });
        pacer_.done(e3_.pin_interval_ms);
        td::Json ids = td::Json::array();
        std::vector<int64_t> to_delete;
        for (size_t i = 0; i < chosen.size(); ++i) {
            if (res["messages"][i].is_null()) {
                ledger_.note(chosen[i], "getMessages returned null for a message we sent");
            } else {
                ids.push_back(chosen[i] << kServerIdShift);
                to_delete.push_back(chosen[i]);
            }
        }
        if (!to_delete.empty()) {
            pacer_.wait();
            std::string result = "ok";
            try {
                resilience_.run("deleteMessages", [&] {
                    return bot_->client().call(td::Json{{"@type", "deleteMessages"},
                                                        {"chat_id", chat_id_},
                                                        {"message_ids", ids},
                                                        {"revoke", true}});
                });
            } catch (const td::TdException& e) {
                result = "error " + e.error().to_string();
            }
            pacer_.done(e3_.pin_interval_ms);
            for (const int64_t sid : to_delete) {
                if (result == "ok") {
                    ledger_.deleted(sid, "ok");
                } else {
                    ledger_.delete_failed(sid, result);
                }
            }
        }
        ledger_.delete_round(static_cast<int>(to_delete.size()));
    }

    // ---- прогресс ----

    void progress(bool force) {
        const int64_t sends = ledger_.bot_sends();
        const int64_t every = e3_.target < 1000 ? 20 : 100;
        if (!force && (sends % every != 0 || sends == last_progress_sends_ || sends == 0)) {
            return;
        }
        last_progress_sends_ = sends;
        const double hours = ms_since(started_) / 3.6e6;
        const double rate =
            hours > 0.01 ? static_cast<double>(ledger_.max_id() - start_max_id_) / hours : 0;
        const int64_t left = std::max<int64_t>(0, e3_.target - ledger_.max_id());
        const std::string eta = rate > 1 ? fmt(static_cast<double>(left) / rate, 1) + " h" : "?";
        log_line("progress: max id " + std::to_string(ledger_.max_id()) + "/" +
                 std::to_string(e3_.target) + ", bot messages " + std::to_string(sends) +
                 ", deleted " + std::to_string(ledger_.deleted_count()) + ", pins " +
                 std::to_string(ledger_.pins()) + ", FLOOD_WAIT " +
                 std::to_string(resilience_.flood_events()) + ", net errors " +
                 std::to_string(resilience_.net_errors()) + ", pace x" + fmt(pacer_.factor(), 2) +
                 ", " + fmt(rate, 0) + " ids/h, ETA " + eta);
        progress_.row({iso_utc_now(), std::to_string(ledger_.max_id()), std::to_string(sends),
                       std::to_string(ledger_.deleted_count()), std::to_string(ledger_.pins()),
                       std::to_string(ledger_.services().size()),
                       std::to_string(resilience_.flood_events()),
                       fmt(resilience_.flood_wait_s(), 0), std::to_string(resilience_.net_errors()),
                       fmt(pacer_.factor(), 2), fmt(rate, 0)});
        persist();
    }

    struct ServiceMsg {
        int64_t sid;
        std::string type;
        std::string detail;
    };

    BenchEnv& env_;
    const BenchOptions& options_;
    const E3Options& e3_;
    const Names& names_;
    td::Json state_;
    const int64_t chat_id_;
    Pacer pacer_;
    Ledger ledger_;
    Resilience resilience_;
    Csv progress_;
    Csv user_delete_csv_;
    std::unique_ptr<BenchAccount> bot_;
    td::Subscription service_sub_;
    std::mutex service_mu_;
    std::vector<ServiceMsg> service_;
    ExpClock::time_point started_ = ExpClock::now();
    int64_t start_max_id_ = 0;
    int64_t last_progress_sends_ = -1;
    int64_t session_sends_ = 0;
    int64_t user_delete_target_ = 0;
};

// ---- scan --------------------------------------------------------------------------------

struct Expectation {
    std::string cls;  // alive | deleted | service | gap | beyond
    std::string kind;
    std::string detail;
};

Expectation expect(const Ledger& ledger, int64_t sid) {
    if (const auto it = ledger.entries().find(sid); it != ledger.entries().end()) {
        return {it->second.deleted ? "deleted" : "alive", it->second.kind, it->second.detail};
    }
    if (const auto it = ledger.services().find(sid); it != ledger.services().end()) {
        return {"service", it->second.substr(0, it->second.find(' ')), ""};
    }
    if (sid > ledger.max_id()) {
        return {"beyond", "-", ""};
    }
    return {"gap", sid == 1 ? "channel_create_assumed" : "service_or_unknown", ""};
}

std::string verdict(const Expectation& want, const Observed& got) {
    if (want.cls == "alive") {
        if (!got.found) {
            return "missed";
        }
        const bool text_kind = want.kind == "user_text" || want.kind == "bot_text";
        const bool type_ok = got.type == (text_kind ? "messageText" : "messageDocument");
        return type_ok && got.text == want.detail ? "ok" : "mismatch";
    }
    if (want.cls == "deleted") {
        return got.found ? "still_present" : "ok";
    }
    if (want.cls == "service") {
        if (!got.found) {
            return "missed";
        }
        return got.type == "messageText" || got.type == "messageDocument" ? "mismatch" : "ok";
    }
    if (want.cls == "gap") {
        if (!got.found) {
            return "gap_null";
        }
        return got.type == "messageText" || got.type == "messageDocument" ? "unexpected_message"
                                                                          : "gap_service";
    }
    return got.found ? "unexpected_message" : "ok";  // beyond
}

void cmd_scan(BenchEnv& env, const BenchOptions& options, const E3Options& e3) {
    const Names names = names_for(options, e3);
    const SessionLock lock(names.lock);
    const td::Json state = load_state(options, names.prefix);
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    const Ledger ledger(names.out / "e3_ledger.csv", false);
    const std::string tag = (e3.fresh ? "fresh" : "session") + (e3.tag.empty() ? "" : "_" + e3.tag);
    const fs::path scan_path = names.out / ("e3_scan_" + tag + ".csv");
    if (fs::exists(scan_path)) {
        throw std::runtime_error(scan_path.string() + " exists; pass --tag to keep both scans");
    }
    const int64_t last = ledger.max_id() + e3.margin;

    const auto t_login = ExpClock::now();
    fs::path dir = names.session;
    if (e3.fresh) {
        dir = names.fresh;
        remove_fresh_bot_dir(dir);
    }
    auto bot = env.open_at(Account::kBot, dir, std::cerr);
    const double login_ms = ms_since(t_login);
    std::string error;
    const auto known = wait_chat_known(bot->client(), chat_id, seconds{120}, &error);
    if (!known) {
        throw std::runtime_error("bot cannot get the channel: " + error);
    }
    log_line("scan " + tag + ": ids 1.." + std::to_string(last) + ", login " + fmt(login_ms, 0) +
             " ms, getChat " + fmt(*known, 0) + " ms");

    Pacer pacer;
    Resilience resilience(names.out, pacer);
    resilience.set_phase("scan-" + tag);
    Csv rows(scan_path, {"server_id", "found", "content_type", "name_or_text", "is_outgoing",
                         "expected", "expected_kind", "verdict"});
    Csv batches(names.out / "e3_scan_batches.csv",
                {"tag", "batch", "first_id", "last_id", "ms", "non_null", "flood_events_so_far"});
    std::map<std::string, int64_t> counts;    // verdict по ожиданию
    std::map<std::string, int64_t> by_class;  // expected/kind -> всего
    std::map<std::string, int64_t> found_class;
    std::vector<double> batch_ms;
    const auto t_scan = ExpClock::now();
    int batch_no = 0;
    for (int64_t from = 1; from <= last; from += 100) {
        const int64_t to = std::min<int64_t>(from + 99, last);
        std::vector<int64_t> ids;
        for (int64_t sid = from; sid <= to; ++sid) {
            ids.push_back(sid);
        }
        const auto t0 = ExpClock::now();
        const td::Json res = resilience.run("getMessages", [&] {
            return bot->client().call(get_messages_request(chat_id, ids), seconds{60});
        });
        const double ms = ms_since(t0);
        int64_t non_null = 0;
        for (size_t i = 0; i < ids.size(); ++i) {
            const Observed got = observe(res["messages"][i]);
            const Expectation want = expect(ledger, ids[i]);
            const std::string v = verdict(want, got);
            non_null += got.found ? 1 : 0;
            std::string outgoing = "-";
            if (got.found) {
                outgoing = got.outgoing ? "1" : "0";
            }
            rows.row({std::to_string(ids[i]), got.found ? "1" : "0", got.type, one_line(got.text),
                      outgoing, want.cls, want.kind, v});
            ++counts[want.cls + "/" + want.kind + " -> " + v];
            ++by_class[want.cls + "/" + want.kind];
            found_class[want.cls + "/" + want.kind] += got.found ? 1 : 0;
        }
        ++batch_no;
        batch_ms.push_back(ms);
        batches.row({tag, std::to_string(batch_no), std::to_string(from), std::to_string(to),
                     fmt(ms, 0), std::to_string(non_null),
                     std::to_string(resilience.flood_events())});
        if (batch_no % 10 == 0) {
            log_line("scan " + tag + ": " + std::to_string(to) + "/" + std::to_string(last));
        }
        if (e3.max_batches > 0 && batch_no >= e3.max_batches) {
            break;
        }
        pacer.done(e3.pause_ms);
        pacer.wait();
    }
    const double total_s = ms_since(t_scan) / 1000.0;

    Csv summary(names.out / "e3_scan_summary.csv", {"tag", "metric", "value"});
    const auto put = [&](const std::string& metric, const std::string& value) {
        summary.row({tag, metric, value});
        std::cout << tag << ": " << metric << " = " << value << "\n";
    };
    put("ids_scanned", std::to_string(std::min<int64_t>(last, int64_t{batch_no} * 100)));
    put("batches", std::to_string(batch_no));
    put("pause_ms", std::to_string(e3.pause_ms));
    put("scan_total_s", fmt(total_s, 1));
    const Summary bs = summarize(batch_ms);
    put("batch_ms_median", fmt(bs.median, 0));
    put("batch_ms_min", fmt(bs.min, 0));
    put("batch_ms_max", fmt(bs.max, 0));
    put("flood_events", std::to_string(resilience.flood_events()));
    put("flood_retry_after_total_s", fmt(resilience.flood_wait_s(), 0));
    put("net_errors", std::to_string(resilience.net_errors()));
    put("login_ms", fmt(login_ms, 0));
    put("get_chat_known_ms", fmt(*known, 0));
    for (const auto& [key, n] : by_class) {
        put("expected " + key, std::to_string(n));
        put("returned " + key, std::to_string(found_class[key]));
    }
    for (const auto& [key, n] : counts) {
        put("verdict " + key, std::to_string(n));
    }
    int64_t alive = 0;
    int64_t alive_ok = 0;
    for (const auto& [key, n] : counts) {
        if (key.starts_with("alive/")) {
            alive += n;
            alive_ok += key.ends_with("-> ok") ? n : 0;
        }
    }
    put("alive_recall",
        alive > 0
            ? fmt(100.0 * static_cast<double>(alive_ok) / static_cast<double>(alive), 2) + " %"
            : "-");
    bot.reset();
    if (e3.fresh) {
        remove_fresh_bot_dir(dir);
    }
}

// ---- status и teardown ---------------------------------------------------------------------

void cmd_status(const BenchOptions& options, const E3Options& e3) {
    const Names names = names_for(options, e3);
    const Ledger ledger(names.out / "e3_ledger.csv", false);
    std::map<std::string, int64_t> alive;
    std::map<std::string, int64_t> deleted;
    for (const auto& [sid, e] : ledger.entries()) {
        ++(e.deleted ? deleted : alive)[e.kind];
    }
    std::cout << "max server id: " << ledger.max_id() << " (target " << e3.target << ")\n"
              << "bot messages (docs and texts): " << ledger.bot_sends()
              << ", commits (idx): " << ledger.idx_sends() << ", pins: " << ledger.pins()
              << ", service messages seen: " << ledger.services().size()
              << ", uncertain sends: " << ledger.uncertain_sends() << "\n"
              << "deleted: " << ledger.deleted_count()
              << ", delete rounds: " << ledger.delete_rounds() << "\n";
    for (const auto& [kind, n] : alive) {
        std::cout << "alive " << kind << ": " << n << "\n";
    }
    for (const auto& [kind, n] : deleted) {
        std::cout << "deleted " << kind << ": " << n << "\n";
    }
    if (has_state(options, names.prefix)) {
        const td::Json state = load_state(options, names.prefix);
        std::cout << "state: bot_added=" << state.value("bot_added", false)
                  << " fill_done=" << state.value("fill_done", false)
                  << " updated=" << state.value("updated", std::string{"-"}) << "\n";
    }
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E3Options& e3) {
    const Names names = names_for(options, e3);
    const SessionLock lock(names.lock);
    const td::Json state = load_state(options, names.prefix);
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    FloodGuard guard(std::cerr, names.out / "e3_flood.csv", "e3-teardown");
    auto user = env.open(Account::kUser, std::cerr);
    std::string error;
    wait_chat_known(user->client(), chat_id, seconds{30}, &error);
    // Право удалить канал приходит с полной информацией о нём: без неё deleteChat отвечает
    // «The chat can't be deleted».
    try {
        const td::Json chat =
            user->client().call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
        const td::Json full = user->client().call(
            td::Json{{"@type", "getSupergroupFullInfo"},
                     {"supergroup_id", chat["type"].value("supergroup_id", int64_t{0})}});
        std::cerr << "can_be_deleted: " << (full.value("can_be_deleted", false) ? "yes" : "no")
                  << "\n";
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
            // После старта процесса чат бывает неизвестен пользователю: подождать и повторить.
            if (attempt_no >= 5 || e.error().code != 400) {
                throw;
            }
            std::this_thread::sleep_for(seconds{3});
            wait_chat_known(user->client(), chat_id, seconds{20}, &error);
        }
    }
    bump_counter(options, names.prefix + ".user_actions", 1);
    user.reset();
    remove_fresh_bot_dir(names.session);
    remove_fresh_bot_dir(names.fresh);
    remove_fresh_bot_dir(names.docs);
    fs::remove(options.state_dir / (names.prefix + ".json"));
    std::cout << "channel deleted, bot session directories removed\n";
}

}  // namespace

void add_e3_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("e3", "Experiment E3: reading old messages by id (SPEC 17.6)");
    cmd->require_subcommand(1);
    auto e3 = std::make_shared<E3Options>();
    e3->out = default_out_dir("03-e3-history-scan");
    cmd->add_flag("--probe", e3->probe, "Trial run: separate channel, session and data (probe/)");
    cmd->add_option("--out", e3->out, "Directory for CSV data")->capture_default_str();
    cmd->add_option("--rights", e3->rights, "Bot rights set")->capture_default_str();

    // Значения пробного прогона применяются, если опция не задана явно.
    auto fixups = std::make_shared<std::vector<std::function<void()>>>();
    const auto tunable = [&](const char* name, int& field, const char* help, int probe_value) {
        auto* opt = cmd->add_option(name, field, help)->capture_default_str();
        int* target = &field;
        fixups->emplace_back([e3, opt, target, probe_value] {
            if (e3->probe && opt->count() == 0) {
                *target = probe_value;
            }
        });
    };
    tunable("--user-messages", e3->user_messages, "Texts the user posts before adding the bot", 5);
    tunable("--user-deletes", e3->user_deletes, "User messages the bot tries to delete", 2);
    tunable("--target", e3->target, "Fill until the maximal server id reaches this", 100);
    tunable("--commit-every", e3->commit_every, "Commit (index, pin, unpin) per N bot messages",
            15);
    tunable("--delete-every", e3->delete_every, "Delete round per N bot messages", 20);
    tunable("--delete-batch", e3->delete_batch, "Messages per delete round", 3);
    cmd->add_option("--user-interval-ms", e3->user_interval_ms, "Interval between user messages")
        ->capture_default_str();
    cmd->add_option("--send-interval-ms", e3->send_interval_ms, "Interval after a send")
        ->capture_default_str();
    cmd->add_option("--pin-interval-ms", e3->pin_interval_ms,
                    "Interval after pin, unpin, delete and read")
        ->capture_default_str();

    cmd->add_subcommand("setup", "User creates the channel, posts texts, adds the bot")
        ->callback([&options, exit_code, e3, fixups] {
            for (const auto& f : *fixups) {
                f();
            }
            install_signal_handlers();
            BenchEnv env(options);
            cmd_setup(env, options, *e3);
            *exit_code = 0;
        });
    cmd->add_subcommand("fill", "Bot fills the channel (resumable, run it in the background)")
        ->callback([&options, exit_code, e3, fixups] {
            for (const auto& f : *fixups) {
                f();
            }
            install_signal_handlers();
            BenchOptions own = options;
            own.tdlib_log =
                options.state_dir / ("tdlib-" + names_for(options, *e3).prefix + ".log");
            BenchEnv env(own);
            const Names names = names_for(own, *e3);
            const SessionLock lock(names.lock);
            Filler filler(env, own, *e3, names);
            *exit_code = filler.run();
        });
    auto* scan = cmd->add_subcommand("scan", "Bot scans ids 1..last+margin with getMessages");
    scan->add_flag("--fresh", e3->fresh, "Start the bot with an empty TDLib database");
    scan->add_option("--pause-ms", e3->pause_ms, "Pause between batches of 100")
        ->capture_default_str();
    scan->add_option("--max-batches", e3->max_batches, "Stop after N batches (pause check)");
    scan->add_option("--margin", e3->margin, "Ids to check beyond the last one")
        ->capture_default_str();
    scan->add_option("--tag", e3->tag, "Suffix of the output file name");
    scan->callback([&options, exit_code, e3, fixups] {
        for (const auto& f : *fixups) {
            f();
        }
        install_signal_handlers();
        BenchOptions own = options;
        own.tdlib_log = options.state_dir / ("tdlib-" + names_for(options, *e3).prefix + ".log");
        BenchEnv env(own);
        cmd_scan(env, own, *e3);
        *exit_code = 0;
    });
    cmd->add_subcommand("status", "What the ledger says")
        ->callback([&options, exit_code, e3, fixups] {
            for (const auto& f : *fixups) {
                f();
            }
            cmd_status(options, *e3);
            *exit_code = 0;
        });
    cmd->add_subcommand("teardown", "Delete the channel (user account)")
        ->callback([&options, exit_code, e3, fixups] {
            for (const auto& f : *fixups) {
                f();
            }
            install_signal_handlers();
            BenchEnv env(options);
            cmd_teardown(env, options, *e3);
            *exit_code = 0;
        });
}

}  // namespace telega::bench
