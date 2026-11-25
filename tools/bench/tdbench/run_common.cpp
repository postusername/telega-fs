#include "run_common.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <ctime>
#include <fstream>
#include <iostream>
#include <thread>

namespace telega::bench::run {

namespace {

constexpr int64_t kMaxFloodWaitSeconds = 3600;  // больше часа — остановиться

std::atomic<bool> g_stop{false};

extern "C" void run_on_stop_signal(int /*sig*/) { g_stop.store(true); }

}  // namespace

bool stopping() { return g_stop.load(); }

void install_signal_handlers() {
    struct sigaction sa {};
    sa.sa_handler = run_on_stop_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

void sleep_interruptible(std::chrono::milliseconds duration) {
    const auto end = ExpClock::now() + duration;
    while (ExpClock::now() < end) {
        if (stopping()) {
            throw Interrupted();
        }
        const auto left =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - ExpClock::now());
        std::this_thread::sleep_for(std::min(left, std::chrono::milliseconds{500}));
    }
    if (stopping()) {
        throw Interrupted();
    }
}

void log_line(const std::string& text) { std::cerr << iso_utc_now() << " " << text << "\n"; }

int64_t epoch_now() { return static_cast<int64_t>(std::time(nullptr)); }

double seconds_between(std::chrono::system_clock::time_point a,
                       std::chrono::system_clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

void wait_until_epoch(int64_t epoch, const std::string& why) {
    int64_t last_log = 0;
    while (epoch_now() < epoch) {
        if (stopping()) {
            throw Interrupted();
        }
        if (epoch_now() - last_log >= 300) {
            last_log = epoch_now();
            log_line(why + ": " + std::to_string(epoch - epoch_now()) + " s left");
        }
        sleep_interruptible(std::chrono::seconds{std::min<int64_t>(epoch - epoch_now(), 30)});
    }
}

SessionLock::SessionLock(const std::filesystem::path& path, const std::string& experiment) {
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (fd_ < 0) {
        throw std::runtime_error("cannot open lock file " + path.string());
    }
    if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("another " + experiment + " process uses this bot session (lock " +
                                 path.string() + ")");
    }
}

SessionLock::~SessionLock() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

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

std::vector<std::map<std::string, std::string>> read_csv(const std::filesystem::path& path) {
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

RunState::RunState(const BenchOptions& options, std::string name)
    : options_(options), name_(std::move(name)) {
    if (has_state(options_, name_)) {
        j_ = load_state(options_, name_);
        j_["next_doc_no"] = j_.value("next_doc_no", int64_t{0}) + 1000;
    } else {
        j_ = td::Json{
            {"series", td::Json::object()}, {"quiet_until", 0}, {"hold_until", 0},
            {"last_flood_end", 0},          {"next_doc_no", 0}, {"created", iso_utc_now()}};
    }
    save();
}

bool RunState::is_done(const std::string& id) const {
    return j_["series"].contains(id) && j_["series"][id].is_object() &&
           j_["series"][id].value("status", std::string{}) == "done";
}

uint64_t RunState::take_doc_no() {
    const auto n = j_["next_doc_no"].get<int64_t>();
    j_["next_doc_no"] = n + 1;
    if (n % 50 == 0) {
        save();
    }
    return static_cast<uint64_t>(n);
}

void RunState::save() {
    j_["updated"] = iso_utc_now();
    save_state(options_, name_, j_);
}

Guard::Guard(const std::filesystem::path& out, const std::string& prefix,
             std::vector<std::string> flood_header)
    : flood_(out / (prefix + "_flood.csv"), flood_header),
      net_(out / (prefix + "_net_errors.csv"),
           {"time_utc", "series", "phase", "what", "error", "backoff_s"}),
      flood_columns_(flood_header.size()) {}

void Guard::on_error(const std::string& what, const td::TdError& error, int* transient) {
    if (error.code == 401 || error.message.find("USER_DEACTIVATED") != std::string::npos ||
        error.message.find("AUTH_KEY") != std::string::npos) {
        throw StopExperiment("account problem at '" + what + "': " + error.to_string());
    }
    if (error.code == td::TdError::kClosed || error.code == td::TdError::kStopped) {
        throw SessionError("client closed at '" + what + "'");
    }
    if (error.is_rate_limit()) {
        const int64_t s = error.retry_after ? error.retry_after->count() : 60;
        const int64_t wait_s = s + s / 5 + 5;
        // Строка вне ступени: заполнены время, серия, фаза, retry_after, ожидание и что.
        std::vector<std::string> row(flood_columns_);
        row[0] = iso_utc_now();
        row[1] = series_;
        row[2] = "housekeeping";
        row[3] = std::to_string(s);
        row[4] = std::to_string(wait_s);
        row.back() = what;
        flood_.row(row);
        if (s > kMaxFloodWaitSeconds) {
            throw StopExperiment("FLOOD_WAIT of " + std::to_string(s) + " s at '" + what +
                                 "': stopping, see the flood csv");
        }
        log_line("FLOOD_WAIT " + std::to_string(s) + " s at '" + what + "': waiting " +
                 std::to_string(wait_s) + " s");
        sleep_interruptible(std::chrono::seconds{wait_s});
    } else if (error.is_local() || error.code >= 500) {
        const int64_t backoff = std::min<int64_t>(300, 15LL << std::min(*transient, 4));
        ++*transient;
        net_.row({iso_utc_now(), series_, "housekeeping", what, error.to_string(),
                  std::to_string(backoff)});
        log_line("network/server error at '" + what + "': " + error.to_string() + "; retry in " +
                 std::to_string(backoff) + " s");
        sleep_interruptible(std::chrono::seconds{backoff});
    } else {
        throw td::TdException(error);
    }
}

}  // namespace telega::bench::run
