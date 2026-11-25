// e6: альбомы в режиме бота. Канал создаёт пользователь (setup, сообщений от
// пользователя нет), всю нагрузку несёт бот в отдельном каталоге сессии <state-dir>/bot-e6.
//
//   tgfs-bench e6 [--probe] setup      пользователь создаёт канал и добавляет бота администратором
//   tgfs-bench e6 [--probe] run        серии отправки: возобновляемо, запускать в фоне
//   tgfs-bench e6 [--probe] status     состояние серий и счётчики
//   tgfs-bench e6 [--probe] teardown   удалить канал (пользователь) и каталоги сессии бота
//
// Серии:
//   - `fail-albums`: альбомы по 10 документов, в каждом один документ заведомо не уйдёт (файл
//     удалён или пуст до отправки; файл испорчен сразу после ответа на sendMessageAlbum).
//     Смотрим, приходят ли апдейты на каждый документ отдельно и что с остальными девятью;
//   - `single-1MiB-p2`: короткая лестница одиночных документов 1 МиБ (в E5 её не было), база для
//     сравнения «в 1,5 раза»; для 16 МиБ базовая линия из E5;
//   - `album-16MiB-p2` и `album-1MiB-p2`: лестницы альбомов по 10 документов. Темп считается в
//     документах в минуту: по месту FLOOD_WAIT и по числу альбомов и документов за минуту до
//     него видно, считается альбом одним действием или десятью. Ступень обрывается на первом
//     FLOOD_WAIT, серия на этом заканчивается; bandwidth_bound и disturbed как в E5.
// После каждой серии бот удаляет её документы пачками по 100, между сериями пауза не меньше
// 10 минут. Всё для отчёта пишется в CSV (без id канала и username); id канала только в
// <state-dir>/e6.json.

#include <sys/stat.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <filesystem>
#include <fstream>
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
#include "run_common.h"
#include "send_load.h"

namespace telega::bench {

namespace {

namespace fs = std::filesystem;
using run::epoch_now;
using run::Interrupted;
using run::log_line;
using run::seconds_between;
using run::SessionError;
using run::sleep_interruptible;
using run::stopping;
using std::chrono::milliseconds;
using std::chrono::seconds;
using WallClock = std::chrono::system_clock;

constexpr int64_t kServerIdShift = 20;
constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * kKiB;
constexpr uint64_t kSeed = 0xE6E6E6E6ULL;
constexpr int64_t kMaxFloodWaitSeconds = 3600;  // больше часа — остановиться
constexpr double kBandwidthShare = 0.8;
constexpr int kMaxStepAttempts = 3;
constexpr int kDeleteBatch = 100;
constexpr int kAlbumSize = 10;

int64_t server_id(int64_t message_id) { return message_id >> kServerIdShift; }

std::string size_name(uint64_t size) {
    return size >= kMiB ? std::to_string(size / kMiB) + "MiB" : std::to_string(size / kKiB) + "KiB";
}

std::string date_stamp() {
    const std::string iso = iso_utc_now();
    return iso.substr(0, 4) + iso.substr(5, 2) + iso.substr(8, 2);
}

// ---- Параметры, имена, план ---------------------------------------------------------------

struct E6Options {
    bool probe = false;
    fs::path out;
    std::string rights = "only:post,edit,delete";
    int step_seconds = 600;
    int series_pause_seconds = 600;
    int case_gap_seconds = 45;  // между альбомами серии сбоев
    int failure_reps = 3;
    int delete_interval_ms = 6000;
};

struct Names {
    std::string prefix;
    std::string run_state;
    fs::path out;
    fs::path session;
    fs::path lock;
    fs::path docs;
};

Names names_for(const BenchOptions& options, const E6Options& e6) {
    Names n;
    n.prefix = "e6";
    n.run_state = e6.probe ? "e6p-run" : "e6-run";
    n.out = e6.probe ? e6.out / "probe" : e6.out;
    n.session = options.state_dir / "bot-e6";
    n.lock = options.state_dir / "e6-run.lock";
    n.docs = options.state_dir / "e6-docs";
    return n;
}

enum class SeriesKind : uint8_t { kLadder, kFailure };

struct SeriesSpec {
    std::string id;
    SeriesKind kind = SeriesKind::kLadder;
    uint64_t size = 0;  // размер документа
    int group = 1;  // документов в задании: 1 — sendMessage, 10 — sendMessageAlbum
    int parallel = 2;        // заданий одновременно
    std::vector<int> rates;  // документов в минуту по ступеням
};

std::vector<SeriesSpec> build_plan(const E6Options& e6) {
    std::vector<SeriesSpec> plan;
    plan.push_back({"fail-albums", SeriesKind::kFailure, 0, kAlbumSize, 1, {}});
    if (e6.probe) {
        plan.push_back({"single-1MiB-p2", SeriesKind::kLadder, kMiB, 1, 2, {5}});
        plan.push_back({"album-1MiB-p2", SeriesKind::kLadder, kMiB, kAlbumSize, 2, {10}});
        plan.push_back({"album-16MiB-p2", SeriesKind::kLadder, 16 * kMiB, kAlbumSize, 2, {10}});
        return plan;
    }
    plan.push_back({"single-1MiB-p2", SeriesKind::kLadder, kMiB, 1, 2, {10, 15, 18, 20, 30}});
    plan.push_back({"album-16MiB-p2",
                    SeriesKind::kLadder,
                    16 * kMiB,
                    kAlbumSize,
                    2,
                    {10, 15, 20, 25, 30, 40, 60}});
    plan.push_back({"album-1MiB-p2",
                    SeriesKind::kLadder,
                    kMiB,
                    kAlbumSize,
                    2,
                    {10, 20, 40, 80, 120, 160, 200, 250, 300, 400, 500}});
    return plan;
}

// Варианты сбоя одного документа альбома (индекс bad).
enum class Break : uint8_t {
    kMissingBefore,
    kEmptyBefore,
    kDeleteAfter,
    kChmodAfter,
    kTruncateAfter
};

struct Variant {
    const char* name;
    Break how;
    uint64_t size;
    int bad;
};

const std::vector<Variant>& variants() {
    static const std::vector<Variant> v{
        {"missing-before-call", Break::kMissingBefore, kMiB, 4},
        {"empty-before-call", Break::kEmptyBefore, kMiB, 4},
        {"delete-after-call", Break::kDeleteAfter, 16 * kMiB, 9},
        {"delete-after-call-first", Break::kDeleteAfter, 16 * kMiB, 0},
        {"chmod000-after-call", Break::kChmodAfter, 16 * kMiB, 9},
        {"truncate-after-call", Break::kTruncateAfter, 16 * kMiB, 9},
    };
    return v;
}

// ---- Статистика ---------------------------------------------------------------------------

struct StepOutcome {
    std::string verdict;  // clean | flood | bandwidth_bound | disturbed | interrupted
    int64_t flood_retry_s = 0;
    WallClock::time_point flood_at;
    double actual_rate = 0;  // запущено документов в минуту
};

struct StepStats {
    int planned_jobs = 0;
    int launched_jobs = 0;
    int launched_docs = 0;
    int docs_ok = 0;
    int docs_flood = 0;
    int docs_other = 0;  // не доставлены по другим причинам
    int jobs_ok = 0;
    int jobs_partial = 0;
    int jobs_failed = 0;
    int net = 0;
    int timeouts = 0;
    int aborted = 0;
    uint64_t bytes_ok = 0;
    std::vector<double> job_durations;  // с
    std::vector<double> spreads;  // разброс прихода апдейтов внутри задания, мс
    double slot_wait_s = 0;
    int64_t max_retry_s = 0;
    std::optional<WallClock::time_point> flood_at;
};

// Что случилось с одним заданием (для серии сбоев).
struct JobSummary {
    uint64_t job = 0;
    int docs = 0;
    int ok = 0;
    int flood = 0;
    int other = 0;
    bool call_ok = true;
    std::string call_error;
    std::string failed_indexes;
    std::string first_error;
    double first_ms = 0;
    double last_ms = 0;
    int distinct_albums = 0;
    std::string ok_ids_contiguous;
};

struct StepContext {
    const SeriesSpec* spec = nullptr;
    int step = 0;
    int rate = 0;
    int attempt = 0;
    StepStats stats;
    struct Launch {
        WallClock::time_point at;
        int docs;
    };
    struct Completion {
        WallClock::time_point at;
        uint64_t bytes;
        int docs;
    };
    std::deque<Launch> launches;
    std::deque<Completion> completions;
    std::vector<JobSummary> summaries;
    WallClock::time_point start_wall;
    ExpClock::time_point t0;
    bool tolerate_failures = false;
};

class Runner {
  public:
    Runner(BenchEnv& env, const BenchOptions& options, const E6Options& e6, const Names& names)
        : env_(env),
          options_(options),
          e6_(e6),
          names_(names),
          setup_(load_state(options, names.prefix)),
          chat_id_(setup_.at("chat_id").get<int64_t>()),
          state_(options, names.run_state),
          plan_(build_plan(e6)),
          guard_(names.out, "e6",
                 {"time_utc",
                  "series",
                  "phase",
                  "retry_after_s",
                  "wait_s",
                  "size_bytes",
                  "group",
                  "parallel",
                  "rate_docs_per_min",
                  "step",
                  "attempt",
                  "offset_in_step_s",
                  "launched_jobs_60s",
                  "launched_docs_60s",
                  "completed_docs_60s",
                  "completed_mib_60s",
                  "in_flight_jobs",
                  "docs_flood_in_job",
                  "docs_in_job",
                  "what"}),
          docs_csv_(names.out / "e6_docs.csv",
                    {"series", "step", "attempt", "rate_docs_per_min", "job", "index", "size_bytes",
                     "group", "parallel", "start_utc", "update_ms", "result", "retry_after_s",
                     "error", "server_id", "album_id"}),
          jobs_csv_(names.out / "e6_jobs.csv", {"series",
                                                "step",
                                                "attempt",
                                                "rate_docs_per_min",
                                                "job",
                                                "group",
                                                "size_bytes",
                                                "start_utc",
                                                "end_utc",
                                                "prepare_ms",
                                                "call_ms",
                                                "call_error",
                                                "docs_ok",
                                                "docs_flood",
                                                "docs_other_failed",
                                                "first_update_ms",
                                                "last_update_ms",
                                                "update_spread_ms",
                                                "distinct_album_ids",
                                                "in_flight_at_start",
                                                "result"}),
          steps_csv_(names.out / "e6_steps.csv", {"series",
                                                  "group",
                                                  "size_bytes",
                                                  "parallel",
                                                  "step",
                                                  "attempt",
                                                  "rate_docs_per_min",
                                                  "rate_jobs_per_min",
                                                  "start_utc",
                                                  "end_utc",
                                                  "window_s",
                                                  "planned_jobs",
                                                  "launched_jobs",
                                                  "launched_docs",
                                                  "docs_ok",
                                                  "docs_flood",
                                                  "docs_other_failed",
                                                  "jobs_ok",
                                                  "jobs_partial",
                                                  "jobs_failed",
                                                  "net_errors",
                                                  "timeouts",
                                                  "aborted",
                                                  "actual_docs_per_min",
                                                  "mib_ok",
                                                  "aggregate_mib_s",
                                                  "job_duration_median_s",
                                                  "job_duration_max_s",
                                                  "update_spread_median_ms",
                                                  "slot_wait_s",
                                                  "flood_retry_s",
                                                  "verdict"}),
          failures_csv_(
              names.out / "e6_failures.csv",
              {"variant", "rep", "size_bytes", "group", "bad_index", "call_ok", "call_error",
               "docs_ok", "docs_failed", "failed_indexes", "first_error", "first_update_ms",
               "last_update_ms", "distinct_album_ids_among_ok", "ok_server_ids_contiguous"}),
          series_csv_(names.out / "e6_series.csv",
                      {"series", "group", "size_bytes", "parallel", "result", "max_clean_rate",
                       "flood_rate", "flood_retry_s", "bandwidth_bound_rate", "steps_run",
                       "started_utc", "ended_utc", "deleted", "note"}),
          progress_csv_(names.out / "e6_progress.csv",
                        {"time_utc", "series", "step", "rate_docs_per_min", "elapsed_s",
                         "launched_jobs", "launched_docs", "docs_ok", "docs_flood", "errors",
                         "in_flight_jobs", "actual_docs_per_min"}),
          deleted_csv_(names.out / "e6_deleted.csv",
                       {"time_utc", "series", "server_id", "result", "error"}) {
        if (!setup_.value("bot_added", false)) {
            throw std::runtime_error("run `e6 setup` first (the bot is not added yet)");
        }
        fs::create_directories(names.docs);
        load_pending();
    }

    // 0: всё готово или остановка по сигналу, 1: ошибка, 2: остановка, решает человек.
    int run() {
        log_line("e6 starts: " + std::to_string(plan_.size()) + " series in the plan, " +
                 std::to_string(pending_.size()) + " documents wait for deletion");
        int failures = 0;
        for (;;) {
            const int64_t before = steps_done_;
            try {
                run_session();
                finish();
                return 0;
            } catch (const Interrupted&) {
                log_line("interrupted by signal; state is saved, rerun `e6 run` to resume");
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
                failures = steps_done_ > before ? 1 : failures + 1;
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
    void close_session() {
        load_.reset();
        bot_.reset();
    }

    void finish() {
        bump_counter(options_, "e6.bot_messages_sent", session_sends_);
        session_sends_ = 0;
        close_session();
        int done = 0;
        for (const auto& spec : plan_) {
            done += state_.is_done(spec.id) ? 1 : 0;
        }
        log_line("e6 done: " + std::to_string(done) + " of " + std::to_string(plan_.size()) +
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
            if (!state_.is_done(spec.id)) {
                run_series(spec);
            }
        }
    }

    // ---- серия ----

    void run_series(const SeriesSpec& spec) {
        td::Json& st = state_.series(spec.id);
        guard_.set_series(spec.id);
        if (st.is_null()) {
            st =
                td::Json{{"status", "pending"}, {"next_step", 0}, {"attempts", td::Json::object()}};
        }
        if (!st.value("finished", false)) {
            run::wait_until_epoch(state_.root().value("quiet_until", int64_t{0}),
                                  "pause before series " + spec.id);
            if (!st.contains("started")) {
                st["started"] = iso_utc_now();
            }
            st["status"] = "running";
            state_.save();
            log_line("series " + spec.id + " starts (group " + std::to_string(spec.group) +
                     ", size " + size_name(spec.size) + ", jobs in flight " +
                     std::to_string(spec.parallel) + ")");
            SendLoad::Options lo;
            lo.parallelism = spec.parallel;
            load_ = std::make_unique<SendLoad>(*bot_, chat_id_, lo);
            if (spec.kind == SeriesKind::kFailure) {
                run_failure_series(spec, st);
            } else {
                run_ladder_series(spec, st);
            }
            load_.reset();
            st["finished"] = true;
            state_.save();
        }
        finalize_series(spec, st);
    }

    void run_ladder_series(const SeriesSpec& spec, td::Json& st) {
        const std::vector<int>& rates = spec.rates;
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
                             " docs/min was disturbed (network?), repeating it");
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
                st["note"] = "step " + std::to_string(rates[i]) + " docs/min disturbed " +
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
        // Выждать с запасом (20 % и 5 с), потом удаление и следующая серия.
        state_.root()["hold_until"] = flood_epoch + out.flood_retry_s + out.flood_retry_s / 5 + 5;
        state_.save();
    }

    void finalize_series(const SeriesSpec& spec, td::Json& st) {
        run::wait_until_epoch(state_.root().value("hold_until", int64_t{0}),
                              "waiting out FLOOD_WAIT of " + spec.id);
        const int deleted = delete_pending(spec.id);
        st["status"] = "done";
        st["ended"] = iso_utc_now();
        state_.root()["quiet_until"] = epoch_now() + e6_.series_pause_seconds;
        series_row(spec, st, deleted);
        bump_counter(options_, "e6.bot_messages_sent", session_sends_);
        session_sends_ = 0;
        state_.save();
        log_line("series " + spec.id + " done: " + st.value("result", std::string{"?"}) +
                 ", max clean rate " + std::to_string(st.value("max_clean_rate", 0)) +
                 " docs/min, deleted " + std::to_string(deleted));
    }

    static std::string opt_int(const td::Json& st, const char* key) {
        return st.contains(key) ? std::to_string(st[key].get<int64_t>()) : "";
    }

    void series_row(const SeriesSpec& spec, const td::Json& st, int deleted) {
        series_csv_.row({spec.id, std::to_string(spec.group), std::to_string(spec.size),
                         std::to_string(spec.parallel), st.value("result", std::string{"?"}),
                         opt_int(st, "max_clean_rate"), opt_int(st, "flood_rate"),
                         opt_int(st, "flood_retry_s"), opt_int(st, "bandwidth_bound_rate"),
                         std::to_string(st.value("steps_run", 0)),
                         st.value("started", std::string{}), iso_utc_now(), std::to_string(deleted),
                         st.value("note", std::string{})});
    }

    // ---- разбор итогов ----

    static int count_launches(const StepContext& ctx, WallClock::time_point t, double window_s,
                              bool docs) {
        int n = 0;
        for (const auto& l : ctx.launches) {
            const double d = seconds_between(l.at, t);
            if (d >= 0 && d <= window_s) {
                n += docs ? l.docs : 1;
            }
        }
        return n;
    }

    static std::pair<int, double> completed_since(const StepContext& ctx, WallClock::time_point t,
                                                  double window_s) {
        int docs = 0;
        double bytes = 0;
        for (const auto& c : ctx.completions) {
            const double d = seconds_between(c.at, t);
            if (d >= 0 && d <= window_s) {
                docs += c.docs;
                bytes += static_cast<double>(c.bytes);
            }
        }
        return {docs, bytes / static_cast<double>(kMiB)};
    }

    void collect(StepContext& ctx) {
        for (const SendResult& r : load_->take_finished()) {
            consume(ctx, r);
        }
    }

    void consume(StepContext& ctx, const SendResult& r) {
        StepStats& s = ctx.stats;
        const size_t n = r.docs.size();
        JobSummary js;
        js.job = r.job.id;
        js.docs = static_cast<int>(n);
        js.call_ok = r.call_error.empty();
        js.call_error = r.call_error;
        js.first_ms = 1e18;
        std::set<int64_t> albums;
        std::vector<int64_t> ok_ids;
        uint64_t bytes_ok = 0;
        const DocOutcome* flood_doc = nullptr;
        for (size_t i = 0; i < n; ++i) {
            const DocOutcome& d = r.docs[i];
            const int64_t sid = d.message_id != 0 ? server_id(d.message_id) : 0;
            docs_csv_.row({ctx.spec->id, std::to_string(ctx.step), std::to_string(ctx.attempt),
                           std::to_string(ctx.rate), std::to_string(r.job.id), std::to_string(i),
                           std::to_string(r.sizes[i]), std::to_string(ctx.spec->group),
                           std::to_string(ctx.spec->parallel), iso_utc_ms(r.start_wall),
                           fmt(d.done_ms, 1), send_kind_name(d.kind),
                           d.retry_after_s != 0 ? std::to_string(d.retry_after_s) : "", d.error,
                           sid != 0 ? std::to_string(sid) : "",
                           d.album_id != 0 ? std::to_string(d.album_id) : ""});
            js.first_ms = std::min(js.first_ms, d.done_ms);
            js.last_ms = std::max(js.last_ms, d.done_ms);
            if (d.kind == SendKind::kOk) {
                ++js.ok;
                bytes_ok += r.sizes[i];
                ok_ids.push_back(sid);
                albums.insert(d.album_id);
                pending_.insert(sid);
                sent_this_session_.insert(sid);
                ++session_sends_;
            } else {
                if (!js.failed_indexes.empty()) {
                    js.failed_indexes += " ";
                }
                js.failed_indexes += std::to_string(i);
                if (js.first_error.empty()) {
                    js.first_error = d.error;
                }
                if (d.kind == SendKind::kFlood) {
                    ++js.flood;
                    s.max_retry_s = std::max(s.max_retry_s, d.retry_after_s);
                    flood_doc = flood_doc != nullptr ? flood_doc : &d;
                } else {
                    ++js.other;
                }
            }
        }
        if (js.first_ms > 1e17) {
            js.first_ms = 0;
        }
        js.distinct_albums = static_cast<int>(albums.size());
        std::ranges::sort(ok_ids);
        bool contiguous = ok_ids.size() > 1;
        for (size_t i = 1; i < ok_ids.size(); ++i) {
            // Каждое сообщение альбома — свой серверный id; закрепление в этом канале не идёт.
            contiguous = contiguous && ok_ids[i] - ok_ids[i - 1] == 1;
        }
        js.ok_ids_contiguous = ok_ids.size() > 1 ? (contiguous ? "yes" : "no") : "";

        const double dur_s = r.duration_ms / 1000.0;
        std::string result = js.call_ok ? (js.ok == js.docs ? "ok" : "partial") : "call_error";
        if (js.ok == 0 && js.call_ok) {
            result = "failed";
        }
        jobs_csv_.row({ctx.spec->id,
                       std::to_string(ctx.step),
                       std::to_string(ctx.attempt),
                       std::to_string(ctx.rate),
                       std::to_string(r.job.id),
                       std::to_string(ctx.spec->group),
                       std::to_string(r.sizes.empty() ? 0 : r.sizes[0]),
                       iso_utc_ms(r.start_wall),
                       iso_utc_ms(r.end_wall),
                       fmt(r.prepare_ms, 0),
                       fmt(r.call_ms, 0),
                       r.call_error,
                       std::to_string(js.ok),
                       std::to_string(js.flood),
                       std::to_string(js.other),
                       fmt(js.first_ms, 1),
                       fmt(js.last_ms, 1),
                       fmt(js.last_ms - js.first_ms, 1),
                       std::to_string(js.distinct_albums),
                       std::to_string(r.in_flight_at_start),
                       result});

        s.docs_ok += js.ok;
        s.docs_flood += js.flood;
        s.docs_other += js.other;
        s.bytes_ok += bytes_ok;
        s.job_durations.push_back(dur_s);
        if (n > 1 && js.ok > 1) {
            s.spreads.push_back(js.last_ms - js.first_ms);
        }
        if (result == "ok") {
            ++s.jobs_ok;
        } else if (js.ok > 0) {
            ++s.jobs_partial;
        } else {
            ++s.jobs_failed;
        }
        if (js.ok > 0) {
            ctx.completions.push_back({r.end_wall, bytes_ok, js.ok});
        }
        ctx.summaries.push_back(js);

        if (flood_doc != nullptr && !s.flood_at) {
            s.flood_at = r.end_wall;
            record_flood(ctx, r, js, *flood_doc);
        }
        for (const DocOutcome& d : r.docs) {
            switch (d.kind) {
                case SendKind::kNet:
                case SendKind::kTimeout:
                    ++(d.kind == SendKind::kNet ? s.net : s.timeouts);
                    guard_.net().row({iso_utc_ms(r.end_wall), ctx.spec->id, "step",
                                      send_kind_name(d.kind), d.error, ""});
                    log_line(std::string{"upload "} + send_kind_name(d.kind) + ": " + d.error);
                    break;
                case SendKind::kAborted:
                    ++s.aborted;
                    if (!stopping()) {
                        throw SessionError("client closed during a send: " + d.error);
                    }
                    break;
                case SendKind::kAccount:
                    throw StopExperiment("account problem on a send: " + d.error);
                case SendKind::kFatal:
                    if (!ctx.tolerate_failures) {
                        throw StopExperiment("non-retryable error on a send: " + d.error);
                    }
                    break;
                case SendKind::kOk:
                case SendKind::kFlood:
                    break;
            }
        }
    }

    void record_flood(const StepContext& ctx, const SendResult& r, const JobSummary& js,
                      const DocOutcome& d) {
        const double offset = seconds_between(ctx.start_wall, r.end_wall);
        const int jobs60 = count_launches(ctx, r.end_wall, 60, false);
        const int docs60 = count_launches(ctx, r.end_wall, 60, true);
        const auto [done_docs, done_mib] = completed_since(ctx, r.end_wall, 60);
        guard_.flood().row({iso_utc_ms(r.end_wall),
                            ctx.spec->id,
                            "step",
                            std::to_string(d.retry_after_s),
                            std::to_string(d.retry_after_s + d.retry_after_s / 5 + 5),
                            std::to_string(ctx.spec->size),
                            std::to_string(ctx.spec->group),
                            std::to_string(ctx.spec->parallel),
                            std::to_string(ctx.rate),
                            std::to_string(ctx.step),
                            std::to_string(ctx.attempt),
                            fmt(offset, 1),
                            std::to_string(jobs60),
                            std::to_string(docs60),
                            std::to_string(done_docs),
                            fmt(done_mib, 0),
                            std::to_string(r.in_flight_at_end),
                            std::to_string(js.flood),
                            std::to_string(js.docs),
                            "send " + d.error});
        log_line("FLOOD_WAIT " + std::to_string(d.retry_after_s) + " s at " +
                 std::to_string(ctx.rate) + " docs/min after " + fmt(offset, 0) + " s (" +
                 std::to_string(jobs60) + " jobs, " + std::to_string(docs60) +
                 " docs launched in the last minute; " + std::to_string(js.flood) + " of " +
                 std::to_string(js.docs) + " docs of this job failed)");
    }

    // Ждёт завершения всех идущих заданий. После сигнала даёт им 2 минуты, потом прерывает.
    void drain(StepContext& ctx) {
        std::optional<ExpClock::time_point> abort_at;
        while (load_->in_flight() > 0 || load_->wait_event(milliseconds{0})) {
            if (stopping()) {
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
        const double rate = elapsed_s > 0 ? s.launched_docs * 60.0 / elapsed_s : 0;
        log_line("[" + ctx.spec->id + "] step " + std::to_string(ctx.step) + " (" +
                 std::to_string(ctx.rate) + " docs/min, attempt " + std::to_string(ctx.attempt) +
                 "): " + fmt(elapsed_s, 0) + " s, launched " + std::to_string(s.launched_jobs) +
                 " jobs/" + std::to_string(s.launched_docs) + " docs, ok " +
                 std::to_string(s.docs_ok) + ", FLOOD " + std::to_string(s.docs_flood) +
                 ", errors " + std::to_string(s.net + s.timeouts + s.docs_other) + ", in flight " +
                 std::to_string(load_->in_flight()) + ", " + fmt(rate, 1) + " docs/min");
        progress_csv_.row({iso_utc_now(), ctx.spec->id, std::to_string(ctx.step),
                           std::to_string(ctx.rate), fmt(elapsed_s, 0),
                           std::to_string(s.launched_jobs), std::to_string(s.launched_docs),
                           std::to_string(s.docs_ok), std::to_string(s.docs_flood),
                           std::to_string(s.net + s.timeouts + s.docs_other),
                           std::to_string(load_->in_flight()), fmt(rate, 2)});
    }

    // Задание на ступень: group документов, содержимое по seed (номер документа).
    SendJob make_job(const SeriesSpec& spec) {
        SendJob job;
        job.id = state_.take_doc_no();
        std::vector<uint64_t> seeds;
        for (int k = 0; k < spec.group; ++k) {
            seeds.push_back(kSeed ^ (state_.take_doc_no() * 0x9E3779B97F4A7C15ULL));
        }
        const uint64_t size = spec.size;
        const fs::path dir = names_.docs;
        job.nominal_size = size;
        job.prepare = [dir, seeds, size] {
            std::vector<fs::path> files;
            files.reserve(seeds.size());
            for (const uint64_t seed : seeds) {
                files.push_back(make_seeded_document(dir, seed, size));
            }
            return files;
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
        const double interval_s = spec.group * 60.0 / rate;  // между заданиями
        const double window_s = e6_.step_seconds;
        ctx.stats.planned_jobs = static_cast<int>(std::ceil(window_s / interval_s - 1e-9));
        ctx.start_wall = WallClock::now();
        ctx.t0 = ExpClock::now();
        log_line("[" + spec.id + "] step " + std::to_string(step) + ": " + std::to_string(rate) +
                 " docs/min (" + fmt(rate / static_cast<double>(spec.group), 2) +
                 " jobs/min) for " + std::to_string(e6_.step_seconds) + " s (attempt " +
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
            if (stopping()) {
                verdict = "interrupted";
                break;
            }
            if (s.docs_flood > 0) {
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
                s.launched_jobs < kBandwidthShare * (std::floor(elapsed_s / interval_s) + 1)) {
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
                ctx.launches.push_back({WallClock::now(), spec.group});
                ++s.launched_jobs;
                s.launched_docs += spec.group;
                load_->launch(make_job(spec));
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
        out.actual_rate = total_s > 0 ? s.launched_docs * 60.0 / std::min(total_s, window_s) : 0;
        if (verdict.empty()) {
            if (s.docs_flood > 0) {
                verdict = "flood";
                out.flood_retry_s = s.max_retry_s;
                out.flood_at = s.flood_at.value_or(WallClock::now());
            } else if (early_bound ||
                       s.launched_jobs < kBandwidthShare * static_cast<double>(s.planned_jobs)) {
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
                 " docs/min) ended: " + verdict + ", launched " + std::to_string(s.launched_jobs) +
                 "/" + std::to_string(s.planned_jobs) + " jobs, docs ok " +
                 std::to_string(s.docs_ok));
        if (verdict == "interrupted") {
            throw Interrupted();
        }
        if (verdict == "flood" && out.flood_retry_s > kMaxFloodWaitSeconds) {
            throw StopExperiment("FLOOD_WAIT of " + std::to_string(out.flood_retry_s) +
                                 " s: stopping, see e6_flood.csv");
        }
        return out;
    }

    void write_step_row(const StepContext& ctx, WallClock::time_point end_wall, double total_s,
                        const std::string& verdict, const StepOutcome& out) {
        const StepStats& s = ctx.stats;
        const Summary du = summarize(s.job_durations);
        const Summary sp = summarize(s.spreads);
        const double mib = static_cast<double>(s.bytes_ok) / static_cast<double>(kMiB);
        steps_csv_.row({ctx.spec->id,
                        std::to_string(ctx.spec->group),
                        std::to_string(ctx.spec->size),
                        std::to_string(ctx.spec->parallel),
                        std::to_string(ctx.step),
                        std::to_string(ctx.attempt),
                        std::to_string(ctx.rate),
                        fmt(ctx.rate / static_cast<double>(ctx.spec->group), 2),
                        iso_utc_ms(ctx.start_wall),
                        iso_utc_ms(end_wall),
                        fmt(total_s, 1),
                        std::to_string(s.planned_jobs),
                        std::to_string(s.launched_jobs),
                        std::to_string(s.launched_docs),
                        std::to_string(s.docs_ok),
                        std::to_string(s.docs_flood),
                        std::to_string(s.docs_other),
                        std::to_string(s.jobs_ok),
                        std::to_string(s.jobs_partial),
                        std::to_string(s.jobs_failed),
                        std::to_string(s.net),
                        std::to_string(s.timeouts),
                        std::to_string(s.aborted),
                        fmt(out.actual_rate, 2),
                        fmt(mib, 0),
                        fmt(total_s > 0 ? mib / total_s : 0, 2),
                        fmt(du.median, 2),
                        fmt(du.max, 2),
                        fmt(sp.median, 0),
                        fmt(s.slot_wait_s, 1),
                        std::to_string(s.max_retry_s),
                        verdict});
    }

    // ---- серия сбоев: альбом, в котором один документ не уйдёт ----

    void run_failure_series(const SeriesSpec& spec, td::Json& st) {
        const auto& vars = variants();
        const int total = e6_.failure_reps * static_cast<int>(vars.size());
        for (int idx = st.value("next_step", 0); idx < total; ++idx) {
            const int rep = idx / static_cast<int>(vars.size()) + 1;
            const Variant& v = vars[static_cast<size_t>(idx) % vars.size()];
            run_failure_case(spec, v, rep, idx);
            st["next_step"] = idx + 1;
            st["steps_run"] = idx + 1;
            state_.save();
            if (idx + 1 < total) {
                sleep_interruptible(seconds{e6_.case_gap_seconds});
            }
        }
        st["result"] = "completed";
    }

    void run_failure_case(const SeriesSpec& spec, const Variant& v, int rep, int idx) {
        StepContext ctx;
        ctx.spec = &spec;
        ctx.step = idx;
        ctx.rate = 0;
        ctx.attempt = rep;
        ctx.tolerate_failures = true;
        ctx.start_wall = WallClock::now();
        ctx.t0 = ExpClock::now();
        log_line("failure case " + std::to_string(idx + 1) + ": " + v.name + " (rep " +
                 std::to_string(rep) + ", " + std::to_string(kAlbumSize) + " x " +
                 size_name(v.size) + ", bad index " + std::to_string(v.bad) + ")");

        SendJob job;
        job.id = state_.take_doc_no();
        std::vector<uint64_t> seeds;
        for (int k = 0; k < kAlbumSize; ++k) {
            seeds.push_back(kSeed ^ (state_.take_doc_no() * 0x9E3779B97F4A7C15ULL));
        }
        const fs::path dir = names_.docs;
        const Variant variant = v;
        job.nominal_size = v.size;
        job.prepare = [dir, seeds, variant] {
            std::vector<fs::path> files;
            for (const uint64_t seed : seeds) {
                files.push_back(make_seeded_document(dir, seed, variant.size));
            }
            const fs::path bad = files[static_cast<size_t>(variant.bad)];
            std::error_code ec;
            if (variant.how == Break::kMissingBefore) {
                fs::remove(bad, ec);
            } else if (variant.how == Break::kEmptyBefore) {
                fs::resize_file(bad, 0, ec);
            }
            return files;
        };
        if (v.how == Break::kDeleteAfter || v.how == Break::kChmodAfter ||
            v.how == Break::kTruncateAfter) {
            job.after_call = [variant](const std::vector<fs::path>& files) {
                const fs::path bad = files[static_cast<size_t>(variant.bad)];
                std::error_code ec;
                if (variant.how == Break::kDeleteAfter) {
                    fs::remove(bad, ec);
                } else if (variant.how == Break::kChmodAfter) {
                    ::chmod(bad.c_str(), 0);
                } else {
                    fs::resize_file(bad, 0, ec);
                }
            };
        }
        ctx.launches.push_back({WallClock::now(), kAlbumSize});
        ++ctx.stats.launched_jobs;
        ctx.stats.launched_docs += kAlbumSize;
        load_->launch(std::move(job));
        drain(ctx);
        ++steps_done_;
        if (ctx.summaries.empty()) {
            return;
        }
        const JobSummary& js = ctx.summaries.back();
        failures_csv_.row({v.name, std::to_string(rep), std::to_string(v.size),
                           std::to_string(kAlbumSize), std::to_string(v.bad),
                           js.call_ok ? "yes" : "no", js.call_error, std::to_string(js.ok),
                           std::to_string(js.docs - js.ok), js.failed_indexes, js.first_error,
                           fmt(js.first_ms, 1), fmt(js.last_ms, 1),
                           std::to_string(js.distinct_albums), js.ok_ids_contiguous});
        log_line("  -> call " + std::string{js.call_ok ? "ok" : "failed"} + ", " +
                 std::to_string(js.ok) + " of " + std::to_string(js.docs) + " docs delivered" +
                 (js.failed_indexes.empty() ? "" : ", failed: " + js.failed_indexes));
        if (ctx.stats.docs_flood > 0) {
            // Редко, но возможно: лимит посреди серии сбоев. Подождать как после ступени.
            const int64_t wait_s = ctx.stats.max_retry_s + ctx.stats.max_retry_s / 5 + 5;
            log_line("FLOOD_WAIT in the failure series, waiting " + std::to_string(wait_s) + " s");
            sleep_interruptible(seconds{wait_s});
        }
    }

    // ---- удаление ----

    void load_pending() {
        std::set<int64_t> sent;
        for (const auto& row : run::read_csv(names_.out / "e6_docs.csv")) {
            if (row.at("result") == "ok" && !row.at("server_id").empty()) {
                sent.insert(std::stoll(row.at("server_id")));
            }
        }
        for (const auto& row : run::read_csv(names_.out / "e6_deleted.csv")) {
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
                const auto next = *last_action + milliseconds{e6_.delete_interval_ms};
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
                    guard_.call("getMessages", [&] {
                        return bot_->client().call(td::Json{{"@type", "getMessages"},
                                                            {"chat_id", chat_id_},
                                                            {"message_ids", message_ids}},
                                                   seconds{60});
                    });
                }
                pace();
                guard_.call("deleteMessages",
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
    const E6Options& e6_;
    const Names& names_;
    td::Json setup_;
    int64_t chat_id_;
    run::RunState state_;
    std::vector<SeriesSpec> plan_;
    run::Guard guard_;
    Csv docs_csv_;
    Csv jobs_csv_;
    Csv steps_csv_;
    Csv failures_csv_;
    Csv series_csv_;
    Csv progress_csv_;
    Csv deleted_csv_;

    std::unique_ptr<BenchAccount> bot_;
    std::unique_ptr<SendLoad> load_;
    std::set<int64_t> pending_;  // серверные id доставленного и ещё не удалённого
    std::set<int64_t> sent_this_session_;
    int64_t session_sends_ = 0;
    int64_t steps_done_ = 0;
};

// ---- setup / status / teardown ------------------------------------------------------------

void cmd_setup(BenchEnv& env, const BenchOptions& options, const E6Options& e6) {
    const Names names = names_for(options, e6);
    const run::SessionLock lock(names.lock, "e6");
    td::Json state;
    if (has_state(options, names.prefix)) {
        state = load_state(options, names.prefix);
        if (state.value("bot_added", false)) {
            throw std::runtime_error("channel e6 already set up; run teardown first");
        }
        log_line("resuming setup: the channel exists");
    }
    FloodGuard guard(std::cerr, names.out / "e6_setup_flood.csv", "e6-setup");
    auto user = env.open(Account::kUser, std::cerr);
    auto bot = env.open_at(Account::kBot, names.session, std::cerr);
    if (state.is_null()) {
        const std::string title = "tgfs-bench-e6-" + date_stamp();
        const int64_t chat_id = create_channel(*user, title, guard, std::cerr);
        bump_counter(options, "e6.user_actions", 1);
        state = td::Json{{"chat_id", chat_id},
                         {"title", title},
                         {"created", iso_utc_now()},
                         {"bot_added", false}};
        save_state(options, names.prefix, state);
    }
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    std::string error;
    wait_chat_known(user->client(), chat_id, seconds{60}, &error);
    add_bot_admin(*user, *bot, chat_id, e6.rights, guard, std::cerr);
    bump_counter(options, "e6.user_actions", 2);
    const auto known = wait_chat_known(bot->client(), chat_id, seconds{90}, &error);
    log_line("bot knows the channel: " + (known ? fmt(*known) + " ms" : "NO (" + error + ")"));
    state["bot_added"] = true;
    state["rights"] = e6.rights;
    state["bot_added_at"] = iso_utc_now();
    save_state(options, names.prefix, state);
    std::cout << "setup done: channel created, bot is administrator (" << e6.rights
              << "), the user sent no messages\n";
}

void cmd_status(const BenchOptions& options, const E6Options& e6) {
    const Names names = names_for(options, e6);
    if (!has_state(options, names.prefix)) {
        std::cout << "no channel (run `e6 setup`)\n";
        return;
    }
    const td::Json state = load_state(options, names.prefix);
    std::cout << "channel: " << state.value("title", std::string{"?"})
              << ", bot_added=" << state.value("bot_added", false) << "\n";
    if (has_state(options, names.run_state)) {
        const td::Json j = load_state(options, names.run_state);
        std::cout << "run state updated: " << j.value("updated", std::string{"-"}) << ", quiet for "
                  << std::max<int64_t>(0, j.value("quiet_until", 0) - epoch_now())
                  << " s more, next doc no " << j.value("next_doc_no", 0) << "\n";
        for (const auto& [id, st] : j["series"].items()) {
            if (!st.is_object()) {
                continue;
            }
            std::cout << "  " << id << ": " << st.value("status", std::string{"?"});
            if (st.contains("result")) {
                std::cout << " -> " << st["result"].get<std::string>();
            }
            if (st.contains("max_clean_rate")) {
                std::cout << ", max clean " << st["max_clean_rate"].get<int>() << " docs/min";
            }
            if (st.contains("flood_rate")) {
                std::cout << ", FLOOD at " << st["flood_rate"].get<int>() << " docs/min ("
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
    for (const auto& row : run::read_csv(names.out / "e6_docs.csv")) {
        ++(row.at("result") == "ok" ? ok : other);
    }
    int64_t deleted = 0;
    for (const auto& row : run::read_csv(names.out / "e6_deleted.csv")) {
        deleted += row.at("result") == "ok" ? 1 : 0;
    }
    std::cout << "docs: " << ok << " ok, " << other << " not ok; deleted: " << deleted << "\n"
              << "FLOOD_WAIT events: " << run::read_csv(names.out / "e6_flood.csv").size() << "\n";
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E6Options& e6) {
    const Names names = names_for(options, e6);
    const run::SessionLock lock(names.lock, "e6");
    const td::Json state = load_state(options, names.prefix);
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    FloodGuard guard(std::cerr, names.out / "e6_setup_flood.csv", "e6-teardown");
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
    bump_counter(options, "e6.user_actions", 1);
    user.reset();
    remove_fresh_bot_dir(names.session);
    remove_fresh_bot_dir(names.docs);
    fs::remove(options.state_dir / (names.prefix + ".json"));
    fs::remove(options.state_dir / "e6-run.json");
    fs::remove(options.state_dir / "e6p-run.json");
    std::cout << "channel deleted, bot session directory removed\n";
}

}  // namespace

void add_e6_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("e6", "Experiment E6: albums in bot mode");
    cmd->require_subcommand(1);
    auto e6 = std::make_shared<E6Options>();
    e6->out = default_out_dir("06-e6-albums");
    cmd->add_flag("--probe", e6->probe, "Trial run: short plan, data in probe/");
    cmd->add_option("--out", e6->out, "Directory for CSV data")->capture_default_str();
    cmd->add_option("--rights", e6->rights, "Bot rights set")->capture_default_str();
    auto* step_opt = cmd->add_option("--step-seconds", e6->step_seconds, "Length of one step")
                         ->capture_default_str();
    auto* pause_opt = cmd->add_option("--series-pause-seconds", e6->series_pause_seconds,
                                      "Pause between series, after deletion")
                          ->capture_default_str();
    auto* reps_opt =
        cmd->add_option("--failure-reps", e6->failure_reps, "Repetitions of each failure variant")
            ->capture_default_str();
    auto* gap_opt = cmd->add_option("--case-gap-seconds", e6->case_gap_seconds,
                                    "Pause between albums of the failure series")
                        ->capture_default_str();
    cmd->add_option("--delete-interval-ms", e6->delete_interval_ms,
                    "Minimal interval between delete batches")
        ->capture_default_str();

    // Значения пробного прогона применяются, если опция не задана явно.
    const auto apply_probe = [e6, step_opt, pause_opt, reps_opt, gap_opt] {
        if (!e6->probe) {
            return;
        }
        if (step_opt->count() == 0) {
            e6->step_seconds = 120;
        }
        if (pause_opt->count() == 0) {
            e6->series_pause_seconds = 20;
        }
        if (reps_opt->count() == 0) {
            e6->failure_reps = 1;
        }
        if (gap_opt->count() == 0) {
            e6->case_gap_seconds = 20;
        }
    };

    cmd->add_subcommand("setup", "User creates the channel and adds the bot")
        ->callback([&options, exit_code, e6, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchEnv env(options);
            cmd_setup(env, options, *e6);
            *exit_code = 0;
        });
    cmd->add_subcommand("run", "Bot runs the series (resumable, run it in the background)")
        ->callback([&options, exit_code, e6, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchOptions own = options;
            own.tdlib_log = options.state_dir / "tdlib-e6.log";
            BenchEnv env(own);
            const Names names = names_for(own, *e6);
            const run::SessionLock lock(names.lock, "e6");
            Runner runner(env, own, *e6, names);
            *exit_code = runner.run();
        });
    cmd->add_subcommand("status", "State of the series and counters")
        ->callback([&options, exit_code, e6, apply_probe] {
            apply_probe();
            cmd_status(options, *e6);
            *exit_code = 0;
        });
    cmd->add_subcommand("teardown", "Delete the channel (user account)")
        ->callback([&options, exit_code, e6, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchEnv env(options);
            cmd_teardown(env, options, *e6);
            *exit_code = 0;
        });
}

}  // namespace telega::bench
