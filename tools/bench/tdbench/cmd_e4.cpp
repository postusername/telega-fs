// e4: скачивание диапазонов (SPEC 17.7), только в режиме бота. Канал создаёт пользователь (только
// настройка), документ заливает и скачивает бот.
//
//   tgfs-bench e4 setup      канал, бот-администратор, документ 512 МиБ (seed), getRemoteFile сразу
//   tgfs-bench e4 run        замеры диапазонов: основной сеанс бота и сеанс с пустой БД
//   tgfs-bench e4 check24    проверка getRemoteFile и скачивания через сутки после загрузки
//   tgfs-bench e4 teardown   удалить канал и состояние

#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "commands.h"
#include "exp_common.h"

namespace telega::bench {

namespace {

namespace fs = std::filesystem;
using std::chrono::seconds;

constexpr const char* kState = "e4";
constexpr int64_t kSegment = 65552;  // 64 КиБ + тег (SPEC 5)
constexpr int64_t kSegmentHeader = 40;
constexpr int64_t k64K = int64_t{64} * 1024;

struct E4Options {
    int size_mib = 512;
    uint64_t seed = 20261119;
    int repeats = 5;
    std::string rights = "full";
    std::string db = "both";  // main | fresh | both
    fs::path out;
};

struct Range {
    std::string shape;
    std::string position;
    int64_t offset = 0;
    int64_t count = 0;
    int64_t limit = -1;  // limit для downloadFile, если не равен count (-1: равен)
};

// Что TDLib сообщил об одном скачивании (updateFile и итоговый ответ).
struct FileSnapshot {
    int64_t downloaded_size = 0;
    int64_t prefix_size = 0;
    int64_t download_offset = 0;
    bool completed = false;
    std::string path;
};

FileSnapshot snapshot_of(const td::Json& file) {
    FileSnapshot s;
    if (file.contains("local")) {
        const td::Json& l = file["local"];
        s.downloaded_size = l.value("downloaded_size", int64_t{0});
        s.prefix_size = l.value("downloaded_prefix_size", int64_t{0});
        s.download_offset = l.value("download_offset", int64_t{0});
        s.completed = l.value("is_downloading_completed", false);
        s.path = l.value("path", std::string{});
    }
    return s;
}

struct DiskUsage {
    int64_t size = -1;   // st_size
    int64_t alloc = -1;  // st_blocks * 512
};

DiskUsage disk_usage(const std::string& path) {
    DiskUsage d;
    struct stat st {};
    if (!path.empty() && ::stat(path.c_str(), &st) == 0) {
        d.size = st.st_size;
        d.alloc = static_cast<int64_t>(st.st_blocks) * 512;
    }
    return d;
}

std::string yn(bool v) { return v ? "1" : "0"; }

std::string date_stamp() {
    const std::string iso = iso_utc_now();
    return iso.substr(0, 4) + iso.substr(5, 2) + iso.substr(8, 2);
}

// Позиции 64 КиБ: начало, середина, конец (выровненные) и те же три для сегмента зашифрованного
// чанка (смещение 40 + i * 65552, длина 65552), как их будет запрашивать ChunkReader.
std::vector<Range> standard_ranges(int64_t size) {
    std::vector<Range> r;
    r.push_back({"aligned64k", "start", 0, k64K});
    r.push_back({"aligned64k", "middle", (size / 2 / k64K) * k64K, k64K});
    r.push_back({"aligned64k", "end", size - k64K, k64K});
    const int64_t last_i = (size - kSegmentHeader - 1) / kSegment;
    const int64_t mid_i = (size / 2 - kSegmentHeader) / kSegment;
    r.push_back({"segment", "start", kSegmentHeader, kSegment});
    r.push_back({"segment", "middle", kSegmentHeader + mid_i * kSegment, kSegment});
    const int64_t end_off = kSegmentHeader + last_i * kSegment;
    r.push_back({"segment", "end", end_off, size - end_off});
    // Хвост с limit больше остатка файла: проверка, как TDLib ведёт себя у конца файла.
    r.push_back({"end_limit1m", "end", size - k64K, k64K, int64_t{1024} * 1024});
    return r;
}

class FileEvents {
  public:
    FileEvents(td::TdClient& client, int32_t file_id) : file_id_(file_id), t0_(ExpClock::now()) {
        sub_ = client.subscribe("updateFile", [this](const td::Json& u) { on_update(u); });
    }
    [[nodiscard]] double first_byte_ms() {
        const std::lock_guard<std::mutex> lock(mu_);
        return first_ms_;
    }
    [[nodiscard]] int count() {
        const std::lock_guard<std::mutex> lock(mu_);
        return count_;
    }
    [[nodiscard]] int64_t max_downloaded() {
        const std::lock_guard<std::mutex> lock(mu_);
        return max_downloaded_;
    }

  private:
    void on_update(const td::Json& u) {
        if (!u.contains("file") || u["file"].value("id", int32_t{-1}) != file_id_) {
            return;
        }
        const FileSnapshot s = snapshot_of(u["file"]);
        const std::lock_guard<std::mutex> lock(mu_);
        ++count_;
        max_downloaded_ = std::max(max_downloaded_, s.downloaded_size);
        if (first_ms_ < 0 && s.downloaded_size > 0) {
            first_ms_ = ms_since(t0_);
        }
    }

    const int32_t file_id_;
    const ExpClock::time_point t0_;
    std::mutex mu_;
    double first_ms_ = -1;
    int count_ = 0;
    int64_t max_downloaded_ = 0;
    td::Subscription sub_;
};

class E4Bench {
  public:
    E4Bench(BenchEnv& env, const BenchOptions& options, const E4Options& e4, td::Json state)
        : env_(env),
          options_(options),
          e4_(e4),
          state_(std::move(state)),
          size_(state_.at("size").get<int64_t>()),
          seed_(state_.at("seed").get<uint64_t>()),
          chat_id_(state_.at("chat_id").get<int64_t>()),
          guard_(std::cerr, e4.out / "flood.csv", "e4") {}

    void run() {
        Csv ranges(e4_.out / "e4_ranges.csv",
                   {"variant", "shape", "position", "repeat", "offset", "count", "download_ms",
                    "first_update_ms", "read_ms", "match", "downloaded_size", "prefix_size",
                    "download_offset", "completed", "disk_size", "disk_alloc", "direct_pread_match",
                    "updates", "error"});
        Csv remote(e4_.out / "e4_remote.csv",
                   {"time_utc", "variant", "path", "ok", "ms", "note", "error"});
        Csv seq(e4_.out / "e4_sequential.csv",
                {"variant", "step", "offset", "download_ms", "downloaded_size", "prefix_size",
                 "match", "error"});
        if (e4_.db == "main" || e4_.db == "both") {
            run_variant("main", env_.open(Account::kBot, std::cerr), true, ranges, remote, seq);
        }
        if (e4_.db == "fresh" || e4_.db == "both") {
            const fs::path dir = fresh_bot_dir(options_);
            const auto t0 = ExpClock::now();
            auto bot = env_.open_at(Account::kBot, dir, std::cerr);
            remote.row({iso_utc_now(), "fresh", "login", "1", fmt(ms_since(t0)), "", ""});
            run_variant("fresh", std::move(bot), true, ranges, remote, seq);
            remove_fresh_bot_dir(dir);
        }
        if (e4_.db == "fresh-remote-only") {
            // Пустая БД, file_id только из getRemoteFile: размер файла TDLib неизвестен.
            const fs::path dir = fresh_bot_dir(options_);
            auto bot = env_.open_at(Account::kBot, dir, std::cerr);
            run_variant("fresh-remote-only", std::move(bot), false, ranges, remote, seq);
            remove_fresh_bot_dir(dir);
        }
    }

    // Проверка через сутки: getRemoteFile по сохранённому id и скачивание трёх диапазонов, в
    // основном сеансе и после перезапуска с пустой БД.
    void check24() {
        Csv csv(e4_.out / "e4_check24.csv",
                {"time_utc", "age_hours", "variant", "step", "ok", "ms", "error"});
        const double age_h =
            static_cast<double>(std::time(nullptr) - state_.value("uploaded_epoch", int64_t{0})) /
            3600.0;
        std::cerr << "age of the uploaded document: " << fmt(age_h, 2) << " h\n";
        const auto one = [&](const std::string& variant, std::unique_ptr<BenchAccount> bot) {
            td::TdClient& c = bot->client();
            int32_t file_id = 0;
            const auto t0 = ExpClock::now();
            try {
                const td::Json f = guard_.run("getRemoteFile", [&] {
                    return c.call(td::Json{{"@type", "getRemoteFile"},
                                           {"remote_file_id", state_.at("remote_file_id")},
                                           {"file_type", {{"@type", "fileTypeDocument"}}}});
                });
                file_id = f.value("id", int32_t{0});
                csv.row({iso_utc_now(), fmt(age_h, 2), variant, "getRemoteFile", "1",
                         fmt(ms_since(t0)), ""});
            } catch (const td::TdException& e) {
                csv.row({iso_utc_now(), fmt(age_h, 2), variant, "getRemoteFile", "0",
                         fmt(ms_since(t0)), e.error().to_string()});
                std::cerr << variant << ": getRemoteFile failed: " << e.error().to_string() << "\n";
            }
            if (file_id != 0) {
                for (const Range& r : standard_ranges(size_)) {
                    if (r.shape != "aligned64k") {
                        continue;
                    }
                    const Measurement m = measure(c, file_id, r);
                    csv.row({iso_utc_now(), fmt(age_h, 2), variant, "download " + r.position,
                             yn(m.match), fmt(m.download_ms), m.error});
                    std::cerr << variant << ": " << r.position << " match=" << yn(m.match) << " "
                              << fmt(m.download_ms) << " ms " << m.error << "\n";
                }
            }
            // Путь через getMessages (если бот знает чат).
            std::string chat_error;
            wait_chat_known(c, chat_id_, seconds{20}, &chat_error);
            const auto t1 = ExpClock::now();
            try {
                const td::Json m = guard_.run("getMessages", [&] {
                    return c.call(
                        td::Json{{"@type", "getMessages"},
                                 {"chat_id", chat_id_},
                                 {"message_ids", td::Json::array({state_.at("message_id")})}});
                });
                const bool ok = !m["messages"].empty() && !m["messages"][0].is_null();
                csv.row({iso_utc_now(), fmt(age_h, 2), variant, "getMessages", yn(ok),
                         fmt(ms_since(t1)), ok ? "" : "message not returned"});
                if (file_id != 0) {
                    const Range end{"aligned64k", "end", size_ - k64K, k64K};
                    const Measurement again = measure(c, file_id, end);
                    csv.row({iso_utc_now(), fmt(age_h, 2), variant,
                             "download end (after getMessages)", yn(again.match),
                             fmt(again.download_ms), again.error});
                }
            } catch (const td::TdException& e) {
                csv.row({iso_utc_now(), fmt(age_h, 2), variant, "getMessages", "0",
                         fmt(ms_since(t1)), e.error().to_string()});
            }
        };
        one("main", env_.open(Account::kBot, std::cerr));
        const fs::path dir = fresh_bot_dir(options_);
        one("fresh", env_.open_at(Account::kBot, dir, std::cerr));
        remove_fresh_bot_dir(dir);
    }

  private:
    struct Measurement {
        bool match = false;
        double download_ms = 0;
        double first_update_ms = -1;
        double read_ms = 0;
        FileSnapshot after;
        DiskUsage disk;
        int updates = 0;
        bool direct_match = false;  // pread по local.path с теми же смещениями
        std::string error;
    };

    // Один замер: сброс локального состояния файла, downloadFile(offset, limit, synchronous),
    // readFilePart, сверка с данными по seed, размеры по updateFile и на диске.
    Measurement measure(td::TdClient& c, int32_t file_id, const Range& r, bool reset = true) {
        Measurement m;
        try {
            if (reset) {
                // файла на диске могло не быть
                attempt([&] { c.call(td::Json{{"@type", "deleteFile"}, {"file_id", file_id}}); });
            }
            FileEvents events(c, file_id);
            const auto t0 = ExpClock::now();
            const td::Json f = guard_.run("downloadFile", [&] {
                return c.call(td::Json{{"@type", "downloadFile"},
                                       {"file_id", file_id},
                                       {"priority", 1},
                                       {"offset", r.offset},
                                       {"limit", r.limit >= 0 ? r.limit : r.count},
                                       {"synchronous", true}},
                              seconds{300});
            });
            m.download_ms = ms_since(t0);
            const FileSnapshot after = snapshot_of(f);
            m.after = after;
            m.first_update_ms = events.first_byte_ms();
            m.updates = events.count();
            const auto t1 = ExpClock::now();
            const td::Json data = c.call(td::Json{{"@type", "readFilePart"},
                                                  {"file_id", file_id},
                                                  {"offset", r.offset},
                                                  {"count", r.count}},
                                         seconds{60});
            m.read_ms = ms_since(t1);
            const std::string got = base64_decode(data.value("data", std::string{}));
            std::string want;
            fill_seeded(seed_, static_cast<uint64_t>(r.offset), want, static_cast<size_t>(r.count));
            m.match = got == want;
            if (!m.match) {
                m.error = "data mismatch (got " + std::to_string(got.size()) + " bytes)";
            }
            const td::Json fresh_file =
                c.call(td::Json{{"@type", "getFile"}, {"file_id", file_id}});
            m.after = snapshot_of(fresh_file);
            m.disk = disk_usage(m.after.path);
            if (!m.after.path.empty()) {
                std::ifstream in(m.after.path, std::ios::binary);
                std::string direct(static_cast<size_t>(r.count), '\0');
                in.seekg(r.offset);
                in.read(direct.data(), static_cast<std::streamsize>(direct.size()));
                m.direct_match = in.gcount() == r.count && direct == want;
            }
        } catch (const td::TdException& e) {
            m.error = e.error().to_string();
        }
        return m;
    }

    void run_variant(const std::string& variant, std::unique_ptr<BenchAccount> bot,
                     bool use_messages, Csv& ranges, Csv& remote, Csv& seq) {
        td::TdClient& c = bot->client();
        std::cerr << "=== variant: " << variant << "\n";
        // Получение file_id: getRemoteFile по сохранённому id (путь 1) и getMessages (путь 2).
        int32_t file_id = 0;
        {
            const auto t0 = ExpClock::now();
            try {
                const td::Json f = guard_.run("getRemoteFile", [&] {
                    return c.call(td::Json{{"@type", "getRemoteFile"},
                                           {"remote_file_id", state_.at("remote_file_id")},
                                           {"file_type", {{"@type", "fileTypeDocument"}}}});
                });
                file_id = f.value("id", int32_t{0});
                const FileSnapshot s = snapshot_of(f);
                remote.row({iso_utc_now(), variant, "getRemoteFile", "1", fmt(ms_since(t0)),
                            "size " + std::to_string(f.value("size", int64_t{0})) + ", local " +
                                (s.path.empty() ? "none" : "present"),
                            ""});
            } catch (const td::TdException& e) {
                remote.row({iso_utc_now(), variant, "getRemoteFile", "0", fmt(ms_since(t0)), "",
                            e.error().to_string()});
                std::cerr << "getRemoteFile failed: " << e.error().to_string() << "\n";
            }
        }
        int32_t message_file_id = 0;
        if (use_messages) {
            std::string err;
            const auto known = wait_chat_known(c, chat_id_, seconds{20}, &err);
            const auto t0 = ExpClock::now();
            if (!known) {
                remote.row({iso_utc_now(), variant, "getMessages", "0", "", "chat unknown", err});
            } else {
                try {
                    const td::Json m = guard_.run("getMessages", [&] {
                        return c.call(
                            td::Json{{"@type", "getMessages"},
                                     {"chat_id", chat_id_},
                                     {"message_ids", td::Json::array({state_.at("message_id")})}});
                    });
                    if (m["messages"].empty() || m["messages"][0].is_null()) {
                        remote.row({iso_utc_now(), variant, "getMessages", "0", fmt(ms_since(t0)),
                                    "message not returned", ""});
                    } else {
                        message_file_id = m["messages"][0]["content"]["document"]["document"].value(
                            "id", int32_t{0});
                        remote.row({iso_utc_now(), variant, "getMessages", "1", fmt(ms_since(t0)),
                                    "file_id " +
                                        std::string{message_file_id == file_id ? "equals"
                                                                               : "differs from"} +
                                        " getRemoteFile",
                                    ""});
                    }
                } catch (const td::TdException& e) {
                    remote.row({iso_utc_now(), variant, "getMessages", "0", fmt(ms_since(t0)), "",
                                e.error().to_string()});
                }
            }
        }
        if (file_id == 0) {
            file_id = message_file_id;
        }
        if (file_id == 0) {
            std::cerr << "no file_id, skipping the variant\n";
            return;
        }
        const std::vector<Range> positions = standard_ranges(size_);
        for (int rep = 1; rep <= e4_.repeats; ++rep) {
            for (const Range& r : positions) {
                const Measurement m = measure(c, file_id, r);
                ranges.row({variant, r.shape, r.position, std::to_string(rep),
                            std::to_string(r.offset), std::to_string(r.count), fmt(m.download_ms),
                            fmt(m.first_update_ms), fmt(m.read_ms), yn(m.match),
                            std::to_string(m.after.downloaded_size),
                            std::to_string(m.after.prefix_size),
                            std::to_string(m.after.download_offset), yn(m.after.completed),
                            std::to_string(m.disk.size), std::to_string(m.disk.alloc),
                            yn(m.direct_match), std::to_string(m.updates), m.error});
                std::cerr << variant << " #" << rep << " " << r.shape << "/" << r.position
                          << ": download " << fmt(m.download_ms) << " ms, read " << fmt(m.read_ms)
                          << " ms, match " << yn(m.match) << ", TDLib downloaded "
                          << m.after.downloaded_size << " B (disk " << m.disk.size << "/"
                          << m.disk.alloc << ") " << m.error << "\n";
            }
        }
        // Диапазоны крупнее: 1, 4 и 16 МиБ из середины (для оценки скорости и chunk_size).
        for (const int64_t mib : {1, 4, 16}) {
            for (int rep = 1; rep <= 3; ++rep) {
                const Range r{"big" + std::to_string(mib) + "m", "middle",
                              (size_ / 2 / k64K) * k64K, mib * 1024 * 1024};
                const Measurement m = measure(c, file_id, r);
                ranges.row({variant, r.shape, r.position, std::to_string(rep),
                            std::to_string(r.offset), std::to_string(r.count), fmt(m.download_ms),
                            fmt(m.first_update_ms), fmt(m.read_ms), yn(m.match),
                            std::to_string(m.after.downloaded_size),
                            std::to_string(m.after.prefix_size),
                            std::to_string(m.after.download_offset), yn(m.after.completed),
                            std::to_string(m.disk.size), std::to_string(m.disk.alloc),
                            yn(m.direct_match), std::to_string(m.updates), m.error});
                std::cerr << variant << " big " << mib << " MiB #" << rep << ": download "
                          << fmt(m.download_ms) << " ms, match " << yn(m.match)
                          << ", TDLib downloaded " << m.after.downloaded_size << " B " << m.error
                          << "\n";
            }
        }
        // Последовательное чтение: 16 соседних диапазонов по 64 КиБ без сброса между ними
        // (как readahead): сколько реально качается и быстрее ли следующие.
        attempt([&] { c.call(td::Json{{"@type", "deleteFile"}, {"file_id", file_id}}); });
        const int64_t base = (size_ / 4 / k64K) * k64K;
        for (int step = 0; step < 16; ++step) {
            const Range r{"seq64k", "quarter", base + step * k64K, k64K};
            const Measurement m = measure(c, file_id, r, /*reset=*/false);
            seq.row({variant, std::to_string(step), std::to_string(r.offset), fmt(m.download_ms),
                     std::to_string(m.after.downloaded_size), std::to_string(m.after.prefix_size),
                     yn(m.match), m.error});
        }
        // Путь через file_id из getMessages: скачивание начала.
        if (message_file_id != 0) {
            const Measurement m = measure(c, message_file_id, positions[0]);
            remote.row({iso_utc_now(), variant, "download via getMessages file_id", yn(m.match),
                        fmt(m.download_ms), "start 64 KiB", m.error});
        }
        attempt([&] { c.call(td::Json{{"@type", "deleteFile"}, {"file_id", file_id}}); });
    }

    BenchEnv& env_;
    const BenchOptions& options_;
    const E4Options& e4_;
    td::Json state_;
    const int64_t size_;
    const uint64_t seed_;
    const int64_t chat_id_;
    FloodGuard guard_;
};

void cmd_setup(BenchEnv& env, const BenchOptions& options, const E4Options& e4) {
    if (has_state(options, kState)) {
        throw std::runtime_error("e4 state already exists; run teardown (or `e4 run`)");
    }
    FloodGuard guard(std::cerr, e4.out / "flood.csv", "e4");
    auto user = env.open(Account::kUser, std::cerr);
    auto bot = env.open(Account::kBot, std::cerr);
    const std::string title = "tgfs-bench-e4-" + date_stamp();
    const int64_t chat_id =
        create_channel_with_bot(*user, *bot, title, e4.rights, guard, std::cerr);
    bump_counter(options, "e4.user_actions", 3);
    std::string error;
    if (!wait_chat_known(bot->client(), chat_id, seconds{90}, &error)) {
        throw std::runtime_error("the bot does not see the channel: " + error);
    }
    user.reset();  // пользователь больше не нужен до teardown

    const int64_t size = static_cast<int64_t>(e4.size_mib) * 1024 * 1024;
    const fs::path payload = options.state_dir / "e4" / (random_hex(16) + ".bin");
    std::cerr << "generating " << e4.size_mib << " MiB (seed " << e4.seed << ")\n";
    write_seeded_file(payload, e4.seed, static_cast<uint64_t>(size));
    td::Json state{{"chat_id", chat_id},
                   {"title", title},
                   {"size", size},
                   {"seed", e4.seed},
                   {"file_name", payload.filename().string()},
                   {"stage", "uploading"}};
    save_state(options, kState, state);

    std::cerr << "uploading as the bot (one message)\n";
    const auto t0 = ExpClock::now();
    std::atomic<int64_t> uploaded{0};
    auto sub = bot->client().subscribe("updateFile", [&](const td::Json& u) {
        uploaded = u["file"]["remote"].value("uploaded_size", int64_t{0});
    });
    td::Json msg;
    try {
        msg = guard.run("send 512 MiB", [&] {
            return bot->tracker().send(send_document_request(chat_id, payload), seconds{60},
                                       std::chrono::hours{3});
        });
    } catch (const td::TdException& e) {
        std::cerr << "upload failed: " << e.error().to_string() << " (uploaded " << uploaded
                  << " B)\n";
        throw;
    }
    const double upload_s = ms_since(t0) / 1000.0;
    sub.reset();
    bump_counter(options, "e4.bot_messages_sent", 1);
    const td::Json doc = msg["content"]["document"]["document"];
    state["message_id"] = msg.value("id", int64_t{0});
    state["remote_file_id"] = doc["remote"].value("id", std::string{});
    state["remote_unique_id"] = doc["remote"].value("unique_id", std::string{});
    state["uploaded_at"] = iso_utc_now();
    state["uploaded_epoch"] = static_cast<int64_t>(std::time(nullptr));
    state["upload_s"] = upload_s;
    state["stage"] = "uploaded";
    save_state(options, kState, state);
    std::cerr << "uploaded in " << fmt(upload_s) << " s ("
              << fmt(static_cast<double>(size) / 1048576.0 / upload_s, 2)
              << " MiB/s), document size " << doc.value("size", int64_t{0}) << "\n";

    // getRemoteFile сразу, в том же сеансе.
    Csv remote(e4.out / "e4_remote.csv",
               {"time_utc", "variant", "path", "ok", "ms", "note", "error"});
    const auto t1 = ExpClock::now();
    try {
        const td::Json f =
            bot->client().call(td::Json{{"@type", "getRemoteFile"},
                                        {"remote_file_id", state["remote_file_id"]},
                                        {"file_type", {{"@type", "fileTypeDocument"}}}});
        remote.row({iso_utc_now(), "immediate", "getRemoteFile", "1", fmt(ms_since(t1)),
                    std::string{"same file_id as upload: "} +
                        (f.value("id", int32_t{0}) == doc.value("id", int32_t{-1}) ? "yes" : "no"),
                    ""});
    } catch (const td::TdException& e) {
        remote.row({iso_utc_now(), "immediate", "getRemoteFile", "0", fmt(ms_since(t1)), "",
                    e.error().to_string()});
    }
    Csv upload(e4.out / "e4_upload.csv", {"time_utc", "size_bytes", "seconds", "mib_per_s"});
    upload.row({iso_utc_now(), std::to_string(size), fmt(upload_s),
                fmt(static_cast<double>(size) / 1048576.0 / upload_s, 2)});
    // Данные известны по seed: локальный источник больше не нужен.
    std::error_code ec;
    fs::remove(payload, ec);
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E4Options& e4) {
    const td::Json state = load_state(options, kState);
    FloodGuard guard(std::cerr, e4.out / "flood.csv", "e4");
    auto user = env.open(Account::kUser, std::cerr);
    std::string error;
    wait_chat_known(user->client(), state.at("chat_id").get<int64_t>(), seconds{30}, &error);
    for (int attempt_no = 1;; ++attempt_no) {
        try {
            guard.run("deleteChat", [&] {
                return user->client().call(td::Json{
                    {"@type", "deleteChat"}, {"chat_id", state.at("chat_id").get<int64_t>()}});
            });
            break;
        } catch (const td::TdException& e) {
            // После старта процесса чат бывает неизвестен пользователю: подождать и повторить.
            if (attempt_no >= 5 || e.error().code != 400) {
                throw;
            }
            std::this_thread::sleep_for(seconds{3});
            wait_chat_known(user->client(), state.at("chat_id").get<int64_t>(), seconds{20},
                            &error);
        }
    }
    bump_counter(options, "e4.user_actions", 1);
    std::error_code ec;
    fs::remove(options.state_dir / (std::string{kState} + ".json"), ec);
    fs::remove_all(options.state_dir / "e4", ec);
    std::cout << "channel deleted\n";
}

}  // namespace

void add_e4_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("e4", "Experiment E4: range downloads, bot mode (SPEC 17.7)");
    cmd->require_subcommand(1);
    auto e4 = std::make_shared<E4Options>();
    e4->out = default_out_dir("04-e4-range-download");
    cmd->add_option("--out", e4->out, "Directory for CSV data")->capture_default_str();

    auto* setup = cmd->add_subcommand("setup", "Create the channel and upload the test document");
    setup->add_option("--size-mib", e4->size_mib, "Document size")->capture_default_str();
    setup->add_option("--seed", e4->seed, "Seed of the document data")->capture_default_str();
    setup->add_option("--rights", e4->rights, "Bot rights set (see e2 run --sets)")
        ->capture_default_str();
    setup->callback([&options, exit_code, e4] {
        BenchEnv env(options);
        try {
            cmd_setup(env, options, *e4);
            *exit_code = 0;
        } catch (const StopExperiment& e) {
            std::cerr << "STOPPED: " << e.what() << "\n";
            *exit_code = 2;
        }
    });

    auto* run = cmd->add_subcommand("run", "Range download measurements");
    run->add_option("--repeats", e4->repeats, "Repeats per range")->capture_default_str();
    run->add_option("--db", e4->db, "main | fresh | both")->capture_default_str();
    run->callback([&options, exit_code, e4] {
        BenchEnv env(options);
        try {
            E4Bench(env, options, *e4, load_state(options, kState)).run();
            *exit_code = 0;
        } catch (const StopExperiment& e) {
            std::cerr << "STOPPED: " << e.what() << "\n";
            *exit_code = 2;
        }
    });

    cmd->add_subcommand("check24", "getRemoteFile and downloads a day after the upload")
        ->callback([&options, exit_code, e4] {
            BenchEnv env(options);
            try {
                E4Bench(env, options, *e4, load_state(options, kState)).check24();
                *exit_code = 0;
            } catch (const StopExperiment& e) {
                std::cerr << "STOPPED: " << e.what() << "\n";
                *exit_code = 2;
            }
        });

    cmd->add_subcommand("teardown", "Delete the E4 channel and the state")
        ->callback([&options, exit_code, e4] {
            BenchEnv env(options);
            cmd_teardown(env, options, *e4);
            *exit_code = 0;
        });
}

}  // namespace telega::bench
