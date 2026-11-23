// e10: чтение и readahead (SPEC 17.13), боевые серверы, режим бота. Канал создаёт пользователь
// (setup, сообщений от пользователя нет), всю нагрузку несёт бот в отдельном каталоге сессии
// <state-dir>/bot-e10. Документы — 32 чанка по 8 МиБ в формате SPEC 5 (зашифрованные, содержимое
// по seed), альбомами.
//
//   tgfs-bench e10 setup            пользователь создаёт канал и добавляет бота; бот заливает 32
//   чанка tgfs-bench e10 run              серии (возобновляемо, запускать в фоне) tgfs-bench e10
//   status           состояние серий tgfs-bench e10 restart-check    внутренняя: проверки пункта 7
//   в новом процессе tgfs-bench e10 teardown         удалить канал (пользователь) и каталоги сессии
//
// Серии (одна серия — один замер пункта 17.13, повторы идут кругами по пунктам):
//   p1-<asc|desc|equal>-rN          приоритет между файлами
//   p2-<control|raise|raise_sync>-rN  повышение приоритета
//   p3-<from0|from3m>-rN            асинхронный ход, проверка префикса по тегам сегментов
//   p4-rN                           уже скачанный диапазон
//   p5-<sync|async|extend>-rN       вытеснение
//   p6-<none|w4|w8|w16|w32|d4_32>-rN  последовательное чтение через прототип планировщика
//   p7-rN                           после перезапуска (подготовка здесь, проверка — дочерний
//   процесс)
// Данные для отчёта пишутся в CSV; id канала лежит только в <state-dir>/e10.json.

#include <fcntl.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "commands.h"
#include "common/bytes.h"
#include "crypto/chunk.h"
#include "crypto/keys.h"
#include "exp_common.h"
#include "read_sched.h"
#include "run_common.h"
#include "send_load.h"

extern char** environ;

namespace telega::bench {

namespace {

namespace fs = std::filesystem;
using run::epoch_now;
using run::Interrupted;
using run::log_line;
using run::SessionError;
using run::sleep_interruptible;
using run::stopping;
using std::chrono::milliseconds;
using std::chrono::seconds;

constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * kKiB;
constexpr int kDocs = 32;
constexpr uint64_t kPlain = 8 * kMiB;  // открытый текст чанка
constexpr uint64_t kSeedBase = 0xE10C0FFEEULL;
constexpr int kAlbumSize = 10;

uint64_t seed_of(int idx) {
    return kSeedBase ^ (static_cast<uint64_t>(idx + 1) * 0x9E3779B97F4A7C15ULL);
}

std::string date_stamp() {
    const std::string iso = iso_utc_now();
    return iso.substr(0, 4) + iso.substr(5, 2) + iso.substr(8, 2);
}

double to_mib(double bytes) { return bytes / static_cast<double>(kMiB); }

double percentile(std::vector<double> v, double p) {
    if (v.empty()) {
        return 0;
    }
    std::sort(v.begin(), v.end());
    const double pos = p * static_cast<double>(v.size() - 1);
    const auto lo = static_cast<size_t>(std::floor(pos));
    const auto hi = static_cast<size_t>(std::ceil(pos));
    return v[lo] + (v[hi] - v[lo]) * (pos - static_cast<double>(lo));
}

// ---- Параметры, имена ---------------------------------------------------------------------

struct E10Options {
    bool probe = false;
    fs::path out;
    std::string rights = "only:post,edit,delete";
    int reps = 5;
    std::string items = "1,2,3,4,5,6,7";
    int seq_mib = 256;
    std::string variants6 = "none,w4,w8,w16,w32,d4_32";
    int download_concurrency = 4;
    int series_pause_seconds = 5;
    int album_pause_seconds = 60;
    // Внутренние (дочерний процесс пункта 7).
    std::string series;
    int rep = 1;
    bool inherit_lock = false;
};

struct Names {
    std::string run_state = "e10-run";
    fs::path out;
    fs::path session;
    fs::path upload_session;
    fs::path lock;
    fs::path docs;
    fs::path key;
};

Names names_for(const BenchOptions& options, const E10Options& e10) {
    Names n;
    n.out = e10.probe ? e10.out / "probe" : e10.out;
    if (e10.probe) {
        n.run_state = "e10p-run";
    }
    n.session = options.state_dir / "bot-e10";
    n.upload_session = options.state_dir / "bot-e10-up";
    n.lock = options.state_dir / "e10-run.lock";
    n.docs = options.state_dir / "e10-docs";
    n.key = options.state_dir / "e10-key";
    return n;
}

// ---- Ключи и документы --------------------------------------------------------------------

struct Crypto {
    crypto::Key key;
    FsUuid uuid{};
};

Crypto load_or_create_crypto(const fs::path& path) {
    crypto::init();
    Crypto c;
    if (fs::exists(path)) {
        std::ifstream in(path, std::ios::binary);
        Bytes raw(crypto::Key::kSize + c.uuid.size());
        in.read(reinterpret_cast<char*>(raw.data()), static_cast<std::streamsize>(raw.size()));
        if (in.gcount() != static_cast<std::streamsize>(raw.size())) {
            throw std::runtime_error("e10-key file is damaged");
        }
        c.key = crypto::Key(ByteSpan{raw}.subspan(0, crypto::Key::kSize));
        std::memcpy(c.uuid.data(), raw.data() + crypto::Key::kSize, c.uuid.size());
        return c;
    }
    c.key = crypto::Key::random();
    c.uuid = crypto::random_fs_uuid();
    const fs::path tmp = path.string() + ".tmp";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        throw std::runtime_error("cannot create the key file");
    }
    bool ok =
        ::write(fd, c.key.data(), crypto::Key::kSize) == static_cast<ssize_t>(crypto::Key::kSize);
    ok = ok && ::write(fd, c.uuid.data(), c.uuid.size()) == static_cast<ssize_t>(c.uuid.size());
    ::close(fd);
    if (!ok) {
        throw std::runtime_error("cannot write the key file");
    }
    fs::rename(tmp, path);
    return c;
}

struct Doc {
    int idx = 0;
    int64_t message_id = 0;
    std::string remote_id;
    int64_t enc_size = 0;
    uint64_t chunk_id = 0;
    uint64_t seed = 0;
    std::string header_hex;  // 40 байт заголовка (только для проверок: nonce лежит в файле)
};

td::Json doc_to_json(const Doc& d) {
    return td::Json{{"idx", d.idx},
                    {"message_id", d.message_id},
                    {"remote_id", d.remote_id},
                    {"enc_size", d.enc_size},
                    {"chunk_id", d.chunk_id},
                    {"seed", d.seed},
                    {"header_hex", d.header_hex}};
}

Doc doc_from_json(const td::Json& j) {
    Doc d;
    d.idx = j.at("idx").get<int>();
    d.message_id = j.at("message_id").get<int64_t>();
    d.remote_id = j.value("remote_id", std::string{});
    d.enc_size = j.at("enc_size").get<int64_t>();
    d.chunk_id = j.at("chunk_id").get<uint64_t>();
    d.seed = j.at("seed").get<uint64_t>();
    d.header_hex = j.value("header_hex", std::string{});
    return d;
}

Bytes from_hex(const std::string& hex) {
    Bytes out;
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        out.push_back(static_cast<std::byte>(std::stoi(hex.substr(i, 2), nullptr, 16)));
    }
    return out;
}

crypto::ChunkHeader header_of(const Doc& d) {
    const Bytes raw = from_hex(d.header_hex);
    return crypto::parse_chunk_header(ByteSpan{raw});
}

bool pread_all(int fd, uint64_t off, void* buf, size_t n) {
    auto* p = static_cast<char*>(buf);
    size_t done = 0;
    while (done < n) {
        const ssize_t r = ::pread(fd, p + done, n - done, static_cast<off_t>(off + done));
        if (r <= 0) {
            return false;
        }
        done += static_cast<size_t>(r);
    }
    return true;
}

bool pread_path(const std::string& path, uint64_t off, void* buf, size_t n) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }
    const bool ok = pread_all(fd, off, buf, n);
    ::close(fd);
    return ok;
}

// Расшифровывает сегменты [first, first + count) файла чанка (по пути) и сверяет с данными по
// seed. false — ошибка тега, нечитаемый файл или несовпадение.
bool verify_segments(const Crypto& c, const Doc& d, const std::string& path, uint64_t first,
                     uint64_t count, int* kind = nullptr) {
    // kind: 0 — верно, 1 — файл нечитаем по этому пути (ENOENT и т. п.), 2 — тег или данные
    // неверны.
    if (kind != nullptr) {
        *kind = 0;
    }
    try {
        const uint64_t plain_off = first * crypto::kSegmentSize;
        const uint64_t plain_len = std::min(count * crypto::kSegmentSize, kPlain - plain_off);
        const crypto::SegmentSpan span = crypto::chunk_segments_for(kPlain, plain_off, plain_len);
        Bytes enc(span.enc_len);
        if (!pread_path(path, span.enc_offset, enc.data(), enc.size())) {
            if (kind != nullptr) {
                *kind = 1;
            }
            return false;
        }
        const Bytes plain = crypto::decrypt_chunk_segments(c.key, c.uuid, d.chunk_id, header_of(d),
                                                           span.first, ByteSpan{enc});
        std::string want;
        fill_seeded(d.seed, plain_off, want, plain_len);
        const bool ok =
            plain.size() == want.size() && std::memcmp(plain.data(), want.data(), want.size()) == 0;
        if (!ok && kind != nullptr) {
            *kind = 2;
        }
        return ok;
    } catch (const std::exception&) {
        if (kind != nullptr) {
            *kind = 2;
        }
        return false;
    }
}

constexpr uint64_t kSegments = kPlain / crypto::kSegmentSize;  // 128

int64_t blocks_bytes(const std::string& path) {
    struct stat st {};
    return !path.empty() && ::stat(path.c_str(), &st) == 0
               ? static_cast<int64_t>(st.st_blocks) * 512
               : -1;
}

int64_t dir_blocks_bytes(const fs::path& dir) {
    int64_t total = 0;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(dir, ec); !ec && it != fs::end(it);
         it.increment(ec)) {
        struct stat st {};
        if (it->is_regular_file(ec) && ::stat(it->path().c_str(), &st) == 0) {
            total += static_cast<int64_t>(st.st_blocks) * 512;
        }
    }
    return total;
}

// ---- Сеанс бота: документы, file_id, холодный кэш ----------------------------------------------

struct NetCounters {
    double sent = 0;
    double received = 0;
};

// Счётчики TDLib обновляются с задержкой около секунды, поэтому перед чтением пауза.
NetCounters read_net(td::TdClient& client, milliseconds settle = milliseconds{1500}) {
    std::this_thread::sleep_for(settle);
    NetCounters n;
    attempt([&] {
        const td::Json stats = client.call(
            td::Json{{"@type", "getNetworkStatistics"}, {"only_current", true}}, seconds{30});
        if (stats.contains("entries") && stats["entries"].is_array()) {
            for (const auto& e : stats["entries"]) {
                n.sent += static_cast<double>(e.value("sent_bytes", int64_t{0}));
                n.received += static_cast<double>(e.value("received_bytes", int64_t{0}));
            }
        }
    });
    return n;
}

struct Session {
    std::unique_ptr<BenchAccount> bot;
    std::vector<Doc> docs;
    std::vector<int32_t> file_ids;  // file_id TDLib этого сеанса
    Crypto crypto;
    int64_t chat_id = 0;
    fs::path files_dir;
    run::Guard* guard = nullptr;

    td::TdClient& c() { return bot->client(); }

    // Индексы документов в сценариях — int; здесь они переводятся в индексы векторов.
    int32_t file_id(int idx) const { return file_ids.at(static_cast<size_t>(idx)); }
    const Doc& doc_at(int idx) const { return docs.at(static_cast<size_t>(idx)); }

    td::Json get_file(int idx) {
        return c().call(td::Json{{"@type", "getFile"}, {"file_id", file_id(idx)}});
    }
    std::string local_path(int idx) { return get_file(idx)["local"].value("path", std::string{}); }

    // deleteFile и проверка, что на диске ничего не осталось. Возвращает число проверенных.
    int cold_reset(const std::vector<int>& idxs) {
        int checked = 0;
        for (const int i : idxs) {
            attempt([&] { c().call(td::Json{{"@type", "deleteFile"}, {"file_id", file_id(i)}}); });
            const td::Json f = get_file(i);
            const std::string path = f["local"].value("path", std::string{});
            const bool gone = path.empty() || !fs::exists(path) || blocks_bytes(path) == 0;
            if (f["local"].value("downloaded_size", int64_t{0}) != 0 ||
                f["local"].value("is_downloading_completed", false) || !gone) {
                throw std::runtime_error("cache is not cold after deleteFile for document " +
                                         std::to_string(i));
            }
            ++checked;
        }
        return checked;
    }
    std::vector<int> all_idxs() const {
        std::vector<int> v(docs.size());
        for (size_t i = 0; i < v.size(); ++i) {
            v[i] = static_cast<int>(i);
        }
        return v;
    }
};

// Загружает документы сеанса по getMessages (одна пачка), file_id и размеры сверяются.
void bind_documents(Session& s, const td::Json& setup_state) {
    s.docs.clear();
    td::Json ids = td::Json::array();
    for (const auto& j : setup_state.at("docs")) {
        s.docs.push_back(doc_from_json(j));
        ids.push_back(s.docs.back().message_id);
    }
    const td::Json reply = s.guard->call("getMessages", [&] {
        return s.c().call(
            td::Json{{"@type", "getMessages"}, {"chat_id", s.chat_id}, {"message_ids", ids}});
    });
    s.file_ids.assign(s.docs.size(), 0);
    for (size_t i = 0; i < s.docs.size(); ++i) {
        const td::Json& m = reply["messages"][i];
        if (m.is_null() || m["content"].value("@type", std::string{}) != "messageDocument") {
            throw std::runtime_error("document " + std::to_string(i) +
                                     " is missing in the channel");
        }
        const td::Json& file = m["content"]["document"]["document"];
        if (file.value("size", int64_t{0}) != s.docs[i].enc_size) {
            throw std::runtime_error("document " + std::to_string(i) + " has an unexpected size");
        }
        s.file_ids[i] = file.value("id", int32_t{0});
        s.docs[i].remote_id = file["remote"].value("id", s.docs[i].remote_id);
    }
}

// ---- Журнал updateFile ------------------------------------------------------------------

class EventLog {
  public:
    struct Ev {
        double t_ms = 0;
        int doc = 0;
        int64_t offset = 0;
        int64_t prefix = 0;
        int64_t downloaded = 0;
        bool active = false;
        bool completed = false;
        std::string path;
    };

    EventLog(Session& s, const std::vector<int>& docs) : t0_(ExpClock::now()) {
        for (const int d : docs) {
            by_id_[s.file_id(d)] = d;
        }
        sub_ = s.c().subscribe("updateFile", [this](const td::Json& u) { on_update(u); });
    }
    void mark() {
        const std::lock_guard<std::mutex> lock(mu_);
        t0_ = ExpClock::now();
        events_.clear();
    }
    [[nodiscard]] double now_ms() const { return ms_since(t0_); }
    [[nodiscard]] ExpClock::time_point origin() const { return t0_; }
    std::vector<Ev> snapshot() {
        const std::lock_guard<std::mutex> lock(mu_);
        return events_;
    }
    template <class P>
    bool wait_until(P pred, milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        return cv_.wait_for(lock, timeout, [&] { return pred(events_); });
    }
    // Ждёт любое новое событие после первых `seen`; возвращает число событий.
    size_t wait_more(size_t seen, milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait_for(lock, timeout, [&] { return events_.size() > seen; });
        return events_.size();
    }

    static std::optional<double> first_byte(const std::vector<Ev>& evs, int doc) {
        for (const Ev& e : evs) {
            if (e.doc == doc && e.downloaded > 0) {
                return e.t_ms;
            }
        }
        return std::nullopt;
    }
    static std::optional<double> done(const std::vector<Ev>& evs, int doc) {
        for (const Ev& e : evs) {
            if (e.doc == doc && e.completed) {
                return e.t_ms;
            }
        }
        return std::nullopt;
    }
    static bool all_done(const std::vector<Ev>& evs, const std::vector<int>& docs) {
        for (const int d : docs) {
            if (!done(evs, d)) {
                return false;
            }
        }
        return true;
    }

  private:
    void on_update(const td::Json& u) {
        if (!u.contains("file") || !u["file"].contains("local")) {
            return;
        }
        const auto it = by_id_.find(u["file"].value("id", int32_t{-1}));
        if (it == by_id_.end()) {
            return;
        }
        const td::Json& l = u["file"]["local"];
        Ev e;
        e.doc = it->second;
        e.offset = l.value("download_offset", int64_t{0});
        e.prefix = l.value("downloaded_prefix_size", int64_t{0});
        e.downloaded = l.value("downloaded_size", int64_t{0});
        e.active = l.value("is_downloading_active", false);
        e.completed = l.value("is_downloading_completed", false);
        e.path = l.value("path", std::string{});
        {
            const std::lock_guard<std::mutex> lock(mu_);
            e.t_ms = ms_since(t0_);
            events_.push_back(e);
        }
        cv_.notify_all();
    }

    ExpClock::time_point t0_;
    std::map<int32_t, int> by_id_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::vector<Ev> events_;
    td::Subscription sub_;
};

// Файловые данные сеанса для CSV.
struct Out {
    Csv p1, p2, p3, p4, p5, p6, p6_reads, p7, events, series;
    explicit Out(const fs::path& dir)
        : p1(dir / "e10_p1.csv",
             {"series", "rep", "variant", "order_idx", "doc", "prio", "issue_ms", "call_ms",
              "first_byte_ms", "done_ms", "done_rank", "others_mib_at_first_done", "error"}),
          p2(dir / "e10_p2.csv",
             {"series", "rep", "mode", "file", "doc", "prio_initial", "issue_ms", "first_byte_ms",
              "done_ms", "done_rank", "a_first_outcome", "a_first_outcome_ms", "raise_issue_ms",
              "raise_call_ms", "a_second_outcome", "a_second_outcome_ms", "error"}),
          p3(dir / "e10_p3.csv",
             {"series", "rep", "variant", "doc", "updates", "first_byte_ms", "done_ms",
              "incr_median_kib", "incr_max_kib", "interval_median_ms", "interval_max_ms",
              "segs_verified_during", "segs_total", "mismatches", "regressions", "path_changes",
              "unreadable_stale_path", "final_ok", "error"}),
          p4(dir / "e10_p4.csv", {"series", "rep", "target", "op", "i", "ms", "ok"}),
          p5(dir / "e10_p5.csv",
             {"series", "rep", "variant", "trigger_ms", "prefix_before_kib", "old_outcome",
              "old_outcome_rel_ms", "new_first_byte_ms", "new_done_ms", "preserved_prefix_kib",
              "preserved_ok", "new_data_ok", "extra_after_done_kib", "max_gap_ms", "final_ok",
              "error"}),
          p6(dir / "e10_p6.csv", {"series",
                                  "rep",
                                  "variant",
                                  "w0_mib",
                                  "wmax_mib",
                                  "mib_s",
                                  "wall_s",
                                  "first_byte_ms",
                                  "lat_median_ms",
                                  "lat_p99_ms",
                                  "lat_max_ms",
                                  "chunk_start_median_ms",
                                  "chunk_start_p99_ms",
                                  "reads",
                                  "downloads",
                                  "displaced",
                                  "absorbed",
                                  "extended",
                                  "raised",
                                  "reprio",
                                  "lost",
                                  "prefix_checks",
                                  "prefix_hits",
                                  "ready_hits",
                                  "calls",
                                  "net_recv_mib",
                                  "mismatches",
                                  "cold_checked",
                                  "error"}),
          p6_reads(dir / "e10_p6_reads.csv", {"series", "i", "off", "total_ms", "ensure_ms"}),
          p7(dir / "e10_p7.csv", {"series", "rep", "step", "ok", "ms", "net_recv_kib", "note"}),
          events(dir / "e10_events.csv", {"item", "series", "doc", "t_ms", "download_offset",
                                          "prefix", "downloaded", "active", "completed"}),
          series(dir / "e10_series.csv", {"series", "item", "status", "start_utc", "end_utc",
                                          "wall_s", "attempts", "cold_checked", "note"}) {}

    void dump_events(const std::string& item, const std::string& series_id,
                     const std::vector<EventLog::Ev>& evs) {
        for (const auto& e : evs) {
            events.row({item, series_id, std::to_string(e.doc), fmt(e.t_ms),
                        std::to_string(e.offset), std::to_string(e.prefix),
                        std::to_string(e.downloaded), e.active ? "1" : "0",
                        e.completed ? "1" : "0"});
        }
    }
};

std::string yn(bool v) { return v ? "1" : "0"; }

td::Json download_request(int32_t file_id, int prio, int64_t off, int64_t lim, bool sync) {
    return td::Json{{"@type", "downloadFile"}, {"file_id", file_id}, {"priority", prio},
                    {"offset", off},           {"limit", lim},       {"synchronous", sync}};
}

// Исход синхронного downloadFile, ожидаемый в отдельном потоке (с моментом исхода).
struct SyncOutcome {
    std::string text = "pending";
    double t_ms = -1;
    std::thread th;
    void start(td::TdFuture fut, const ExpClock::time_point t0) {
        th = std::thread([this, t0, f = std::move(fut)]() mutable {
            std::string r;
            try {
                f.get();
                r = "ok";
            } catch (const td::TdException& e) {
                r = e.error().to_string();
            }
            text = r;
            t_ms = ms_since(t0);
        });
    }
    ~SyncOutcome() {
        if (th.joinable()) {
            th.join();
        }
    }
};

// ---- Пункт 1: приоритет между файлами -----------------------------------------------------

void run_p1(Session& s, Out& out, const std::string& id, int rep, const std::string& variant,
            int variant_no) {
    const int g = (rep * 3 + variant_no) % 8;
    std::vector<int> docs{4 * g, 4 * g + 1, 4 * g + 2, 4 * g + 3};
    s.cold_reset(docs);
    std::vector<int> prios = variant == "asc"    ? std::vector<int>{1, 8, 16, 32}
                             : variant == "desc" ? std::vector<int>{32, 16, 8, 1}
                                                 : std::vector<int>{16, 16, 16, 16};
    EventLog ev(s, docs);
    ev.mark();
    std::vector<double> issue_ms(4);
    std::vector<double> call_ms(4);
    for (size_t k = 0; k < 4; ++k) {
        issue_ms[k] = ev.now_ms();
        const auto t = ExpClock::now();
        s.c().call(download_request(s.file_id(docs[k]), prios[k], 0, 0, false));
        call_ms[k] = ms_since(t);
    }
    const bool ok =
        ev.wait_until([&](auto& e) { return EventLog::all_done(e, docs); }, seconds{240});
    const auto evs = ev.snapshot();
    // Порядок завершения и объём, который успели получить остальные к первому завершению.
    std::vector<std::pair<double, size_t>> done_order;
    for (size_t k = 0; k < 4; ++k) {
        done_order.emplace_back(EventLog::done(evs, docs[k]).value_or(1e18), k);
    }
    std::sort(done_order.begin(), done_order.end());
    double others_mib = 0;
    if (ok) {
        const double t_first = done_order.front().first;
        for (size_t k = 0; k < 4; ++k) {
            if (k == done_order.front().second) {
                continue;
            }
            int64_t best = 0;
            for (const auto& e : evs) {
                if (e.doc == docs[k] && e.t_ms <= t_first) {
                    best = std::max(best, e.downloaded);
                }
            }
            others_mib += to_mib(static_cast<double>(best));
        }
    }
    for (size_t k = 0; k < 4; ++k) {
        int rank = 0;
        for (size_t r = 0; r < done_order.size(); ++r) {
            if (done_order[r].second == k) {
                rank = static_cast<int>(r) + 1;
            }
        }
        const auto fb = EventLog::first_byte(evs, docs[k]);
        const auto dn = EventLog::done(evs, docs[k]);
        out.p1.row({id, std::to_string(rep), variant, std::to_string(k), std::to_string(docs[k]),
                    std::to_string(prios[k]), fmt(issue_ms[k]), fmt(call_ms[k]), fb ? fmt(*fb) : "",
                    dn ? fmt(*dn) : "", dn ? std::to_string(rank) : "", fmt(others_mib, 2),
                    ok ? "" : "timeout"});
    }
    out.dump_events("p1", id, evs);
    if (!ok) {
        throw std::runtime_error("p1: downloads did not finish");
    }
}

// ---- Пункт 2: повышение приоритета --------------------------------------------------------

void run_p2(Session& s, Out& out, const std::string& id, int rep, const std::string& mode,
            int mode_no) {
    const int g = (rep * 3 + mode_no + 1) % 8;
    // A — документ 0 группы; B, C, D — остальные.
    const int a = 4 * g;
    std::vector<int> bcd{4 * g + 1, 4 * g + 2, 4 * g + 3};
    std::vector<int> all{a, bcd[0], bcd[1], bcd[2]};
    s.cold_reset(all);
    EventLog ev(s, all);
    ev.mark();
    std::map<int, double> issue;
    for (const int d : bcd) {
        issue[d] = ev.now_ms();
        s.c().call(download_request(s.file_id(d), 8, 0, 0, false));
    }
    SyncOutcome first;
    SyncOutcome second;
    issue[a] = ev.now_ms();
    first.start(s.c().call_async(download_request(s.file_id(a), 1, 0, 0, true), seconds{300}),
                ev.origin());
    std::this_thread::sleep_for(milliseconds{300});
    double raise_issue = -1;
    double raise_call = -1;
    if (mode != "control") {
        raise_issue = ev.now_ms();
        const auto t = ExpClock::now();
        if (mode == "raise") {
            s.c().call(download_request(s.file_id(a), 32, 0, 0, false));
            raise_call = ms_since(t);
        } else {
            second.start(
                s.c().call_async(download_request(s.file_id(a), 32, 0, 0, true), seconds{300}),
                ev.origin());
            raise_call = ms_since(t);
        }
    }
    const bool ok =
        ev.wait_until([&](auto& e) { return EventLog::all_done(e, all); }, seconds{240});
    if (first.th.joinable()) {
        // Исход ждёт завершения или отмены; после завершения файла он приходит сразу.
        for (int i = 0; i < 100 && first.t_ms < 0; ++i) {
            std::this_thread::sleep_for(milliseconds{100});
        }
    }
    const auto evs = ev.snapshot();
    std::vector<std::pair<double, int>> order;
    for (const int d : all) {
        order.emplace_back(EventLog::done(evs, d).value_or(1e18), d);
    }
    std::sort(order.begin(), order.end());
    const char* tags[] = {"A", "B", "C", "D"};
    for (size_t k = 0; k < 4; ++k) {
        const int d = all[k];
        int rank = 0;
        for (size_t r = 0; r < order.size(); ++r) {
            rank = order[r].second == d ? static_cast<int>(r) + 1 : rank;
        }
        const auto fb = EventLog::first_byte(evs, d);
        const auto dn = EventLog::done(evs, d);
        out.p2.row({id, std::to_string(rep), mode, tags[k], std::to_string(d),
                    std::to_string(k == 0 ? 1 : 8), fmt(issue[d]), fb ? fmt(*fb) : "",
                    dn ? fmt(*dn) : "", dn ? std::to_string(rank) : "", k == 0 ? first.text : "",
                    k == 0 ? fmt(first.t_ms) : "",
                    k == 0 && raise_issue >= 0 ? fmt(raise_issue) : "",
                    k == 0 && raise_call >= 0 ? fmt(raise_call) : "",
                    k == 0 && mode == "raise_sync" ? second.text : "",
                    k == 0 && second.t_ms >= 0 ? fmt(second.t_ms) : "", ok ? "" : "timeout"});
    }
    out.dump_events("p2", id, evs);
    if (!ok) {
        throw std::runtime_error("p2: downloads did not finish");
    }
}

// ---- Пункт 3: асинхронный ход и данные префикса -----------------------------------------------

void run_p3(Session& s, Out& out, const std::string& id, int rep, const std::string& variant) {
    const int doc = (rep * 2 + (variant == "from0" ? 0 : 1)) % kDocs;
    s.cold_reset({doc});
    const Doc& d = s.doc_at(doc);
    const int64_t offset =
        variant == "from0"
            ? 0
            : static_cast<int64_t>(crypto::kChunkHeaderSize + 48 * crypto::kEncSegmentSize);
    const uint64_t first_seg = variant == "from0" ? 0 : 48;
    EventLog ev(s, {doc});
    ev.mark();
    s.c().call(download_request(s.file_id(doc), 16, offset, 0, false));
    std::set<uint64_t> verified;
    uint64_t during = 0;
    uint64_t mismatches = 0;
    uint64_t unreadable = 0;  // сегменты, не прочитанные по устаревшему пути (повтор по новому)
    size_t seen = 0;
    int64_t prev_prefix = -1;
    int64_t prev_offset = -1;
    uint64_t regressions = 0;
    std::vector<double> incr;
    std::vector<double> intervals;
    double prev_t = -1;
    std::string path;
    uint64_t path_changes = 0;
    bool finished = false;
    const auto deadline = ExpClock::now() + seconds{240};
    const auto verify_new = [&](int64_t off, int64_t prefix, bool completed) {
        // Полные сегменты внутри [off, off + prefix).
        const int64_t end = off + prefix;
        std::vector<uint64_t> fresh;
        for (uint64_t seg = first_seg; seg < kSegments; ++seg) {
            const uint64_t eo = crypto::kChunkHeaderSize + seg * crypto::kEncSegmentSize;
            if (static_cast<int64_t>(eo + crypto::kEncSegmentSize) > end && !completed) {
                break;
            }
            if (!verified.contains(seg) && static_cast<int64_t>(eo) >= off) {
                fresh.push_back(seg);
            }
        }
        if (fresh.empty() || path.empty()) {
            return;
        }
        // Подряд идущие куски проверяются пачкой.
        size_t i = 0;
        while (i < fresh.size()) {
            size_t j = i;
            while (j + 1 < fresh.size() && fresh[j + 1] == fresh[j] + 1) {
                ++j;
            }
            int kind = 0;
            bool ok = verify_segments(s.crypto, d, path, fresh[i], fresh[j] - fresh[i] + 1, &kind);
            if (kind == 1) {
                // Путь устарел (файл переименован при завершении): повтор по свежему пути TDLib.
                unreadable += fresh[j] - fresh[i] + 1;
                path = s.local_path(doc);
                ok = verify_segments(s.crypto, d, path, fresh[i], fresh[j] - fresh[i] + 1, &kind);
            }
            for (size_t k = i; k <= j; ++k) {
                verified.insert(fresh[k]);
            }
            if (!ok) {
                mismatches += fresh[j] - fresh[i] + 1;
            } else if (!completed) {
                during += fresh[j] - fresh[i] + 1;
            }
            i = j + 1;
        }
    };
    while (!finished && ExpClock::now() < deadline) {
        const size_t n = ev.wait_more(seen, milliseconds{500});
        const auto evs = ev.snapshot();
        for (; seen < n; ++seen) {
            const auto& e = evs[seen];
            if (e.offset == prev_offset && prev_prefix >= 0) {
                if (e.prefix < prev_prefix && !e.completed) {
                    ++regressions;
                } else if (e.prefix > prev_prefix) {
                    incr.push_back(static_cast<double>(e.prefix - prev_prefix) / 1024.0);
                }
            }
            if (prev_t >= 0) {
                intervals.push_back(e.t_ms - prev_t);
            }
            prev_t = e.t_ms;
            prev_prefix = e.prefix;
            prev_offset = e.offset;
            if (!e.path.empty()) {
                if (e.path != path && !path.empty()) {
                    ++path_changes;
                }
                path = e.path;
            }
            const bool end_reached = e.completed || (e.offset == offset &&
                                                     e.offset + e.prefix >= s.doc_at(doc).enc_size);
            verify_new(e.offset, e.prefix, end_reached);
            finished = finished || end_reached;
        }
    }
    const auto evs = ev.snapshot();
    // Итог: весь файл от first_seg.
    const std::string final_path = s.local_path(doc);
    const bool final_ok =
        finished && verify_segments(s.crypto, d, final_path, first_seg, kSegments - first_seg);
    const auto fb = EventLog::first_byte(evs, doc);
    std::optional<double> dn = EventLog::done(evs, doc);
    if (!dn && finished && !evs.empty()) {
        dn = evs.back().t_ms;
    }
    out.p3.row({id, std::to_string(rep), variant, std::to_string(doc), std::to_string(evs.size()),
                fb ? fmt(*fb) : "", dn ? fmt(*dn) : "", fmt(percentile(incr, 0.5)),
                fmt(percentile(incr, 1.0)), fmt(percentile(intervals, 0.5)),
                fmt(percentile(intervals, 1.0)), std::to_string(during),
                std::to_string(verified.size()), std::to_string(mismatches),
                std::to_string(regressions), std::to_string(path_changes),
                std::to_string(unreadable), yn(final_ok), finished ? "" : "timeout"});
    out.dump_events("p3", id, evs);
    if (!finished) {
        throw std::runtime_error("p3: download did not finish");
    }
}

// ---- Пункт 4: уже скачанный диапазон ------------------------------------------------------

void run_p4(Session& s, Out& out, const std::string& id, int rep) {
    const int x = (rep * 2) % kDocs;
    const int y = (rep * 2 + 1) % kDocs;
    s.cold_reset({x, y});
    EventLog ev(s, {y});
    ev.mark();
    s.c().call(download_request(s.file_id(x), 16, 0, 0, true), seconds{240});
    // Y: только [0, 4 МиБ).
    s.c().call(download_request(s.file_id(y), 16, 0, static_cast<int64_t>(4 * kMiB), false));
    if (!ev.wait_until(
            [&](auto& e) {
                for (const auto& v : e) {
                    if (v.offset == 0 && v.prefix >= static_cast<int64_t>(4 * kMiB)) {
                        return true;
                    }
                }
                return false;
            },
            seconds{120})) {
        throw std::runtime_error("p4: the partial file did not reach 4 MiB");
    }
    std::mt19937_64 rng(0xE10A4ULL + static_cast<uint64_t>(rep));
    const uint64_t len = 2 * crypto::kEncSegmentSize;  // 128 КиБ открытого текста
    struct Target {
        std::string name;
        int doc;
        uint64_t max_k;
        bool miss;
    };
    const uint64_t k_x =
        (static_cast<uint64_t>(s.doc_at(x).enc_size) - crypto::kChunkHeaderSize - len) /
        crypto::kEncSegmentSize;
    const uint64_t k_y = (4 * kMiB - crypto::kChunkHeaderSize - len) / crypto::kEncSegmentSize;
    for (const Target& t : {Target{"X-full", x, k_x, false}, Target{"Y-partial", y, k_y, false},
                            Target{"Y-miss", y, k_x, true}}) {
        const std::string path = s.local_path(t.doc);
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("p4: cannot open the local file");
        }
        std::vector<uint64_t> offs;
        for (int i = 0; i < 100; ++i) {
            uint64_t k = rng() % (t.max_k + 1);
            if (t.miss) {
                k = std::max<uint64_t>(k,
                                       (5 * kMiB) / crypto::kEncSegmentSize);  // за пределами 4 МиБ
                k = std::min(k, t.max_k);
            }
            offs.push_back(crypto::kChunkHeaderSize + k * crypto::kEncSegmentSize);
        }
        std::string buf(len, '\0');
        for (size_t i = 0; i < 100; ++i) {
            const auto t0 = ExpClock::now();
            const td::Json r = s.c().call(td::Json{{"@type", "getFileDownloadedPrefixSize"},
                                                   {"file_id", s.file_id(t.doc)},
                                                   {"offset", offs[i]}});
            const double ms = ms_since(t0);
            const bool ok = (r.value("size", int64_t{0}) >= static_cast<int64_t>(len)) != t.miss;
            out.p4.row({id, std::to_string(rep), t.name, "prefix_size", std::to_string(i),
                        fmt(ms, 3), yn(ok)});
        }
        if (!t.miss) {
            for (size_t i = 0; i < 100; ++i) {
                const auto t0 = ExpClock::now();
                const bool ok = pread_all(fd, offs[i], buf.data(), buf.size());
                out.p4.row({id, std::to_string(rep), t.name, "pread", std::to_string(i),
                            fmt(ms_since(t0), 3), yn(ok)});
            }
            const NetCounters n0 = read_net(s.c());
            for (size_t i = 0; i < 100; ++i) {
                const auto t0 = ExpClock::now();
                bool ok = true;
                try {
                    s.c().call(download_request(s.file_id(t.doc), 16, static_cast<int64_t>(offs[i]),
                                                static_cast<int64_t>(len), true),
                               seconds{60});
                } catch (const td::TdException&) {
                    ok = false;
                }
                out.p4.row({id, std::to_string(rep), t.name, "downloadFile", std::to_string(i),
                            fmt(ms_since(t0), 3), yn(ok)});
            }
            const NetCounters n1 = read_net(s.c());
            out.p4.row({id, std::to_string(rep), t.name, "downloadFile_net_recv_kib", "0",
                        fmt((n1.received - n0.received) / 1024.0), "1"});
        }
        ::close(fd);
    }
}

// ---- Пункт 5: вытеснение ------------------------------------------------------------------

void run_p5(Session& s, Out& out, const std::string& id, int rep, const std::string& variant,
            int variant_no) {
    const int doc = (rep * 3 + variant_no + 8) % kDocs;
    s.cold_reset({doc});
    const Doc& d = s.doc_at(doc);
    EventLog ev(s, {doc});
    ev.mark();
    const bool sync = variant != "async";
    const bool extend = variant == "extend";
    const int64_t old_limit =
        extend ? static_cast<int64_t>(4 * kMiB) : static_cast<int64_t>(8 * kMiB);
    SyncOutcome old_req;
    SyncOutcome new_req;
    const auto t_origin = ExpClock::now();
    if (sync) {
        old_req.start(s.c().call_async(download_request(s.file_id(doc), 16, 0, old_limit, true),
                                       seconds{300}),
                      t_origin);
    } else {
        s.c().call(download_request(s.file_id(doc), 16, 0, old_limit, false));
    }
    // Ждём, пока скачан хотя бы 1 МиБ.
    if (!ev.wait_until(
            [&](auto& e) {
                for (const auto& v : e) {
                    if (v.downloaded >= static_cast<int64_t>(kMiB)) {
                        return true;
                    }
                }
                return false;
            },
            seconds{120})) {
        throw std::runtime_error("p5: the download did not progress");
    }
    const double trigger_ms = ev.now_ms();
    int64_t prefix_before = 0;
    for (const auto& v : ev.snapshot()) {
        if (v.offset == 0) {
            prefix_before = std::max(prefix_before, v.prefix);
        }
    }
    const int64_t new_off = extend ? 0 : static_cast<int64_t>(6 * kMiB);
    const int64_t new_lim = extend ? 0 : static_cast<int64_t>(kMiB);  // extend: до конца файла
    const size_t events_before = ev.snapshot().size();
    if (sync && !extend) {
        new_req.start(s.c().call_async(download_request(s.file_id(doc), 16, new_off, new_lim, true),
                                       seconds{300}),
                      t_origin);
    } else {
        s.c().call(download_request(s.file_id(doc), 16, new_off, new_lim, false));
    }
    const auto reached = [&](const std::vector<EventLog::Ev>& e) {
        for (size_t i = events_before; i < e.size(); ++i) {
            if (extend ? e[i].completed
                       : (e[i].offset == new_off && e[i].prefix >= new_lim) || e[i].completed) {
                return true;
            }
        }
        return false;
    };
    const bool ok = ev.wait_until(reached, seconds{120});
    const auto evs_done = ev.snapshot();
    std::optional<double> new_first;
    std::optional<double> new_done;
    for (size_t i = events_before; i < evs_done.size(); ++i) {
        const auto& e = evs_done[i];
        if (!new_first && e.offset == new_off && e.prefix > 0) {
            new_first = e.t_ms - trigger_ms;
        }
        if (!new_done && ((e.offset == new_off && e.prefix >= new_lim) || e.completed)) {
            new_done = e.t_ms - trigger_ms;
        }
    }
    // Наибольший зазор между апдейтами в окне вокруг смены запроса.
    double max_gap = 0;
    for (size_t i = 1; i < evs_done.size(); ++i) {
        max_gap = std::max(max_gap, evs_done[i].t_ms - evs_done[i - 1].t_ms);
    }
    // Что осталось и продолжает ли TDLib качать прежнее окно.
    const int64_t downloaded_at_done =
        s.get_file(doc)["local"].value("downloaded_size", int64_t{0});
    std::this_thread::sleep_for(seconds{3});
    const td::Json after = s.get_file(doc);
    const int64_t downloaded_after = after["local"].value("downloaded_size", int64_t{0});
    const std::string path = after["local"].value("path", std::string{});
    const td::Json pr = s.c().call(td::Json{
        {"@type", "getFileDownloadedPrefixSize"}, {"file_id", s.file_id(doc)}, {"offset", 0}});
    const int64_t preserved = pr.value("size", int64_t{0});
    // Сохранённая часть читается и верна: целые сегменты внутри [0, prefix_before).
    const uint64_t segs_before =
        prefix_before > static_cast<int64_t>(crypto::kChunkHeaderSize)
            ? (static_cast<uint64_t>(prefix_before) - crypto::kChunkHeaderSize) /
                  crypto::kEncSegmentSize
            : 0;
    const bool preserved_ok =
        preserved >= prefix_before &&
        (segs_before == 0 || verify_segments(s.crypto, d, path, 0, segs_before));
    // Данные нового окна: сегменты, целиком лежащие в [6 МиБ, 7 МиБ).
    bool new_data_ok = true;
    bool final_ok = true;
    if (extend) {
        final_ok = verify_segments(s.crypto, d, path, 0, kSegments);
    } else {
        const uint64_t seg_a =
            (static_cast<uint64_t>(new_off) - crypto::kChunkHeaderSize) / crypto::kEncSegmentSize +
            1;
        const uint64_t seg_b =
            (static_cast<uint64_t>(new_off + new_lim) - crypto::kChunkHeaderSize) /
            crypto::kEncSegmentSize;
        new_data_ok = seg_b > seg_a && verify_segments(s.crypto, d, path, seg_a, seg_b - seg_a);
    }
    // Исходы прежнего и нового запросов (синхронные ждут конца или отмены).
    for (int i = 0; i < 50 && (sync && old_req.t_ms < 0); ++i) {
        std::this_thread::sleep_for(milliseconds{100});
    }
    out.p5.row({id, std::to_string(rep), variant, fmt(trigger_ms),
                fmt(static_cast<double>(prefix_before) / 1024.0),
                sync ? old_req.text : "n/a (async)",
                sync && old_req.t_ms >= 0 ? fmt(old_req.t_ms - trigger_ms) : "",
                new_first ? fmt(*new_first) : "", new_done ? fmt(*new_done) : "",
                fmt(static_cast<double>(preserved) / 1024.0), yn(preserved_ok), yn(new_data_ok),
                fmt(static_cast<double>(downloaded_after - downloaded_at_done) / 1024.0),
                fmt(max_gap), yn(final_ok), ok ? "" : "timeout"});
    out.dump_events("p5", id, evs_done);
    if (!ok) {
        throw std::runtime_error("p5: the new request did not finish");
    }
}

// ---- Пункт 6: прототип ChunkReader поверх планировщика ------------------------------------

class ChunkReader {
  public:
    struct Cfg {
        bool readahead = false;
        int64_t w0 = 0;
        int64_t wmax = 0;
    };
    struct Info {
        double total_ms = 0;
        double ensure_ms = 0;
    };

    ChunkReader(Session& s, ReadScheduler& sched, Cfg cfg)
        : s_(s), sched_(sched), cfg_(cfg), fds_(kDocs, -1), headers_(kDocs) {}
    ~ChunkReader() {
        for (const int fd : fds_) {
            if (fd >= 0) {
                ::close(fd);
            }
        }
    }

    Bytes read(uint64_t off, uint64_t len, Info* info) {
        const auto t0 = ExpClock::now();
        const uint64_t total = kDocs * kPlain;
        len = std::min(len, total - off);
        const bool seq = have_last_ && off + w_ >= last_end_ && off <= last_end_ + w_;
        if (!seq) {
            w_ = 0;
            ra_end_ = 0;
        } else {
            w_ = w_ == 0 ? static_cast<uint64_t>(cfg_.w0)
                         : std::min<uint64_t>(w_ * 2, static_cast<uint64_t>(cfg_.wmax));
        }
        if (cfg_.readahead && seq) {
            const uint64_t end = off + len;
            if (ra_end_ < end + w_ / 2) {
                const uint64_t start = std::max(ra_end_, end);
                const uint64_t target = std::min(total, end + w_);
                prefetch_plain(start, target, off / kPlain);
                ra_end_ = std::max(ra_end_, target);
            }
        }
        Bytes out;
        double ensure_ms = 0;
        for (uint64_t pos = off; pos < off + len;) {
            const uint64_t c = pos / kPlain;
            const uint64_t in_chunk = pos % kPlain;
            const uint64_t n = std::min(off + len - pos, kPlain - in_chunk);
            const Bytes part = read_chunk(c, in_chunk, n, &ensure_ms);
            out.insert(out.end(), part.begin(), part.end());
            pos += n;
        }
        have_last_ = true;
        last_end_ = off + len;
        if (info != nullptr) {
            info->total_ms = ms_since(t0);
            info->ensure_ms = ensure_ms;
        }
        return out;
    }

  private:
    // Кусок открытого текста [p, q) чанка c -> диапазон файла чанка. Заголовок включается, если
    // кусок начинается в первом мегабайте.
    static std::pair<int64_t, int64_t> enc_range(uint64_t in_chunk, uint64_t n) {
        const crypto::SegmentSpan span = crypto::chunk_segments_for(kPlain, in_chunk, n);
        const uint64_t a = span.enc_offset <= kMiB ? 0 : span.enc_offset;
        return {static_cast<int64_t>(a), static_cast<int64_t>(span.enc_offset + span.enc_len)};
    }

    void prefetch_plain(uint64_t start, uint64_t target, uint64_t cur_chunk) {
        for (uint64_t pos = start; pos < target;) {
            const uint64_t c = pos / kPlain;
            const uint64_t in_chunk = pos % kPlain;
            const uint64_t n = std::min(target - pos, kPlain - in_chunk);
            const auto [a, b] = enc_range(in_chunk, n);
            sched_.prefetch(s_.file_ids[c], a, b, static_cast<int>(c - cur_chunk));
            pos += n;
        }
    }

    Bytes read_chunk(size_t c, uint64_t in_chunk, uint64_t n, double* ensure_ms) {
        const Doc& d = s_.docs[c];
        const crypto::SegmentSpan span = crypto::chunk_segments_for(kPlain, in_chunk, n);
        auto [a, b] = enc_range(in_chunk, n);
        const auto t0 = ExpClock::now();
        std::string path;
        if (!headers_[c] && a > 0) {
            path = sched_.ensure(s_.file_ids[c], 0, static_cast<int64_t>(crypto::kChunkHeaderSize));
        }
        path = sched_.ensure(s_.file_ids[c], a, b);
        *ensure_ms += ms_since(t0);
        if (fds_[c] < 0) {
            fds_[c] = ::open(path.c_str(), O_RDONLY);
            if (fds_[c] < 0) {
                throw std::runtime_error("cannot open the local file of chunk " +
                                         std::to_string(c));
            }
        }
        if (!headers_[c]) {
            Bytes raw(crypto::kChunkHeaderSize);
            if (!pread_all(fds_[c], 0, raw.data(), raw.size())) {
                throw std::runtime_error("cannot read the chunk header");
            }
            headers_[c] = crypto::parse_chunk_header(ByteSpan{raw});
        }
        Bytes enc(span.enc_len);
        if (!pread_all(fds_[c], span.enc_offset, enc.data(), enc.size())) {
            throw std::runtime_error("cannot read the chunk range");
        }
        Bytes plain = crypto::decrypt_chunk_segments(s_.crypto.key, s_.crypto.uuid, d.chunk_id,
                                                     *headers_[c], span.first, ByteSpan{enc});
        const uint64_t skip = in_chunk - span.plain_offset;
        return Bytes(plain.begin() + static_cast<std::ptrdiff_t>(skip),
                     plain.begin() + static_cast<std::ptrdiff_t>(skip + n));
    }

    Session& s_;
    ReadScheduler& sched_;
    Cfg cfg_;
    std::vector<int> fds_;
    std::vector<std::optional<crypto::ChunkHeader>> headers_;
    bool have_last_ = false;
    uint64_t last_end_ = 0;
    uint64_t w_ = 0;
    uint64_t ra_end_ = 0;
};

struct Variant6 {
    std::string name;
    bool readahead;
    int w0_mib;
    int wmax_mib;
};

Variant6 variant6(const std::string& name) {
    if (name == "none") {
        return {name, false, 0, 0};
    }
    if (name == "d4_32") {
        return {name, true, 4, 32};
    }
    const int w = std::stoi(name.substr(1));
    return {name, true, w, w};
}

void run_p6(Session& s, Out& out, const E10Options& e10, const std::string& id, int rep,
            const std::string& vname) {
    const Variant6 v = variant6(vname);
    const int cold = s.cold_reset(s.all_idxs());
    SchedConfig cfg;
    cfg.download_concurrency = e10.download_concurrency;
    const NetCounters n0 = read_net(s.c());
    ReadScheduler sched(s.c(), cfg);
    for (size_t i = 0; i < s.docs.size(); ++i) {
        sched.add_file(s.file_ids[i], s.docs[i].enc_size);
    }
    ChunkReader reader(s, sched,
                       {v.readahead, static_cast<int64_t>(v.w0_mib) * static_cast<int64_t>(kMiB),
                        static_cast<int64_t>(v.wmax_mib) * static_cast<int64_t>(kMiB)});
    const uint64_t total = static_cast<uint64_t>(e10.seq_mib) * kMiB;
    constexpr uint64_t kRead = 128 * kKiB;
    std::vector<double> lat;
    std::vector<double> chunk_start;
    std::vector<std::tuple<uint64_t, double, double>> reads;
    uint64_t mismatches = 0;
    double verify_ms = 0;
    double first_byte = 0;
    std::string error;
    const auto t_start = ExpClock::now();
    try {
        for (uint64_t off = 0; off < total; off += kRead) {
            if (stopping()) {
                throw Interrupted();
            }
            ChunkReader::Info info;
            const Bytes got = reader.read(off, kRead, &info);
            if (off == 0) {
                first_byte = info.total_ms;
            }
            lat.push_back(info.total_ms);
            if (off % kPlain == 0 && off != 0) {
                chunk_start.push_back(info.total_ms);
            }
            reads.emplace_back(off, info.total_ms, info.ensure_ms);
            const auto tv = ExpClock::now();
            std::string want;
            fill_seeded(s.docs[off / kPlain].seed, off % kPlain, want, got.size());
            if (got.size() != want.size() ||
                std::memcmp(got.data(), want.data(), want.size()) != 0) {
                ++mismatches;
            }
            verify_ms += ms_since(tv);
        }
    } catch (const td::TdException&) {
        throw;
    } catch (const Interrupted&) {
        throw;
    } catch (const std::exception& e) {
        error = e.what();
    }
    const double wall_s = (ms_since(t_start) - verify_ms) / 1000.0;
    const NetCounters n1 = read_net(s.c());
    const SchedStats st = sched.stats();
    for (const auto& [off, total_ms, ensure_ms] :
         error.empty() && mismatches == 0 ? reads : decltype(reads){}) {
        out.p6_reads.row({id, std::to_string(off / kRead), std::to_string(off), fmt(total_ms, 3),
                          fmt(ensure_ms, 3)});
    }
    out.p6.row({id,
                std::to_string(rep),
                vname,
                std::to_string(v.w0_mib),
                std::to_string(v.wmax_mib),
                error.empty() ? fmt(to_mib(static_cast<double>(total)) / wall_s, 3) : "",
                fmt(wall_s, 2),
                fmt(first_byte),
                fmt(percentile(lat, 0.5), 3),
                fmt(percentile(lat, 0.99), 3),
                fmt(percentile(lat, 1.0), 3),
                fmt(percentile(chunk_start, 0.5), 3),
                fmt(percentile(chunk_start, 0.99), 3),
                std::to_string(lat.size()),
                std::to_string(st.downloads),
                std::to_string(st.displaced),
                std::to_string(st.absorbed),
                std::to_string(st.extended),
                std::to_string(st.raised),
                std::to_string(st.reprio),
                std::to_string(st.lost),
                std::to_string(st.prefix_checks),
                std::to_string(st.prefix_hits),
                std::to_string(st.ready_hits),
                std::to_string(st.calls),
                fmt(to_mib(n1.received - n0.received), 1),
                std::to_string(mismatches),
                std::to_string(cold),
                error});
    if (!error.empty() || mismatches != 0) {
        throw std::runtime_error("p6: " + (error.empty() ? "data mismatch" : error));
    }
}

// ---- Пункт 7: после перезапуска ---------------------------------------------------------------

struct P7Doc {
    int idx = 0;
    std::string remote_id;
    std::string path;
    int64_t downloaded = 0;
    int64_t prefix0 = 0;
};

td::Json p7_to_json(const std::map<std::string, P7Doc>& m) {
    td::Json j = td::Json::object();
    for (const auto& [k, d] : m) {
        j[k] = td::Json{{"idx", d.idx},
                        {"remote_id", d.remote_id},
                        {"path", d.path},
                        {"downloaded", d.downloaded},
                        {"prefix0", d.prefix0}};
    }
    return j;
}

P7Doc p7_from_json(const td::Json& j) {
    P7Doc d;
    d.idx = j.at("idx").get<int>();
    d.remote_id = j.at("remote_id").get<std::string>();
    d.path = j.at("path").get<std::string>();
    d.downloaded = j.at("downloaded").get<int64_t>();
    d.prefix0 = j.at("prefix0").get<int64_t>();
    return d;
}

void run_p7_prepare(Session& s, Out& out, const BenchOptions& options, const std::string& id,
                    int rep) {
    const int base = (rep * 6) % kDocs;
    const auto doc = [&](int k) { return (base + k) % kDocs; };
    const int x = doc(0);
    const int y = doc(1);
    const int z = doc(2);
    const int w1 = doc(3);
    const int w2 = doc(4);
    s.cold_reset({x, y, z, w1, w2, doc(5)});
    const int64_t limit_part = static_cast<int64_t>(3 * kMiB);
    const auto full = [&](int d) {
        s.c().call(download_request(s.file_id(d), 16, 0, 0, true), seconds{240});
    };
    const auto partial = [&](int d) {
        s.c().call(download_request(s.file_id(d), 16, 0, limit_part, false));
        for (int i = 0; i < 600; ++i) {
            const td::Json pr = s.c().call(td::Json{{"@type", "getFileDownloadedPrefixSize"},
                                                    {"file_id", s.file_id(d)},
                                                    {"offset", 0}});
            if (pr.value("size", int64_t{0}) >= limit_part) {
                return;
            }
            std::this_thread::sleep_for(milliseconds{100});
        }
        throw std::runtime_error("p7: the partial download did not reach 3 MiB");
    };
    full(x);
    partial(y);
    full(z);
    full(w1);
    partial(w2);
    std::map<std::string, P7Doc> info;
    const auto snapshot = [&](const std::string& key, int d) {
        const td::Json f = s.get_file(d);
        P7Doc p;
        p.idx = d;
        p.remote_id = s.doc_at(d).remote_id;
        p.path = f["local"].value("path", std::string{});
        p.downloaded = f["local"].value("downloaded_size", int64_t{0});
        p.prefix0 = s.c()
                        .call(td::Json{{"@type", "getFileDownloadedPrefixSize"},
                                       {"file_id", s.file_id(d)},
                                       {"offset", 0}})
                        .value("size", int64_t{0});
        info[key] = p;
        out.p7.row({id, std::to_string(rep), "before restart: " + key, "1", "", "",
                    "downloaded " + std::to_string(p.downloaded) + ", prefix " +
                        std::to_string(p.prefix0) + ", st_blocks " +
                        std::to_string(blocks_bytes(p.path))});
    };
    snapshot("X", x);
    snapshot("Y", y);
    snapshot("Z", z);
    // deleteFile освобождает место.
    for (const auto& [key, d] :
         {std::pair<std::string, int>{"full", w1}, std::pair<std::string, int>{"partial", w2}}) {
        const std::string path = s.local_path(d);
        const int64_t before = blocks_bytes(path);
        const int64_t dir_before = dir_blocks_bytes(s.files_dir);
        const auto t0 = ExpClock::now();
        s.c().call(td::Json{{"@type", "deleteFile"}, {"file_id", s.file_id(d)}});
        const double ms = ms_since(t0);
        const int64_t after = blocks_bytes(path);
        const int64_t dir_after = dir_blocks_bytes(s.files_dir);
        const bool gone = !fs::exists(path);
        out.p7.row({id, std::to_string(rep), "deleteFile " + key,
                    yn(gone && dir_before > dir_after), fmt(ms), "",
                    "st_blocks of the file " + std::to_string(before) + " -> " +
                        std::to_string(after) + ", files dir " + std::to_string(dir_before) +
                        " -> " + std::to_string(dir_after) + " bytes"});
    }
    td::Json state{{"docs", p7_to_json(info)}};
    save_state(options, "e10-p7-" + std::to_string(rep), state);
}

int spawn_wait(const std::vector<std::string>& args) {
    std::vector<char*> argv;
    for (const auto& a : args) {
        argv.push_back(const_cast<char*>(a.c_str()));
    }
    argv.push_back(nullptr);
    pid_t pid = 0;
    const int rc = posix_spawn(&pid, args[0].c_str(), nullptr, nullptr, argv.data(), environ);
    if (rc != 0) {
        throw std::runtime_error("posix_spawn failed");
    }
    int status = 0;
    while (::waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            throw std::runtime_error("waitpid failed");
        }
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

// Проверки пункта 7 в новом процессе с той же БД TDLib.
void restart_check(BenchAccount& bot, Out& out, const BenchOptions& options, int64_t chat_id,
                   const std::string& id, int rep, const Crypto& crypto,
                   const std::vector<Doc>& docs) {
    td::TdClient& c = bot.client();
    const td::Json state = load_state(options, "e10-p7-" + std::to_string(rep));
    std::map<std::string, P7Doc> before;
    for (const auto& [k, j] : state["docs"].items()) {
        before[k] = p7_from_json(j);
    }
    const auto row = [&](const std::string& step, bool ok, double ms, double net_kib,
                         const std::string& note) {
        out.p7.row(
            {id, std::to_string(rep), step, yn(ok), ms < 0 ? "" : fmt(ms), fmt(net_kib), note});
        std::cerr << id << " " << step << ": " << (ok ? "ok" : "FAIL") << " " << note << "\n";
    };
    std::map<std::string, int32_t> fid;
    // 1-3. getRemoteFile и состояние на диске без нового скачивания.
    const NetCounters n_start = read_net(c);
    for (const auto& [key, b] : before) {
        const auto t0 = ExpClock::now();
        try {
            const td::Json f = c.call(td::Json{{"@type", "getRemoteFile"},
                                               {"remote_file_id", b.remote_id},
                                               {"file_type", {{"@type", "fileTypeDocument"}}}});
            const double ms = ms_since(t0);
            fid[key] = f.value("id", int32_t{0});
            const td::Json& l = f["local"];
            const std::string path = l.value("path", std::string{});
            const int64_t prefix = c.call(td::Json{{"@type", "getFileDownloadedPrefixSize"},
                                                   {"file_id", fid[key]},
                                                   {"offset", 0}})
                                       .value("size", int64_t{0});
            const bool file_exists = !path.empty() && fs::exists(path);
            row("getRemoteFile " + key, !path.empty() && path == b.path, ms, 0,
                "path same: " + std::string{path == b.path ? "yes" : "no"} +
                    ", file exists: " + (file_exists ? "yes" : "no") + ", size " +
                    std::to_string(f.value("size", int64_t{0})) + ", downloaded " +
                    std::to_string(l.value("downloaded_size", int64_t{0})) + " (was " +
                    std::to_string(b.downloaded) + "), completed " +
                    (l.value("is_downloading_completed", false) ? "yes" : "no") + ", prefix(0) " +
                    std::to_string(prefix) + " (was " + std::to_string(b.prefix0) + ")");
        } catch (const td::TdException& e) {
            row("getRemoteFile " + key, false, ms_since(t0), 0, e.error().to_string());
        }
    }
    const NetCounters n_after_lookup = read_net(c);
    row("network during getRemoteFile and prefix checks", true, -1,
        (n_after_lookup.received - n_start.received) / 1024.0, "KiB received");
    // Данные X и Y читаются и верны.
    if (fid.contains("X")) {
        const bool ok = verify_segments(
            crypto, docs[static_cast<size_t>(before["X"].idx)],
            bot.client().call(td::Json{{"@type", "getFile"}, {"file_id", fid["X"]}})["local"].value(
                "path", std::string{}),
            0, kSegments);
        row("pread X after restart", ok, -1, 0, "all 128 segments decrypt and match");
    }
    if (fid.contains("Y")) {
        const int64_t prefix = before["Y"].prefix0;
        const uint64_t segs =
            (static_cast<uint64_t>(prefix) - crypto::kChunkHeaderSize) / crypto::kEncSegmentSize;
        const bool ok = verify_segments(
            crypto, docs[static_cast<size_t>(before["Y"].idx)],
            c.call(td::Json{{"@type", "getFile"}, {"file_id", fid["Y"]}})["local"].value(
                "path", std::string{}),
            0, segs);
        row("pread Y prefix after restart", ok, -1, 0, std::to_string(segs) + " segments");
    }
    const auto timed_download = [&](const std::string& step, int32_t file_id, int64_t off,
                                    int64_t lim, const std::string& note_on_ok) {
        const NetCounters n0 = read_net(c);
        const auto t0 = ExpClock::now();
        bool ok = true;
        std::string note = note_on_ok;
        try {
            c.call(download_request(file_id, 16, off, lim, true), seconds{120});
        } catch (const td::TdException& e) {
            ok = false;
            note = e.error().to_string();
        }
        const double ms = ms_since(t0);
        const NetCounters n1 = read_net(c);
        row(step, ok, ms, (n1.received - n0.received) / 1024.0, note);
        return ok;
    };
    // 4. downloadFile уже скачанного: без сети.
    if (fid.contains("X")) {
        timed_download("downloadFile X whole (already downloaded)", fid["X"], 0, 0,
                       "expect no network");
    }
    if (fid.contains("Y")) {
        timed_download("downloadFile Y [0, 3 MiB) (already downloaded)", fid["Y"], 0,
                       static_cast<int64_t>(3 * kMiB), "expect no network");
        timed_download("downloadFile Y [3 MiB, 4 MiB) (rest)", fid["Y"],
                       static_cast<int64_t>(3 * kMiB), static_cast<int64_t>(kMiB),
                       "expect about 1 MiB");
        const std::string path =
            c.call(td::Json{{"@type", "getFile"}, {"file_id", fid["Y"]}})["local"].value(
                "path", std::string{});
        const uint64_t seg_a = (3 * kMiB - crypto::kChunkHeaderSize) / crypto::kEncSegmentSize + 1;
        const uint64_t seg_b = (4 * kMiB - crypto::kChunkHeaderSize) / crypto::kEncSegmentSize;
        row("pread Y [3 MiB, 4 MiB) after resume",
            verify_segments(crypto, docs[static_cast<size_t>(before["Y"].idx)], path, seg_a,
                            seg_b - seg_a),
            -1, 0, "");
    }
    // 5. Z: файл удалён с диска за спиной TDLib до перезапуска.
    if (fid.contains("Z")) {
        const std::string old_path = before["Z"].path;
        row("Z file removed behind TDLib before restart", !fs::exists(old_path), -1, 0,
            "old path exists: " + std::string{fs::exists(old_path) ? "yes" : "no"});
        const bool ok = timed_download("downloadFile Z whole (file was removed)", fid["Z"], 0, 0,
                                       "expect about 8 MiB");
        const std::string path =
            c.call(td::Json{{"@type", "getFile"}, {"file_id", fid["Z"]}})["local"].value(
                "path", std::string{});
        row("pread Z after re-download",
            ok && verify_segments(crypto, docs[static_cast<size_t>(before["Z"].idx)], path, 0,
                                  kSegments),
            -1, 0, "");
    }
    // 6. U: файл удалён с диска при работающем TDLib.
    {
        const int u = (before.at("X").idx + 5) % kDocs;
        const td::Json msgs = c.call(
            td::Json{{"@type", "getMessages"},
                     {"chat_id", chat_id},
                     {"message_ids", td::Json::array({docs[static_cast<size_t>(u)].message_id})}});
        if (!msgs["messages"].empty() && !msgs["messages"][0].is_null()) {
            const int32_t file_id =
                msgs["messages"][0]["content"]["document"]["document"].value("id", int32_t{0});
            attempt([&] { c.call(td::Json{{"@type", "deleteFile"}, {"file_id", file_id}}); });
            timed_download("downloadFile U whole (first)", file_id, 0, 0, "about 8 MiB");
            const std::string path =
                c.call(td::Json{{"@type", "getFile"}, {"file_id", file_id}})["local"].value(
                    "path", std::string{});
            std::error_code ec;
            fs::remove(path, ec);
            const bool ok = timed_download("downloadFile U whole (file removed while TDLib runs)",
                                           file_id, 0, 0, "expect about 8 MiB");
            const std::string path2 =
                c.call(td::Json{{"@type", "getFile"}, {"file_id", file_id}})["local"].value(
                    "path", std::string{});
            row("pread U after re-download",
                ok && verify_segments(crypto, docs[static_cast<size_t>(u)], path2, 0, kSegments),
                -1, 0, "new path equals old: " + std::string{path2 == path ? "yes" : "no"});
        }
    }
}

// ---- Прогон -----------------------------------------------------------------------------

struct SeriesSpec {
    std::string id;
    int item = 0;
    int rep = 0;
    std::string variant;
    int variant_no = 0;
};

class Runner {
  public:
    Runner(BenchEnv& env, const BenchOptions& options, const E10Options& e10, const Names& names)
        : env_(env),
          options_(options),
          e10_(e10),
          names_(names),
          setup_(load_state(options, "e10")),
          state_(options, names.run_state),
          guard_(names.out, "e10",
                 {"time_utc", "series", "phase", "retry_after_s", "wait_s", "size_mib", "group",
                  "what"}),
          out_(names.out) {
        if (!setup_.value("uploaded", false)) {
            throw std::runtime_error("run `e10 setup` first (the documents are not uploaded)");
        }
        session_.chat_id = setup_.at("chat_id").get<int64_t>();
        session_.guard = &guard_;
        session_.files_dir = names.session / "files";
    }

    // 0: всё готово или остановка по сигналу, 1: ошибка, 2: остановка, решает человек.
    int run() {
        const auto plan = build_plan();
        log_line("e10 starts: " + std::to_string(plan.size()) + " series, " +
                 std::to_string(e10_.reps) + " repeats");
        int failures = 0;
        for (;;) {
            const int64_t before = progress_;
            try {
                run_session(plan);
                close_session();
                state_.save();
                log_line(failed_series_ == 0
                             ? "e10 done"
                             : "e10 done with failed series: " + std::to_string(failed_series_));
                return failed_series_ == 0 ? 0 : 2;
            } catch (const Interrupted&) {
                log_line("interrupted by signal; state is saved, rerun `e10 run` to resume");
                close_session();
                state_.save();
                return 0;
            } catch (const StopExperiment& e) {
                log_line(std::string{"STOPPED: "} + e.what());
                close_session();
                state_.save();
                return 2;
            } catch (const td::TdException& e) {
                if (e.error().code != td::TdError::kClosed &&
                    e.error().code != td::TdError::kStopped &&
                    e.error().code != td::TdError::kTimeout && !e.error().is_rate_limit() &&
                    e.error().code != 401 && e.error().code < 500) {
                    // Нераспознанная ошибка TDLib вне серии: остановка.
                    log_line("STOPPED on a non-retryable error: " + e.error().to_string());
                    close_session();
                    state_.save();
                    return 2;
                }
                close_session();
                failures = progress_ > before ? 1 : failures + 1;
                log_line("session error: " + e.error().to_string() + " (failure " +
                         std::to_string(failures) + " of 30)");
                if (failures >= 30) {
                    state_.save();
                    return 1;
                }
                try {
                    sleep_interruptible(seconds{std::min(600, 60 * failures)});
                } catch (const Interrupted&) {
                    state_.save();
                    return 0;
                }
            } catch (const std::exception& e) {
                close_session();
                failures = progress_ > before ? 1 : failures + 1;
                log_line(std::string{"session error: "} + e.what() + " (failure " +
                         std::to_string(failures) + " of 30)");
                if (failures >= 30) {
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
    [[nodiscard]] bool item_enabled(int item) const {
        std::stringstream ss(e10_.items);
        std::string tok;
        while (std::getline(ss, tok, ',')) {
            if (std::stoi(tok) == item) {
                return true;
            }
        }
        return false;
    }

    std::vector<SeriesSpec> build_plan() const {
        std::vector<SeriesSpec> plan;
        std::vector<std::string> v6;
        {
            std::stringstream ss(e10_.variants6);
            std::string tok;
            while (std::getline(ss, tok, ',')) {
                v6.push_back(tok);
            }
        }
        for (int rep = 1; rep <= e10_.reps; ++rep) {
            const std::string r = "-r" + std::to_string(rep);
            const auto add = [&](int item, const std::string& base, const std::string& variant,
                                 int no) {
                plan.push_back(
                    {"p" + std::to_string(item) + "-" + base + r, item, rep, variant, no});
            };
            if (item_enabled(1)) {
                int no = 0;
                for (const char* v : {"asc", "desc", "equal"}) {
                    add(1, v, v, no++);
                }
            }
            if (item_enabled(2)) {
                int no = 0;
                for (const char* v : {"control", "raise", "raise_sync"}) {
                    add(2, v, v, no++);
                }
            }
            if (item_enabled(3)) {
                for (const char* v : {"from0", "from3m"}) {
                    add(3, v, v, 0);
                }
            }
            if (item_enabled(4)) {
                plan.push_back({"p4" + r, 4, rep, "", 0});
            }
            if (item_enabled(5)) {
                int no = 0;
                for (const char* v : {"sync", "async", "extend"}) {
                    add(5, v, v, no++);
                }
            }
            if (item_enabled(7)) {
                plan.push_back({"p7" + r, 7, rep, "", 0});
            }
            if (item_enabled(6) && !v6.empty()) {
                // Порядок вариантов вращается по кругам: время суток не смещает один вариант.
                for (size_t i = 0; i < v6.size(); ++i) {
                    const std::string& v = v6[(i + static_cast<size_t>(rep - 1)) % v6.size()];
                    add(6, v, v, 0);
                }
            }
        }
        return plan;
    }

    void close_session() { session_.bot.reset(); }

    void run_session(const std::vector<SeriesSpec>& plan) {
        session_.crypto = load_or_create_crypto(names_.key);
        session_.bot = env_.open_at(Account::kBot, names_.session, std::cerr);
        std::string error;
        if (!wait_chat_known(session_.c(), session_.chat_id, seconds{120}, &error)) {
            throw std::runtime_error("bot cannot get the channel: " + error);
        }
        bind_documents(session_, setup_);
        log_line("session open, " + std::to_string(session_.docs.size()) + " documents bound");
        for (const SeriesSpec& spec : plan) {
            if (state_.is_done(spec.id)) {
                continue;
            }
            run_series(spec);
            sleep_interruptible(seconds{e10_.series_pause_seconds});
        }
    }

    void run_series(const SeriesSpec& spec) {
        td::Json& st = state_.series(spec.id);
        if (st.is_null()) {
            st = td::Json{
                {"status", "pending"}, {"item", spec.item}, {"rep", spec.rep}, {"attempts", 0}};
        }
        guard_.set_series(spec.id);
        const std::string start = iso_utc_now();
        const auto t0 = ExpClock::now();
        for (;;) {
            if (stopping()) {
                throw Interrupted();
            }
            const int attempt_no = st.value("attempts", 0) + 1;
            st["attempts"] = attempt_no;
            st["status"] = "running";
            state_.save();
            log_line("series " + spec.id + " (attempt " + std::to_string(attempt_no) + ")");
            std::string note;
            try {
                dispatch(spec);
                st["status"] = "done";
                state_.save();
                ++progress_;
                out_.series.row({spec.id, std::to_string(spec.item), "done", start, iso_utc_now(),
                                 fmt(ms_since(t0) / 1000.0, 1), std::to_string(attempt_no), "",
                                 ""});
                return;
            } catch (const td::TdException& e) {
                const td::TdError& err = e.error();
                if (err.is_rate_limit()) {
                    const int64_t s = err.retry_after ? err.retry_after->count() : 60;
                    out_.series.row({spec.id, std::to_string(spec.item), "flood", start,
                                     iso_utc_now(), fmt(ms_since(t0) / 1000.0, 1),
                                     std::to_string(attempt_no), "",
                                     "FLOOD_WAIT " + std::to_string(s) + " s"});
                    if (s > 3600) {
                        throw StopExperiment("FLOOD_WAIT of " + std::to_string(s) +
                                             " s in series " + spec.id);
                    }
                    log_line("FLOOD_WAIT " + std::to_string(s) + " s in " + spec.id + ": waiting");
                    sleep_interruptible(seconds{s + s / 5 + 5});
                    continue;
                }
                if (err.code == td::TdError::kClosed || err.code == td::TdError::kStopped ||
                    err.code == 401) {
                    throw;
                }
                note = err.to_string();
            } catch (const Interrupted&) {
                throw;
            } catch (const StopExperiment&) {
                throw;
            } catch (const std::exception& e) {
                note = e.what();
            }
            out_.series.row({spec.id, std::to_string(spec.item), "failed", start, iso_utc_now(),
                             fmt(ms_since(t0) / 1000.0, 1), std::to_string(attempt_no), "", note});
            log_line("series " + spec.id + " failed: " + note);
            if (attempt_no >= 3) {
                st["status"] = "failed";
                st["error"] = note;
                state_.save();
                ++failed_series_;
                ++progress_;
                return;
            }
            sleep_interruptible(seconds{30});
        }
    }

    void dispatch(const SeriesSpec& spec) {
        switch (spec.item) {
            case 1:
                run_p1(session_, out_, spec.id, spec.rep, spec.variant, spec.variant_no);
                break;
            case 2:
                run_p2(session_, out_, spec.id, spec.rep, spec.variant, spec.variant_no);
                break;
            case 3:
                run_p3(session_, out_, spec.id, spec.rep, spec.variant);
                break;
            case 4:
                run_p4(session_, out_, spec.id, spec.rep);
                break;
            case 5:
                run_p5(session_, out_, spec.id, spec.rep, spec.variant, spec.variant_no);
                break;
            case 6:
                run_p6(session_, out_, e10_, spec.id, spec.rep, spec.variant);
                break;
            case 7:
                run_p7(spec);
                break;
            default:
                throw std::runtime_error("unknown item");
        }
    }

    void run_p7(const SeriesSpec& spec) {
        run_p7_prepare(session_, out_, options_, spec.id, spec.rep);
        const td::Json state = load_state(options_, "e10-p7-" + std::to_string(spec.rep));
        // Файл Z удаляется с диска за спиной TDLib: после закрытия клиента, до перезапуска.
        const std::string z_path = state["docs"]["Z"].value("path", std::string{});
        close_session();
        std::error_code ec;
        fs::remove(z_path, ec);
        const fs::path exe = fs::read_symlink("/proc/self/exe");
        const std::vector<std::string> args{exe.string(),
                                            "--creds",
                                            options_.creds.string(),
                                            "--state-dir",
                                            options_.state_dir.string(),
                                            "e10",
                                            "--out",
                                            e10_.out.string(),
                                            "--rep",
                                            std::to_string(spec.rep),
                                            "--series",
                                            spec.id,
                                            "restart-check",
                                            "--inherit-lock"};
        std::vector<std::string> full = args;
        if (e10_.probe) {
            full.insert(full.begin() + 6, "--probe");
        }
        const int rc = spawn_wait(full);
        // Сеанс открывается заново.
        session_.bot = env_.open_at(Account::kBot, names_.session, std::cerr);
        std::string error;
        if (!wait_chat_known(session_.c(), session_.chat_id, seconds{120}, &error)) {
            throw std::runtime_error("bot cannot get the channel: " + error);
        }
        bind_documents(session_, setup_);
        if (rc != 0) {
            throw std::runtime_error("restart-check exited with " + std::to_string(rc));
        }
    }

    BenchEnv& env_;
    const BenchOptions& options_;
    const E10Options& e10_;
    const Names& names_;
    td::Json setup_;
    run::RunState state_;
    run::Guard guard_;
    Out out_;
    Session session_;
    int64_t progress_ = 0;
    int failed_series_ = 0;
};

// ---- setup, status, teardown ---------------------------------------------------------------

void cmd_setup(BenchEnv& env, const BenchOptions& options, const E10Options& e10) {
    const Names names = names_for(options, e10);
    const run::SessionLock lock(names.lock, "e10");
    td::Json state;
    if (has_state(options, "e10")) {
        state = load_state(options, "e10");
        if (state.value("uploaded", false)) {
            throw std::runtime_error("e10 already set up; run teardown first");
        }
        log_line("resuming setup");
    }
    fs::create_directories(names.out);
    FloodGuard guard(std::cerr, names.out / "e10_setup_flood.csv", "e10-setup");
    const Crypto crypto = load_or_create_crypto(names.key);
    auto bot = env.open_at(Account::kBot, names.upload_session, std::cerr);
    if (state.is_null()) {
        auto user = env.open(Account::kUser, std::cerr);
        const std::string title = "tgfs-bench-e10-" + date_stamp();
        const int64_t chat_id = create_channel(*user, title, guard, std::cerr);
        bump_counter(options, "e10.user_actions", 1);
        state = td::Json{{"chat_id", chat_id}, {"title", title},    {"created", iso_utc_now()},
                         {"bot_added", false}, {"uploaded", false}, {"docs", td::Json::array()}};
        save_state(options, "e10", state);
        std::string error;
        wait_chat_known(user->client(), chat_id, seconds{60}, &error);
        add_bot_admin(*user, *bot, chat_id, e10.rights, guard, std::cerr);
        bump_counter(options, "e10.user_actions", 2);
        state["bot_added"] = true;
        state["rights"] = e10.rights;
        save_state(options, "e10", state);
    }
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    std::string error;
    const auto known = wait_chat_known(bot->client(), chat_id, seconds{90}, &error);
    log_line("bot knows the channel: " + (known ? fmt(*known) + " ms" : "NO (" + error + ")"));
    if (!known) {
        throw std::runtime_error("the bot does not see the channel");
    }

    // Заливка: 32 чанка по 8 МиБ в формате SPEC 5, альбомами по 10 с паузой (лимит по байтам).
    fs::create_directories(names.docs);
    td::Json docs = state["docs"];
    std::vector<Doc> have;
    for (const auto& j : docs) {
        have.push_back(doc_from_json(j));
    }
    while (static_cast<int>(have.size()) < kDocs) {
        const int first = static_cast<int>(have.size());
        const int count = std::min(kAlbumSize, kDocs - first);
        std::vector<fs::path> files;
        std::vector<Doc> batch;
        for (int i = first; i < first + count; ++i) {
            Doc d;
            d.idx = i;
            d.chunk_id = static_cast<uint64_t>(i + 1);
            d.seed = seed_of(i);
            const fs::path plain = names.docs / (random_hex(16) + ".plain");
            {
                std::ofstream o(plain, std::ios::binary | std::ios::trunc);
                std::string block;
                for (uint64_t off = 0; off < kPlain; off += kMiB) {
                    fill_seeded(d.seed, off, block, kMiB);
                    o.write(block.data(), static_cast<std::streamsize>(block.size()));
                }
            }
            const fs::path doc_path = names.docs / (random_hex(16) + ".bin");
            const int fd = ::open(plain.c_str(), O_RDONLY);
            crypto::encrypt_chunk_file(crypto.key, crypto.uuid, d.chunk_id, fd, kPlain, doc_path);
            ::close(fd);
            fs::remove(plain);
            d.enc_size = static_cast<int64_t>(fs::file_size(doc_path));
            Bytes head(crypto::kChunkHeaderSize);
            std::ifstream in(doc_path, std::ios::binary);
            in.read(reinterpret_cast<char*>(head.data()),
                    static_cast<std::streamsize>(head.size()));
            d.header_hex = to_hex(ByteSpan{head});
            files.push_back(doc_path);
            batch.push_back(d);
        }
        log_line("uploading documents " + std::to_string(first) + ".." +
                 std::to_string(first + count - 1));
        const std::vector<td::Json> msgs = guard.run("send album", [&] {
            return bot->tracker().send_album(album_document_request(chat_id, files), seconds{120},
                                             std::chrono::hours{1});
        });
        if (static_cast<int>(msgs.size()) != count) {
            throw std::runtime_error("the album returned an unexpected number of messages");
        }
        bump_counter(options, "e10.bot_messages_sent", count);
        for (size_t i = 0; i < msgs.size(); ++i) {
            Doc d = batch[i];
            d.message_id = msgs[i].value("id", int64_t{0});
            d.remote_id =
                msgs[i]["content"]["document"]["document"]["remote"].value("id", std::string{});
            if (msgs[i]["content"]["document"]["document"].value("size", int64_t{0}) !=
                d.enc_size) {
                throw std::runtime_error("the delivered document has an unexpected size");
            }
            have.push_back(d);
            state["docs"].push_back(doc_to_json(d));
        }
        save_state(options, "e10", state);
        for (const auto& f : files) {
            std::error_code ec;
            fs::remove(f, ec);
        }
        if (static_cast<int>(have.size()) < kDocs) {
            std::this_thread::sleep_for(seconds{e10.album_pause_seconds});
        }
    }
    state["uploaded"] = true;
    state["uploaded_at"] = iso_utc_now();
    save_state(options, "e10", state);
    bot.reset();
    remove_fresh_bot_dir(names.upload_session);
    remove_fresh_bot_dir(names.docs);
    std::cout << "setup done: channel created, bot is administrator (" << e10.rights << "), "
              << kDocs << " chunks uploaded, the user sent no messages\n";
}

void cmd_status(const BenchOptions& options, const E10Options& e10) {
    const Names names = names_for(options, e10);
    if (!has_state(options, "e10")) {
        std::cout << "no channel (run `e10 setup`)\n";
        return;
    }
    const td::Json state = load_state(options, "e10");
    std::cout << "channel: " << state.value("title", std::string{"?"})
              << ", uploaded=" << state.value("uploaded", false) << "\n";
    if (!has_state(options, names.run_state)) {
        std::cout << "run has not started\n";
        return;
    }
    const td::Json j = load_state(options, names.run_state);
    std::cout << "run state updated: " << j.value("updated", std::string{"-"}) << "\n";
    int done = 0;
    int failed = 0;
    int total = 0;
    for (const auto& [id, st] : j["series"].items()) {
        if (!st.is_object()) {
            continue;
        }
        ++total;
        const std::string status = st.value("status", std::string{"?"});
        done += status == "done" ? 1 : 0;
        failed += status == "failed" ? 1 : 0;
        if (status != "done") {
            std::cout << "  " << id << ": " << status << " (attempts " << st.value("attempts", 0)
                      << ")\n";
        }
    }
    std::cout << done << " series done, " << failed << " failed, " << total << " started\n";
    std::cout << "FLOOD_WAIT events: " << run::read_csv(names.out / "e10_flood.csv").size()
              << ", net errors: " << run::read_csv(names.out / "e10_net_errors.csv").size() << "\n";
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E10Options& e10) {
    const Names names = names_for(options, e10);
    const run::SessionLock lock(names.lock, "e10");
    const td::Json state = load_state(options, "e10");
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    fs::create_directories(names.out);
    FloodGuard guard(std::cerr, names.out / "e10_setup_flood.csv", "e10-teardown");
    auto user = env.open(Account::kUser, std::cerr);
    std::string error;
    wait_chat_known(user->client(), chat_id, seconds{30}, &error);
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
    bump_counter(options, "e10.user_actions", 1);
    user.reset();
    remove_fresh_bot_dir(names.session);
    remove_fresh_bot_dir(names.upload_session);
    remove_fresh_bot_dir(names.docs);
    std::error_code ec;
    fs::remove(options.state_dir / "e10.json", ec);
    fs::remove(options.state_dir / "e10-run.json", ec);
    fs::remove(options.state_dir / "e10p-run.json", ec);
    for (int rep = 1; rep <= 10; ++rep) {
        fs::remove(options.state_dir / ("e10-p7-" + std::to_string(rep) + ".json"), ec);
    }
    std::cout << "channel deleted, bot session directories removed\n";
}

int cmd_restart_check(BenchEnv& env, const BenchOptions& options, const E10Options& e10) {
    const Names names = names_for(options, e10);
    std::optional<run::SessionLock> lock;
    if (!e10.inherit_lock) {
        lock.emplace(names.lock, "e10");
    }
    fs::create_directories(names.out);
    Out out(names.out);
    const td::Json setup = load_state(options, "e10");
    std::vector<Doc> docs;
    for (const auto& j : setup.at("docs")) {
        docs.push_back(doc_from_json(j));
    }
    const Crypto crypto = load_or_create_crypto(names.key);
    auto bot = env.open_at(Account::kBot, names.session, std::cerr);
    std::string error;
    wait_chat_known(bot->client(), setup.at("chat_id").get<int64_t>(), seconds{60}, &error);
    restart_check(*bot, out, options, setup.at("chat_id").get<int64_t>(),
                  e10.series.empty() ? "p7" : e10.series, e10.rep, crypto, docs);
    return 0;
}

}  // namespace

void add_e10_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd =
        app.add_subcommand("e10", "Experiment E10: reads and readahead, bot mode (SPEC 17.13)");
    cmd->require_subcommand(1);
    auto e10 = std::make_shared<E10Options>();
    e10->out = default_out_dir("11-e10-read-readahead");
    cmd->add_flag("--probe", e10->probe,
                  "Trial run: one repeat, short sequential read, data in probe/");
    cmd->add_option("--out", e10->out, "Directory for CSV data")->capture_default_str();
    cmd->add_option("--rights", e10->rights, "Bot rights set")->capture_default_str();
    auto* reps_opt =
        cmd->add_option("--reps", e10->reps, "Repeats of every series")->capture_default_str();
    cmd->add_option("--items", e10->items, "Items of SPEC 17.13 to run (comma separated)")
        ->capture_default_str();
    auto* seq_opt = cmd->add_option("--seq-mib", e10->seq_mib, "Sequential read size, MiB (item 6)")
                        ->capture_default_str();
    cmd->add_option("--variants6", e10->variants6, "Readahead variants of item 6")
        ->capture_default_str();
    cmd->add_option("--download-concurrency", e10->download_concurrency, "download_concurrency")
        ->capture_default_str();
    cmd->add_option("--series-pause-seconds", e10->series_pause_seconds, "Pause between series")
        ->capture_default_str();
    cmd->add_option("--album-pause-seconds", e10->album_pause_seconds,
                    "Pause between albums of setup")
        ->capture_default_str();
    cmd->add_option("--rep", e10->rep, "(internal) repeat of the restart check")->group("");
    cmd->add_option("--series", e10->series, "(internal) series id of the restart check")
        ->group("");

    const auto apply_probe = [e10, reps_opt, seq_opt] {
        if (!e10->probe) {
            return;
        }
        if (reps_opt->count() == 0) {
            e10->reps = 1;
        }
        if (seq_opt->count() == 0) {
            e10->seq_mib = 64;
        }
    };

    cmd->add_subcommand("setup",
                        "User creates the channel and adds the bot; the bot uploads 32 chunks")
        ->callback([&options, exit_code, e10, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchEnv env(options);
            cmd_setup(env, options, *e10);
            *exit_code = 0;
        });
    cmd->add_subcommand("run", "Bot runs the series (resumable, run it in the background)")
        ->callback([&options, exit_code, e10, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchOptions own = options;
            own.tdlib_log = options.state_dir / "tdlib-e10.log";
            BenchEnv env(own);
            const Names names = names_for(own, *e10);
            fs::create_directories(names.out);
            const run::SessionLock lock(names.lock, "e10");
            Runner runner(env, own, *e10, names);
            *exit_code = runner.run();
        });
    cmd->add_subcommand("status", "State of the series")
        ->callback([&options, exit_code, e10, apply_probe] {
            apply_probe();
            cmd_status(options, *e10);
            *exit_code = 0;
        });
    auto* rc = cmd->add_subcommand("restart-check", "(internal) item 7 checks in a new process");
    rc->add_flag("--inherit-lock", e10->inherit_lock, "The parent run holds the session lock");
    rc->callback([&options, exit_code, e10, apply_probe] {
        apply_probe();
        BenchOptions own = options;
        own.tdlib_log = options.state_dir / "tdlib-e10c.log";
        BenchEnv env(own);
        *exit_code = cmd_restart_check(env, own, *e10);
    });
    cmd->add_subcommand("teardown", "Delete the channel (user account)")
        ->callback([&options, exit_code, e10, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchEnv env(options);
            cmd_teardown(env, options, *e10);
            *exit_code = 0;
        });
}

}  // namespace telega::bench
