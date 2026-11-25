// e7: размер чанка в режиме бота. Канал создаёт пользователь (setup, сообщений от
// пользователя нет), всю нагрузку несёт бот в отдельном каталоге сессии <state-dir>/bot-e7.
//
//   tgfs-bench e7 corpus [--out FILE]  описание набора C3 (размеры файлов) и состав набора E7
//   tgfs-bench e7 [--probe] setup      пользователь создаёт канал и добавляет бота администратором
//   tgfs-bench e7 [--probe] run        серии: возобновляемо, запускать в фоне
//   tgfs-bench e7 [--probe] status     состояние серий и счётчики
//   tgfs-bench e7 [--probe] teardown   удалить канал (пользователь) и каталоги сессии бота
//
// Серия — это размер чанка S (8, 16, 32, 64 МиБ) и номер повтора. Серия выполняет по порядку:
//   upload_big    заливка файла big (1 ГиБ, C3) настоящими зашифрованными чанками;
//   upload_small  заливка первых по порядку C3 файлов 1–10 МиБ на суммарный 1 ГиБ;
//   dl_big, dl_small   скачивание тех же документов с холодным кэшем (`getMessages` -> file_id,
//                 `deleteFile`, `downloadFile` целиком), затем расшифровка и сверка с данными;
//   rmw           100 перезаписей по 4 КиБ в случайных местах файла big: getMessages, скачать чанк
//                 целиком, расшифровать, изменить, зашифровать заново (новый chunk_id), залить;
//   cleanup       удаление всех документов серии (старые версии чанков тоже).
// Заливка идёт как в демоне: два token bucket (send_rate с всплеском, send_bytes_rate), документы
// не больше album_max_doc_size — альбомами по 10 (альбом = 2 действия), остальные по одному,
// upload_concurrency заданий одновременно, FLOOD_WAIT останавливает все отправки на указанное
// время плюс джиттер, повторы как в демоне (при 429 весь альбом, при 400 только
// неудавшиеся документы). Содержимое файлов случайное с фиксированным seed.
//
// Серии идут по кругу (по повтору на каждый размер), чтобы время суток не смещало один размер.
// Серия, в которой хоть одна фаза возобновлялась после обрыва или были сетевые ошибки, не
// считается чистой, и для её размера добавляется ещё один повтор (до --max-extra).
// Всё для отчёта пишется в CSV (без id канала и username); id канала лежит только в
// <state-dir>/e7.json. Ключ шифрования чанков — случайный, в <state-dir>/e7-key (0600).

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <ranges>
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
#include "run_common.h"
#include "send_load.h"

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
using WallClock = std::chrono::system_clock;

constexpr int64_t kServerIdShift = 20;
constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * kKiB;
constexpr uint64_t kBigSeed = 0xC3B16F11ULL;
constexpr uint64_t kSmallSeedBase = 0xC35A11ULL;
constexpr uint64_t kPatchSeed = 0xE7A7C4ULL;
constexpr uint64_t kRmwPickSeed = 0xE7D1CEULL;
constexpr uint64_t kC3SizeSeed = 0xC3C3C3C3ULL;
constexpr uint64_t kPatchSize = 4 * kKiB;
constexpr int64_t kMaxFloodWaitSeconds = 3600;  // больше часа — остановиться
constexpr int kAlbumSize = 10;
constexpr int kC3Files = 500;
constexpr int kDeleteBatch = 100;

int64_t server_id(int64_t message_id) { return message_id >> kServerIdShift; }

uint64_t splitmix64(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

// То же содержимое, что у fill_seeded (байт p определяется только (seed, p)), но по 8 байт.
void fill_words(uint64_t seed, uint64_t offset, std::string& out, size_t count) {
    static_assert(std::endian::native == std::endian::little);
    out.resize(count);
    size_t i = 0;
    while (i < count) {
        const uint64_t pos = offset + i;
        const uint64_t word = splitmix64(seed ^ ((pos / 8) * 0xD6E8FEB86659FD93ULL));
        const size_t skip = pos % 8;
        const size_t n = std::min<size_t>(8 - skip, count - i);
        std::memcpy(out.data() + i, reinterpret_cast<const char*>(&word) + skip, n);
        i += n;
    }
}

std::string date_stamp() {
    const std::string iso = iso_utc_now();
    return iso.substr(0, 4) + iso.substr(5, 2) + iso.substr(8, 2);
}

double to_mib(double bytes) { return bytes / static_cast<double>(kMiB); }

// ---- Параметры, имена ---------------------------------------------------------------------

struct E7Options {
    bool probe = false;
    fs::path out;
    std::string rights = "only:post,edit,delete";
    std::vector<int> sizes_mib{8, 16, 32, 64};
    int reps = 5;
    int max_extra = 3;
    uint64_t big_mib = 1024;
    uint64_t small_mib = 1024;
    int rmw_ops = 100;
    // Параметры отправки: значения по умолчанию демона.
    int send_rate = 12;
    int send_burst = 5;
    int bytes_rate_mib = 224;
    int bytes_cap_mib = 96;
    int upload_concurrency = 2;
    int download_concurrency = 4;
    int album_max_mib = 8;
    int series_pause_seconds = 300;
    int lookup_pause_ms = 10000;  // между пачками getMessages по 100 (read_pause)
    int delete_interval_ms = 6000;
};

struct Names {
    std::string run_state;
    fs::path out;
    fs::path session;
    fs::path lock;
    fs::path docs;
    fs::path key;
};

Names names_for(const BenchOptions& options, const E7Options& e7) {
    Names n;
    n.run_state = e7.probe ? "e7p-run" : "e7-run";
    n.out = e7.probe ? e7.out / "probe" : e7.out;
    n.session = options.state_dir / "bot-e7";
    n.lock = options.state_dir / "e7-run.lock";
    n.docs = options.state_dir / "e7-docs";
    n.key = options.state_dir / "e7-key";
    return n;
}

// ---- Набор C3 -----------------------------------------------------------------------------

// Размер i-го файла 1–10 МиБ набора C3: равномерно по байтам, зависит только от i.
uint64_t c3_size(int index) {
    return kMiB + splitmix64(kC3SizeSeed ^ static_cast<uint64_t>(index)) % (9 * kMiB + 1);
}

struct FileSpec {
    int id = 0;  // 0 — big, k >= 1 — k-й по порядку файл C3 (1..500)
    uint64_t seed = 0;
    uint64_t size = 0;
};

// Состав набора E7: файл big и первые по порядку файлы C3, пока их сумма не превысит small_bytes.
std::vector<FileSpec> build_layout(uint64_t big_bytes, uint64_t small_bytes) {
    std::vector<FileSpec> files;
    files.push_back({0, kBigSeed, big_bytes});
    uint64_t sum = 0;
    for (int i = 0; i < kC3Files; ++i) {
        const uint64_t size = c3_size(i);
        if (sum + size > small_bytes) {
            break;
        }
        sum += size;
        files.push_back(
            {i + 1, kSmallSeedBase ^ (static_cast<uint64_t>(i + 1) * 0x9E3779B97F4A7C15ULL), size});
    }
    return files;
}

// ---- Таблица чанков серии -----------------------------------------------------------------

struct Chunk {
    int file = 0;      // индекс в layout
    uint64_t idx = 0;  // позиция в файле
    uint64_t plain = 0;
    uint64_t chunk_id = 0;
    int64_t sid = 0;  // серверный id сообщения, 0 — не залит
};

uint64_t enc_size_of(uint64_t plain) { return crypto::chunk_encrypted_size(plain); }

td::Json chunks_to_json(const std::vector<Chunk>& chunks) {
    td::Json arr = td::Json::array();
    for (const Chunk& c : chunks) {
        arr.push_back(td::Json::array({c.file, c.idx, c.plain, c.chunk_id, c.sid}));
    }
    return arr;
}

std::vector<Chunk> chunks_from_json(const td::Json& arr) {
    std::vector<Chunk> chunks;
    for (const auto& e : arr) {
        chunks.push_back(Chunk{e[0].get<int>(), e[1].get<uint64_t>(), e[2].get<uint64_t>(),
                               e[3].get<uint64_t>(), e[4].get<int64_t>()});
    }
    return chunks;
}

// Правка 4 КиБ в чанке: (смещение в чанке, номер правки). Содержимое правки по номеру.
using Patches = std::vector<std::pair<uint64_t, uint64_t>>;

std::string patch_content(uint64_t number) {
    std::string out;
    fill_words(kPatchSeed ^ (number * 0x9E3779B97F4A7C15ULL), 0, out, kPatchSize);
    return out;
}

// Ожидаемый открытый текст чанка с учётом правок.
std::string expected_plain(const FileSpec& file, const Chunk& chunk, uint64_t chunk_size,
                           const Patches& patches) {
    std::string out;
    fill_words(file.seed, chunk.idx * chunk_size, out, chunk.plain);
    for (const auto& [off, number] : patches) {
        const std::string p = patch_content(number);
        std::memcpy(out.data() + off, p.data(), p.size());
    }
    return out;
}

// ---- Ключи и шифрование -------------------------------------------------------------------

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
            throw std::runtime_error("e7-key file is damaged");
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

// Зашифровать открытый текст в файл `<dir>/<16 hex>.bin` (формат чанка). Открытый текст
// ложится во временный файл: encrypt_chunk_file читает чанк из дескриптора.
fs::path encrypt_plain_to_document(const fs::path& dir, const Crypto& c, uint64_t chunk_id,
                                   const std::function<void(std::ofstream&)>& write_plain,
                                   uint64_t plain_size) {
    fs::create_directories(dir);
    const fs::path plain_path = dir / (random_hex(16) + ".plain");
    {
        std::ofstream out(plain_path, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("cannot write the plaintext staging file");
        }
        write_plain(out);
        out.flush();
        if (!out) {
            throw std::runtime_error("write failed in the plaintext staging file");
        }
    }
    const int fd = ::open(plain_path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("cannot reopen the plaintext staging file");
    }
    const fs::path doc = dir / (random_hex(16) + ".bin");
    try {
        crypto::encrypt_chunk_file(c.key, c.uuid, chunk_id, fd, plain_size, doc);
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
    std::error_code ec;
    fs::remove(plain_path, ec);
    if (fs::file_size(doc) != enc_size_of(plain_size)) {
        throw std::runtime_error("encrypted chunk has an unexpected size");
    }
    return doc;
}

fs::path make_chunk_document(const fs::path& dir, const Crypto& c, const FileSpec& file,
                             const Chunk& chunk, uint64_t chunk_size) {
    return encrypt_plain_to_document(
        dir, c, chunk.chunk_id,
        [&](std::ofstream& out) {
            std::string block;
            for (uint64_t off = 0; off < chunk.plain; off += kMiB) {
                const auto n = static_cast<size_t>(std::min<uint64_t>(kMiB, chunk.plain - off));
                fill_words(file.seed, chunk.idx * chunk_size + off, block, n);
                out.write(block.data(), static_cast<std::streamsize>(n));
            }
        },
        chunk.plain);
}

// Читает скачанный зашифрованный файл чанка целиком и расшифровывает его.
Bytes decrypt_downloaded(const Crypto& c, const Chunk& chunk, const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open the downloaded file");
    }
    Bytes raw(enc_size_of(chunk.plain));
    in.read(reinterpret_cast<char*>(raw.data()), static_cast<std::streamsize>(raw.size()));
    if (in.gcount() != static_cast<std::streamsize>(raw.size())) {
        throw std::runtime_error("downloaded file is shorter than the encrypted chunk");
    }
    const crypto::ChunkHeader header =
        crypto::parse_chunk_header(ByteSpan{raw}.subspan(0, crypto::kChunkHeaderSize));
    if (header.plain_size != chunk.plain) {
        throw std::runtime_error("plain_size in the header does not match the table");
    }
    const crypto::SegmentSpan span = crypto::chunk_segments_for(chunk.plain, 0, chunk.plain);
    return crypto::decrypt_chunk_segments(c.key, c.uuid, chunk.chunk_id, header, 0,
                                          ByteSpan{raw}.subspan(span.enc_offset, span.enc_len));
}

bool same_bytes(const Bytes& got, const std::string& want) {
    return got.size() == want.size() && std::memcmp(got.data(), want.data(), want.size()) == 0;
}

// ---- Два token bucket ---------------------------------------------------------

class Limiter {
  public:
    Limiter(double actions_per_min, double action_cap, double bytes_per_min, double bytes_cap)
        : action_rate_(actions_per_min / 60.0),
          action_cap_(action_cap),
          byte_rate_(bytes_per_min / 60.0),
          byte_cap_(bytes_cap),
          last_(ExpClock::now()) {}

    // Через сколько секунд хватит и действий, и байт (0 — сразу).
    double ready_in(double actions, double bytes) {
        refill();
        const double cap_b = std::max(byte_cap_, bytes);
        const double need_a = std::max(0.0, actions - actions_) / action_rate_;
        const double need_b = std::max(0.0, std::min(bytes, cap_b) - bytes_) / byte_rate_;
        return std::max(need_a, need_b);
    }

    void take(double actions, double bytes) {
        refill();
        actions_ -= actions;
        bytes_ -= bytes;
    }

  private:
    void refill() {
        const auto now = ExpClock::now();
        const double dt = std::chrono::duration<double>(now - last_).count();
        last_ = now;
        actions_ = std::min(std::max(action_cap_, 2.0), actions_ + dt * action_rate_);
        bytes_ = std::min(byte_cap_, bytes_ + dt * byte_rate_);
    }

    double action_rate_;
    double action_cap_;
    double byte_rate_;
    double byte_cap_;
    double actions_ = 0;  // каждая фаза начинается с пустых бакетов: без всплеска в замере
    double bytes_ = 0;
    ExpClock::time_point last_;
};

// ---- Заливка ------------------------------------------------------------------------------

struct UItem {
    size_t key = 0;  // индекс в таблице чанков (или номер вызывающего)
    int file = 0;
    uint64_t chunk_idx = 0;
    uint64_t plain = 0;
    uint64_t enc = 0;
    std::function<fs::path()> make;  // создаёт файл документа; вызывается в основном потоке
    fs::path file_path;
    int attempts = 0;
};

struct UJob {
    std::vector<UItem> items;
    bool prepared = false;
    std::optional<ExpClock::time_point> ready_at;
    double wait_ms = 0;
};

struct DocDone {
    int64_t message_id = 0;
    int64_t album_id = 0;
    double doc_ms = 0;
    int group = 1;
    uint64_t job = 0;
    double wait_ms = 0;
};

struct UploadStats {
    int docs_ok = 0;
    int jobs = 0;
    int albums = 0;
    int flood_events = 0;
    double flood_wait_s = 0;
    int net_events = 0;
    int timeouts = 0;
    int retries = 0;
    double wait_s = 0;  // ожидание бакетов и пауз после FLOOD_WAIT у голов очереди
    double plain_bytes_ok = 0;
    double enc_bytes_sent = 0;  // с учётом повторов
    std::optional<ExpClock::time_point> first_done;
    std::optional<ExpClock::time_point> last_done;
    double first_job_plain = 0;
};

struct UploadSinks {
    Csv* docs = nullptr;
    Csv* jobs = nullptr;
    // FLOOD_WAIT: что, retry_after, пауза, документов в задании.
    std::function<void(const std::string&, int64_t, double, int)> on_flood;
    std::function<void(const std::string&, const std::string&, double)> on_net;
};

struct UploadContext {
    std::string series;
    std::string phase;
    int size_mib = 0;
    int rep = 0;
};

class Uploader {
  public:
    struct Config {
        int concurrency = 2;
        double rate_per_min = 12;
        double burst = 5;
        double bytes_per_min = 224.0 * static_cast<double>(kMiB);
        double bytes_cap = 96.0 * static_cast<double>(kMiB);
        uint64_t album_max = 8 * kMiB;
    };

    Uploader(BenchAccount& bot, int64_t chat_id, Config cfg, UploadSinks sinks,
             UploadContext context)
        : cfg_(cfg),
          sinks_(std::move(sinks)),
          ctx_(std::move(context)),
          limiter_(cfg.rate_per_min, cfg.burst, cfg.bytes_per_min, cfg.bytes_cap),
          rng_(std::random_device{}()) {
        SendLoad::Options lo;
        lo.parallelism = cfg.concurrency;
        load_ = std::make_unique<SendLoad>(bot, chat_id, lo);
    }

    [[nodiscard]] UploadStats& stats() { return stats_; }

    // Заливает документы по порядку. on_ok вызывается в основном потоке для каждого доставленного
    // документа. Исключения: Interrupted (сигнал: идущие задания доделаны и учтены),
    // StopExperiment, SessionError.
    void run(std::vector<UItem> items,
             const std::function<void(const UItem&, const DocDone&)>& on_ok) {
        on_ok_ = &on_ok;
        todo_ = group(std::move(items));
        retry_.clear();
        try {
            loop();
        } catch (const Interrupted&) {
            drain(true);
            on_ok_ = nullptr;
            throw;
        } catch (...) {
            load_->abort();
            drain(false);
            on_ok_ = nullptr;
            throw;
        }
        on_ok_ = nullptr;
    }

    // Файлы невыполненных заданий (после обрыва) удаляются вызывающим вместе с каталогом.
  private:
    [[nodiscard]] bool eligible(const UItem& item) const { return item.plain <= cfg_.album_max; }

    [[nodiscard]] std::deque<UJob> group(std::vector<UItem> items) const {
        std::deque<UJob> jobs;
        UJob cur;
        const auto flush = [&] {
            if (!cur.items.empty()) {
                jobs.push_back(std::move(cur));
                cur = UJob{};
            }
        };
        for (UItem& item : items) {
            if (eligible(item)) {
                cur.items.push_back(std::move(item));
                if (cur.items.size() == kAlbumSize) {
                    flush();
                }
            } else {
                flush();
                UJob single;
                single.items.push_back(std::move(item));
                jobs.push_back(std::move(single));
            }
        }
        flush();
        return jobs;
    }

    void loop() {
        while (!todo_.empty() || !retry_.empty() || !inflight_.empty()) {
            collect(true);
            if (stopping()) {
                throw Interrupted();
            }
            const auto now = ExpClock::now();
            std::deque<UJob>& queue = retry_.empty() ? todo_ : retry_;
            if (queue.empty()) {
                load_->wait_event(milliseconds{200});
                continue;
            }
            if (!load_->has_free_slot()) {
                load_->wait_event(milliseconds{100});
                continue;
            }
            UJob& head = queue.front();
            if (!head.prepared) {
                prepare(head);
            }
            if (!head.ready_at) {
                head.ready_at = ExpClock::now();
            }
            if (now < gate_until_) {
                load_->wait_event(std::min<milliseconds>(
                    std::chrono::duration_cast<milliseconds>(gate_until_ - now),
                    milliseconds{200}));
                continue;
            }
            const double actions = head.items.size() > 1 ? 2.0 : 1.0;
            double bytes = 0;
            for (const UItem& it : head.items) {
                bytes += static_cast<double>(it.enc);
            }
            const double wait_s = limiter_.ready_in(actions, bytes);
            if (wait_s > 0) {
                load_->wait_event(std::max<milliseconds>(
                    std::min<milliseconds>(milliseconds{static_cast<int64_t>(wait_s * 1000) + 1},
                                           milliseconds{200}),
                    milliseconds{1}));
                continue;
            }
            limiter_.take(actions, bytes);
            head.wait_ms = ms_since(head.ready_at.value_or(ExpClock::now()));
            stats_.wait_s += head.wait_ms / 1000.0;
            launch(std::move(head));
            queue.pop_front();
        }
    }

    static void prepare(UJob& job) {
        for (UItem& item : job.items) {
            if (item.file_path.empty()) {
                item.file_path = item.make();
            }
        }
        job.prepared = true;
    }

    void launch(UJob&& job) {
        SendJob sj;
        sj.id = ++job_seq_;
        sj.tag = ctx_.phase;
        sj.remove_files = false;
        for (UItem& item : job.items) {
            sj.files.push_back(item.file_path);
            sj.nominal_size = item.enc;
            ++item.attempts;
            stats_.enc_bytes_sent += static_cast<double>(item.enc);
        }
        ++stats_.jobs;
        stats_.albums += job.items.size() > 1 ? 1 : 0;
        const uint64_t id = sj.id;
        inflight_.emplace(id, std::move(job));
        load_->launch(std::move(sj));
    }

    void collect(bool accept_retries) {
        for (SendResult& r : load_->take_finished()) {
            process(std::move(r), accept_retries);
        }
    }

    // Ждёт идущие задания. После сигнала даёт им 2 минуты, потом прерывает.
    void drain(bool graceful) {
        const auto abort_at = ExpClock::now() + seconds{graceful ? 120 : 5};
        while (!inflight_.empty()) {
            if (ExpClock::now() > abort_at) {
                load_->abort();
            }
            load_->wait_event(milliseconds{200});
            try {
                collect(false);
            } catch (const std::exception& e) {
                log_line(std::string{"error while draining: "} + e.what());
            }
            if (ExpClock::now() > abort_at + seconds{60}) {
                break;
            }
        }
    }

    void bump_gate(double seconds_to_wait) {
        const auto until = ExpClock::now() + std::chrono::duration_cast<ExpClock::duration>(
                                                 std::chrono::duration<double>(seconds_to_wait));
        if (until > gate_until_) {
            const auto from = std::max(gate_until_, ExpClock::now());
            stats_.flood_wait_s += std::chrono::duration<double>(until - from).count();
            gate_until_ = until;
        }
    }

    void process(SendResult&& r, bool accept_retries) {
        const auto it = inflight_.find(r.job.id);
        if (it == inflight_.end()) {
            return;
        }
        UJob job = std::move(it->second);
        inflight_.erase(it);
        const size_t n = job.items.size();
        const int group_size = static_cast<int>(n);
        std::vector<UItem> failed;
        double job_plain_ok = 0;
        for (size_t i = 0; i < n; ++i) {
            UItem& item = job.items[i];
            const DocOutcome& d = r.docs[i];
            sinks_.docs->row({ctx_.series, ctx_.phase, std::to_string(r.job.id), std::to_string(i),
                              std::to_string(item.file), std::to_string(item.chunk_idx),
                              std::to_string(item.plain), std::to_string(item.enc),
                              std::to_string(group_size), std::to_string(item.attempts),
                              fmt(job.wait_ms, 0), iso_utc_ms(r.start_wall), fmt(d.done_ms, 1),
                              send_kind_name(d.kind),
                              d.retry_after_s != 0 ? std::to_string(d.retry_after_s) : "", d.error,
                              d.message_id != 0 ? std::to_string(server_id(d.message_id)) : ""});
            switch (d.kind) {
                case SendKind::kOk: {
                    ++stats_.docs_ok;
                    stats_.plain_bytes_ok += static_cast<double>(item.plain);
                    stats_.last_done = ExpClock::now();
                    job_plain_ok += static_cast<double>(item.plain);
                    DocDone done;
                    done.message_id = d.message_id;
                    done.album_id = d.album_id;
                    done.doc_ms = d.done_ms;
                    done.group = group_size;
                    done.job = r.job.id;
                    done.wait_ms = job.wait_ms;
                    std::error_code ec;
                    fs::remove(item.file_path, ec);
                    (*on_ok_)(item, done);
                    consecutive_net_ = 0;
                    break;
                }
                case SendKind::kFlood: {
                    ++stats_.flood_events;
                    if (d.retry_after_s > kMaxFloodWaitSeconds) {
                        throw StopExperiment("FLOOD_WAIT of " + std::to_string(d.retry_after_s) +
                                             " s on an upload: stopping, see e7_flood.csv");
                    }
                    const double jitter = std::uniform_real_distribution<double>(0.0, 0.1)(rng_);
                    const double wait = static_cast<double>(d.retry_after_s) * (1.0 + jitter);
                    bump_gate(wait);
                    sinks_.on_flood("send " + d.error, d.retry_after_s, wait, group_size);
                    log_line("FLOOD_WAIT " + std::to_string(d.retry_after_s) + " s in " +
                             ctx_.phase + " (job of " + std::to_string(group_size) +
                             "), all sends wait " + fmt(wait, 1) + " s");
                    failed.push_back(std::move(item));
                    break;
                }
                case SendKind::kNet:
                case SendKind::kTimeout: {
                    ++(d.kind == SendKind::kNet ? stats_.net_events : stats_.timeouts);
                    const double backoff = std::min(60.0, std::pow(2.0, consecutive_net_));
                    ++consecutive_net_;
                    bump_gate(backoff);
                    sinks_.on_net(send_kind_name(d.kind), d.error, backoff);
                    log_line(std::string{"upload "} + send_kind_name(d.kind) + ": " + d.error);
                    failed.push_back(std::move(item));
                    break;
                }
                case SendKind::kAborted:
                    if (!stopping() && accept_retries) {
                        throw SessionError("client closed during a send: " + d.error);
                    }
                    break;
                case SendKind::kAccount:
                    throw StopExperiment("account problem on a send: " + d.error);
                case SendKind::kFatal:
                    throw StopExperiment("non-retryable error on a send: " + d.error);
            }
        }
        if (!stats_.first_done && job_plain_ok > 0) {
            // Первое завершённое задание после старта: начало «установившейся» скорости.
            stats_.first_done = stats_.last_done;
            stats_.first_job_plain = job_plain_ok;
        }
        sinks_.jobs->row(
            {ctx_.series, ctx_.phase, std::to_string(r.job.id), std::to_string(group_size),
             std::to_string(r.sizes.empty() ? 0 : r.sizes[0]), iso_utc_ms(r.start_wall),
             iso_utc_ms(r.end_wall), fmt(job.wait_ms, 0), fmt(r.call_ms, 0), r.call_error,
             fmt(r.duration_ms, 0), std::to_string(group_size - static_cast<int>(failed.size())),
             std::to_string(failed.size()), std::to_string(r.in_flight_at_start)});
        if (!failed.empty() && accept_retries) {
            stats_.retries += static_cast<int>(failed.size());
            std::deque<UJob> again = group(std::move(failed));
            for (UJob& j : std::ranges::reverse_view(again)) {
                j.prepared = true;
                retry_.push_front(std::move(j));
            }
        }
    }

    Config cfg_;
    UploadSinks sinks_;
    UploadContext ctx_;
    Limiter limiter_;
    std::mt19937_64 rng_;
    std::unique_ptr<SendLoad> load_;
    UploadStats stats_;
    const std::function<void(const UItem&, const DocDone&)>* on_ok_ = nullptr;
    std::deque<UJob> todo_;
    std::deque<UJob> retry_;
    std::map<uint64_t, UJob> inflight_;
    ExpClock::time_point gate_until_;
    uint64_t job_seq_ = 0;
    int consecutive_net_ = 0;
};

// ---- Служебные вызовы TDLib ---------------------------------------------------------------

struct RemoteFile {
    int32_t file_id = 0;
    int64_t size = 0;
    std::string local_path;
    int64_t downloaded = 0;
    bool completed = false;
    bool found = false;
};

RemoteFile parse_file(const td::Json& file) {
    RemoteFile rf;
    rf.file_id = file.value("id", 0);
    rf.size = file.value("size", int64_t{0});
    if (file.contains("local")) {
        rf.local_path = file["local"].value("path", std::string{});
        rf.downloaded = file["local"].value("downloaded_size", int64_t{0});
        rf.completed = file["local"].value("is_downloading_completed", false);
    }
    rf.found = rf.file_id != 0;
    return rf;
}

RemoteFile parse_message_file(const td::Json& message) {
    if (!message.is_object() || !message.contains("content") ||
        message["content"].value("@type", std::string{}) != "messageDocument") {
        return {};
    }
    return parse_file(message["content"]["document"]["document"]);
}

struct NetCounters {
    double sent = 0;
    double received = 0;
};

NetCounters read_net_counters(td::TdClient& client) {
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

// ---- Ход серий ----------------------------------------------------------------------------

struct SeriesRef {
    int size_mib = 0;
    int rep = 0;
    [[nodiscard]] std::string id() const {
        return "s" + std::to_string(size_mib) + "-r" + std::to_string(rep);
    }
    [[nodiscard]] uint64_t chunk_size() const { return static_cast<uint64_t>(size_mib) * kMiB; }
};

const char* const kPhases[] = {"upload_big", "upload_small", "dl_big",
                               "dl_small",   "rmw",          "cleanup"};

class Runner {
  public:
    Runner(BenchEnv& env, const BenchOptions& options, const E7Options& e7, const Names& names)
        : env_(env),
          options_(options),
          e7_(e7),
          names_(names),
          setup_(load_state(options, "e7")),
          chat_id_(setup_.at("chat_id").get<int64_t>()),
          state_(options, names.run_state),
          layout_(build_layout(e7.big_mib * kMiB, e7.small_mib * kMiB)),
          crypto_(load_or_create_crypto(names.key)),
          guard_(names.out, "e7",
                 {"time_utc", "series", "phase", "retry_after_s", "wait_s", "size_mib", "group",
                  "what"}),
          docs_csv_(names.out / "e7_docs.csv",
                    {"series", "phase", "job", "pos", "file", "chunk", "plain_bytes", "enc_bytes",
                     "group", "attempt", "wait_ms", "start_utc", "update_ms", "result",
                     "retry_after_s", "error", "server_id"}),
          jobs_csv_(names.out / "e7_jobs.csv",
                    {"series", "phase", "job", "group", "first_doc_bytes", "start_utc", "end_utc",
                     "limiter_wait_ms", "call_ms", "call_error", "duration_ms", "docs_ok",
                     "docs_failed", "in_flight_at_start"}),
          downloads_csv_(names.out / "e7_downloads.csv",
                         {"series", "phase", "file", "chunk", "plain_bytes", "enc_bytes",
                          "start_offset_s", "download_ms", "downloaded_bytes", "attempts", "cold",
                          "verify_ms", "match", "error"}),
          rmw_csv_(names.out / "e7_rmw.csv", {"series",
                                              "size_mib",
                                              "rep",
                                              "op",
                                              "chunk",
                                              "offset_in_file",
                                              "start_utc",
                                              "lookup_ms",
                                              "download_ms",
                                              "decrypt_ms",
                                              "modify_encrypt_ms",
                                              "upload_ms",
                                              "limiter_wait_ms",
                                              "total_ms",
                                              "total_no_wait_ms",
                                              "flood_events",
                                              "attempts",
                                              "verified",
                                              "downloaded_bytes",
                                              "uploaded_bytes"}),
          phases_csv_(
              names.out / "e7_phases.csv",
              {"series",       "size_mib",   "rep",          "phase",          "resumed",
               "start_utc",    "end_utc",    "wall_s",       "messages",       "plain_mib",
               "enc_mib_xfer", "mib_s",      "steady_mib_s", "flood_events",   "flood_wait_s",
               "net_errors",   "timeouts",   "retries",      "limiter_wait_s", "albums",
               "lookup_s",     "mismatches", "net_sent_mib", "net_recv_mib"}),
          series_csv_(names.out / "e7_series.csv", {"series",
                                                    "size_mib",
                                                    "rep",
                                                    "clean",
                                                    "messages",
                                                    "messages_per_gib",
                                                    "upload_big_mib_s",
                                                    "upload_small_mib_s",
                                                    "upload_total_mib_s",
                                                    "download_total_mib_s",
                                                    "rmw_median_ms",
                                                    "rmw_min_ms",
                                                    "rmw_max_ms",
                                                    "rmw_no_wait_median_ms",
                                                    "rmw_wall_s",
                                                    "flood_events",
                                                    "net_errors",
                                                    "resumed_phases",
                                                    "mismatches",
                                                    "traffic_up_mib",
                                                    "traffic_down_mib",
                                                    "started_utc",
                                                    "ended_utc",
                                                    "deleted"}),
          deleted_csv_(names.out / "e7_deleted.csv",
                       {"time_utc", "series", "server_id", "result", "error"}) {
        if (!setup_.value("bot_added", false)) {
            throw std::runtime_error("run `e7 setup` first (the bot is not added yet)");
        }
        fs::create_directories(names.docs);
        // Незавершённые файлы прошлого процесса.
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(names.docs, ec)) {
            fs::remove_all(entry.path(), ec);
        }
        check_layout();
        load_pending();
    }

    // 0: всё готово или остановка по сигналу, 1: ошибка, 2: остановка, решает человек.
    int run() {
        log_line("e7 starts: sizes " + sizes_text() + " MiB, " + std::to_string(e7_.reps) +
                 " clean repeats each (up to " + std::to_string(e7_.max_extra) +
                 " extra), set: " + std::to_string(layout_.size()) + " files, " +
                 std::to_string(pending_.size()) + " documents wait for deletion");
        int failures = 0;
        for (;;) {
            const int64_t before = progress_;
            try {
                run_session();
                finish();
                return 0;
            } catch (const Interrupted&) {
                log_line("interrupted by signal; state is saved, rerun `e7 run` to resume");
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
    [[nodiscard]] std::string sizes_text() const {
        std::string s;
        for (const int v : e7_.sizes_mib) {
            s += (s.empty() ? "" : ",") + std::to_string(v);
        }
        return s;
    }

    void check_layout() {
        td::Json& root = state_.root();
        const td::Json want{{"big_mib", e7_.big_mib},
                            {"small_mib", e7_.small_mib},
                            {"files", layout_.size()},
                            {"rmw_ops", e7_.rmw_ops}};
        if (root.contains("layout") && root["layout"] != want) {
            throw std::runtime_error("the run state was made with another set (big/small/rmw)");
        }
        root["layout"] = want;
        state_.save();
    }

    void close_session() {
        bot_.reset();
        known_.clear();
    }

    void finish() {
        bump_counter(options_, "e7.bot_messages_sent", session_sends_);
        session_sends_ = 0;
        close_session();
        log_line("e7 done");
    }

    // Чистых серий этого размера.
    int clean_count(int size_mib) {
        int n = 0;
        for (const auto& [id, st] : state_.root()["series"].items()) {
            if (st.is_object() && st.value("size_mib", 0) == size_mib &&
                st.value("status", std::string{}) == "done" && st.value("clean", false)) {
                ++n;
            }
        }
        return n;
    }

    void run_session() {
        bot_ = env_.open_at(Account::kBot, names_.session, std::cerr);
        std::string error;
        if (!wait_chat_known(bot_->client(), chat_id_, seconds{120}, &error)) {
            throw std::runtime_error("bot cannot get the channel: " + error);
        }
        known_.clear();
        for (int round = 1; round <= e7_.reps + e7_.max_extra; ++round) {
            for (const int size : e7_.sizes_mib) {
                if (clean_count(size) >= e7_.reps) {
                    continue;
                }
                const SeriesRef ref{size, round};
                if (!state_.is_done(ref.id())) {
                    run_series(ref);
                }
            }
        }
    }

    // ---- серия ----

    void run_series(const SeriesRef& ref) {
        const std::string id = ref.id();
        td::Json& st = state_.series(id);
        guard_.set_series(id);
        if (st.is_null()) {
            st = td::Json{{"status", "pending"},
                          {"size_mib", ref.size_mib},
                          {"rep", ref.rep},
                          {"phases", td::Json::object()},
                          {"garbage", td::Json::array()},
                          {"patches", td::Json::object()},
                          {"next_op", 0},
                          {"rmw_ms", td::Json::array()},
                          {"rmw_no_wait_ms", td::Json::array()}};
        }
        run::wait_until_epoch(state_.root().value("quiet_until", int64_t{0}),
                              "pause before series " + id);
        if (!st.contains("started")) {
            st["started"] = iso_utc_now();
            std::vector<Chunk> chunks;
            for (size_t f = 0; f < layout_.size(); ++f) {
                const uint64_t n = (layout_[f].size + ref.chunk_size() - 1) / ref.chunk_size();
                for (uint64_t i = 0; i < n; ++i) {
                    const uint64_t plain =
                        std::min(ref.chunk_size(), layout_[f].size - i * ref.chunk_size());
                    chunks.push_back(Chunk{static_cast<int>(f), i, plain, next_chunk_id(), 0});
                }
            }
            st["chunks"] = chunks_to_json(chunks);
        }
        st["status"] = "running";
        state_.save();
        chunks_ = chunks_from_json(st["chunks"]);
        log_line("series " + id + " starts (chunk " + std::to_string(ref.size_mib) + " MiB, " +
                 std::to_string(chunks_.size()) + " chunks)");

        for (const char* phase : kPhases) {
            if (st["phases"].contains(phase) &&
                st["phases"][phase].value("state", std::string{}) == "done") {
                continue;
            }
            run_phase(ref, st, phase);
        }
        finalize_series(ref, st);
    }

    uint64_t next_chunk_id() {
        const auto n = state_.root().value("next_chunk_id", int64_t{1});
        state_.root()["next_chunk_id"] = n + 1;
        return static_cast<uint64_t>(n);
    }

    void save_series(td::Json& st) {
        st["chunks"] = chunks_to_json(chunks_);
        state_.save();
    }

    // Возвращает true, если фаза начата повторно (после обрыва её цифры ненадёжны).
    bool phase_begin(td::Json& st, const std::string& phase) {
        td::Json& p = st["phases"][phase];
        const bool resumed = p.is_object() && p.value("state", std::string{}) == "running";
        if (!p.is_object()) {
            p = td::Json{{"state", "running"}, {"resumed", false}, {"started", iso_utc_now()}};
        }
        p["state"] = "running";
        if (resumed) {
            p["resumed"] = true;
        }
        save_series(st);
        return p.value("resumed", false);
    }

    void run_phase(const SeriesRef& ref, td::Json& st, const std::string& phase) {
        log_line("[" + ref.id() + "] phase " + phase);
        const bool resumed = phase_begin(st, phase);
        const NetCounters net0 = read_net_counters(bot_->client());
        const auto t0 = ExpClock::now();
        const auto wall0 = WallClock::now();
        td::Json row;
        if (phase == "upload_big") {
            row = phase_upload(ref, st, phase, [](const Chunk& c) { return c.file == 0; });
        } else if (phase == "upload_small") {
            row = phase_upload(ref, st, phase, [](const Chunk& c) { return c.file != 0; });
        } else if (phase == "dl_big") {
            row = phase_download(ref, st, phase, [](const Chunk& c) { return c.file == 0; });
        } else if (phase == "dl_small") {
            row = phase_download(ref, st, phase, [](const Chunk& c) { return c.file != 0; });
        } else if (phase == "rmw") {
            row = phase_rmw(ref, st);
        } else {
            row = phase_cleanup(ref, st);
        }
        const NetCounters net1 = read_net_counters(bot_->client());
        row["resumed"] = resumed;
        row["start_utc"] = iso_utc_ms(wall0);
        row["end_utc"] = iso_utc_ms(WallClock::now());
        row["wall_s"] = std::chrono::duration<double>(ExpClock::now() - t0).count();
        row["net_sent_mib"] = to_mib(net1.sent - net0.sent);
        row["net_recv_mib"] = to_mib(net1.received - net0.received);
        write_phase_row(ref, phase, row);
        td::Json& p = st["phases"][phase];
        p["state"] = "done";
        p["row"] = row;
        save_series(st);
        ++progress_;
    }

    static std::string jstr(const td::Json& j, const char* key, int digits = 1) {
        if (!j.contains(key)) {
            return "";
        }
        if (j[key].is_boolean()) {
            return j[key].get<bool>() ? "1" : "0";
        }
        if (j[key].is_string()) {
            return j[key].get<std::string>();
        }
        return fmt(j[key].get<double>(), digits);
    }

    void write_phase_row(const SeriesRef& ref, const std::string& phase, const td::Json& row) {
        phases_csv_.row({ref.id(),
                         std::to_string(ref.size_mib),
                         std::to_string(ref.rep),
                         phase,
                         jstr(row, "resumed"),
                         jstr(row, "start_utc"),
                         jstr(row, "end_utc"),
                         jstr(row, "wall_s", 2),
                         jstr(row, "messages", 0),
                         jstr(row, "plain_mib", 1),
                         jstr(row, "enc_mib_xfer", 1),
                         jstr(row, "mib_s", 3),
                         jstr(row, "steady_mib_s", 3),
                         jstr(row, "flood_events", 0),
                         jstr(row, "flood_wait_s", 1),
                         jstr(row, "net_errors", 0),
                         jstr(row, "timeouts", 0),
                         jstr(row, "retries", 0),
                         jstr(row, "limiter_wait_s", 1),
                         jstr(row, "albums", 0),
                         jstr(row, "lookup_s", 1),
                         jstr(row, "mismatches", 0),
                         jstr(row, "net_sent_mib", 1),
                         jstr(row, "net_recv_mib", 1)});
    }

    // ---- заливка набора ----

    Uploader::Config uploader_config() const {
        Uploader::Config cfg;
        cfg.concurrency = e7_.upload_concurrency;
        cfg.rate_per_min = e7_.send_rate;
        cfg.burst = e7_.send_burst;
        cfg.bytes_per_min = static_cast<double>(e7_.bytes_rate_mib) * static_cast<double>(kMiB);
        cfg.bytes_cap = static_cast<double>(e7_.bytes_cap_mib) * static_cast<double>(kMiB);
        cfg.album_max = static_cast<uint64_t>(e7_.album_max_mib) * kMiB;
        return cfg;
    }

    UploadSinks upload_sinks(const std::string& series, const std::string& phase, int size_mib) {
        UploadSinks sinks;
        sinks.docs = &docs_csv_;
        sinks.jobs = &jobs_csv_;
        sinks.on_flood = [this, series, phase, size_mib](const std::string& what, int64_t retry,
                                                         double wait, int group) {
            guard_.flood().row({iso_utc_ms(WallClock::now()), series, phase, std::to_string(retry),
                                fmt(wait, 1), std::to_string(size_mib), std::to_string(group),
                                what});
        };
        sinks.on_net = [this, series, phase](const std::string& kind, const std::string& error,
                                             double backoff) {
            guard_.net().row(
                {iso_utc_ms(WallClock::now()), series, phase, kind, error, fmt(backoff, 0)});
        };
        return sinks;
    }

    template <class Pred>
    td::Json phase_upload(const SeriesRef& ref, td::Json& st, const std::string& phase, Pred pred) {
        std::vector<UItem> items;
        for (size_t i = 0; i < chunks_.size(); ++i) {
            const Chunk& c = chunks_[i];
            if (c.sid != 0 || !pred(c)) {
                continue;
            }
            UItem item;
            item.key = i;
            item.file = layout_[static_cast<size_t>(c.file)].id;
            item.chunk_idx = c.idx;
            item.plain = c.plain;
            item.enc = enc_size_of(c.plain);
            const uint64_t chunk_size = ref.chunk_size();
            item.make = [this, i, chunk_size] {
                const Chunk& ch = chunks_[i];
                return make_chunk_document(names_.docs, crypto_,
                                           layout_[static_cast<size_t>(ch.file)], ch, chunk_size);
            };
            items.push_back(std::move(item));
        }
        log_line("[" + ref.id() + "] " + phase + ": " + std::to_string(items.size()) +
                 " documents to upload");
        const auto t0 = ExpClock::now();
        Uploader up(*bot_, chat_id_, uploader_config(), upload_sinks(ref.id(), phase, ref.size_mib),
                    UploadContext{ref.id(), phase, ref.size_mib, ref.rep});
        const std::function<void(const UItem&, const DocDone&)> on_ok =
            [this, &st](const UItem& item, const DocDone& done) {
                chunks_[item.key].sid = server_id(done.message_id);
                pending_.insert(chunks_[item.key].sid);
                known_.insert(chunks_[item.key].sid);
                ++session_sends_;
                save_series(st);
            };
        std::exception_ptr error;
        try {
            up.run(std::move(items), on_ok);
        } catch (...) {
            error = std::current_exception();
        }
        const double wall_s = std::chrono::duration<double>(ExpClock::now() - t0).count();
        const UploadStats& s = up.stats();
        if (error) {
            save_series(st);
            std::rethrow_exception(error);
        }
        td::Json row;
        row["messages"] = s.docs_ok;
        row["plain_mib"] = to_mib(s.plain_bytes_ok);
        row["enc_mib_xfer"] = to_mib(s.enc_bytes_sent);
        row["mib_s"] = wall_s > 0 ? to_mib(s.plain_bytes_ok) / wall_s : 0.0;
        double steady = 0;
        if (s.first_done && s.last_done && *s.last_done > *s.first_done) {
            steady = to_mib(s.plain_bytes_ok - s.first_job_plain) /
                     std::chrono::duration<double>(*s.last_done - *s.first_done).count();
        }
        row["steady_mib_s"] = steady;
        row["flood_events"] = s.flood_events;
        row["flood_wait_s"] = s.flood_wait_s;
        row["net_errors"] = s.net_events;
        row["timeouts"] = s.timeouts;
        row["retries"] = s.retries;
        row["limiter_wait_s"] = s.wait_s;
        row["albums"] = s.albums;
        log_line("[" + ref.id() + "] " + phase + " done: " + std::to_string(s.docs_ok) +
                 " documents, " + fmt(to_mib(s.plain_bytes_ok), 0) + " MiB in " + fmt(wall_s, 0) +
                 " s = " + fmt(to_mib(s.plain_bytes_ok) / std::max(wall_s, 1e-9), 2) +
                 " MiB/s, FLOOD_WAIT " + std::to_string(s.flood_events));
        return row;
    }

    // ---- скачивание ----

    // getMessages пачками по 100 с паузой read_pause; обновляет file_id чанков.
    std::map<int64_t, RemoteFile> lookup(const std::vector<size_t>& indexes) {
        std::map<int64_t, RemoteFile> out;
        for (size_t i = 0; i < indexes.size(); i += kDeleteBatch) {
            if (i > 0) {
                sleep_interruptible(milliseconds{e7_.lookup_pause_ms});
            }
            const size_t end = std::min(indexes.size(), i + kDeleteBatch);
            td::Json ids = td::Json::array();
            for (size_t k = i; k < end; ++k) {
                ids.push_back(chunks_[indexes[k]].sid << kServerIdShift);
            }
            const td::Json reply = guard_.call("getMessages", [&] {
                return bot_->client().call(
                    td::Json{{"@type", "getMessages"}, {"chat_id", chat_id_}, {"message_ids", ids}},
                    seconds{60});
            });
            for (size_t k = i; k < end; ++k) {
                const td::Json& m = reply["messages"][k - i];
                const RemoteFile rf = parse_message_file(m);
                const Chunk& c = chunks_[indexes[k]];
                if (!rf.found || rf.size != static_cast<int64_t>(enc_size_of(c.plain))) {
                    throw StopExperiment(
                        "a document of the set is missing or has a wrong size "
                        "(message " +
                        std::to_string(c.sid) + ")");
                }
                known_.insert(c.sid);
                out[c.sid] = rf;
            }
        }
        return out;
    }

    struct DlResult {
        size_t chunk = 0;
        double start_s = 0;
        double download_ms = 0;
        int64_t downloaded = 0;
        int attempts = 0;
        bool cold = true;
        std::string path;
        std::string error;
    };

    // Скачивание одного документа целиком; повторяет при FLOOD_WAIT и сетевых ошибках.
    // После любой ошибки получает file_id заново через getMessages.
    DlResult download_one(size_t chunk_index, RemoteFile rf, ExpClock::time_point t0) {
        DlResult res;
        res.chunk = chunk_index;
        const int64_t sid = chunks_[chunk_index].sid;
        int transient = 0;
        for (int attempt_no = 1;; ++attempt_no) {
            if (stopping()) {
                throw Interrupted();
            }
            res.attempts = attempt_no;
            res.start_s = std::chrono::duration<double>(ExpClock::now() - t0).count();
            const auto t1 = ExpClock::now();
            try {
                const td::Json f = bot_->client().call(td::Json{{"@type", "downloadFile"},
                                                                {"file_id", rf.file_id},
                                                                {"priority", 1},
                                                                {"offset", 0},
                                                                {"limit", 0},
                                                                {"synchronous", true}},
                                                       seconds{900});
                const RemoteFile done = parse_file(f);
                res.download_ms = ms_since(t1);
                if (!done.completed || done.local_path.empty()) {
                    throw td::TdException(
                        td::TdError::make(td::TdError::kUnexpected, "download not completed"));
                }
                res.downloaded = done.downloaded;
                res.path = done.local_path;
                return res;
            } catch (const td::TdException& e) {
                const td::TdError& err = e.error();
                if (err.code == 401 || err.message.find("AUTH_KEY") != std::string::npos) {
                    throw StopExperiment("account problem on a download: " + err.to_string());
                }
                if (err.code == td::TdError::kClosed || err.code == td::TdError::kStopped) {
                    throw SessionError("client closed during a download");
                }
                res.error = err.to_string();
                if (err.is_rate_limit()) {
                    const int64_t s = err.retry_after ? err.retry_after->count() : 60;
                    if (s > kMaxFloodWaitSeconds) {
                        throw StopExperiment("FLOOD_WAIT of " + std::to_string(s) +
                                             " s on a download");
                    }
                    log_line("FLOOD_WAIT " + std::to_string(s) + " s on a download");
                    sleep_interruptible(seconds{s + s / 5 + 5});
                } else if (attempt_no >= 8 && !err.is_local() && err.code < 500) {
                    throw StopExperiment("a download keeps failing: " + err.to_string());
                } else {
                    const int64_t backoff = std::min<int64_t>(60, 1LL << std::min(transient, 6));
                    ++transient;
                    log_line("download error: " + err.to_string() + "; retry in " +
                             std::to_string(backoff) + " s");
                    sleep_interruptible(seconds{backoff});
                }
                // Заново взять file_id по сообщению.
                try {
                    const td::Json reply = bot_->client().call(
                        td::Json{{"@type", "getMessages"},
                                 {"chat_id", chat_id_},
                                 {"message_ids", td::Json::array({sid << kServerIdShift})}},
                        seconds{60});
                    const RemoteFile again = parse_message_file(reply["messages"][0]);
                    if (again.found) {
                        rf = again;
                    }
                } catch (const td::TdException& lookup_error) {
                    // Следующая попытка скачивания повторит запрос.
                    log_line("getMessages after a download error failed: " +
                             lookup_error.error().to_string());
                }
            }
        }
    }

    template <class Pred>
    td::Json phase_download(const SeriesRef& ref, td::Json& st, const std::string& phase,
                            Pred pred) {
        std::vector<size_t> indexes;
        for (size_t i = 0; i < chunks_.size(); ++i) {
            if (pred(chunks_[i])) {
                if (chunks_[i].sid == 0) {
                    throw std::runtime_error("a chunk is not uploaded before the download phase");
                }
                indexes.push_back(i);
            }
        }
        const auto tl = ExpClock::now();
        std::map<int64_t, RemoteFile> files = lookup(indexes);
        const double lookup_s = std::chrono::duration<double>(ExpClock::now() - tl).count();
        // Холодный кэш: локальные копии (в том числе путь исходника, с которого документ
        // отправлялся этим сеансом) сбрасываются до замера.
        std::map<int64_t, bool> cold;
        for (auto& [sid, rf] : files) {
            bool is_cold = false;
            attempt([&] {
                bot_->client().call(td::Json{{"@type", "deleteFile"}, {"file_id", rf.file_id}},
                                    seconds{60});
                // Холодно, если локального файла нет на диске (TDLib помнит путь исходника
                // отправки и после deleteFile, поэтому его собственные поля не показатель).
                const RemoteFile now = parse_file(bot_->client().call(
                    td::Json{{"@type", "getFile"}, {"file_id", rf.file_id}}, seconds{60}));
                std::error_code ec;
                is_cold = now.local_path.empty() || !fs::exists(now.local_path, ec);
            });
            cold[sid] = is_cold;
            rf.downloaded = 0;
            rf.completed = false;
        }

        std::vector<DlResult> results(indexes.size());
        std::atomic<size_t> next{0};
        std::atomic<bool> failed{false};
        std::mutex error_mu;
        std::exception_ptr error;
        const auto t0 = ExpClock::now();
        std::vector<std::thread> pool;
        pool.reserve(static_cast<size_t>(std::max(1, e7_.download_concurrency)));
        const int workers = std::max(1, e7_.download_concurrency);
        for (int w = 0; w < workers; ++w) {
            pool.emplace_back([&] {
                for (;;) {
                    const size_t k = next.fetch_add(1);
                    if (k >= indexes.size() || failed.load()) {
                        return;
                    }
                    try {
                        results[k] =
                            download_one(indexes[k], files.at(chunks_[indexes[k]].sid), t0);
                        results[k].cold = cold.at(chunks_[indexes[k]].sid);
                    } catch (...) {
                        const std::lock_guard<std::mutex> lock(error_mu);
                        if (!error) {
                            error = std::current_exception();
                        }
                        failed.store(true);
                        return;
                    }
                }
            });
        }
        for (auto& t : pool) {
            t.join();
        }
        const double wall_s = std::chrono::duration<double>(ExpClock::now() - t0).count();
        if (error) {
            for (const DlResult& r : results) {
                if (!r.path.empty()) {
                    attempt([&] {
                        bot_->client().call(
                            td::Json{{"@type", "deleteFile"},
                                     {"file_id", files.at(chunks_[r.chunk].sid).file_id}},
                            seconds{60});
                    });
                }
            }
            std::rethrow_exception(error);
        }

        // Расшифровка и сверка с данными по seed (не входит в замер скачивания).
        int mismatches = 0;
        double plain_mib = 0;
        double downloaded_mib = 0;
        int retries = 0;
        for (const DlResult& r : results) {
            const Chunk& c = chunks_[r.chunk];
            const auto tv = ExpClock::now();
            bool match = false;
            std::string err = r.error;
            try {
                const Bytes plain = decrypt_downloaded(crypto_, c, r.path);
                match =
                    same_bytes(plain, expected_plain(layout_[static_cast<size_t>(c.file)], c,
                                                     ref.chunk_size(), patches_of(st, r.chunk)));
            } catch (const std::exception& e) {
                err = std::string{"verify: "} + e.what();
            }
            mismatches += match ? 0 : 1;
            plain_mib += to_mib(static_cast<double>(c.plain));
            downloaded_mib += to_mib(static_cast<double>(r.downloaded));
            retries += r.attempts - 1;
            downloads_csv_.row(
                {ref.id(), phase, std::to_string(layout_[static_cast<size_t>(c.file)].id),
                 std::to_string(c.idx), std::to_string(c.plain),
                 std::to_string(enc_size_of(c.plain)), fmt(r.start_s, 2), fmt(r.download_ms, 0),
                 std::to_string(r.downloaded), std::to_string(r.attempts), r.cold ? "1" : "0",
                 fmt(ms_since(tv), 0), match ? "1" : "0", err});
            attempt([&] {
                bot_->client().call(
                    td::Json{{"@type", "deleteFile"}, {"file_id", files.at(c.sid).file_id}},
                    seconds{60});
            });
        }
        td::Json row;
        row["messages"] = indexes.size();
        row["plain_mib"] = plain_mib;
        row["enc_mib_xfer"] = downloaded_mib;  // для скачивания — принято
        row["mib_s"] = wall_s > 0 ? plain_mib / wall_s : 0.0;
        row["steady_mib_s"] = row["mib_s"];
        row["retries"] = retries;
        row["lookup_s"] = lookup_s;
        row["mismatches"] = mismatches;
        log_line("[" + ref.id() + "] " + phase + " done: " + std::to_string(indexes.size()) +
                 " documents, " + fmt(plain_mib, 0) + " MiB in " + fmt(wall_s, 0) +
                 " s = " + fmt(plain_mib / std::max(wall_s, 1e-9), 2) + " MiB/s, mismatches " +
                 std::to_string(mismatches));
        return row;
    }

    // Правки чанка из состояния серии.
    static Patches patches_of(const td::Json& st, size_t chunk_index) {
        Patches out;
        const std::string key = std::to_string(chunk_index);
        if (st.contains("patches") && st["patches"].contains(key)) {
            for (const auto& p : st["patches"][key]) {
                out.emplace_back(p[0].get<uint64_t>(), p[1].get<uint64_t>());
            }
        }
        return out;
    }

    // ---- read-modify-write ----

    td::Json phase_rmw(const SeriesRef& ref, td::Json& st) {
        const uint64_t file_size = layout_[0].size;
        const uint64_t cs = ref.chunk_size();
        Uploader up(*bot_, chat_id_, uploader_config(), upload_sinks(ref.id(), "rmw", ref.size_mib),
                    UploadContext{ref.id(), "rmw", ref.size_mib, ref.rep});
        const auto t_phase = ExpClock::now();
        const int first_op = st.value("next_op", 0);
        int verified_bad = 0;
        double uploaded = 0;
        double downloaded = 0;
        for (int op = first_op; op < e7_.rmw_ops; ++op) {
            if (stopping()) {
                throw Interrupted();
            }
            const uint64_t pick = splitmix64(kRmwPickSeed ^ (static_cast<uint64_t>(op) + 1) ^
                                             (static_cast<uint64_t>(ref.size_mib) << 32) ^
                                             (static_cast<uint64_t>(ref.rep) << 48));
            const uint64_t off = pick % (file_size - kPatchSize + 1);
            const auto ci = static_cast<size_t>(off / cs);
            const uint64_t in_off = std::min(off - ci * cs, chunks_[ci].plain - kPatchSize);
            const uint64_t file_off = ci * cs + in_off;
            Chunk& old = chunks_[ci];
            const int64_t old_sid = old.sid;
            const uint64_t old_id = old.chunk_id;
            const uint64_t patch_no =
                static_cast<uint64_t>(ref.size_mib) * 1000 + static_cast<uint64_t>(op) + 1;

            const auto wall = WallClock::now();
            const auto t0 = ExpClock::now();
            // 1. file_id по сообщению (путь пустой БД).
            const td::Json reply = guard_.call("getMessages", [&] {
                return bot_->client().call(
                    td::Json{{"@type", "getMessages"},
                             {"chat_id", chat_id_},
                             {"message_ids", td::Json::array({old_sid << kServerIdShift})}},
                    seconds{60});
            });
            RemoteFile rf = parse_message_file(reply["messages"][0]);
            if (!rf.found || rf.size != static_cast<int64_t>(enc_size_of(old.plain))) {
                throw StopExperiment("the chunk document is missing for a rewrite");
            }
            known_.insert(old_sid);
            const double lookup_ms = ms_since(t0);
            // 2. Скачать чанк целиком (с холодным кэшем).
            const auto t1 = ExpClock::now();
            attempt([&] {
                bot_->client().call(td::Json{{"@type", "deleteFile"}, {"file_id", rf.file_id}},
                                    seconds{60});
            });
            rf.downloaded = 0;
            rf.completed = false;
            const DlResult dl = download_one(ci, rf, t0);
            const double download_ms = ms_since(t1);
            // 3. Расшифровать.
            const auto t2 = ExpClock::now();
            Bytes plain = decrypt_downloaded(crypto_, old, dl.path);
            const double decrypt_ms = ms_since(t2);
            const std::string patch = patch_content(patch_no);
            // 4. Изменить и зашифровать заново (новый chunk_id, новый nonce).
            const auto t3 = ExpClock::now();
            const uint64_t new_id = next_chunk_id();
            std::memcpy(plain.data() + in_off, patch.data(), patch.size());
            const fs::path doc = encrypt_plain_to_document(
                names_.docs, crypto_, new_id,
                [&](std::ofstream& out) {
                    out.write(reinterpret_cast<const char*>(plain.data()),
                              static_cast<std::streamsize>(plain.size()));
                },
                old.plain);
            const double modify_ms = ms_since(t3);
            // 5. Залить; время до updateMessageSendSucceeded.
            const auto t4 = ExpClock::now();
            const UploadStats before = up.stats();
            UItem item;
            item.key = ci;
            item.file = 0;
            item.chunk_idx = old.idx;
            item.plain = old.plain;
            item.enc = enc_size_of(old.plain);
            item.file_path = doc;
            DocDone result;
            const std::function<void(const UItem&, const DocDone&)> on_ok =
                [&result](const UItem& /*i*/, const DocDone& d) { result = d; };
            up.run({std::move(item)}, on_ok);
            const double total_ms = ms_since(t0);
            const double upload_ms = ms_since(t4);
            const double wait_ms = result.wait_ms;
            const int floods = up.stats().flood_events - before.flood_events;
            const int attempts = floods + (up.stats().net_events - before.net_events) +
                                 (up.stats().timeouts - before.timeouts) + 1;
            // Состояние: новая версия чанка, старая — мусор для удаления.
            old.sid = server_id(result.message_id);
            old.chunk_id = new_id;
            pending_.insert(old.sid);
            known_.insert(old.sid);
            ++session_sends_;
            st["garbage"].push_back(old_sid);
            td::Json& plist = st["patches"][std::to_string(ci)];
            plist.push_back(td::Json::array({in_off, patch_no}));
            st["next_op"] = op + 1;
            st["rmw_ms"].push_back(total_ms);
            st["rmw_no_wait_ms"].push_back(total_ms - wait_ms);
            save_series(st);
            // 6. Проверка: скачанный чанк до правки совпадал с ожидаемым (вне замера).
            bool verified = false;
            Patches before_patches = patches_of(st, ci);
            before_patches.pop_back();
            verified = same_bytes(
                decrypt_downloaded(crypto_, Chunk{old.file, old.idx, old.plain, old_id, old_sid},
                                   dl.path),
                expected_plain(layout_[0], old, cs, before_patches));
            verified_bad += verified ? 0 : 1;
            uploaded += static_cast<double>(item_enc(old.plain) * static_cast<uint64_t>(attempts));
            downloaded += static_cast<double>(dl.downloaded);
            attempt([&] {
                bot_->client().call(td::Json{{"@type", "deleteFile"}, {"file_id", rf.file_id}},
                                    seconds{60});
            });
            rmw_csv_.row({ref.id(),
                          std::to_string(ref.size_mib),
                          std::to_string(ref.rep),
                          std::to_string(op),
                          std::to_string(ci),
                          std::to_string(file_off),
                          iso_utc_ms(wall),
                          fmt(lookup_ms, 0),
                          fmt(download_ms, 0),
                          fmt(decrypt_ms, 0),
                          fmt(modify_ms, 0),
                          fmt(upload_ms, 0),
                          fmt(wait_ms, 0),
                          fmt(total_ms, 0),
                          fmt(total_ms - wait_ms, 0),
                          std::to_string(floods),
                          std::to_string(attempts),
                          verified ? "1" : "0",
                          std::to_string(dl.downloaded),
                          std::to_string(item_enc(old.plain) * static_cast<uint64_t>(attempts))});
            if ((op + 1) % 10 == 0) {
                log_line("[" + ref.id() + "] rmw " + std::to_string(op + 1) + "/" +
                         std::to_string(e7_.rmw_ops) + ", last " + fmt(total_ms, 0) + " ms (" +
                         fmt(wait_ms, 0) + " ms waiting for limits)");
            }
        }
        td::Json row;
        row["messages"] = e7_.rmw_ops;
        row["plain_mib"] = to_mib(static_cast<double>(e7_.rmw_ops) * static_cast<double>(cs));
        row["enc_mib_xfer"] = to_mib(uploaded);
        row["mib_s"] = 0.0;
        row["steady_mib_s"] = 0.0;
        row["flood_events"] = up.stats().flood_events;
        row["flood_wait_s"] = up.stats().flood_wait_s;
        row["net_errors"] = up.stats().net_events;
        row["timeouts"] = up.stats().timeouts;
        row["retries"] = up.stats().retries;
        row["limiter_wait_s"] = up.stats().wait_s;
        row["mismatches"] = verified_bad;
        row["download_mib"] = to_mib(downloaded);
        row["rmw_wall_s"] = std::chrono::duration<double>(ExpClock::now() - t_phase).count();
        return row;
    }

    static uint64_t item_enc(uint64_t plain) { return enc_size_of(plain); }

    // ---- удаление ----

    td::Json phase_cleanup(const SeriesRef& ref, td::Json& st) {
        std::set<int64_t> mine;
        for (const Chunk& c : chunks_) {
            if (c.sid != 0) {
                mine.insert(c.sid);
            }
        }
        for (const auto& g : st["garbage"]) {
            mine.insert(g.get<int64_t>());
        }
        const int deleted = delete_ids(ref.id(), mine);
        td::Json row;
        row["messages"] = deleted;
        row["plain_mib"] = 0.0;
        st["deleted"] = deleted;
        return row;
    }

    void load_pending() {
        // Документы серий, которые уже были залиты, но ещё не удалены (после аварии).
        std::set<int64_t> sent;
        for (const auto& row : run::read_csv(names_.out / "e7_docs.csv")) {
            if (row.at("result") == "ok" && !row.at("server_id").empty()) {
                sent.insert(std::stoll(row.at("server_id")));
            }
        }
        for (const auto& row : run::read_csv(names_.out / "e7_deleted.csv")) {
            if (row.at("result") == "ok") {
                sent.erase(std::stoll(row.at("server_id")));
            }
        }
        pending_ = std::move(sent);
    }

    // Удаляет документы пачками по 100, не чаще 10 пачек в минуту. Возвращает число удалённых.
    int delete_ids(const std::string& series, const std::set<int64_t>& wanted) {
        guard_.set_series(series);
        const std::vector<int64_t> ids(wanted.begin(), wanted.end());
        int deleted = 0;
        std::optional<ExpClock::time_point> last_action;
        const auto pace = [&] {
            if (last_action) {
                const auto next = *last_action + milliseconds{e7_.delete_interval_ms};
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
                known = known && known_.contains(sid);
            }
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
                guard_.call("deleteMessages", [&] {
                    return bot_->client().call(td::Json{{"@type", "deleteMessages"},
                                                        {"chat_id", chat_id_},
                                                        {"message_ids", message_ids},
                                                        {"revoke", true}},
                                               seconds{60});
                });
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
                 " documents of " + series);
        return deleted;
    }

    // ---- итог серии ----

    void finalize_series(const SeriesRef& ref, td::Json& st) {
        const td::Json& ph = st["phases"];
        const auto num = [&](const char* phase, const char* key) {
            return ph.contains(phase) && ph[phase]["row"].contains(key)
                       ? ph[phase]["row"][key].get<double>()
                       : 0.0;
        };
        int resumed = 0;
        double floods = 0;
        double net = 0;
        double mism = 0;
        double up_mib = 0;
        double down_mib = 0;
        double messages = 0;
        for (const char* phase : kPhases) {
            if (!ph.contains(phase)) {
                continue;
            }
            resumed += ph[phase]["row"].value("resumed", false) ? 1 : 0;
            floods += num(phase, "flood_events");
            net += num(phase, "net_errors") + num(phase, "timeouts");
            mism += num(phase, "mismatches");
            up_mib += num(phase, "net_sent_mib");
            down_mib += num(phase, "net_recv_mib");
        }
        messages = num("upload_big", "messages") + num("upload_small", "messages");
        const double set_mib = num("upload_big", "plain_mib") + num("upload_small", "plain_mib");
        const double up_wall = num("upload_big", "wall_s") + num("upload_small", "wall_s");
        const double dl_mib = num("dl_big", "plain_mib") + num("dl_small", "plain_mib");
        const double dl_wall = num("dl_big", "wall_s") + num("dl_small", "wall_s");
        const Summary rmw = summarize(st["rmw_ms"].get<std::vector<double>>());
        const Summary rmw_nw = summarize(st["rmw_no_wait_ms"].get<std::vector<double>>());
        const bool clean = resumed == 0 && net == 0 && mism == 0;
        st["clean"] = clean;
        st["status"] = "done";
        st["ended"] = iso_utc_now();
        state_.root()["quiet_until"] = epoch_now() + e7_.series_pause_seconds;
        series_csv_.row({ref.id(),
                         std::to_string(ref.size_mib),
                         std::to_string(ref.rep),
                         clean ? "1" : "0",
                         fmt(messages, 0),
                         fmt(set_mib > 0 ? messages / (set_mib / 1024.0) : 0, 1),
                         fmt(num("upload_big", "mib_s"), 3),
                         fmt(num("upload_small", "mib_s"), 3),
                         fmt(up_wall > 0 ? set_mib / up_wall : 0, 3),
                         fmt(dl_wall > 0 ? dl_mib / dl_wall : 0, 3),
                         fmt(rmw.median, 0),
                         fmt(rmw.min, 0),
                         fmt(rmw.max, 0),
                         fmt(rmw_nw.median, 0),
                         fmt(num("rmw", "rmw_wall_s"), 0),
                         fmt(floods, 0),
                         fmt(net, 0),
                         std::to_string(resumed),
                         fmt(mism, 0),
                         fmt(up_mib, 0),
                         fmt(down_mib, 0),
                         st.value("started", std::string{}),
                         st["ended"].get<std::string>(),
                         std::to_string(st.value("deleted", 0))});
        bump_counter(options_, "e7.bot_messages_sent", session_sends_);
        session_sends_ = 0;
        state_.save();
        log_line("series " + ref.id() + " done: " + (clean ? "clean" : "NOT clean") + ", upload " +
                 fmt(up_wall > 0 ? set_mib / up_wall : 0, 2) + " MiB/s, download " +
                 fmt(dl_wall > 0 ? dl_mib / dl_wall : 0, 2) + " MiB/s, rmw median " +
                 fmt(rmw.median, 0) + " ms");
        ++progress_;
    }

    BenchEnv& env_;
    const BenchOptions& options_;
    const E7Options& e7_;
    const Names& names_;
    td::Json setup_;
    int64_t chat_id_;
    run::RunState state_;
    std::vector<FileSpec> layout_;
    Crypto crypto_;
    run::Guard guard_;
    Csv docs_csv_;
    Csv jobs_csv_;
    Csv downloads_csv_;
    Csv rmw_csv_;
    Csv phases_csv_;
    Csv series_csv_;
    Csv deleted_csv_;

    std::unique_ptr<BenchAccount> bot_;
    std::vector<Chunk> chunks_;
    std::set<int64_t> pending_;  // серверные id залитого и ещё не удалённого
    std::set<int64_t> known_;  // id, которые TDLib этого сеанса уже видел
    int64_t session_sends_ = 0;
    int64_t progress_ = 0;
};

// ---- corpus / setup / status / teardown ---------------------------------------------------

void cmd_corpus(const E7Options& e7, const fs::path& csv) {
    const std::vector<FileSpec> files = build_layout(e7.big_mib * kMiB, e7.small_mib * kMiB);
    uint64_t sum_small = 0;
    for (size_t i = 1; i < files.size(); ++i) {
        sum_small += files[i].size;
    }
    uint64_t total500 = 0;
    for (int i = 0; i < kC3Files; ++i) {
        total500 += c3_size(i);
    }
    std::cout << "C3: " << kC3Files << " files 1-10 MiB, "
              << fmt(to_mib(static_cast<double>(total500)), 1) << " MiB in total\n"
              << "E7 set: big file " << e7.big_mib << " MiB + first " << files.size() - 1
              << " files of C3 = " << fmt(to_mib(static_cast<double>(sum_small)), 1)
              << " MiB (limit " << e7.small_mib << " MiB)\n";
    for (const int size_mib : e7.sizes_mib) {
        const auto s = static_cast<uint64_t>(size_mib);
        const uint64_t chunks_big = (e7.big_mib * kMiB + s * kMiB - 1) / (s * kMiB);
        uint64_t chunks_small = 0;
        uint64_t album_docs = 0;
        for (size_t i = 1; i < files.size(); ++i) {
            const uint64_t n = (files[i].size + s * kMiB - 1) / (s * kMiB);
            chunks_small += n;
            for (uint64_t k = 0; k < n; ++k) {
                const uint64_t plain = std::min<uint64_t>(s * kMiB, files[i].size - k * s * kMiB);
                album_docs += plain <= static_cast<uint64_t>(e7.album_max_mib) * kMiB ? 1 : 0;
            }
        }
        std::cout << "  chunk " << s << " MiB: " << chunks_big << " chunks for big, "
                  << chunks_small << " for the small files (" << album_docs
                  << " of them go by albums)\n";
    }
    if (!csv.empty()) {
        std::ofstream out(csv);
        out << "index,size_bytes\n";
        for (int i = 0; i < kC3Files; ++i) {
            out << i + 1 << "," << c3_size(i) << "\n";
        }
        std::cout << "sizes written to " << csv.filename().string() << "\n";
    }
}

void cmd_setup(BenchEnv& env, const BenchOptions& options, const E7Options& e7) {
    const Names names = names_for(options, e7);
    const run::SessionLock lock(names.lock, "e7");
    td::Json state;
    if (has_state(options, "e7")) {
        state = load_state(options, "e7");
        if (state.value("bot_added", false)) {
            throw std::runtime_error("channel e7 already set up; run teardown first");
        }
        log_line("resuming setup: the channel exists");
    }
    fs::create_directories(names.out);
    FloodGuard guard(std::cerr, names.out / "e7_setup_flood.csv", "e7-setup");
    auto user = env.open(Account::kUser, std::cerr);
    auto bot = env.open_at(Account::kBot, names.session, std::cerr);
    if (state.is_null()) {
        const std::string title = "tgfs-bench-e7-" + date_stamp();
        const int64_t chat_id = create_channel(*user, title, guard, std::cerr);
        bump_counter(options, "e7.user_actions", 1);
        state = td::Json{{"chat_id", chat_id},
                         {"title", title},
                         {"created", iso_utc_now()},
                         {"bot_added", false}};
        save_state(options, "e7", state);
    }
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    std::string error;
    wait_chat_known(user->client(), chat_id, seconds{60}, &error);
    add_bot_admin(*user, *bot, chat_id, e7.rights, guard, std::cerr);
    bump_counter(options, "e7.user_actions", 2);
    const auto known = wait_chat_known(bot->client(), chat_id, seconds{90}, &error);
    log_line("bot knows the channel: " + (known ? fmt(*known) + " ms" : "NO (" + error + ")"));
    state["bot_added"] = true;
    state["rights"] = e7.rights;
    state["bot_added_at"] = iso_utc_now();
    save_state(options, "e7", state);
    std::cout << "setup done: channel created, bot is administrator (" << e7.rights
              << "), the user sent no messages\n";
}

void cmd_status(const BenchOptions& options, const E7Options& e7) {
    const Names names = names_for(options, e7);
    if (!has_state(options, "e7")) {
        std::cout << "no channel (run `e7 setup`)\n";
        return;
    }
    const td::Json state = load_state(options, "e7");
    std::cout << "channel: " << state.value("title", std::string{"?"})
              << ", bot_added=" << state.value("bot_added", false) << "\n";
    if (!has_state(options, names.run_state)) {
        std::cout << "run has not started\n";
        return;
    }
    const td::Json j = load_state(options, names.run_state);
    std::cout << "run state updated: " << j.value("updated", std::string{"-"}) << ", quiet for "
              << std::max<int64_t>(0, j.value("quiet_until", 0) - epoch_now()) << " s more\n";
    int done = 0;
    int clean = 0;
    for (const auto& [id, st] : j["series"].items()) {
        if (!st.is_object()) {
            continue;
        }
        const std::string status = st.value("status", std::string{"?"});
        done += status == "done" ? 1 : 0;
        clean += status == "done" && st.value("clean", false) ? 1 : 0;
        std::cout << "  " << id << ": " << status;
        if (status == "done") {
            std::cout << (st.value("clean", false) ? ", clean" : ", NOT clean");
        } else if (st.contains("phases")) {
            for (const auto& [name, p] : st["phases"].items()) {
                std::cout << " [" << name << ":" << p.value("state", std::string{"?"}) << "]";
            }
            std::cout << ", rmw ops " << st.value("next_op", 0);
        }
        std::cout << "\n";
    }
    std::cout << done << " series done, " << clean << " clean\n";
    const auto count = [&](const char* file, const char* col, const char* want) {
        int64_t n = 0;
        for (const auto& row : run::read_csv(names.out / file)) {
            n += row.at(col) == want ? 1 : 0;
        }
        return n;
    };
    std::cout << "docs ok: " << count("e7_docs.csv", "result", "ok")
              << ", FLOOD_WAIT events: " << run::read_csv(names.out / "e7_flood.csv").size()
              << ", net errors: " << run::read_csv(names.out / "e7_net_errors.csv").size()
              << ", deleted: " << count("e7_deleted.csv", "result", "ok") << "\n";
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E7Options& e7) {
    const Names names = names_for(options, e7);
    const run::SessionLock lock(names.lock, "e7");
    const td::Json state = load_state(options, "e7");
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    fs::create_directories(names.out);
    FloodGuard guard(std::cerr, names.out / "e7_setup_flood.csv", "e7-teardown");
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
    bump_counter(options, "e7.user_actions", 1);
    user.reset();
    remove_fresh_bot_dir(names.session);
    remove_fresh_bot_dir(names.docs);
    fs::remove(options.state_dir / "e7.json");
    fs::remove(options.state_dir / "e7-run.json");
    fs::remove(options.state_dir / "e7p-run.json");
    std::cout << "channel deleted, bot session directory removed\n";
}

}  // namespace

void add_e7_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("e7", "Experiment E7: chunk size in bot mode");
    cmd->require_subcommand(1);
    auto e7 = std::make_shared<E7Options>();
    e7->out = default_out_dir("07-e7-chunk-size");
    cmd->add_flag("--probe", e7->probe, "Trial run: small set, one repeat, data in probe/");
    cmd->add_option("--out", e7->out, "Directory for CSV data")->capture_default_str();
    cmd->add_option("--rights", e7->rights, "Bot rights set")->capture_default_str();
    auto* sizes_opt = cmd->add_option("--sizes", e7->sizes_mib, "Chunk sizes, MiB")
                          ->capture_default_str()
                          ->delimiter(',');
    auto* reps_opt =
        cmd->add_option("--reps", e7->reps, "Clean repeats per chunk size")->capture_default_str();
    cmd->add_option("--max-extra", e7->max_extra, "Extra repeats allowed for disturbed series")
        ->capture_default_str();
    auto* big_opt =
        cmd->add_option("--big-mib", e7->big_mib, "Size of the big file (rewrites run on it), MiB")
            ->capture_default_str();
    auto* small_opt = cmd->add_option("--small-mib", e7->small_mib,
                                      "Total of the first C3 files 1-10 MiB in the set, MiB")
                          ->capture_default_str();
    auto* rmw_opt = cmd->add_option("--rmw-ops", e7->rmw_ops, "Rewrites of 4 KiB per series")
                        ->capture_default_str();
    cmd->add_option("--send-rate", e7->send_rate, "Actions per minute")->capture_default_str();
    cmd->add_option("--send-burst", e7->send_burst, "Burst of the action bucket")
        ->capture_default_str();
    cmd->add_option("--send-bytes-rate-mib", e7->bytes_rate_mib, "MiB per minute")
        ->capture_default_str();
    cmd->add_option("--upload-concurrency", e7->upload_concurrency, "Parallel uploads")
        ->capture_default_str();
    cmd->add_option("--download-concurrency", e7->download_concurrency, "Parallel downloads")
        ->capture_default_str();
    cmd->add_option("--album-max-mib", e7->album_max_mib, "album_max_doc_size, MiB")
        ->capture_default_str();
    auto* pause_opt = cmd->add_option("--series-pause-seconds", e7->series_pause_seconds,
                                      "Pause between series, after deletion")
                          ->capture_default_str();
    auto* lookup_opt = cmd->add_option("--lookup-pause-ms", e7->lookup_pause_ms,
                                       "Pause between getMessages batches of 100")
                           ->capture_default_str();
    cmd->add_option("--delete-interval-ms", e7->delete_interval_ms,
                    "Minimal interval between delete batches")
        ->capture_default_str();
    auto corpus_out = std::make_shared<fs::path>();

    // Значения пробного прогона применяются, если опция не задана явно.
    const auto apply_probe = [e7, sizes_opt, reps_opt, big_opt, small_opt, rmw_opt, pause_opt,
                              lookup_opt] {
        if (!e7->probe) {
            return;
        }
        if (sizes_opt->count() == 0) {
            e7->sizes_mib = {8, 64};
        }
        if (reps_opt->count() == 0) {
            e7->reps = 1;
        }
        if (big_opt->count() == 0) {
            e7->big_mib = 256;
        }
        if (small_opt->count() == 0) {
            e7->small_mib = 64;
        }
        if (rmw_opt->count() == 0) {
            e7->rmw_ops = 6;
        }
        if (pause_opt->count() == 0) {
            e7->series_pause_seconds = 30;
        }
        if (lookup_opt->count() == 0) {
            e7->lookup_pause_ms = 10000;
        }
    };

    auto* corpus =
        cmd->add_subcommand("corpus", "Describe the C3 set and the E7 subset (no network)");
    corpus->add_option("--csv", *corpus_out, "Write the C3 sizes to FILE")->type_name("FILE");
    corpus->callback([e7, apply_probe, corpus_out, exit_code] {
        apply_probe();
        cmd_corpus(*e7, *corpus_out);
        *exit_code = 0;
    });
    cmd->add_subcommand("setup", "User creates the channel and adds the bot")
        ->callback([&options, exit_code, e7, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchEnv env(options);
            cmd_setup(env, options, *e7);
            *exit_code = 0;
        });
    cmd->add_subcommand("run", "Bot runs the series (resumable, run it in the background)")
        ->callback([&options, exit_code, e7, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchOptions own = options;
            own.tdlib_log = options.state_dir / "tdlib-e7.log";
            BenchEnv env(own);
            const Names names = names_for(own, *e7);
            fs::create_directories(names.out);
            const run::SessionLock lock(names.lock, "e7");
            Runner runner(env, own, *e7, names);
            *exit_code = runner.run();
        });
    cmd->add_subcommand("status", "State of the series and counters")
        ->callback([&options, exit_code, e7, apply_probe] {
            apply_probe();
            cmd_status(options, *e7);
            *exit_code = 0;
        });
    cmd->add_subcommand("teardown", "Delete the channel (user account)")
        ->callback([&options, exit_code, e7, apply_probe] {
            apply_probe();
            run::install_signal_handlers();
            BenchEnv env(options);
            cmd_teardown(env, options, *e7);
            *exit_code = 0;
        });
}

}  // namespace telega::bench
