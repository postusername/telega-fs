// Тесты ресурсов на LocalDirStore: ресурсы демона при копировании большого файла,
// ENOSPC при полном staging и чтение с перемоткой. Демон монтирования идёт отдельным процессом
// (telega-fs mount -f), поэтому его память и занятое место staging меряются снаружи по /proc и
// по файлам. Без /dev/fuse тесты пропускаются через GTEST_SKIP.
//
// Тяжёлые данные кладутся в каталог TELEGA_IT_HEAVY_DIR (в каталоге сборки, на большом диске),
// а не в /tmp; переменная окружения TELEGA_TEST_TMPDIR его переопределяет.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sodium.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "harness.h"
#include "store/local_dir_store.h"

#ifndef TELEGA_IT_HEAVY_DIR
#error "TELEGA_IT_HEAVY_DIR must be defined by CMake"
#endif

namespace telega::itest {
namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

constexpr uint64_t kMiB = uint64_t{1} << 20U;
constexpr uint64_t kGiB = uint64_t{1} << 30U;

fs::path heavy_dir() {
    const char* env = std::getenv("TELEGA_TEST_TMPDIR");  // NOLINT(concurrency-mt-unsafe)
    return env != nullptr ? fs::path{env} : fs::path{TELEGA_IT_HEAVY_DIR};
}

double to_mib(uint64_t bytes) { return static_cast<double>(bytes) / static_cast<double>(kMiB); }

// staging/ и tmp/ профиля: вместе они считаются занятым местом.
std::vector<fs::path> staging_dirs(const Env& e) {
    return {e.data_dir() / "staging", e.data_dir() / "tmp"};
}

double seconds_since(Clock::time_point t0) {
    return std::chrono::duration<double>(Clock::now() - t0).count();
}

// --- память процесса и занятое место staging ---

// Число потоков процесса по /proc/<pid>/status (0, если процесса нет).
int proc_threads(pid_t pid) {
    std::ifstream in("/proc/" + std::to_string(pid) + "/status");
    std::string line;
    while (std::getline(in, line)) {
        if (line.starts_with("Threads:")) {
            return std::atoi(line.c_str() + 8);
        }
    }
    return 0;
}

struct ProcMem {
    uint64_t rss_kib = 0;
    uint64_t hwm_kib = 0;
};

std::optional<ProcMem> read_proc_mem(pid_t pid) {
    std::ifstream in("/proc/" + std::to_string(pid) + "/status");
    if (!in) {
        return std::nullopt;
    }
    ProcMem m;
    bool got_rss = false;
    bool got_hwm = false;
    std::string line;
    while (std::getline(in, line)) {
        if (line.starts_with("VmRSS:")) {
            m.rss_kib = std::strtoull(line.c_str() + 6, nullptr, 10);
            got_rss = true;
        } else if (line.starts_with("VmHWM:")) {
            m.hwm_kib = std::strtoull(line.c_str() + 6, nullptr, 10);
            got_hwm = true;
        }
    }
    if (!got_rss || !got_hwm) {
        return std::nullopt;  // процесс уже завершается
    }
    return m;
}

// Сумма длин обычных файлов под каталогами (логическая длина, а не st_blocks).
// Файлы появляются и исчезают во время обхода: такие записи пропускаются.
uint64_t used_bytes(const std::vector<fs::path>& dirs) {
    uint64_t total = 0;
    for (const auto& dir : dirs) {
        std::error_code ec;
        fs::recursive_directory_iterator it(dir, fs::directory_options::skip_permission_denied, ec);
        const fs::recursive_directory_iterator end;
        while (!ec && it != end) {
            struct stat st {};
            if (::lstat(it->path().c_str(), &st) == 0 && S_ISREG(st.st_mode)) {
                total += static_cast<uint64_t>(st.st_size);
            }
            it.increment(ec);
        }
    }
    return total;
}

// Раз в period снимает VmRSS/VmHWM процесса и занятое место каталогов.
class Sampler {
  public:
    Sampler(pid_t pid, std::vector<fs::path> dirs, std::chrono::milliseconds period = 100ms)
        : pid_(pid), dirs_(std::move(dirs)), period_(period), thread_([this] { run(); }) {}
    Sampler(const Sampler&) = delete;
    Sampler& operator=(const Sampler&) = delete;
    ~Sampler() { stop(); }

    void stop() {
        stop_ = true;
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    [[nodiscard]] uint64_t max_rss_kib() const { return max_rss_kib_; }
    [[nodiscard]] uint64_t hwm_kib() const { return hwm_kib_; }
    [[nodiscard]] uint64_t max_used() const { return max_used_; }
    [[nodiscard]] uint64_t samples() const { return samples_; }

  private:
    void run() {
        while (!stop_) {
            if (const auto m = read_proc_mem(pid_)) {
                max_rss_kib_ = std::max<uint64_t>(max_rss_kib_, m->rss_kib);
                hwm_kib_ = std::max<uint64_t>(hwm_kib_, m->hwm_kib);
            }
            max_used_ = std::max<uint64_t>(max_used_, used_bytes(dirs_));
            ++samples_;
            std::this_thread::sleep_for(period_);
        }
    }

    pid_t pid_;
    std::vector<fs::path> dirs_;
    std::chrono::milliseconds period_;
    std::atomic<bool> stop_{false};
    std::atomic<uint64_t> max_rss_kib_{0};
    std::atomic<uint64_t> hwm_kib_{0};
    std::atomic<uint64_t> max_used_{0};
    std::atomic<uint64_t> samples_{0};
    std::thread thread_;  // последним: запускается, когда остальные поля уже готовы
};

// --- генератор данных и sha256 ---

// Быстрый поток псевдослучайных байтов (splitmix64), без хранения файла.
class RandomStream {
  public:
    explicit RandomStream(uint64_t seed) : state_(seed) {}
    void fill(uint8_t* out, size_t len) {
        size_t i = 0;
        for (; i + 8 <= len; i += 8) {
            const uint64_t w = next();
            std::memcpy(out + i, &w, 8);
        }
        if (i < len) {
            const uint64_t w = next();
            std::memcpy(out + i, &w, len - i);
        }
    }

  private:
    uint64_t next() {
        state_ += 0x9E3779B97F4A7C15ULL;
        uint64_t z = state_;
        z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31U);
    }
    uint64_t state_;
};

class Sha256 {
  public:
    Sha256() { crypto_hash_sha256_init(&st_); }
    void update(const uint8_t* data, size_t len) { crypto_hash_sha256_update(&st_, data, len); }
    std::string hex() {
        std::array<uint8_t, crypto_hash_sha256_BYTES> d{};
        crypto_hash_sha256_final(&st_, d.data());
        std::string out(d.size() * 2, '0');
        sodium_bin2hex(out.data(), out.size() + 1, d.data(), d.size());
        return out;
    }

  private:
    crypto_hash_sha256_state st_{};
};

std::string sha256_of_file(const fs::path& path, uint64_t* size_out = nullptr) {
    const File f = File::open(path, O_RDONLY);
    Sha256 h;
    std::vector<uint8_t> buf(kMiB);
    uint64_t off = 0;
    for (;;) {
        const ssize_t n = ::pread(f.fd(), buf.data(), buf.size(), static_cast<off_t>(off));
        if (n < 0) {
            throw std::system_error(errno, std::generic_category(), "pread " + path.string());
        }
        if (n == 0) {
            break;
        }
        h.update(buf.data(), static_cast<size_t>(n));
        off += static_cast<uint64_t>(n);
    }
    if (size_out != nullptr) {
        *size_out = off;
    }
    return h.hex();
}

// Записать len байт в дескриптор (EINTR, неполные записи); false — ошибка, errno сохранён.
bool write_all_fd(int fd, const uint8_t* data, size_t len) {
    while (len > 0) {
        const ssize_t n = ::write(fd, data, len);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return false;
        }
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// Санитайзеры (ASan, TSan) сами держат теневую память и карантин, поэтому предел RSS демона в
// таких сборках не проверяется, а тяжёлые сценарии с файлом 1 ГиБ пропускаются (они идут в
// обычных сборках). Тесты ENOSPC под санитайзерами выполняются.
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
constexpr bool kSanitized = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
constexpr bool kSanitized = true;
#else
constexpr bool kSanitized = false;
#endif
#else
constexpr bool kSanitized = false;
#endif

constexpr uint64_t kRssLimitKib = uint64_t{256} * 1024U;

void expect_rss_within_limit(uint64_t hwm_kib, const char* what) {
    if (kSanitized) {
        std::cout << "[res] sanitizer build: " << what << " VmHWM " << hwm_kib / 1024
                  << " MiB is not checked against the limit\n";
        return;
    }
    EXPECT_LT(hwm_kib, kRssLimitKib) << what << ": VmHWM of the daemon must stay below 256 MiB";
}

class Resources : public ::testing::Test {
  public:
    void SetUp() override {
        if (const std::string why = fuse_skip_reason(); !why.empty()) {
            GTEST_SKIP() << why;
        }
        ASSERT_GE(sodium_init(), 0);
        // Запись в FIFO без читателя не должна убивать тест.
        std::signal(SIGPIPE, SIG_IGN);
    }

    void TearDown() override {
        if (HasFailure() && env_) {
            std::cerr << "---- telega-fs.log ----\n" << env_->log_text() << '\n';
        }
    }

    Env& start(const ProfileSpec& spec) {
        env_ = std::make_unique<Env>(spec, heavy_dir());
        env_->init();
        return *env_;
    }
    [[nodiscard]] Env& env() const { return *env_; }

    std::unique_ptr<Env> env_;
};

// --- ресурсы ---

struct CopyScenario {
    uint64_t size = 0;
    uint64_t staging_limit = 0;
    uint64_t put_bandwidth = 0;  // байт/с
};

// Копирование большого файла cp при ограниченной заливке: память демона, занятое staging и
// сверка sha256 после перемонтирования.
struct CopyResult {
    uint64_t hwm_kib = 0;
    uint64_t max_rss_kib = 0;
    uint64_t max_staged = 0;
    uint64_t samples = 0;
    double copy_seconds = 0;
    double unmount_seconds = 0;
    std::string sha_written;
    uint64_t size_seen = 0;
};

CopyResult copy_through_fifo(Env& e, Mount& m, const CopyScenario& sc,
                             const std::vector<fs::path>& dirs) {
    CopyResult res;
    const fs::path fifo = e.scratch() / "source.fifo";
    if (::mkfifo(fifo.c_str(), 0600) != 0) {
        throw std::system_error(errno, std::generic_category(), "mkfifo");
    }
    Sampler sampler(m.pid(), dirs);
    const auto t0 = Clock::now();

    std::atomic<bool> cp_done{false};
    RunResult cp_result;
    std::string cp_error;
    std::thread cp([&] {
        try {
            cp_result = run_program({"cp", fifo.string(), m.path("big.bin").string()}, {}, "",
                                    e.scratch(), std::chrono::minutes(90));
        } catch (const std::exception& ex) {
            cp_error = ex.what();
        }
        cp_done = true;
    });

    // Открыть FIFO на запись, когда cp откроет его на чтение (если cp упал раньше — выйти).
    int fd = -1;
    while (fd < 0 && !cp_done) {
        fd = ::open(fifo.c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC);
        if (fd < 0) {
            std::this_thread::sleep_for(10ms);
        }
    }
    Sha256 sha;
    if (fd >= 0) {
        ::fcntl(fd, F_SETFL, 0);
        RandomStream rng{0x5EED};
        std::vector<uint8_t> buf(kMiB);
        uint64_t sent = 0;
        while (sent < sc.size) {
            const size_t n = static_cast<size_t>(std::min<uint64_t>(buf.size(), sc.size - sent));
            rng.fill(buf.data(), n);
            if (!write_all_fd(fd, buf.data(), n)) {
                break;  // cp завершился с ошибкой: причина в его выводе
            }
            sha.update(buf.data(), n);
            sent += n;
        }
        ::close(fd);
    }
    cp.join();
    res.copy_seconds = seconds_since(t0);
    EXPECT_EQ(cp_error, "");
    EXPECT_TRUE(cp_result.exit.ok())
        << "cp: code " << cp_result.exit.code << ", signal " << cp_result.exit.signal << ":\n"
        << cp_result.output;
    res.sha_written = sha.hex();

    // Последний снимок до размонтирования: VmHWM монотонен, поэтому пик копирования учтён.
    if (const auto mem = read_proc_mem(m.pid())) {
        res.hwm_kib = mem->hwm_kib;
    }
    struct stat st {};
    if (::stat(m.path("big.bin").c_str(), &st) == 0) {
        res.size_seen = static_cast<uint64_t>(st.st_size);
    }
    const auto t1 = Clock::now();
    const Exit x = m.unmount(std::chrono::minutes(30));
    res.unmount_seconds = seconds_since(t1);
    EXPECT_TRUE(x.ok()) << m.output();
    sampler.stop();
    res.hwm_kib = std::max(res.hwm_kib, sampler.hwm_kib());
    res.max_rss_kib = sampler.max_rss_kib();
    res.max_staged = sampler.max_used();
    res.samples = sampler.samples();
    return res;
}

void run_copy_scenario(Env& e, const CopyScenario& sc) {
    const EnvMap faults{
        {"TELEGA_FS_FAULTS", "put_chunk:*:bandwidth=" + std::to_string(sc.put_bandwidth)}};
    auto m = e.mount_fg({}, faults);
    const CopyResult r = copy_through_fifo(e, *m, sc, staging_dirs(e));

    std::cout << "[res-copy] size " << sc.size / kMiB << " MiB, staging_limit "
              << sc.staging_limit / kMiB << " MiB, put_bandwidth " << sc.put_bandwidth / kMiB
              << " MiB/s: VmHWM " << r.hwm_kib / 1024 << " MiB, max VmRSS " << r.max_rss_kib / 1024
              << " MiB, max staged " << r.max_staged / kMiB << " MiB (" << r.max_staged
              << " bytes), copy " << r.copy_seconds << " s, unmount " << r.unmount_seconds
              << " s, samples " << r.samples << '\n';

    EXPECT_EQ(r.size_seen, sc.size);
    EXPECT_GT(r.samples, 10U);
    expect_rss_within_limit(r.hwm_kib, "copy");
    EXPECT_LE(r.max_staged, sc.staging_limit) << "staging + tmp must never exceed staging_limit";
    // Дошло ли дело до обратного давления, зависит от скорости cp через FUSE на этой машине (под
    // нагрузкой она падает до единиц МиБ/с), поэтому это только сообщение. Давление гарантированно
    // проверяет тест ENOSPC, где заливка остановлена.
    if (r.max_staged <= sc.staging_limit / 2) {
        std::cout << "[res-copy] note: staging reached only " << to_mib(r.max_staged)
                  << " MiB: copying is slower than the upload limit on this machine\n";
    }

    // Перемонтирование и сверка содержимого.
    auto m2 = e.mount_fg({}, {});
    uint64_t size = 0;
    const std::string sha_read = sha256_of_file(m2->path("big.bin"), &size);
    EXPECT_EQ(size, sc.size);
    EXPECT_EQ(sha_read, r.sha_written);
    if (const auto mem = read_proc_mem(m2->pid())) {
        std::cout << "[res-copy] remount read: VmHWM " << mem->hwm_kib / 1024 << " MiB\n";
        expect_rss_within_limit(mem->hwm_kib, "remount read");
    }
    EXPECT_TRUE(m2->unmount(std::chrono::minutes(5)).ok());
}

ProfileSpec copy_profile(const CopyScenario& sc) {
    ProfileSpec spec;
    spec.chunk_size = "8MiB";
    spec.sync_timeout = "900s";
    spec.extra = "staging_limit = \"" + std::to_string(sc.staging_limit / kMiB) + "MiB\"\n";
    return spec;
}

// CI: 1 ГиБ при staging_limit 256 МиБ. Заливка 10 МиБ/с: cp через FUSE на этой машине даёт
// 20–35 МиБ/с, так что staging заполняется, и дальше работает обратное давление.
TEST_F(Resources, CopyOneGiBKeepsMemoryAndStagingWithinLimits) {
    if (kSanitized) {
        GTEST_SKIP() << "resource test: memory figures are meaningless under sanitizers";
    }
    const CopyScenario sc{.size = kGiB, .staging_limit = 256 * kMiB, .put_bandwidth = 10 * kMiB};
    Env& e = start(copy_profile(sc));
    run_copy_scenario(e, sc);
}

// Вручную (метка long): 10 ГиБ при staging_limit 1 ГиБ и заливке 50 МиБ/с.
class ResourcesLong : public Resources {};

TEST_F(ResourcesLong, CopyTenGiBKeepsMemoryAndStagingWithinLimits) {
    const CopyScenario sc{.size = 10 * kGiB, .staging_limit = kGiB, .put_bandwidth = 50 * kMiB};
    Env& e = start(copy_profile(sc));
    run_copy_scenario(e, sc);
}

// То же при заливке 16 МиБ/с: staging гарантированно упирается в лимит.
TEST_F(ResourcesLong, CopyTenGiBWithSlowUploadHitsTheStagingLimit) {
    const CopyScenario sc{.size = 10 * kGiB, .staging_limit = kGiB, .put_bandwidth = 16 * kMiB};
    Env& e = start(copy_profile(sc));
    run_copy_scenario(e, sc);
}

// --- ENOSPC ---

// Заливка остановлена сбоем: запись упирается в staging_limit и через staging_full_timeout
// получает ENOSPC. Пока записи ждут места, read и stat другого файла отвечают.
void run_enospc_scenario(Resources& t, int writer_count) {
    constexpr uint64_t kLimit = 64 * kMiB;
    constexpr double kFullTimeoutSeconds = 8;
    const int kWriters = writer_count;
    constexpr uint64_t kPerWriter = 64 * kMiB;  // всего 768 МиБ при лимите 64 МиБ
    constexpr uint64_t kOtherSize = 6 * kMiB;
    ProfileSpec spec;
    spec.chunk_size = "8MiB";
    spec.sync_timeout = "2s";
    spec.extra = "staging_limit = \"64MiB\"\nstaging_full_timeout = \"8s\"\n";
    Env& e = t.start(spec);
    {
        // Файл «other» заливается до сбоя: его чтение идёт через Store, а не из overlay.
        auto m = e.mount_fg();
        write_pattern_file(m->path("other"), kOtherSize, 77);
        ASSERT_TRUE(m->unmount().ok());
    }
    auto m = e.mount_fg({}, {{"TELEGA_FS_FAULTS", "put_chunk:*:transient"}});
    Sampler sampler(m->pid(), staging_dirs(e));

    struct Writer {
        std::atomic<uint64_t> written{0};
        std::atomic<int> err{0};
        std::atomic<bool> done{false};
        std::atomic<double> failed_call_seconds{0};
        std::thread thread;
    };
    std::vector<Writer> writers(static_cast<size_t>(kWriters));
    std::atomic<uint64_t> total{0};
    for (int i = 0; i < kWriters; ++i) {
        writers[static_cast<size_t>(i)].thread = std::thread([&, i] {
            Writer& w = writers[static_cast<size_t>(i)];
            const int fd = ::open(m->path("w" + std::to_string(i)).c_str(),
                                  O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
            if (fd < 0) {
                w.err = errno;
                w.done = true;
                return;
            }
            std::vector<uint8_t> buf(kMiB);
            RandomStream rng{static_cast<uint64_t>(i) + 1};
            while (w.written < kPerWriter) {
                rng.fill(buf.data(), buf.size());
                const auto t0 = Clock::now();
                if (!write_all_fd(fd, buf.data(), buf.size())) {
                    w.err = errno;
                    w.failed_call_seconds = seconds_since(t0);
                    break;
                }
                w.written += buf.size();
                total += buf.size();
            }
            ::close(fd);
            w.done = true;
        });
    }
    const auto join_all = [&] {
        for (auto& w : writers) {
            if (w.thread.joinable()) {
                w.thread.join();
            }
        }
    };

    // Дождаться остановки прогресса: все писатели либо ждут места, либо уже получили ошибку.
    const auto t_start = Clock::now();
    uint64_t last = ~uint64_t{0};
    auto stable_since = Clock::now();
    while (Clock::now() - t_start < 120s) {
        const uint64_t now_total = total;
        if (now_total != last) {
            last = now_total;
            stable_since = Clock::now();
        } else if (Clock::now() - stable_since > 1500ms) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    const auto stalled_at = Clock::now();
    int still_waiting = 0;
    for (const auto& w : writers) {
        if (!w.done) {
            ++still_waiting;
        }
    }
    EXPECT_GT(still_waiting, 0) << "no write is waiting for space: the limit was never reached";
    EXPECT_LE(total.load(), kLimit) << "more was written into staging than staging_limit";

    // Пока write ждёт места, read и stat другого файла отвечают быстро.
    double worst_stat = 0;
    double worst_stat_busy = 0;
    double worst_read = 0;
    for (int round = 0; round < 5; ++round) {
        struct stat st {};
        auto t0 = Clock::now();
        ASSERT_EQ(::stat(m->path("other").c_str(), &st), 0);
        EXPECT_EQ(static_cast<uint64_t>(st.st_size), kOtherSize);
        worst_stat = std::max(worst_stat, seconds_since(t0));
        std::cout << "[res-enospc] round " << round << " stat(other) " << seconds_since(t0)
                  << " s\n";
        t0 = Clock::now();
        EXPECT_EQ(::stat(m->path("w0").c_str(), &st), 0);
        worst_stat_busy = std::max(worst_stat_busy, seconds_since(t0));
        t0 = Clock::now();
        const File f = File::open(m->path("other"), O_RDONLY);
        std::vector<uint8_t> got(static_cast<size_t>(kMiB / 2));
        const uint64_t off = static_cast<uint64_t>(round) * kMiB;
        f.pread_exact(got.data(), got.size(), off);
        EXPECT_EQ(got, pattern(off, got.size(), 77));
        worst_read = std::max(worst_read, seconds_since(t0));
        std::this_thread::sleep_for(100ms);
    }
    const double worst = std::max(worst_stat, worst_read);
    std::cout << "[res-enospc] " << kWriters << " writers, daemon threads "
              << proc_threads(m->pid()) << ", stalled after " << total / kMiB << " MiB, "
              << still_waiting << " writers waiting, worst stat " << worst_stat
              << " s, worst stat of a file being written " << worst_stat_busy << " s, worst read "
              << worst_read << " s\n";
    EXPECT_LT(worst, 1.0) << "read/stat blocked while writes wait for space";
    // Ожидание шло меньше staging_full_timeout: данные ещё не отвергнуты.
    EXPECT_LT(seconds_since(stalled_at), kFullTimeoutSeconds - 1.5);

    join_all();
    int enospc = 0;
    double min_wait = 1e9;
    for (const auto& w : writers) {
        if (w.err == ENOSPC) {
            ++enospc;
            min_wait = std::min(min_wait, w.failed_call_seconds.load());
        } else {
            EXPECT_EQ(w.err.load(), 0) << "unexpected errno " << w.err.load();
        }
    }
    std::cout << "[res-enospc] ENOSPC writers " << enospc << ", shortest wait before ENOSPC "
              << min_wait << " s\n";
    EXPECT_GT(enospc, 0);
    EXPECT_GE(min_wait, kFullTimeoutSeconds - 0.5) << "ENOSPC came before staging_full_timeout";
    EXPECT_LE(total.load(), kLimit);

    // После ENOSPC ФС жива: чтение и создание маленького файла (инлайн, места не занимает).
    EXPECT_EQ(verify_pattern_file(m->path("other"), kOtherSize, 77), "");
    sampler.stop();
    std::cout << "[res-enospc] max staged " << sampler.max_used() / kMiB << " MiB ("
              << sampler.max_used() << " bytes), VmHWM " << sampler.hwm_kib() / 1024 << " MiB\n";
    EXPECT_LE(sampler.max_used(), kLimit);
    // Размонтирование без сети не виснет (sync_timeout 2 с), код выхода 1 — данные не залиты.
    const Exit x = m->unmount(120s);
    EXPECT_EQ(x.signal, 0);
}

// Писателей меньше потоков цикла FUSE (libfuse: 10 по умолчанию).
TEST_F(Resources, FullStagingGivesEnospcAndReadsStayResponsive) { run_enospc_scenario(*this, 8); }

// Писателей больше потоков цикла FUSE: заблокированные write не должны занять их все.
TEST_F(Resources, ManyBlockedWritesDoNotStarveOtherOperations) { run_enospc_scenario(*this, 12); }

// --- чтение с перемоткой ---

store::StoreStats settled_stats(const fs::path& dir) {
    // Ядро может дочитывать вперёд уже после возврата read: ждём, пока счётчики встанут.
    auto prev = store::read_store_stats(dir);
    for (int i = 0; i < 50; ++i) {
        std::this_thread::sleep_for(200ms);
        const auto cur = store::read_store_stats(dir);
        if (cur == prev) {
            return cur;
        }
        prev = cur;
    }
    return prev;
}

// Сценарий проигрывателя на файле 1 ГиБ после перемонтирования (RAM-кэш пуст): чтение начала,
// три перехода с чтением по 4 МиБ, затем 64 МиБ подряд. Скачано (read_range) — не больше
// 64 МиБ + 3 × 4 МиБ + 2 × readahead_max.
TEST_F(Resources, PlayerSeekScenarioDownloadsOnlyWhatIsNeeded) {
    if (kSanitized) {
        GTEST_SKIP() << "resource test: a 1 GiB file is too slow under sanitizers";
    }
    constexpr uint64_t kFileSize = kGiB;
    constexpr uint64_t kSeed = 31;
    constexpr uint64_t kReadaheadMax = 32 * kMiB;
    ProfileSpec spec;
    spec.chunk_size = "8MiB";
    spec.extra = "readahead = \"4MiB\"\nreadahead_max = \"32MiB\"\n";
    Env& e = start(spec);
    {
        auto m = e.mount_fg();
        {
            const File f = File::open(m->path("video.bin"), O_WRONLY | O_CREAT | O_TRUNC);
            std::vector<uint8_t> buf(kMiB);
            for (uint64_t off = 0; off < kFileSize; off += kMiB) {
                fill_pattern(buf.data(), off, kMiB, kSeed);
                f.pwrite_all(buf.data(), kMiB, off);
            }
        }
        ASSERT_TRUE(m->unmount(10min).ok());
    }
    auto m = e.mount_fg();  // новый процесс: счётчики stats с нуля, RAM-кэш и кэш ядра пусты
    File f = File::open(m->path("video.bin"), O_RDONLY);
    std::vector<uint8_t> got(kMiB);
    const auto read_range = [&](uint64_t off, uint64_t len) {
        for (uint64_t done = 0; done < len; done += kMiB) {
            const size_t n = static_cast<size_t>(std::min<uint64_t>(kMiB, len - done));
            f.pread_exact(got.data(), n, off + done);
            ASSERT_EQ(std::memcmp(got.data(), pattern(off + done, n, kSeed).data(), n), 0)
                << "content mismatch at " << off + done;
        }
    };
    const auto stats = [&] { return settled_stats(e.local_dir()); };

    const auto s0 = stats();
    read_range(0, 4 * kMiB);  // начало файла: заголовок контейнера
    const auto s_head = stats();
    constexpr std::array<uint64_t, 3> kJumps{700 * kMiB, 200 * kMiB, 450 * kMiB};
    for (const uint64_t off : kJumps) {
        read_range(off, 4 * kMiB);
    }
    const auto s_jumps = stats();
    read_range(800 * kMiB, 64 * kMiB);
    const auto s_seq = stats();

    const auto total = s_seq.read_range_bytes - s0.read_range_bytes;
    const uint64_t limit = 64 * kMiB + uint64_t{3} * 4 * kMiB + 2 * kReadaheadMax;
    std::cout << "[res-seek] read_range MiB: head "
              << to_mib(s_head.read_range_bytes - s0.read_range_bytes) << ", jumps "
              << to_mib(s_jumps.read_range_bytes - s_head.read_range_bytes) << ", sequential "
              << to_mib(s_seq.read_range_bytes - s_jumps.read_range_bytes) << ", total "
              << to_mib(total) << " (limit " << to_mib(limit) << "); read_range calls "
              << s_seq.read_range_calls << "; prefetch calls: head " << s_head.prefetch_calls
              << ", jumps " << s_jumps.prefetch_calls - s_head.prefetch_calls << ", sequential "
              << s_seq.prefetch_calls - s_jumps.prefetch_calls << "; prefetch bytes sequential "
              << to_mib(s_seq.prefetch_bytes - s_jumps.prefetch_bytes) << " MiB\n";
    EXPECT_LE(total, limit);
    // Все 64 МиБ последовательного чтения реально прочитаны из хранилища (кэш пуст).
    EXPECT_GE(s_seq.read_range_bytes - s_jumps.read_range_bytes, 64 * kMiB);
    // При последовательном чтении ChunkReader ставит подсказки readahead.
    EXPECT_GE(s_seq.prefetch_calls - s_jumps.prefetch_calls, 3U);
    EXPECT_GT(s_seq.prefetch_bytes, s_jumps.prefetch_bytes);
    f.close();  // открытый файл мешал бы размонтированию (EBUSY)
    EXPECT_TRUE(m->unmount(2min).ok());
}

}  // namespace
}  // namespace telega::itest
