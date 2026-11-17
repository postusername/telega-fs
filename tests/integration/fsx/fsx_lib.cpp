#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <deque>
#include <exception>
#include <format>
#include <fstream>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "fsx.h"

namespace telega::fsx {

namespace {

constexpr std::array<std::string_view, kOpTypeCount> kOpNames = {
    "read", "write", "mapread", "mapwrite", "truncate", "fsync", "reopen"};

// Веса выбора операций. Записей чуть больше, чем чтений, чтобы файл успевал меняться;
// fsync и переоткрытие реже, но достаточно часто, чтобы попадать между любыми операциями.
constexpr std::array<std::uint64_t, kOpTypeCount> kOpWeights = {24, 28, 12, 12, 10, 7, 7};

constexpr std::uint64_t kMaxFileLen = 64ULL << 30U;  // модель живёт в памяти
constexpr std::uint64_t kMaxOpLen = 1ULL << 30U;
constexpr std::size_t kCheckChunk = 1U << 20U;
constexpr std::string_view kStateMagic = "tgfs-fsx-state";
constexpr int kStateVersion = 1;

std::string_view op_name(OpType t) { return kOpNames.at(static_cast<std::size_t>(t)); }

std::optional<OpType> op_from_name(std::string_view name) {
    for (std::size_t i = 0; i < kOpTypeCount; ++i) {
        if (kOpNames.at(i) == name) {
            return static_cast<OpType>(i);
        }
    }
    return std::nullopt;
}

// splitmix64: маленькое состояние (одно число), поэтому его легко сохранить в контрольной точке.
class SplitMix64 {
  public:
    explicit SplitMix64(std::uint64_t state) : state_(state) {}

    std::uint64_t next() {
        state_ += 0x9E3779B97F4A7C15ULL;
        std::uint64_t z = state_;
        z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31U);
    }

    // Равномерно в [0, n); смещение от взятия остатка для n << 2^64 несущественно.
    std::uint64_t below(std::uint64_t n) { return n == 0 ? 0 : next() % n; }

    [[nodiscard]] std::uint64_t state() const { return state_; }

  private:
    std::uint64_t state_;
};

// Сбой проверяемой ФС: расхождение с моделью или неожиданная ошибка системного вызова.
class TestFailure : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Ошибка окружения или параметров, не относящаяся к проверяемой ФС.
class SetupError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

std::string errno_text(int err) { return std::generic_category().message(err); }

[[noreturn]] void fail_errno(std::string_view what) {
    const int err = errno;
    throw TestFailure(std::format("{} failed: {}", what, errno_text(err)));
}

off_t to_off(std::uint64_t v) { return static_cast<off_t>(v); }

std::uint64_t digit_sum(std::uint64_t v) {
    std::uint64_t sum = 0;
    for (; v != 0; v >>= 8U) {
        sum += v & 0xFFU;
    }
    return sum;
}

// Данные записи: байт = номер операции + сумма байтов смещения (mod 256). Как в fsx, по
// байту видно, какая операция его записала; сумма старших байтов смещения добавлена, чтобы
// сдвиг на 256 байт, страницу или чанк не давал тех же данных.
std::uint8_t pattern_byte(std::uint64_t op, std::uint64_t off) {
    return static_cast<std::uint8_t>(op + digit_sum(off));
}

// Внутри выровненного блока в 256 байт шаблон — возрастающая последовательность байтов,
// поэтому блок копируется из «пилы» kRamp, а не считается по байту (важно для Debug-сборок).
constexpr std::array<std::uint8_t, 512> kRamp = [] {
    std::array<std::uint8_t, 512> r{};
    for (std::size_t i = 0; i < r.size(); ++i) {
        r.at(i) = static_cast<std::uint8_t>(i);
    }
    return r;
}();

void fill_pattern(std::span<std::uint8_t> buf, std::uint64_t op, std::uint64_t off) {
    std::size_t i = 0;
    while (i < buf.size()) {
        const std::uint64_t pos = off + i;
        const auto low = static_cast<std::size_t>(pos & 0xFFU);
        const std::size_t n = std::min<std::size_t>(buf.size() - i, 256U - low);
        const auto first = static_cast<std::size_t>((op + digit_sum(pos >> 8U) + low) & 0xFFU);
        std::memcpy(buf.subspan(i, n).data(), kRamp.data() + first, n);
        i += n;
    }
}

std::size_t pread_full(int fd, std::span<std::uint8_t> buf, std::uint64_t off) {
    std::size_t done = 0;
    while (done < buf.size()) {
        const auto rest = buf.subspan(done);
        const ssize_t n = ::pread(fd, rest.data(), rest.size(), to_off(off + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            fail_errno(std::format("pread(off={:#x}, len={:#x})", off + done, rest.size()));
        }
        if (n == 0) {
            break;
        }
        done += static_cast<std::size_t>(n);
    }
    return done;
}

void pwrite_full(int fd, std::span<const std::uint8_t> buf, std::uint64_t off) {
    std::size_t done = 0;
    while (done < buf.size()) {
        const auto rest = buf.subspan(done);
        const ssize_t n = ::pwrite(fd, rest.data(), rest.size(), to_off(off + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            fail_errno(std::format("pwrite(off={:#x}, len={:#x})", off + done, rest.size()));
        }
        if (n == 0) {
            throw TestFailure(std::format("pwrite(off={:#x}) wrote 0 bytes", off + done));
        }
        done += static_cast<std::size_t>(n);
    }
}

// Отображение области файла; munmap в деструкторе на случай исключения.
class Mapping {
  public:
    Mapping(int fd, std::uint64_t pg_off, std::size_t len, int prot) : len_(len) {
        addr_ = ::mmap(nullptr, len, prot, MAP_SHARED, fd, to_off(pg_off));
        if (addr_ == MAP_FAILED) {
            addr_ = nullptr;
            fail_errno(std::format("mmap(off={:#x}, len={:#x})", pg_off, len));
        }
    }
    Mapping(const Mapping&) = delete;
    Mapping& operator=(const Mapping&) = delete;
    Mapping(Mapping&&) = delete;
    Mapping& operator=(Mapping&&) = delete;
    ~Mapping() {
        if (addr_ != nullptr) {
            ::munmap(addr_, len_);
        }
    }

    [[nodiscard]] std::span<std::uint8_t> bytes() const {
        return {static_cast<std::uint8_t*>(addr_), len_};
    }

    void sync() const {
        if (::msync(addr_, len_, MS_SYNC) != 0) {
            fail_errno("msync");
        }
    }

    void unmap() {
        void* addr = std::exchange(addr_, nullptr);
        if (::munmap(addr, len_) != 0) {
            fail_errno("munmap");
        }
    }

  private:
    void* addr_ = nullptr;
    std::size_t len_;
};

struct LogEntry {
    std::uint64_t num = 0;
    OpType type = OpType::kRead;
    std::uint64_t off = 0;
    std::uint64_t len = 0;
    std::uint64_t size_before = 0;
    std::uint64_t size_after = 0;
    std::string note;  // без пробелов: пишется в файл состояния
};

std::string format_entry(const LogEntry& e) {
    std::string s = std::format("{:>9} {:<8}", e.num, op_name(e.type));
    switch (e.type) {
        case OpType::kRead:
        case OpType::kWrite:
        case OpType::kMapRead:
        case OpType::kMapWrite:
            s += std::format(" {:#010x} .. {:#010x} ({:#x} bytes)", e.off, e.off + e.len, e.len);
            break;
        case OpType::kTruncate:
            s += std::format(" to {:#010x}", e.off);
            break;
        case OpType::kFsync:
        case OpType::kReopen:
            break;
    }
    s += std::format("  size {:#x} -> {:#x}", e.size_before, e.size_after);
    if (!e.note.empty()) {
        s += "  [" + e.note + "]";
    }
    return s;
}

void add_note(LogEntry& e, std::string_view note) {
    if (!e.note.empty()) {
        e.note += ',';
    }
    e.note += note;
}

std::string hex_row(std::span<const std::uint8_t> data, std::int64_t start) {
    std::string s;
    for (std::int64_t i = start; i < start + 16; ++i) {
        if (i >= 0 && std::cmp_less(i, data.size())) {
            s += std::format(" {:02x}", data[static_cast<std::size_t>(i)]);
        } else {
            s += " ..";
        }
    }
    return s;
}

class Engine {
  public:
    Engine(Options opts, std::ostream& out, std::ostream& err)
        : opts_(std::move(opts)), out_(out), err_(err), rng_(opts_.seed) {
        const long page = ::sysconf(_SC_PAGESIZE);
        page_size_ = page > 0 ? static_cast<std::uint64_t>(page) : 4096U;
    }
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;
    Engine(Engine&&) = delete;
    Engine& operator=(Engine&&) = delete;
    ~Engine() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    int run();

  private:
    void start_fresh();
    void start_resume();
    void print_header() const;
    void step();
    OpType pick_op();
    void do_read(LogEntry& e);
    void do_write(LogEntry& e);
    void do_map_read(LogEntry& e);
    void do_map_write(LogEntry& e);
    void do_truncate(LogEntry& e);
    void do_fsync() const;
    void do_reopen();
    void pick_write_range(LogEntry& e);
    void inject_corruption(LogEntry& e);
    void open_file(int flags);
    void close_file();
    [[nodiscard]] std::uint64_t file_size() const;
    void check_size(std::string_view when) const;
    void full_check(std::string_view why);
    void compare(std::string_view what, std::uint64_t off, std::span<const std::uint8_t> expected,
                 std::span<const std::uint8_t> actual) const;
    [[nodiscard]] std::string describe_byte(std::uint64_t off, std::uint8_t expected,
                                            std::uint8_t actual) const;
    void checkpoint();
    void save_state() const;
    void load_state();
    [[nodiscard]] std::string state_path() const;
    [[nodiscard]] std::string dump_path(std::string_view suffix) const;
    void write_dumps();
    void report_failure(std::string_view msg);
    [[nodiscard]] double elapsed() const;

    Options opts_;
    std::ostream& out_;
    std::ostream& err_;
    SplitMix64 rng_;
    std::uint64_t op_ = 1;  // номер текущей (следующей) операции, с 1
    std::uint64_t first_op_ = 1;
    std::vector<std::uint8_t> model_;
    std::vector<std::uint8_t> buf_;
    std::deque<LogEntry> log_;
    int fd_ = -1;
    std::uint64_t page_size_ = 4096;
    bool inject_pending_ = false;
    std::chrono::steady_clock::time_point started_ = std::chrono::steady_clock::now();
};

double Engine::elapsed() const {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - started_).count();
}

std::string Engine::state_path() const {
    return opts_.state_path.empty() ? opts_.path + ".fsxstate" : opts_.state_path;
}

std::string Engine::dump_path(std::string_view suffix) const {
    if (opts_.dump_dir.empty()) {
        return opts_.path + std::string(suffix);
    }
    const auto slash = opts_.path.rfind('/');
    const std::string base = slash == std::string::npos ? opts_.path : opts_.path.substr(slash + 1);
    return opts_.dump_dir + "/" + base + std::string(suffix);
}

void Engine::open_file(int flags) {
    while (true) {
        fd_ = ::open(opts_.path.c_str(), flags | O_CLOEXEC, 0644);
        if (fd_ >= 0 || errno != EINTR) {
            break;
        }
    }
    if (fd_ < 0) {
        fail_errno(std::format("open({})", opts_.path));
    }
}

void Engine::close_file() {
    const int fd = std::exchange(fd_, -1);
    // close не повторяется по EINTR: в Linux дескриптор к этому моменту уже освобождён.
    if (::close(fd) != 0) {
        fail_errno("close");
    }
}

std::uint64_t Engine::file_size() const {
    struct stat st {};
    if (::fstat(fd_, &st) != 0) {
        fail_errno("fstat");
    }
    return static_cast<std::uint64_t>(st.st_size);
}

void Engine::check_size(std::string_view when) const {
    const std::uint64_t actual = file_size();
    if (actual != model_.size()) {
        throw TestFailure(std::format("size mismatch {}: fstat says {:#x}, model says {:#x}", when,
                                      actual, model_.size()));
    }
}

void Engine::start_fresh() {
    try {
        open_file(O_RDWR | O_CREAT | O_TRUNC);
    } catch (const TestFailure& e) {
        throw SetupError(e.what());
    }
    print_header();
    check_size("after create");
}

void Engine::start_resume() {
    load_state();
    print_header();
    if (!opts_.quiet) {
        out_ << std::format("fsx: resuming at op {} from {}\n", op_, state_path());
    }
    // Если файла нет после перемонтирования, это сбой ФС, а не ошибка использования.
    open_file(O_RDWR);
    full_check("on resume");
}

void Engine::print_header() const {
    if (opts_.quiet) {
        return;
    }
    std::string ops;
    for (std::size_t i = 0; i < kOpTypeCount; ++i) {
        if (opts_.enabled.at(i)) {
            ops += ops.empty() ? "" : ",";
            ops += kOpNames.at(i);
        }
    }
    out_ << std::format(
        "fsx: file {}, seed {}, ops {}..{}, maxlen {:#x}, maxop {:#x}, page {}, enabled {}\n",
        opts_.path, opts_.seed, op_, opts_.num_ops, opts_.max_len, opts_.max_op, page_size_, ops);
}

OpType Engine::pick_op() {
    std::uint64_t total = 0;
    for (std::size_t i = 0; i < kOpTypeCount; ++i) {
        total += opts_.enabled.at(i) ? kOpWeights.at(i) : 0;
    }
    std::uint64_t r = rng_.below(total);
    for (std::size_t i = 0; i < kOpTypeCount; ++i) {
        if (!opts_.enabled.at(i)) {
            continue;
        }
        if (r < kOpWeights.at(i)) {
            return static_cast<OpType>(i);
        }
        r -= kOpWeights.at(i);
    }
    return OpType::kRead;
}

void Engine::step() {
    LogEntry& e = log_.emplace_back();
    while (log_.size() > std::max<std::size_t>(opts_.log_len, 1)) {
        log_.pop_front();
    }
    e.num = op_;
    e.type = pick_op();
    e.size_before = model_.size();
    e.size_after = model_.size();
    switch (e.type) {
        case OpType::kRead:
            do_read(e);
            break;
        case OpType::kWrite:
            do_write(e);
            break;
        case OpType::kMapRead:
            do_map_read(e);
            break;
        case OpType::kMapWrite:
            do_map_write(e);
            break;
        case OpType::kTruncate:
            do_truncate(e);
            break;
        case OpType::kFsync:
            do_fsync();
            break;
        case OpType::kReopen:
            do_reopen();
            break;
    }
    e.size_after = model_.size();

    if (opts_.inject_corruption_at != 0 && op_ == opts_.inject_corruption_at) {
        inject_pending_ = true;
    }
    if (inject_pending_ && !model_.empty()) {
        inject_corruption(e);
    }
    if (opts_.verbose) {
        out_ << format_entry(e) << '\n';
    }
    if (opts_.full_check_every != 0 && op_ % opts_.full_check_every == 0) {
        full_check("periodic");
    }
    if (!opts_.quiet && opts_.progress_every != 0 && op_ % opts_.progress_every == 0) {
        out_ << std::format("fsx: op {}/{}, size {:#x}, {:.1f} s\n", op_, opts_.num_ops,
                            model_.size(), elapsed());
    }
}

void Engine::do_read(LogEntry& e) {
    const std::uint64_t size = model_.size();
    e.off = rng_.below(size);
    e.len = 1 + rng_.below(opts_.max_op);
    // Просим e.len байт, даже если это за концом файла: проверяется и короткое чтение у EOF.
    const std::uint64_t expected = std::min(e.len, size - e.off);
    buf_.resize(static_cast<std::size_t>(e.len));
    const std::size_t got = pread_full(fd_, buf_, e.off);
    const std::span<const std::uint8_t> want(model_.data() + e.off,
                                             static_cast<std::size_t>(expected));
    compare("read", e.off, want, std::span<const std::uint8_t>(buf_).first(got));
}

void Engine::pick_write_range(LogEntry& e) {
    e.off = rng_.below(opts_.max_len);
    e.len = 1 + rng_.below(opts_.max_op);
    e.len = std::min(e.len, opts_.max_len - e.off);
}

void Engine::do_write(LogEntry& e) {
    pick_write_range(e);
    buf_.resize(static_cast<std::size_t>(e.len));
    fill_pattern(buf_, op_, e.off);
    if (e.off > model_.size()) {
        add_note(e, "hole");
    }
    pwrite_full(fd_, buf_, e.off);
    const std::uint64_t end = e.off + e.len;
    if (end > model_.size()) {
        model_.resize(static_cast<std::size_t>(end));
    }
    std::memcpy(model_.data() + e.off, buf_.data(), buf_.size());
    check_size("after write");
}

void Engine::do_map_read(LogEntry& e) {
    const std::uint64_t size = model_.size();
    if (size == 0) {
        add_note(e, "skip-empty");
        return;
    }
    e.off = rng_.below(size);
    e.len = std::min(1 + rng_.below(opts_.max_op), size - e.off);
    const std::uint64_t pg_off = e.off & ~(page_size_ - 1);
    const auto delta = static_cast<std::size_t>(e.off - pg_off);
    const auto len = static_cast<std::size_t>(e.len);
    buf_.resize(len);
    {
        Mapping map(fd_, pg_off, delta + len, PROT_READ);
        std::memcpy(buf_.data(), map.bytes().subspan(delta, len).data(), len);
        map.unmap();
    }
    compare("mapread", e.off, std::span<const std::uint8_t>(model_).subspan(e.off, len), buf_);
}

void Engine::do_map_write(LogEntry& e) {
    pick_write_range(e);
    const std::uint64_t end = e.off + e.len;
    // Как в fsx: отображать за концом файла нельзя, поэтому сначала расширяем его ftruncate.
    if (end > model_.size()) {
        add_note(e, "extend");
        if (::ftruncate(fd_, to_off(end)) != 0) {
            fail_errno(std::format("ftruncate(size={:#x}) before mapwrite", end));
        }
        model_.resize(static_cast<std::size_t>(end));
        check_size("after mapwrite extension");
    }
    const std::uint64_t pg_off = e.off & ~(page_size_ - 1);
    const auto delta = static_cast<std::size_t>(e.off - pg_off);
    const auto len = static_cast<std::size_t>(e.len);
    buf_.resize(len);
    fill_pattern(buf_, op_, e.off);
    {
        Mapping map(fd_, pg_off, delta + len, PROT_READ | PROT_WRITE);
        std::memcpy(map.bytes().subspan(delta, len).data(), buf_.data(), len);
        map.sync();
        map.unmap();
    }
    std::memcpy(model_.data() + e.off, buf_.data(), buf_.size());
    check_size("after mapwrite");
}

void Engine::do_truncate(LogEntry& e) {
    e.off = rng_.below(opts_.max_len + 1);
    add_note(e, e.off >= model_.size() ? "up" : "down");
    if (::ftruncate(fd_, to_off(e.off)) != 0) {
        fail_errno(std::format("ftruncate(size={:#x})", e.off));
    }
    // При расширении vector::resize заполняет хвост нулями — как и ФС.
    model_.resize(static_cast<std::size_t>(e.off));
    check_size("after truncate");
}

void Engine::do_fsync() const {
    if (::fsync(fd_) != 0) {
        fail_errno("fsync");
    }
}

void Engine::do_reopen() {
    close_file();
    open_file(O_RDWR);
    check_size("after reopen");
}

void Engine::inject_corruption(LogEntry& e) {
    inject_pending_ = false;
    SplitMix64 g(opts_.seed ^ (op_ * 0x2545F4914F6CDD1DULL));
    const std::uint64_t off = g.below(model_.size());
    std::array<std::uint8_t, 1> byte{};
    if (pread_full(fd_, byte, off) != 1) {
        throw TestFailure(std::format("inject: short read at {:#x}", off));
    }
    byte[0] ^= 0xA5U;
    pwrite_full(fd_, byte, off);  // модель не меняется: это и есть порча мимо модели
    add_note(e, std::format("corrupt@{:#x}", off));
    if (!opts_.quiet) {
        out_ << std::format("fsx: injected corruption at offset {:#x} after op {}\n", off, op_);
    }
}

void Engine::full_check(std::string_view why) {
    check_size(std::format("in full check ({})", why));
    buf_.resize(kCheckChunk);
    const std::string what = std::format("full check ({})", why);
    for (std::uint64_t off = 0; off < model_.size(); off += kCheckChunk) {
        const auto len =
            static_cast<std::size_t>(std::min<std::uint64_t>(kCheckChunk, model_.size() - off));
        const std::span<std::uint8_t> chunk = std::span<std::uint8_t>(buf_).first(len);
        const std::size_t got = pread_full(fd_, chunk, off);
        compare(what, off, std::span<const std::uint8_t>(model_).subspan(off, len),
                chunk.first(got));
    }
    std::array<std::uint8_t, 1> tail{};
    if (pread_full(fd_, tail, model_.size()) != 0) {
        throw TestFailure(std::format("{}: read at EOF {:#x} returned data", what, model_.size()));
    }
}

void Engine::compare(std::string_view what, std::uint64_t off,
                     std::span<const std::uint8_t> expected,
                     std::span<const std::uint8_t> actual) const {
    if (expected.size() == actual.size() &&
        (expected.empty() || std::memcmp(expected.data(), actual.data(), expected.size()) == 0)) {
        return;
    }
    const std::size_t common = std::min(expected.size(), actual.size());
    const auto [exp_it, act_it] =
        std::ranges::mismatch(expected.first(common), actual.first(common));
    const auto first = static_cast<std::size_t>(exp_it - expected.begin());
    if (first == common && expected.size() == actual.size()) {
        return;
    }

    std::string msg = std::format("MISMATCH in {} of {:#x}..{:#x} (op {})\n", what, off,
                                  off + expected.size(), op_);
    if (expected.size() != actual.size()) {
        msg += std::format("  length: expected {:#x} bytes, got {:#x}\n", expected.size(),
                           actual.size());
    }
    if (first < common) {
        std::size_t last = first;
        std::size_t count = 0;
        for (std::size_t i = first; i < common; ++i) {
            if (expected[i] != actual[i]) {
                last = i;
                ++count;
            }
        }
        msg += std::format("  first bad byte at {:#x}, last at {:#x}, {} of {:#x} bytes differ\n",
                           off + first, off + last, count, common);
        // Окно 64 байта, выровненное по 16 в координатах файла.
        const std::uint64_t row0 = ((off + first) & ~std::uint64_t{15}) -
                                   std::min<std::uint64_t>(16, (off + first) & ~std::uint64_t{15});
        for (std::uint64_t row = row0; row < row0 + 64; row += 16) {
            const auto rel = static_cast<std::int64_t>(row) - static_cast<std::int64_t>(off);
            std::string marks;
            for (std::int64_t i = rel; i < rel + 16; ++i) {
                const bool bad =
                    i >= 0 && std::cmp_less(i, common) &&
                    expected[static_cast<std::size_t>(i)] != actual[static_cast<std::size_t>(i)];
                marks += bad ? " ^^" : "   ";
            }
            msg += std::format("  {:#010x} expected{}\n", row, hex_row(expected, rel));
            msg += std::format("  {:10} actual  {}\n", "", hex_row(actual, rel));
            if (marks.find('^') != std::string::npos) {
                msg += std::format("  {:10}         {}\n", "", marks);
            }
        }
        msg += describe_byte(off + first, expected[first], actual[first]);
    }
    throw TestFailure(msg);
}

// Подсказка по журналу: кто должен был записать ожидаемый байт и чей шаблон похож на фактический.
std::string Engine::describe_byte(std::uint64_t off, std::uint8_t expected,
                                  std::uint8_t actual) const {
    auto writes = [off](const LogEntry& e) {
        return (e.type == OpType::kWrite || e.type == OpType::kMapWrite) && e.off <= off &&
               off < e.off + e.len;
    };
    // Операция задела байт, если записала его или изменила размер файла через него
    // (обрезка, расширение нулями, дыра перед записью за концом файла).
    auto covers = [&](const LogEntry& e) {
        return writes(e) || (std::min(e.size_before, e.size_after) <= off &&
                             off < std::max(e.size_before, e.size_after));
    };
    auto source = [&](std::uint8_t byte) -> std::string {
        for (const LogEntry& e : std::views::reverse(log_)) {
            if (writes(e) && pattern_byte(e.num, off) == byte) {
                return std::format("matches data of op {} ({})", e.num, op_name(e.type));
            }
        }
        const auto residue = static_cast<std::uint8_t>(byte - digit_sum(off));
        return std::format("{}pattern of an op with number % 256 == {}, not in the log",
                           byte == 0 ? "zero, or " : "", residue);
    };
    std::string last_touch = "no op in the log touched it";
    for (const LogEntry& e : std::views::reverse(log_)) {
        if (covers(e)) {
            last_touch = std::format("last touched by op {} ({})", e.num, op_name(e.type));
            break;
        }
    }
    return std::format("  byte at {:#x}: expected {:#04x} ({}; {}),\n  {:16}actual {:#04x} ({})\n",
                       off, expected, last_touch, source(expected), "", actual, source(actual));
}

void Engine::checkpoint() {
    full_check("before checkpoint");
    close_file();
    save_state();
    if (!opts_.quiet) {
        out_ << std::format("fsx: checkpoint after op {}, state saved to {} ({:.1f} s)\n", op_ - 1,
                            state_path(), elapsed());
    }
}

// Формат: текстовый заголовок «ключ значение», журнал строками, затем «model N\n» и N байт модели.
void Engine::save_state() const {
    const std::string path = state_path();
    const std::string tmp = path + ".tmp";
    {
        std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
        std::uint64_t ops_mask = 0;
        for (std::size_t i = 0; i < kOpTypeCount; ++i) {
            ops_mask |= opts_.enabled.at(i) ? (1ULL << i) : 0;
        }
        f << kStateMagic << ' ' << kStateVersion << '\n'
          << "seed " << opts_.seed << '\n'
          << "rng " << rng_.state() << '\n'
          << "next_op " << op_ << '\n'
          << "num_ops " << opts_.num_ops << '\n'
          << "max_len " << opts_.max_len << '\n'
          << "max_op " << opts_.max_op << '\n'
          << "ops " << ops_mask << '\n'
          << "log " << log_.size() << '\n';
        for (const LogEntry& e : log_) {
            f << e.num << ' ' << op_name(e.type) << ' ' << e.off << ' ' << e.len << ' '
              << e.size_before << ' ' << e.size_after << ' ' << (e.note.empty() ? "-" : e.note)
              << '\n';
        }
        f << "model " << model_.size() << '\n';
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): байты модели в ostream
        f.write(reinterpret_cast<const char*>(model_.data()),
                static_cast<std::streamsize>(model_.size()));
        f.close();
        if (!f) {
            throw SetupError(std::format("cannot write state file {}", tmp));
        }
    }
    if (std::rename(tmp.c_str(), path.c_str()) != 0) {
        const int err = errno;
        throw SetupError(std::format("rename {} -> {}: {}", tmp, path, errno_text(err)));
    }
}

void Engine::load_state() {
    const std::string path = state_path();
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        throw SetupError(std::format("cannot open state file {}", path));
    }
    auto bad = [&path](std::string_view why) {
        return SetupError(std::format("bad state file {}: {}", path, why));
    };
    std::string magic;
    int version = 0;
    f >> magic >> version;
    if (!f || magic != kStateMagic || version != kStateVersion) {
        throw bad("unknown format");
    }
    auto read_kv = [&](std::string_view key) {
        std::string k;
        std::uint64_t v = 0;
        f >> k >> v;
        if (!f || k != key) {
            throw bad(std::format("expected '{}'", key));
        }
        return v;
    };
    const std::uint64_t seed = read_kv("seed");
    const std::uint64_t rng = read_kv("rng");
    const std::uint64_t next_op = read_kv("next_op");
    const std::uint64_t num_ops = read_kv("num_ops");
    const std::uint64_t max_len = read_kv("max_len");
    const std::uint64_t max_op = read_kv("max_op");
    const std::uint64_t ops_mask = read_kv("ops");
    std::array<bool, kOpTypeCount> enabled{};
    for (std::size_t i = 0; i < kOpTypeCount; ++i) {
        enabled.at(i) = (ops_mask & (1ULL << i)) != 0;
    }
    if (next_op == 0 || max_len == 0 || max_len > kMaxFileLen || max_op == 0 ||
        max_op > kMaxOpLen || !enabled.at(static_cast<std::size_t>(OpType::kRead)) ||
        !enabled.at(static_cast<std::size_t>(OpType::kWrite))) {
        throw bad("invalid parameters");
    }

    auto conflict = [&](ParamBit bit, std::string_view opt, bool same) {
        if ((opts_.explicit_params & bit) != 0 && !same) {
            throw SetupError(std::format("{} conflicts with the value saved in {}", opt, path));
        }
    };
    conflict(kParamSeed, "-S", opts_.seed == seed);
    conflict(kParamMaxLen, "-l", opts_.max_len == max_len);
    conflict(kParamMaxOp, "-o", opts_.max_op == max_op);
    conflict(kParamOps, "-R/-W/-T/-F/-C", opts_.enabled == enabled);
    opts_.seed = seed;
    opts_.max_len = max_len;
    opts_.max_op = max_op;
    opts_.enabled = enabled;
    if ((opts_.explicit_params & kParamNumOps) == 0) {
        opts_.num_ops = num_ops;
    }
    rng_ = SplitMix64(rng);
    op_ = next_op;
    first_op_ = next_op;

    const std::uint64_t log_count = read_kv("log");
    for (std::uint64_t i = 0; i < log_count; ++i) {
        LogEntry e;
        std::string name;
        f >> e.num >> name >> e.off >> e.len >> e.size_before >> e.size_after >> e.note;
        const auto type = op_from_name(name);
        if (!f || !type) {
            throw bad("broken log entry");
        }
        e.type = *type;
        if (e.note == "-") {
            e.note.clear();
        }
        log_.push_back(std::move(e));
    }
    while (log_.size() > std::max<std::size_t>(opts_.log_len, 1)) {
        log_.pop_front();
    }

    const std::uint64_t size = read_kv("model");
    if (size > max_len || f.get() != '\n') {
        throw bad("broken model header");
    }
    model_.resize(static_cast<std::size_t>(size));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): байты модели из istream
    f.read(reinterpret_cast<char*>(model_.data()), static_cast<std::streamsize>(size));
    if (std::cmp_not_equal(f.gcount(), size)) {
        throw bad("model is truncated");
    }
}

void Engine::write_dumps() {
    auto save = [this](const std::string& path, std::span<const std::uint8_t> data) {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): байты образа в ostream
        f.write(reinterpret_cast<const char*>(data.data()),
                static_cast<std::streamsize>(data.size()));
        f.close();
        if (!f) {
            err_ << std::format("fsx: warning: cannot write {}\n", path);
            return false;
        }
        return true;
    };
    const std::string good = dump_path(".fsxgood");
    const std::string bad = dump_path(".fsxbad");
    const bool good_ok = save(good, model_);

    // Фактическое содержимое читаем заново через отдельный дескриптор: рабочий мог закрыться.
    std::vector<std::uint8_t> actual;
    bool bad_ok = false;
    const int fd = ::open(opts_.path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        try {
            struct stat st {};
            if (::fstat(fd, &st) == 0 && st.st_size >= 0) {
                actual.resize(static_cast<std::size_t>(st.st_size));
                actual.resize(pread_full(fd, actual, 0));
                bad_ok = save(bad, actual);
            }
        } catch (const std::exception& e) {
            err_ << std::format("fsx: warning: cannot read {}: {}\n", opts_.path, e.what());
        }
        ::close(fd);
    } else {
        const int err = errno;
        err_ << std::format("fsx: warning: cannot open {}: {}\n", opts_.path, errno_text(err));
    }
    if (good_ok) {
        err_ << std::format("fsx: model (expected contents) saved to {}\n", good);
    }
    if (bad_ok) {
        err_ << std::format("fsx: actual file contents saved to {}\n", bad);
    }
}

void Engine::report_failure(std::string_view msg) {
    err_ << "fsx: " << msg;
    if (!msg.ends_with('\n')) {
        err_ << '\n';
    }
    err_ << std::format("fsx: last {} operations (oldest first):\n", log_.size());
    for (const LogEntry& e : log_) {
        err_ << "  " << format_entry(e) << '\n';
    }
    err_ << std::format("fsx: FAILED at op {} of {}, seed {}, file {}\n", op_, opts_.num_ops,
                        opts_.seed, opts_.path);
    write_dumps();
    err_.flush();
}

int Engine::run() {
    try {
        if (opts_.resume) {
            start_resume();
        } else {
            start_fresh();
        }
        if (opts_.num_ops + 1 < op_) {
            throw SetupError(
                std::format("-N {} is below the saved position (next op {})", opts_.num_ops, op_));
        }
        if (opts_.checkpoint_at != 0 &&
            (opts_.checkpoint_at < op_ || opts_.checkpoint_at >= opts_.num_ops)) {
            throw SetupError(std::format("--checkpoint {} must be in [{}, {})", opts_.checkpoint_at,
                                         op_, opts_.num_ops));
        }
    } catch (const SetupError& e) {
        err_ << "fsx: " << e.what() << '\n';
        return kExitUsage;
    } catch (const TestFailure& e) {
        report_failure(e.what());
        return kExitFailure;
    }

    try {
        for (; op_ <= opts_.num_ops; ++op_) {
            if (opts_.checkpoint_at != 0 && op_ > opts_.checkpoint_at) {
                checkpoint();
                return kExitOk;
            }
            step();
        }
        op_ = opts_.num_ops;
        full_check("final");
        close_file();
    } catch (const SetupError& e) {
        err_ << "fsx: " << e.what() << '\n';
        return kExitUsage;
    } catch (const TestFailure& e) {
        report_failure(e.what());
        return kExitFailure;
    }

    if (opts_.resume) {
        // Состояние израсходовано; устаревший файл состояния не должен попасть в следующий прогон.
        static_cast<void>(std::remove(state_path().c_str()));
    }
    if (!opts_.quiet) {
        out_ << std::format(
            "fsx: ops {}..{} completed without mismatches in {:.2f} s, final size {:#x}\n",
            first_op_, opts_.num_ops, elapsed(), model_.size());
    }
    return kExitOk;
}

std::uint64_t parse_number(std::string_view opt, std::string_view text, bool allow_suffix) {
    std::uint64_t value = 0;
    const char* const begin = text.data();
    const char* const end = begin + text.size();
    const auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc{} || ptr == begin) {
        throw std::invalid_argument(std::format("{}: invalid number '{}'", opt, text));
    }
    const std::string_view suffix(ptr, static_cast<std::size_t>(end - ptr));
    unsigned shift = 0;
    if (suffix.empty()) {
        return value;
    }
    if (allow_suffix && suffix.size() == 1) {
        switch (suffix[0]) {
            case 'k':
            case 'K':
                shift = 10;
                break;
            case 'm':
            case 'M':
                shift = 20;
                break;
            case 'g':
            case 'G':
                shift = 30;
                break;
            default:
                break;
        }
    }
    if (shift == 0) {
        throw std::invalid_argument(std::format("{}: invalid suffix in '{}'", opt, text));
    }
    if (value > (UINT64_MAX >> shift)) {
        throw std::invalid_argument(std::format("{}: value '{}' is too large", opt, text));
    }
    return value << shift;
}

}  // namespace

void print_usage(std::ostream& out) {
    out << "usage: tgfs-fsx [options] FILE\n"
           "\n"
           "Random read/write/truncate/mmap/fsync/reopen operations on FILE, checked against an\n"
           "in-memory model (a port of fsx from xfstests). Exit code: 0 ok, 1 mismatch or\n"
           "file system error, 2 usage error.\n"
           "\n"
           "  -N n                 number of operations (default 10000)\n"
           "  -S n                 seed (default 1)\n"
           "  -l size              maximum file length (default 256k; suffixes k, m, g)\n"
           "  -o size              maximum length of one operation (default 64k)\n"
           "  -R                   no mmap reads\n"
           "  -W                   no mmap writes\n"
           "  -T                   no truncate\n"
           "  -F                   no fsync\n"
           "  -C                   no close/reopen\n"
           "  -p n                 print progress every n operations\n"
           "  -q                   quiet: print only failures\n"
           "  -v                   print every operation\n"
           "  -L n                 length of the operation log (default 100)\n"
           "  --full-check n       compare the whole file every n operations (default 1000,\n"
           "                       0 = only at the end)\n"
           "  --checkpoint k       after k operations save state, close FILE and exit 0\n"
           "  --resume             load state, verify FILE, continue with the next operation\n"
           "  --state path         state file (default FILE.fsxstate)\n"
           "  --dump-dir dir       where to write .fsxgood/.fsxbad (default next to FILE)\n"
           "  --inject-corruption n  self-test: corrupt one byte of FILE behind the model's\n"
           "                       back after operation n (must be detected)\n"
           "  -h, --help           this help\n";
}

Options parse_args(const std::vector<std::string>& args) {
    Options o;
    std::vector<std::string> positional;
    for (std::size_t i = 0; i < args.size(); ++i) {
        const std::string& a = args[i];
        auto value_for = [&](std::string_view opt, std::optional<std::string> attached) {
            if (attached) {
                return *attached;
            }
            if (i + 1 >= args.size()) {
                throw std::invalid_argument(std::format("{} requires a value", opt));
            }
            return args[++i];
        };

        if (a == "--") {
            positional.insert(positional.end(), args.begin() + static_cast<std::ptrdiff_t>(i) + 1,
                              args.end());
            break;
        }
        if (a.starts_with("--")) {
            std::string name = a;
            std::optional<std::string> attached;
            if (const auto eq = a.find('='); eq != std::string::npos) {
                name = a.substr(0, eq);
                attached = a.substr(eq + 1);
            }
            const bool is_flag = name == "--help" || name == "--resume";
            if (is_flag && attached) {
                throw std::invalid_argument(std::format("{} takes no value", name));
            }
            if (name == "--help") {
                o.help = true;
            } else if (name == "--resume") {
                o.resume = true;
            } else if (name == "--checkpoint") {
                o.checkpoint_at = parse_number(name, value_for(name, attached), false);
                if (o.checkpoint_at == 0) {
                    throw std::invalid_argument("--checkpoint must be positive");
                }
            } else if (name == "--state") {
                o.state_path = value_for(name, attached);
            } else if (name == "--dump-dir") {
                o.dump_dir = value_for(name, attached);
            } else if (name == "--full-check") {
                o.full_check_every = parse_number(name, value_for(name, attached), false);
            } else if (name == "--inject-corruption") {
                o.inject_corruption_at = parse_number(name, value_for(name, attached), false);
                if (o.inject_corruption_at == 0) {
                    throw std::invalid_argument("--inject-corruption must be positive");
                }
            } else {
                throw std::invalid_argument(std::format("unknown option {}", name));
            }
            continue;
        }
        if (a.size() > 1 && a[0] == '-') {
            for (std::size_t j = 1; j < a.size(); ++j) {
                const char c = a[j];
                const std::string opt = std::string("-") + c;
                auto disable = [&o](OpType t) {
                    o.enabled.at(static_cast<std::size_t>(t)) = false;
                    o.explicit_params |= kParamOps;
                };
                switch (c) {
                    case 'R':
                        disable(OpType::kMapRead);
                        continue;
                    case 'W':
                        disable(OpType::kMapWrite);
                        continue;
                    case 'T':
                        disable(OpType::kTruncate);
                        continue;
                    case 'F':
                        disable(OpType::kFsync);
                        continue;
                    case 'C':
                        disable(OpType::kReopen);
                        continue;
                    case 'q':
                        o.quiet = true;
                        continue;
                    case 'v':
                        o.verbose = true;
                        continue;
                    case 'h':
                        o.help = true;
                        continue;
                    default:
                        break;
                }
                std::optional<std::string> attached;
                if (j + 1 < a.size()) {
                    attached = a.substr(j + 1);
                }
                switch (c) {
                    case 'N':
                        o.num_ops = parse_number(opt, value_for(opt, attached), false);
                        o.explicit_params |= kParamNumOps;
                        break;
                    case 'S':
                        o.seed = parse_number(opt, value_for(opt, attached), false);
                        o.explicit_params |= kParamSeed;
                        break;
                    case 'l':
                        o.max_len = parse_number(opt, value_for(opt, attached), true);
                        o.explicit_params |= kParamMaxLen;
                        break;
                    case 'o':
                        o.max_op = parse_number(opt, value_for(opt, attached), true);
                        o.explicit_params |= kParamMaxOp;
                        break;
                    case 'p':
                        o.progress_every = parse_number(opt, value_for(opt, attached), false);
                        break;
                    case 'L':
                        o.log_len = static_cast<std::size_t>(
                            parse_number(opt, value_for(opt, attached), false));
                        break;
                    default:
                        throw std::invalid_argument(std::format("unknown option {}", opt));
                }
                break;  // значение съело остаток аргумента
            }
            continue;
        }
        positional.push_back(a);
    }

    if (o.help) {
        return o;
    }
    if (positional.size() != 1) {
        throw std::invalid_argument("exactly one FILE is required");
    }
    o.path = positional[0];
    if (o.max_len == 0 || o.max_len > kMaxFileLen) {
        throw std::invalid_argument(std::format("-l must be in [1, {}]", kMaxFileLen));
    }
    if (o.max_op == 0 || o.max_op > kMaxOpLen) {
        throw std::invalid_argument(std::format("-o must be in [1, {}]", kMaxOpLen));
    }
    return o;
}

int run(const Options& opts, std::ostream& out, std::ostream& err) {
    Engine engine(opts, out, err);
    return engine.run();
}

int fsx_main(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    try {
        const Options opts = parse_args(args);
        if (opts.help) {
            print_usage(out);
            return kExitOk;
        }
        return run(opts, out, err);
    } catch (const std::invalid_argument& e) {
        err << "tgfs-fsx: " << e.what() << "\n(see tgfs-fsx --help)\n";
        return kExitUsage;
    } catch (const std::exception& e) {
        err << "tgfs-fsx: " << e.what() << '\n';
        return kExitUsage;
    }
}

}  // namespace telega::fsx
