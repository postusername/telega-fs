#include "store/local_dir_store.h"

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>

#include "common/files.h"

// Устройство LocalDirStore.
//
// Раскладка каталога: next_id — следующий msg_id текстом, msgs/<msg_id> — сообщения,
// pinned — закреплённые msg_id по одному в строке. Состояние не кэшируется в памяти: каждый вызов
// читает next_id и pinned с диска, поэтому новый экземпляр (или перезапущенный процесс) видит всё,
// что записал прежний. Межпроцессной блокировки нет: писатель один.
//
// mu_ сериализует изменения next_id и pinned и защищает правила сбоев и счётчики. Копирование
// содержимого в msgs/, чтение диапазонов и блобов индекса и задержки выполняются без mu_:
// файл сообщения после rename не меняется, а его id до возврата из put_* никому не известен.
//
// Порядок записи в put_*: сначала долговечно сдвигается next_id, потом файл копируется через
// tmp + fsync + rename + fsync каталога. Сбой между шагами оставляет дырку в id, но не
// повторную выдачу id. В delete_messages сначала обновляется pinned, потом удаляются файлы,
// чтобы после сбоя pinned не ссылался на удалённое сообщение.

namespace telega::store {

namespace {

constexpr int kCrashExitCode = 86;
constexpr size_t kCopyBlock = size_t{1} << 20;

constexpr std::array<std::pair<StoreOp, std::string_view>, kStoreOpCount> kOpNames{{
    {StoreOp::put_chunk, "put_chunk"},
    {StoreOp::read_range, "read_range"},
    {StoreOp::delete_messages, "delete_messages"},
    {StoreOp::read_head, "read_head"},
    {StoreOp::read_index, "read_index"},
    {StoreOp::put_index, "put_index"},
    {StoreOp::set_head, "set_head"},
}};

std::string_view op_name(StoreOp op) {
    for (const auto& [o, name] : kOpNames) {
        if (o == op) {
            return name;
        }
    }
    return "unknown";
}

std::string_view trim(std::string_view s) {
    constexpr std::string_view kSpace = " \t\r\n\v\f";
    const auto begin = s.find_first_not_of(kSpace);
    if (begin == std::string_view::npos) {
        return {};
    }
    const auto end = s.find_last_not_of(kSpace);
    return s.substr(begin, end - begin + 1);
}

// Целое без знака и пробелов внутри; nullopt — не число или переполнение.
template <class Int>
std::optional<Int> parse_int(std::string_view s) {
    if (s.empty() || s.front() < '0' || s.front() > '9') {
        return std::nullopt;
    }
    Int value{};
    const auto* end = s.data() + s.size();
    const auto [ptr, ec] = std::from_chars(s.data(), end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

std::string_view as_text(ByteSpan data) {
    return {reinterpret_cast<const char*>(data.data()),  // NOLINT(*-reinterpret-cast): байты текста
            data.size()};
}

[[noreturn]] void throw_store(StoreErrorKind kind, const std::string& what) {
    throw StoreError(kind, "LocalDirStore: " + what);
}

uint64_t file_size(const Fd& fd) {
    struct stat st {};
    if (::fstat(fd.get(), &st) != 0) {
        throw SysError(errno, "fstat");
    }
    return static_cast<uint64_t>(st.st_size);
}

// Скопировать src в dst через dst.tmp + fsync + rename + fsync каталога.
// throttle(n) вызывается перед записью каждого блока из n байт.
void copy_file_durable(const Fd& src, const std::filesystem::path& dst,
                       const std::function<void(uint64_t)>& throttle) {
    auto tmp = dst;
    tmp += ".tmp";
    try {
        {
            const Fd out = Fd::open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
            Bytes buf(kCopyBlock);
            uint64_t offset = 0;
            for (;;) {
                const size_t n = pread_full(src.get(), buf, offset);
                if (n == 0) {
                    break;
                }
                throttle(n);
                pwrite_full(out.get(), ByteSpan{buf}.first(n), offset);
                offset += n;
            }
            if (::fsync(out.get()) != 0) {
                throw SysError(errno, "fsync " + tmp.string());
            }
        }
        std::error_code ec;
        std::filesystem::rename(tmp, dst, ec);
        if (ec) {
            throw SysError(ec.value(), "rename " + tmp.string());
        }
    } catch (...) {
        std::error_code ignored;
        std::filesystem::remove(tmp, ignored);
        throw;
    }
    fsync_dir(dst.parent_path());
}

// Числовые имена в msgs/ по возрастанию; *.tmp и прочее пропускаются.
std::vector<int64_t> scan_messages(const std::filesystem::path& msgs_dir) {
    std::vector<int64_t> ids;
    for (const auto& entry : std::filesystem::directory_iterator(msgs_dir)) {
        const auto name = entry.path().filename().string();
        if (const auto id = parse_int<int64_t>(name)) {
            ids.push_back(*id);
        }
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::string format_next_id(int64_t id) { return std::to_string(id) + "\n"; }

}  // namespace

// ---------------------------------------------------------------------------------------------
// Правила сбоев

std::vector<FaultRule> parse_fault_rules(std::string_view spec) {
    std::vector<FaultRule> rules;
    size_t pos = 0;
    while (pos <= spec.size()) {
        const auto semi = spec.find(';', pos);
        const auto end = semi == std::string_view::npos ? spec.size() : semi;
        const auto rule_text = trim(spec.substr(pos, end - pos));
        pos = end + 1;
        if (rule_text.empty()) {
            continue;  // пустая строка или лишний ';'
        }
        const std::string quoted = "fault rule '" + std::string(rule_text) + "': ";

        const auto c1 = rule_text.find(':');
        const auto c2 = c1 == std::string_view::npos ? c1 : rule_text.find(':', c1 + 1);
        if (c2 == std::string_view::npos || rule_text.find(':', c2 + 1) != std::string_view::npos) {
            throw std::invalid_argument(quoted + "expected '<operation>:<n>:<action>'");
        }
        const auto op_text = trim(rule_text.substr(0, c1));
        const auto n_text = trim(rule_text.substr(c1 + 1, c2 - c1 - 1));
        const auto action_text = trim(rule_text.substr(c2 + 1));

        FaultRule rule;
        const auto* op_it = std::find_if(kOpNames.begin(), kOpNames.end(),
                                         [&](const auto& p) { return p.second == op_text; });
        if (op_it == kOpNames.end()) {
            throw std::invalid_argument(quoted + "unknown operation '" + std::string(op_text) +
                                        "'");
        }
        rule.op = op_it->first;

        if (n_text != "*") {
            const auto n = parse_int<uint64_t>(n_text);
            if (!n || *n == 0) {
                throw std::invalid_argument(quoted + "call number must be an integer >= 1 or '*'");
            }
            rule.nth = n;
        }

        constexpr std::string_view kDelayPrefix = "delay=";
        constexpr std::string_view kBandwidthPrefix = "bandwidth=";
        if (action_text == "transient") {
            rule.action = FaultAction::transient;
        } else if (action_text == "permanent") {
            rule.action = FaultAction::permanent;
        } else if (action_text == "crash_before") {
            rule.action = FaultAction::crash_before;
        } else if (action_text == "crash_after") {
            rule.action = FaultAction::crash_after;
        } else if (action_text.starts_with(kDelayPrefix)) {
            const auto ms = parse_int<int64_t>(trim(action_text.substr(kDelayPrefix.size())));
            if (!ms) {
                throw std::invalid_argument(quoted + "delay must be a non-negative integer of ms");
            }
            rule.action = FaultAction::delay;
            rule.delay = std::chrono::milliseconds{*ms};
        } else if (action_text.starts_with(kBandwidthPrefix)) {
            const auto bps = parse_int<uint64_t>(trim(action_text.substr(kBandwidthPrefix.size())));
            if (!bps || *bps == 0) {
                throw std::invalid_argument(quoted +
                                            "bandwidth must be a positive integer of bytes/s");
            }
            if (rule.op != StoreOp::put_chunk && rule.op != StoreOp::put_index) {
                throw std::invalid_argument(quoted +
                                            "bandwidth is only for put_chunk and put_index");
            }
            if (rule.nth) {
                throw std::invalid_argument(quoted + "bandwidth needs call number '*'");
            }
            rule.action = FaultAction::bandwidth;
            rule.bytes_per_sec = *bps;
        } else {
            throw std::invalid_argument(
                quoted + "unknown action '" + std::string(action_text) +
                "' (expected transient, permanent, crash_before, crash_after, delay=<ms>, "
                "bandwidth=<bytes/s>)");
        }
        rules.push_back(rule);
    }
    return rules;
}

std::vector<FaultRule> fault_rules_from_env() {
    // NOLINTNEXTLINE(concurrency-mt-unsafe): читается один раз при запуске, до потоков
    const char* value = std::getenv(kFaultsEnvVar);
    if (value == nullptr) {
        return {};
    }
    return parse_fault_rules(value);
}

// ---------------------------------------------------------------------------------------------
// LocalDirStore

LocalDirStore::LocalDirStore(std::filesystem::path dir)
    : dir_(std::move(dir)), msgs_dir_(dir_ / "msgs"), crash_handler_([] {
          ::_exit(kCrashExitCode);  // имитация kill -9: без деструкторов и flush
      }) {
    std::filesystem::create_directories(msgs_dir_);
    {
        const std::lock_guard stats_lock(stats_mu_);
        write_stats_locked();
    }
    // next_id не меньше наибольшего существующего id + 1: защита от потерянного или
    // испорченного вручную next_id, чтобы id никогда не выдавались повторно.
    const auto existing = scan_messages(msgs_dir_);
    const int64_t min_next = existing.empty() ? 1 : existing.back() + 1;
    const std::lock_guard lock(mu_);
    std::optional<int64_t> stored;
    if (std::filesystem::exists(dir_ / "next_id")) {
        stored = load_next_id_locked();
    }
    if (!stored || *stored < min_next) {
        const auto text = format_next_id(std::max(stored.value_or(1), min_next));
        write_file_atomic(dir_ / "next_id", as_bytes(text));
    }
}

StoreStats parse_store_stats(std::string_view text) {
    StoreStats st;
    while (!text.empty()) {
        const size_t eol = text.find('\n');
        const std::string_view line = text.substr(0, eol);
        text = eol == std::string_view::npos ? std::string_view{} : text.substr(eol + 1);
        const size_t sp = line.find(' ');
        if (sp == std::string_view::npos) {
            continue;
        }
        const std::string_view name = line.substr(0, sp);
        uint64_t value = 0;
        const std::string_view digits = line.substr(sp + 1);
        const auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), value);
        if (ec != std::errc{} || ptr != digits.data() + digits.size()) {
            continue;
        }
        if (name == "read_range_calls") {
            st.read_range_calls = value;
        } else if (name == "read_range_bytes") {
            st.read_range_bytes = value;
        } else if (name == "prefetch_calls") {
            st.prefetch_calls = value;
        } else if (name == "prefetch_bytes") {
            st.prefetch_bytes = value;
        }
    }
    return st;
}

StoreStats read_store_stats(const std::filesystem::path& dir) {
    try {
        const Bytes data = read_file(dir / kStatsFileName);
        return parse_store_stats(
            std::string_view{reinterpret_cast<const char*>(data.data()), data.size()});
    } catch (const std::exception&) {
        return {};
    }
}

StoreStats LocalDirStore::stats() const {
    const std::lock_guard lock(stats_mu_);
    return stats_;
}

// Под stats_mu_. Ошибки записи не мешают работе хранилища: статистика только для тестов.
void LocalDirStore::write_stats_locked() const {
    const std::string text = "read_range_calls " + std::to_string(stats_.read_range_calls) +
                             "\nread_range_bytes " + std::to_string(stats_.read_range_bytes) +
                             "\nprefetch_calls " + std::to_string(stats_.prefetch_calls) +
                             "\nprefetch_bytes " + std::to_string(stats_.prefetch_bytes) + "\n";
    const auto tmp = dir_ / (std::string{kStatsFileName} + ".tmp");
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) {
        return;
    }
    const bool wrote = ::write(fd, text.data(), text.size()) == static_cast<ssize_t>(text.size());
    ::close(fd);
    std::error_code ec;
    if (wrote) {
        std::filesystem::rename(tmp, dir_ / kStatsFileName, ec);
    }
}

void LocalDirStore::count_read(uint64_t bytes) {
    const std::lock_guard lock(stats_mu_);
    ++stats_.read_range_calls;
    stats_.read_range_bytes += bytes;
    write_stats_locked();
}

void LocalDirStore::count_prefetch(uint64_t bytes) {
    const std::lock_guard lock(stats_mu_);
    ++stats_.prefetch_calls;
    stats_.prefetch_bytes += bytes;
    write_stats_locked();
}

void LocalDirStore::prefetch(const ChunkRef& /*ref*/, uint64_t /*offset*/, uint64_t len,
                             uint32_t /*distance*/) {
    count_prefetch(len);
}

void LocalDirStore::set_faults(std::vector<FaultRule> rules) {
    const std::lock_guard lock(mu_);
    bandwidth_.fill(0);
    for (const auto& rule : rules) {
        if (rule.action == FaultAction::bandwidth) {
            bandwidth_.at(static_cast<size_t>(rule.op)) = rule.bytes_per_sec;
        }
    }
    rules_ = std::move(rules);
}

void LocalDirStore::set_crash_handler(std::function<void()> handler) {
    const std::lock_guard lock(mu_);
    crash_handler_ = std::move(handler);
}

uint64_t LocalDirStore::call_count(StoreOp op) const {
    const std::lock_guard lock(mu_);
    return calls_.at(static_cast<size_t>(op));
}

LocalDirStore::FaultPlan LocalDirStore::plan_faults(StoreOp op) {
    const std::lock_guard lock(mu_);
    const uint64_t n = ++calls_.at(static_cast<size_t>(op));
    FaultPlan plan;
    // Совпавшие правила складываются: задержки суммируются, из ошибок берётся первая.
    for (const auto& rule : rules_) {
        if (rule.op != op || (rule.nth && *rule.nth != n)) {
            continue;
        }
        switch (rule.action) {
            case FaultAction::transient:
                plan.error = plan.error.value_or(StoreErrorKind::transient);
                break;
            case FaultAction::permanent:
                plan.error = plan.error.value_or(StoreErrorKind::permanent);
                break;
            case FaultAction::crash_before:
                plan.crash_before = true;
                break;
            case FaultAction::crash_after:
                plan.crash_after = true;
                break;
            case FaultAction::delay:
                plan.delay += rule.delay;
                break;
            case FaultAction::bandwidth:
                break;  // свойство хранилища, см. set_faults и throttle_put
        }
    }
    if (plan.crash_before || plan.crash_after) {
        plan.crash_handler = crash_handler_;
    }
    return plan;
}

template <class Body>
auto LocalDirStore::run_op(StoreOp op, Body&& body) {
    auto plan = plan_faults(op);
    const auto crash = [&plan] {
        if (plan.crash_handler) {
            plan.crash_handler();
        }
        // Обработчик вернул управление: процесс «упал», результат операции потерян.
        throw CrashInjected();
    };
    if (plan.delay.count() > 0) {
        std::this_thread::sleep_for(plan.delay);
    }
    if (plan.crash_before) {
        crash();
    }
    if (plan.error) {
        throw_store(*plan.error, "injected fault in " + std::string(op_name(op)));
    }
    // Системные ошибки наружу уходят как StoreError(permanent): вызывающий ловит только его.
    const auto guarded = [&]() -> decltype(body()) {
        try {
            return body();
        } catch (const StoreError&) {
            throw;
        } catch (const SysError& e) {
            throw_store(StoreErrorKind::permanent, e.what());
        } catch (const std::filesystem::filesystem_error& e) {
            throw_store(StoreErrorKind::permanent, e.what());
        }
    };
    if constexpr (std::is_void_v<decltype(body())>) {
        guarded();
        if (plan.crash_after) {
            crash();
        }
    } else {
        auto result = guarded();
        if (plan.crash_after) {
            crash();
        }
        return result;
    }
}

std::filesystem::path LocalDirStore::msg_path(int64_t msg_id) const {
    return msgs_dir_ / std::to_string(msg_id);
}

int64_t LocalDirStore::load_next_id_locked() const {
    const Bytes data = read_file(dir_ / "next_id");
    const auto id = parse_int<int64_t>(trim(as_text(data)));
    if (!id || *id < 1) {
        throw_store(StoreErrorKind::permanent, "corrupt next_id in " + dir_.string());
    }
    return *id;
}

std::vector<int64_t> LocalDirStore::load_pinned_locked() const {
    const auto path = dir_ / "pinned";
    if (!std::filesystem::exists(path)) {
        return {};
    }
    const Bytes data = read_file(path);
    std::string_view text = as_text(data);
    std::vector<int64_t> pins;
    while (!text.empty()) {
        const auto nl = text.find('\n');
        const auto line = trim(text.substr(0, nl));
        text = nl == std::string_view::npos ? std::string_view{} : text.substr(nl + 1);
        if (line.empty()) {
            continue;
        }
        const auto id = parse_int<int64_t>(line);
        if (!id) {
            throw_store(StoreErrorKind::permanent, "corrupt pinned in " + dir_.string());
        }
        pins.push_back(*id);
    }
    std::sort(pins.begin(), pins.end());
    pins.erase(std::unique(pins.begin(), pins.end()), pins.end());
    return pins;
}

void LocalDirStore::save_pinned_locked(const std::vector<int64_t>& pins) const {
    std::string text;
    for (const auto id : pins) {
        text += std::to_string(id);
        text += '\n';
    }
    write_file_atomic(dir_ / "pinned", as_bytes(text));
}

std::vector<int64_t> LocalDirStore::list_messages() const { return scan_messages(msgs_dir_); }

std::vector<int64_t> LocalDirStore::pinned() const {
    const std::lock_guard lock(mu_);
    return load_pinned_locked();
}

namespace {

// Открыть сообщение на чтение; нет файла — StoreError(not_found).
Fd open_message(const std::filesystem::path& path, int64_t msg_id) {
    try {
        return Fd::open(path, O_RDONLY);
    } catch (const SysError& e) {
        if (e.err() == ENOENT) {
            throw_store(StoreErrorKind::not_found, "no message " + std::to_string(msg_id));
        }
        throw;
    }
}

}  // namespace

void LocalDirStore::throttle_put(StoreOp op, uint64_t n) {
    uint64_t rate = 0;
    {
        const std::lock_guard lock(mu_);
        rate = bandwidth_.at(static_cast<size_t>(op));
    }
    if (rate == 0) {
        return;
    }
    // Резерв времени в общей полосе: каждый блок занимает n / rate секунд после предыдущего.
    // Сон идёт до конца своего резерва, поэтому параллельные заливки делят полосу.
    using Clock = std::chrono::steady_clock;
    const auto cost = std::chrono::nanoseconds{
        static_cast<int64_t>(static_cast<long double>(n) * 1e9L / static_cast<long double>(rate))};
    Clock::time_point until;
    {
        const std::lock_guard lock(bandwidth_mu_);
        bandwidth_free_ = std::max(bandwidth_free_, Clock::now()) + cost;
        until = bandwidth_free_;
    }
    std::this_thread::sleep_until(until);
}

int64_t LocalDirStore::put_message(const std::filesystem::path& src, StoreOp op) {
    // Источник открывается до выделения id: его отсутствие не должно оставлять дырку.
    const Fd in = Fd::open(src, O_RDONLY);
    int64_t id = 0;
    {
        const std::lock_guard lock(mu_);
        id = load_next_id_locked();
        write_file_atomic(dir_ / "next_id", as_bytes(format_next_id(id + 1)));
    }
    copy_file_durable(in, msg_path(id), [&](uint64_t n) { throttle_put(op, n); });
    return id;
}

ChunkRef LocalDirStore::put_chunk(const std::filesystem::path& encrypted_file) {
    return run_op(StoreOp::put_chunk, [&] {
        const int64_t id = put_message(encrypted_file, StoreOp::put_chunk);
        return ChunkRef{.msg_id = id, .remote_file_id = "local:" + std::to_string(id)};
    });
}

int64_t LocalDirStore::put_index(const std::filesystem::path& blob) {
    return run_op(StoreOp::put_index, [&] { return put_message(blob, StoreOp::put_index); });
}

void LocalDirStore::read_range(const ChunkRef& ref, uint64_t offset, std::span<std::byte> out) {
    run_op(StoreOp::read_range, [&] {
        const Fd fd = open_message(msg_path(ref.msg_id), ref.msg_id);
        const uint64_t size = file_size(fd);
        if (offset > size || out.size() > size - offset) {
            throw_store(StoreErrorKind::permanent,
                        "range [" + std::to_string(offset) + ", +" + std::to_string(out.size()) +
                            ") is beyond the end of message " + std::to_string(ref.msg_id) +
                            " (size " + std::to_string(size) + ")");
        }
        if (pread_full(fd.get(), out, offset) != out.size()) {
            throw_store(StoreErrorKind::permanent,
                        "short read of message " + std::to_string(ref.msg_id));
        }
        count_read(out.size());
    });
}

Bytes LocalDirStore::read_index(int64_t msg_id) {
    return run_op(StoreOp::read_index, [&] {
        const Fd fd = open_message(msg_path(msg_id), msg_id);
        Bytes data(static_cast<size_t>(file_size(fd)));
        data.resize(pread_full(fd.get(), data, 0));
        return data;
    });
}

void LocalDirStore::delete_messages(std::span<const int64_t> msg_ids) {
    run_op(StoreOp::delete_messages, [&] {
        const std::lock_guard lock(mu_);
        auto pins = load_pinned_locked();
        const auto old_size = pins.size();
        std::erase_if(pins, [&](int64_t id) {
            return std::find(msg_ids.begin(), msg_ids.end(), id) != msg_ids.end();
        });
        if (pins.size() != old_size) {
            save_pinned_locked(pins);
        }
        bool removed = false;
        for (const auto id : msg_ids) {
            std::error_code ec;
            removed = std::filesystem::remove(msg_path(id), ec) || removed;
            if (ec && ec != std::errc::no_such_file_or_directory) {
                throw SysError(ec.value(), "remove " + msg_path(id).string());
            }
        }
        if (removed) {
            fsync_dir(msgs_dir_);
        }
    });
}

std::vector<bool> LocalDirStore::messages_exist(std::span<const int64_t> msg_ids) {
    // Без правил сбоев и без счётчика вызовов: служебная проверка fsck не должна сдвигать номера
    // вызовов Store в тестах инъекции.
    std::vector<bool> out;
    out.reserve(msg_ids.size());
    for (const int64_t id : msg_ids) {
        std::error_code ec;
        out.push_back(std::filesystem::exists(msg_path(id), ec));
    }
    return out;
}

std::optional<HeadInfo> LocalDirStore::read_head() {
    return run_op(StoreOp::read_head, [&]() -> std::optional<HeadInfo> {
        // Под mu_, чтобы закреплённое не удалили между выбором головы и чтением заголовка;
        // читается не больше 256 байт.
        const std::lock_guard lock(mu_);
        const auto pins = load_pinned_locked();
        if (pins.empty()) {
            return std::nullopt;
        }
        HeadInfo head;
        head.msg_id = pins.back();
        const Fd fd = open_message(msg_path(head.msg_id), head.msg_id);
        head.header.resize(kHeadHeaderSize);
        head.header.resize(pread_full(fd.get(), head.header, 0));
        return head;
    });
}

void LocalDirStore::set_head(int64_t new_msg_id, std::optional<int64_t> old_msg_id) {
    run_op(StoreOp::set_head, [&] {
        const std::lock_guard lock(mu_);
        if (!std::filesystem::exists(msg_path(new_msg_id))) {
            throw_store(StoreErrorKind::not_found,
                        "cannot pin missing message " + std::to_string(new_msg_id));
        }
        auto pins = load_pinned_locked();
        if (!std::binary_search(pins.begin(), pins.end(), new_msg_id)) {
            pins.insert(std::upper_bound(pins.begin(), pins.end(), new_msg_id), new_msg_id);
            save_pinned_locked(pins);  // точка коммита
        }
        if (!old_msg_id || *old_msg_id == new_msg_id) {
            return;
        }
        // Неудачный unpin не ошибка, головой остаётся самое новое закреплённое.
        try {
            const auto it = std::lower_bound(pins.begin(), pins.end(), *old_msg_id);
            if (it != pins.end() && *it == *old_msg_id) {
                pins.erase(it);
                save_pinned_locked(pins);
            }
        } catch (const std::exception& e) {
            spdlog::warn("LocalDirStore: unpin {} failed: {}", *old_msg_id, e.what());
        }
    });
}

}  // namespace telega::store
