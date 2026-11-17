#include "harness.h"

#include <fcntl.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers)
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>

#ifndef TELEGA_FS_BINARY
#error "TELEGA_FS_BINARY must be defined by CMake"
#endif

extern char** environ;  // NOLINT(readability-redundant-declaration)

namespace telega::itest {

namespace {

using Clock = std::chrono::steady_clock;

[[noreturn]] void fail(const std::string& what) { throw std::runtime_error(what); }

[[noreturn]] void fail_errno(const std::string& what) {
    throw std::system_error(errno, std::generic_category(), what);
}

std::string slurp(const fs::path& path) {
    const std::ifstream in(path, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

bool in_path(const std::string& program) {
    const char* path = std::getenv("PATH");  // NOLINT(concurrency-mt-unsafe)
    if (path == nullptr) {
        return false;
    }
    std::stringstream ss{path};
    std::string dir;
    while (std::getline(ss, dir, ':')) {
        if (!dir.empty() && ::access((fs::path{dir} / program).c_str(), X_OK) == 0) {
            return true;
        }
    }
    return false;
}

// Состояние процесса по /proc/<pid>/stat: нет файла или Z/X — процесса уже нет.
bool process_alive(pid_t pid) {
    std::ifstream in("/proc/" + std::to_string(pid) + "/stat");
    if (!in) {
        return false;
    }
    std::string line;
    std::getline(in, line);
    const auto rp = line.rfind(')');
    if (rp == std::string::npos || rp + 2 >= line.size()) {
        return false;
    }
    const char state = line[rp + 2];
    return state != 'Z' && state != 'X';
}

pid_t spawn(const std::vector<std::string>& argv, const EnvMap& env_changes,
            const fs::path& stdin_file, const fs::path& out_file) {
    std::vector<std::string> env_strings;
    for (char** e = environ; e != nullptr && *e != nullptr; ++e) {
        const std::string s{*e};
        const auto eq = s.find('=');
        const std::string name = s.substr(0, eq);
        if (env_changes.contains(name)) {
            continue;
        }
        env_strings.push_back(s);
    }
    for (const auto& [name, value] : env_changes) {
        if (value) {
            env_strings.push_back(name + "=" + *value);
        }
    }
    std::vector<char*> envp;
    envp.reserve(env_strings.size() + 1);
    for (std::string& s : env_strings) {
        envp.push_back(s.data());
    }
    envp.push_back(nullptr);
    std::vector<std::string> args = argv;
    std::vector<char*> argp;
    argp.reserve(args.size() + 1);
    for (std::string& s : args) {
        argp.push_back(s.data());
    }
    argp.push_back(nullptr);

    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 0, stdin_file.empty() ? "/dev/null" : stdin_file.c_str(),
                                     O_RDONLY, 0);
    posix_spawn_file_actions_addopen(&fa, 1, out_file.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600);
    posix_spawn_file_actions_adddup2(&fa, 1, 2);
    pid_t pid = 0;
    const int rc = ::posix_spawnp(&pid, argp[0], &fa, nullptr, argp.data(), envp.data());
    posix_spawn_file_actions_destroy(&fa);
    if (rc != 0) {
        throw std::system_error(rc, std::generic_category(), "posix_spawnp " + argv[0]);
    }
    return pid;
}

Exit decode(int status) {
    Exit e;
    if (WIFEXITED(status)) {
        e.code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        e.signal = WTERMSIG(status);
    }
    return e;
}

std::optional<Exit> try_wait(pid_t pid) {
    int status = 0;
    const pid_t r = ::waitpid(pid, &status, WNOHANG);
    if (r == pid) {
        return decode(status);
    }
    if (r < 0 && errno != EINTR) {
        return Exit{};  // уже собран или не наш
    }
    return std::nullopt;
}

fs::path unique_file(const fs::path& dir, const std::string& stem) {
    static std::atomic<int> counter{0};
    fs::create_directories(dir);
    return dir / (stem + "-" + std::to_string(counter.fetch_add(1)) + ".txt");
}

}  // namespace

std::string fuse_skip_reason() {
    const int fd = ::open("/dev/fuse", O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        return std::string("/dev/fuse is not available: ") + std::generic_category().message(errno);
    }
    ::close(fd);
    if (!in_path("fusermount3")) {
        return "fusermount3 is not in PATH";
    }
    return {};
}

RunResult run_program(const std::vector<std::string>& argv, const EnvMap& env,
                      const std::string& stdin_text, const fs::path& scratch_dir,
                      milliseconds timeout) {
    fs::path stdin_file;
    if (!stdin_text.empty()) {
        stdin_file = unique_file(scratch_dir, "stdin");
        std::ofstream(stdin_file, std::ios::binary) << stdin_text;
        fs::permissions(stdin_file, fs::perms::owner_read | fs::perms::owner_write);
    }
    const fs::path out_file = unique_file(scratch_dir, "out");
    const pid_t pid = spawn(argv, env, stdin_file, out_file);
    const auto deadline = Clock::now() + timeout;
    RunResult result;
    for (;;) {
        if (const auto e = try_wait(pid)) {
            result.exit = *e;
            break;
        }
        if (Clock::now() > deadline) {
            ::kill(pid, SIGKILL);
            int status = 0;
            ::waitpid(pid, &status, 0);
            result.output = slurp(out_file);
            fail("timed out running " + argv[0] + "; output so far:\n" + result.output);
        }
        std::this_thread::sleep_for(milliseconds(5));
    }
    result.output = slurp(out_file);
    if (!stdin_file.empty()) {
        std::error_code ec;
        fs::remove(stdin_file, ec);
    }
    return result;
}

namespace {

// Разбор строки mountinfo: ... mount_point(5) options(6) ... - fstype source super_options
struct MountInfo {
    std::string mountpoint;
    std::string options;
    std::string fstype;
    std::string super_options;
};

std::vector<MountInfo> read_mountinfo() {
    std::vector<MountInfo> out;
    std::ifstream in("/proc/self/mountinfo");
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss{line};
        std::vector<std::string> f;
        std::string tok;
        while (ss >> tok) {
            f.push_back(tok);
        }
        const auto dash = std::find(f.begin(), f.end(), "-");
        if (f.size() < 7 || dash == f.end() || dash + 2 >= f.end()) {
            continue;
        }
        MountInfo m;
        m.mountpoint = f[4];
        m.options = f[5];
        m.fstype = *(dash + 1);
        if (dash + 3 < f.end()) {
            m.super_options = *(dash + 3);
        }
        out.push_back(std::move(m));
    }
    return out;
}

}  // namespace

bool is_mounted(const fs::path& dir) {
    // Без canonical(): на «мёртвой» точке монтирования (ENOTCONN) она не работает. Каталоги
    // тестов лежат под уже канонизированным корнем.
    const std::string real = dir.lexically_normal().string();
    const std::vector<MountInfo> all = read_mountinfo();
    return std::ranges::any_of(all, [&](const MountInfo& m) {
        return m.mountpoint == real && m.fstype == "fuse.telega-fs";
    });
}

std::string mount_options_of(const fs::path& dir) {
    // Без canonical(): на «мёртвой» точке монтирования (ENOTCONN) она не работает. Каталоги
    // тестов лежат под уже канонизированным корнем.
    const std::string real = dir.lexically_normal().string();
    for (const MountInfo& m : read_mountinfo()) {
        if (m.mountpoint == real && m.fstype == "fuse.telega-fs") {
            return m.options + "," + m.super_options;
        }
    }
    return {};
}

// --- Mount ---

Mount::~Mount() {
    try {
        // Уже размонтированный объект не трогаем: тот же каталог мог быть смонтирован заново.
        if (!finished_ && (alive() || is_mounted(dir_))) {
            kill_hard();
        }
    } catch (...) {  // NOLINT(bugprone-empty-catch): деструктор не бросает
    }
}

std::optional<Exit> Mount::poll_exit() {
    if (exit_) {
        return exit_;
    }
    if (owned_) {
        exit_ = try_wait(pid_);
    } else if (!process_alive(pid_)) {
        exit_ = Exit{.code = 0, .signal = 0};  // чужой процесс: код выхода недоступен
    }
    return exit_;
}

std::string Mount::output() const { return out_.empty() ? std::string{} : slurp(out_); }

bool Mount::alive() { return !poll_exit().has_value(); }

Exit Mount::wait_gone(milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        if (const auto e = poll_exit()) {
            return *e;
        }
        if (Clock::now() > deadline) {
            fail("process " + std::to_string(pid_) + " did not exit within the timeout");
        }
        std::this_thread::sleep_for(milliseconds(5));
    }
}

Exit Mount::wait_exit(milliseconds timeout) { return wait_gone(timeout); }

Exit Mount::unmount(milliseconds timeout) {
    const RunResult r = run_program({"fusermount3", "-u", dir_.string()}, {}, "",
                                    dir_.parent_path() / "scratch", timeout);
    if (!r.exit.ok()) {
        fail("fusermount3 -u failed: " + r.output);
    }
    const Exit x = wait_gone(timeout);
    finished_ = true;
    return x;
}

Exit Mount::signal_and_wait(int sig, milliseconds timeout) {
    ::kill(pid_, sig);
    return wait_gone(timeout);
}

void Mount::kill_hard() {
    if (alive()) {
        ::kill(pid_, SIGKILL);
    }
    // Ленивое размонтирование отцепляет «мёртвую» точку монтирования, даже если процесс завис.
    for (int i = 0; i < 50 && is_mounted(dir_); ++i) {
        (void)run_program({"fusermount3", "-uz", dir_.string()}, {}, "",
                          dir_.parent_path() / "scratch", std::chrono::seconds(30));
        if (is_mounted(dir_)) {
            std::this_thread::sleep_for(milliseconds(100));
        }
    }
    try {
        wait_gone(std::chrono::seconds(30));
    } catch (...) {  // NOLINT(bugprone-empty-catch): процесс в D-состоянии; не мешаем очистке
    }
    finished_ = true;
}

// --- Env ---

Env::Env(ProfileSpec spec) : spec_(std::move(spec)) {
    const char* base = std::getenv("TELEGA_TEST_TMPDIR");  // NOLINT(concurrency-mt-unsafe)
    const fs::path tmp = base != nullptr ? fs::path{base} : fs::temp_directory_path();
    std::string pattern = (tmp / "tgfs-it-XXXXXX").string();
    if (::mkdtemp(pattern.data()) == nullptr) {
        fail_errno("mkdtemp " + pattern);
    }
    root_ = fs::canonical(pattern);
    fs::create_directories(scratch());
    local_dir_ = root_ / "channel";
    keyfile_ = root_ / "keyfile";
    config_file_ = root_ / "config" / "telega-fs" / "config.toml";
    replace_keyfile();
    new_machine();
    write_config(spec_);
}

Env::~Env() {
    std::error_code ec;
    fs::remove_all(root_, ec);
}

fs::path Env::data_dir() const { return data_home_ / "telega-fs" / "main"; }

std::string Env::log_text() const { return slurp(log_file()); }

fs::path Env::mountpoint(const std::string& name) {
    const fs::path p = root_ / name;
    fs::create_directories(p);
    return p;
}

void Env::new_machine() {
    ++machine_;
    data_home_ = root_ / ("data" + std::to_string(machine_));
    cache_home_ = root_ / ("cache" + std::to_string(machine_));
    fs::create_directories(data_home_);
    fs::create_directories(cache_home_);
}

void Env::replace_keyfile() {
    std::array<uint8_t, 32> key{};
    std::random_device rd;
    for (auto& b : key) {
        b = static_cast<uint8_t>(rd());
    }
    std::ofstream out(keyfile_, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(key.data()), key.size());
    out.close();
    fs::permissions(keyfile_, fs::perms::owner_read | fs::perms::owner_write);
}

void Env::write_config(const ProfileSpec& spec) {
    spec_ = spec;
    fs::create_directories(config_file_.parent_path());
    std::ostringstream cfg;
    cfg << "[profiles.main]\n"
        << "store = \"" << spec.store << "\"\n"
        << "local_dir = \"" << local_dir_.string() << "\"\n";
    if (!spec.passphrase) {
        cfg << "keyfile = \"" << keyfile_.string() << "\"\n";
    }
    cfg << "chunk_size = \"" << spec.chunk_size << "\"\n"
        << "sync_timeout = \"" << spec.sync_timeout << "\"\n"
        << spec.extra << '\n';
    std::ofstream out(config_file_, std::ios::trunc);
    out << cfg.str();
    out.close();
    fs::permissions(config_file_, fs::perms::owner_read | fs::perms::owner_write);
}

EnvMap Env::base_env() const {
    EnvMap env;
    env["XDG_CONFIG_HOME"] = (root_ / "config").string();
    env["XDG_DATA_HOME"] = data_home_.string();
    env["XDG_CACHE_HOME"] = cache_home_.string();
    env["TELEGA_FS_FAULTS"] = std::nullopt;
    env["TELEGA_API_ID"] = std::nullopt;
    env["TELEGA_API_HASH"] = std::nullopt;
    return env;
}

RunResult Env::run(const std::vector<std::string>& args, const EnvMap& extra_env,
                   const std::string& stdin_text) const {
    EnvMap env = base_env();
    for (const auto& [k, v] : extra_env) {
        env[k] = v;
    }
    std::vector<std::string> argv{TELEGA_FS_BINARY};
    argv.insert(argv.end(), args.begin(), args.end());
    return run_program(argv, env, stdin_text, scratch());
}

void Env::init(const std::string& stdin_text) const {
    const RunResult r = run({"init"}, {}, stdin_text);
    if (!r.exit.ok()) {
        fail("telega-fs init failed (code " + std::to_string(r.exit.code) + "):\n" + r.output);
    }
}

std::unique_ptr<Mount> Env::mount(const std::vector<std::string>& extra, const EnvMap& extra_env,
                                  const std::string& stdin_text, const std::string& dir_name) {
    const fs::path dir = mountpoint(dir_name);
    std::vector<std::string> args{"mount", dir.string()};
    args.insert(args.end(), extra.begin(), extra.end());
    const RunResult r = run(args, extra_env, stdin_text);
    if (!r.exit.ok()) {
        fail("telega-fs mount failed (code " + std::to_string(r.exit.code) + "):\n" + r.output +
             "\nlog:\n" + log_text());
    }
    // Вывод: "telega-fs: mounted (pid N, log ...)".
    const auto at = r.output.find("(pid ");
    if (at == std::string::npos) {
        fail("unexpected mount output: " + r.output);
    }
    const pid_t pid = static_cast<pid_t>(std::stol(r.output.substr(at + 5)));
    std::unique_ptr<Mount> m{new Mount(dir, pid, false)};
    // Главное свойство фонового режима: после возврата ФС уже смонтирована.
    if (!is_mounted(dir)) {
        fail("mount returned but " + dir.string() + " is not mounted");
    }
    return m;
}

std::unique_ptr<Mount> Env::mount_fg(const std::vector<std::string>& extra, const EnvMap& extra_env,
                                     const std::string& dir_name) {
    const fs::path dir = mountpoint(dir_name);
    EnvMap env = base_env();
    for (const auto& [k, v] : extra_env) {
        env[k] = v;
    }
    std::vector<std::string> argv{TELEGA_FS_BINARY, "mount", "-f", dir.string()};
    argv.insert(argv.end(), extra.begin(), extra.end());
    const fs::path out = scratch() / ("fg-" + std::to_string(fg_runs_++) + ".txt");
    const pid_t pid = spawn(argv, env, {}, out);
    std::unique_ptr<Mount> m{new Mount(dir, pid, true, out)};
    const auto deadline = Clock::now() + std::chrono::seconds(60);
    while (!is_mounted(dir)) {
        if (const auto e = m->poll_exit()) {
            fail("telega-fs mount -f exited early (code " + std::to_string(e->code) + "):\n" +
                 slurp(out));
        }
        if (Clock::now() > deadline) {
            fail("timed out waiting for the mount; output:\n" + slurp(out));
        }
        std::this_thread::sleep_for(milliseconds(10));
    }
    return m;
}

// --- данные и файлы ---

namespace {

uint64_t splitmix(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27U)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31U);
}

}  // namespace

void fill_pattern(uint8_t* out, uint64_t offset, uint64_t len, uint64_t seed) {
    uint64_t word_index = ~uint64_t{0};
    uint64_t word = 0;
    for (uint64_t i = 0; i < len; ++i) {
        const uint64_t pos = offset + i;
        if (pos / 8 != word_index) {
            word_index = pos / 8;
            word = splitmix(seed * 0x2545F4914F6CDD1DULL + word_index);
        }
        out[i] = static_cast<uint8_t>(word >> ((pos % 8) * 8));
    }
}

std::vector<uint8_t> pattern(uint64_t offset, uint64_t len, uint64_t seed) {
    std::vector<uint8_t> v(len);
    fill_pattern(v.data(), offset, len, seed);
    return v;
}

File& File::operator=(File&& o) noexcept {
    if (this != &o) {
        close();
        fd_ = o.fd_;
        o.fd_ = -1;
    }
    return *this;
}

File::~File() { close(); }

void File::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

File File::open(const fs::path& path, int flags, mode_t mode) {
    const int fd = ::open(path.c_str(), flags | O_CLOEXEC, mode);
    if (fd < 0) {
        fail_errno("open " + path.string());
    }
    return File{fd};
}

void File::pwrite_all(const void* data, size_t len, uint64_t off) const {
    const auto* p = static_cast<const char*>(data);
    while (len > 0) {
        const ssize_t n = ::pwrite(fd_, p, len, static_cast<off_t>(off));
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            fail_errno("pwrite");
        }
        p += n;
        off += static_cast<uint64_t>(n);
        len -= static_cast<size_t>(n);
    }
}

void File::pread_exact(void* out, size_t len, uint64_t off) const {
    auto* p = static_cast<char*>(out);
    while (len > 0) {
        const ssize_t n = ::pread(fd_, p, len, static_cast<off_t>(off));
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            fail_errno("pread");
        }
        if (n == 0) {
            fail("pread: unexpected end of file");
        }
        p += n;
        off += static_cast<uint64_t>(n);
        len -= static_cast<size_t>(n);
    }
}

void write_file(const fs::path& path, const std::vector<uint8_t>& data, mode_t mode) {
    const File f = File::open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (!data.empty()) {
        f.pwrite_all(data.data(), data.size(), 0);
    }
}

std::vector<uint8_t> read_file(const fs::path& path) {
    const File f = File::open(path, O_RDONLY);
    struct stat st {};
    if (::fstat(f.fd(), &st) != 0) {
        fail_errno("fstat " + path.string());
    }
    std::vector<uint8_t> out(static_cast<size_t>(st.st_size));
    if (!out.empty()) {
        f.pread_exact(out.data(), out.size(), 0);
    }
    return out;
}

void write_pattern_file(const fs::path& path, uint64_t size, uint64_t seed) {
    const File f = File::open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    std::mt19937_64 rng{seed};
    std::vector<uint8_t> buf;
    uint64_t off = 0;
    while (off < size) {
        // Размеры записи разные (в том числе не кратные 4096), чтобы пересекать границы чанков.
        const uint64_t n = std::min<uint64_t>(size - off, 1 + rng() % (uint64_t{192} * 1024));
        buf.resize(n);
        fill_pattern(buf.data(), off, n, seed);
        f.pwrite_all(buf.data(), n, off);
        off += n;
    }
}

std::string verify_pattern_file(const fs::path& path, uint64_t size, uint64_t seed) {
    const File f = File::open(path, O_RDONLY);
    struct stat st {};
    if (::fstat(f.fd(), &st) != 0) {
        fail_errno("fstat " + path.string());
    }
    if (static_cast<uint64_t>(st.st_size) != size) {
        return "size " + std::to_string(st.st_size) + " != expected " + std::to_string(size);
    }
    constexpr uint64_t kBlock = 1 << 20;
    std::vector<uint8_t> got(kBlock);
    std::vector<uint8_t> want(kBlock);
    for (uint64_t off = 0; off < size; off += kBlock) {
        const uint64_t n = std::min(kBlock, size - off);
        f.pread_exact(got.data(), n, off);
        fill_pattern(want.data(), off, n, seed);
        if (std::memcmp(got.data(), want.data(), n) != 0) {
            for (uint64_t i = 0; i < n; ++i) {
                if (got[i] != want[i]) {
                    return "first mismatch at offset " + std::to_string(off + i);
                }
            }
        }
    }
    return {};
}

}  // namespace telega::itest
