#include "app/commands.h"

#include <fcntl.h>
#include <sodium.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include "app/config.h"
#include "common/bytes.h"
#include "crypto/keys.h"
#include "fuse/frontend.h"
#include "meta/init.h"
#include "meta/mount.h"
#include "store/local_dir_store.h"
#include "vfs/vfs.h"

namespace telega::app {

namespace fs = std::filesystem;

namespace {

// Ошибка команды: текст для пользователя, процесс завершается с кодом 1.
class CommandError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Строка с секретом: затирается при уничтожении. В логи и вывод не попадает.
class Secret {
  public:
    Secret() = default;
    Secret(const Secret&) = delete;
    Secret& operator=(const Secret&) = delete;
    Secret(Secret&&) = delete;
    Secret& operator=(Secret&&) = delete;
    ~Secret() { wipe(); }

    void set(std::string value) {
        wipe();
        value_ = std::move(value);
    }
    [[nodiscard]] const std::string& value() const { return value_; }
    [[nodiscard]] bool empty() const { return value_.empty(); }
    void wipe() {
        if (!value_.empty()) {
            sodium_memzero(value_.data(), value_.size());
        }
        value_.clear();
    }

  private:
    std::string value_;
};

// Прочитать пароль. С терминала — без эха (termios); если stdin не терминал, одну строку
// из него (для скриптов).
void read_secret(const char* prompt, Secret& out) {
    const bool tty = ::isatty(STDIN_FILENO) != 0;
    termios saved{};
    bool echo_off = false;
    if (tty) {
        if (::tcgetattr(STDIN_FILENO, &saved) == 0) {
            termios quiet = saved;
            quiet.c_lflag &= ~static_cast<tcflag_t>(ECHO);
            echo_off = ::tcsetattr(STDIN_FILENO, TCSAFLUSH, &quiet) == 0;
        }
        std::cerr << prompt << std::flush;
    }
    std::string line;
    const bool ok = static_cast<bool>(std::getline(std::cin, line));
    if (echo_off) {
        ::tcsetattr(STDIN_FILENO, TCSAFLUSH, &saved);
    }
    if (tty) {
        std::cerr << '\n';
    }
    out.set(std::move(line));
    if (!ok && out.empty()) {
        throw CommandError("cannot read the passphrase from standard input");
    }
}

// Затереть копию пароля в std::optional (best effort).
void wipe(std::optional<std::string>& s) {
    if (s && !s->empty()) {
        sodium_memzero(s->data(), s->size());
    }
    s.reset();
}

fs::path absolute_path(const fs::path& p) { return fs::absolute(p).lexically_normal(); }

struct Loaded {
    fs::path config_path;
    Profile profile;
    ProfilePaths paths;
};

// Конфиг и профиль для обеих команд. store = "telegram" — M2.
Loaded load_profile(const GlobalArgs& global) {
    Loaded l;
    l.config_path = global.config ? absolute_path(*global.config) : default_config_path();
    const Config cfg = load_config(l.config_path);
    l.profile = cfg.profile(global.profile);
    if (l.profile.store == StoreKind::telegram) {
        throw CommandError(
            "Telegram store is implemented in M2; use store = \"local\" with "
            "local_dir in the profile for now");
    }
    if (l.profile.local_dir.empty()) {
        throw CommandError("profile '" + global.profile +
                           "' has store = \"local\" but no local_dir");
    }
    l.profile.local_dir = absolute_path(l.profile.local_dir);
    if (!l.profile.keyfile.empty()) {
        l.profile.keyfile = absolute_path(l.profile.keyfile);
    }
    l.paths = profile_paths(global.profile);
    return l;
}

void setup_logging(bool verbose, bool color) {
    std::shared_ptr<spdlog::logger> logger =
        color ? spdlog::stderr_color_mt("telega-fs") : spdlog::stderr_logger_mt("telega-fs");
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    logger->set_level(verbose ? spdlog::level::debug : spdlog::level::info);
    logger->flush_on(spdlog::level::info);
    spdlog::set_default_logger(std::move(logger));
}

// --- init ---

int init_impl(const InitArgs& args) {
    const Loaded l = load_profile(args.global);
    crypto::init();

    meta::InitOptions opts;
    const Profile& p = l.profile;
    opts.chunk_size = static_cast<int64_t>(p.chunk_size);
    opts.inline_max = static_cast<int64_t>(p.inline_max);
    opts.inline_budget = static_cast<int64_t>(p.inline_budget);
    opts.pack_threshold = static_cast<int64_t>(p.pack_threshold);
    opts.pack_target = static_cast<int64_t>(p.pack_target);
    if (args.chunk_size) {
        opts.chunk_size = static_cast<int64_t>(parse_size(*args.chunk_size));
    }
    if (opts.chunk_size <= 0) {
        throw CommandError("chunk size must be positive");
    }

    fs::path keyfile = p.keyfile;
    if (args.keyfile) {
        keyfile = absolute_path(*args.keyfile);
    }
    Secret passphrase;
    if (!keyfile.empty()) {
        opts.key.keyfile = keyfile;
    } else {
        Secret first;
        Secret second;
        read_secret("New passphrase: ", first);
        read_secret("Repeat passphrase: ", second);
        if (first.empty()) {
            throw CommandError("the passphrase must not be empty");
        }
        if (first.value() != second.value()) {
            throw CommandError("the passphrases do not match");
        }
        passphrase.set(first.value());
        opts.key.passphrase = passphrase.value();
    }

    ensure_profile_dirs(l.paths);
    const ProfileLock lock{l.paths.lock()};
    store::LocalDirStore store{p.local_dir};
    const auto wipe_on_exit = [&](void*) { wipe(opts.key.passphrase); };
    const std::unique_ptr<void, decltype(wipe_on_exit)> wiper{&opts, wipe_on_exit};
    try {
        const meta::InitResult result = meta::init_fs(store, meta::FsPaths{l.paths.data_dir}, opts);
        std::cout << "Initialized file system " << to_hex(result.fs_uuid) << " in profile '"
                  << args.global.profile << "' (chunk size " << opts.chunk_size << " bytes)\n"
                  << "Channel directory: " << p.local_dir.string() << "\n"
                  << "Data directory:    " << l.paths.data_dir.string() << '\n';
    } catch (const meta::InitError& e) {
        throw CommandError(std::string("init failed: ") + e.what());
    }
    std::cout << "\nWARNING: if you lose the "
              << (keyfile.empty() ? "passphrase" : "keyfile " + keyfile.string())
              << ", your data is lost for good: there is no recovery. Keep a copy.\n";
    if (args.keyfile && p.keyfile.empty()) {
        std::cout << "Set keyfile = \"" << keyfile.string() << "\" in profile '"
                  << args.global.profile << "' of " << l.config_path.string()
                  << " to mount with it.\n";
    }
    return 0;
}

// --- mount ---

std::string describe(const meta::MountError& e) {
    using meta::MountErrorKind;
    switch (e.kind()) {
        case MountErrorKind::wrong_password:
            return "wrong passphrase or keyfile";
        case MountErrorKind::bad_key_source:
            return std::string("key source mismatch: ") + e.what();
        case MountErrorKind::no_head:
            return std::string("the channel has no file system head: ") + e.what() +
                   " (run `telega-fs init` first)";
        case MountErrorKind::not_index_blob:
            return std::string("the channel head is not an index blob: ") + e.what();
        case MountErrorKind::corrupt:
            return std::string("the channel data is corrupted or was tampered with: ") + e.what();
        case MountErrorKind::foreign_fs:
            return std::string("the channel or local files belong to a different file system: ") +
                   e.what();
        case MountErrorKind::local_conflict:
            return std::string(
                       "the channel is newer than the local state and there are local "
                       "changes: ") +
                   e.what() +
                   "\n  use --ro to mount read-only without changes, or --discard-local to drop "
                   "the local changes";
        case MountErrorKind::rollback:
            return std::string("the channel head is older than the local state (rolled back?): ") +
                   e.what() +
                   "\n  use --ro to mount read-only, or --force-local to upload the local state "
                   "as the new head";
        case MountErrorKind::local_state:
            return std::string("the local state files are damaged or inconsistent: ") + e.what();
    }
    return e.what();
}

std::string describe_exception(const std::exception& e) {
    if (const auto* me = dynamic_cast<const meta::MountError*>(&e)) {
        return describe(*me);
    }
    if (const auto* se = dynamic_cast<const store::StoreError*>(&e)) {
        return std::string("channel storage error: ") + se->what();
    }
    if (const auto* ce = dynamic_cast<const crypto::CorruptError*>(&e)) {
        return std::string("file system data is corrupted: ") + ce->what();
    }
    return e.what();
}

// Сообщает родителю (через pipe), что ФС смонтирована или почему нет. Без fork (-f) пустой.
class Ready {
  public:
    explicit Ready(int fd) : fd_(fd) {}
    Ready(const Ready&) = delete;
    Ready& operator=(const Ready&) = delete;
    ~Ready() { close(); }

    void ok() { send("OK " + std::to_string(::getpid()) + "\n"); }
    void fail(const std::string& text) { send("ERR\n" + text + "\n"); }

  private:
    void send(const std::string& msg) {
        if (fd_ < 0) {
            return;
        }
        const char* p = msg.data();
        size_t left = msg.size();
        while (left > 0) {
            const ssize_t n = ::write(fd_, p, left);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n <= 0) {
                break;
            }
            p += n;
            left -= static_cast<size_t>(n);
        }
        close();
    }
    void close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
    int fd_;
};

struct MountContext {
    Loaded loaded;
    MountArgs args;
    meta::KeySource key;
    Secret passphrase;  // key.passphrase — копия этого значения; затираются обе
};

// Остановка и финальный коммит (SPEC 7.2, «Размонтирование»): сначала Vfs::shutdown (залить
// всё, не дольше sync_timeout), затем MountedFs::unmount (финальный коммит, clean_shutdown = 1).
// Ошибки только в лог и в код выхода. Вызывается из destroy FUSE (ядро размонтировало), из
// close() (сигнал) или при ошибке запуска; выполняется один раз.
class Teardown {
  public:
    void run(vfs::Vfs* vfs, meta::MountedFs* fs) noexcept {
        std::call_once(once_, [&] {
            spdlog::info("unmounting: flushing data and committing");
            if (vfs != nullptr) {
                try {
                    vfs->shutdown();
                } catch (const std::exception& e) {
                    spdlog::error("flush on unmount failed (data stays in the local overlay): {}",
                                  e.what());
                    failed_ = true;
                }
            }
            if (fs != nullptr) {
                try {
                    fs->unmount();
                } catch (const meta::CommitError& e) {
                    // unmount() бросает после завершения остановки (SPEC 7.2): коммитер
                    // остановлен, флаги записаны, незакоммиченное остаётся в локальном
                    // состоянии. Для вызывающего это неудача размонтирования: код выхода 1.
                    spdlog::error(
                        "final commit failed (changes stay in the local state and are "
                        "committed by the next mount): {}",
                        e.what());
                    failed_ = true;
                } catch (const std::exception& e) {
                    spdlog::error("final commit failed: {}", e.what());
                    failed_ = true;
                }
            }
            spdlog::info("unmounted");
        });
    }
    [[nodiscard]] bool failed() const { return failed_; }

  private:
    std::once_flag once_;
    bool failed_ = false;
};

// Обслуживание ФС в текущем процессе: все потоки (CommitManager, Vfs, FUSE) создаются здесь,
// поэтому до fork их нет.
int serve(MountContext& ctx, Ready& ready) {
    crypto::init();
    const Profile& p = ctx.loaded.profile;

    std::unique_ptr<store::LocalDirStore> store;
    std::unique_ptr<meta::MountedFs> mounted;
    std::unique_ptr<vfs::Vfs> vfs;
    Teardown teardown;
    meta::MountOptions mo;
    // Копии пароля живут в этом процессе (у ребёнка после fork — свои): затираются, как только
    // ключи разблокированы (mount_fs вернулся), а при ошибке — в обработчике ниже.
    const auto wipe_passphrase = [&] {
        ctx.passphrase.wipe();
        wipe(ctx.key.passphrase);
        wipe(mo.key.passphrase);
    };
    try {
        store = std::make_unique<store::LocalDirStore>(p.local_dir);
        try {
            store->set_faults(store::fault_rules_from_env());
        } catch (const std::invalid_argument& e) {
            throw CommandError(std::string("invalid ") + store::kFaultsEnvVar + ": " + e.what());
        }

        mo.key = ctx.key;
        mo.read_only = ctx.args.read_only;
        mo.discard_local = ctx.args.discard_local;
        mo.force_local = ctx.args.force_local;
        mo.commit.commit_interval = p.commit_interval;
        mo.commit.commit_every_chunks = p.commit_every_chunks;
        mo.commit.patch_take_rows = p.patch_take_rows;
        mo.commit.max_chain = p.max_chain;
        mo.commit.checkpoint_ratio = p.checkpoint_ratio;
        mo.commit.retain_generations = p.retain_generations;
        mounted = meta::mount_fs(*store, meta::FsPaths{ctx.loaded.paths.data_dir}, mo);
        wipe_passphrase();  // ключи разблокированы, пароль больше не нужен
        const meta::MountReport& rep = mounted->report();
        spdlog::info("file system opened: remote generation {}, local generation {}{}",
                     rep.remote_generation, rep.local_generation,
                     rep.unclean_shutdown ? " (previous session ended uncleanly)" : "");

        vfs::VfsOptions vo;
        vo.seal_delay = p.seal_delay;
        vo.upload_concurrency = p.upload_concurrency;
        vo.download_concurrency = p.download_concurrency;
        vo.plain_cache = p.plain_cache;
        vo.read_timeout = p.read_timeout;
        vo.sync_timeout = p.sync_timeout;
        vo.read_only = ctx.args.read_only;
        vfs = std::make_unique<vfs::Vfs>(
            mounted->meta(), mounted->state(), mounted->store(), mounted->committer(),
            mounted->keys(), vfs::VfsPaths{ctx.loaded.paths.staging(), ctx.loaded.paths.tmp()}, vo);

        std::string mount_options;
        for (const std::string& o : ctx.args.options) {
            if (!mount_options.empty()) {
                mount_options += ',';
            }
            mount_options += o;
        }
        fuse::Options fo;
        fo.mountpoint = ctx.args.mountpoint;
        fo.mount_options = mount_options;
        fo.read_only = ctx.args.read_only;
        // Frontend объявлен после vfs и mounted: уничтожается раньше них.
        fuse::Frontend frontend{*vfs, fo, [&] { teardown.run(vfs.get(), mounted.get()); }};
        frontend.mount();
        spdlog::info("mounted {} (profile '{}')", fo.mountpoint.string(), ctx.args.global.profile);
        ready.ok();
        frontend.serve();
        frontend.close();
        teardown.run(vfs.get(), mounted.get());
        return teardown.failed() ? 1 : 0;
    } catch (const std::exception& e) {
        wipe_passphrase();
        const std::string text = describe_exception(e);
        spdlog::error("mount failed: {}", text);
        teardown.run(vfs.get(), mounted.get());
        ready.fail(text);
        return 1;
    }
}

// Родитель при fork: ждать сообщения ребёнка. 0 — ФС смонтирована.
int wait_for_child(int read_fd, pid_t child, const fs::path& log_path) {
    std::string data;
    std::array<char, 512> buf{};
    for (;;) {
        const ssize_t n = ::read(read_fd, buf.data(), buf.size());
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            break;
        }
        data.append(buf.data(), static_cast<size_t>(n));
        if (data.starts_with("OK") && data.find('\n') != std::string::npos) {
            break;
        }
    }
    ::close(read_fd);
    if (data.starts_with("OK")) {
        std::cout << "telega-fs: mounted (pid " << data.substr(3, data.find('\n') - 3) << ", log "
                  << log_path.string() << ")\n";
        return 0;
    }
    if (data.starts_with("ERR\n")) {
        std::string text = data.substr(4);
        while (!text.empty() && text.back() == '\n') {
            text.pop_back();
        }
        std::cerr << "telega-fs: " << text << '\n';
        ::waitpid(child, nullptr, 0);
        return 1;
    }
    int status = 0;
    ::waitpid(child, &status, 0);
    std::cerr << "telega-fs: the background process exited unexpectedly";
    if (WIFSIGNALED(status)) {
        std::cerr << " (signal " << WTERMSIG(status) << ')';
    } else if (WIFEXITED(status)) {
        std::cerr << " (exit code " << WEXITSTATUS(status) << ')';
    }
    std::cerr << "; see " << log_path.string() << '\n';
    return 1;
}

int child_main(MountContext& ctx, int write_fd, const fs::path& log_path) {
    Ready ready{write_fd};
    ::setsid();
    ::signal(SIGPIPE, SIG_IGN);
    if (::chdir("/") != 0) {
        ready.fail("cannot chdir to /");
        return 1;
    }
    const int null_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    const int log_fd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (null_fd < 0 || log_fd < 0) {
        ready.fail("cannot open " + log_path.string() + ": " +
                   std::generic_category().message(errno));
        return 1;
    }
    // Всё, что libfuse и fusermount3 пишут в stdout/stderr, попадает в лог.
    ::dup2(null_fd, STDIN_FILENO);
    ::dup2(log_fd, STDOUT_FILENO);
    ::dup2(log_fd, STDERR_FILENO);
    ::close(null_fd);
    ::close(log_fd);
    setup_logging(ctx.args.global.verbose, false);
    return serve(ctx, ready);
}

int mount_impl(const MountArgs& args) {
    MountContext ctx;
    ctx.args = args;
    ctx.loaded = load_profile(args.global);
    ctx.args.mountpoint = absolute_path(args.mountpoint);
    std::error_code ec;
    if (!fs::is_directory(ctx.args.mountpoint, ec)) {
        throw CommandError("mount point " + ctx.args.mountpoint.string() + " is not a directory");
    }
    ensure_profile_dirs(ctx.loaded.paths);
    // Lock берётся до fork и остаётся у процесса, который обслуживает ФС: при fork дескриптор
    // (и flock на нём) наследуется ребёнком, родитель при выходе свою копию закрывает.
    const ProfileLock lock{ctx.loaded.paths.lock()};

    if (!ctx.loaded.profile.keyfile.empty()) {
        ctx.key.keyfile = ctx.loaded.profile.keyfile;
    } else {
        read_secret("Passphrase: ", ctx.passphrase);
        ctx.key.passphrase = ctx.passphrase.value();
    }

    if (args.foreground) {
        setup_logging(args.global.verbose, ::isatty(STDERR_FILENO) != 0);
        Ready none{-1};
        return serve(ctx, none);
    }

    // Фоновый режим. fork до любых потоков: ни CommitManager, ни Vfs, ни FUSE ещё не созданы.
    // Всё, что спрашивает пароль, — выше. Родитель выходит только когда ребёнок сообщил
    // «смонтировано» или ошибку, поэтому после возврата `telega-fs mount` ФС уже смонтирована.
    const fs::path log_path = ctx.loaded.paths.data_dir / "telega-fs.log";
    std::array<int, 2> pipefd{};
    if (::pipe2(pipefd.data(), O_CLOEXEC) != 0) {
        throw CommandError(std::string("pipe: ") + std::generic_category().message(errno));
    }
    std::cout.flush();
    std::cerr.flush();
    const pid_t pid = ::fork();
    if (pid < 0) {
        throw CommandError(std::string("fork: ") + std::generic_category().message(errno));
    }
    if (pid == 0) {
        ::close(pipefd[0]);
        return child_main(ctx, pipefd[1], log_path);
    }
    ::close(pipefd[1]);
    ctx.passphrase.wipe();
    wipe(ctx.key.passphrase);
    return wait_for_child(pipefd[0], pid, log_path);
}

}  // namespace

int run_init(const InitArgs& args) {
    try {
        return init_impl(args);
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        return 1;
    }
}

int run_mount(const MountArgs& args) {
    try {
        return mount_impl(args);
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        return 1;
    }
}

}  // namespace telega::app
