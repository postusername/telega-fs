#pragma once

// Обвязка интеграционных FUSE-тестов (SPEC 14): запускает настоящий бинарник telega-fs во
// временном окружении (XDG_*-каталоги, config.toml, keyfile) и следит, чтобы после теста не
// осталось ни монтирований, ни процессов.
//
// Среда: путь бинарника передаётся определением TELEGA_FS_BINARY (см. CMakeLists.txt).
// Окружение проверяет fuse_skip_reason(): без /dev/fuse или fusermount3 тесты
// пропускаются через GTEST_SKIP с причиной.

#include <sys/types.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace telega::itest {

namespace fs = std::filesystem;
using std::chrono::milliseconds;

// Пустая строка — FUSE доступен; иначе причина, по которой тесты надо пропустить.
std::string fuse_skip_reason();

struct Exit {
    int code = -1;   // код выхода, если процесс завершился сам
    int signal = 0;  // сигнал, если убит
    [[nodiscard]] bool ok() const { return signal == 0 && code == 0; }
};

struct RunResult {
    Exit exit;
    std::string output;  // stdout + stderr
};

// Значение nullopt — убрать переменную из окружения.
using EnvMap = std::map<std::string, std::optional<std::string>>;

// Запустить программу, дождаться (по таймауту убить) и вернуть код выхода и вывод.
RunResult run_program(const std::vector<std::string>& argv, const EnvMap& env,
                      const std::string& stdin_text, const fs::path& scratch_dir,
                      milliseconds timeout = std::chrono::minutes(3));

// Точка монтирования есть в /proc/self/mountinfo (fstype fuse.telega-fs).
bool is_mounted(const fs::path& dir);
// Строка опций монтирования из mountinfo (vfs options + super options) или пусто.
std::string mount_options_of(const fs::path& dir);

class Env;

// Смонтированная (или запускаемая) ФС. Деструктор убирает всё, что осталось: ленивое
// размонтирование и SIGKILL.
class Mount {
  public:
    Mount(const Mount&) = delete;
    Mount& operator=(const Mount&) = delete;
    ~Mount();

    [[nodiscard]] const fs::path& dir() const { return dir_; }
    [[nodiscard]] pid_t pid() const { return pid_; }
    [[nodiscard]] bool alive();

    // fusermount3 -u, затем ожидание выхода процесса. Ошибка размонтирования или таймаут —
    // исключение. Для запущенного с -f возвращает его настоящий код выхода, для фонового —
    // Exit{0}.
    Exit unmount(milliseconds timeout = std::chrono::minutes(2));
    // Процесс сам завершился (падение): дождаться и вернуть код/сигнал.
    Exit wait_exit(milliseconds timeout = std::chrono::minutes(1));
    // Послать сигнал процессу и дождаться выхода.
    Exit signal_and_wait(int sig, milliseconds timeout = std::chrono::minutes(2));
    // fusermount3 -uz и SIGKILL (для тестов, где процесс «упал» или завис).
    void kill_hard();

    // Вывод процесса, запущенного с -f (там лог идёт в stderr); для фонового — пусто.
    [[nodiscard]] std::string output() const;
    // Путь внутри ФС.
    [[nodiscard]] fs::path path(const std::string& rel) const { return dir_ / rel; }

  private:
    friend class Env;
    Mount(fs::path dir, pid_t pid, bool owned, fs::path out = {})
        : dir_(std::move(dir)), pid_(pid), owned_(owned), out_(std::move(out)) {}

    std::optional<Exit> poll_exit();
    Exit wait_gone(milliseconds timeout);

    fs::path dir_;
    pid_t pid_;
    bool owned_;  // наш дочерний процесс (mount -f): код выхода известен
    fs::path out_;
    std::optional<Exit> exit_;
    bool finished_ = false;  // размонтирована или убита: деструктор ничего не делает
};

struct ProfileSpec {
    std::string chunk_size = "1MiB";
    std::string sync_timeout = "30s";
    std::string store = "local";
    bool passphrase = false;  // без keyfile
    std::string extra;        // дополнительные строки профиля
};

// Одна «машина»: XDG_CONFIG/DATA/CACHE_HOME. new_machine() заводит новую пару data/cache
// при том же config.toml, local_dir и keyfile.
class Env {
  public:
    explicit Env(ProfileSpec spec = {});
    Env(const Env&) = delete;
    Env& operator=(const Env&) = delete;
    ~Env();

    [[nodiscard]] const fs::path& root() const { return root_; }
    [[nodiscard]] const fs::path& config_file() const { return config_file_; }
    [[nodiscard]] const fs::path& keyfile() const { return keyfile_; }
    [[nodiscard]] const fs::path& local_dir() const { return local_dir_; }
    [[nodiscard]] fs::path data_dir() const;  // <data_home>/telega-fs/main
    [[nodiscard]] fs::path log_file() const { return data_dir() / "telega-fs.log"; }
    [[nodiscard]] std::string log_text() const;
    // Каталог для служебных файлов тестов (состояние fsx, дампы) вне точек монтирования.
    [[nodiscard]] fs::path scratch() const { return root_ / "scratch"; }
    // Точка монтирования по имени (создаётся).
    [[nodiscard]] fs::path mountpoint(const std::string& name = "mnt");

    void new_machine();
    // Другой keyfile (для проверки неверного ключа): перезаписывает файл случайными байтами.
    void replace_keyfile();
    void write_config(const ProfileSpec& spec);

    [[nodiscard]] EnvMap base_env() const;
    [[nodiscard]] RunResult run(const std::vector<std::string>& args, const EnvMap& extra_env = {},
                                const std::string& stdin_text = "") const;
    // telega-fs init; исключение, если код выхода не 0.
    void init(const std::string& stdin_text = "") const;

    // mount без -f: после возврата ФС должна быть смонтирована (проверяется). Исключение
    // при ошибке. extra — дополнительные аргументы mount (--ro, -o ...).
    std::unique_ptr<Mount> mount(const std::vector<std::string>& extra = {},
                                 const EnvMap& extra_env = {}, const std::string& stdin_text = "",
                                 const std::string& dir_name = "mnt");
    // mount -f в фоне: ждёт появления монтирования; исключение, если процесс вышел раньше.
    std::unique_ptr<Mount> mount_fg(const std::vector<std::string>& extra = {},
                                    const EnvMap& extra_env = {},
                                    const std::string& dir_name = "mnt");

  private:
    fs::path root_;
    fs::path config_file_;
    fs::path keyfile_;
    fs::path local_dir_;
    fs::path data_home_;
    fs::path cache_home_;
    int machine_ = 0;
    int fg_runs_ = 0;
    ProfileSpec spec_;
};

// --- вспомогательные средства для тестов ---

// Детерминированные данные: байт на позиции i зависит только от seed и i, поэтому любой
// диапазон файла проверяется без хранения всего содержимого.
void fill_pattern(uint8_t* out, uint64_t offset, uint64_t len, uint64_t seed);
std::vector<uint8_t> pattern(uint64_t offset, uint64_t len, uint64_t seed);

// RAII-дескриптор для тестов. Все функции бросают std::system_error при ошибке, кроме
// явно возвращающих errno.
class File {
  public:
    File() = default;
    explicit File(int fd) : fd_(fd) {}
    File(const File&) = delete;
    File& operator=(const File&) = delete;
    File(File&& o) noexcept : fd_(o.fd_) { o.fd_ = -1; }
    File& operator=(File&& o) noexcept;
    ~File();

    static File open(const fs::path& path, int flags, mode_t mode = 0644);
    [[nodiscard]] int fd() const { return fd_; }
    void close();
    void pwrite_all(const void* data, size_t len, uint64_t off) const;
    void pread_exact(void* out, size_t len, uint64_t off) const;

  private:
    int fd_ = -1;
};

// Записать/прочитать файл целиком.
void write_file(const fs::path& path, const std::vector<uint8_t>& data, mode_t mode = 0644);
std::vector<uint8_t> read_file(const fs::path& path);
// Записать pattern(seed) длиной size блоками разного размера, с fsync по желанию.
void write_pattern_file(const fs::path& path, uint64_t size, uint64_t seed);
// Сверить файл с pattern(seed) побайтно; возвращает пустую строку или описание первого
// расхождения.
std::string verify_pattern_file(const fs::path& path, uint64_t size, uint64_t seed);

// errno вызова: 0 при успехе (для проверок вида EXPECT_EQ(errno_of(...), EROFS)).
template <class F>
int errno_of(F&& f) {
    errno = 0;
    const auto r = f();
    return r < 0 ? errno : 0;
}

}  // namespace telega::itest
