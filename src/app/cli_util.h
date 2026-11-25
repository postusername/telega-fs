#pragma once

// Общие помощники команд CLI: ошибка команды, ввод секретов, загрузка профиля, логирование,
// текст ошибок для пользователя. Без TDLib.

#include <filesystem>
#include <iosfwd>
#include <optional>
#include <stdexcept>
#include <string>

#include "app/commands.h"
#include "app/config.h"

namespace telega::app {

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
    ~Secret();

    void set(std::string value);
    [[nodiscard]] const std::string& value() const { return value_; }
    [[nodiscard]] bool empty() const { return value_.empty(); }
    void wipe();

  private:
    std::string value_;
};

// Прочитать секрет. С терминала — без эха (termios); если stdin не терминал, одну строку из
// него (для скриптов). Приглашение пишется в stderr только для терминала.
void read_secret(const char* prompt, Secret& out);

// Прочитать строку с эхом (телефон, код, имя). Приглашение пишется в stderr всегда. Конец
// ввода — CommandError.
std::string read_line(const std::string& prompt);

// Затереть копию пароля в std::optional (best effort).
void wipe(std::optional<std::string>& s);

std::filesystem::path absolute_path(const std::filesystem::path& p);

struct Loaded {
    std::filesystem::path config_path;
    Config config;  // api_id и api_hash внутри: в логи не пишутся
    Profile profile;
    ProfilePaths paths;
};

// Конфиг и профиль. Для store = "local" требует local_dir.
Loaded load_profile(const GlobalArgs& global);

void setup_logging(bool verbose, bool color);

// Текст ошибки для пользователя (MountError, StoreError, TdException и так далее).
std::string describe_exception(const std::exception& e);

}  // namespace telega::app
