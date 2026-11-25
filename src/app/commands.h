#pragma once

// Команды CLI login, logout, init и mount. Разбор аргументов (CLI11) остаётся в
// main.cpp, здесь — сама работа. Эта библиотека не линкуется с TDLib: транспорт
// TDLib приходит из main через set_transport_factory (app/tg_session.h).

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace telega::app {

struct GlobalArgs {
    std::optional<std::filesystem::path> config;  // --config FILE
    std::string profile = "main";                 // -p/--profile
    bool verbose = false;                         // -v: уровень debug
};

struct LoginArgs {
    GlobalArgs global;
    bool bot = false;  // --bot: токен бота вместо телефона
    bool qr = false;   // --qr: вход пользователя по QR-коду
    bool test_dc = false;  // --test-dc: тестовые серверы Telegram (записывается в профиль)
};

struct LogoutArgs {
    GlobalArgs global;
};

struct InitArgs {
    GlobalArgs global;
    std::optional<std::filesystem::path> keyfile;  // --keyfile F
    std::optional<std::string> chunk_size;         // --chunk-size 16M
    std::optional<int64_t> chat;  // --chat ID: пустой существующий канал
    std::optional<std::string> title;  // --title T: создать канал (только пользователь)
};

struct MountArgs {
    GlobalArgs global;
    std::filesystem::path mountpoint;
    bool foreground = false;  // -f
    bool read_only = false;   // --ro
    bool discard_local = false;
    bool force_local = false;
    std::vector<std::string> options;  // -o OPTS (можно несколько раз)
};

// Возвращают код выхода процесса: 0 — успех. Сообщения об ошибках — в stderr.
int run_login(const LoginArgs& args);
int run_logout(const LogoutArgs& args);
int run_init(const InitArgs& args);
// Без -f возвращается после того, как ФС смонтирована (в родителе); обслуживает ФС
// дочерний процесс (см. commands.cpp). С -f возвращается после размонтирования.
int run_mount(const MountArgs& args);

}  // namespace telega::app
