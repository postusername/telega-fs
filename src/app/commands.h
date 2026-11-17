#pragma once

// Команды CLI init и mount (SPEC 12.3). Разбор аргументов (CLI11) остаётся в main.cpp, здесь —
// сама работа. Эта библиотека не линкуется с TDLib (SPEC 15.4).

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

struct InitArgs {
    GlobalArgs global;
    std::optional<std::filesystem::path> keyfile;  // --keyfile F
    std::optional<std::string> chunk_size;         // --chunk-size 16M
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
int run_init(const InitArgs& args);
// Без -f возвращается после того, как ФС смонтирована (в родителе); обслуживает ФС
// дочерний процесс (см. commands.cpp). С -f возвращается после размонтирования.
int run_mount(const MountArgs& args);

}  // namespace telega::app
