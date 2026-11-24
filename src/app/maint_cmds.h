#pragma once

// Обслуживающие команды без FUSE: fsck и recover --export-dirty (SPEC 12.3). Работают под lock
// профиля и при смонтированной ФС отказываются.

#include <filesystem>

#include "app/commands.h"

namespace telega::app {

struct FsckArgs {
    GlobalArgs global;
    bool repair = false;  // --repair: исправить индекс и закоммитить
    bool verify_data = false;  // --verify-data: скачать и проверить теги всех чанков
};

struct RecoverArgs {
    GlobalArgs global;
    std::filesystem::path export_dir;  // --export-dirty DIR
};

// Коды выхода: 0 — всё в порядке; 1 — ошибка команды; 2 — найдены проблемы (fsck) или часть
// файлов выгружена неполностью (recover). Отчёт — в stdout, ошибки — в stderr.
int run_fsck(const FsckArgs& args);
int run_recover(const RecoverArgs& args);

}  // namespace telega::app
