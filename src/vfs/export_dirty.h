#pragma once

// recover --export-dirty: выгрузка незакоммиченных данных в каталог обычными
// файлами. Работает офлайн над локальным состоянием (index.sqlite, state.sqlite, staging/) и
// ничего не меняет ни в канале, ни локально: индекс и state.sqlite открываются без миграции,
// пишется только в выходной каталог.
//
// Что выгружается: (1) файлы со строками overlay (dirty_chunks, dirty_attrs);
// (2) файлы и симлинки, у которых ctime_ns или mtime_ns не раньше committed_take_ns − 5 минут:
// залитые, но не закоммиченные данные уже без overlay, инлайн-файлы,
// запечатанные после коммита, изменения прав и имён; (3) каталоги, изменённые после этого порога,
// но без выгруженных файлов, создаются пустыми и перечисляются в empty_dirs. Если
// committed_take_ns в state.sqlite нет (старое состояние), выгружаются overlay и все инлайн-файлы,
// в отчёт идёт предупреждение. Содержимое собирается так же, как читает Vfs: позиция в
// overlay — из staging-файла (в том числе объекты открытого пакета), позиция 0
// инлайн-файла — из inline_data, остальное — из залитых чанков по экстентам (обычным и пакетным)
// через Store; без экстента позиция — дыра, нули. Размер и mtime берутся из dirty_attrs, иначе из
// inodes. Пути — из локального индекса.
//
// Если залитую часть прочитать нельзя (Store недоступен, чанк пропал или испорчен), файл всё
// равно пишется (недоступные позиции остаются нулевыми дырами), а его позиции перечисляются в
// отчёте как недостающие. Store может быть nullptr: тогда все залитые позиции недоступны.
//

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "crypto/keys.h"
#include "meta/mount.h"
#include "store/store.h"

namespace telega::vfs {

struct ExportOptions {
    // Срок чтения одного чанка через Store (повторы транзиентных ошибок внутри него). После первой
    // неудачи следующие чтения ждут не больше fast_timeout: недоступный Store не должен держать
    // выгрузку часами.
    std::chrono::milliseconds read_timeout{30'000};
    std::chrono::milliseconds fast_timeout{2'000};
};

struct ExportedFile {
    std::string path;  // путь внутри ФС, для отчёта (непечатные байты как \xNN)
    uint64_t size = 0;
    uint64_t overlay_positions = 0;  // позиций из staging-файлов
    std::vector<int64_t> missing;  // позиции, которых не удалось прочитать
    bool symlink = false;          // симлинк: size — длина цели
    [[nodiscard]] bool complete() const { return missing.empty(); }
};

struct ExportReport {
    std::vector<ExportedFile> files;
    // Каталоги, изменённые после последнего коммита и не получившие выгруженных файлов: созданы
    // пустыми.
    std::vector<std::string> empty_dirs;
    // Строки overlay, которым нечего выгружать (inode удалён, не обычный файл, файл отсоединён),
    // потерянные staging-файлы и тому подобное.
    std::vector<std::string> warnings;
    uint64_t bytes_written = 0;

    [[nodiscard]] size_t incomplete_count() const;
};

// Выгрузить в out_dir (нет — создаётся с правами 0700; есть — должен быть пустым, иначе
// std::runtime_error). Нет локального индекса или он не открывается — std::runtime_error.
ExportReport export_dirty(const meta::FsPaths& paths, store::Store* store,
                          const crypto::SubKeys& keys, const std::filesystem::path& out_dir,
                          const ExportOptions& options = {});

}  // namespace telega::vfs
