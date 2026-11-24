#pragma once

// Состояние ФС для `telega-fs status` и команды `status` управляющего сокета (SPEC 12.3, 12.4).
//
// Источник один и тот же в обоих режимах: state.sqlite, открытый ТОЛЬКО на чтение отдельным
// соединением (WAL позволяет читать рядом с писателем смонтированного процесса и не требует
// `lock`). Смонтированный процесс дополняет картину живыми данными Vfs и CommitManager
// (очередь заливки, ошибки): без монтирования их нет, и ошибки не переживают процесс.

#include <nlohmann/json.hpp>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "meta/commit.h"
#include "vfs/vfs.h"

namespace telega::app {

struct FsStatus {
    bool mounted = false;
    bool read_only = false;   // только при mounted: --ro или CAS-конфликт
    std::string fs_uuid;      // hex; пусто, если ключа в state.sqlite нет
    uint64_t generation = 0;  // base_generation: последний успешный коммит
    int64_t head_msg_id = 0;
    size_t chain_length = 0;  // элементов цепочки: снимок и его дельты
    int64_t chain_bytes = 0;  // сумма размеров блобов цепочки
    bool index_dirty = false;  // индекс менялся после последнего коммита
    bool force_snapshot = false;  // следующий коммит — снимок
    bool clean_shutdown = true;  // false: прошлый сеанс завершился нечисто (или идёт монтирование)
    size_t pending_patchsets = 0;  // забраны из сессии, ещё не закоммичены
    // Overlay: грязные чанки (SPEC 6.4, 8.3).
    size_t dirty_chunks = 0;
    size_t dirty_inodes = 0;
    uint64_t dirty_bytes = 0;  // сумма длин staging-файлов грязных чанков (SPEC 8.3)
    size_t open_pack_objects = 0;
    int64_t open_pack_bytes = 0;
    // Очередь заливки. Без монтирования заливки нет: всё грязное ждёт монтирования
    // (upload_queued = dirty_chunks, остальное 0).
    size_t upload_queued = 0;
    size_t upload_active = 0;
    size_t upload_failed = 0;
    // Только при mounted.
    uint64_t uploaded_chunks = 0;  // залито с монтирования
    uint64_t deduped_chunks = 0;  // привязано к готовым чанкам без заливки (SPEC 8.1)
    uint64_t staging_used = 0;
    uint64_t staging_limit = 0;
    uint64_t commits = 0;  // коммитов с монтирования
    // Последние ошибки заливки и коммита (только при mounted: на диске они не хранятся).
    std::vector<std::string> errors;
};

// Прочитать state.sqlite без записи и без lock. Файла нет — std::runtime_error с понятным
// текстом; повреждённый или чужой — SqliteError.
FsStatus read_state_status(const std::filesystem::path& state_db,
                           const std::filesystem::path& staging_dir);

// Дополнить статус живыми данными смонтированного процесса.
void add_live_status(FsStatus& status, const vfs::VfsStatus& vfs, const meta::CommitStats& commit,
                     bool read_only);

[[nodiscard]] nlohmann::json status_to_json(const FsStatus& status);
// Обратное преобразование для клиента сокета; отсутствующие поля остаются по умолчанию.
[[nodiscard]] FsStatus status_from_json(const nlohmann::json& j);
// Человекочитаемый вывод для `telega-fs status`.
[[nodiscard]] std::string format_status(const FsStatus& status, const std::string& profile);

// «12.3 MiB»: двоичные приставки, одна цифра после точки.
[[nodiscard]] std::string format_bytes(uint64_t bytes);

}  // namespace telega::app
