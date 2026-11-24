#pragma once

// fsck (SPEC 12.3): проверка согласованности индекса, существования сообщений чанков и, по
// запросу, тегов всех чанков; исправление только самого индекса.
//
// Проверка индекса (SPEC 5, 6.2, 6.5, 7.4):
// - PRAGMA integrity_check и foreign_key_check, ключи meta, корень, счётчики next_ino и
//   next_chunk_id, inline_bytes и бюджет инлайна;
// - refcount и live_bytes каждого чанка против его экстентов, dead_since_gen при refcount = 0
//   (и NULL при refcount > 0);
// - экстенты на несуществующие чанки, границы экстента (chunk_off, len, plain_size), chunk_off > 0
//   только у пакетов, пакетные экстенты только с idx = 0, пересечение объектов пакета;
// - форма строки чанка: вид, размеры (enc_size = размер зашифрованного файла по plain_size),
//   длина content_mac.
// Существование: msg_id всех чанков и элементов текущей цепочки (state.chain) через
// Store::messages_exist пачками по 100. С verify_data каждый чанк ещё скачивается и
// расшифровывается целиком: проверяются заголовок и теги сегментов (SPEC 5).
//
// Чанк с обнулённым content_mac (так его помечает fsck --repair) уже известный: сообщение и теги
// такого чанка не проверяются заново, а затронутые им файлы выводятся как примечание (SPEC 12.3:
// отдельной пометки в схеме нет, чтение таких диапазонов и так даёт EIO).
//
// fsck_repair меняет только index.sqlite одной транзакцией MetaStore::write и ничего не делает
// в канале: пересчитывает refcount, live_bytes, dead_since_gen, счётчики и inline_bytes, а у
// подтверждённо битых или пропавших чанков обнуляет content_mac (32 нулевых байта), чтобы новая
// запись на них не дедуплицировалась. Исправления уходят обычным коммитом (его делает вызывающий).
// Чанк, который не удалось проверить (ошибка Store), не помечается.

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "crypto/keys.h"
#include "meta/index_db.h"
#include "meta/state_db.h"
#include "store/store.h"

namespace telega::meta {

enum class FsckKind : uint8_t {
    sqlite_integrity,  // PRAGMA integrity_check
    foreign_key,       // PRAGMA foreign_key_check
    meta_params,       // нет ключей meta или неверный format_version
    root_missing,      // нет корня
    counter_behind,  // next_ino или next_chunk_id не больше выданных номеров
    inline_bytes,          // meta.inline_bytes не равен SUM(LENGTH(inline_data))
    inline_budget,         // SUM(LENGTH(inline_data)) больше inline_budget
    inline_with_extents,   // у инлайн-файла есть экстенты
    refcount,              // refcount не равен числу экстентов
    live_bytes,            // live_bytes не равен сумме len экстентов
    dead_since_gen,        // refcount = 0 без dead_since_gen или refcount > 0 с ним
    extent_missing_chunk,  // экстент на несуществующий чанк
    extent_bounds,         // len, chunk_off или chunk_off + len вне чанка
    extent_chunk_off,      // chunk_off > 0 у позиционного чанка
    pack_extent_idx,       // экстент пакета не на idx = 0
    pack_overlap,          // объекты пакета пересекаются
    chunk_shape,           // вид, размеры или content_mac чанка неверны
    message_missing,       // сообщения чанка нет в канале
    chain_message_missing,  // нет сообщения элемента текущей цепочки
    chunk_corrupt,          // verify_data: заголовок или тег не сошлись
    chunk_unverified,  // verify_data: чанк не удалось прочитать (ошибка Store)
    store_unavailable,  // проверка существования не состоялась (ошибка Store)
};

[[nodiscard]] std::string_view fsck_kind_name(FsckKind kind);

struct FsckIssue {
    FsckKind kind = FsckKind::sqlite_integrity;
    // true — fsck --repair это исправляет (пересчёт или обнуление content_mac). Остальное
    // требует решения человека и остаётся в отчёте.
    bool repairable = false;
    std::optional<int64_t> chunk_id;
    std::string detail;
};

struct FsckOptions {
    bool verify_data = false;
    // false — только индекс: сообщения и теги не проверяются, Store не вызывается (повторная
    // проверка после fsck --repair).
    bool check_messages = true;
    // Пауза между пачками getMessages (read_pause, SPEC 10.2); 0 — без паузы.
    std::chrono::milliseconds batch_pause{0};
    // Прогресс долгих этапов (строки без перевода строки в конце).
    std::function<void(const std::string&)> progress;
};

struct FsckReport {
    std::vector<FsckIssue> issues;
    uint64_t chunks = 0;
    uint64_t extents = 0;
    uint64_t messages_checked = 0;
    uint64_t chunks_verified = 0;
    // Подтверждённо битые или пропавшие чанки: fsck_repair обнулит их content_mac.
    std::set<int64_t> bad_chunks;
    // Чанки с уже обнулённым content_mac (прошлый fsck --repair): не проверяются заново.
    std::set<int64_t> known_bad_chunks;
    bool message_check_incomplete = false;  // проверка существования прервана ошибкой Store

    [[nodiscard]] bool clean() const { return issues.empty(); }
    [[nodiscard]] bool has_unrepairable() const;
};

// Проверить индекс (и сообщения). state — для элементов цепочки; nullptr — цепочку не проверять.
// Ошибка чтения индекса — SqliteError.
FsckReport fsck_check(MetaStore& meta, StateStore* state, store::Store& store,
                      const crypto::SubKeys& keys, const FsckOptions& options = {});

struct RepairResult {
    uint64_t refcounts_fixed = 0;
    uint64_t live_bytes_fixed = 0;
    uint64_t dead_since_fixed = 0;
    uint64_t counters_fixed = 0;
    uint64_t macs_zeroed = 0;
    bool inline_bytes_fixed = false;
    // Пути файлов, чьи данные лежат в битых или пропавших чанках (по возрастанию).
    std::vector<std::string> affected_files;

    [[nodiscard]] bool changed() const {
        return refcounts_fixed + live_bytes_fixed + dead_since_fixed + counters_fixed +
                       macs_zeroed >
                   0 ||
               inline_bytes_fixed;
    }
};

// Исправить индекс по отчёту (см. шапку). base_generation — поколение для dead_since_gen чанков,
// у которых после пересчёта refcount = 0.
RepairResult fsck_repair(MetaStore& meta, const FsckReport& report, int64_t base_generation);

// Пути файлов, экстенты которых ссылаются на чанки из chunk_ids, по возрастанию. Открытый
// удалённый файл печатается как «<unlinked inode N>».
std::vector<std::string> files_using_chunks(MetaStore& meta, const std::set<int64_t>& chunk_ids);

}  // namespace telega::meta
