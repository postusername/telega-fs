#pragma once

// Операции над index.sqlite (SPEC 6.2, 6.5, 7.4) — свободные функции над соединением.
// Читающие функции работают на любом соединении (MetaStore::read или внутри write).
// Меняющие функции ДОЛЖНЫ вызываться внутри транзакции на писательском соединении
// (MetaStore::write): многие из них — несколько выражений, согласованных только вместе.
//
// Ошибки уровня ФС — FsError с errno, нарушение внутренних инвариантов (refcount ниже нуля,
// экстент у инлайн-файла) — std::logic_error, ошибки SQLite — SqliteError.
//
// Правила учёта чанков (SPEC 6.5, 7.4):
// - refcount — число экстентов, ссылающихся на чанк; live_bytes — сумма их len.
// - когда refcount падает до 0, dead_since_gen = base_gen (последнее закоммиченное поколение,
//   в котором чанк ещё мог быть жив); когда растёт с 0 — dead_since_gen = NULL.
// - новый чанк вставляется с refcount = 0 и dead_since_gen = base_gen: если на него так и не
//   сослался ни один экстент (например, все объекты пакета устарели за время заливки),
//   его уберёт обычная отложенная уборка.

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.h"
#include "meta/sqlite.h"

namespace telega::meta {

inline constexpr int64_t kRootIno = 1;
inline constexpr size_t kNameMax = 255;
inline constexpr size_t kSymlinkMax = 4095;
inline constexpr uint32_t kPermMask = 07777;

enum class InodeKind : uint8_t { file = 1, dir = 2, symlink = 3 };
enum class ChunkKind : uint8_t { positional = 1, pack = 2 };
enum class BlobKind : uint8_t { snapshot = 1, delta = 2 };

struct Inode {
    int64_t ino = 0;
    std::optional<int64_t> parent;  // NULL у корня и у открытых удалённых файлов
    std::optional<Bytes> name;  // NULL там же
    InodeKind kind = InodeKind::file;
    uint32_t mode = 0;  // только биты прав
    uint32_t uid = 0;
    uint32_t gid = 0;
    int64_t size = 0;  // закоммиченный размер
    int64_t atime_ns = 0;
    int64_t mtime_ns = 0;
    int64_t ctime_ns = 0;
    std::optional<Bytes> target;  // цель symlink
    // Длина inline_data, если файл инлайн. Само содержимое — get_inline (не тянется в readdir).
    std::optional<int64_t> inline_len;
};

struct NewInode {
    int64_t parent = kRootIno;
    Bytes name;
    InodeKind kind = InodeKind::file;
    uint32_t mode = 0;  // берутся только биты 07777
    uint32_t uid = 0;
    uint32_t gid = 0;
    int64_t now_ns = 0;  // atime, mtime и ctime
    Bytes target;  // только для symlink, 1..4095 байт; size = длина цели
};

// Выборочное изменение атрибутов: меняются только заданные поля.
struct AttrUpdate {
    std::optional<uint32_t> mode;  // берутся только биты 07777
    std::optional<uint32_t> uid;
    std::optional<uint32_t> gid;
    std::optional<int64_t> size;
    std::optional<int64_t> atime_ns;
    std::optional<int64_t> mtime_ns;
    std::optional<int64_t> ctime_ns;
};

struct Extent {
    int64_t ino = 0;
    int64_t idx = 0;  // позиция: байты [idx·S, (idx+1)·S)
    int64_t chunk_id = 0;
    int64_t chunk_off = 0;  // > 0 только у пакетов
    int64_t len = 0;  // валидные байты начиная с chunk_off, 1..plain_size - chunk_off
};

struct Chunk {
    int64_t chunk_id = 0;
    ChunkKind kind = ChunkKind::positional;
    int64_t msg_id = 0;
    std::optional<std::string> remote_file_id;
    int64_t plain_size = 0;
    int64_t enc_size = 0;
    Bytes content_mac;
    int64_t refcount = 0;
    int64_t live_bytes = 0;
    std::optional<int64_t> dead_since_gen;
};

// Новый чанк: chunk_id берётся из alloc_chunk_id, refcount и live_bytes начинаются с 0.
struct NewChunk {
    int64_t chunk_id = 0;
    ChunkKind kind = ChunkKind::positional;
    int64_t msg_id = 0;
    std::optional<std::string> remote_file_id;
    int64_t plain_size = 0;
    int64_t enc_size = 0;
    Bytes content_mac;
};

struct HistoryEntry {
    int64_t generation = 0;
    int64_t msg_id = 0;
    BlobKind kind = BlobKind::snapshot;
};

// Параметры ФС из таблицы meta, неизменные после init.
struct IndexParams {
    FsUuid fs_uuid{};
    int64_t format_version = 0;
    int64_t chunk_size = 0;
    int64_t inline_max = 0;
    int64_t inline_budget = 0;
    int64_t pack_threshold = 0;
    int64_t pack_target = 0;
};

// Проверить имя записи каталога: 1..255 байт, без '/' и '\0', не "." и "..".
// Иначе FsError(ENAMETOOLONG) для длинного, FsError(EINVAL) для остальных.
void validate_name(ByteSpan name);

// --- inodes ---

std::optional<Inode> get_inode(Db& db, int64_t ino);
// Запись каталога parent с именем name. Имя проверяется (validate_name).
std::optional<Inode> lookup(Db& db, int64_t parent, ByteSpan name);
// До limit записей каталога по возрастанию name (побайтово), строго после after_name.
std::vector<Inode> list_children(Db& db, int64_t parent, const std::optional<Bytes>& after_name,
                                 int64_t limit);
int64_t count_children(Db& db, int64_t parent);
// Число подкаталогов: nlink каталога = 2 + count_subdirs.
int64_t count_subdirs(Db& db, int64_t parent);

// Создать inode, ino берётся из meta.next_ino. parent должен существовать (ENOENT) и быть
// каталогом (ENOTDIR); имя занято — EEXIST; плохое имя или цель symlink — EINVAL/ENAMETOOLONG.
int64_t insert_inode(Db& db, const NewInode& node);
// Изменить заданные атрибуты. Нет inode — ENOENT. Экстенты не трогает: truncate — это
// drop_extents_from + set_extent_len + size, всё в одной транзакции.
void update_attrs(Db& db, int64_t ino, const AttrUpdate& update);
// Переместить запись (rename). Целевое имя занято — EEXIST (замену делает вызывающий,
// удалив цель в той же транзакции). Корень двигать нельзя — EINVAL. Проверку «каталог
// внутрь своего поддерева» делает вызывающий.
void move_inode(Db& db, int64_t ino, int64_t new_parent, ByteSpan new_name);
// Убрать запись из каталога, оставив inode (открытый удалённый файл): parent = name = NULL.
void detach_inode(Db& db, int64_t ino);
// Удалить inode: освободить экстенты (refcount--, live_bytes -= len), удалить строку.
// Непустой каталог — ENOTEMPTY, корень — EINVAL, нет inode — ENOENT.
void delete_inode(Db& db, int64_t ino, int64_t base_gen);
// inode с parent IS NULL, кроме корня (SPEC 7.2, шаг 8).
std::vector<int64_t> list_detached(Db& db);

// --- экстенты ---

std::optional<Extent> get_extent(Db& db, int64_t ino, int64_t idx);
// Экстенты файла по возрастанию idx.
std::vector<Extent> list_extents(Db& db, int64_t ino);
// Поставить экстент (ino, idx), заменив прежний. У прежнего чанка refcount-- и
// live_bytes -= old.len, у нового refcount++ и live_bytes += len; тот же чанк — только
// live_bytes на разницу. Нужен обычный файл без инлайн-данных (иначе logic_error), чанк
// должен существовать, 0 < len и chunk_off + len ≤ plain_size (иначе invalid_argument).
void put_extent(Db& db, const Extent& extent, int64_t base_gen);
// Удалить экстенты с idx ≥ first_idx, освободив их чанки.
void drop_extents_from(Db& db, int64_t ino, int64_t first_idx, int64_t base_gen);
// Уменьшить len экстента (частичный последний при truncate), live_bytes -= разница.
// Увеличивать нельзя: хвост после обратного расширения обязан читаться нулями (SPEC 6.2).
// Нет экстента — logic_error, len вне 1..old.len — invalid_argument.
void set_extent_len(Db& db, int64_t ino, int64_t idx, int64_t len);

// --- чанки ---

// Выдать номер чанка: meta.next_chunk_id++.
int64_t alloc_chunk_id(Db& db);
// Вставить строку чанка с refcount = 0, live_bytes = 0, dead_since_gen = base_gen.
void insert_chunk(Db& db, const NewChunk& chunk, int64_t base_gen);
std::optional<Chunk> get_chunk(Db& db, int64_t chunk_id);
// Позиционный чанк с тем же content_mac и plain_size (дедупликация, SPEC 6.5: только kind 1).
std::optional<Chunk> find_chunk_by_mac(Db& db, ByteSpan mac, int64_t plain_size);
// Мёртвые чанки (refcount = 0) с dead_since_gen ≤ max_dead_gen, по chunk_id, не больше limit.
std::vector<Chunk> list_gc_candidates(Db& db, int64_t max_dead_gen, int64_t limit = INT64_MAX);
// Удалить строку чанка. Живой чанк (refcount > 0) — logic_error.
void delete_chunk_row(Db& db, int64_t chunk_id);

// --- инлайн (SPEC 6.5) ---

// Сделать файл инлайн: если data.size() ≤ inline_max и inline_total() без старых данных
// этого файла плюс data.size() ≤ inline_budget, удалить все экстенты (освобождая чанки),
// записать inline_data и вернуть true. Иначе ничего не менять и вернуть false.
// Размер (size) не трогает. Не обычный файл — EINVAL, нет inode — ENOENT.
bool set_inline(Db& db, int64_t ino, ByteSpan data, int64_t base_gen);
// inline_data = NULL.
void clear_inline(Db& db, int64_t ino);
// Содержимое инлайн-файла или nullopt, если файл не инлайн.
std::optional<Bytes> get_inline(Db& db, int64_t ino);
// SUM(LENGTH(inline_data)), хранится счётчиком meta.inline_bytes (O(1)).
int64_t inline_total(Db& db);

// --- index_history ---

void add_history(Db& db, int64_t generation, int64_t msg_id, BlobKind kind);
// По возрастанию generation.
std::vector<HistoryEntry> list_history(Db& db);
// Удалить записи с generation ≤ max_generation (уборка старых цепочек, SPEC 7.4).
void delete_history_upto(Db& db, int64_t max_generation);

// --- meta ---

IndexParams read_params(Db& db);

}  // namespace telega::meta
