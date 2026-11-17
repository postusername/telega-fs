#include "meta/index_ops.h"

#include <sqlite3.h>

#include <algorithm>
#include <cerrno>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

#include "meta/errors.h"

namespace telega::meta {

FsError::FsError(int err, const std::string& what)
    : std::runtime_error(what + ": " + std::generic_category().message(err)), err_(err) {}

namespace {

constexpr std::string_view kInodeColumns =
    "ino, parent, name, kind, mode, uid, gid, size, atime_ns, mtime_ns, ctime_ns, target, "
    "LENGTH(inline_data)";

constexpr std::string_view kChunkColumns =
    "chunk_id, kind, msg_id, remote_file_id, plain_size, enc_size, content_mac, refcount, "
    "live_bytes, dead_since_gen";

std::string sql(std::string_view head, std::string_view columns, std::string_view tail) {
    std::string out;
    out.reserve(head.size() + columns.size() + tail.size());
    out.append(head).append(columns).append(tail);
    return out;
}

Inode inode_from_row(const Stmt& st) {
    Inode node;
    node.ino = st.column_int64(0);
    node.parent = st.column_opt_int64(1);
    node.name = st.column_opt_blob(2);
    node.kind = static_cast<InodeKind>(st.column_int64(3));
    node.mode = static_cast<uint32_t>(st.column_int64(4));
    node.uid = static_cast<uint32_t>(st.column_int64(5));
    node.gid = static_cast<uint32_t>(st.column_int64(6));
    node.size = st.column_int64(7);
    node.atime_ns = st.column_int64(8);
    node.mtime_ns = st.column_int64(9);
    node.ctime_ns = st.column_int64(10);
    node.target = st.column_opt_blob(11);
    node.inline_len = st.column_opt_int64(12);
    return node;
}

Chunk chunk_from_row(const Stmt& st) {
    Chunk c;
    c.chunk_id = st.column_int64(0);
    c.kind = static_cast<ChunkKind>(st.column_int64(1));
    c.msg_id = st.column_int64(2);
    if (!st.is_null(3)) {
        c.remote_file_id = st.column_text(3);
    }
    c.plain_size = st.column_int64(4);
    c.enc_size = st.column_int64(5);
    c.content_mac = st.column_blob(6);
    c.refcount = st.column_int64(7);
    c.live_bytes = st.column_int64(8);
    c.dead_since_gen = st.column_opt_int64(9);
    return c;
}

Extent extent_from_row(const Stmt& st) {
    return Extent{st.column_int64(0), st.column_int64(1), st.column_int64(2), st.column_int64(3),
                  st.column_int64(4)};
}

std::string ino_str(int64_t ino) { return "ino " + std::to_string(ino); }

bool is_unique_violation(const SqliteError& e) {
    return e.code() == SQLITE_CONSTRAINT_UNIQUE || e.code() == SQLITE_CONSTRAINT_PRIMARYKEY;
}

int64_t get_meta_int(Db& db, std::string_view key) {
    Stmt st = db.cached("SELECT value FROM meta WHERE key = ?");
    st.bind_text(1, key);
    if (!st.step() || st.column_type(0) != SQLITE_INTEGER) {
        throw IndexCorrupt("meta." + std::string(key) + " is missing or not an integer");
    }
    return st.column_int64(0);
}

void set_meta_int(Db& db, std::string_view key, int64_t value);

// Вычесть инлайн-данные inode из meta.inline_bytes перед их удалением.
void release_inline_bytes(Db& db, int64_t ino) {
    int64_t own = 0;
    {
        Stmt st = db.cached("SELECT COALESCE(LENGTH(inline_data), 0) FROM inodes WHERE ino = ?");
        st.bind(1, ino);
        if (!st.step()) {
            return;
        }
        own = st.column_int64(0);
    }
    if (own != 0) {
        set_meta_int(db, "inline_bytes", get_meta_int(db, "inline_bytes") - own);
    }
}

void set_meta_int(Db& db, std::string_view key, int64_t value) {
    Stmt st = db.cached("UPDATE meta SET value = ? WHERE key = ?");
    st.bind(1, value).bind_text(2, key).run();
    if (db.changes() != 1) {
        throw IndexCorrupt("meta." + std::string(key) + " is missing");
    }
}

// Вид inode или nullopt, если его нет.
std::optional<InodeKind> inode_kind(Db& db, int64_t ino) {
    Stmt st = db.cached("SELECT kind FROM inodes WHERE ino = ?");
    st.bind(1, ino);
    if (!st.step()) {
        return std::nullopt;
    }
    return static_cast<InodeKind>(st.column_int64(0));
}

void require_dir(Db& db, int64_t ino) {
    const auto kind = inode_kind(db, ino);
    if (!kind) {
        throw FsError(ENOENT, "parent " + ino_str(ino));
    }
    if (*kind != InodeKind::dir) {
        throw FsError(ENOTDIR, "parent " + ino_str(ino));
    }
}

// Освободить count ссылок суммарной длиной bytes на чанк.
void release_chunk(Db& db, int64_t chunk_id, int64_t count, int64_t bytes, int64_t base_gen) {
    Stmt st = db.cached(
        "UPDATE chunks SET refcount = refcount - ?1, live_bytes = live_bytes - ?2, "
        "dead_since_gen = CASE WHEN refcount - ?1 = 0 THEN ?3 ELSE dead_since_gen END "
        "WHERE chunk_id = ?4 RETURNING refcount, live_bytes");
    st.bind(1, count).bind(2, bytes).bind(3, base_gen).bind(4, chunk_id);
    if (!st.step()) {
        throw std::logic_error("release of missing chunk " + std::to_string(chunk_id));
    }
    if (st.column_int64(0) < 0 || st.column_int64(1) < 0) {
        throw std::logic_error("refcount or live_bytes below zero for chunk " +
                               std::to_string(chunk_id));
    }
}

void acquire_chunk(Db& db, int64_t chunk_id, int64_t bytes) {
    Stmt st = db.cached(
        "UPDATE chunks SET refcount = refcount + 1, live_bytes = live_bytes + ?1, "
        "dead_since_gen = NULL WHERE chunk_id = ?2");
    st.bind(1, bytes).bind(2, chunk_id).run();
    if (db.changes() != 1) {
        throw std::logic_error("acquire of missing chunk " + std::to_string(chunk_id));
    }
}

void adjust_live_bytes(Db& db, int64_t chunk_id, int64_t delta) {
    if (delta == 0) {
        return;
    }
    Stmt st = db.cached(
        "UPDATE chunks SET live_bytes = live_bytes + ?1 WHERE chunk_id = ?2 RETURNING live_bytes");
    st.bind(1, delta).bind(2, chunk_id);
    if (!st.step() || st.column_int64(0) < 0) {
        throw std::logic_error("bad live_bytes update for chunk " + std::to_string(chunk_id));
    }
}

// Проверить, что ino — обычный файл; вернуть, инлайн ли он.
bool require_file(Db& db, int64_t ino) {
    Stmt st = db.cached("SELECT kind, inline_data IS NOT NULL FROM inodes WHERE ino = ?");
    st.bind(1, ino);
    if (!st.step()) {
        throw FsError(ENOENT, ino_str(ino));
    }
    if (static_cast<InodeKind>(st.column_int64(0)) != InodeKind::file) {
        throw FsError(EINVAL, ino_str(ino) + " is not a regular file");
    }
    return st.column_int64(1) != 0;
}

}  // namespace

void validate_name(ByteSpan name) {
    if (name.empty()) {
        throw FsError(EINVAL, "empty name");
    }
    if (name.size() > kNameMax) {
        throw FsError(ENAMETOOLONG, "name of " + std::to_string(name.size()) + " bytes");
    }
    const bool bad_byte = std::any_of(name.begin(), name.end(), [](std::byte b) {
        return b == std::byte{'/'} || b == std::byte{0};
    });
    if (bad_byte) {
        throw FsError(EINVAL, "name contains '/' or NUL");
    }
    const auto dot = std::byte{'.'};
    if ((name.size() == 1 && name[0] == dot) ||
        (name.size() == 2 && name[0] == dot && name[1] == dot)) {
        throw FsError(EINVAL, "name is '.' or '..'");
    }
}

// --- inodes ---

std::optional<Inode> get_inode(Db& db, int64_t ino) {
    static const std::string kSql = sql("SELECT ", kInodeColumns, " FROM inodes WHERE ino = ?");
    Stmt st = db.cached(kSql);
    st.bind(1, ino);
    if (!st.step()) {
        return std::nullopt;
    }
    return inode_from_row(st);
}

std::optional<Inode> lookup(Db& db, int64_t parent, ByteSpan name) {
    validate_name(name);
    static const std::string kSql =
        sql("SELECT ", kInodeColumns, " FROM inodes WHERE parent = ? AND name = ?");
    Stmt st = db.cached(kSql);
    st.bind(1, parent).bind(2, name);
    if (!st.step()) {
        return std::nullopt;
    }
    return inode_from_row(st);
}

std::vector<Inode> list_children(Db& db, int64_t parent, const std::optional<Bytes>& after_name,
                                 int64_t limit) {
    static const std::string kFirst =
        sql("SELECT ", kInodeColumns, " FROM inodes WHERE parent = ?1 ORDER BY name LIMIT ?2");
    static const std::string kAfter =
        sql("SELECT ", kInodeColumns,
            " FROM inodes WHERE parent = ?1 AND name > ?3 ORDER BY name LIMIT ?2");
    Stmt st = db.cached(after_name ? kAfter : kFirst);
    st.bind(1, parent).bind(2, limit);
    if (after_name) {
        st.bind(3, ByteSpan{*after_name});
    }
    std::vector<Inode> out;
    while (st.step()) {
        out.push_back(inode_from_row(st));
    }
    return out;
}

int64_t count_children(Db& db, int64_t parent) {
    Stmt st = db.cached("SELECT COUNT(*) FROM inodes WHERE parent = ?");
    st.bind(1, parent);
    st.step();
    return st.column_int64(0);
}

int64_t count_subdirs(Db& db, int64_t parent) {
    Stmt st = db.cached("SELECT COUNT(*) FROM inodes WHERE parent = ? AND kind = 2");
    st.bind(1, parent);
    st.step();
    return st.column_int64(0);
}

int64_t insert_inode(Db& db, const NewInode& node) {
    validate_name(node.name);
    if (node.kind == InodeKind::symlink) {
        if (node.target.empty()) {
            throw FsError(EINVAL, "empty symlink target");
        }
        if (node.target.size() > kSymlinkMax) {
            throw FsError(ENAMETOOLONG, "symlink target too long");
        }
    } else if (!node.target.empty()) {
        throw std::invalid_argument("target is only for symlinks");
    }
    if (node.kind != InodeKind::file && node.kind != InodeKind::dir &&
        node.kind != InodeKind::symlink) {
        throw std::invalid_argument("bad inode kind");
    }
    require_dir(db, node.parent);

    const int64_t ino = get_meta_int(db, "next_ino");
    Stmt st = db.cached(
        "INSERT INTO inodes (ino, parent, name, kind, mode, uid, gid, size, atime_ns, mtime_ns, "
        "ctime_ns, target) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, ?9, ?10)");
    st.bind(1, ino)
        .bind(2, node.parent)
        .bind(3, ByteSpan{node.name})
        .bind(4, static_cast<int64_t>(node.kind))
        .bind(5, int64_t{node.mode & kPermMask})
        .bind(6, int64_t{node.uid})
        .bind(7, int64_t{node.gid})
        .bind(8, node.kind == InodeKind::symlink ? static_cast<int64_t>(node.target.size()) : 0)
        .bind(9, node.now_ns);
    if (node.kind == InodeKind::symlink) {
        st.bind(10, ByteSpan{node.target});
    } else {
        st.bind_null(10);
    }
    try {
        st.run();
    } catch (const SqliteError& e) {
        if (is_unique_violation(e)) {
            throw FsError(EEXIST, "create in " + ino_str(node.parent));
        }
        throw;
    }
    set_meta_int(db, "next_ino", ino + 1);
    return ino;
}

void update_attrs(Db& db, int64_t ino, const AttrUpdate& update) {
    std::string text = "UPDATE inodes SET ";
    std::vector<int64_t> values;
    auto add = [&](const char* column, std::optional<int64_t> value) {
        if (!value) {
            return;
        }
        if (!values.empty()) {
            text += ", ";
        }
        text += column;
        text += " = ?";
        values.push_back(*value);
    };
    auto widen = [](std::optional<uint32_t> v, uint32_t mask) -> std::optional<int64_t> {
        if (!v) {
            return std::nullopt;
        }
        return int64_t{*v & mask};
    };
    add("mode", widen(update.mode, kPermMask));
    add("uid", widen(update.uid, ~0U));
    add("gid", widen(update.gid, ~0U));
    add("size", update.size);
    add("atime_ns", update.atime_ns);
    add("mtime_ns", update.mtime_ns);
    add("ctime_ns", update.ctime_ns);
    if (update.size && *update.size < 0) {
        throw FsError(EINVAL, "negative size");
    }
    if (values.empty()) {
        if (!inode_kind(db, ino)) {
            throw FsError(ENOENT, ino_str(ino));
        }
        return;
    }
    text += " WHERE ino = ?";
    Stmt st = db.cached(text);
    int idx = 1;
    for (const int64_t v : values) {
        st.bind(idx++, v);
    }
    st.bind(idx, ino).run();
    if (db.changes() != 1) {
        throw FsError(ENOENT, ino_str(ino));
    }
}

void move_inode(Db& db, int64_t ino, int64_t new_parent, ByteSpan new_name) {
    if (ino == kRootIno) {
        throw FsError(EINVAL, "cannot move the root");
    }
    validate_name(new_name);
    require_dir(db, new_parent);
    Stmt st = db.cached("UPDATE inodes SET parent = ?, name = ? WHERE ino = ?");
    st.bind(1, new_parent).bind(2, new_name).bind(3, ino);
    try {
        st.run();
    } catch (const SqliteError& e) {
        if (is_unique_violation(e)) {
            throw FsError(EEXIST, "rename into " + ino_str(new_parent));
        }
        throw;
    }
    if (db.changes() != 1) {
        throw FsError(ENOENT, ino_str(ino));
    }
}

void detach_inode(Db& db, int64_t ino) {
    if (ino == kRootIno) {
        throw FsError(EINVAL, "cannot detach the root");
    }
    Stmt st = db.cached("UPDATE inodes SET parent = NULL, name = NULL WHERE ino = ?");
    st.bind(1, ino).run();
    if (db.changes() != 1) {
        throw FsError(ENOENT, ino_str(ino));
    }
}

void delete_inode(Db& db, int64_t ino, int64_t base_gen) {
    if (ino == kRootIno) {
        throw FsError(EINVAL, "cannot delete the root");
    }
    const auto kind = inode_kind(db, ino);
    if (!kind) {
        throw FsError(ENOENT, ino_str(ino));
    }
    if (*kind == InodeKind::dir && count_children(db, ino) > 0) {
        throw FsError(ENOTEMPTY, ino_str(ino));
    }
    drop_extents_from(db, ino, 0, base_gen);
    release_inline_bytes(db, ino);
    Stmt st = db.cached("DELETE FROM inodes WHERE ino = ?");
    st.bind(1, ino).run();
}

std::vector<int64_t> list_detached(Db& db) {
    Stmt st = db.cached("SELECT ino FROM inodes WHERE parent IS NULL AND ino <> 1 ORDER BY ino");
    std::vector<int64_t> out;
    while (st.step()) {
        out.push_back(st.column_int64(0));
    }
    return out;
}

// --- экстенты ---

std::optional<Extent> get_extent(Db& db, int64_t ino, int64_t idx) {
    Stmt st = db.cached(
        "SELECT ino, idx, chunk_id, chunk_off, len FROM extents WHERE ino = ? AND idx = ?");
    st.bind(1, ino).bind(2, idx);
    if (!st.step()) {
        return std::nullopt;
    }
    return extent_from_row(st);
}

std::vector<Extent> list_extents(Db& db, int64_t ino) {
    Stmt st = db.cached(
        "SELECT ino, idx, chunk_id, chunk_off, len FROM extents WHERE ino = ? ORDER BY idx");
    st.bind(1, ino);
    std::vector<Extent> out;
    while (st.step()) {
        out.push_back(extent_from_row(st));
    }
    return out;
}

void put_extent(Db& db, const Extent& extent, int64_t base_gen) {
    if (require_file(db, extent.ino)) {
        throw std::logic_error("put_extent on inline " + ino_str(extent.ino));
    }
    if (extent.idx < 0 || extent.chunk_off < 0 || extent.len <= 0) {
        throw std::invalid_argument("bad extent geometry");
    }
    {
        Stmt st = db.cached("SELECT plain_size FROM chunks WHERE chunk_id = ?");
        st.bind(1, extent.chunk_id);
        if (!st.step()) {
            throw std::invalid_argument("extent refers to missing chunk " +
                                        std::to_string(extent.chunk_id));
        }
        if (extent.chunk_off + extent.len > st.column_int64(0)) {
            throw std::invalid_argument("extent exceeds chunk " + std::to_string(extent.chunk_id));
        }
    }

    const auto old = get_extent(db, extent.ino, extent.idx);
    if (old && old->chunk_id == extent.chunk_id) {
        adjust_live_bytes(db, extent.chunk_id, extent.len - old->len);
    } else {
        if (old) {
            release_chunk(db, old->chunk_id, 1, old->len, base_gen);
        }
        acquire_chunk(db, extent.chunk_id, extent.len);
    }

    Stmt st = db.cached(
        "INSERT INTO extents (ino, idx, chunk_id, chunk_off, len) VALUES (?1, ?2, ?3, ?4, ?5) "
        "ON CONFLICT (ino, idx) DO UPDATE SET chunk_id = excluded.chunk_id, "
        "chunk_off = excluded.chunk_off, len = excluded.len");
    st.bind(1, extent.ino)
        .bind(2, extent.idx)
        .bind(3, extent.chunk_id)
        .bind(4, extent.chunk_off)
        .bind(5, extent.len)
        .run();
}

void drop_extents_from(Db& db, int64_t ino, int64_t first_idx, int64_t base_gen) {
    struct Release {
        int64_t chunk_id;
        int64_t count;
        int64_t bytes;
    };
    std::vector<Release> releases;
    {
        Stmt st = db.cached(
            "SELECT chunk_id, COUNT(*), SUM(len) FROM extents WHERE ino = ? AND idx >= ? "
            "GROUP BY chunk_id");
        st.bind(1, ino).bind(2, first_idx);
        while (st.step()) {
            releases.push_back({st.column_int64(0), st.column_int64(1), st.column_int64(2)});
        }
    }
    if (releases.empty()) {
        return;
    }
    for (const Release& r : releases) {
        release_chunk(db, r.chunk_id, r.count, r.bytes, base_gen);
    }
    Stmt st = db.cached("DELETE FROM extents WHERE ino = ? AND idx >= ?");
    st.bind(1, ino).bind(2, first_idx).run();
}

void set_extent_len(Db& db, int64_t ino, int64_t idx, int64_t len) {
    const auto old = get_extent(db, ino, idx);
    if (!old) {
        throw std::logic_error("set_extent_len: no extent " + std::to_string(idx) + " of " +
                               ino_str(ino));
    }
    if (len <= 0 || len > old->len) {
        throw std::invalid_argument("set_extent_len: len must be in 1..old len");
    }
    if (len == old->len) {
        return;
    }
    Stmt st = db.cached("UPDATE extents SET len = ? WHERE ino = ? AND idx = ?");
    st.bind(1, len).bind(2, ino).bind(3, idx).run();
    adjust_live_bytes(db, old->chunk_id, len - old->len);
}

// --- чанки ---

int64_t alloc_chunk_id(Db& db) {
    const int64_t id = get_meta_int(db, "next_chunk_id");
    set_meta_int(db, "next_chunk_id", id + 1);
    return id;
}

void insert_chunk(Db& db, const NewChunk& chunk, int64_t base_gen) {
    if (chunk.chunk_id <= 0 || chunk.plain_size < 0 || chunk.enc_size < 0 ||
        chunk.content_mac.size() != 32) {
        throw std::invalid_argument("bad chunk row");
    }
    Stmt st = db.cached(
        "INSERT INTO chunks (chunk_id, kind, msg_id, remote_file_id, plain_size, enc_size, "
        "content_mac, refcount, live_bytes, dead_since_gen) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 0, ?8)");
    st.bind(1, chunk.chunk_id).bind(2, static_cast<int64_t>(chunk.kind)).bind(3, chunk.msg_id);
    if (chunk.remote_file_id) {
        st.bind_text(4, *chunk.remote_file_id);
    } else {
        st.bind_null(4);
    }
    st.bind(5, chunk.plain_size)
        .bind(6, chunk.enc_size)
        .bind(7, ByteSpan{chunk.content_mac})
        .bind(8, base_gen)
        .run();
}

std::optional<Chunk> get_chunk(Db& db, int64_t chunk_id) {
    static const std::string kSql =
        sql("SELECT ", kChunkColumns, " FROM chunks WHERE chunk_id = ?");
    Stmt st = db.cached(kSql);
    st.bind(1, chunk_id);
    if (!st.step()) {
        return std::nullopt;
    }
    return chunk_from_row(st);
}

std::optional<Chunk> find_chunk_by_mac(Db& db, ByteSpan mac, int64_t plain_size) {
    static const std::string kSql =
        sql("SELECT ", kChunkColumns,
            " FROM chunks WHERE content_mac = ? AND plain_size = ? AND kind = 1 "
            "ORDER BY chunk_id LIMIT 1");
    Stmt st = db.cached(kSql);
    st.bind(1, mac).bind(2, plain_size);
    if (!st.step()) {
        return std::nullopt;
    }
    return chunk_from_row(st);
}

std::vector<Chunk> list_gc_candidates(Db& db, int64_t max_dead_gen, int64_t limit) {
    static const std::string kSql =
        sql("SELECT ", kChunkColumns,
            " FROM chunks WHERE dead_since_gen IS NOT NULL AND dead_since_gen <= ? "
            "AND refcount = 0 ORDER BY chunk_id LIMIT ?");
    Stmt st = db.cached(kSql);
    st.bind(1, max_dead_gen).bind(2, limit);
    std::vector<Chunk> out;
    while (st.step()) {
        out.push_back(chunk_from_row(st));
    }
    return out;
}

void delete_chunk_row(Db& db, int64_t chunk_id) {
    {
        Stmt st = db.cached("SELECT refcount FROM chunks WHERE chunk_id = ?");
        st.bind(1, chunk_id);
        if (st.step() && st.column_int64(0) != 0) {
            throw std::logic_error("delete of live chunk " + std::to_string(chunk_id));
        }
    }
    Stmt st = db.cached("DELETE FROM chunks WHERE chunk_id = ?");
    st.bind(1, chunk_id).run();
}

// --- инлайн ---

bool set_inline(Db& db, int64_t ino, ByteSpan data, int64_t base_gen) {
    require_file(db, ino);
    const auto size = static_cast<int64_t>(data.size());
    if (size > get_meta_int(db, "inline_max")) {
        return false;
    }
    int64_t own = 0;
    {
        Stmt st = db.cached("SELECT COALESCE(LENGTH(inline_data), 0) FROM inodes WHERE ino = ?");
        st.bind(1, ino);
        st.step();
        own = st.column_int64(0);
    }
    const int64_t total = inline_total(db);
    if (total - own + size > get_meta_int(db, "inline_budget")) {
        return false;
    }
    drop_extents_from(db, ino, 0, base_gen);
    Stmt st = db.cached("UPDATE inodes SET inline_data = ? WHERE ino = ?");
    st.bind(1, data).bind(2, ino).run();
    set_meta_int(db, "inline_bytes", total - own + size);
    return true;
}

void clear_inline(Db& db, int64_t ino) {
    release_inline_bytes(db, ino);
    Stmt st = db.cached("UPDATE inodes SET inline_data = NULL WHERE ino = ?");
    st.bind(1, ino).run();
    if (db.changes() != 1) {
        throw FsError(ENOENT, ino_str(ino));
    }
}

std::optional<Bytes> get_inline(Db& db, int64_t ino) {
    Stmt st = db.cached("SELECT inline_data FROM inodes WHERE ino = ?");
    st.bind(1, ino);
    if (!st.step()) {
        throw FsError(ENOENT, ino_str(ino));
    }
    return st.column_opt_blob(0);
}

int64_t inline_total(Db& db) { return get_meta_int(db, "inline_bytes"); }

// --- index_history ---

void add_history(Db& db, int64_t generation, int64_t msg_id, BlobKind kind) {
    Stmt st =
        db.cached("INSERT INTO index_history (generation, msg_id, blob_kind) VALUES (?, ?, ?)");
    st.bind(1, generation).bind(2, msg_id).bind(3, static_cast<int64_t>(kind)).run();
}

std::vector<HistoryEntry> list_history(Db& db) {
    Stmt st =
        db.cached("SELECT generation, msg_id, blob_kind FROM index_history ORDER BY generation");
    std::vector<HistoryEntry> out;
    while (st.step()) {
        out.push_back(
            {st.column_int64(0), st.column_int64(1), static_cast<BlobKind>(st.column_int64(2))});
    }
    return out;
}

void delete_history_upto(Db& db, int64_t max_generation) {
    Stmt st = db.cached("DELETE FROM index_history WHERE generation <= ?");
    st.bind(1, max_generation).run();
}

// --- meta ---

IndexParams read_params(Db& db) {
    IndexParams p;
    {
        Stmt st = db.cached("SELECT value FROM meta WHERE key = 'fs_uuid'");
        if (!st.step() || st.column_type(0) != SQLITE_BLOB) {
            throw IndexCorrupt("meta.fs_uuid is missing");
        }
        const Bytes uuid = st.column_blob(0);
        if (uuid.size() != p.fs_uuid.size()) {
            throw IndexCorrupt("meta.fs_uuid has wrong size");
        }
        std::copy(uuid.begin(), uuid.end(), p.fs_uuid.begin());
    }
    p.format_version = get_meta_int(db, "format_version");
    p.chunk_size = get_meta_int(db, "chunk_size");
    p.inline_max = get_meta_int(db, "inline_max");
    p.inline_budget = get_meta_int(db, "inline_budget");
    p.pack_threshold = get_meta_int(db, "pack_threshold");
    p.pack_target = get_meta_int(db, "pack_target");
    return p;
}

}  // namespace telega::meta
