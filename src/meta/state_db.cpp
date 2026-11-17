#include "meta/state_db.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>

#include "common/files.h"

namespace telega::meta {

namespace {

// Схема ровно как в SPEC 6.4.
constexpr std::string_view kSchema = R"sql(
CREATE TABLE state (key TEXT PRIMARY KEY, value BLOB NOT NULL);

CREATE TABLE pending_patchsets (
  seq INTEGER PRIMARY KEY,
  patchset BLOB NOT NULL
);

CREATE TABLE chain (
  pos INTEGER PRIMARY KEY,
  msg_id INTEGER NOT NULL,
  blob_size INTEGER NOT NULL
);

CREATE TABLE dirty_chunks (
  ino INTEGER NOT NULL, idx INTEGER NOT NULL,
  path TEXT NOT NULL,
  version INTEGER NOT NULL,
  last_write_ns INTEGER NOT NULL,
  PRIMARY KEY (ino, idx)
);

CREATE TABLE dirty_attrs (
  ino INTEGER PRIMARY KEY, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL
);

CREATE TABLE upload_journal (
  chunk_id INTEGER PRIMARY KEY,
  msg_id INTEGER,
  started_ns INTEGER NOT NULL
);

CREATE TABLE open_pack (
  ino INTEGER PRIMARY KEY,
  version INTEGER NOT NULL,
  off INTEGER NOT NULL,
  len INTEGER NOT NULL
);
)sql";

std::filesystem::path with_suffix(const std::filesystem::path& p, std::string_view suffix) {
    std::filesystem::path out = p;
    out += suffix;
    return out;
}

DirtyChunk dirty_chunk_from_row(const Stmt& st) {
    return DirtyChunk{st.column_int64(0), st.column_int64(1), st.column_text(2), st.column_int64(3),
                      st.column_int64(4)};
}

Db open_existing(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        throw std::runtime_error("StateStore: " + path.string() + " does not exist");
    }
    return Db::open(path);
}

}  // namespace

void StateStore::create(const std::filesystem::path& path) {
    if (std::filesystem::exists(path)) {
        throw std::runtime_error("StateStore::create: " + path.string() + " already exists");
    }
    const auto tmp = with_suffix(path, ".tmp-create");
    auto cleanup = [&] {
        std::error_code ignored;
        for (const char* suffix : {"", "-wal", "-shm", "-journal"}) {
            std::filesystem::remove(with_suffix(tmp, suffix), ignored);
        }
    };
    cleanup();
    try {
        {
            OpenOptions options;
            options.create = true;
            Db db = Db::open(tmp, options);
            Transaction tx{db};
            db.exec(kSchema);
            tx.commit();
        }
        {
            const Fd fd = Fd::open(tmp, O_RDONLY);
            if (::fsync(fd.get()) != 0) {
                throw SysError(errno, "fsync " + tmp.string());
            }
        }
        std::filesystem::rename(tmp, path);
        fsync_dir(path.has_parent_path() ? path.parent_path() : std::filesystem::path{"."});
    } catch (...) {
        cleanup();
        throw;
    }
}

StateStore::StateStore(const std::filesystem::path& path) : db_(open_existing(path)) {}

StateStore::~StateStore() = default;

void StateStore::transaction_impl(const std::function<void()>& fn) {
    const std::lock_guard lock(mutex_);
    Transaction tx{db_};
    fn();
    tx.commit();
}

// --- state ---

std::optional<int64_t> StateStore::get_int(std::string_view key) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("SELECT value FROM state WHERE key = ?");
    st.bind_text(1, key);
    if (!st.step()) {
        return std::nullopt;
    }
    return st.column_int64(0);
}

int64_t StateStore::get_int_or(std::string_view key, int64_t fallback) {
    return get_int(key).value_or(fallback);
}

void StateStore::set_int(std::string_view key, int64_t value) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached(
        "INSERT INTO state (key, value) VALUES (?1, ?2) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value");
    st.bind_text(1, key).bind(2, value).run();
}

std::optional<Bytes> StateStore::get_blob(std::string_view key) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("SELECT value FROM state WHERE key = ?");
    st.bind_text(1, key);
    if (!st.step()) {
        return std::nullopt;
    }
    return st.column_blob(0);
}

void StateStore::set_blob(std::string_view key, ByteSpan value) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached(
        "INSERT INTO state (key, value) VALUES (?1, ?2) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value");
    st.bind_text(1, key).bind(2, value).run();
}

void StateStore::erase(std::string_view key) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("DELETE FROM state WHERE key = ?");
    st.bind_text(1, key).run();
}

// --- pending_patchsets ---

int64_t StateStore::append_pending(ByteSpan patchset) {
    const std::lock_guard lock(mutex_);
    // seq — rowid: MAX(seq) + 1, то есть больше любого лежащего в таблице патча.
    Stmt st = db_.cached("INSERT INTO pending_patchsets (patchset) VALUES (?) RETURNING seq");
    st.bind(1, patchset);
    st.step();
    const int64_t seq = st.column_int64(0);
    st.run();
    return seq;
}

std::vector<PendingPatchset> StateStore::list_pending() {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("SELECT seq, patchset FROM pending_patchsets ORDER BY seq");
    std::vector<PendingPatchset> out;
    while (st.step()) {
        out.push_back({st.column_int64(0), st.column_blob(1)});
    }
    return out;
}

void StateStore::delete_pending_upto(int64_t max_seq) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("DELETE FROM pending_patchsets WHERE seq <= ?");
    st.bind(1, max_seq).run();
}

int64_t StateStore::max_pending_seq() {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("SELECT COALESCE(MAX(seq), 0) FROM pending_patchsets");
    st.step();
    const int64_t seq = st.column_int64(0);
    st.run();
    return seq;
}

void StateStore::clear_pending() {
    const std::lock_guard lock(mutex_);
    db_.exec("DELETE FROM pending_patchsets");
}

// --- chain ---

std::vector<ChainElement> StateStore::list_chain() {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("SELECT pos, msg_id, blob_size FROM chain ORDER BY pos");
    std::vector<ChainElement> out;
    while (st.step()) {
        out.push_back({st.column_int64(0), st.column_int64(1), st.column_int64(2)});
    }
    return out;
}

int64_t StateStore::append_chain(int64_t msg_id, int64_t blob_size) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached(
        "INSERT INTO chain (pos, msg_id, blob_size) "
        "SELECT MAX(pos) + 1, ?1, ?2 FROM chain HAVING COUNT(*) > 0 RETURNING pos");
    st.bind(1, msg_id).bind(2, blob_size);
    if (!st.step()) {
        throw std::logic_error("append_chain: the chain has no snapshot");
    }
    const int64_t pos = st.column_int64(0);
    st.run();
    return pos;
}

void StateStore::reset_chain_to_snapshot(int64_t msg_id, int64_t blob_size) {
    const std::lock_guard lock(mutex_);
    Transaction tx{db_};
    db_.exec("DELETE FROM chain");
    Stmt st = db_.cached("INSERT INTO chain (pos, msg_id, blob_size) VALUES (0, ?, ?)");
    st.bind(1, msg_id).bind(2, blob_size).run();
    tx.commit();
}

// --- dirty_chunks ---

int64_t StateStore::upsert_dirty_chunk(int64_t ino, int64_t idx, const std::string& path,
                                       int64_t last_write_ns) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached(
        "INSERT INTO dirty_chunks (ino, idx, path, version, last_write_ns) "
        "VALUES (?1, ?2, ?3, 1, ?4) "
        "ON CONFLICT (ino, idx) DO UPDATE SET path = excluded.path, version = version + 1, "
        "last_write_ns = excluded.last_write_ns RETURNING version");
    st.bind(1, ino).bind(2, idx).bind_text(3, path).bind(4, last_write_ns);
    st.step();
    const int64_t version = st.column_int64(0);
    st.run();
    return version;
}

std::vector<DirtyChunk> StateStore::query_dirty_chunks(std::string_view sql,
                                                       std::optional<int64_t> ino) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached(sql);
    if (ino) {
        st.bind(1, *ino);
    }
    std::vector<DirtyChunk> out;
    while (st.step()) {
        out.push_back(dirty_chunk_from_row(st));
    }
    return out;
}

std::optional<DirtyChunk> StateStore::get_dirty_chunk(int64_t ino, int64_t idx) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached(
        "SELECT ino, idx, path, version, last_write_ns FROM dirty_chunks "
        "WHERE ino = ? AND idx = ?");
    st.bind(1, ino).bind(2, idx);
    if (!st.step()) {
        return std::nullopt;
    }
    return dirty_chunk_from_row(st);
}

std::vector<DirtyChunk> StateStore::list_dirty_chunks() {
    return query_dirty_chunks(
        "SELECT ino, idx, path, version, last_write_ns FROM dirty_chunks ORDER BY ino, idx",
        std::nullopt);
}

std::vector<DirtyChunk> StateStore::list_dirty_chunks_for_ino(int64_t ino) {
    return query_dirty_chunks(
        "SELECT ino, idx, path, version, last_write_ns FROM dirty_chunks WHERE ino = ? "
        "ORDER BY idx",
        ino);
}

bool StateStore::delete_dirty_chunk_if_version(int64_t ino, int64_t idx, int64_t version) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("DELETE FROM dirty_chunks WHERE ino = ? AND idx = ? AND version = ?");
    st.bind(1, ino).bind(2, idx).bind(3, version).run();
    return db_.changes() == 1;
}

void StateStore::delete_dirty_chunks_for_ino(int64_t ino) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("DELETE FROM dirty_chunks WHERE ino = ?");
    st.bind(1, ino).run();
}

// --- dirty_attrs ---

void StateStore::set_dirty_attrs(const DirtyAttrs& attrs) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached(
        "INSERT INTO dirty_attrs (ino, size, mtime_ns) VALUES (?1, ?2, ?3) "
        "ON CONFLICT (ino) DO UPDATE SET size = excluded.size, mtime_ns = excluded.mtime_ns");
    st.bind(1, attrs.ino).bind(2, attrs.size).bind(3, attrs.mtime_ns).run();
}

std::optional<DirtyAttrs> StateStore::get_dirty_attrs(int64_t ino) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("SELECT ino, size, mtime_ns FROM dirty_attrs WHERE ino = ?");
    st.bind(1, ino);
    if (!st.step()) {
        return std::nullopt;
    }
    return DirtyAttrs{st.column_int64(0), st.column_int64(1), st.column_int64(2)};
}

void StateStore::delete_dirty_attrs(int64_t ino) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("DELETE FROM dirty_attrs WHERE ino = ?");
    st.bind(1, ino).run();
}

std::vector<DirtyAttrs> StateStore::list_dirty_attrs() {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("SELECT ino, size, mtime_ns FROM dirty_attrs ORDER BY ino");
    std::vector<DirtyAttrs> out;
    while (st.step()) {
        out.push_back({st.column_int64(0), st.column_int64(1), st.column_int64(2)});
    }
    return out;
}

// --- upload_journal ---

void StateStore::upload_started(int64_t chunk_id, int64_t started_ns) {
    const std::lock_guard lock(mutex_);
    Stmt st =
        db_.cached("INSERT INTO upload_journal (chunk_id, msg_id, started_ns) VALUES (?, NULL, ?)");
    st.bind(1, chunk_id).bind(2, started_ns).run();
}

void StateStore::upload_finished(int64_t chunk_id, int64_t msg_id) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("UPDATE upload_journal SET msg_id = ? WHERE chunk_id = ?");
    st.bind(1, msg_id).bind(2, chunk_id).run();
    if (db_.changes() != 1) {
        throw std::logic_error("upload_finished: no journal record for chunk " +
                               std::to_string(chunk_id));
    }
}

std::vector<UploadRecord> StateStore::list_uploads() {
    const std::lock_guard lock(mutex_);
    Stmt st =
        db_.cached("SELECT chunk_id, msg_id, started_ns FROM upload_journal ORDER BY chunk_id");
    std::vector<UploadRecord> out;
    while (st.step()) {
        out.push_back({st.column_int64(0), st.column_opt_int64(1), st.column_int64(2)});
    }
    return out;
}

void StateStore::remove_upload(int64_t chunk_id) {
    const std::lock_guard lock(mutex_);
    Stmt st = db_.cached("DELETE FROM upload_journal WHERE chunk_id = ?");
    st.bind(1, chunk_id).run();
}

}  // namespace telega::meta
