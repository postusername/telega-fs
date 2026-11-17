#include "meta/index_db.h"

#include <fcntl.h>
#include <sqlite3.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <vector>

#include "common/files.h"
#include "meta/index_ops.h"

namespace telega::meta {

namespace {

// Схема ровно как в SPEC 6.2 (PRAGMA выставляет Db::open).
constexpr std::string_view kSchema = R"sql(
CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);

CREATE TABLE inodes (
  ino       INTEGER PRIMARY KEY,
  parent    INTEGER REFERENCES inodes(ino),
  name      BLOB,
  kind      INTEGER NOT NULL,
  mode      INTEGER NOT NULL,
  uid       INTEGER NOT NULL,
  gid       INTEGER NOT NULL,
  size      INTEGER NOT NULL DEFAULT 0,
  atime_ns  INTEGER NOT NULL,
  mtime_ns  INTEGER NOT NULL,
  ctime_ns  INTEGER NOT NULL,
  target    BLOB,
  inline_data BLOB,
  UNIQUE (parent, name)
);

CREATE TABLE extents (
  ino       INTEGER NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE,
  idx       INTEGER NOT NULL,
  chunk_id  INTEGER NOT NULL REFERENCES chunks(chunk_id),
  chunk_off INTEGER NOT NULL DEFAULT 0,
  len       INTEGER NOT NULL,
  PRIMARY KEY (ino, idx)
);

CREATE TABLE chunks (
  chunk_id        INTEGER PRIMARY KEY,
  kind            INTEGER NOT NULL,
  msg_id          INTEGER NOT NULL,
  remote_file_id  TEXT,
  plain_size      INTEGER NOT NULL,
  enc_size        INTEGER NOT NULL,
  content_mac     BLOB NOT NULL,
  refcount        INTEGER NOT NULL,
  live_bytes      INTEGER NOT NULL,
  dead_since_gen  INTEGER
);
CREATE INDEX chunks_dead ON chunks(dead_since_gen) WHERE dead_since_gen IS NOT NULL;
CREATE INDEX chunks_mac  ON chunks(content_mac);

CREATE TABLE index_history (
  generation INTEGER PRIMARY KEY,
  msg_id     INTEGER NOT NULL,
  blob_kind  INTEGER NOT NULL
);
)sql";

std::filesystem::path with_suffix(const std::filesystem::path& p, std::string_view suffix) {
    std::filesystem::path out = p;
    out += suffix;
    return out;
}

// Удалить файл базы вместе со спутниками SQLite.
void remove_db_files(const std::filesystem::path& p) {
    std::error_code ignored;
    for (const char* suffix : {"", "-wal", "-shm", "-journal"}) {
        std::filesystem::remove(with_suffix(p, suffix), ignored);
    }
}

void fsync_file(const std::filesystem::path& p) {
    const Fd fd = Fd::open(p, O_RDONLY);
    if (::fsync(fd.get()) != 0) {
        throw SysError(errno, "fsync " + p.string());
    }
}

void switch_to_wal(Db& db) {
    Stmt st{db, "PRAGMA journal_mode = WAL"};
    if (!st.step() || st.column_text(0) != "wal") {
        throw SqliteError(SQLITE_ERROR, "cannot switch " + db.path().string() + " to WAL");
    }
}

// Коды SQLite, которые при разборе или применении патча означают порчу данных, а не сбой
// ввода-вывода или нехватку памяти.
bool is_corruption(const SqliteError& e) {
    switch (e.primary_code()) {
        case SQLITE_CORRUPT:
        case SQLITE_ABORT:
        case SQLITE_CONSTRAINT:
        case SQLITE_MISMATCH:
        case SQLITE_ERROR:
        case SQLITE_SCHEMA:
        case SQLITE_RANGE:
        case SQLITE_FORMAT:
        case SQLITE_TOOBIG:
            return true;
        default:
            return false;
    }
}

void insert_meta(Db& db, std::string_view key, int64_t value) {
    Stmt st{db, "INSERT INTO meta (key, value) VALUES (?, ?)"};
    st.bind_text(1, key).bind(2, value).run();
}

std::string quote_ident(std::string_view name) {
    std::string out = "\"";
    for (const char c : name) {
        out += c;
        if (c == '"') {
            out += '"';
        }
    }
    out += '"';
    return out;
}

void append_value(std::string& out, const Stmt& st, int col) {
    switch (st.column_type(col)) {
        case SQLITE_NULL:
            out += "NULL";
            break;
        case SQLITE_INTEGER:
            out += std::to_string(st.column_int64(col));
            break;
        case SQLITE_FLOAT: {
            std::array<char, 64> buf{};
            std::snprintf(buf.data(), buf.size(),
                          "%.17g",  // NOLINT(cppcoreguidelines-pro-type-vararg)
                          sqlite3_column_double(st.handle(), col));
            out += buf.data();
            break;
        }
        case SQLITE_TEXT:
            out += '\'';
            out += st.column_text(col);
            out += '\'';
            break;
        default:
            out += "x'";
            out += to_hex(st.column_blob(col));
            out += '\'';
            break;
    }
}

}  // namespace

// ---------------------------------------------------------------------------------------------
// Создание и открытие

void MetaStore::create(const std::filesystem::path& path, const InitParams& params) {
    if (params.chunk_size <= 0 || params.inline_max < 0 || params.inline_budget < 0 ||
        params.pack_threshold <= 0 || params.pack_threshold > params.chunk_size ||
        params.pack_target <= 0 || params.inline_max > params.chunk_size) {
        throw std::invalid_argument("MetaStore::create: inconsistent parameters");
    }
    if (std::filesystem::exists(path)) {
        throw std::runtime_error("MetaStore::create: " + path.string() + " already exists");
    }
    const auto tmp = with_suffix(path, ".tmp-create");
    remove_db_files(tmp);
    try {
        {
            OpenOptions options;
            options.create = true;
            Db db = Db::open(tmp, options);
            Transaction tx{db};
            db.exec(kSchema);
            {
                Stmt st{db, "INSERT INTO meta (key, value) VALUES ('fs_uuid', ?)"};
                st.bind(1, ByteSpan{params.fs_uuid}).run();
            }
            insert_meta(db, "format_version", kFormatVersion);
            insert_meta(db, "chunk_size", params.chunk_size);
            insert_meta(db, "next_chunk_id", 1);
            insert_meta(db, "next_ino", kRootIno + 1);
            insert_meta(db, "inline_max", params.inline_max);
            insert_meta(db, "inline_budget", params.inline_budget);
            insert_meta(db, "inline_bytes", 0);
            insert_meta(db, "pack_threshold", params.pack_threshold);
            insert_meta(db, "pack_target", params.pack_target);
            Stmt root{db,
                      "INSERT INTO inodes (ino, parent, name, kind, mode, uid, gid, size, "
                      "atime_ns, mtime_ns, ctime_ns) VALUES (?1, NULL, NULL, ?2, ?3, ?4, ?5, 0, "
                      "?6, ?6, ?6)"};
            root.bind(1, kRootIno)
                .bind(2, static_cast<int64_t>(InodeKind::dir))
                .bind(3, int64_t{params.root_mode & kPermMask})
                .bind(4, int64_t{params.root_uid})
                .bind(5, int64_t{params.root_gid})
                .bind(6, params.now_ns)
                .run();
            tx.commit();
        }  // закрытие последнего соединения переносит WAL в основной файл и удаляет его
        fsync_file(tmp);
        std::filesystem::rename(tmp, path);
        fsync_dir(path.has_parent_path() ? path.parent_path() : std::filesystem::path{"."});
    } catch (...) {
        remove_db_files(tmp);
        throw;
    }
}

MetaStore::MetaStore(std::filesystem::path path, DirtyHook dirty_hook)
    : path_(std::move(path)), dirty_hook_(std::move(dirty_hook)) {
    if (!std::filesystem::exists(path_)) {
        throw std::runtime_error("MetaStore: " + path_.string() + " does not exist");
    }
    writer_ = std::make_unique<Db>(Db::open(path_));
    const IndexParams params = read_params(*writer_);
    if (params.format_version != kFormatVersion) {
        throw IndexCorrupt("unsupported index format_version " +
                           std::to_string(params.format_version));
    }
    session_ = std::make_unique<Session>(*writer_);
}

MetaStore::~MetaStore() = default;

void MetaStore::set_dirty_hook(DirtyHook hook) {
    const std::lock_guard lock(mutex_);
    dirty_hook_ = std::move(hook);
}

void MetaStore::set_rows_hook(int64_t threshold, std::function<void()> hook) {
    const std::lock_guard lock(mutex_);
    rows_threshold_ = threshold;
    rows_hook_ = std::move(hook);
}

// ---------------------------------------------------------------------------------------------
// Запись и чтение

void MetaStore::write_impl(const std::function<void(Db&)>& fn, WriteKind kind) {
    const std::lock_guard lock(mutex_);
    if (in_write_) {
        throw std::logic_error("nested MetaStore::write");
    }
    if (kind == WriteKind::user && !dirty_.load()) {
        if (dirty_hook_) {
            dirty_hook_();
        }
        dirty_.store(true);
    }
    in_write_ = true;
    struct Reset {
        bool& flag;
        explicit Reset(bool& f) : flag(f) {}
        Reset(const Reset&) = delete;
        Reset& operator=(const Reset&) = delete;
        Reset(Reset&&) = delete;
        Reset& operator=(Reset&&) = delete;
        ~Reset() { flag = false; }
    } const reset{in_write_};
    const int64_t changes_before = writer_->total_changes();
    Transaction tx{*writer_};
    fn(*writer_);
    tx.commit();
    // Откатанная запись сюда не доходит: в сессии от неё ничего нет.
    const int64_t before = rows_.load();
    const int64_t after = before + (writer_->total_changes() - changes_before);
    rows_.store(after);
    if (rows_hook_ && rows_threshold_ > 0 && before < rows_threshold_ && after >= rows_threshold_) {
        rows_hook_();
    }
}

MetaStore::Reader& MetaStore::reader_for_this_thread() {
    const std::lock_guard lock(readers_mutex_);
    auto& slot = readers_[std::this_thread::get_id()];
    if (!slot) {
        OpenOptions options;
        options.read_only = true;
        slot = std::make_unique<Reader>(Reader{Db::open(path_, options), 0});
    }
    return *slot;
}

void MetaStore::read_impl(const std::function<void(Db&)>& fn) {
    Reader& reader = reader_for_this_thread();
    struct Depth {
        int& depth;
        explicit Depth(int& d) : depth(d) { ++depth; }
        Depth(const Depth&) = delete;
        Depth& operator=(const Depth&) = delete;
        Depth(Depth&&) = delete;
        Depth& operator=(Depth&&) = delete;
        ~Depth() { --depth; }
    };
    if (reader.depth > 0) {
        const Depth depth{reader.depth};
        fn(reader.db);
        return;
    }
    const Depth depth{reader.depth};
    Transaction tx{reader.db, Transaction::Mode::deferred};
    fn(reader.db);
    tx.commit();
}

// ---------------------------------------------------------------------------------------------
// Коммит

void MetaStore::reset_session() {
    // Сначала новая сессия, потом удаление старой: если создание не удалось, старая продолжает
    // записывать изменения.
    auto fresh = std::make_unique<Session>(*writer_);
    session_ = std::move(fresh);
}

Bytes MetaStore::take_patchset(TakeStats* stats) {
    using Clock = std::chrono::steady_clock;
    const auto start = Clock::now();
    const std::lock_guard lock(mutex_);
    const auto locked = Clock::now();
    if (in_write_) {
        throw std::logic_error("take_patchset inside MetaStore::write");
    }
    Bytes patch = session_->patchset();
    reset_session();
    dirty_.store(false);
    rows_.store(0);
    if (stats != nullptr) {
        stats->wait = std::chrono::duration_cast<std::chrono::nanoseconds>(locked - start);
        stats->locked = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - locked);
        stats->patch_bytes = patch.size();
    }
    return patch;
}

void MetaStore::vacuum_into(const std::filesystem::path& path) {
    const std::lock_guard lock(mutex_);
    if (in_write_) {
        throw std::logic_error("vacuum_into inside MetaStore::write");
    }
    Stmt st{*writer_, "VACUUM INTO ?"};
    st.bind_text(1, path.string()).run();
    reset_session();
    dirty_.store(false);
    rows_.store(0);
}

// ---------------------------------------------------------------------------------------------
// Дельта-журнал

Bytes MetaStore::concat_patchsets(std::span<const Bytes> patches) {
    try {
        return meta::concat_patchsets(patches);
    } catch (const SqliteError& e) {
        if (is_corruption(e)) {
            throw IndexCorrupt(std::string("cannot concatenate patchsets: ") + e.what());
        }
        throw;
    }
}

void MetaStore::apply_patchset(Db& db, ByteSpan patch) {
    std::string conflict_info;
    const ConflictHandler abort_all = [&](int conflict, sqlite3_changeset_iter* it) {
        const char* table = nullptr;
        int columns = 0;
        int op = 0;
        int indirect = 0;
        if (sqlite3changeset_op(it, &table, &columns, &op, &indirect) == SQLITE_OK &&
            table != nullptr) {
            conflict_info = std::string("table ") + table + ", op " + std::to_string(op) + ", ";
        }
        conflict_info += "conflict " + std::to_string(conflict);
        return ConflictAction::abort;
    };
    try {
        meta::apply_patchset(db, patch, abort_all);
    } catch (const SqliteError& e) {
        if (is_corruption(e)) {
            throw IndexCorrupt("patchset does not apply (" + conflict_info + "): " + e.what());
        }
        throw;
    }
}

void MetaStore::verify(Db& db) {
    {
        Stmt st{db, "PRAGMA foreign_key_check"};
        if (st.step()) {
            throw IndexCorrupt("foreign key violation in table " + st.column_text(0) + ", rowid " +
                               std::to_string(st.column_int64(1)));
        }
    }
    {
        Stmt st{db, "PRAGMA integrity_check"};
        const std::string result = st.step() ? st.column_text(0) : std::string{"no result"};
        if (result != "ok") {
            throw IndexCorrupt("integrity_check: " + result);
        }
    }
    const IndexParams params = read_params(db);
    if (params.format_version != kFormatVersion) {
        throw IndexCorrupt("unsupported index format_version " +
                           std::to_string(params.format_version));
    }
    // Счётчики должны быть больше любого выданного номера, корень — каталогом без родителя.
    Stmt st{db,
            "SELECT (SELECT value FROM meta WHERE key = 'next_ino') > "
            "       (SELECT COALESCE(MAX(ino), 0) FROM inodes), "
            "       (SELECT value FROM meta WHERE key = 'next_chunk_id') > "
            "       (SELECT COALESCE(MAX(chunk_id), 0) FROM chunks), "
            "       EXISTS (SELECT 1 FROM inodes WHERE ino = 1 AND kind = 2 AND parent IS NULL), "
            "       (SELECT value FROM meta WHERE key = 'inline_bytes') = "
            "       (SELECT COALESCE(SUM(LENGTH(inline_data)), 0) FROM inodes)"};
    st.step();
    if (st.column_int64(0) != 1 || st.column_int64(1) != 1) {
        throw IndexCorrupt("meta counters are behind the tables");
    }
    if (st.column_int64(2) != 1) {
        throw IndexCorrupt("root directory is missing");
    }
    if (st.column_int64(3) != 1) {
        throw IndexCorrupt("meta.inline_bytes does not match inline data");
    }
}

void MetaStore::build_from_chain(const std::filesystem::path& snapshot_file,
                                 std::span<const Bytes> patches,
                                 const std::filesystem::path& out_path) {
    const auto tmp = with_suffix(out_path, ".tmp-build");
    remove_db_files(tmp);
    try {
        std::filesystem::copy_file(snapshot_file, tmp,
                                   std::filesystem::copy_options::overwrite_existing);
        {
            OpenOptions options;
            options.foreign_keys = false;  // SPEC 6.6: патчи применяются с выключенными FK
            options.wal = false;  // в WAL переводим после проверки
            Db db = Db::open(tmp, options);
            {
                Transaction tx{db};
                for (const Bytes& patch : patches) {
                    apply_patchset(db, patch);
                }
                try {
                    verify(db);
                } catch (const SqliteError& e) {
                    if (is_corruption(e)) {
                        throw IndexCorrupt(std::string("verify: ") + e.what());
                    }
                    throw;
                }
                tx.commit();
            }
            switch_to_wal(db);
        }
        fsync_file(tmp);
        // Чужой WAL рядом с новым файлом SQLite попытался бы к нему применить.
        for (const char* suffix : {"-wal", "-shm", "-journal"}) {
            std::filesystem::remove(with_suffix(out_path, suffix));
        }
        std::filesystem::rename(tmp, out_path);
        fsync_dir(out_path.has_parent_path() ? out_path.parent_path() : std::filesystem::path{"."});
    } catch (const SqliteError& e) {
        remove_db_files(tmp);
        // Снимок, который SQLite не может открыть или прочитать, — тоже порча цепочки.
        if (e.primary_code() == SQLITE_CORRUPT || e.primary_code() == SQLITE_NOTADB) {
            throw IndexCorrupt(std::string("snapshot: ") + e.what());
        }
        throw;
    } catch (...) {
        remove_db_files(tmp);
        throw;
    }
}

std::string MetaStore::dump(Db& db) {
    std::string out;
    std::vector<std::string> tables;
    {
        Stmt st{db,
                "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"};
        while (st.step()) {
            out += "schema|";
            for (int c = 0; c < 4; ++c) {
                append_value(out, st, c);
                out += '|';
            }
            out += '\n';
            if (st.column_text(0) == "table") {
                tables.push_back(st.column_text(1));
            }
        }
    }
    for (const std::string& table : tables) {
        std::vector<std::pair<int64_t, std::string>> pk;
        {
            Stmt info{db, "SELECT name, pk FROM pragma_table_info(?)"};
            info.bind_text(1, table);
            while (info.step()) {
                if (info.column_int64(1) > 0) {
                    pk.emplace_back(info.column_int64(1), info.column_text(0));
                }
            }
        }
        std::sort(pk.begin(), pk.end());
        std::string order;
        for (const auto& [pos, column] : pk) {
            order += order.empty() ? " ORDER BY " : ", ";
            order += quote_ident(column);
        }
        Stmt rows{db, "SELECT * FROM " + quote_ident(table) + order};
        const int columns = rows.column_count();
        while (rows.step()) {
            out += table;
            for (int c = 0; c < columns; ++c) {
                out += '|';
                append_value(out, rows, c);
            }
            out += '\n';
        }
    }
    return out;
}

}  // namespace telega::meta
