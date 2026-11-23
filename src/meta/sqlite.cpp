#include "meta/sqlite.h"

#include <sqlite3.h>

#include <climits>
#include <cstring>
#include <exception>
#include <string>
#include <unordered_map>
#include <utility>

#if defined(__SANITIZE_THREAD__)
#define TELEGA_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define TELEGA_TSAN 1
#endif
#endif

#ifdef TELEGA_TSAN
// SQLite в WAL-режиме согласует соединения одного процесса через разделяемую память -shm без
// атомиков C11: заголовок wal-index читается дважды и сверяется по контрольной сумме, хеш-таблица
// кадров дописывается писателем под барьерами памяти. TSan видит в этом гонку. Подавление узкое:
// только функции протокола wal-index. Гонки из-за одного соединения в нескольких потоках
// (pager, btree, vdbe) по-прежнему видны.
// NOLINTNEXTLINE(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp): интерфейс TSan
extern "C" const char* __tsan_default_suppressions() {
    return "race:walIndexTryHdr\n"
           "race:walIndexWriteHdr\n"
           "race:walIndexReadHdr\n"
           "race:walTryBeginRead\n"
           "race:walRestartHdr\n"
           "race:walCheckpoint\n"
           "race:walIndexAppend\n"
           "race:walHashGet\n"
           "race:walFindFrame\n";
}
#endif

namespace telega::meta {

namespace {

// Буфер, выделенный SQLite (sqlite3_malloc), с копированием в Bytes.
Bytes take_sqlite_buffer(void* data, int size) {
    Bytes out;
    if (data != nullptr && size > 0) {
        const auto* begin = static_cast<const std::byte*>(data);
        out.assign(begin, begin + size);
    }
    sqlite3_free(data);
    return out;
}

int checked_int_size(size_t size, const char* what) {
    if (size > static_cast<size_t>(INT_MAX)) {
        throw SqliteError(SQLITE_TOOBIG, std::string(what) + ": buffer too large");
    }
    return static_cast<int>(size);
}

// SQLite принимает буфер патча как void*, но не меняет его.
void* mutable_ptr(ByteSpan data) {
    return const_cast<std::byte*>(data.data());  // NOLINT(cppcoreguidelines-pro-type-const-cast)
}

std::string describe(int code, const std::string& what) {
    return what + " (" + sqlite3_errstr(code) + ", code " + std::to_string(code) + ")";
}

}  // namespace

SqliteError::SqliteError(int code, const std::string& what)
    : std::runtime_error(describe(code, what)), code_(code) {}

bool SqliteError::busy() const noexcept {
    return primary_code() == SQLITE_BUSY || primary_code() == SQLITE_LOCKED;
}

// ---------------------------------------------------------------------------------------------
// Db

struct CacheEntry {
    sqlite3_stmt* stmt = nullptr;
    bool in_use = false;
};

struct Db::Impl {
    sqlite3* db = nullptr;
    std::filesystem::path path;
    // Ссылки на элементы unordered_map не инвалидируются при рехеше, поэтому Stmt может
    // держать указатель на in_use.
    std::unordered_map<std::string, CacheEntry> cache;

    Impl() = default;
    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    Impl(Impl&&) = delete;
    Impl& operator=(Impl&&) = delete;
    ~Impl() {
        for (auto& [sql, entry] : cache) {
            sqlite3_finalize(entry.stmt);
        }
        // close_v2 откладывает закрытие, если остались незавершённые выражения.
        sqlite3_close_v2(db);
    }
};

Db::Db(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Db::Db(Db&& other) noexcept = default;
Db& Db::operator=(Db&& other) noexcept = default;
Db::~Db() = default;

Db Db::open(const std::filesystem::path& path, const OpenOptions& options) {
    int flags = options.read_only ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE;
    if (options.create && !options.read_only) {
        flags |= SQLITE_OPEN_CREATE;
    }
    flags |= SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_EXRESCODE;

    auto impl = std::make_unique<Impl>();
    impl->path = path;
    const int rc = sqlite3_open_v2(path.c_str(), &impl->db, flags, nullptr);
    if (rc != SQLITE_OK) {
        const std::string msg = impl->db != nullptr ? sqlite3_errmsg(impl->db) : sqlite3_errstr(rc);
        throw SqliteError(rc, "open " + path.string() + ": " + msg);
    }
    sqlite3_extended_result_codes(impl->db, 1);

    Db db{std::move(impl)};
    db.busy_timeout(options.busy_timeout);
    db.set_foreign_keys(options.foreign_keys);
    if (!options.read_only) {
        if (options.wal) {
            Stmt mode{db, "PRAGMA journal_mode = WAL"};
            const std::string result = mode.step() ? mode.column_text(0) : std::string{};
            if (result != "wal" && result != "memory") {
                throw SqliteError(
                    SQLITE_ERROR,
                    "open " + path.string() + ": cannot switch to WAL, journal_mode = " + result);
            }
        }
        db.exec("PRAGMA synchronous = FULL");
    }
    return db;
}

void Db::exec(std::string_view sql) {
    const std::string text{sql};
    char* err = nullptr;
    const int rc = sqlite3_exec(impl_->db, text.c_str(), nullptr, nullptr, &err);
    if (rc != SQLITE_OK) {
        const std::string msg = err != nullptr ? err : sqlite3_errmsg(impl_->db);
        sqlite3_free(err);
        throw SqliteError(rc, text + ": " + msg);
    }
}

void Db::busy_timeout(std::chrono::milliseconds timeout) {
    const auto ms = timeout.count();
    sqlite3_busy_timeout(impl_->db, ms > INT_MAX ? INT_MAX : static_cast<int>(ms));
}

void Db::set_foreign_keys(bool on) {
    exec(on ? "PRAGMA foreign_keys = ON" : "PRAGMA foreign_keys = OFF");
}

int64_t Db::last_insert_rowid() const { return sqlite3_last_insert_rowid(impl_->db); }
int64_t Db::changes() const { return sqlite3_changes64(impl_->db); }

int64_t Db::total_changes() const { return sqlite3_total_changes64(impl_->db); }
bool Db::autocommit() const { return sqlite3_get_autocommit(impl_->db) != 0; }
const std::filesystem::path& Db::path() const { return impl_->path; }
sqlite3* Db::handle() const noexcept { return impl_->db; }

void Db::raise(int rc, std::string_view context) const {
    throw SqliteError(rc, std::string(context) + ": " + sqlite3_errmsg(impl_->db));
}

Stmt Db::cached(std::string_view sql) {
    auto [it, inserted] = impl_->cache.try_emplace(std::string(sql));
    CacheEntry& entry = it->second;
    if (inserted) {
        const int rc =
            sqlite3_prepare_v3(impl_->db, sql.data(), checked_int_size(sql.size(), "prepare"),
                               SQLITE_PREPARE_PERSISTENT, &entry.stmt, nullptr);
        if (rc != SQLITE_OK || entry.stmt == nullptr) {
            impl_->cache.erase(it);
            raise(rc == SQLITE_OK ? SQLITE_MISUSE : rc, "prepare " + std::string(sql));
        }
    }
    if (entry.in_use) {
        return Stmt{*this, sql};
    }
    entry.in_use = true;
    return Stmt{*this, entry.stmt, &entry.in_use};
}

// ---------------------------------------------------------------------------------------------
// Stmt

Stmt::Stmt(Db& db, std::string_view sql) : db_(&db), stmt_(nullptr), lease_(nullptr) {
    const int rc = sqlite3_prepare_v2(db.handle(), sql.data(),
                                      checked_int_size(sql.size(), "prepare"), &stmt_, nullptr);
    if (rc != SQLITE_OK || stmt_ == nullptr) {
        sqlite3_finalize(stmt_);
        db.raise(rc == SQLITE_OK ? SQLITE_MISUSE : rc, "prepare " + std::string(sql));
    }
}

Stmt::Stmt(Db& db, sqlite3_stmt* stmt, bool* lease) noexcept
    : db_(&db), stmt_(stmt), lease_(lease) {}

Stmt::Stmt(Stmt&& other) noexcept
    : db_(other.db_),
      stmt_(std::exchange(other.stmt_, nullptr)),
      lease_(std::exchange(other.lease_, nullptr)) {}

Stmt::~Stmt() {
    if (stmt_ == nullptr) {
        return;
    }
    if (lease_ != nullptr) {
        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
        *lease_ = false;
    } else {
        sqlite3_finalize(stmt_);
    }
}

void Stmt::check_bind(int rc, int idx) {
    if (rc != SQLITE_OK) {
        db_->raise(rc, "bind #" + std::to_string(idx) + " in " + sqlite3_sql(stmt_));
    }
}

Stmt& Stmt::bind(int idx, int64_t value) {
    check_bind(sqlite3_bind_int64(stmt_, idx, value), idx);
    return *this;
}

Stmt& Stmt::bind(int idx, ByteSpan value) {
    // Пустой буфер с nullptr SQLite привязал бы как NULL.
    if (value.empty()) {
        check_bind(sqlite3_bind_zeroblob(stmt_, idx, 0), idx);
    } else {
        check_bind(sqlite3_bind_blob64(stmt_, idx, value.data(), value.size(), SQLITE_TRANSIENT),
                   idx);
    }
    return *this;
}

Stmt& Stmt::bind_text(int idx, std::string_view value) {
    const char* data = value.data() != nullptr ? value.data() : "";
    check_bind(sqlite3_bind_text64(stmt_, idx, data, value.size(), SQLITE_TRANSIENT, SQLITE_UTF8),
               idx);
    return *this;
}

Stmt& Stmt::bind_null(int idx) {
    check_bind(sqlite3_bind_null(stmt_, idx), idx);
    return *this;
}

Stmt& Stmt::bind(int idx, const std::optional<int64_t>& value) {
    return value ? bind(idx, *value) : bind_null(idx);
}

bool Stmt::step() {
    const int rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ROW) {
        return true;
    }
    if (rc == SQLITE_DONE) {
        return false;
    }
    db_->raise(rc, std::string("step ") + sqlite3_sql(stmt_));
}

void Stmt::run() {
    while (step()) {
    }
}

void Stmt::reset() {
    sqlite3_reset(stmt_);
    sqlite3_clear_bindings(stmt_);
}

bool Stmt::is_null(int col) const { return sqlite3_column_type(stmt_, col) == SQLITE_NULL; }

int64_t Stmt::column_int64(int col) const { return sqlite3_column_int64(stmt_, col); }

Bytes Stmt::column_blob(int col) const {
    const void* data = sqlite3_column_blob(stmt_, col);
    const int size = sqlite3_column_bytes(stmt_, col);
    if (data == nullptr || size <= 0) {
        return {};
    }
    const auto* begin = static_cast<const std::byte*>(data);
    return {begin, begin + size};
}

std::string Stmt::column_text(int col) const {
    const unsigned char* data = sqlite3_column_text(stmt_, col);
    const int size = sqlite3_column_bytes(stmt_, col);
    if (data == nullptr || size <= 0) {
        return {};
    }
    return {reinterpret_cast<const char*>(data), static_cast<size_t>(size)};
}

std::optional<int64_t> Stmt::column_opt_int64(int col) const {
    if (is_null(col)) {
        return std::nullopt;
    }
    return column_int64(col);
}

std::optional<Bytes> Stmt::column_opt_blob(int col) const {
    if (is_null(col)) {
        return std::nullopt;
    }
    return column_blob(col);
}

int Stmt::column_type(int col) const { return sqlite3_column_type(stmt_, col); }
int Stmt::column_count() const { return sqlite3_column_count(stmt_); }

// ---------------------------------------------------------------------------------------------
// Transaction

Transaction::Transaction(Db& db, Mode mode) : db_(db), savepoint_(!db.autocommit()) {
    if (savepoint_) {
        db_.exec("SAVEPOINT telega_tx");
    } else {
        db_.exec(mode == Mode::immediate ? "BEGIN IMMEDIATE" : "BEGIN DEFERRED");
    }
}

Transaction::~Transaction() { rollback(); }

void Transaction::commit() {
    if (done_) {
        throw std::logic_error("transaction already finished");
    }
    db_.exec(savepoint_ ? "RELEASE telega_tx" : "COMMIT");
    done_ = true;
}

void Transaction::rollback() noexcept {
    if (done_) {
        return;
    }
    done_ = true;
    try {
        // После некоторых ошибок (SQLITE_FULL, SQLITE_IOERR) SQLite откатывает транзакцию сам.
        if (db_.autocommit()) {
            return;
        }
        db_.exec(savepoint_ ? "ROLLBACK TO telega_tx; RELEASE telega_tx" : "ROLLBACK");
    } catch (...) {  // NOLINT(bugprone-empty-catch): деструктор, ошибку отката некуда деть
    }
}

// ---------------------------------------------------------------------------------------------
// Session

Session::Session(Db& db) {
    int rc = sqlite3session_create(db.handle(), "main", &session_);
    if (rc != SQLITE_OK) {
        db.raise(rc, "sqlite3session_create");
    }
    rc = sqlite3session_attach(session_, nullptr);
    if (rc != SQLITE_OK) {
        sqlite3session_delete(session_);
        session_ = nullptr;
        db.raise(rc, "sqlite3session_attach");
    }
}

Session::~Session() { sqlite3session_delete(session_); }

Bytes Session::patchset() const {
    int size = 0;
    void* data = nullptr;
    const int rc = sqlite3session_patchset(session_, &size, &data);
    if (rc != SQLITE_OK) {
        sqlite3_free(data);
        throw SqliteError(rc, "sqlite3session_patchset");
    }
    return take_sqlite_buffer(data, size);
}

bool Session::empty() const { return sqlite3session_isempty(session_) != 0; }

int64_t Session::memory_used() const { return sqlite3session_memory_used(session_); }

// ---------------------------------------------------------------------------------------------
// Патчи

Bytes concat_patchsets(std::span<const Bytes> patches) {
    sqlite3_changegroup* group = nullptr;
    int rc = sqlite3changegroup_new(&group);
    if (rc != SQLITE_OK) {
        throw SqliteError(rc, "sqlite3changegroup_new");
    }
    const std::unique_ptr<sqlite3_changegroup, void (*)(sqlite3_changegroup*)> guard{
        group, sqlite3changegroup_delete};
    for (size_t i = 0; i < patches.size(); ++i) {
        const Bytes& patch = patches[i];
        if (patch.empty()) {
            continue;
        }
        rc = sqlite3changegroup_add(group, checked_int_size(patch.size(), "changegroup_add"),
                                    mutable_ptr(patch));
        if (rc != SQLITE_OK) {
            throw SqliteError(rc, "sqlite3changegroup_add, patch #" + std::to_string(i));
        }
    }
    int size = 0;
    void* data = nullptr;
    rc = sqlite3changegroup_output(group, &size, &data);
    if (rc != SQLITE_OK) {
        sqlite3_free(data);
        throw SqliteError(rc, "sqlite3changegroup_output");
    }
    return take_sqlite_buffer(data, size);
}

namespace {

struct ApplyContext {
    const ConflictHandler* handler;
    std::exception_ptr error;
};

int conflict_trampoline(void* ctx, int conflict, sqlite3_changeset_iter* it) {
    auto* context = static_cast<ApplyContext*>(ctx);
    // Исключение не должно пересечь C-код SQLite: запоминаем его и прерываем применение.
    try {
        switch ((*context->handler)(conflict, it)) {
            case ConflictAction::omit:
                return SQLITE_CHANGESET_OMIT;
            case ConflictAction::replace:
                return SQLITE_CHANGESET_REPLACE;
            case ConflictAction::abort:
                return SQLITE_CHANGESET_ABORT;
        }
    } catch (...) {
        context->error = std::current_exception();
    }
    return SQLITE_CHANGESET_ABORT;
}

}  // namespace

void apply_patchset(Db& db, ByteSpan patch, const ConflictHandler& on_conflict) {
    if (patch.empty()) {
        return;
    }
    ApplyContext context{&on_conflict, nullptr};
    const int rc =
        sqlite3changeset_apply(db.handle(), checked_int_size(patch.size(), "changeset_apply"),
                               mutable_ptr(patch), nullptr, conflict_trampoline, &context);
    if (context.error) {
        std::rethrow_exception(context.error);
    }
    if (rc != SQLITE_OK) {
        db.raise(rc, "sqlite3changeset_apply");
    }
}

}  // namespace telega::meta
