#include "meta/snapshot.h"

#include <fcntl.h>
#include <sqlite3.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "common/files.h"

namespace telega::meta {

namespace {

using Clock = std::chrono::steady_clock;

OpenOptions reader_options() {
    OpenOptions o;
    o.read_only = true;
    return o;
}

OpenOptions copy_options() {
    OpenOptions o;
    o.create = true;
    o.wal = false;  // копии не нужны -wal и -shm в tmp/
    return o;
}

void remove_db_files(const std::filesystem::path& p) noexcept {
    std::error_code ignored;
    for (const char* suffix : {"", "-wal", "-shm", "-journal"}) {
        std::filesystem::path f = p;
        f += suffix;
        std::filesystem::remove(f, ignored);
    }
}

// sqlite3_backup_step(-1) на соединении снимка: страницы читаются с уже открытого среза, источник
// не пишет, перезапуска от записей других соединений нет, результат обязан быть SQLITE_DONE.
void backup_all(Db& dst, Db& src) {
    sqlite3_backup* b = sqlite3_backup_init(dst.handle(), "main", src.handle(), "main");
    if (b == nullptr) {
        dst.raise(sqlite3_errcode(dst.handle()), "sqlite3_backup_init");
    }
    const int rc = sqlite3_backup_step(b, -1);
    const int fin = sqlite3_backup_finish(b);
    if (rc != SQLITE_DONE) {
        throw SqliteError(rc, "sqlite3_backup_step(-1) returned " + std::to_string(rc));
    }
    if (fin != SQLITE_OK) {
        throw SqliteError(fin, "sqlite3_backup_finish");
    }
}

}  // namespace

SnapshotReader::SnapshotReader(const std::filesystem::path& index)
    : db_(Db::open(index, reader_options())) {
    db_.exec("BEGIN");
    try {
        Stmt st = db_.cached("SELECT 1 FROM sqlite_schema LIMIT 1");
        st.step();
        db_.exec("COMMIT");
    } catch (...) {
        try {
            db_.exec("ROLLBACK");
        } catch (const std::exception&) {  // NOLINT(bugprone-empty-catch)
            // соединение всё равно закроется деструктором
        }
        throw;
    }
}

SnapshotReader::~SnapshotReader() { end(); }

void SnapshotReader::begin() {
    if (open_) {
        throw std::logic_error("SnapshotReader::begin: a transaction is already open");
    }
    db_.exec("BEGIN");
    open_ = true;
    try {
        // BEGIN отложенный и среза не берёт: срез фиксирует чтение страницы.
        Stmt st = db_.cached("SELECT 1 FROM sqlite_schema LIMIT 1");
        st.step();
    } catch (...) {
        end();
        throw;
    }
}

void SnapshotReader::end() noexcept {
    if (!open_) {
        return;
    }
    open_ = false;
    try {
        db_.exec("COMMIT");
    } catch (const std::exception&) {
        try {
            db_.exec("ROLLBACK");
        } catch (const std::exception&) {  // NOLINT(bugprone-empty-catch)
            // транзакция чтения закроется вместе с соединением
        }
    }
}

SnapshotCopy::SnapshotCopy(const std::filesystem::path& index, std::filesystem::path raw,
                           std::filesystem::path out, SnapshotHook hook)
    : reader_(index), raw_(std::move(raw)), out_(std::move(out)), hook_(std::move(hook)) {
    try {
        dst_ = std::make_unique<Db>(Db::open(raw_, copy_options()));
        dst_->exec("PRAGMA synchronous = OFF");
    } catch (...) {
        remove_raw();
        throw;
    }
}

SnapshotCopy::~SnapshotCopy() {
    reader_.end();
    dst_.reset();
    remove_raw();
}

void SnapshotCopy::remove_raw() noexcept { remove_db_files(raw_); }

SnapshotCopyStats SnapshotCopy::copy() {
    SnapshotCopyStats stats;
    // Чужой файл по out_ (VACUUM INTO откажется писать поверх него) при сбое не трогаем.
    std::error_code exists_ec;
    const bool out_preexisted = std::filesystem::exists(out_, exists_ec);
    try {
        if (!reader_.in_transaction()) {
            throw std::logic_error("SnapshotCopy::copy: the snapshot was not cut");
        }
        if (hook_) {
            hook_(SnapshotStage::backup, *dst_);
        }
        auto t = Clock::now();
        backup_all(*dst_, reader_.db());
        // Закрыть читательскую транзакцию сразу: пока она открыта, контрольная точка WAL стоит.
        reader_.end();
        stats.backup = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t);

        if (hook_) {
            hook_(SnapshotStage::vacuum, *dst_);
        }
        t = Clock::now();
        // backup переносит в страницу 1 режим WAL источника: без возврата в DELETE копия сама
        // перешла бы в WAL, а VACUUM писал бы в -wal.
        dst_->exec("PRAGMA journal_mode = DELETE");
        {
            Stmt st{*dst_, "VACUUM INTO ?"};
            st.bind_text(1, out_.string()).run();
        }
        dst_.reset();
        remove_raw();
        // Копия идёт с synchronous = OFF: итоговый файл надо сбросить на диск самим (и каталог),
        // иначе после потери питания между pin и шагом 8 он мог бы оказаться неполным, а по
        // нему монтирование узнаёт свой снимок (SPEC 7.2).
        {
            const Fd fd = Fd::open(out_, O_RDONLY);
            if (::fsync(fd.get()) != 0) {
                throw SysError(errno, "fsync " + out_.string());
            }
        }
        fsync_dir(out_.has_parent_path() ? out_.parent_path() : std::filesystem::path{"."});
        stats.vacuum = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t);
    } catch (...) {
        reader_.end();
        dst_.reset();
        remove_raw();
        if (!out_preexisted) {
            remove_db_files(out_);
        }
        throw;
    }
    return stats;
}

}  // namespace telega::meta
