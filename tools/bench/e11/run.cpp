// E11: снимок индекса без блокировки записи. Варианты:
//   (a) VACUUM INTO под блокировкой записи (исходный вариант, база сравнения);
//   (b) срез читательской транзакцией под блокировкой, backup API после блокировки, VACUUM копии;
//   (bs) как (b), но копия с PRAGMA synchronous = OFF;
//   (bi) как (bs), но вместо VACUUM на месте — VACUUM INTO нового файла с копии;
//   (bn) как (bs), но без VACUUM копии;
//   (m) реальный код: MetaStore::cut_snapshot под блокировкой, SnapshotCopy (backup, VACUUM
//   INTO)
//       после неё; блокировка MetaStore — честная (FairRecursiveMutex);
//   (c) то же через sqlite3_snapshot_get / sqlite3_snapshot_open (SQLITE_ENABLE_SNAPSHOT).
// Во время снимка работает непрерывный писатель (MetaStore::write) и поток промежуточного
// забора патча (rows_hook). Корректность: снимок плюс патчи, забранные после среза,
// применённые через sqlite3changeset_apply, построчно равны живому индексу.

#include <sqlite3.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>

#include "common/files.h"
#include "crypto/index_blob.h"
#include "e11/e11.h"
#include "meta/index_ops.h"

namespace telega::bench::e11 {

namespace {

using namespace std::chrono_literals;

int64_t wall_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

struct Sample {
    int64_t t0 = 0;  // начало вызова write (от эпохи прогона), нс
    int64_t t1 = 0;  // конец вызова
};

// ---------------------------------------------------------------------------------------------
// Обвязка прогона: писатель, промежуточный забор, замер размера WAL.

class Harness {
  public:
    Harness(ScratchFs& env, const Options& o, uint64_t seed)
        : o_(o),
          meta_(env.mounted->meta()),
          index_path_(env.paths.index_db()),
          seed_(seed),
          epoch_(Clock::now()) {
        load_targets();
        samples_.reserve(size_t{1} << 21U);
    }
    Harness(const Harness&) = delete;
    Harness& operator=(const Harness&) = delete;
    ~Harness() { stop(); }

    meta::MetaStore& meta() { return meta_; }
    const fs::path& index_path() const { return index_path_; }
    [[nodiscard]] int64_t rel_now() const { return ns_since(epoch_); }

    // Чистое начало: сессия пуста, WAL усечён.
    void quiesce() {
        meta_.with_lock([&] {
            (void)meta_.take_patchset();
            const std::lock_guard g(pm_);
            patches_.clear();
        });
        meta::Db db = meta::Db::open(index_path_);
        meta::Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
        st.step();
        if (st.column_int64(0) != 0) {
            throw std::runtime_error("wal_checkpoint(TRUNCATE) was busy");
        }
    }

    void start() {
        stop_.store(false);
        epoch_ = Clock::now();
        meta_.set_rows_hook(o_.take_rows, [this] {
            {
                const std::lock_guard g(tm_);
                want_take_ = true;
            }
            tcv_.notify_one();
        });
        writer_ = std::thread([this] { writer_loop(); });
        taker_ = std::thread([this] { taker_loop(); });
        sampler_ = std::thread([this] { sampler_loop(); });
        started_ = true;
    }

    // Остановить потоки; патч, оставшийся в сессии, забирается в конце (take_final).
    void stop() {
        if (!started_) {
            return;
        }
        stop_.store(true);
        tcv_.notify_all();
        writer_.join();
        taker_.join();
        sampler_.join();
        meta_.set_rows_hook(0, {});
        started_ = false;
        if (!error_.empty()) {
            throw std::runtime_error("harness thread failed: " + error_);
        }
    }

    // Забрать патч под блокировкой MetaStore и положить в очередь в порядке забора. clear — срез
    // снимка: всё, что забрано раньше, к снимку уже не относится.
    meta::TakeStats take_into_queue(bool clear) {
        meta::TakeStats ts;
        prio_lock([&] {
            Bytes p = meta_.take_patchset(&ts);
            const std::lock_guard g(pm_);
            if (clear) {
                patches_.clear();
            }
            patches_.push_back(std::move(p));
        });
        return ts;
    }

    // Блокировка MetaStore для коммита и промежуточного забора. std::recursive_mutex не честный:
    // писатель без пауз перехватывает её сразу после отпускания, и забор может ждать секунды
    // (см. отчёт). В режиме fair писатель уступает, пока кто-то из них ждёт: так ведёт себя
    // честная блокировка (FIFO), которую должен дать MetaStore.
    template <typename F>
    void prio_lock(F&& fn) {
        waiters_.fetch_add(1);
        meta_.with_lock([&] {
            waiters_.fetch_sub(1);
            fn();
        });
    }

    // Срез реального кода (вариант m): патча среза нет, а всё, что забрано раньше, к снимку уже
    // не относится. Зовётся под блокировкой MetaStore (внутри prio_lock), поэтому заборы не
    // вклиниваются между срезом и очисткой. patches_[0] остаётся пустой заглушкой, как у
    // take_into_queue(true).
    void mark_cut() {
        const std::lock_guard g(pm_);
        patches_.clear();
        patches_.emplace_back();
    }

    [[nodiscard]] std::vector<Bytes> patches_after_cut() const {
        const std::lock_guard g(pm_);
        return {patches_.begin() + 1, patches_.end()};
    }

    [[nodiscard]] const std::vector<Sample>& samples() const { return samples_; }
    [[nodiscard]] int64_t wal_now() const { return wal_now_.load(); }
    void reset_wal_peak() { wal_peak_.store(wal_now_.load()); }
    [[nodiscard]] int64_t wal_peak() const { return wal_peak_.load(); }
    [[nodiscard]] int64_t intermediate_takes() const { return takes_.load(); }

  private:
    void load_targets() {
        meta_.read([&](meta::Db& db) {
            meta::Stmt f{db, "SELECT ino FROM inodes WHERE kind = 1"};
            while (f.step()) {
                files_.push_back(f.column_int64(0));
            }
            meta::Stmt i{
                db, "SELECT ino, LENGTH(inline_data) FROM inodes WHERE inline_data IS NOT NULL"};
            while (i.step()) {
                inlines_.emplace_back(i.column_int64(0), i.column_int64(1));
            }
        });
        if (files_.empty()) {
            throw std::runtime_error("no files in the index");
        }
    }

    void writer_loop() {
        try {
            Rng rng(seed_);
            int64_t t = wall_now_ns();
            uint64_t k = 0;
            while (!stop_.load()) {
                while (o_.fair && waiters_.load() > 0 && !stop_.load()) {
                    std::this_thread::sleep_for(20us);
                }
                const int64_t a = rel_now();
                meta_.write([&](meta::Db& db) {
                    for (int i = 0; i < o_.writer_rows; ++i) {
                        t += 1000 + static_cast<int64_t>(rng.below(4000));
                        meta::AttrUpdate u;
                        u.atime_ns = t;
                        u.mtime_ns = t;
                        u.ctime_ns = t;
                        meta::update_attrs(db, files_[rng.below(files_.size())], u);
                    }
                    if (k % 16 == 0 && !inlines_.empty()) {
                        const auto [ino, len] = inlines_[rng.below(inlines_.size())];
                        Bytes data(static_cast<size_t>(len));
                        rng.fill(data.data(), data.size());
                        if (!meta::set_inline(db, ino, data, 1)) {
                            throw std::runtime_error("inline rewrite did not fit the budget");
                        }
                    }
                });
                samples_.push_back({a, rel_now()});
                ++k;
                if (o_.writer_pause_us > 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(o_.writer_pause_us));
                }
            }
        } catch (const std::exception& e) {
            error_ = std::string("writer: ") + e.what();
            stop_.store(true);
        }
    }

    void taker_loop() {
        try {
            for (;;) {
                {
                    std::unique_lock lk(tm_);
                    tcv_.wait(lk, [&] { return want_take_ || stop_.load(); });
                    if (stop_.load()) {
                        return;
                    }
                    want_take_ = false;
                }
                (void)take_into_queue(false);
                takes_.fetch_add(1);
            }
        } catch (const std::exception& e) {
            error_ = std::string("taker: ") + e.what();
            stop_.store(true);
        }
    }

    void sampler_loop() {
        const fs::path wal = fs::path(index_path_.string() + "-wal");
        while (!stop_.load()) {
            std::error_code ec;
            const auto sz = fs::exists(wal, ec) ? fs::file_size(wal, ec) : 0;
            const auto v = ec ? 0 : static_cast<int64_t>(sz);
            wal_now_.store(v);
            if (v > wal_peak_.load()) {
                wal_peak_.store(v);
            }
            std::this_thread::sleep_for(5ms);
        }
    }

    const Options& o_;
    meta::MetaStore& meta_;
    fs::path index_path_;
    uint64_t seed_;
    Clock::time_point epoch_;
    std::vector<int64_t> files_;
    std::vector<std::pair<int64_t, int64_t>> inlines_;
    std::vector<Sample> samples_;  // только писатель до join
    std::atomic<bool> stop_{false};
    bool started_ = false;
    std::thread writer_;
    std::thread taker_;
    std::thread sampler_;
    std::mutex tm_;
    std::condition_variable tcv_;
    bool want_take_ = false;
    mutable std::mutex pm_;
    std::vector<Bytes> patches_;  // [0] — патч среза, дальше — забранные после него
    std::atomic<int64_t> wal_now_{0};
    std::atomic<int64_t> wal_peak_{0};
    std::atomic<int64_t> takes_{0};
    std::atomic<int> waiters_{0};
    std::string error_;
};

// ---------------------------------------------------------------------------------------------
// Варианты снимка

struct SnapTimes {
    int64_t t_start = 0;  // от эпохи прогона: запрос блокировки
    int64_t t_end = 0;    // готов файл снимка
    int64_t open_ns = 0;  // открытие соединений (вне блокировки)
    int64_t wait_ns = 0;  // ожидание блокировки MetaStore
    int64_t hold_ns = 0;  // удержание блокировки
    int64_t take_ns = 0;  // из них забор патча
    int64_t cut_ns = 0;  // из них фиксация среза (BEGIN + чтение страницы / VACUUM INTO)
    int64_t handoff_ns = 0;  // (c): snapshot_open на втором соединении после блокировки
    int64_t backup_ns = 0;
    int64_t vacuum_ns = 0;
    int64_t total_ns = 0;  // от открытия соединений до готового файла
    int64_t raw_bytes = 0;  // файл до VACUUM копии (у (a) равен final_bytes)
    int64_t final_bytes = 0;
    int64_t patch_rows_hint = 0;
    fs::path path;
};

void backup_all(meta::Db& dst, meta::Db& src) {
    sqlite3_backup* b = sqlite3_backup_init(dst.handle(), "main", src.handle(), "main");
    if (b == nullptr) {
        dst.raise(sqlite3_errcode(dst.handle()), "sqlite3_backup_init");
    }
    const int rc = sqlite3_backup_step(b, -1);
    sqlite3_backup_finish(b);
    if (rc != SQLITE_DONE) {
        throw meta::SqliteError(rc, "sqlite3_backup_step(-1) returned " + std::to_string(rc));
    }
}

meta::Db open_reader(const fs::path& index) {
    meta::OpenOptions ro;
    ro.read_only = true;
    meta::Db db = meta::Db::open(index, ro);
    // Прогрев: схема и выражение готовы заранее, под блокировкой остаются BEGIN и чтение страницы.
    db.exec("BEGIN");
    {
        auto st = db.cached("SELECT 1 FROM sqlite_schema LIMIT 1");
        st.step();
    }
    db.exec("COMMIT");
    return db;
}

// Копия снимка: tmp/index-<G+1>.sqlite. sync_off — PRAGMA synchronous = OFF: копия
// воспроизводима (при сбое tmp/ выбрасывается), долговечность ей не нужна.
meta::Db open_dest(const fs::path& path, bool sync_off = false) {
    meta::OpenOptions o;
    o.create = true;
    o.wal = false;  // копия не в WAL (ей не нужен -wal/-shm в tmp/)
    meta::Db db = meta::Db::open(path, o);
    if (sync_off) {
        db.exec("PRAGMA synchronous = OFF");
    }
    return db;
}

// Под блокировкой: забор патча и фиксация среза читательской транзакцией.
void cut_read_txn(meta::Db& reader) {
    reader.exec("BEGIN");
    auto st = reader.cached("SELECT 1 FROM sqlite_schema LIMIT 1");
    st.step();
}

// Завершение копии: backup переносит в страницу 1 байты заголовка источника (режим WAL), поэтому
// после него соединение назначения само переходит в WAL. Возвращаем rollback-журнал (иначе
// VACUUM пишет в -wal, а основной файл остаётся несжатым до контрольной точки), делаем VACUUM
// и закрываем соединение: после этого файл самодостаточен.
enum class Finish : uint8_t {
    vacuum,  // VACUUM на месте
    into,  // VACUUM INTO нового файла с копии, сырая копия удаляется
    none,  // без VACUUM: копия как есть
};

void finish_copy(std::unique_ptr<meta::Db>& dst, Finish finish, const fs::path& raw,
                 const fs::path& dest) {
    dst->exec("PRAGMA journal_mode = DELETE");
    switch (finish) {
        case Finish::vacuum:
            dst->exec("VACUUM");
            break;
        case Finish::into: {
            meta::Stmt st{*dst, "VACUUM INTO ?"};
            st.bind_text(1, dest.string()).run();
            break;
        }
        case Finish::none:
            break;
    }
    dst.reset();
    if (finish == Finish::into) {
        std::error_code ec;
        fs::remove(raw, ec);
    }
}

SnapTimes snapshot_a(Harness& h, const fs::path& dest) {
    SnapTimes r;
    r.path = dest;
    const auto t_total = Clock::now();
    r.t_start = h.rel_now();
    const auto t_call = Clock::now();
    h.prio_lock([&] {
        const auto h0 = Clock::now();
        r.wait_ns = to_ns(h0 - t_call);
        const meta::TakeStats ts = h.take_into_queue(true);
        r.take_ns = to_ns(ts.locked);
        const auto c0 = Clock::now();
        h.meta().vacuum_into(dest);
        r.cut_ns = ns_since(c0);
        r.hold_ns = ns_since(h0);
    });
    r.t_end = h.rel_now();
    r.total_ns = ns_since(t_total);
    r.raw_bytes = r.final_bytes = static_cast<int64_t>(fs::file_size(dest));
    return r;
}

SnapTimes snapshot_b_impl(Harness& h, const fs::path& dest, bool sync_off, Finish finish) {
    SnapTimes r;
    r.path = dest;
    const auto t_total = Clock::now();
    meta::Db src = open_reader(h.index_path());
    const fs::path raw_path = finish == Finish::into ? fs::path(dest.string() + ".raw") : dest;
    auto dst = std::make_unique<meta::Db>(open_dest(raw_path, sync_off));
    r.open_ns = ns_since(t_total);
    r.t_start = h.rel_now();
    const auto t_call = Clock::now();
    h.prio_lock([&] {
        const auto h0 = Clock::now();
        r.wait_ns = to_ns(h0 - t_call);
        const meta::TakeStats ts = h.take_into_queue(true);
        r.take_ns = to_ns(ts.locked);
        const auto c0 = Clock::now();
        cut_read_txn(src);
        r.cut_ns = ns_since(c0);
        r.hold_ns = ns_since(h0);
    });
    auto t = Clock::now();
    backup_all(*dst, src);
    src.exec("COMMIT");
    r.backup_ns = ns_since(t);
    {
        // Размер до VACUUM: страницы лежат в -wal, пока не сделан checkpoint, поэтому смотрим
        // число страниц, а не размер файла.
        meta::Stmt pc{*dst, "PRAGMA page_count"};
        pc.step();
        meta::Stmt ps{*dst, "PRAGMA page_size"};
        ps.step();
        r.raw_bytes = pc.column_int64(0) * ps.column_int64(0);
    }
    t = Clock::now();
    finish_copy(dst, finish, raw_path, dest);
    r.vacuum_ns = ns_since(t);
    r.t_end = h.rel_now();
    r.total_ns = ns_since(t_total);
    r.final_bytes = static_cast<int64_t>(fs::file_size(dest));
    return r;
}

// Реальный код: SnapshotCopy открывается заранее, под блокировкой только cut_snapshot.
SnapTimes snapshot_m(Harness& h, const fs::path& dest) {
    SnapTimes r;
    r.path = dest;
    const auto t_total = Clock::now();
    const fs::path raw_path = fs::path(dest.string() + ".raw");
    meta::SnapshotCopy copy{h.index_path(), raw_path, dest};
    r.open_ns = ns_since(t_total);
    r.t_start = h.rel_now();
    const auto t_call = Clock::now();
    h.prio_lock([&] {
        const auto h0 = Clock::now();
        r.wait_ns = to_ns(h0 - t_call);
        const auto c0 = Clock::now();
        h.meta().cut_snapshot(copy.reader());
        h.mark_cut();
        r.cut_ns = ns_since(c0);
        r.hold_ns = ns_since(h0);
    });
    const meta::SnapshotCopyStats st = copy.copy();
    r.backup_ns = to_ns(st.backup);
    r.vacuum_ns = to_ns(st.vacuum);
    r.t_end = h.rel_now();
    r.total_ns = ns_since(t_total);
    r.final_bytes = static_cast<int64_t>(fs::file_size(dest));
    r.raw_bytes = r.final_bytes;
    return r;
}

SnapTimes snapshot_b(Harness& h, const fs::path& dest) {
    return snapshot_b_impl(h, dest, false, Finish::vacuum);
}
SnapTimes snapshot_bs(Harness& h, const fs::path& dest) {
    return snapshot_b_impl(h, dest, true, Finish::vacuum);
}
SnapTimes snapshot_bi(Harness& h, const fs::path& dest) {
    return snapshot_b_impl(h, dest, true, Finish::into);
}
SnapTimes snapshot_bn(Harness& h, const fs::path& dest) {
    return snapshot_b_impl(h, dest, true, Finish::none);
}

#ifdef SQLITE_ENABLE_SNAPSHOT
SnapTimes snapshot_c(Harness& h, const fs::path& dest) {
    SnapTimes r;
    r.path = dest;
    const auto t_total = Clock::now();
    meta::Db getter = open_reader(h.index_path());
    meta::Db opener = open_reader(h.index_path());
    auto dst = std::make_unique<meta::Db>(open_dest(dest));
    r.open_ns = ns_since(t_total);
    r.t_start = h.rel_now();
    const auto t_call = Clock::now();
    sqlite3_snapshot* snap = nullptr;
    h.prio_lock([&] {
        const auto h0 = Clock::now();
        r.wait_ns = to_ns(h0 - t_call);
        const meta::TakeStats ts = h.take_into_queue(true);
        r.take_ns = to_ns(ts.locked);
        const auto c0 = Clock::now();
        cut_read_txn(getter);
        const int rc = sqlite3_snapshot_get(getter.handle(), "main", &snap);
        if (rc != SQLITE_OK) {
            throw meta::SqliteError(rc, "sqlite3_snapshot_get");
        }
        r.cut_ns = ns_since(c0);
        r.hold_ns = ns_since(h0);
    });
    auto t = Clock::now();
    opener.exec("BEGIN");
    const int rc = sqlite3_snapshot_open(opener.handle(), "main", snap);
    if (rc != SQLITE_OK) {
        sqlite3_snapshot_free(snap);
        throw meta::SqliteError(rc, "sqlite3_snapshot_open");
    }
    getter.exec("COMMIT");
    sqlite3_snapshot_free(snap);
    r.handoff_ns = ns_since(t);
    t = Clock::now();
    backup_all(*dst, opener);
    opener.exec("COMMIT");
    r.backup_ns = ns_since(t);
    {
        meta::Stmt pc{*dst, "PRAGMA page_count"};
        pc.step();
        meta::Stmt ps{*dst, "PRAGMA page_size"};
        ps.step();
        r.raw_bytes = pc.column_int64(0) * ps.column_int64(0);
    }
    t = Clock::now();
    finish_copy(dst, Finish::vacuum, dest, dest);
    r.vacuum_ns = ns_since(t);
    r.t_end = h.rel_now();
    r.total_ns = ns_since(t_total);
    r.final_bytes = static_cast<int64_t>(fs::file_size(dest));
    return r;
}
#endif

// ---------------------------------------------------------------------------------------------
// Проверка корректности

struct CompareResult {
    int64_t tables = 0;
    int64_t rows = 0;
    int64_t mismatches = 0;
    std::string first;
};

std::string quote_ident(const std::string& name) {
    std::string out = "\"";
    for (const char c : name) {
        out += c;
        if (c == '"') {
            out += '"';
        }
    }
    return out + '"';
}

// Значения ячейки совпадают: тип, затем содержимое.
bool same_cell(sqlite3_stmt* a, sqlite3_stmt* b, int col) {
    const int ta = sqlite3_column_type(a, col);
    if (ta != sqlite3_column_type(b, col)) {
        return false;
    }
    switch (ta) {
        case SQLITE_NULL:
            return true;
        case SQLITE_INTEGER:
            return sqlite3_column_int64(a, col) == sqlite3_column_int64(b, col);
        case SQLITE_FLOAT:
            return sqlite3_column_double(a, col) == sqlite3_column_double(b, col);
        default: {
            const int na = sqlite3_column_bytes(a, col);
            const int nb = sqlite3_column_bytes(b, col);
            if (na != nb) {
                return false;
            }
            return na == 0 ||
                   std::memcmp(
                       ta == SQLITE_TEXT ? static_cast<const void*>(sqlite3_column_text(a, col))
                                         : sqlite3_column_blob(a, col),
                       ta == SQLITE_TEXT ? static_cast<const void*>(sqlite3_column_text(b, col))
                                         : sqlite3_column_blob(b, col),
                       static_cast<size_t>(na)) == 0;
        }
    }
}

// Построчное сравнение всех таблиц (в порядке первичного ключа) двух баз, без текстового дампа.
CompareResult compare_dbs(meta::Db& a, meta::Db& b) {
    CompareResult res;
    std::vector<std::string> tables;
    {
        meta::Stmt st{a,
                      "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE "
                      "'sqlite_%' ORDER BY name"};
        while (st.step()) {
            tables.push_back(st.column_text(0));
        }
    }
    // Схемы (включая индексы) должны совпадать дословно.
    {
        meta::Stmt sa{a, "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY name"};
        meta::Stmt sb{b, "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY name"};
        for (;;) {
            const bool ra = sa.step();
            const bool rb = sb.step();
            if (ra != rb) {
                ++res.mismatches;
                res.first = "schema: different number of objects";
                break;
            }
            if (!ra) {
                break;
            }
            for (int c = 0; c < 4; ++c) {
                if (!same_cell(sa.handle(), sb.handle(), c)) {
                    ++res.mismatches;
                    if (res.first.empty()) {
                        res.first = "schema: object " + sa.column_text(1);
                    }
                }
            }
        }
    }
    for (const std::string& table : tables) {
        ++res.tables;
        std::vector<std::pair<int64_t, std::string>> pk;
        {
            meta::Stmt info{a, "SELECT name, pk FROM pragma_table_info(?)"};
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
        const std::string sql = "SELECT * FROM " + quote_ident(table) + order;
        meta::Stmt sa{a, sql};
        meta::Stmt sb{b, sql};
        const int cols = sa.column_count();
        int64_t table_rows = 0;
        for (;;) {
            const bool ra = sa.step();
            const bool rb = sb.step();
            if (ra != rb) {
                ++res.mismatches;
                if (res.first.empty()) {
                    res.first = table + ": row count differs after " + std::to_string(table_rows);
                }
                break;
            }
            if (!ra) {
                break;
            }
            ++table_rows;
            for (int c = 0; c < cols; ++c) {
                if (!same_cell(sa.handle(), sb.handle(), c)) {
                    ++res.mismatches;
                    if (res.first.empty()) {
                        res.first = table + ": row " + std::to_string(table_rows) + ", column " +
                                    std::to_string(c);
                    }
                    break;
                }
            }
        }
        res.rows += table_rows;
    }
    return res;
}

struct Verify {
    bool integrity_ok = false;
    int64_t integrity_ms = 0;
    bool applied = false;
    std::string apply_error;
    int64_t patches = 0;
    int64_t patch_bytes = 0;
    CompareResult cmp;
    int64_t apply_ms = 0;
    int64_t compare_ms = 0;
};

Verify verify_snapshot(Harness& h, ScratchFs& env, const fs::path& snap) {
    Verify v;
    {
        const auto t = Clock::now();
        meta::OpenOptions ro;
        ro.read_only = true;
        meta::Db db = meta::Db::open(snap, ro);
        meta::Stmt st{db, "PRAGMA integrity_check"};
        const std::string res = st.step() ? st.column_text(0) : std::string{"no result"};
        v.integrity_ok = res == "ok";
        v.integrity_ms = ns_since(t) / 1'000'000;
    }
    const fs::path copy = env.paths.tmp() / "verify.sqlite";
    for (const char* suffix : {"", "-wal", "-shm", "-journal"}) {
        std::error_code ec;
        fs::remove(copy.string() + suffix, ec);
    }
    fs::copy_file(snap, copy);
    const auto patches = h.patches_after_cut();
    v.patches = static_cast<int64_t>(patches.size());
    meta::OpenOptions oo;
    oo.foreign_keys = false;  // патчи применяются с выключенными FK
    oo.wal = false;
    meta::Db db = meta::Db::open(copy, oo);
    auto t = Clock::now();
    {
        meta::Transaction tx{db};
        try {
            for (const Bytes& p : patches) {
                v.patch_bytes += static_cast<int64_t>(p.size());
                meta::MetaStore::apply_patchset(db, p);
            }
            v.applied = true;
        } catch (const std::exception& e) {
            v.apply_error = e.what();
        }
        tx.commit();
    }
    v.apply_ms = ns_since(t) / 1'000'000;
    t = Clock::now();
    h.meta().read([&](meta::Db& live) { v.cmp = compare_dbs(db, live); });
    v.compare_ms = ns_since(t) / 1'000'000;
    return v;
}

// ---------------------------------------------------------------------------------------------
// Статистика писателя

struct Window {
    int64_t t0;  // запрос блокировки
    int64_t t1;  // снимок готов
    int64_t post_end;
};

struct LatStats {
    int64_t n = 0;
    double span_s = 0;
    double p50_us = 0;
    double p99_us = 0;
    double max_us = 0;
};

LatStats lat_stats(std::vector<int64_t> lat_ns, double span_s) {
    LatStats s;
    s.n = static_cast<int64_t>(lat_ns.size());
    s.span_s = span_s;
    if (lat_ns.empty()) {
        return s;
    }
    std::sort(lat_ns.begin(), lat_ns.end());
    const auto at = [&](double q) {
        const auto idx = static_cast<size_t>(std::ceil(q * static_cast<double>(lat_ns.size()))) - 1;
        return static_cast<double>(lat_ns[std::min(idx, lat_ns.size() - 1)]) / 1e3;
    };
    s.p50_us = at(0.5);
    s.p99_us = at(0.99);
    s.max_us = static_cast<double>(lat_ns.back()) / 1e3;
    return s;
}

struct WriterStats {
    LatStats idle;
    LatStats during;
    LatStats post;
};

WriterStats classify(const std::vector<Sample>& samples, const std::vector<Window>& windows,
                     int64_t first_start) {
    std::vector<int64_t> idle;
    std::vector<int64_t> during;
    std::vector<int64_t> post;
    double during_s = 0;
    double post_s = 0;
    for (const Window& w : windows) {
        during_s += static_cast<double>(w.t1 - w.t0) / 1e9;
        post_s += static_cast<double>(w.post_end - w.t1) / 1e9;
    }
    for (const Sample& s : samples) {
        const int64_t lat = s.t1 - s.t0;
        if (s.t1 < first_start) {
            idle.push_back(lat);  // окно до первого снимка (с прогревом после старта: 200 мс)
            continue;
        }
        bool placed = false;
        for (const Window& w : windows) {
            if (s.t1 >= w.t0 && s.t0 <= w.t1) {
                during.push_back(lat);
                placed = true;
                break;
            }
        }
        if (placed) {
            continue;
        }
        for (const Window& w : windows) {
            if (s.t0 >= w.t1 && s.t0 < w.post_end) {
                post.push_back(lat);
                break;
            }
        }
    }
    WriterStats ws;
    ws.idle = lat_stats(idle, static_cast<double>(first_start) / 1e9);
    ws.during = lat_stats(during, during_s);
    ws.post = lat_stats(post, post_s);
    return ws;
}

// ---------------------------------------------------------------------------------------------

struct Csvs {
    Csv snaps;
    Csv writer;
    Csv correct;
    Csv corpora;
};

using SnapFn = SnapTimes (*)(Harness&, const fs::path&);

SnapFn pick(const std::string& v) {
    if (v == "a") {
        return snapshot_a;
    }
    if (v == "b") {
        return snapshot_b;
    }
    if (v == "bs") {
        return snapshot_bs;
    }
    if (v == "bi") {
        return snapshot_bi;
    }
    if (v == "bn") {
        return snapshot_bn;
    }
    if (v == "m") {
        return snapshot_m;
    }
#ifdef SQLITE_ENABLE_SNAPSHOT
    if (v == "c") {
        return snapshot_c;
    }
#endif
    return nullptr;
}

void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

void run_one(const Options& o, ScratchFs& env, const CorpusInfo& info, const std::string& variant,
             int rep, Csvs& csv) {
    const SnapFn fn = pick(variant);
    const uint64_t seed = o.seed + static_cast<uint64_t>(rep) * 7919U;
    Harness h(env, o, seed);
    h.quiesce();
    const fs::path tmp = env.paths.tmp();
    std::vector<Window> windows;
    std::vector<SnapTimes> rounds;
    std::vector<int64_t> wal_snap;
    std::vector<int64_t> wal_post;
    std::vector<int64_t> wal_peak_post;
    h.start();
    sleep_ms(o.pre_ms);
    const int64_t first_start = h.rel_now();
    fs::path last;
    for (int round = 1; round <= info.rounds; ++round) {
        const fs::path dest = tmp / ("index-e11-" + std::to_string(round) + ".sqlite");
        std::error_code ec;
        fs::remove(dest, ec);
        h.reset_wal_peak();
        SnapTimes st = fn(h, dest);
        wal_snap.push_back(h.wal_peak());
        h.reset_wal_peak();
        sleep_ms(info.post_ms);
        windows.push_back({st.t_start, st.t_end, h.rel_now()});
        wal_post.push_back(h.wal_now());
        wal_peak_post.push_back(h.wal_peak());
        rounds.push_back(st);
        if (round < info.rounds) {
            fs::remove(dest, ec);
        } else {
            last = dest;
        }
    }
    h.stop();
    const int64_t takes = h.intermediate_takes();
    (void)h.take_into_queue(false);  // остаток сессии: тоже «после среза»

    const WriterStats ws = classify(h.samples(), windows, first_start);

    // Корректность и размер после zstd: на последнем снимке.
    const Verify v = verify_snapshot(h, env, last);
    int64_t zstd_bytes = 0;
    int64_t zstd_ms = 0;
    {
        const Bytes raw = read_file(last);
        const auto t = Clock::now();
        const Bytes z = crypto::zstd_compress(raw, 9);
        zstd_ms = ns_since(t) / 1'000'000;
        zstd_bytes = static_cast<int64_t>(z.size());
    }

    for (size_t i = 0; i < rounds.size(); ++i) {
        const SnapTimes& r = rounds[i];
        const bool lastr = i + 1 == rounds.size();
        csv.snaps.row({info.name,
                       variant,
                       cell(rep),
                       cell(static_cast<int64_t>(i + 1)),
                       cell(r.open_ns),
                       cell(r.wait_ns),
                       cell(r.hold_ns),
                       cell(r.take_ns),
                       cell(r.cut_ns),
                       cell(r.handoff_ns),
                       cell(r.backup_ns),
                       cell(r.vacuum_ns),
                       cell(r.total_ns),
                       cell(r.raw_bytes),
                       cell(r.final_bytes),
                       cell(lastr ? zstd_bytes : int64_t{-1}),
                       cell(lastr ? zstd_ms : int64_t{-1}),
                       cell(wal_snap[i]),
                       cell(wal_post[i]),
                       cell(wal_peak_post[i])});
    }
    csv.snaps.flush();
    csv.writer.row({info.name,
                    variant,
                    cell(rep),
                    cell(ws.idle.n),
                    cell(ws.idle.span_s),
                    cell(ws.idle.p50_us),
                    cell(ws.idle.p99_us),
                    cell(ws.idle.max_us),
                    cell(ws.during.n),
                    cell(ws.during.span_s),
                    cell(ws.during.p50_us),
                    cell(ws.during.p99_us),
                    cell(ws.during.max_us),
                    cell(ws.post.n),
                    cell(ws.post.span_s),
                    cell(ws.post.p50_us),
                    cell(ws.post.p99_us),
                    cell(ws.post.max_us),
                    cell(static_cast<int64_t>(h.samples().size())),
                    cell(takes)});
    csv.writer.flush();
    csv.correct.row({info.name, variant, cell(rep), cell(v.integrity_ok ? 1 : 0),
                     cell(v.integrity_ms), cell(v.applied ? 1 : 0), cell(v.patches),
                     cell(v.patch_bytes), cell(v.cmp.tables), cell(v.cmp.rows),
                     cell(v.cmp.mismatches), cell(v.apply_ms), cell(v.compare_ms),
                     v.cmp.first.empty() ? std::string{"-"} : v.cmp.first,
                     v.apply_error.empty() ? std::string{"-"} : "apply error"});
    csv.correct.flush();

    std::vector<double> hold;
    std::vector<double> total;
    for (const SnapTimes& r : rounds) {
        hold.push_back(static_cast<double>(r.hold_ns) / 1e6);
        total.push_back(static_cast<double>(r.total_ns) / 1e6);
    }
    const Summary sh = summarize(hold);
    const Summary st = summarize(total);
    std::cout << "  " << info.name << " (" << variant << ") rep " << rep << ": hold median "
              << sh.median << " ms max " << sh.max << " ms, total median " << st.median
              << " ms, writer p99 idle/during/post " << ws.idle.p99_us / 1e3 << '/'
              << ws.during.p99_us / 1e3 << '/' << ws.post.p99_us / 1e3 << " ms (max during "
              << ws.during.max_us / 1e3 << ", post " << ws.post.max_us / 1e3 << "), writer ops "
              << h.samples().size() << ", wal peak " << wal_snap.back() / 1024 << " KiB, correct: "
              << (v.integrity_ok && v.applied && v.cmp.mismatches == 0 ? "yes" : "NO") << " ("
              << v.cmp.rows << " rows, " << v.patches << " patches)\n";
    if (!v.apply_error.empty() || v.cmp.mismatches != 0 || !v.integrity_ok) {
        std::cout << "    CORRECTNESS FAILURE: " << v.apply_error << ' ' << v.cmp.first << '\n';
    }
    std::error_code ec;
    for (const SnapTimes& r : rounds) {
        fs::remove(r.path, ec);
    }
    fs::remove(env.paths.tmp() / "verify.sqlite", ec);
}

}  // namespace

std::string negative_control(const Options& o, ScratchFs& env) {
    Harness h(env, o, o.seed + 99);
    h.quiesce();
    h.start();
    sleep_ms(300);
    const fs::path snap = env.paths.tmp() / "neg-control.sqlite";
    std::error_code ec;
    fs::remove(snap, ec);
    (void)snapshot_bi(h, snap);
    sleep_ms(500);
    h.stop();
    (void)h.take_into_queue(false);
    // Без патчей: копия снимка сравнивается с живым индексом как есть.
    const fs::path copy = env.paths.tmp() / "neg-copy.sqlite";
    fs::remove(copy, ec);
    fs::copy_file(snap, copy);
    meta::OpenOptions oo;
    oo.foreign_keys = false;
    oo.wal = false;
    CompareResult without;
    {
        meta::Db db = meta::Db::open(copy, oo);
        h.meta().read([&](meta::Db& live) { without = compare_dbs(db, live); });
    }
    const Verify with = verify_snapshot(h, env, snap);
    fs::remove(snap, ec);
    fs::remove(copy, ec);
    return "без патчей расхождений " + std::to_string(without.mismatches) +
           " (первое: " + (without.first.empty() ? std::string{"-"} : without.first) + "), с " +
           std::to_string(with.patches) + " патчами (" + std::to_string(with.patch_bytes) +
           " байт) расхождений " + std::to_string(with.cmp.mismatches) + ", строк сравнено " +
           std::to_string(with.cmp.rows) + ", писатель сделал " +
           std::to_string(h.samples().size()) + " транзакций";
}

int run_snapshots(const Options& o) {
    Csvs csv{
        Csv(o.out / "snapshots.csv", {"corpus",
                                      "variant",
                                      "rep",
                                      "round",
                                      "open_ns",
                                      "wait_ns",
                                      "hold_ns",
                                      "take_ns",
                                      "cut_ns",
                                      "handoff_ns",
                                      "backup_ns",
                                      "vacuum_ns",
                                      "total_ns",
                                      "raw_bytes",
                                      "final_bytes",
                                      "zstd9_bytes",
                                      "zstd9_ms",
                                      "wal_peak_snapshot_bytes",
                                      "wal_after_post_bytes",
                                      "wal_peak_post_bytes"}),
        Csv(o.out / "writer.csv", {"corpus",
                                   "variant",
                                   "rep",
                                   "idle_n",
                                   "idle_s",
                                   "idle_p50_us",
                                   "idle_p99_us",
                                   "idle_max_us",
                                   "during_n",
                                   "during_s",
                                   "during_p50_us",
                                   "during_p99_us",
                                   "during_max_us",
                                   "post_n",
                                   "post_s",
                                   "post_p50_us",
                                   "post_p99_us",
                                   "post_max_us",
                                   "writer_ops_total",
                                   "intermediate_takes"}),
        Csv(o.out / "correctness.csv",
            {"corpus", "variant", "rep", "integrity_ok", "integrity_ms", "patches_applied",
             "patches", "patch_bytes", "tables", "rows_compared", "mismatches", "apply_ms",
             "compare_ms", "first_mismatch", "apply_error"}),
        Csv(o.out / "corpora.csv", {"corpus", "inodes", "files", "inline_files", "index_bytes",
                                    "build_s", "rounds", "post_ms"}),
    };
    for (const std::string& name : o.corpora) {
        std::cout << "corpus " << name << '\n';
        CorpusInfo info;
        auto env = build_corpus(o, name, info);
        std::cout << "  built in " << info.build_s << " s: " << info.inodes << " inodes, "
                  << info.files << " files, " << info.inline_files << " inline, index "
                  << info.index_bytes / (1024 * 1024) << " MiB\n";
        csv.corpora.row({info.name, cell(info.inodes), cell(info.files), cell(info.inline_files),
                         cell(info.index_bytes), cell(info.build_s), cell(info.rounds),
                         cell(info.post_ms)});
        csv.corpora.flush();
        for (const std::string& variant : o.variants) {
            if (pick(variant) == nullptr) {
                std::cout << "  variant (" << variant
                          << ") is unavailable in this build (SQLITE_ENABLE_SNAPSHOT is off)\n";
                continue;
            }
            for (int rep = 1; rep <= o.repeats; ++rep) {
                run_one(o, *env, info, variant, rep, csv);
            }
        }
        env.reset();
        if (!o.keep) {
            std::error_code ec;
            fs::remove_all(o.work / ("fs-" + name), ec);
        }
    }
    return 0;
}

}  // namespace telega::bench::e11
