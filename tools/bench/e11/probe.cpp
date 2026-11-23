// E11: проверка тонких мест варианта (b) и SQLITE_ENABLE_SNAPSHOT. Каждая проверка печатает
// строку «ПРОВЕРКА: результат» и пишет её в probe.csv. Исходный код SQLite (backup.c в
// amalgamation) читался отдельно, ссылки на него — в отчёте.

#include <sqlite3.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <thread>

#include "common/files.h"
#include "crypto/index_blob.h"
#include "e11/e11.h"
#include "meta/index_db.h"
#include "meta/index_ops.h"

namespace telega::bench::e11 {

namespace {

using namespace std::chrono_literals;

std::string fingerprint(meta::Db& db) {
    meta::Stmt st{db,
                  "SELECT COUNT(*), SUM(ctime_ns % 1000003), SUM(mtime_ns % 1000003), SUM(atime_ns "
                  "% 1000003) FROM inodes"};
    st.step();
    return std::to_string(st.column_int64(0)) + "/" + std::to_string(st.column_int64(1)) + "/" +
           std::to_string(st.column_int64(2)) + "/" + std::to_string(st.column_int64(3));
}

std::string live_fingerprint(meta::MetaStore& m) {
    return m.read([&](meta::Db& db) { return fingerprint(db); });
}

std::string integrity(meta::Db& db) {
    meta::Stmt st{db, "PRAGMA integrity_check"};
    return st.step() ? st.column_text(0) : std::string{"no result"};
}

void remove_db(const fs::path& p) {
    std::error_code ec;
    for (const char* s : {"", "-wal", "-shm", "-journal"}) {
        fs::remove(p.string() + s, ec);
    }
}

int64_t file_size_or0(const fs::path& p) {
    std::error_code ec;
    const auto s = fs::file_size(p, ec);
    return ec ? 0 : static_cast<int64_t>(s);
}

// Синхронная серия транзакций писателя (как в прогоне, но в вызывающем потоке).
class Targets {
  public:
    explicit Targets(meta::MetaStore& m) : meta_(m) {
        m.read([&](meta::Db& db) {
            meta::Stmt f{db, "SELECT ino FROM inodes WHERE kind = 1 LIMIT 20000"};
            while (f.step()) {
                files_.push_back(f.column_int64(0));
            }
        });
    }
    void burst(int txns, uint64_t seed) {
        Rng rng(seed);
        for (int k = 0; k < txns; ++k) {
            meta_.write([&](meta::Db& db) { one(db, rng); });
        }
    }
    void one(meta::Db& db, Rng& rng) {
        for (int i = 0; i < 10; ++i) {
            t_ += 1000 + static_cast<int64_t>(rng.below(4000));
            meta::AttrUpdate u;
            u.atime_ns = t_;
            u.mtime_ns = t_;
            u.ctime_ns = t_;
            meta::update_attrs(db, files_[rng.below(files_.size())], u);
        }
    }
    meta::MetaStore& meta() { return meta_; }

  private:
    meta::MetaStore& meta_;
    std::vector<int64_t> files_;
    int64_t t_ = 1'800'000'000'000'000'000LL;
};

class BgWriter {
  public:
    BgWriter(Targets& t, uint64_t seed) : t_(t), seed_(seed) {
        th_ = std::thread([this] {
            Rng rng(seed_);
            while (!stop_.load()) {
                const auto a = Clock::now();
                t_.meta().write([&](meta::Db& db) { t_.one(db, rng); });
                const int64_t l = ns_since(a);
                ++ops_;
                int64_t m = max_ns_.load();
                while (l > m && !max_ns_.compare_exchange_weak(m, l)) {
                }
            }
        });
    }
    BgWriter(const BgWriter&) = delete;
    BgWriter& operator=(const BgWriter&) = delete;
    ~BgWriter() { stop(); }
    void stop() {
        if (th_.joinable()) {
            stop_.store(true);
            th_.join();
        }
    }
    int64_t take_max_ns() { return max_ns_.exchange(0); }
    int64_t ops() const { return ops_.load(); }

  private:
    Targets& t_;
    uint64_t seed_;
    std::atomic<bool> stop_{false};
    std::atomic<int64_t> max_ns_{0};
    std::atomic<int64_t> ops_{0};
    std::thread th_;
};

meta::Db reader_txn(const fs::path& index) {
    meta::OpenOptions ro;
    ro.read_only = true;
    meta::Db db = meta::Db::open(index, ro);
    db.exec("BEGIN");
    meta::Stmt st{db, "SELECT 1 FROM sqlite_schema LIMIT 1"};
    st.step();
    return db;
}

meta::Db dest_db(const fs::path& path) {
    remove_db(path);
    meta::OpenOptions o;
    o.create = true;
    o.wal = false;
    return meta::Db::open(path, o);
}

std::string clean(std::string s) {
    for (char& c : s) {
        if (c == ',' || c == '"' || c == '\n') {
            c = ';';
        }
    }
    return s;
}

struct Report {
    Csv csv;
    void line(const std::string& check, const std::string& result, const std::string& detail) {
        std::cout << "  " << check << ": " << result << (detail.empty() ? "" : "  [" + detail + "]")
                  << '\n';
        csv.row({clean(check), clean(result), detail.empty() ? "-" : clean(detail)});
        csv.flush();
    }
};

// ---------------------------------------------------------------------------------------------

// 1. Backup берёт уже открытую читательскую транзакцию источника, а не открывает свою.
void check_backup_uses_open_txn(Targets& t, const fs::path& tmp, Report& r) {
    auto& meta = t.meta();
    meta::Db src = reader_txn(meta.path());
    const std::string fp_cut = fingerprint(src);
    t.burst(200, 11);
    const std::string fp_after = live_fingerprint(meta);
    meta::Db dst = dest_db(tmp / "p1-a.sqlite");
    sqlite3_backup* b = sqlite3_backup_init(dst.handle(), "main", src.handle(), "main");
    const int rc = sqlite3_backup_step(b, -1);
    sqlite3_backup_finish(b);
    const bool still_in_txn = !src.autocommit();
    meta::Db copy = meta::Db::open(tmp / "p1-a.sqlite");
    const std::string fp_copy = fingerprint(copy);
    r.line("backup(-1) в открытой читательской транзакции",
           (rc == SQLITE_DONE && fp_copy == fp_cut && fp_copy != fp_after && still_in_txn) ? "OK"
                                                                                           : "FAIL",
           "rc=" + std::to_string(rc) +
               ", копия совпала со срезом: " + (fp_copy == fp_cut ? "да" : "нет") +
               ", отличается от живого индекса после 200 транзакций: " +
               (fp_copy != fp_after ? "да" : "нет") +
               ", транзакция источника осталась открытой: " + (still_in_txn ? "да" : "нет"));
    src.exec("COMMIT");

    // Контраст: без явной транзакции backup открывает свою на шаге и видит состояние на тот момент.
    meta::OpenOptions ro;
    ro.read_only = true;
    meta::Db src2 = meta::Db::open(meta.path(), ro);
    {
        meta::Stmt warm{src2, "SELECT 1 FROM sqlite_schema LIMIT 1"};
        warm.step();
    }
    const std::string fp_cut2 = live_fingerprint(meta);
    t.burst(200, 12);
    const std::string fp_after2 = live_fingerprint(meta);
    meta::Db dst2 = dest_db(tmp / "p1-b.sqlite");
    b = sqlite3_backup_init(dst2.handle(), "main", src2.handle(), "main");
    const int rc2 = sqlite3_backup_step(b, -1);
    sqlite3_backup_finish(b);
    meta::Db copy2 = meta::Db::open(tmp / "p1-b.sqlite");
    const std::string fp_copy2 = fingerprint(copy2);
    r.line("контраст: backup(-1) без явной транзакции",
           (rc2 == SQLITE_DONE && fp_copy2 == fp_after2 && fp_copy2 != fp_cut2)
               ? "OK (ожидаемо видит позднее состояние)"
               : "неожиданно",
           "копия = состояние на момент шага, а не на момент до записей: " +
               std::string(fp_copy2 == fp_after2 ? "да" : "нет"));
}

// 2. Перезапуск backup от записей других соединений.
void check_multistep(Targets& t, const fs::path& tmp, Report& r) {
    auto& meta = t.meta();
    for (const bool with_txn : {true, false}) {
        BgWriter bg(t, 21);
        std::this_thread::sleep_for(200ms);
        std::string fp_cut;
        meta::Db src = [&] {
            if (with_txn) {
                meta::Db d = reader_txn(meta.path());
                fp_cut = fingerprint(d);
                return d;
            }
            meta::OpenOptions ro;
            ro.read_only = true;
            return meta::Db::open(meta.path(), ro);
        }();
        meta::Db dst = dest_db(tmp / (with_txn ? "p2-a.sqlite" : "p2-b.sqlite"));
        sqlite3_backup* b = sqlite3_backup_init(dst.handle(), "main", src.handle(), "main");
        int steps = 0;
        int restarts = 0;
        int rc = SQLITE_OK;
        int last_remaining = -1;
        int busy = 0;
        const auto t0 = Clock::now();
        while (steps < 200000 && ns_since(t0) < 20'000'000'000LL) {
            rc = sqlite3_backup_step(b, 64);
            ++steps;
            const int rem = sqlite3_backup_remaining(b);
            if (last_remaining >= 0 && rem > last_remaining) {
                ++restarts;
            }
            last_remaining = rem;
            if (rc == SQLITE_DONE) {
                break;
            }
            if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
                ++busy;
                std::this_thread::sleep_for(1ms);
            } else if (rc != SQLITE_OK) {
                break;
            }
        }
        const int pagecount = sqlite3_backup_pagecount(b);
        sqlite3_backup_finish(b);
        bg.stop();
        std::string detail = "шагов " + std::to_string(steps) + " по 64 страницы, страниц " +
                             std::to_string(pagecount) + ", перезапусков (рост remaining) " +
                             std::to_string(restarts) + ", BUSY/LOCKED " + std::to_string(busy) +
                             ", итоговый rc " + std::to_string(rc) + ", время " +
                             std::to_string(ns_since(t0) / 1'000'000) +
                             " мс, транзакций писателя " + std::to_string(bg.ops());
        if (rc == SQLITE_DONE) {
            if (with_txn) {
                src.exec("COMMIT");
            }
            meta::Db copy = meta::Db::open(tmp / (with_txn ? "p2-a.sqlite" : "p2-b.sqlite"));
            const std::string fp = fingerprint(copy);
            const std::string ic = integrity(copy);
            std::string verify = "ok";
            try {
                meta::MetaStore::verify(copy);
            } catch (const std::exception& e) {
                verify = e.what();
            }
            detail += ", integrity_check " + ic + ", verify(): " + verify;
            if (with_txn) {
                detail +=
                    std::string(", копия совпала со срезом: ") + (fp == fp_cut ? "да" : "нет");
            }
            r.line(with_txn ? "backup по 64 страницы в читательской транзакции под писателем"
                            : "контраст: backup по 64 страницы без транзакции под писателем",
                   with_txn ? (fp == fp_cut && restarts == 0 && ic == "ok" ? "OK" : "FAIL")
                            : (ic == "ok" && verify == "ok" ? "завершился, копия согласована"
                                                            : "завершился, копия НЕСОГЛАСОВАНА"),
                   clean(detail));
        } else {
            r.line(with_txn ? "backup по 64 страницы в читательской транзакции под писателем"
                            : "контраст: backup по 64 страницы без транзакции под писателем",
                   with_txn ? "FAIL" : "не завершился за 20 с", clean(detail));
        }
    }
}

// 3. Поведение WAL: рост при открытой читательской транзакции и после её закрытия.
void check_wal(Targets& t, Report& r) {
    auto& meta = t.meta();
    const fs::path wal = fs::path(meta.path().string() + "-wal");
    {
        meta::Db db = meta::Db::open(meta.path());
        meta::Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
        st.step();
    }
    BgWriter bg(t, 31);
    std::this_thread::sleep_for(1500ms);
    const int64_t wal_free = file_size_or0(wal);
    int64_t peak_free = 0;
    for (int i = 0; i < 10; ++i) {
        peak_free = std::max(peak_free, file_size_or0(wal));
        std::this_thread::sleep_for(100ms);
    }
    const int64_t ops0 = bg.ops();
    meta::Db rd = reader_txn(meta.path());
    std::string timeline;
    const auto t0 = Clock::now();
    for (int i = 1; i <= 6; ++i) {
        std::this_thread::sleep_for(500ms);
        timeline +=
            std::to_string(i * 500) + "мс:" + std::to_string(file_size_or0(wal) / 1024) + "КиБ ";
    }
    const int64_t ops_held = bg.ops() - ops0;
    const double held_s = static_cast<double>(ns_since(t0)) / 1e9;
    const int64_t wal_held = file_size_or0(wal);
    bg.take_max_ns();
    rd.exec("COMMIT");
    std::this_thread::sleep_for(1500ms);
    const int64_t max_after_ns = bg.take_max_ns();
    const int64_t wal_after = file_size_or0(wal);
    bg.stop();
    r.line("рост WAL при открытой читательской транзакции (писатель на полной скорости)",
           "измерено",
           clean("WAL без читателя: " + std::to_string(wal_free / 1024) + " КиБ (пик за 1 с " +
                 std::to_string(peak_free / 1024) + "), при удержании " + timeline +
                 "; транзакций писателя за " + std::to_string(held_s) + " с удержания " +
                 std::to_string(ops_held) + "; через 1,5 с после закрытия читателя файл WAL " +
                 std::to_string(wal_after / 1024) +
                 " КиБ (не уменьшается), max задержка писателя после закрытия " +
                 std::to_string(max_after_ns / 1000) + " мкс; пик WAL при удержании " +
                 std::to_string(wal_held / 1024) + " КиБ"));
    // Файл WAL усекается только TRUNCATE или journal_size_limit на писательском соединении.
    {
        meta::Db db = meta::Db::open(meta.path());
        meta::Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
        st.step();
        r.line("wal_checkpoint(TRUNCATE) после закрытия читателя",
               st.column_int64(0) == 0 ? "OK, WAL усечён" : "busy",
               "размер WAL после: " + std::to_string(file_size_or0(wal)) + " байт");
    }
    // Стоимость RESTART/TRUNCATE при открытой читательской транзакции: блокирует писателя.
    {
        meta::Db rd2 = reader_txn(meta.path());
        BgWriter bg2(t, 32);
        std::this_thread::sleep_for(300ms);
        bg2.take_max_ns();
        meta::Db ck = meta::Db::open(meta.path());
        ck.busy_timeout(std::chrono::milliseconds(500));
        const auto c0 = Clock::now();
        meta::Stmt st{ck, "PRAGMA wal_checkpoint(TRUNCATE)"};
        st.step();
        const int64_t busy_flag = st.column_int64(0);
        const int64_t ck_ms = ns_since(c0) / 1'000'000;
        std::this_thread::sleep_for(100ms);
        const int64_t stall = bg2.take_max_ns();
        bg2.stop();
        rd2.exec("COMMIT");
        r.line(
            "wal_checkpoint(TRUNCATE) при открытой читательской транзакции (busy_timeout 500 мс)",
            "измерено",
            "busy=" + std::to_string(busy_flag) + ", вызов длился " + std::to_string(ck_ms) +
                " мс, максимальная задержка писателя за это время " +
                std::to_string(stall / 1'000'000) + " мс");
    }
    // journal_size_limit: WAL после контрольной точки усекается при следующем перезапуске журнала.
    {
        const fs::path p = meta.path().parent_path() / "p3-limit.sqlite";
        remove_db(p);
        meta::Db db = meta::Db::open(p, [] {
            meta::OpenOptions o;
            o.create = true;
            return o;
        }());
        db.exec("PRAGMA journal_size_limit = 1048576");
        db.exec("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)");
        meta::Db rd3 = reader_txn(p);
        {
            meta::Transaction tx{db};
            for (int i = 0; i < 3000; ++i) {
                meta::Stmt s{db, "INSERT INTO t (b) VALUES (zeroblob(4096))"};
                s.run();
            }
            tx.commit();
        }
        const int64_t big = file_size_or0(fs::path(p.string() + "-wal"));
        rd3.exec("COMMIT");
        db.exec("PRAGMA wal_checkpoint(PASSIVE)");
        db.exec("INSERT INTO t (b) VALUES (zeroblob(100))");
        const int64_t after = file_size_or0(fs::path(p.string() + "-wal"));
        r.line("journal_size_limit = 1 МиБ на писательском соединении",
               after < big ? "OK, WAL усекается" : "не усечён",
               "WAL пока читатель держал: " + std::to_string(big / 1024) +
                   " КиБ, после checkpoint и следующей записи: " + std::to_string(after / 1024) +
                   " КиБ");
        remove_db(p);
    }
}

// 4. Формат файла копии: байты 18–19 заголовка (2 = WAL) и совместимость с монтированием.
void check_header(Targets& t, const fs::path& tmp, Report& r) {
    auto& meta = t.meta();
    const auto hdr = [](const fs::path& p) {
        std::ifstream in(p, std::ios::binary);
        char h[100]{};
        in.read(h, sizeof h);
        return std::to_string(static_cast<int>(static_cast<unsigned char>(h[18]))) + "/" +
               std::to_string(static_cast<int>(static_cast<unsigned char>(h[19])));
    };
    {
        meta::Db db = meta::Db::open(meta.path());
        meta::Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
        st.step();
    }
    const fs::path vi = tmp / "p4-vacuum-into.sqlite";
    remove_db(vi);
    meta.vacuum_into(vi);
    const fs::path raw = tmp / "p4-raw.sqlite";
    meta::Db src = reader_txn(meta.path());
    {
        meta::Db dst = dest_db(raw);
        sqlite3_backup* b = sqlite3_backup_init(dst.handle(), "main", src.handle(), "main");
        sqlite3_backup_step(b, -1);
        sqlite3_backup_finish(b);
    }
    src.exec("COMMIT");
    const std::string h_raw = hdr(raw);
    const fs::path vac = tmp / "p4-vac.sqlite";
    fs::copy_file(raw, vac, fs::copy_options::overwrite_existing);
    {
        meta::OpenOptions o;
        o.wal = false;
        meta::Db db = meta::Db::open(vac, o);
        db.exec("PRAGMA journal_mode = DELETE");
        db.exec("VACUUM");
    }
    const std::string h_vac = hdr(vac);
    const auto zsize = [](const fs::path& p) {
        const Bytes z = crypto::zstd_compress(read_file(p), 9);
        return std::to_string(file_size_or0(p) / 1024) + " КиБ -> " +
               std::to_string(z.size() / 1024) + " КиБ";
    };
    const std::string sizes = "VACUUM INTO: " + zsize(vi) +
                              ", копия backup без VACUUM: " + zsize(raw) +
                              ", копия с VACUUM: " + zsize(vac);
    const std::string h_live = hdr(meta.path());
    const std::string h_vi = hdr(vi);
    // Монтирование собирает индекс из снимка этим же кодом: проверим на копии из backup + VACUUM.
    std::string mount = "ok";
    try {
        meta::MetaStore::build_from_chain(vac, {}, tmp / "p4-built.sqlite");
        meta::OpenOptions ro;
        ro.read_only = true;
        meta::Db db = meta::Db::open(tmp / "p4-built.sqlite", ro);
        mount = integrity(db);
    } catch (const std::exception& e) {
        mount = e.what();
    }
    r.line("байты 18/19 заголовка (1 = rollback, 2 = WAL)", "измерено",
           "живой индекс " + h_live + ", VACUUM INTO " + h_vi + ", копия backup до VACUUM " +
               h_raw + ", после journal_mode=DELETE и VACUUM " + h_vac +
               "; build_from_chain на копии: " + clean(mount));
    r.line("размеры файла и после zstd 9 (индекс после записей)", "измерено", sizes);
    for (const fs::path& p : {vi, raw, vac, tmp / "p4-built.sqlite"}) {
        remove_db(p);
    }
}

// 6. Стоимость забора патча и пересоздания сессии в зависимости от числа изменённых строк:
// сколько из блокировки уходит, если срез снимка не сериализует patchset, а только заменяет сессию.
void check_session_reset(Targets& t, const fs::path& tmp, Report& r) {
    auto& meta = t.meta();
    {
        meta::Db db = meta::Db::open(meta.path());
        meta::Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
        st.step();
    }
    const fs::path copy = tmp / "p6.sqlite";
    remove_db(copy);
    fs::copy_file(meta.path(), copy);
    meta::Db db = meta::Db::open(copy);
    std::vector<int64_t> files;
    {
        meta::Stmt f{db, "SELECT ino FROM inodes WHERE kind = 1"};
        while (f.step()) {
            files.push_back(f.column_int64(0));
        }
    }
    std::string detail;
    for (const int64_t rows : {int64_t{500}, int64_t{2000}, int64_t{10000}, int64_t{50000}}) {
        std::vector<double> patch_us;
        std::vector<double> reset_us;
        std::vector<double> fresh_us;
        for (int rep = 0; rep < 5; ++rep) {
            auto session = std::make_unique<meta::Session>(db);
            Rng rng(100 + static_cast<uint64_t>(rep));
            int64_t now = 1'900'000'000'000'000'000LL + rep * 1'000'000'000LL;
            {
                meta::Transaction tx{db};
                for (int64_t i = 0; i < rows; ++i) {
                    meta::AttrUpdate u;
                    u.atime_ns = ++now;
                    u.mtime_ns = now;
                    u.ctime_ns = now;
                    meta::update_attrs(db, files[rng.below(files.size())], u);
                }
                tx.commit();
            }
            auto t0 = Clock::now();
            const Bytes p = session->patchset();
            patch_us.push_back(static_cast<double>(ns_since(t0)) / 1e3);
            // take_patchset: patchset(), затем новая сессия, затем удаление старой.
            t0 = Clock::now();
            auto fresh = std::make_unique<meta::Session>(db);
            session = std::move(fresh);
            reset_us.push_back(static_cast<double>(ns_since(t0)) / 1e3);
            t0 = Clock::now();
            auto empty = std::make_unique<meta::Session>(db);
            fresh_us.push_back(static_cast<double>(ns_since(t0)) / 1e3);
            session = std::move(empty);
        }
        detail += std::to_string(rows) + " строк: patchset() " +
                  std::to_string(static_cast<int64_t>(summarize(patch_us).median)) +
                  " мкс, пересоздание сессии (новая + удаление старой) " +
                  std::to_string(static_cast<int64_t>(summarize(reset_us).median)) + " мкс; ";
    }
    remove_db(copy);
    r.line("стоимость patchset() и пересоздания сессии по числу строк (индекс " +
               meta.path().filename().string() + ")",
           "измерено", detail);
}

#ifdef SQLITE_ENABLE_SNAPSHOT
// 5. Ограничения sqlite3_snapshot_*.
void check_snapshot_api(Targets& t, Report& r) {
    auto& meta = t.meta();
    const auto truncate = [&] {
        meta::Db db = meta::Db::open(meta.path());
        meta::Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
        st.step();
        return st.column_int64(0);
    };
    // 5a. Пустой WAL: снимок получить нельзя.
    truncate();
    {
        meta::Db g = reader_txn(meta.path());
        sqlite3_snapshot* s = nullptr;
        const int rc = sqlite3_snapshot_get(g.handle(), "main", &s);
        if (s != nullptr) {
            sqlite3_snapshot_free(s);
        }
        r.line("sqlite3_snapshot_get при пустом WAL (после TRUNCATE-checkpoint, записей нет)",
               rc == SQLITE_OK ? "получен" : "ОШИБКА " + std::to_string(rc),
               "rc=" + std::to_string(rc) +
                   " ; файл -wal существует (его держит писательское соединение), документация "
                   "требует только ненулевого WAL у только что открытой базы");
        g.exec("COMMIT");
    }
    // 5b. Снимок живёт, пока его держит читатель; после контрольной точки и перезапуска WAL
    // недействителен.
    t.burst(50, 41);
    sqlite3_snapshot* snap = nullptr;
    {
        meta::Db g = reader_txn(meta.path());
        const int rc = sqlite3_snapshot_get(g.handle(), "main", &snap);
        r.line("sqlite3_snapshot_get после записей",
               rc == SQLITE_OK ? "OK" : "ОШИБКА " + std::to_string(rc), "rc=" + std::to_string(rc));
        g.exec("COMMIT");  // читатель отпущен: snap никто не держит
    }
    {
        const int64_t busy = truncate();  // WAL усечён
        t.burst(50, 42);                  // новый журнал с другим salt
        meta::OpenOptions ro;
        ro.read_only = true;
        meta::Db o = meta::Db::open(meta.path(), ro);
        o.exec("BEGIN");
        const int rc = sqlite3_snapshot_open(o.handle(), "main", snap);
        r.line("sqlite3_snapshot_open после TRUNCATE-checkpoint без держателя среза",
               rc == SQLITE_OK ? "открылся (неожиданно)" : "ОШИБКА, срез потерян",
               "rc=" + std::to_string(rc) + " (SQLITE_ERROR = 1, SQLITE_ERROR_SNAPSHOT = " +
                   std::to_string(SQLITE_ERROR_SNAPSHOT) +
                   "), truncate busy=" + std::to_string(busy));
        o.exec("ROLLBACK");
    }
    if (snap != nullptr) {
        sqlite3_snapshot_free(snap);
        snap = nullptr;
    }
    // 5c. Автоконтрольная точка писателя между get и open (держателя нет), писатель на полной
    // скорости.
    {
        int lost = 0;
        int ok = 0;
        for (int i = 0; i < 20; ++i) {
            t.burst(5, 50 + static_cast<uint64_t>(i));
            sqlite3_snapshot* s = nullptr;
            meta::Db g = reader_txn(meta.path());
            if (sqlite3_snapshot_get(g.handle(), "main", &s) != SQLITE_OK) {
                continue;
            }
            g.exec("COMMIT");  // отпустили до открытия
            BgWriter bg(t, 60 + static_cast<uint64_t>(i));
            std::this_thread::sleep_for(200ms);
            bg.stop();
            meta::OpenOptions ro;
            ro.read_only = true;
            meta::Db o = meta::Db::open(meta.path(), ro);
            o.exec("BEGIN");
            if (sqlite3_snapshot_open(o.handle(), "main", s) == SQLITE_OK) {
                ++ok;
            } else {
                ++lost;
            }
            o.exec("ROLLBACK");
            sqlite3_snapshot_free(s);
        }
        r.line("sqlite3_snapshot_open после 200 мс записей без держателя среза (20 попыток)",
               "измерено", "открылся " + std::to_string(ok) + ", потерян " + std::to_string(lost));
    }
}
#endif

}  // namespace

int run_probe(const Options& o) {
    Report r{Csv(o.out / ("probe-" + o.probe_corpus + ".csv"), {"check", "result", "detail"})};
    CorpusInfo info;
    auto env = build_corpus(o, o.probe_corpus, info);
    auto& meta = env->mounted->meta();
    const fs::path tmp = env->paths.tmp();
    Targets t(meta);
    std::cout << "probe on " << o.probe_corpus << " (" << info.index_bytes / (1024 * 1024)
              << " MiB)\n";
    check_backup_uses_open_txn(t, tmp, r);
    check_multistep(t, tmp, r);
    check_wal(t, r);
    check_header(t, tmp, r);
    r.line("отрицательный контроль сравнения (снимок без патчей после среза обязан расходиться)",
           "измерено", negative_control(o, *env));
    check_session_reset(t, tmp, r);
#ifdef SQLITE_ENABLE_SNAPSHOT
    check_snapshot_api(t, r);
#else
    r.line("sqlite3_snapshot_*", "пропущено", "сборка без SQLITE_ENABLE_SNAPSHOT");
#endif
    env.reset();
    if (!o.keep) {
        std::error_code ec;
        fs::remove_all(o.work / ("fs-" + o.probe_corpus), ec);
    }
    return 0;
}

}  // namespace telega::bench::e11
