// Обёртки SQLite (meta/sqlite.h): соединение, выражения, транзакции, сессии и патчи.

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <stdexcept>
#include <string>
#include <vector>

#include "meta/sqlite.h"
#include "test_util.h"

namespace telega::meta {
namespace {

using test::bytes_of;
using test::TempDir;

Db open_db(const TempDir& dir, const char* name = "t.sqlite") {
    OpenOptions options;
    options.create = true;
    return Db::open(dir / name, options);
}

std::string pragma_text(Db& db, const char* pragma) {
    Stmt st{db, std::string("PRAGMA ") + pragma};
    EXPECT_TRUE(st.step());
    return st.column_text(0);
}

std::vector<std::string> rows(Db& db) {
    std::vector<std::string> out;
    Stmt st{db, "SELECT id, name FROM t ORDER BY id"};
    while (st.step()) {
        out.push_back(std::to_string(st.column_int64(0)) + ":" + st.column_text(1));
    }
    return out;
}

TEST(SqliteWrapper, OpenSetsPragmas) {
    const TempDir dir;
    Db db = open_db(dir);
    EXPECT_EQ(pragma_text(db, "journal_mode"), "wal");
    EXPECT_EQ(pragma_text(db, "synchronous"), "2");  // FULL
    EXPECT_EQ(pragma_text(db, "foreign_keys"), "1");
    db.set_foreign_keys(false);
    EXPECT_EQ(pragma_text(db, "foreign_keys"), "0");
}

TEST(SqliteWrapper, OpenMissingFileFails) {
    const TempDir dir;
    try {
        (void)Db::open(dir / "missing.sqlite");
        FAIL() << "expected SqliteError";
    } catch (const SqliteError& e) {
        EXPECT_EQ(e.primary_code(), SQLITE_CANTOPEN);
    }
}

TEST(SqliteWrapper, ReadOnlyConnectionRejectsWrites) {
    const TempDir dir;
    {
        Db db = open_db(dir);
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    }
    OpenOptions ro;
    ro.read_only = true;
    Db db = Db::open(dir / "t.sqlite", ro);
    try {
        db.exec("INSERT INTO t VALUES (1, 'a')");
        FAIL() << "expected SqliteError";
    } catch (const SqliteError& e) {
        EXPECT_EQ(e.primary_code(), SQLITE_READONLY);
    }
}

TEST(SqliteWrapper, BindAndColumns) {
    const TempDir dir;
    Db db = open_db(dir);
    db.exec("CREATE TABLE v (id INTEGER PRIMARY KEY, i INTEGER, b BLOB, s TEXT)");
    {
        Stmt st{db, "INSERT INTO v VALUES (?, ?, ?, ?)"};
        st.bind(1, int64_t{1}).bind(2, int64_t{-5}).bind(3, ByteSpan{bytes_of("ab")});
        st.bind_text(4, "hello").run();
        st.reset();
        // Пустой буфер — пустой BLOB, а не NULL; пустая строка — пустой TEXT.
        st.bind(1, int64_t{2}).bind(2, std::optional<int64_t>{}).bind(3, ByteSpan{});
        st.bind_text(4, "").run();
        st.reset();
        st.bind(1, int64_t{3}).bind_null(2).bind_null(3).bind_null(4).run();
    }
    Stmt st{db, "SELECT i, b, s, typeof(b) FROM v ORDER BY id"};
    ASSERT_TRUE(st.step());
    EXPECT_EQ(st.column_int64(0), -5);
    EXPECT_EQ(st.column_blob(1), bytes_of("ab"));
    EXPECT_EQ(st.column_text(2), "hello");
    ASSERT_TRUE(st.step());
    EXPECT_FALSE(st.column_opt_int64(0).has_value());
    ASSERT_TRUE(st.column_opt_blob(1).has_value());
    EXPECT_TRUE(test::must(st.column_opt_blob(1)).empty());
    EXPECT_EQ(st.column_text(3), "blob");
    EXPECT_EQ(st.column_type(2), SQLITE_TEXT);
    ASSERT_TRUE(st.step());
    EXPECT_TRUE(st.is_null(0));
    EXPECT_FALSE(st.column_opt_blob(1).has_value());
    EXPECT_EQ(st.column_text(3), "null");
    EXPECT_FALSE(st.step());
    EXPECT_EQ(st.column_count(), 4);
}

TEST(SqliteWrapper, ErrorsCarryExtendedCodes) {
    const TempDir dir;
    Db db = open_db(dir);
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)");
    db.exec("INSERT INTO t VALUES (1, 'a')");
    try {
        db.exec("INSERT INTO t VALUES (2, 'a')");
        FAIL() << "expected SqliteError";
    } catch (const SqliteError& e) {
        EXPECT_EQ(e.code(), SQLITE_CONSTRAINT_UNIQUE);
        EXPECT_EQ(e.primary_code(), SQLITE_CONSTRAINT);
        EXPECT_NE(std::string(e.what()).find("UNIQUE"), std::string::npos) << e.what();
    }
    EXPECT_THROW(Stmt(db, "SELECT nonsense FROM"), SqliteError);
    EXPECT_THROW(Stmt(db, "SELECT ?").bind(2, int64_t{1}), SqliteError);
}

TEST(SqliteWrapper, CachedStatementsAreResetAndReentrant) {
    const TempDir dir;
    Db db = open_db(dir);
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    db.exec("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    const char* sql = "SELECT name FROM t WHERE id >= ? ORDER BY id";
    sqlite3_stmt* first_handle = nullptr;
    {
        Stmt outer = db.cached(sql);
        first_handle = outer.handle();
        outer.bind(1, int64_t{1});
        ASSERT_TRUE(outer.step());
        // То же выражение, пока внешнее занято: отдельный объект, не из кэша.
        Stmt inner = db.cached(sql);
        EXPECT_NE(inner.handle(), first_handle);
        inner.bind(1, int64_t{2});
        ASSERT_TRUE(inner.step());
        EXPECT_EQ(inner.column_text(0), "b");
        EXPECT_EQ(outer.column_text(0), "a");
    }
    // После возврата в кэш выражение сброшено: привязки очищены, нет открытого чтения.
    Stmt again = db.cached(sql);
    EXPECT_EQ(again.handle(), first_handle);
    EXPECT_FALSE(again.step()) << "NULL binding must match nothing";
    EXPECT_TRUE(db.autocommit());
}

TEST(SqliteWrapper, TransactionCommitRollbackAndNesting) {
    const TempDir dir;
    Db db = open_db(dir);
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    {
        Transaction tx{db};
        db.exec("INSERT INTO t VALUES (1, 'a')");
        tx.commit();
    }
    {
        const Transaction tx{db};
        db.exec("INSERT INTO t VALUES (2, 'b')");
    }  // откат деструктором
    EXPECT_EQ(rows(db), (std::vector<std::string>{"1:a"}));
    {
        Transaction outer{db};
        db.exec("INSERT INTO t VALUES (3, 'c')");
        {
            const Transaction inner{db};  // SAVEPOINT
            db.exec("INSERT INTO t VALUES (4, 'd')");
        }
        {
            Transaction inner{db};
            db.exec("INSERT INTO t VALUES (5, 'e')");
            inner.commit();
        }
        EXPECT_FALSE(db.autocommit());
        outer.commit();
    }
    EXPECT_TRUE(db.autocommit());
    EXPECT_EQ(rows(db), (std::vector<std::string>{"1:a", "3:c", "5:e"}));
}

TEST(SqliteWrapper, SessionPatchsetConcatApply) {
    const TempDir dir;
    Db live = open_db(dir, "live.sqlite");
    Db replica = open_db(dir, "replica.sqlite");
    for (Db* db : {&live, &replica}) {
        db->exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
        db->exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    }
    std::vector<Bytes> patches;
    {
        const Session session{live};
        EXPECT_TRUE(session.empty());
        live.exec("UPDATE t SET name = 'aa' WHERE id = 1; INSERT INTO t VALUES (4, 'd')");
        EXPECT_FALSE(session.empty());
        EXPECT_GT(session.memory_used(), 0);
        patches.push_back(session.patchset());
    }
    {
        const Session session{live};
        live.exec("UPDATE t SET name = 'dd' WHERE id = 4; DELETE FROM t WHERE id = 2");
        patches.push_back(session.patchset());
    }
    patches.emplace_back();  // пустой патч пропускается
    const Bytes merged = concat_patchsets(patches);
    int conflicts = 0;
    apply_patchset(replica, merged, [&](int, sqlite3_changeset_iter*) {
        ++conflicts;
        return ConflictAction::abort;
    });
    EXPECT_EQ(conflicts, 0);
    EXPECT_EQ(rows(replica), rows(live));
}

TEST(SqliteWrapper, ApplyConflictAbortsAndHandlerExceptionPropagates) {
    const TempDir dir;
    Db live = open_db(dir, "live.sqlite");
    Db replica = open_db(dir, "replica.sqlite");
    for (Db* db : {&live, &replica}) {
        db->exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
        db->exec("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    }
    replica.exec("DELETE FROM t WHERE id = 2");
    const auto before = rows(replica);
    Bytes patch;
    {
        const Session session{live};
        live.exec("INSERT INTO t VALUES (3, 'c'); UPDATE t SET name = 'bb' WHERE id = 2");
        patch = session.patchset();
    }
    int seen = 0;
    try {
        apply_patchset(replica, patch, [&](int conflict, sqlite3_changeset_iter*) {
            ++seen;
            EXPECT_EQ(conflict, SQLITE_CHANGESET_NOTFOUND);
            return ConflictAction::abort;
        });
        FAIL() << "expected SqliteError";
    } catch (const SqliteError& e) {
        EXPECT_EQ(e.primary_code(), SQLITE_ABORT);
    }
    EXPECT_EQ(seen, 1);
    EXPECT_EQ(rows(replica), before) << "aborted patch must not be partially applied";

    EXPECT_THROW(apply_patchset(replica, patch,
                                [](int, sqlite3_changeset_iter*) -> ConflictAction {
                                    throw std::runtime_error("handler failed");
                                }),
                 std::runtime_error);
    EXPECT_EQ(rows(replica), before);

    // OMIT пропускает конфликтующее изменение, остальное применяется.
    apply_patchset(replica, patch,
                   [](int, sqlite3_changeset_iter*) { return ConflictAction::omit; });
    EXPECT_EQ(rows(replica), (std::vector<std::string>{"1:a", "3:c"}));
}

}  // namespace
}  // namespace telega::meta
