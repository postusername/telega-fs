// Проверки своей сборки SQLite (SPEC 15.2): флаги компиляции и session extension,
// на которых построен дельта-журнал (SPEC 6.6).

#include <gtest/gtest.h>
#include <sqlite3.h>
#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {

struct DbCloser {
    void operator()(sqlite3* db) const { sqlite3_close(db); }
};
using DbPtr = std::unique_ptr<sqlite3, DbCloser>;

struct SessionDeleter {
    void operator()(sqlite3_session* s) const { sqlite3session_delete(s); }
};
using SessionPtr = std::unique_ptr<sqlite3_session, SessionDeleter>;

struct Blob {
    int size = 0;
    void* data = nullptr;
    Blob() = default;
    Blob(const Blob&) = delete;
    Blob& operator=(const Blob&) = delete;
    Blob(Blob&& other) noexcept
        : size(std::exchange(other.size, 0)), data(std::exchange(other.data, nullptr)) {}
    Blob& operator=(Blob&&) = delete;
    ~Blob() { sqlite3_free(data); }
};

DbPtr open_memory_db() {
    sqlite3* raw = nullptr;
    EXPECT_EQ(sqlite3_open(":memory:", &raw), SQLITE_OK);
    return DbPtr{raw};
}

::testing::AssertionResult exec(sqlite3* db, const std::string& sql) {
    char* err = nullptr;
    const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err);
    const std::string message = err != nullptr ? err : "";
    sqlite3_free(err);
    if (rc != SQLITE_OK) {
        return ::testing::AssertionFailure() << sql << ": " << message;
    }
    return ::testing::AssertionSuccess();
}

std::vector<std::pair<long long, std::string>> dump(sqlite3* db) {
    std::vector<std::pair<long long, std::string>> rows;
    sqlite3_stmt* stmt = nullptr;
    EXPECT_EQ(sqlite3_prepare_v2(db, "SELECT id, name FROM t ORDER BY id", -1, &stmt, nullptr),
              SQLITE_OK);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        rows.emplace_back(sqlite3_column_int64(stmt, 0),
                          reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)));
    }
    sqlite3_finalize(stmt);
    return rows;
}

SessionPtr start_session(sqlite3* db) {
    sqlite3_session* raw = nullptr;
    EXPECT_EQ(sqlite3session_create(db, "main", &raw), SQLITE_OK);
    EXPECT_EQ(sqlite3session_attach(raw, nullptr), SQLITE_OK);
    return SessionPtr{raw};
}

Blob take_patchset(sqlite3_session* session) {
    Blob blob;
    EXPECT_EQ(sqlite3session_patchset(session, &blob.size, &blob.data), SQLITE_OK);
    return blob;
}

int abort_on_conflict(void* /*ctx*/, int /*conflict*/, sqlite3_changeset_iter* /*iter*/) {
    return SQLITE_CHANGESET_ABORT;
}

int apply(sqlite3* db, const Blob& patch) {
    return sqlite3changeset_apply(db, patch.size, patch.data, nullptr, abort_on_conflict, nullptr);
}

// Удаляет файл при выходе из области видимости, в том числе после сработавшего ASSERT.
class TempFile {
  public:
    explicit TempFile(const std::string& stem)
        : path_(std::filesystem::temp_directory_path() /
                (stem + "-" + std::to_string(::getpid()) + ".sqlite")) {
        std::filesystem::remove(path_);
    }
    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;
    TempFile(TempFile&&) = delete;
    TempFile& operator=(TempFile&&) = delete;
    ~TempFile() {
        std::error_code ignored;
        std::filesystem::remove(path_, ignored);
    }
    [[nodiscard]] const std::filesystem::path& path() const { return path_; }

  private:
    std::filesystem::path path_;
};

constexpr const char* kSchema = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
constexpr const char* kSeed = "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')";

// Две одинаковые базы с исходными строками, созданными до начала сессии: так патч содержит
// настоящие UPDATE и DELETE по существующим строкам, как в дельта-журнале (SPEC 6.6).
class SqliteSession : public ::testing::Test {
  protected:
    void SetUp() override {
        for (sqlite3* db : {live.get(), replica.get()}) {
            ASSERT_TRUE(exec(db, kSchema));
            ASSERT_TRUE(exec(db, kSeed));
        }
    }

    DbPtr live = open_memory_db();
    DbPtr replica = open_memory_db();
};

}  // namespace

TEST(Sqlite, CompileOptions) {
    EXPECT_EQ(sqlite3_threadsafe(), 2);
    for (const char* option :
         {"ENABLE_SESSION", "ENABLE_PREUPDATE_HOOK", "OMIT_LOAD_EXTENSION", "OMIT_DEPRECATED",
          "DQS=0", "DEFAULT_MEMSTATUS=0", "LIKE_DOESNT_MATCH_BLOBS"}) {
        EXPECT_EQ(sqlite3_compileoption_used(option), 1) << option;
    }
}

TEST_F(SqliteSession, PatchsetRoundTrip) {
    const SessionPtr session = start_session(live.get());
    ASSERT_TRUE(exec(live.get(), "INSERT INTO t VALUES (4, 'd')"));
    ASSERT_TRUE(exec(live.get(), "UPDATE t SET name = 'bb' WHERE id = 2"));
    ASSERT_TRUE(exec(live.get(), "DELETE FROM t WHERE id = 3"));

    const Blob patch = take_patchset(session.get());
    ASSERT_GT(patch.size, 0);
    ASSERT_EQ(apply(replica.get(), patch), SQLITE_OK);

    EXPECT_EQ(dump(replica.get()), dump(live.get()));
}

TEST_F(SqliteSession, ChangegroupConcatenatesPatchsets) {
    // Два патча подряд, как pending_patchsets после неудавшегося коммита (SPEC 6.6).
    SessionPtr session = start_session(live.get());
    ASSERT_TRUE(exec(live.get(), "UPDATE t SET name = 'aa' WHERE id = 1"));
    ASSERT_TRUE(exec(live.get(), "INSERT INTO t VALUES (4, 'd')"));
    const Blob first = take_patchset(session.get());

    session = start_session(live.get());
    ASSERT_TRUE(exec(live.get(), "UPDATE t SET name = 'dd' WHERE id = 4"));
    ASSERT_TRUE(exec(live.get(), "DELETE FROM t WHERE id = 2"));
    const Blob second = take_patchset(session.get());

    sqlite3_changegroup* group = nullptr;
    ASSERT_EQ(sqlite3changegroup_new(&group), SQLITE_OK);
    EXPECT_EQ(sqlite3changegroup_add(group, first.size, first.data), SQLITE_OK);
    EXPECT_EQ(sqlite3changegroup_add(group, second.size, second.data), SQLITE_OK);
    Blob merged;
    EXPECT_EQ(sqlite3changegroup_output(group, &merged.size, &merged.data), SQLITE_OK);
    sqlite3changegroup_delete(group);

    ASSERT_EQ(apply(replica.get(), merged), SQLITE_OK);
    EXPECT_EQ(dump(replica.get()), dump(live.get()));
}

TEST_F(SqliteSession, ConflictAbortsWholePatch) {
    // Реплика разошлась с живой базой: строки 2 в ней нет, применять UPDATE некуда.
    ASSERT_TRUE(exec(replica.get(), "DELETE FROM t WHERE id = 2"));
    const auto before = dump(replica.get());

    const SessionPtr session = start_session(live.get());
    ASSERT_TRUE(exec(live.get(), "INSERT INTO t VALUES (4, 'd')"));
    ASSERT_TRUE(exec(live.get(), "UPDATE t SET name = 'bb' WHERE id = 2"));
    const Blob patch = take_patchset(session.get());

    EXPECT_EQ(apply(replica.get(), patch), SQLITE_ABORT);
    EXPECT_EQ(dump(replica.get()), before) << "aborted patch must not be partially applied";
}

TEST(Sqlite, VacuumInto) {
    const DbPtr db = open_memory_db();
    ASSERT_TRUE(exec(db.get(), kSchema));
    ASSERT_TRUE(exec(db.get(), kSeed));

    const TempFile file{"telega-vacuum"};
    sqlite3_stmt* stmt = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(db.get(), "VACUUM INTO ?", -1, &stmt, nullptr), SQLITE_OK);
    const std::string path = file.path().string();
    sqlite3_bind_text(stmt, 1, path.c_str(), -1, SQLITE_TRANSIENT);
    const int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    ASSERT_EQ(rc, SQLITE_DONE) << sqlite3_errmsg(db.get());

    sqlite3* raw = nullptr;
    const int open_rc = sqlite3_open_v2(path.c_str(), &raw, SQLITE_OPEN_READONLY, nullptr);
    const DbPtr copy{raw};  // sqlite3_open_v2 выдаёт хендл и при ошибке, его надо закрыть
    ASSERT_EQ(open_rc, SQLITE_OK);
    EXPECT_EQ(dump(copy.get()), dump(db.get()));
}
