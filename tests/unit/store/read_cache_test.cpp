// Учёт кэша чтения (SPEC 8.2): st_blocks, LRU до 90 % лимита, занятые файлы, сверка с диском
// после перезапуска, таблица read_cache в state.sqlite.

#include "store/read_cache.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "meta/sqlite.h"

namespace telega::store {
namespace {

namespace fs = std::filesystem;

class ReadCacheTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::string tmpl = "/tmp/telega_rcache_XXXXXX";
        ASSERT_NE(::mkdtemp(tmpl.data()), nullptr);
        root = tmpl;
        dir = root / "tdfiles";
        fs::create_directories(dir / "documents");
        state_db = root / "state.sqlite";
        meta::Db::open(state_db, meta::OpenOptions{.create = true}).exec("SELECT 1");
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    // Файл на диске в 4 КиБ (один блок).
    [[nodiscard]] std::string make_file(const std::string& name, size_t bytes = 4096) const {
        const fs::path p = dir / "documents" / name;
        std::ofstream(p, std::ios::binary) << std::string(bytes, 'x');
        return p.string();
    }
    static uint64_t blocks_of(const std::string& path) {
        struct stat st {};
        EXPECT_EQ(::stat(path.c_str(), &st), 0);
        return static_cast<uint64_t>(st.st_blocks) * 512;
    }
    ReadCacheOptions options(uint64_t limit) {
        ReadCacheOptions o;
        o.state_db = state_db;
        o.dir = dir;
        o.limit = limit;
        o.now_ns = [this] { return ++clock_ns; };
        return o;
    }
    [[nodiscard]] std::vector<std::pair<std::string, std::string>> table_rows() const {
        meta::Db db = meta::Db::open(state_db);
        meta::Stmt st{db, "SELECT path, remote_file_id FROM read_cache ORDER BY path"};
        std::vector<std::pair<std::string, std::string>> rows;
        while (st.step()) {
            rows.emplace_back(st.column_text(0), st.column_text(1));
        }
        return rows;
    }

    fs::path root;
    fs::path dir;
    fs::path state_db;
    int64_t clock_ns = 1000;
};

TEST_F(ReadCacheTest, NoteCountsBlocksAndIgnoresPathsOutsideTheCache) {
    ReadCache cache(options(0));
    const std::string a = make_file("a", 10'000);
    cache.note(a, "remote-a");
    EXPECT_EQ(cache.total_bytes(), blocks_of(a));
    const auto entries = cache.entries();
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_EQ(entries[0].remote_file_id, "remote-a");

    // Повторный учёт не удваивает сумму, пустой remote_file_id не затирает известный.
    cache.note(a, "");
    EXPECT_EQ(cache.total_bytes(), blocks_of(a));
    EXPECT_EQ(cache.entries()[0].remote_file_id, "remote-a");

    // Файл вне tdfiles/ (отправленный этим процессом) в учёт не входит.
    const fs::path outside = root / "sent.bin";
    std::ofstream(outside) << "x";
    cache.note(outside.string(), "remote-x");
    EXPECT_EQ(cache.entries().size(), 1U);
}

TEST_F(ReadCacheTest, NoteOfMissingFileDropsTheEntry) {
    ReadCache cache(options(0));
    const std::string a = make_file("a");
    cache.note(a, "r");
    fs::remove(a);
    cache.note(a, "r");
    EXPECT_TRUE(cache.entries().empty());
    EXPECT_EQ(cache.total_bytes(), 0U);
}

TEST_F(ReadCacheTest, NoVictimsWhileWithinTheLimit) {
    ReadCache cache(options(uint64_t{10} * 4096));
    for (int i = 0; i < 10; ++i) {
        cache.note(make_file("f" + std::to_string(i)), "r");
    }
    EXPECT_FALSE(cache.over_limit());
    EXPECT_TRUE(cache.choose_victims({}).empty());
}

TEST_F(ReadCacheTest, VictimsAreLeastRecentlyUsedDownTo90PercentOfTheLimit) {
    constexpr int kFiles = 12;
    const uint64_t block = blocks_of(make_file("probe"));
    fs::remove(dir / "documents" / "probe");
    ReadCache cache(options(10 * block));
    std::vector<std::string> paths;
    for (int i = 0; i < kFiles; ++i) {
        paths.push_back(make_file("f" + std::to_string(100 + i)));
        cache.note(paths.back(), "r");
    }
    // f100 использовали позже всех: он не должен вылететь первым.
    cache.note(paths[0], "r");
    ASSERT_TRUE(cache.over_limit());
    const auto victims = cache.choose_victims({});
    // 12 файлов, цель 9: вытесняются три самых давних (f101, f102, f103), а не f100.
    ASSERT_EQ(victims.size(), 3U);
    EXPECT_EQ(victims[0].path, paths[1]);
    EXPECT_EQ(victims[1].path, paths[2]);
    EXPECT_EQ(victims[2].path, paths[3]);
    for (const auto& v : victims) {
        cache.forget(v.path);
    }
    EXPECT_EQ(cache.total_bytes(), 9 * block);
}

TEST_F(ReadCacheTest, BusyFilesAreSkipped) {
    const uint64_t block = blocks_of(make_file("probe"));
    fs::remove(dir / "documents" / "probe");
    ReadCache cache(options(2 * block));
    std::vector<std::string> paths;
    for (int i = 0; i < 4; ++i) {
        paths.push_back(make_file("f" + std::to_string(i)));
        cache.note(paths.back(), "r");
    }
    // Лимит 2 блока, цель 1,8: нужно убрать всё, но самый давний файл занят.
    const auto victims =
        cache.choose_victims([&](const ReadCacheEntry& e) { return e.path == paths[0]; });
    std::set<std::string> names;
    for (const auto& v : victims) {
        names.insert(v.path);
    }
    EXPECT_FALSE(names.contains(paths[0]));
    EXPECT_EQ(names.size(), 3U);
}

TEST_F(ReadCacheTest, FlushWritesRowsAndRemovals) {
    {
        ReadCache cache(options(0));
        cache.note(make_file("a"), "remote-a");
        cache.note(make_file("b"), "remote-b");
        cache.flush();
        EXPECT_EQ(table_rows().size(), 2U);
        cache.forget((dir / "documents" / "a").string());
        cache.flush();
        const auto rows = table_rows();
        ASSERT_EQ(rows.size(), 1U);
        EXPECT_EQ(rows[0].first, (dir / "documents" / "b").string());
        EXPECT_EQ(rows[0].second, "remote-b");
        cache.note(make_file("c"), "remote-c");  // без явного flush: сбросит деструктор
    }
    EXPECT_EQ(table_rows().size(), 2U);
}

TEST_F(ReadCacheTest, ReconcileAdoptsFileWithoutRowAndDropsRowWithoutFile) {
    const std::string kept = make_file("kept", 8192);
    const std::string orphan = make_file("orphan", 4096);  // после сбоя строки нет
    const std::string gone = make_file("gone");
    {
        ReadCache cache(options(0));
        cache.note(kept, "remote-kept");
        cache.note(gone, "remote-gone");
        cache.flush();
    }
    fs::remove(gone);  // строка осталась, файла нет
    // Время доступа найденного файла берётся из mtime.
    const std::array<timespec, 2> times{{{1'000'000, 0}, {1'000'000, 0}}};
    ASSERT_EQ(::utimensat(AT_FDCWD, orphan.c_str(), times.data(), 0), 0);

    ReadCache cache(options(0));
    const ReconcileReport r = cache.reconcile();
    EXPECT_EQ(r.kept, 1U);
    EXPECT_EQ(r.adopted, 1U);
    EXPECT_EQ(r.dropped, 1U);
    const auto entries = cache.entries();
    ASSERT_EQ(entries.size(), 2U);
    for (const auto& e : entries) {
        if (e.path == orphan) {
            EXPECT_EQ(e.last_access_ns, 1'000'000LL * 1'000'000'000LL);
            EXPECT_TRUE(e.remote_file_id.empty());
        } else {
            EXPECT_EQ(e.path, kept);
            EXPECT_EQ(e.remote_file_id, "remote-kept");  // пережил перезапуск
        }
    }
    EXPECT_EQ(cache.total_bytes(), blocks_of(kept) + blocks_of(orphan));
    // Результат сверки уже в таблице.
    const auto rows = table_rows();
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_NE(rows[0].first, gone);
    EXPECT_NE(rows[1].first, gone);
}

// Файлы, учтённые до сверки (блобы индекса читаются раньше start_read_cache), не теряют
// remote_file_id и время доступа при reconcile(); таблица дополняет их, а не заменяет.
TEST_F(ReadCacheTest, ReconcileKeepsRowsNotedBeforeIt) {
    const std::string known = make_file("known");
    const std::string in_table = make_file("in_table");
    {
        ReadCache first(options(0));
        first.note(in_table, "remote-table");
        first.flush();
    }
    ReadCache cache(options(0));
    cache.note(known, "remote-known");
    cache.note(in_table, "remote-newer");
    const int64_t noted_at = [&] {
        for (const ReadCacheEntry& e : cache.entries()) {
            if (e.path == in_table) {
                return e.last_access_ns;
            }
        }
        return int64_t{0};
    }();
    const ReconcileReport report = cache.reconcile();
    EXPECT_EQ(report.dropped, 0U);
    std::map<std::string, ReadCacheEntry> by_path;
    for (const ReadCacheEntry& e : cache.entries()) {
        by_path[e.path] = e;
    }
    ASSERT_EQ(by_path.size(), 2U);
    EXPECT_EQ(by_path[known].remote_file_id, "remote-known");
    EXPECT_EQ(by_path[in_table].remote_file_id, "remote-newer");
    EXPECT_GE(by_path[in_table].last_access_ns, noted_at);
    cache.flush();
    const auto rows = table_rows();
    ASSERT_EQ(rows.size(), 2U);
    for (const auto& [path, remote] : rows) {
        EXPECT_FALSE(remote.empty());
    }
}

TEST_F(ReadCacheTest, WorksWithoutStateDatabase) {
    ReadCacheOptions o = options(0);
    o.state_db.clear();
    ReadCache cache(o);
    cache.note(make_file("a"), "r");
    cache.flush();  // тихо ничего не делает
    EXPECT_EQ(cache.entries().size(), 1U);
}

}  // namespace
}  // namespace telega::store
