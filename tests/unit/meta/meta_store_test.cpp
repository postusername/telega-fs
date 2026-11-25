// MetaStore (meta/index_db.h): dirty-хук, транзакции записи, читатели в потоках, замер
// забора патча.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "meta/errors.h"
#include "meta/index_db.h"
#include "meta/index_ops.h"
#include "test_util.h"

namespace telega::meta {
namespace {

using test::bytes_of;
using test::new_node;

class MetaStoreTest : public ::testing::Test {
  protected:
    void SetUp() override {
        MetaStore::create(path(), test::test_params());
        store_ = std::make_unique<MetaStore>(path(), [this] { ++hook_calls_; });
    }

    [[nodiscard]] std::filesystem::path path() const { return dir_ / "index.sqlite"; }

    int64_t create_file(std::string_view name) {
        return store_->write([&](Db& db) { return insert_inode(db, new_node(kRootIno, name)); });
    }

    test::TempDir dir_;
    std::atomic<int> hook_calls_{0};
    std::unique_ptr<MetaStore> store_;
};

TEST_F(MetaStoreTest, OpenRequiresExistingIndex) {
    EXPECT_THROW(MetaStore(dir_ / "missing.sqlite"), std::runtime_error);
    {
        OpenOptions options;
        options.create = true;
        Db db = Db::open(dir_ / "other.sqlite", options);
        db.exec("CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)");
    }
    EXPECT_THROW(MetaStore(dir_ / "other.sqlite"), IndexCorrupt);
}

TEST_F(MetaStoreTest, DirtyHookOncePerTake) {
    EXPECT_FALSE(store_->dirty_since_take());
    create_file("a");
    create_file("b");
    EXPECT_EQ(hook_calls_.load(), 1);
    EXPECT_TRUE(store_->dirty_since_take());

    (void)store_->take_patchset();
    EXPECT_FALSE(store_->dirty_since_take());
    create_file("c");
    create_file("d");
    EXPECT_EQ(hook_calls_.load(), 2);

    store_->vacuum_into(dir_ / "snap.sqlite");
    EXPECT_FALSE(store_->dirty_since_take());
    create_file("e");
    EXPECT_EQ(hook_calls_.load(), 3);
}

TEST_F(MetaStoreTest, HousekeepingIsRecordedButNotDirty) {
    (void)store_->take_patchset();
    store_->write([](Db& db) { add_history(db, 1, 10, BlobKind::snapshot); },
                  WriteKind::housekeeping);
    EXPECT_EQ(hook_calls_.load(), 0);
    EXPECT_FALSE(store_->dirty_since_take());
    const Bytes patch = store_->take_patchset();
    EXPECT_FALSE(patch.empty()) << "housekeeping writes must reach the next patch";

    // После служебной записи первая пользовательская всё равно вызывает хук.
    store_->write([](Db& db) { add_history(db, 2, 11, BlobKind::delta); }, WriteKind::housekeeping);
    create_file("x");
    EXPECT_EQ(hook_calls_.load(), 1);
}

TEST_F(MetaStoreTest, HookFailureCancelsWrite) {
    store_->set_dirty_hook([] { throw std::runtime_error("state.sqlite is gone"); });
    EXPECT_THROW(create_file("a"), std::runtime_error);
    EXPECT_FALSE(store_->dirty_since_take());
    store_->read([](Db& db) { EXPECT_FALSE(lookup(db, kRootIno, bytes_of("a"))); });
    int calls = 0;
    store_->set_dirty_hook([&] { ++calls; });
    create_file("a");
    EXPECT_EQ(calls, 1);
}

TEST_F(MetaStoreTest, FailedWriteRollsBackAndKeepsSessionClean) {
    (void)store_->take_patchset();
    EXPECT_THROW(store_->write([](Db& db) {
        insert_inode(db, new_node(kRootIno, "a"));
        throw std::runtime_error("boom");
    }),
                 std::runtime_error);
    store_->read([](Db& db) { EXPECT_FALSE(lookup(db, kRootIno, bytes_of("a"))); });
    // Откаченные изменения в патч не попадают.
    EXPECT_TRUE(store_->take_patchset().empty());
}

TEST_F(MetaStoreTest, NestingRules) {
    EXPECT_THROW(store_->write([&](Db&) { store_->write([](Db&) {}); }), std::logic_error);
    EXPECT_THROW(store_->write([&](Db&) { (void)store_->take_patchset(); }), std::logic_error);
    // with_lock допускает запись и забор патча внутри (CommitManager).
    const bool dirty = store_->with_lock([&] {
        create_file("a");
        const bool was_dirty = store_->dirty_since_take();
        (void)store_->take_patchset();
        return was_dirty;
    });
    EXPECT_TRUE(dirty);
    EXPECT_FALSE(store_->dirty_since_take());
}

TEST_F(MetaStoreTest, ReadSeesCommittedStateOnly) {
    store_->write([&](Db& db) {
        insert_inode(db, new_node(kRootIno, "a"));
        // Читатель не видит незакоммиченное.
        store_->read([](Db& rdb) { EXPECT_FALSE(lookup(rdb, kRootIno, bytes_of("a"))); });
        EXPECT_TRUE(lookup(db, kRootIno, bytes_of("a")));
    });
    store_->read([](Db& db) {
        EXPECT_TRUE(lookup(db, kRootIno, bytes_of("a")));
        // Вложенный read — та же транзакция и то же соединение.
        EXPECT_FALSE(db.autocommit());
    });
    // Читательское соединение только для чтения.
    EXPECT_THROW(store_->read([](Db& db) { insert_inode(db, new_node(kRootIno, "b")); }),
                 SqliteError);
}

TEST_F(MetaStoreTest, ReadersInParallelWithWriter) {
    constexpr int kReaders = 4;
    constexpr int kFiles = 300;
    std::atomic<bool> done{false};
    std::atomic<int64_t> reads{0};
    std::vector<std::thread> readers;
    std::atomic<int> failures{0};
    readers.reserve(kReaders);
    for (int t = 0; t < kReaders; ++t) {
        readers.emplace_back([&] {
            do {
                store_->read([&](Db& db) {
                    // Внутри одной читающей транзакции снимок согласован.
                    const int64_t n = count_children(db, kRootIno);
                    const auto list = list_children(db, kRootIno, std::nullopt, 1'000'000);
                    if (static_cast<int64_t>(list.size()) != n) {
                        ++failures;
                    }
                    for (const Inode& node : list) {
                        if (node.size != node.mtime_ns) {  // писатель меняет их вместе
                            ++failures;
                        }
                    }
                });
                ++reads;
            } while (!done.load());
        });
    }
    std::thread taker([&] {
        while (!done.load()) {
            (void)store_->take_patchset();
            std::this_thread::yield();
        }
    });
    for (int i = 0; i < kFiles; ++i) {
        store_->write([&](Db& db) {
            NewInode n = new_node(kRootIno, "f" + std::to_string(i));
            n.now_ns = 0;
            const int64_t ino = insert_inode(db, n);
            AttrUpdate u;
            u.size = ino;
            u.mtime_ns = ino;
            update_attrs(db, ino, u);
        });
    }
    done.store(true);
    for (auto& t : readers) {
        t.join();
    }
    taker.join();
    EXPECT_EQ(failures.load(), 0);
    EXPECT_GT(reads.load(), 0);
    store_->read([&](Db& db) { EXPECT_EQ(count_children(db, kRootIno), kFiles); });
}

// Замер забора патча под блокировкой на заметной нагрузке (для E1): 20 000 новых
// файлов, переименование каталога и правка атрибутов.
TEST_F(MetaStoreTest, TakePatchsetTiming) {
    constexpr int kFiles = 20'000;
    const int64_t dir = store_->write(
        [&](Db& db) { return insert_inode(db, new_node(kRootIno, "dir", InodeKind::dir)); });
    store_->write([&](Db& db) {
        for (int i = 0; i < kFiles; ++i) {
            insert_inode(db, new_node(dir, "file-" + std::to_string(i)));
        }
    });
    TakeStats create_stats;
    const Bytes create_patch = store_->take_patchset(&create_stats);

    store_->write([&](Db& db) {
        const int64_t new_parent = kRootIno;
        move_inode(db, dir, new_parent, bytes_of("renamed"));
        for (int64_t ino = dir + 1; ino <= dir + kFiles; ino += 2) {
            AttrUpdate u;
            u.mtime_ns = ino;
            update_attrs(db, ino, u);
        }
    });
    TakeStats touch_stats;
    const Bytes touch_patch = store_->take_patchset(&touch_stats);

    TakeStats empty_stats;
    EXPECT_TRUE(store_->take_patchset(&empty_stats).empty());

    EXPECT_EQ(create_stats.patch_bytes, create_patch.size());
    EXPECT_GT(create_patch.size(), touch_patch.size());
    const auto us = [](const TakeStats& s) {
        return std::chrono::duration_cast<std::chrono::microseconds>(s.locked).count();
    };
    RecordProperty("take_create_20k_us", std::to_string(us(create_stats)));
    RecordProperty("take_create_20k_bytes", std::to_string(create_patch.size()));
    RecordProperty("take_touch_10k_us", std::to_string(us(touch_stats)));
    RecordProperty("take_touch_10k_bytes", std::to_string(touch_patch.size()));
    std::printf(
        "[ E1 ] take_patchset: create 20k files %lld us, %zu bytes; "
        "rename + touch 10k %lld us, %zu bytes; empty %lld us\n",
        static_cast<long long>(us(create_stats)), create_patch.size(),
        static_cast<long long>(us(touch_stats)), touch_patch.size(),
        static_cast<long long>(us(empty_stats)));
}

// --- счётчик строк для промежуточного забора ---

TEST_F(MetaStoreTest, RowsSinceTakeCountsCommittedRowsAndResetsOnTake) {
    EXPECT_EQ(store_->rows_since_take(), 0);
    create_file("a");
    const int64_t one = store_->rows_since_take();
    EXPECT_GE(one, 1);
    create_file("b");
    EXPECT_EQ(store_->rows_since_take(), 2 * one);
    // Откатанная запись не считается.
    EXPECT_THROW(store_->write([&](Db& db) {
        insert_inode(db, new_node(kRootIno, "c"));
        throw std::runtime_error("boom");
    }),
                 std::runtime_error);
    EXPECT_EQ(store_->rows_since_take(), 2 * one);
    // Служебные записи считаются, хотя dirty не ставят.
    (void)store_->take_patchset();
    EXPECT_EQ(store_->rows_since_take(), 0);
    store_->write(
        [&](Db& db) {
            AttrUpdate u;
            u.mtime_ns = 7;
            update_attrs(db, kRootIno, u);
        },
        WriteKind::housekeeping);
    EXPECT_EQ(store_->rows_since_take(), 1);
    create_file("d");
    EXPECT_GT(store_->rows_since_take(), 1);
    store_->vacuum_into(dir_ / "snap.sqlite");
    EXPECT_EQ(store_->rows_since_take(), 0);
}

TEST_F(MetaStoreTest, RowsHookFiresOnceWhenThresholdIsCrossed) {
    int calls = 0;
    store_->set_rows_hook(5, [&] { ++calls; });
    create_file("a");
    EXPECT_EQ(calls, 0);
    store_->write([&](Db& db) {
        for (int i = 0; i < 10; ++i) {
            insert_inode(db, new_node(kRootIno, "f" + std::to_string(i)));
        }
    });
    EXPECT_EQ(calls, 1);
    create_file("b");  // уже выше порога: повторно не зовём
    EXPECT_EQ(calls, 1);
    (void)store_->take_patchset();
    store_->write([&](Db& db) {
        for (int i = 0; i < 10; ++i) {
            insert_inode(db, new_node(kRootIno, "g" + std::to_string(i)));
        }
    });
    EXPECT_EQ(calls, 2);
    store_->set_rows_hook(0, {});
    (void)store_->take_patchset();
    store_->write([&](Db& db) {
        for (int i = 0; i < 10; ++i) {
            insert_inode(db, new_node(kRootIno, "h" + std::to_string(i)));
        }
    });
    EXPECT_EQ(calls, 2);
}

}  // namespace
}  // namespace telega::meta
