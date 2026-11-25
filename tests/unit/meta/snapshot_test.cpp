// Снимок индекса без долгой блокировки записи:
// фиксация среза под блокировкой, копирование после неё, корректность при конкурентном
// писателе, закрытие читательской транзакции на любом пути выхода.
// Синхронизация потоков идёт через барьеры и хуки этапов копирования, не через тайминги.

#include "meta/snapshot.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "meta/index_db.h"
#include "meta/index_ops.h"
#include "test_util.h"

namespace telega::meta {
namespace {

using namespace std::chrono_literals;
using test::bytes_of;
using test::new_node;

// Предел только для того, чтобы сломанная реализация (блокировка держится на всё копирование)
// роняла тест, а не вешала его.
bool wait_until(const std::function<bool()>& pred) {
    const auto deadline = std::chrono::steady_clock::now() + 60s;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

std::string dump_file(const std::filesystem::path& path) {
    OpenOptions options;
    options.read_only = true;
    Db db = Db::open(path, options);
    return MetaStore::dump(db);
}

class Snapshot : public ::testing::Test {
  protected:
    void SetUp() override {
        MetaStore::create(index_path(), test::test_params());
        store_ = std::make_unique<MetaStore>(index_path());
        // Индекс побольше пустого: десятки страниц, есть что копировать.
        store_->write([&](Db& db) {
            for (int i = 0; i < 400; ++i) {
                known_.push_back(insert_inode(db, new_node(kRootIno, "seed-" + std::to_string(i))));
            }
        });
    }

    [[nodiscard]] std::filesystem::path index_path() const { return dir_ / "index.sqlite"; }
    [[nodiscard]] std::filesystem::path raw_path() const { return dir_ / "index-2.sqlite.raw"; }
    [[nodiscard]] std::filesystem::path out_path() const { return dir_ / "index-2.sqlite"; }

    std::string live_dump() {
        return store_->read([](Db& db) { return MetaStore::dump(db); });
    }

    // Читательская транзакция закрыта: контрольная точка WAL проходит до конца (пока читатель
    // со срезом жив, она вернула бы busy = 1).
    void expect_checkpoint_not_blocked() {
        Db db = Db::open(index_path());
        Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
        ASSERT_TRUE(st.step());
        EXPECT_EQ(st.column_int64(0), 0) << "a snapshot reader still holds the WAL";
    }

    void expect_no_copy_files() {
        for (const char* suffix : {"", "-journal", "-wal", "-shm"}) {
            EXPECT_FALSE(std::filesystem::exists(raw_path().string() + suffix))
                << "raw" << suffix << " left behind";
        }
    }

    test::TempDir dir_;
    std::unique_ptr<MetaStore> store_;
    std::vector<int64_t> known_;
};

TEST_F(Snapshot, JournalSizeLimitIsSetOnWriter) {
    const int64_t limit = store_->write([](Db& db) {
        Stmt st{db, "PRAGMA journal_size_limit"};
        st.step();
        return st.column_int64(0);
    });
    EXPECT_EQ(limit, kJournalSizeLimit);
    EXPECT_EQ(kJournalSizeLimit, int64_t{64} << 20);
}

TEST_F(Snapshot, CutRecreatesSessionWithoutPatchAndResetsCounters) {
    EXPECT_TRUE(store_->dirty_since_take());
    EXPECT_GT(store_->rows_since_take(), 0);

    SnapshotCopy copy{index_path(), raw_path(), out_path()};
    TakeStats stats;
    store_->cut_snapshot(copy.reader(), &stats);
    EXPECT_TRUE(copy.reader().in_transaction());
    EXPECT_FALSE(store_->dirty_since_take());
    EXPECT_EQ(store_->rows_since_take(), 0);
    EXPECT_EQ(stats.patch_bytes, 0U);
    // Строки до среза в новую сессию не попали: патча от них нет.
    EXPECT_TRUE(store_->take_patchset().empty());

    // Записи после среза идут в новую сессию и не попадают в снимок.
    const int64_t after =
        store_->write([&](Db& db) { return insert_inode(db, new_node(kRootIno, "after-cut")); });
    const std::string live = live_dump();
    copy.copy();
    const std::string snap = dump_file(out_path());
    EXPECT_EQ(snap.find(to_hex(bytes_of("after-cut"))), std::string::npos);
    EXPECT_NE(snap, live);
    EXPECT_NE(snap.find(to_hex(bytes_of("seed-399"))), std::string::npos);
    EXPECT_GT(after, 0);

    // Снимок плюс патч после среза равен живому индексу.
    const Bytes patch = store_->take_patchset();
    ASSERT_FALSE(patch.empty());
    const std::vector<Bytes> patches{patch};
    MetaStore::build_from_chain(out_path(), patches, dir_ / "built.sqlite");
    EXPECT_EQ(dump_file(dir_ / "built.sqlite"), live);
    expect_no_copy_files();
}

TEST_F(Snapshot, ResultIsSelfContainedRollbackJournalFile) {
    SnapshotCopy copy{index_path(), raw_path(), out_path()};
    store_->cut_snapshot(copy.reader());
    copy.copy();
    EXPECT_FALSE(std::filesystem::exists(out_path().string() + "-wal"));
    EXPECT_FALSE(std::filesystem::exists(out_path().string() + "-shm"));
    OpenOptions ro;
    ro.read_only = true;
    Db db = Db::open(out_path(), ro);
    Stmt mode{db, "PRAGMA journal_mode"};
    ASSERT_TRUE(mode.step());
    EXPECT_EQ(mode.column_text(0), "delete");
    MetaStore::verify(db);
    expect_checkpoint_not_blocked();
}

TEST_F(Snapshot, CutInsideWriteIsRejected) {
    SnapshotCopy copy{index_path(), raw_path(), out_path()};
    EXPECT_THROW(store_->write([&](Db&) { store_->cut_snapshot(copy.reader()); }),
                 std::logic_error);
    EXPECT_FALSE(copy.reader().in_transaction());
}

TEST_F(Snapshot, CopyWithoutCutIsRejected) {
    SnapshotCopy copy{index_path(), raw_path(), out_path()};
    EXPECT_THROW(copy.copy(), std::logic_error);
    expect_no_copy_files();
}

// Блокировка не держится на время копирования: пока идёт backup, писатель и забор патча из
// другого потока проходят (хук этапа backup ждёт их, а не наоборот).
TEST_F(Snapshot, WriterAndTakerProceedWhileCopyIsRunning) {
    std::atomic<bool> hook_called{false};
    std::atomic<int> written_in_copy{0};
    std::atomic<bool> taker_done{false};
    std::vector<Bytes> taken;  // после среза, по порядку
    std::mutex taken_mu;

    SnapshotCopy copy{index_path(), raw_path(), out_path(), [&](SnapshotStage stage, Db& /*copy*/) {
                          if (stage != SnapshotStage::backup) {
                              return;
                          }
                          hook_called.store(true);
                          // Срез зафиксирован, копирование начинается. Если бы блокировка
                          // MetaStore ещё держалась, эти потоки не прошли бы никогда.
                          std::thread writer([&] {
                              for (int i = 0; i < 25; ++i) {
                                  store_->write([&](Db& db) {
                                      insert_inode(
                                          db, new_node(kRootIno, "during-" + std::to_string(i)));
                                  });
                                  written_in_copy.fetch_add(1);
                              }
                          });
                          std::thread taker([&] {
                              Bytes p = store_->take_patchset();
                              const std::lock_guard lock(taken_mu);
                              taken.push_back(std::move(p));
                              taker_done.store(true);
                          });
                          EXPECT_TRUE(wait_until([&] { return written_in_copy.load() == 25; }));
                          EXPECT_TRUE(wait_until([&] { return taker_done.load(); }));
                          writer.join();
                          taker.join();
                      }};
    store_->cut_snapshot(copy.reader());
    copy.copy();
    ASSERT_TRUE(hook_called.load());
    EXPECT_EQ(written_in_copy.load(), 25);

    // Всё, что произошло во время копирования, — в патчах после среза, не в снимке.
    taken.push_back(store_->take_patchset());
    std::vector<Bytes> patches;
    for (auto& p : taken) {
        if (!p.empty()) {
            patches.push_back(std::move(p));
        }
    }
    const std::string snap = dump_file(out_path());
    EXPECT_EQ(snap.find(to_hex(bytes_of("during-0"))), std::string::npos);
    MetaStore::build_from_chain(out_path(), patches, dir_ / "built.sqlite");
    EXPECT_EQ(dump_file(dir_ / "built.sqlite"), live_dump());
}

// Корректность при конкурентном писателе (по образцу проверки E11): снимок плюс патчи, забранные
// после среза (промежуточный забор идёт во время копирования), равны живому индексу построчно.
TEST_F(Snapshot, SnapshotPlusPatchesAfterCutEqualsLiveIndexWithConcurrentWriter) {
    std::atomic<bool> stop{false};
    std::atomic<int64_t> writes{0};
    std::atomic<int64_t> takes{0};
    std::mutex queue_mu;  // как patch_queue_mu_ в CommitManager
    std::vector<Bytes> patches;  // под queue_mu: патчи после среза, по порядку забора

    std::thread writer([&] {
        std::mt19937_64 rng(20261122);
        std::vector<int64_t> mine = known_;
        int64_t rolled_back = 0;
        int64_t next_name = 0;
        while (!stop.load()) {
            const int op = static_cast<int>(rng() % 10);
            try {
                store_->write([&](Db& db) {
                    if (op < 3 || mine.empty()) {
                        mine.push_back(insert_inode(
                            db, new_node(kRootIno, "w-" + std::to_string(next_name++))));
                    } else if (op < 6) {
                        AttrUpdate u;
                        u.mtime_ns = static_cast<int64_t>(rng() % 1'000'000);
                        update_attrs(db, mine[rng() % mine.size()], u);
                    } else if (op < 8) {
                        move_inode(db, mine[rng() % mine.size()], kRootIno,
                                   bytes_of("m-" + std::to_string(next_name++)));
                    } else if (op == 8) {
                        const size_t at = rng() % mine.size();
                        delete_inode(db, mine[at], 1);
                        mine.erase(mine.begin() + static_cast<std::ptrdiff_t>(at));
                    } else {
                        add_history(db, next_name++, 7, BlobKind::delta);
                    }
                });
            } catch (const FsError&) {
                ++rolled_back;  // EEXIST и подобное откатывает транзакцию: часть сценария
            }
            writes.fetch_add(1);
        }
        EXPECT_GE(rolled_back, 0);
    });
    std::thread taker([&] {
        while (!stop.load()) {
            {
                const std::lock_guard queue(queue_mu);
                Bytes p = store_->take_patchset();
                if (!p.empty()) {
                    patches.push_back(std::move(p));
                }
            }
            takes.fetch_add(1);
            std::this_thread::yield();
        }
    });
    ASSERT_TRUE(wait_until([&] { return writes.load() > 50; }));

    int64_t writes_at_cut = 0;
    int64_t takes_at_cut = 0;
    int64_t writes_at_backup = 0;
    SnapshotCopy copy{index_path(), raw_path(), out_path(), [&](SnapshotStage stage, Db& /*copy*/) {
                          if (stage != SnapshotStage::backup) {
                              return;
                          }
                          // Пока копия не начата, писатель и забор должны сделать ещё работу.
                          EXPECT_TRUE(wait_until([&] {
                              return writes.load() >= writes_at_cut + 40 &&
                                     takes.load() >= takes_at_cut + 3;
                          }));
                          writes_at_backup = writes.load();
                      }};
    {
        // Как в CommitManager: очередь патчей, затем блокировка MetaStore.
        const std::lock_guard queue(queue_mu);
        store_->with_lock([&] {
            store_->cut_snapshot(copy.reader());
            writes_at_cut = writes.load();
            takes_at_cut = takes.load();
        });
        patches.clear();  // всё, что забрано до среза, к снимку отношения не имеет
    }
    copy.copy();
    EXPECT_GE(writes_at_backup, writes_at_cut + 40);

    stop.store(true);
    writer.join();
    taker.join();
    const Bytes tail = store_->take_patchset();
    if (!tail.empty()) {
        patches.push_back(tail);
    }
    ASSERT_FALSE(patches.empty());

    const std::string live = live_dump();
    EXPECT_NE(dump_file(out_path()), live);  // снимок без патчей отстаёт (сравнение не пустое)
    MetaStore::build_from_chain(out_path(), patches, dir_ / "built.sqlite");
    EXPECT_EQ(dump_file(dir_ / "built.sqlite"), live);
}

// --- ошибки на каждом шаге ---

struct FailCase {
    const char* name;
    // Подготовить сбой: перед этапом stage хук вызывает fn.
    SnapshotStage stage;
    std::function<void(Db& copy)> inject;
    bool out_preexists = false;
};

class SnapshotFailure : public Snapshot, public ::testing::WithParamInterface<FailCase> {};

TEST_P(SnapshotFailure, ClosesReaderTransactionAndRemovesFiles) {
    const FailCase& c = GetParam();
    if (c.out_preexists) {
        std::ofstream(out_path()) << "not a database";
    }
    bool injected = false;
    {
        SnapshotCopy copy{index_path(), raw_path(), out_path(), [&](SnapshotStage stage, Db& db) {
                              if (stage == c.stage) {
                                  injected = true;
                                  c.inject(db);
                              }
                          }};
        store_->cut_snapshot(copy.reader());
        // Запись после среза не должна терять срез ни при каком исходе.
        store_->write([&](Db& db) { insert_inode(db, new_node(kRootIno, "after-cut")); });
        EXPECT_THROW(copy.copy(), std::exception);
        EXPECT_TRUE(injected);
        // Транзакция закрыта сразу после сбоя, а не при уничтожении объекта.
        EXPECT_FALSE(copy.reader().in_transaction());
        EXPECT_TRUE(copy.reader().db().autocommit());
        expect_checkpoint_not_blocked();
    }
    expect_no_copy_files();
    if (c.out_preexists) {
        EXPECT_TRUE(std::filesystem::exists(out_path())) << "a foreign file must not be deleted";
    } else {
        EXPECT_FALSE(std::filesystem::exists(out_path()));
    }
    expect_checkpoint_not_blocked();
}

INSTANTIATE_TEST_SUITE_P(
    Steps, SnapshotFailure,
    ::testing::Values(FailCase{"hook_before_backup", SnapshotStage::backup,
                               [](Db&) { throw std::runtime_error("injected before backup"); }},
                      // Нехватка места при backup: копия не вмещает страниц среза (SQLITE_FULL).
                      FailCase{"no_space_in_backup", SnapshotStage::backup,
                               [](Db& copy) { copy.exec("PRAGMA max_page_count = 3"); }},
                      FailCase{"hook_before_vacuum", SnapshotStage::vacuum,
                               [](Db&) { throw std::runtime_error("injected before vacuum"); }},
                      // VACUUM INTO отказывается писать поверх существующего файла.
                      FailCase{"vacuum_into_fails", SnapshotStage::backup, [](Db&) {}, true}),
    [](const auto& param_info) { return std::string(param_info.param.name); });

// Сбой SnapshotCopy до среза (путь копии недоступен) ничего не оставляет.
TEST_F(Snapshot, UnopenableCopyLeavesNothing) {
    EXPECT_THROW(SnapshotCopy(index_path(), dir_ / "no-such-dir" / "x.raw", out_path()),
                 SqliteError);
    EXPECT_FALSE(std::filesystem::exists(out_path()));
}

// Уничтожение без copy(): транзакция закрыта, .raw убран.
TEST_F(Snapshot, DestructionAfterCutReleasesWal) {
    {
        SnapshotCopy copy{index_path(), raw_path(), out_path()};
        store_->cut_snapshot(copy.reader());
        store_->write([&](Db& db) { insert_inode(db, new_node(kRootIno, "x")); });
        Db db = Db::open(index_path());
        Stmt st{db, "PRAGMA wal_checkpoint(PASSIVE)"};
        ASSERT_TRUE(st.step());
        // Читатель со срезом держит контрольную точку: не все страницы WAL перенесены.
        EXPECT_NE(st.column_int64(1), st.column_int64(2));
    }
    expect_no_copy_files();
    expect_checkpoint_not_blocked();
}

}  // namespace
}  // namespace telega::meta
