// StateStore (meta/state_db.h): все таблицы state.sqlite, транзакции, переоткрытие, потоки.

#include "meta/state_db.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "test_util.h"

namespace telega::meta {
namespace {

using test::bytes_of;

class StateStoreTest : public ::testing::Test {
  protected:
    void SetUp() override {
        StateStore::create(path());
        reopen();
    }

    void reopen() {
        state_.reset();
        state_ = std::make_unique<StateStore>(path());
    }

    [[nodiscard]] std::filesystem::path path() const { return dir_ / "state.sqlite"; }

    test::TempDir dir_;
    std::unique_ptr<StateStore> state_;
};

TEST_F(StateStoreTest, CreateAndOpenErrors) {
    EXPECT_THROW(StateStore::create(path()), std::runtime_error);
    EXPECT_THROW(StateStore(dir_ / "missing.sqlite"), std::runtime_error);
}

TEST_F(StateStoreTest, KeyValue) {
    EXPECT_FALSE(state_->get_int(state_key::base_generation));
    EXPECT_EQ(state_->get_int_or(state_key::clean_shutdown, 1), 1);
    state_->set_int(state_key::base_generation, 7);
    state_->set_int(state_key::base_generation, 8);
    state_->set_int(state_key::head_msg_id, 1LL << 40);
    state_->set_int(state_key::index_dirty, 1);
    const Bytes uuid(16, std::byte{0x5A});
    state_->set_blob(state_key::fs_uuid, uuid);
    state_->set_blob(state_key::head_hash, Bytes(32, std::byte{1}));
    state_->set_int(state_key::force_snapshot, 1);
    state_->erase(state_key::force_snapshot);

    reopen();
    EXPECT_EQ(state_->get_int(state_key::base_generation), 8);
    EXPECT_EQ(state_->get_int(state_key::head_msg_id), 1LL << 40);
    EXPECT_EQ(state_->get_int_or(state_key::index_dirty, 0), 1);
    EXPECT_EQ(state_->get_blob(state_key::fs_uuid), uuid);
    EXPECT_EQ(test::must(state_->get_blob(state_key::head_hash)).size(), 32U);
    EXPECT_FALSE(state_->get_int(state_key::force_snapshot));
    EXPECT_FALSE(state_->get_blob("unknown"));
}

TEST_F(StateStoreTest, PendingPatchsets) {
    EXPECT_TRUE(state_->list_pending().empty());
    const int64_t s1 = state_->append_pending(bytes_of("p1"));
    const int64_t s2 = state_->append_pending(bytes_of("p2"));
    const int64_t s3 = state_->append_pending(Bytes{});
    EXPECT_LT(s1, s2);
    EXPECT_LT(s2, s3);
    reopen();
    auto pending = state_->list_pending();
    ASSERT_EQ(pending.size(), 3U);
    EXPECT_EQ(pending[0].patchset, bytes_of("p1"));
    EXPECT_EQ(pending[1].seq, s2);
    EXPECT_TRUE(pending[2].patchset.empty());

    state_->delete_pending_upto(s2);
    pending = state_->list_pending();
    ASSERT_EQ(pending.size(), 1U);
    EXPECT_EQ(pending[0].seq, s3);
    EXPECT_GT(state_->append_pending(bytes_of("p4")), s3);
    state_->clear_pending();
    EXPECT_TRUE(state_->list_pending().empty());
}

TEST_F(StateStoreTest, Chain) {
    EXPECT_THROW(state_->append_chain(5, 50), std::logic_error) << "chain starts with a snapshot";
    state_->reset_chain_to_snapshot(10, 1000);
    EXPECT_EQ(state_->append_chain(11, 100), 1);
    EXPECT_EQ(state_->append_chain(12, 120), 2);
    reopen();
    auto chain = state_->list_chain();
    ASSERT_EQ(chain.size(), 3U);
    EXPECT_EQ(chain[0].pos, 0);
    EXPECT_EQ(chain[0].msg_id, 10);
    EXPECT_EQ(chain[0].blob_size, 1000);
    EXPECT_EQ(chain[2].msg_id, 12);

    state_->reset_chain_to_snapshot(20, 2000);
    chain = state_->list_chain();
    ASSERT_EQ(chain.size(), 1U);
    EXPECT_EQ(chain[0].msg_id, 20);
}

// Версия не сбрасывается при пересоздании строки (O_TRUNC, заливка): иначе новый объект файла
// совпал бы по (ino, version) с прежним, который ещё лежит в закрытом пакете.
TEST_F(StateStoreTest, DirtyChunkVersionIsMonotonicAcrossRecreation) {
    const int64_t v1 = state_->upsert_dirty_chunk(5, 0, "a", 1);
    const int64_t v2 = state_->upsert_dirty_chunk(5, 0, "a", 2);
    EXPECT_TRUE(state_->delete_dirty_chunk_if_version(5, 0, v2));
    const int64_t v3 = state_->upsert_dirty_chunk(5, 0, "a", 3);
    EXPECT_GT(v2, v1);
    EXPECT_GT(v3, v2);
    state_->delete_dirty_chunks_for_ino(5);
    const int64_t v4 = state_->upsert_dirty_chunk(5, 0, "a", 4);
    EXPECT_GT(v4, v3);
    // Счётчик другого inode общий: версии разных строк не повторяются.
    const int64_t w = state_->upsert_dirty_chunk(6, 0, "b", 5);
    EXPECT_GT(w, v4);
}

// После перезапуска счётчик продолжается выше версий оставшихся строк dirty_chunks и open_pack.
TEST_F(StateStoreTest, DirtyChunkVersionContinuesAfterReopen) {
    state_->upsert_dirty_chunk(5, 0, "a", 1);
    const int64_t last = state_->upsert_dirty_chunk(5, 0, "a", 2);
    state_->upsert_open_pack({9, last + 10, 100, 1});  // версия объекта, чей чанк уже ушёл
    reopen();
    EXPECT_GT(state_->upsert_dirty_chunk(7, 0, "c", 3), last + 10);
}

// Старая схема (open_pack без added_ns, нет read_cache): rw-открытие мигрирует, --ro (migrate =
// false) не меняет файл и не ломается на старой схеме.
class StateStoreMigration : public StateStoreTest {
  protected:
    void downgrade() {
        state_.reset();
        {
            Db db = Db::open(path());
            db.exec(
                "DROP TABLE open_pack; "
                "CREATE TABLE open_pack (ino INTEGER PRIMARY KEY, version INTEGER NOT NULL, "
                "off INTEGER NOT NULL, len INTEGER NOT NULL); "
                "INSERT INTO open_pack VALUES (3, 1, 0, 10); "
                "DROP TABLE read_cache;");
            db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
        }
    }
    [[nodiscard]] std::string file_bytes() const {
        std::ifstream in(path(), std::ios::binary);
        return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }
};

TEST_F(StateStoreMigration, ReadOnlyOpenDoesNotChangeTheFile) {
    downgrade();
    const std::string before = file_bytes();
    {
        StateStore ro{path(), false};
        EXPECT_TRUE(ro.list_open_pack().empty());  // пакеты в --ro не используются
        EXPECT_FALSE(ro.get_int(state_key::base_generation));
    }
    EXPECT_EQ(file_bytes(), before);
}

TEST_F(StateStoreMigration, ReadWriteOpenMigratesOnceAndLaterOpensDoNotWrite) {
    downgrade();
    state_ = std::make_unique<StateStore>(path());
    EXPECT_TRUE(state_->list_open_pack().empty());  // строки open_pack пересозданы пустыми
    state_->upsert_open_pack({4, 1, 10, 7});
    reopen();
    ASSERT_EQ(state_->list_open_pack().size(), 1U);
    state_.reset();
    const std::string before = file_bytes();
    state_ = std::make_unique<StateStore>(path());
    state_.reset();
    EXPECT_EQ(file_bytes(), before);
}

TEST_F(StateStoreTest, DirtyChunks) {
    // version берётся из общего монотонного счётчика: строго растёт при каждой записи.
    EXPECT_EQ(state_->upsert_dirty_chunk(5, 0, "staging/5-0", 100), 1);
    EXPECT_EQ(state_->upsert_dirty_chunk(5, 0, "staging/5-0b", 200), 2);
    EXPECT_EQ(state_->upsert_dirty_chunk(5, 3, "staging/5-3", 150), 3);
    EXPECT_EQ(state_->upsert_dirty_chunk(2, 1, "staging/2-1", 160), 4);
    reopen();
    const DirtyChunk c = test::must(state_->get_dirty_chunk(5, 0));
    EXPECT_EQ(c.path, "staging/5-0b");
    EXPECT_EQ(c.version, 2);
    EXPECT_EQ(c.last_write_ns, 200);
    EXPECT_FALSE(state_->get_dirty_chunk(5, 1));

    const auto all = state_->list_dirty_chunks();
    ASSERT_EQ(all.size(), 3U);
    EXPECT_EQ(all[0].ino, 2);
    const auto for_ino = state_->list_dirty_chunks_for_ino(5);
    ASSERT_EQ(for_ino.size(), 2U);
    EXPECT_EQ(for_ino[1].idx, 3);

    // Запись после запечатывания (version сменилась) не даёт удалить запись.
    EXPECT_FALSE(state_->delete_dirty_chunk_if_version(5, 0, 1));
    EXPECT_TRUE(state_->delete_dirty_chunk_if_version(5, 0, 2));
    EXPECT_FALSE(state_->get_dirty_chunk(5, 0));
    state_->delete_dirty_chunks_for_ino(5);
    EXPECT_EQ(state_->list_dirty_chunks().size(), 1U);
}

TEST_F(StateStoreTest, DirtyAttrs) {
    state_->set_dirty_attrs({7, 100, 1000});
    state_->set_dirty_attrs({7, 200, 2000});
    state_->set_dirty_attrs({3, 1, 1});
    reopen();
    const DirtyAttrs a = test::must(state_->get_dirty_attrs(7));
    EXPECT_EQ(a.size, 200);
    EXPECT_EQ(a.mtime_ns, 2000);
    const auto all = state_->list_dirty_attrs();
    ASSERT_EQ(all.size(), 2U);
    EXPECT_EQ(all[0].ino, 3);
    state_->delete_dirty_attrs(7);
    EXPECT_FALSE(state_->get_dirty_attrs(7));
}

TEST_F(StateStoreTest, UploadJournal) {
    state_->upload_started(1, 100);
    state_->upload_started(2, 200);
    state_->upload_finished(1, 5555);
    EXPECT_THROW(state_->upload_finished(9, 1), std::logic_error);
    reopen();
    const auto uploads = state_->list_uploads();
    ASSERT_EQ(uploads.size(), 2U);
    EXPECT_EQ(uploads[0].msg_id, 5555);
    EXPECT_FALSE(uploads[1].msg_id);
    EXPECT_EQ(uploads[1].started_ns, 200);
    state_->remove_upload(1);
    EXPECT_EQ(state_->list_uploads().size(), 1U);
}

TEST_F(StateStoreTest, OpenPackRows) {
    EXPECT_TRUE(state_->list_open_pack().empty());
    state_->upsert_open_pack({5, 3, 100, 1000});
    state_->upsert_open_pack({2, 1, 50, 900});
    state_->upsert_open_pack({5, 4, 120, 1100});  // замена строки
    reopen();
    const auto rows = state_->list_open_pack();
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0].ino, 2);
    EXPECT_EQ(rows[1].ino, 5);
    EXPECT_EQ(rows[1].version, 4);
    EXPECT_EQ(rows[1].len, 120);
    EXPECT_EQ(rows[1].added_ns, 1100);
    EXPECT_FALSE(state_->delete_open_pack_if_version(5, 3));  // версия изменилась
    EXPECT_TRUE(state_->delete_open_pack_if_version(5, 4));
    state_->delete_open_pack(2);
    state_->delete_open_pack(99);  // нет строки — не ошибка
    EXPECT_TRUE(state_->list_open_pack().empty());
}

TEST_F(StateStoreTest, ReadCacheTableExists) {
    state_.reset();
    Db db = Db::open(path());
    db.exec(
        "INSERT INTO read_cache (path, remote_file_id, blocks, last_access_ns) "
        "VALUES ('/x', 'id', 4096, 7)");
    Stmt st{db, "SELECT COUNT(*) FROM read_cache"};
    ASSERT_TRUE(st.step());
    EXPECT_EQ(st.column_int64(0), 1);
}

// state.sqlite со старой схемой (open_pack со столбцом off, без read_cache) при открытии
// получает новую open_pack и read_cache; остальные таблицы не меняются.
TEST_F(StateStoreTest, SchemaWithoutReadCacheIsMigrated) {
    state_->set_int(state_key::base_generation, 9);
    state_->upsert_dirty_chunk(4, 0, "4-0", 77);
    state_.reset();
    {
        Db db = Db::open(path());
        db.exec("DROP TABLE open_pack; DROP TABLE read_cache;");
        db.exec(
            "CREATE TABLE open_pack (ino INTEGER PRIMARY KEY, version INTEGER NOT NULL, "
            "off INTEGER NOT NULL, len INTEGER NOT NULL)");
    }
    reopen();
    EXPECT_TRUE(state_->list_open_pack().empty());
    state_->upsert_open_pack({1, 2, 3, 4});
    EXPECT_EQ(state_->list_open_pack().size(), 1U);
    EXPECT_EQ(state_->get_int(state_key::base_generation), 9);
    EXPECT_EQ(state_->list_dirty_chunks().size(), 1U);
    // Повторное открытие уже новой схемы ничего не стирает.
    reopen();
    EXPECT_EQ(state_->list_open_pack().size(), 1U);
    state_.reset();
    Db db = Db::open(path());
    Stmt st{db, "SELECT COUNT(*) FROM read_cache"};
    ASSERT_TRUE(st.step());
    EXPECT_EQ(st.column_int64(0), 0);
}

// Шаг 8 коммита: всё одной транзакцией, сбой посередине не оставляет следов.
TEST_F(StateStoreTest, TransactionIsAtomic) {
    state_->reset_chain_to_snapshot(10, 1000);
    const int64_t seq = state_->append_pending(bytes_of("p"));
    state_->set_int(state_key::index_dirty, 1);

    EXPECT_THROW(state_->transaction([&] {
        state_->set_int(state_key::base_generation, 2);
        state_->delete_pending_upto(seq);
        state_->append_chain(11, 100);
        throw std::runtime_error("crash");
    }),
                 std::runtime_error);
    EXPECT_FALSE(state_->get_int(state_key::base_generation));
    EXPECT_EQ(state_->list_pending().size(), 1U);
    EXPECT_EQ(state_->list_chain().size(), 1U);

    const int64_t pos = state_->transaction([&] {
        state_->set_int(state_key::base_generation, 2);
        state_->set_int(state_key::index_dirty, 0);
        state_->delete_pending_upto(seq);
        // reset_chain_to_snapshot внутри — вложенная точка сохранения.
        state_->reset_chain_to_snapshot(10, 1000);
        return state_->append_chain(11, 100);
    });
    EXPECT_EQ(pos, 1);
    reopen();
    EXPECT_EQ(state_->get_int(state_key::base_generation), 2);
    EXPECT_EQ(state_->get_int(state_key::index_dirty), 0);
    EXPECT_TRUE(state_->list_pending().empty());
    EXPECT_EQ(state_->list_chain().size(), 2U);
}

TEST_F(StateStoreTest, ConcurrentUse) {
    constexpr int kThreads = 4;
    constexpr int kPerThread = 50;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                state_->upsert_dirty_chunk(t, i, "p", i);
                state_->transaction([&] {
                    const int64_t v = state_->get_int_or("counter", 0);
                    state_->set_int("counter", v + 1);
                });
                (void)state_->list_dirty_chunks_for_ino(t);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(state_->get_int("counter"), kThreads * kPerThread);
    EXPECT_EQ(state_->list_dirty_chunks().size(), static_cast<size_t>(kThreads * kPerThread));
}

}  // namespace
}  // namespace telega::meta
