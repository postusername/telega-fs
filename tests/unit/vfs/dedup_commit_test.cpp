// Дедупликация и отложенная уборка (SPEC 8.1 шаг 2, 7.4): настоящее монтирование (MountedFs с
// CommitManager) на LocalDirStore, Vfs поверх него. Дедупликация только на живые чанки: мёртвый
// не оживляется и убирается в срок, убранный не находится; уборка и дедупликация, идущие
// одновременно, не удаляют сообщение живого чанка.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "../commit/commit_test_util.h"
#include "vfs/vfs.h"
#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

using namespace std::chrono_literals;
using meta::MountedFs;
using meta::test::CommitEnv;
using store::StoreOp;

// Размеры по init_options() CommitEnv: pack_threshold 4096, chunk_size 64 КиБ: 40 000 байт —
// ровно один позиционный чанк.
constexpr size_t kBig = 40'000;

struct Machine {
    std::unique_ptr<MountedFs> fs;
    std::unique_ptr<Vfs> vfs;  // объявлен после fs: уничтожается раньше
};

class DedupGc : public CommitEnv {
  protected:
    Machine open(const meta::FsPaths& paths) {
        Machine m;
        m.fs = mount(paths);
        VfsOptions o = fast_options();
        o.sync_timeout = 60s;
        m.vfs = std::make_unique<Vfs>(m.fs->meta(), m.fs->state(), m.fs->store(), m.fs->committer(),
                                      m.fs->keys(), VfsPaths{paths.staging(), paths.tmp()}, o);
        return m;
    }

    static int64_t put(Vfs& v, const std::string& name, const Bytes& data) {
        const Created c = v.create(meta::kRootIno, bytes_of(name), 0644, O_RDWR, 1000, 1000);
        EXPECT_EQ(v.write(c.attr.ino, 0, data), data.size());
        v.fsync(c.attr.ino);  // заливка (или привязка) и коммит
        v.release(c.fh);
        v.forget(c.attr.ino, 1);
        return c.attr.ino;
    }
    static Bytes read_all(Vfs& v, int64_t ino) {
        const Attr a = v.getattr(ino);
        return v.read(ino, 0, static_cast<size_t>(a.size) + 1);
    }
    static void remove(Vfs& v, const std::string& name) {
        v.unlink(meta::kRootIno, bytes_of(name));
    }

    // Изменить индекс и закоммитить: поколение растёт, уборка идёт.
    void bump(Machine& m) {
        mkfile(*m.fs, "bump" + std::to_string(++bumps_));
        m.fs->committer().commit_now();
    }

    static meta::Chunk chunk_of(MountedFs& fs, int64_t ino) {
        return fs.meta().read([&](meta::Db& db) {
            const auto e = meta::test::must(meta::get_extent(db, ino, 0));
            return meta::test::must(meta::get_chunk(db, e.chunk_id));
        });
    }
    static std::optional<meta::Chunk> chunk_row(MountedFs& fs, int64_t chunk_id) {
        return fs.meta().read([&](meta::Db& db) { return meta::get_chunk(db, chunk_id); });
    }
    [[nodiscard]] uint64_t puts() const { return store_->call_count(StoreOp::put_chunk); }

    // Сообщение каждого живого чанка есть в канале, учёт согласован.
    void expect_live_chunks_have_messages(MountedFs& fs) {
        std::vector<std::pair<int64_t, int64_t>> live;
        fs.meta().read([&](meta::Db& db) {
            meta::Stmt st{db, "SELECT chunk_id, msg_id FROM chunks WHERE refcount > 0"};
            while (st.step()) {
                live.emplace_back(st.column_int64(0), st.column_int64(1));
            }
        });
        for (const auto& [id, msg] : live) {
            EXPECT_TRUE(has_message(msg)) << "live chunk " << id << " lost its message " << msg;
        }
        EXPECT_TRUE(fs.meta().read(
            [](meta::Db& db) { return meta::test::chunk_accounting_consistent(db); }));
    }

    int bumps_ = 0;
};

// Мёртвый чанк не оживляется, даже пока до созревания retain_generations далеко (SPEC 8.1,
// шаг 2): новая запись заливает свой чанк, а мёртвый убирается уборкой в срок.
TEST_F(DedupGc, DeadChunkIsNotRevivedAndIsCollectedOnTime) {
    commit_config_.retain_generations = 2;
    init();
    auto m = open(a_);
    Vfs& v = *m.vfs;
    const Bytes data = pattern(kBig, 1);
    const int64_t f1 = put(v, "f1", data);
    const meta::Chunk x = chunk_of(*m.fs, f1);
    ASSERT_EQ(puts(), 1U);

    remove(v, "f1");
    ASSERT_TRUE(chunk_row(*m.fs, x.chunk_id).has_value());
    ASSERT_EQ(meta::test::must(chunk_row(*m.fs, x.chunk_id)).refcount, 0);
    bump(m);  // созревания ещё нет: dead_since_gen + 2 > G
    ASSERT_TRUE(has_message(x.msg_id));

    const int64_t f2 = put(v, "f2", data);  // мёртвый X не находится
    EXPECT_EQ(puts(), 2U) << "uploaded again instead of reviving";
    const meta::Chunk y = chunk_of(*m.fs, f2);
    EXPECT_NE(y.chunk_id, x.chunk_id);
    EXPECT_EQ(y.refcount, 1);
    EXPECT_FALSE(y.dead_since_gen.has_value());

    for (int i = 0; i < 12; ++i) {
        bump(m);
    }
    EXPECT_FALSE(has_message(x.msg_id)) << "the dead chunk is collected";
    EXPECT_FALSE(chunk_row(*m.fs, x.chunk_id).has_value());
    EXPECT_TRUE(has_message(y.msg_id));
    EXPECT_EQ(read_all(v, f2), data);
    expect_live_chunks_have_messages(*m.fs);

    // Перемонтирование: то же самое из локального индекса.
    m.vfs.reset();
    m.fs->unmount();
    m = open(a_);
    EXPECT_EQ(read_all(*m.vfs, f2), data);
    EXPECT_EQ(chunk_of(*m.fs, f2).refcount, 1);
}

TEST_F(DedupGc, CollectedChunkIsNotFoundAndIsUploadedAgain) {
    commit_config_.retain_generations = 1;
    init();
    auto m = open(a_);
    Vfs& v = *m.vfs;
    const Bytes data = pattern(kBig, 2);
    const int64_t f1 = put(v, "f1", data);
    const meta::Chunk x = chunk_of(*m.fs, f1);
    remove(v, "f1");
    for (int i = 0; i < 4 && has_message(x.msg_id); ++i) {
        bump(m);
    }
    ASSERT_FALSE(has_message(x.msg_id));
    ASSERT_FALSE(chunk_row(*m.fs, x.chunk_id).has_value());

    const int64_t f2 = put(v, "f2", data);
    EXPECT_EQ(puts(), 2U);
    const meta::Chunk y = chunk_of(*m.fs, f2);
    EXPECT_NE(y.chunk_id, x.chunk_id);
    EXPECT_TRUE(has_message(y.msg_id));
    EXPECT_EQ(read_all(v, f2), data);
    expect_live_chunks_have_messages(*m.fs);
}

TEST_F(DedupGc, SharedChunkIsKeptWhileOneCopyIsAlive) {
    commit_config_.retain_generations = 1;
    init();
    auto m = open(a_);
    Vfs& v = *m.vfs;
    const Bytes data = pattern(kBig, 3);
    const int64_t f1 = put(v, "f1", data);
    const int64_t f2 = put(v, "f2", data);
    const meta::Chunk x = chunk_of(*m.fs, f1);
    ASSERT_EQ(puts(), 1U);
    ASSERT_EQ(chunk_of(*m.fs, f2).chunk_id, x.chunk_id);
    remove(v, "f1");
    for (int i = 0; i < 5; ++i) {
        bump(m);
    }
    EXPECT_TRUE(has_message(x.msg_id));
    EXPECT_EQ(read_all(v, f2), data);
    remove(v, "f2");
    for (int i = 0; i < 4; ++i) {
        bump(m);
    }
    EXPECT_FALSE(has_message(x.msg_id)) << "both copies are gone: the chunk is collected";
}

// Уборка (каждый коммит) и дедупликация (каждый fsync) идут одновременно. Содержимое
// ограничено тремя вариантами, чтобы привязки к убираемым чанкам случались постоянно:
// retain_generations = 1, чанк умирает на одном коммите и убирается на следующем. Ни одно
// сообщение живого чанка не должно пропасть, каждый файл читается сразу после fsync.
TEST_F(DedupGc, ConcurrentCollectionAndDeduplicationNeverDeleteLiveChunks) {
    commit_config_.retain_generations = 1;
    init();
    auto m = open(a_);
    Vfs& v = *m.vfs;
    constexpr int kWriters = 4;
    constexpr int kRounds = 12;
    std::vector<Bytes> variants;
    for (uint64_t i = 0; i < 3; ++i) {
        variants.push_back(pattern(kBig, 100 + i));
    }

    std::atomic<int> failures{0};
    std::atomic<int> writers_left{kWriters};
    std::vector<std::thread> threads;
    threads.reserve(kWriters + 1);
    for (int t = 0; t < kWriters; ++t) {
        threads.emplace_back([&, t] {
            try {
                for (int i = 0; i < kRounds; ++i) {
                    const Bytes& data = variants[static_cast<size_t>(t + i) % variants.size()];
                    const std::string name = "w" + std::to_string(t) + "_" + std::to_string(i);
                    const int64_t ino = put(v, name, data);
                    if (read_all(v, ino) != data) {
                        failures.fetch_add(1);
                    }
                    if (i > 0) {
                        remove(v, "w" + std::to_string(t) + "_" + std::to_string(i - 1));
                    }
                }
            } catch (const std::exception&) {
                failures.fetch_add(1);
            }
            writers_left.fetch_sub(1);
        });
    }
    threads.emplace_back([&] {
        try {
            int n = 0;
            while (writers_left.load() > 0) {
                const std::string name = "bump" + std::to_string(n++);
                m.fs->meta().write([&](meta::Db& db) {
                    meta::insert_inode(db, meta::test::new_node(meta::kRootIno, name));
                });
                m.fs->committer().commit_now();
            }
        } catch (const std::exception&) {
            failures.fetch_add(1);
        }
    });
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(failures.load(), 0);

    // Последний файл каждого писателя читается; дедупликация реально случалась.
    for (int t = 0; t < kWriters; ++t) {
        const std::string name = "w" + std::to_string(t) + "_" + std::to_string(kRounds - 1);
        const Attr a = v.lookup(meta::kRootIno, bytes_of(name));
        EXPECT_EQ(read_all(v, a.ino), variants[static_cast<size_t>(t + kRounds - 1) % 3]);
        v.forget(a.ino, 1);
    }
    EXPECT_GT(v.status().deduped_chunks, 0U);
    expect_live_chunks_have_messages(*m.fs);
    EXPECT_TRUE(m.fs->state().list_dirty_chunks().empty());
}

}  // namespace
}  // namespace telega::vfs::test
