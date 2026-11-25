// Дедупликация позиционных чанков на LocalDirStore: повторная запись того же
// содержимого не заливает ничего нового, refcount растёт и падает вместе с экстентами, чанк с
// refcount = 0 не находится и не оживает, пакеты и инлайн не дедуплицируются, нулевой content_mac
// не находится, RMW и truncate возвращаются к тому же чанку, сбой между транзакциями индекса и
// state.sqlite не портит refcount. Совместная работа с отложенной уборкой —
// dedup_commit_test.cpp.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <filesystem>
#include <set>
#include <string>

#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

using store::StoreOp;

constexpr size_t kBig = 100'000;  // позиционный класс при pack_threshold = 64 КиБ
constexpr size_t kSmall = 20'000;  // класс «в пакете»

class VfsDedup : public ::testing::Test {
  protected:
    void SetUp() override { env.start(options()); }

    static VfsOptions options() {
        VfsOptions o = fast_options();
        o.upload_concurrency = 1;  // чанки идут по одному: поздний видит закоммиченный ранний
        return o;
    }
    [[nodiscard]] Vfs& v() const { return *env.vfs; }

    // Файл с содержимым data, залитый и закоммиченный (fsync). Возвращает inode; lookup-ссылка
    // сброшена, чтобы unlink убирал inode сразу.
    int64_t put(const std::string& name, const Bytes& data) {
        const Created c = v().create(kRoot, bytes_of(name), 0644, O_RDWR, 1000, 1000);
        EXPECT_EQ(v().write(c.attr.ino, 0, data), data.size());
        v().fsync(c.attr.ino);
        v().release(c.fh);
        v().forget(c.attr.ino, 1);
        return c.attr.ino;
    }
    void remove(const std::string& name) { v().unlink(kRoot, bytes_of(name)); }

    [[nodiscard]] uint64_t puts() const { return env.store->call_count(StoreOp::put_chunk); }
    [[nodiscard]] size_t messages() const { return env.store->list_messages().size(); }

    [[nodiscard]] meta::Chunk chunk(int64_t chunk_id) const {
        return env.meta->read(
            [&](meta::Db& db) { return meta::test::must(meta::get_chunk(db, chunk_id)); });
    }
    [[nodiscard]] int64_t chunk_of(int64_t ino, int64_t idx = 0) const {
        const auto exts = env.extents(ino);
        for (const auto& e : exts) {
            if (e.idx == idx) {
                return e.chunk_id;
            }
        }
        ADD_FAILURE() << "no extent " << idx << " of " << ino;
        return 0;
    }
    [[nodiscard]] int64_t chunk_count() const {
        return env.meta->read([](meta::Db& db) {
            meta::Stmt st{db, "SELECT COUNT(*) FROM chunks"};
            st.step();
            return st.column_int64(0);
        });
    }

    VfsEnv env;
};

TEST_F(VfsDedup, SecondIdenticalFileUploadsNothing) {
    const Bytes data = pattern(kBig, 1);
    const int64_t a = put("a", data);
    ASSERT_EQ(puts(), 1U);
    const size_t msgs = messages();
    EXPECT_EQ(v().status().deduped_chunks, 0U);

    const int64_t b = put("b", data);
    EXPECT_EQ(puts(), 1U) << "the second copy must not be uploaded";
    EXPECT_EQ(messages(), msgs);
    EXPECT_EQ(v().status().deduped_chunks, 1U);
    EXPECT_EQ(v().status().uploaded_chunks, 1U);
    EXPECT_EQ(chunk_of(a), chunk_of(b));
    EXPECT_EQ(chunk(chunk_of(a)).refcount, 2);
    EXPECT_EQ(chunk(chunk_of(a)).live_bytes, 2 * static_cast<int64_t>(kBig));
    EXPECT_FALSE(chunk(chunk_of(a)).dead_since_gen.has_value());
    EXPECT_EQ(chunk_count(), 1);
    EXPECT_EQ(env.read_all(a), data);
    EXPECT_EQ(env.read_all(b), data);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(env.tmp()),
                            std::filesystem::directory_iterator()),
              0);
    EXPECT_TRUE(env.state->list_uploads().empty());
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
    EXPECT_TRUE(env.accounting_ok());
    EXPECT_EQ(v().staging().used(), 0U) << "staging is released as after an upload";

    env.restart();
    EXPECT_EQ(env.read_all(a), data);
    EXPECT_EQ(env.read_all(b), data);
}

TEST_F(VfsDedup, DifferentContentOrSizeIsNotDeduplicated) {
    const Bytes data = pattern(kBig, 2);
    put("a", data);
    Bytes other = data;
    other[1234] ^= std::byte{1};
    put("b", other);
    EXPECT_EQ(puts(), 2U);
    const Bytes shorter(data.begin(), data.end() - 1);
    put("c", shorter);
    EXPECT_EQ(puts(), 3U);
    EXPECT_EQ(v().status().deduped_chunks, 0U);
    EXPECT_EQ(chunk_count(), 3);
}

TEST_F(VfsDedup, RemovingOneCopyLeavesTheOtherReadable) {
    const Bytes data = pattern(kBig, 3);
    const int64_t a = put("a", data);
    const int64_t b = put("b", data);
    const int64_t id = chunk_of(a);
    remove("a");
    EXPECT_EQ(chunk(id).refcount, 1);
    EXPECT_EQ(chunk(id).live_bytes, static_cast<int64_t>(kBig));
    EXPECT_FALSE(chunk(id).dead_since_gen.has_value());
    EXPECT_EQ(env.read_all(b), data);
    remove("b");
    EXPECT_EQ(chunk(id).refcount, 0);
    EXPECT_TRUE(chunk(id).dead_since_gen.has_value());
    EXPECT_TRUE(env.accounting_ok());
}

// Мёртвый чанк (refcount = 0) не переиспользуется: уборка могла удалить его
// сообщение, а строка остаться в индексе канала. Запись того же содержимого заливается заново.
TEST_F(VfsDedup, DeadChunkIsNotReused) {
    const Bytes data = pattern(kBig, 4);
    const int64_t a = put("a", data);
    const int64_t id = chunk_of(a);
    env.committer.generation.store(5);
    remove("a");
    ASSERT_EQ(chunk(id).refcount, 0);
    ASSERT_EQ(chunk(id).dead_since_gen, 5);

    const int64_t b = put("b", data);
    EXPECT_EQ(puts(), 2U);
    EXPECT_NE(chunk_of(b), id);
    EXPECT_EQ(chunk(chunk_of(b)).refcount, 1);
    EXPECT_EQ(chunk(id).refcount, 0);
    EXPECT_EQ(chunk(id).dead_since_gen, 5) << "the dead chunk stays dead";
    EXPECT_EQ(v().status().deduped_chunks, 0U);
    EXPECT_EQ(env.read_all(b), data);
    EXPECT_TRUE(env.accounting_ok());
}

// Уборка удалила сообщение и строку служебной записью, коммита не было: в индексе канала (а
// после --discard-local и в локальном) строка осталась с refcount = 0, сообщения нет. Запись того
// же содержимого заливается заново и читается.
TEST_F(VfsDedup, DeadRowWithoutMessageIsNotReferenced) {
    const Bytes data = pattern(kBig, 14);
    const int64_t a = put("a", data);
    const int64_t id = chunk_of(a);
    const int64_t msg = chunk(id).msg_id;
    remove("a");
    const std::array<int64_t, 1> ids{msg};
    env.store->delete_messages(ids);  // как отложенная уборка; строка chunks осталась
    ASSERT_EQ(chunk(id).refcount, 0);

    const int64_t b = put("b", data);
    EXPECT_EQ(puts(), 2U);
    EXPECT_NE(chunk_of(b), id);
    EXPECT_EQ(env.read_all(b), data);
    env.restart();
    EXPECT_EQ(env.read_all(b), data);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsDedup, IdenticalChunksInsideOneFile) {
    const auto n = static_cast<size_t>(3 * kChunk);
    const Bytes zeros(n);
    const int64_t f = put("zeros", zeros);
    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(v().status().deduped_chunks, 2U);
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 3U);
    EXPECT_EQ(exts[0].chunk_id, exts[1].chunk_id);
    EXPECT_EQ(exts[1].chunk_id, exts[2].chunk_id);
    EXPECT_EQ(chunk(exts[0].chunk_id).refcount, 3);
    EXPECT_EQ(chunk(exts[0].chunk_id).live_bytes, static_cast<int64_t>(n));
    EXPECT_EQ(env.read_all(f), zeros);
    EXPECT_TRUE(env.accounting_ok());
    env.restart();
    EXPECT_EQ(env.read_all(f), zeros);
}

TEST_F(VfsDedup, ShortTailIsNotDeduplicatedAgainstFullChunk) {
    const Bytes zeros(static_cast<size_t>(kChunk) + kChunk / 2);
    const int64_t f = put("zeros", zeros);
    // Полный чанк и хвост другой длины: plain_size разный, MAC разный.
    EXPECT_EQ(puts(), 2U);
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 2U);
    EXPECT_NE(exts[0].chunk_id, exts[1].chunk_id);
    EXPECT_EQ(env.read_all(f), zeros);
}

TEST_F(VfsDedup, PacksAndInlineAreNotDeduplicated) {
    // Инлайн: данных в Store нет вовсе.
    const Bytes tiny = pattern(1000, 5);
    put("t1", tiny);
    put("t2", tiny);
    EXPECT_EQ(puts(), 0U);
    EXPECT_EQ(chunk_count(), 0);

    // Пакеты: каждый fsync закрывает свой пакет, одинаковое содержимое заливается заново.
    const Bytes small = pattern(kSmall, 6);
    const int64_t s1 = put("s1", small);
    const int64_t s2 = put("s2", small);
    EXPECT_EQ(puts(), 2U);
    EXPECT_EQ(v().status().deduped_chunks, 0U);
    EXPECT_NE(chunk_of(s1), chunk_of(s2));
    EXPECT_EQ(chunk(chunk_of(s1)).kind, meta::ChunkKind::pack);
    EXPECT_EQ(chunk(chunk_of(s2)).kind, meta::ChunkKind::pack);
    EXPECT_EQ(env.read_all(s1), small);
    EXPECT_EQ(env.read_all(s2), small);
    EXPECT_TRUE(env.accounting_ok());
}

// Между подсказкой дедупликации и привязкой чанк умер (последнюю ссылку убрали) или его убрала
// уборка (строка и сообщение): привязывать не к чему, позиция заливается обычным путём.
class VfsDedupRace : public VfsDedup, public ::testing::WithParamInterface<bool> {};

TEST_P(VfsDedupRace, ChunkDyingBetweenHintAndBindingIsUploadedAnew) {
    const bool collected = GetParam();
    const Bytes data = pattern(kBig, 13);
    const int64_t a = put("a", data);
    const int64_t id = chunk_of(a);
    const int64_t msg = chunk(id).msg_id;

    std::atomic<int> hints{0};
    VfsOptions o = options();
    o.upload_probe = [&](std::string_view step) {
        if (step == "dedup_hinted" && hints.fetch_add(1) == 0) {
            remove("a");
            if (collected) {
                env.meta->write([&](meta::Db& db) { meta::delete_chunk_row(db, id); });
                const std::array<int64_t, 1> ids{msg};
                env.store->delete_messages(ids);
            }
        }
    };
    env.restart(o);
    const int64_t b = put("b", data);
    EXPECT_EQ(hints.load(), 1) << "the second pass finds nothing to reuse";
    EXPECT_EQ(puts(), 2U);
    EXPECT_NE(chunk_of(b), id);
    EXPECT_EQ(chunk(chunk_of(b)).refcount, 1);
    EXPECT_EQ(env.read_all(b), data);
    EXPECT_EQ(v().status().deduped_chunks, 0U);
    EXPECT_TRUE(env.accounting_ok());
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
}

INSTANTIATE_TEST_SUITE_P(Steps, VfsDedupRace, ::testing::Bool(),
                         [](const auto& p) { return p.param ? "Collected" : "Died"; });

TEST_F(VfsDedup, ZeroContentMacIsNeverFound) {
    const Bytes data = pattern(kBig, 7);
    const int64_t a = put("a", data);
    const int64_t id = chunk_of(a);
    // Как fsck --repair: битому чанку обнуляется content_mac.
    env.meta->write([&](meta::Db& db) {
        meta::Stmt st{db, "UPDATE chunks SET content_mac = zeroblob(32) WHERE chunk_id = ?"};
        st.bind(1, id).run();
    });
    const int64_t b = put("b", data);
    EXPECT_EQ(puts(), 2U) << "a chunk with zero MAC must not be reused";
    EXPECT_NE(chunk_of(b), id);
    EXPECT_EQ(chunk(id).refcount, 1);
    EXPECT_EQ(env.read_all(b), data);
    // А настоящий MAC найден и теперь: новый чанк b принимает третью копию.
    const int64_t c = put("c", data);
    EXPECT_EQ(puts(), 2U);
    EXPECT_EQ(chunk_of(c), chunk_of(b));
}

TEST_F(VfsDedup, RewritingTheSameBytesKeepsTheChunk) {
    const Bytes data = pattern(kBig, 8);
    const int64_t f = put("f", data);
    const int64_t id = chunk_of(f);
    // Перезапись теми же байтами: чанк тот же, refcount не растёт.
    const uint64_t fh = v().open(f, O_RDWR);
    ASSERT_EQ(v().write(f, 0, data), data.size());
    v().fsync(f);
    v().release(fh);
    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(chunk_of(f), id);
    EXPECT_EQ(chunk(id).refcount, 1);
    EXPECT_EQ(chunk(id).live_bytes, static_cast<int64_t>(kBig));
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsDedup, ReadModifyWriteLeavesTheSharedChunkAndReturnsToIt) {
    const Bytes data = pattern(kBig, 9);
    const int64_t a = put("a", data);
    const int64_t b = put("b", data);
    const int64_t shared = chunk_of(a);
    ASSERT_EQ(chunk(shared).refcount, 2);

    // RMW в b: у b новый чанк, у a прежний.
    const uint64_t fh = v().open(b, O_RDWR);
    ASSERT_EQ(v().write(b, 500, bytes_of("patched")), 7U);
    v().fsync(b);
    EXPECT_EQ(puts(), 2U);
    EXPECT_NE(chunk_of(b), shared);
    EXPECT_EQ(chunk(shared).refcount, 1);
    EXPECT_EQ(env.read_all(a), data);

    // Возвращаем исходные байты: b снова ссылается на общий чанк, новый чанк мёртв.
    const int64_t patched = chunk_of(b);
    ASSERT_EQ(v().write(b, 500, ByteSpan{data.data() + 500, 7}), 7U);
    v().fsync(b);
    v().release(fh);
    EXPECT_EQ(puts(), 2U);
    EXPECT_EQ(chunk_of(b), shared);
    EXPECT_EQ(chunk(shared).refcount, 2);
    EXPECT_EQ(chunk(patched).refcount, 0);
    EXPECT_TRUE(chunk(patched).dead_since_gen.has_value());
    EXPECT_EQ(env.read_all(b), data);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsDedup, TruncateAndRegrowReturnsToTheSameChunk) {
    const Bytes data = pattern(kBig, 10);
    const int64_t a = put("a", data);
    const int64_t b = put("b", data);
    const int64_t shared = chunk_of(a);

    const uint64_t fh = v().open(b, O_RDWR);
    SetAttr cut;
    cut.size = 50'000;
    v().setattr(b, cut);
    EXPECT_EQ(chunk(shared).refcount, 2) << "truncate does not re-upload";
    EXPECT_EQ(env.extents(b).front().len, 50'000);
    // Дописываем те же байты: RMW собирает прежний чанк, MAC совпадает.
    ASSERT_EQ(v().write(b, 50'000, ByteSpan{data.data() + 50'000, kBig - 50'000}), kBig - 50'000);
    v().fsync(b);
    v().release(fh);
    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(chunk_of(b), shared);
    EXPECT_EQ(chunk(shared).refcount, 2);
    EXPECT_EQ(chunk(shared).live_bytes, 2 * static_cast<int64_t>(kBig));
    EXPECT_EQ(env.read_all(b), data);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsDedup, TruncateToZeroAndRewriteUploadsAgain) {
    const Bytes data = pattern(kBig, 11);
    const int64_t a = put("a", data);
    const int64_t id = chunk_of(a);
    const uint64_t fh = v().open(a, O_RDWR | O_TRUNC);
    EXPECT_EQ(chunk(id).refcount, 0);
    ASSERT_EQ(v().write(a, 0, data), data.size());
    v().fsync(a);
    v().release(fh);
    EXPECT_EQ(puts(), 2U) << "the dead chunk is not reused";
    EXPECT_EQ(chunk_count(), 2);
    EXPECT_NE(chunk_of(a), id);
    EXPECT_EQ(chunk(id).refcount, 0);
    EXPECT_EQ(chunk(chunk_of(a)).refcount, 1);
    EXPECT_EQ(env.read_all(a), data);
    EXPECT_TRUE(env.accounting_ok());
}

// Сбой между транзакцией index.sqlite (экстент и refcount++) и транзакцией state.sqlite (строка
// dirty_chunks): после перезапуска чанк привязывается повторно, refcount остаётся верным.
TEST_F(VfsDedup, CrashBetweenIndexAndStateTransactionsKeepsRefcount) {
    const Bytes data = pattern(kBig, 12);
    const int64_t a = put("a", data);
    const int64_t id = chunk_of(a);

    // Грязная позиция b, ничего ещё не залито: дескриптор открыт, таймер выключен.
    const Created c = v().create(kRoot, bytes_of("b"), 0644, O_RDWR, 1000, 1000);
    const int64_t b = c.attr.ino;
    ASSERT_EQ(v().write(b, 0, data), data.size());
    ASSERT_EQ(env.state->list_dirty_chunks().size(), 1U);

    // «Падение» до заливки: копия state.sqlite и staging/ как они были на диске.
    env.vfs.reset();
    env.state.reset();
    env.meta.reset();
    const auto backup = env.dir.path() / "backup";
    std::filesystem::create_directories(backup / "staging");
    std::filesystem::copy_file(env.state_path(), backup / "state.sqlite");
    for (const auto& de : std::filesystem::directory_iterator(env.staging())) {
        std::filesystem::copy_file(de.path(), backup / "staging" / de.path().filename());
    }

    env.open_dbs();
    env.start(options());
    env.vfs->flush_all();
    ASSERT_EQ(puts(), 1U);
    ASSERT_EQ(chunk(id).refcount, 2);
    ASSERT_EQ(chunk_of(b), id);

    // «Падение» после index.sqlite и до state.sqlite: индекс уже привязан, строка dirty_chunks
    // и staging-файл вернулись.
    env.vfs.reset();
    env.state.reset();
    env.meta.reset();
    std::filesystem::remove(env.state_path());
    std::filesystem::remove(env.state_path().string() + "-wal");
    std::filesystem::remove(env.state_path().string() + "-shm");
    std::filesystem::copy_file(backup / "state.sqlite", env.state_path());
    for (const auto& de : std::filesystem::directory_iterator(backup / "staging")) {
        std::filesystem::copy_file(de.path(), env.staging() / de.path().filename(),
                                   std::filesystem::copy_options::overwrite_existing);
    }
    env.open_dbs();
    ASSERT_EQ(env.state->list_dirty_chunks().size(), 1U);
    env.start(options());
    env.vfs->flush_all();

    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(chunk(id).refcount, 2) << "the repeated binding must not count twice";
    EXPECT_EQ(chunk(id).live_bytes, 2 * static_cast<int64_t>(kBig));
    EXPECT_EQ(chunk_count(), 1);
    EXPECT_TRUE(env.accounting_ok());
    EXPECT_EQ(env.read_all(b), data);
    env.restart();
    EXPECT_EQ(chunk(id).refcount, 2);
    EXPECT_EQ(env.read_all(a), data);
    EXPECT_EQ(env.read_all(b), data);
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
}

}  // namespace
}  // namespace telega::vfs::test
