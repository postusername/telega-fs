// fsck (SPEC 12.3): согласованность индекса, существование сообщений, проверка тегов, исправление
// только индекса. ФС строится настоящим Vfs на LocalDirStore: инлайн, пакеты и позиционные чанки.

#include "meta/fsck.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <string>

#include "../vfs/vfs_test_util.h"
#include "commit_test_util.h"
#include "vfs/vfs.h"

namespace telega::meta {
namespace {

using test::CommitEnv;
using test::normalized;
using vfs::Vfs;
using vfs::VfsPaths;

constexpr size_t kBigSize = 200 * 1024 + 123;  // 4 позиционных чанка по 64 КиБ (init_options)
constexpr size_t kSmall = 1500;  // класс «в пакете»

bool has_kind(const FsckReport& r, FsckKind kind) {
    return std::any_of(r.issues.begin(), r.issues.end(),
                       [&](const FsckIssue& i) { return i.kind == kind; });
}

class Fsck : public CommitEnv {
  protected:
    // Файлы всех трёх классов, закоммитить, размонтировать. Запоминает ключи и номера.
    void populate() {
        init();
        auto fs = mount(a_);
        Vfs v{fs->meta(),      fs->state(), fs->store(),
              fs->committer(), fs->keys(),  VfsPaths{a_.staging(), a_.tmp()},
              options()};
        big_ = vfs::test::pattern(kBigSize, 1);
        create(v, "big", big_);
        for (uint64_t i = 0; i < 3; ++i) {
            small_.push_back(vfs::test::pattern(kSmall + i, 10 + i));
            create(v, "small" + std::to_string(i), small_.back());
        }
        create(v, "dir_tiny", vfs::test::pattern(10, 99));
        v.sync();
        v.shutdown();
        keys_ = fs->keys();
        fs->unmount();
        auto again = mount_maintenance(a_, false);
        again->meta().read([&](Db& db) {
            big_ino_ = lookup(db, kRootIno, test::bytes_of("big"))->ino;
            small_ino_ = lookup(db, kRootIno, test::bytes_of("small0"))->ino;
            tiny_ino_ = lookup(db, kRootIno, test::bytes_of("dir_tiny"))->ino;
        });
    }

    static vfs::VfsOptions options() {
        vfs::VfsOptions o = vfs::test::fast_options();
        o.pack_max_age = std::chrono::hours(1);
        o.sync_timeout = std::chrono::seconds(20);
        return o;
    }

    static void create(Vfs& v, const std::string& name, const Bytes& data) {
        const vfs::Created c = v.create(kRootIno, test::bytes_of(name), 0644, O_RDWR, 1000, 1000);
        EXPECT_EQ(v.write(c.attr.ino, 0, data), data.size());
        v.release(c.fh);
        v.forget(c.attr.ino, 1);
    }

    // Служебное монтирование, как у fsck --repair (rw) или проверки (ro).
    std::unique_ptr<MountedFs> mount_maintenance(const FsPaths& paths, bool repair) {
        MountOptions o = mount_options();
        o.maintenance = true;
        o.read_only = !repair;
        return mount(paths, o);
    }

    FsckReport check(MountedFs& fs, bool verify = false) {
        FsckOptions o;
        o.verify_data = verify;
        return fsck_check(fs.meta(), &fs.state(), *store_, fs.keys(), o);
    }

    // Испортить индекс через сессию MetaStore: порча уходит и в канал первым же коммитом.
    static void corrupt(MountedFs& fs, const std::string& sql) {
        fs.meta().write([&](Db& db) { db.exec(sql); });
        fs.committer().commit_now();
    }

    static std::vector<Extent> extents_of(MountedFs& fs, int64_t ino) {
        return fs.meta().read([&](Db& db) { return list_extents(db, ino); });
    }
    static Chunk chunk_row(MountedFs& fs, int64_t chunk_id) {
        return test::must(fs.meta().read([&](Db& db) { return get_chunk(db, chunk_id); }));
    }

    // Чистый индекс после repair: заново и на другой машине.
    void expect_repaired_everywhere(MountedFs& fs) {
        const FsckReport again = check(fs);
        EXPECT_TRUE(again.clean()) << (again.issues.empty() ? "" : again.issues.front().detail);
        EXPECT_TRUE(
            fs.meta().read([](Db& db) { return meta::test::chunk_accounting_consistent(db); }));
        EXPECT_EQ(remote_state(), normalized(test::dump_of(fs)));
    }

    Bytes big_;
    std::vector<Bytes> small_;
    int64_t big_ino_ = 0;
    int64_t small_ino_ = 0;
    int64_t tiny_ino_ = 0;
    crypto::SubKeys keys_;
};

TEST_F(Fsck, HealthyFileSystemIsClean) {
    populate();
    auto fs = mount_maintenance(a_, false);
    const FsckReport r = check(*fs);
    EXPECT_TRUE(r.clean()) << (r.issues.empty() ? "" : r.issues.front().detail);
    EXPECT_GE(r.chunks, 5U);  // 4 чанка big и пакет
    EXPECT_GE(r.extents, 7U);
    EXPECT_GE(r.messages_checked, r.chunks);
    const FsckReport verified = check(*fs, true);
    EXPECT_TRUE(verified.clean());
    EXPECT_EQ(verified.chunks_verified, verified.chunks);
}

TEST_F(Fsck, BrokenRefcountLiveBytesAndDeadSinceAreFoundAndRepaired) {
    populate();
    auto fs = mount_maintenance(a_, true);
    const auto ext = extents_of(*fs, big_ino_);
    ASSERT_EQ(ext.size(), 4U);
    const int64_t c0 = ext[0].chunk_id;
    const int64_t c1 = ext[1].chunk_id;
    const int64_t c2 = ext[2].chunk_id;
    const int64_t c3 = ext[3].chunk_id;
    const auto gen = static_cast<int64_t>(fs->committer().base_generation());
    corrupt(*fs,
            "UPDATE chunks SET refcount = refcount + 5 WHERE chunk_id = " + std::to_string(c0));
    corrupt(*fs, "UPDATE chunks SET live_bytes = 7 WHERE chunk_id = " + std::to_string(c1));
    // Живой чанк с dead_since_gen и мёртвый (refcount = 0) без него.
    corrupt(*fs, "UPDATE chunks SET dead_since_gen = 3 WHERE chunk_id = " + std::to_string(c2));
    corrupt(*fs, "UPDATE chunks SET refcount = 0 WHERE chunk_id = " + std::to_string(c3));

    const FsckReport r = check(*fs);
    EXPECT_FALSE(r.clean());
    EXPECT_TRUE(has_kind(r, FsckKind::refcount));
    EXPECT_TRUE(has_kind(r, FsckKind::live_bytes));
    EXPECT_TRUE(has_kind(r, FsckKind::dead_since_gen));
    EXPECT_FALSE(r.has_unrepairable());
    EXPECT_TRUE(r.bad_chunks.empty());  // содержимое не трогается

    const RepairResult res = fsck_repair(fs->meta(), r, gen);
    EXPECT_TRUE(res.changed());
    EXPECT_EQ(res.macs_zeroed, 0U);
    EXPECT_GE(res.refcounts_fixed, 2U);   // c0, c3
    EXPECT_EQ(res.live_bytes_fixed, 1U);  // c1
    EXPECT_GE(res.dead_since_fixed, 1U);  // c2
    fs->committer().commit_now();
    expect_repaired_everywhere(*fs);
    // Пересчёт вернул числа к экстентам.
    EXPECT_EQ(chunk_row(*fs, c0).refcount, 1);
    EXPECT_EQ(chunk_row(*fs, c1).live_bytes, ext[1].len);
    EXPECT_FALSE(chunk_row(*fs, c2).dead_since_gen.has_value());
    EXPECT_EQ(chunk_row(*fs, c3).refcount, 1);
}

// Мёртвый чанк, чьё сообщение уже убрано уборкой, а строка осталась (SPEC 6.6, служебные записи
// не уходят в канал без коммита): это не порча, fsck чистый и ничего не помечает.
TEST_F(Fsck, DeadChunkRowWithoutMessageIsNotAnIssue) {
    populate();
    auto fs = mount_maintenance(a_, true);
    const auto ext = extents_of(*fs, big_ino_);
    ASSERT_EQ(ext.size(), 4U);
    const Chunk dead = chunk_row(*fs, ext[3].chunk_id);
    const auto gen = static_cast<int64_t>(fs->committer().base_generation());
    corrupt(*fs, "DELETE FROM extents WHERE chunk_id = " + std::to_string(dead.chunk_id) +
                     "; UPDATE chunks SET refcount = 0, live_bytes = 0, dead_since_gen = " +
                     std::to_string(gen) + " WHERE chunk_id = " + std::to_string(dead.chunk_id));
    store_->delete_messages(std::vector<int64_t>{dead.msg_id});

    const FsckReport r = check(*fs, true);
    EXPECT_FALSE(has_kind(r, FsckKind::message_missing));
    EXPECT_TRUE(r.bad_chunks.empty());
    EXPECT_TRUE(r.clean()) << (r.issues.empty() ? "" : r.issues.front().detail);
}

TEST_F(Fsck, MetaCountersAndInlineBytesAreRepaired) {
    populate();
    auto fs = mount_maintenance(a_, true);
    corrupt(*fs, "UPDATE meta SET value = 1 WHERE key IN ('next_chunk_id', 'next_ino')");
    corrupt(*fs, "UPDATE meta SET value = value + 3 WHERE key = 'inline_bytes'");
    const FsckReport r = check(*fs);
    EXPECT_TRUE(has_kind(r, FsckKind::counter_behind));
    EXPECT_TRUE(has_kind(r, FsckKind::inline_bytes));
    EXPECT_FALSE(r.has_unrepairable());
    const RepairResult res = fsck_repair(fs->meta(), r, 2);
    EXPECT_EQ(res.counters_fixed, 2U);
    EXPECT_TRUE(res.inline_bytes_fixed);
    fs->committer().commit_now();
    expect_repaired_everywhere(*fs);
}

TEST_F(Fsck, MissingChunkMessageIsFoundAndRepairMarksTheChunkAndListsTheFiles) {
    populate();
    auto fs = mount_maintenance(a_, true);
    const auto ext = extents_of(*fs, big_ino_);
    const Chunk lost = chunk_row(*fs, ext[1].chunk_id);
    store_->delete_messages(std::vector<int64_t>{lost.msg_id});
    // До repair такой чанк дедупликацию находит (если сообщение целое).
    EXPECT_TRUE(
        fs->meta()
            .read([&](Db& db) { return find_chunk_by_mac(db, lost.content_mac, lost.plain_size); })
            .has_value());

    const FsckReport r = check(*fs);
    ASSERT_TRUE(has_kind(r, FsckKind::message_missing));
    EXPECT_EQ(r.bad_chunks, (std::set<int64_t>{lost.chunk_id}));
    EXPECT_FALSE(r.has_unrepairable());

    const RepairResult res = fsck_repair(fs->meta(), r, 2);
    EXPECT_EQ(res.macs_zeroed, 1U);
    EXPECT_EQ(res.affected_files, (std::vector<std::string>{"/big"}));
    fs->committer().commit_now();
    // Только индекс: в канале ничего не удалено и не добавлено.
    EXPECT_FALSE(has_message(lost.msg_id));
    const Chunk marked = chunk_row(*fs, lost.chunk_id);
    EXPECT_EQ(marked.content_mac, Bytes(32, std::byte{0}));
    EXPECT_EQ(marked.refcount, 1);  // экстент остаётся: чтение даёт EIO, а не нули
    EXPECT_FALSE(
        fs->meta()
            .read([&](Db& db) { return find_chunk_by_mac(db, lost.content_mac, lost.plain_size); })
            .has_value());

    // Повторный fsck чистый, а известный битый чанк только упомянут.
    const FsckReport again = check(*fs);
    EXPECT_TRUE(again.clean());
    EXPECT_EQ(again.known_bad_chunks, (std::set<int64_t>{lost.chunk_id}));
    EXPECT_EQ(files_using_chunks(fs->meta(), again.known_bad_chunks),
              (std::vector<std::string>{"/big"}));
    expect_repaired_everywhere(*fs);
    // Остальные файлы на месте.
    EXPECT_EQ(chunk_row(*fs, ext[0].chunk_id).content_mac.size(), 32U);
}

TEST_F(Fsck, NewWriteOfTheSameDataIsNotDeduplicatedOntoABrokenChunk) {
    populate();
    int64_t lost_chunk = 0;
    {
        auto fs = mount_maintenance(a_, true);
        const auto ext = extents_of(*fs, big_ino_);
        lost_chunk = ext[0].chunk_id;
        store_->delete_messages(std::vector<int64_t>{chunk_row(*fs, lost_chunk).msg_id});
        const FsckReport r = check(*fs);
        fsck_repair(fs->meta(), r, 2);
        fs->committer().commit_now();
    }
    // Те же 64 КиБ новым файлом: чанк обязан быть новым.
    auto fs = mount(a_);
    Vfs v{fs->meta(),      fs->state(), fs->store(),
          fs->committer(), fs->keys(),  VfsPaths{a_.staging(), a_.tmp()},
          options()};
    const Bytes first_chunk(big_.begin(), big_.begin() + (1 << 16));
    create(v, "copy", first_chunk);
    v.sync();
    const auto copy_ino =
        fs->meta().read([&](Db& db) { return lookup(db, kRootIno, test::bytes_of("copy"))->ino; });
    const auto copy_ext = extents_of(*fs, copy_ino);
    ASSERT_EQ(copy_ext.size(), 1U);
    EXPECT_NE(copy_ext[0].chunk_id, lost_chunk);
    EXPECT_EQ(v.read(copy_ino, 0, first_chunk.size()), first_chunk);
    // А чтение битого диапазона старого файла — EIO, как и обещает спецификация.
    EXPECT_THROW((void)v.read(fs->meta().read([&](Db& db) {
        return lookup(db, kRootIno, test::bytes_of("big"))->ino;
    }),
                              0, 100),
                 FsError);
    v.shutdown();
    fs->unmount();
}

TEST_F(Fsck, CorruptedChunkIsFoundOnlyWithVerifyData) {
    populate();
    auto fs = mount_maintenance(a_, true);
    const auto ext = extents_of(*fs, big_ino_);
    const Chunk victim = chunk_row(*fs, ext[2].chunk_id);
    {
        // Один байт шифртекста сегмента: тег не сойдётся.
        constexpr std::streamoff kAt = 40 + 5;
        std::fstream f(msg_file(victim.msg_id), std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.good());
        char b = 0;
        f.seekg(kAt);
        f.read(&b, 1);
        b = static_cast<char>(b ^ 0x01);
        f.seekp(kAt);
        f.write(&b, 1);
    }
    // Сообщение на месте: без проверки данных fsck ничего не видит.
    EXPECT_TRUE(check(*fs).clean());

    const FsckReport r = check(*fs, true);
    EXPECT_TRUE(has_kind(r, FsckKind::chunk_corrupt));
    EXPECT_EQ(r.bad_chunks, (std::set<int64_t>{victim.chunk_id}));
    const RepairResult res = fsck_repair(fs->meta(), r, 2);
    EXPECT_EQ(res.macs_zeroed, 1U);
    EXPECT_EQ(res.affected_files, (std::vector<std::string>{"/big"}));
    fs->committer().commit_now();
    EXPECT_TRUE(has_message(victim.msg_id));  // в канале ничего не удалялось
    const FsckReport again = check(*fs, true);
    EXPECT_TRUE(again.clean());
    EXPECT_EQ(again.known_bad_chunks.size(), 1U);
    expect_repaired_everywhere(*fs);
}

TEST_F(Fsck, TruncatedMessageIsCorrupt) {
    populate();
    auto fs = mount_maintenance(a_, true);
    const auto ext = extents_of(*fs, big_ino_);
    const Chunk victim = chunk_row(*fs, ext[0].chunk_id);
    std::filesystem::resize_file(msg_file(victim.msg_id), 1000);
    const FsckReport r = check(*fs, true);
    EXPECT_TRUE(has_kind(r, FsckKind::chunk_corrupt));
    EXPECT_EQ(r.bad_chunks.size(), 1U);
}

TEST_F(Fsck, BrokenPackAndExtentBoundsAreReportedButNotRepaired) {
    populate();
    auto fs = mount_maintenance(a_, true);
    const auto ext = extents_of(*fs, small_ino_);
    ASSERT_EQ(ext.size(), 1U);
    ASSERT_EQ(chunk_row(*fs, ext[0].chunk_id).kind, ChunkKind::pack);
    // Объект small0 наезжает на соседний (в том же пакете), у другого chunk_off за пределами чанка.
    corrupt(*fs, "UPDATE extents SET chunk_off = 1 WHERE ino = " + std::to_string(small_ino_));
    corrupt(*fs, "UPDATE extents SET chunk_off = 99999999 WHERE ino = " +
                     std::to_string(tiny_ino_) + " OR idx = 3");
    const FsckReport r = check(*fs);
    EXPECT_TRUE(has_kind(r, FsckKind::extent_bounds));
    EXPECT_TRUE(r.has_unrepairable());
    const RepairResult res = fsck_repair(fs->meta(), r, 2);
    EXPECT_EQ(res.macs_zeroed, 0U);
    EXPECT_TRUE(check(*fs).has_unrepairable());  // решение человека, repair этого не трогает
}

TEST_F(Fsck, PackObjectsThatOverlapAndPositionalChunkOffAreReported) {
    populate();
    auto fs = mount_maintenance(a_, true);
    const Chunk pack = chunk_row(*fs, extents_of(*fs, small_ino_)[0].chunk_id);
    ASSERT_EQ(pack.kind, ChunkKind::pack);
    // Все объекты пакета «в начало»: пересекаются.
    corrupt(*fs,
            "UPDATE extents SET chunk_off = 0 WHERE chunk_id = " + std::to_string(pack.chunk_id));
    // Позиционный чанк с chunk_off > 0 и экстент пакета не на idx 0.
    const auto big_ext = extents_of(*fs, big_ino_);
    corrupt(*fs, "UPDATE extents SET chunk_off = 5 WHERE ino = " + std::to_string(big_ino_) +
                     " AND idx = 0");
    const FsckReport r = check(*fs);
    EXPECT_TRUE(has_kind(r, FsckKind::pack_overlap));
    EXPECT_TRUE(has_kind(r, FsckKind::extent_chunk_off));
    EXPECT_TRUE(r.has_unrepairable());
    (void)big_ext;
}

TEST_F(Fsck, ExtentOnAMissingChunkRowIsReportedAndLeftAlone) {
    populate();
    int64_t chunk_id = 0;
    {
        auto fs = mount_maintenance(a_, true);
        chunk_id = extents_of(*fs, big_ino_)[3].chunk_id;
    }
    {
        // Строку чанка удаляем мимо внешних ключей, как это сделала бы порча.
        Db db = Db::open(a_.index_db());
        db.set_foreign_keys(false);
        db.exec("DELETE FROM chunks WHERE chunk_id = " + std::to_string(chunk_id));
    }
    auto fs = mount_maintenance(a_, true);
    const FsckReport r = check(*fs);
    EXPECT_TRUE(has_kind(r, FsckKind::extent_missing_chunk));
    EXPECT_TRUE(r.has_unrepairable());
    const RepairResult res = fsck_repair(fs->meta(), r, 2);
    EXPECT_EQ(extents_of(*fs, big_ino_).size(),
              4U);  // экстент не удалён: данные не подменяются нулями
    EXPECT_TRUE(check(*fs).has_unrepairable());
    (void)res;
}

TEST_F(Fsck, MissingChainElementIsReported) {
    populate();
    auto fs = mount_maintenance(a_, false);
    const auto chain = fs->state().list_chain();
    ASSERT_FALSE(chain.empty());
    store_->delete_messages(std::vector<int64_t>{chain.front().msg_id});
    const FsckReport r = check(*fs);
    EXPECT_TRUE(has_kind(r, FsckKind::chain_message_missing));
    EXPECT_TRUE(r.has_unrepairable());
}

// Store, который не отвечает на проверку существования.
class DownStore final : public store::Store {
  public:
    explicit DownStore(store::Store& inner) : inner_(inner) {}
    store::ChunkRef put_chunk(const std::filesystem::path& f) override {
        return inner_.put_chunk(f);
    }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        inner_.read_range(r, off, out);
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_.delete_messages(ids); }
    std::vector<bool> messages_exist(std::span<const int64_t> /*msg_ids*/) override {
        throw store::StoreError(store::StoreErrorKind::transient, "network is down");
    }
    std::optional<store::HeadInfo> read_head() override { return inner_.read_head(); }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const std::filesystem::path& f) override { return inner_.put_index(f); }
    void set_head(int64_t n, std::optional<int64_t> o) override { inner_.set_head(n, o); }

  private:
    store::Store& inner_;
};

TEST_F(Fsck, UnavailableStoreIsAnIssueAndNothingIsMarkedBad) {
    populate();
    auto fs = mount_maintenance(a_, true);
    DownStore down{*store_};
    const FsckReport r = fsck_check(fs->meta(), &fs->state(), down, fs->keys(), FsckOptions{});
    EXPECT_TRUE(has_kind(r, FsckKind::store_unavailable));
    EXPECT_TRUE(r.message_check_incomplete);
    EXPECT_TRUE(r.bad_chunks.empty());
    EXPECT_TRUE(r.has_unrepairable());
    const RepairResult res = fsck_repair(fs->meta(), r, 2);
    EXPECT_FALSE(res.changed());
}

// fsck ничего не удаляет в канале (SPEC 12.3): служебное монтирование не убирает сирот
// upload_journal, а обычное — убирает.
TEST_F(Fsck, MaintenanceMountDoesNotDeleteOrphanMessages) {
    populate();
    const auto file = tmp_ / "orphan.bin";
    write_file_atomic(file, Bytes(300, std::byte{7}));
    const int64_t orphan = store_->put_chunk(file).msg_id;
    {
        StateStore state{a_.state_db()};
        state.upload_started(9999, 1);
        state.upload_finished(9999, orphan);
    }
    {
        auto fs = mount_maintenance(a_, true);
        EXPECT_EQ(fs->report().orphans_deleted, 0U);
        EXPECT_TRUE(has_message(orphan));
        EXPECT_EQ(fs->state().list_uploads().size(), 1U);
        // После служебного монтирования ФС не помечена «смонтированной»: clean_shutdown прежний.
        EXPECT_EQ(state_int(*fs, state_key::clean_shutdown), 1);
    }
    auto fs = mount(a_);
    EXPECT_EQ(fs->report().orphans_deleted, 1U);
    EXPECT_FALSE(has_message(orphan));
}

}  // namespace
}  // namespace telega::meta
