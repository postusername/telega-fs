// Монтирование (SPEC 7.2): каждая строка таблицы, разблокировка ключей, проверка цепочки
// (порча, перестановка, пропуск, подмена), сироты upload_journal, открытые удалённые inode.

#include <gtest/gtest.h>
#include <sodium.h>

#include <fstream>

#include "commit_test_util.h"
#include "crypto/keyring.h"
#include "meta/chain.h"

namespace telega::meta {
namespace {

using store::FaultAction;
using store::StoreOp;
using test::CommitEnv;
using test::normalized;

// Store, который первые `hidden` вызовов read_head отвечает заданной старой головой (или 404),
// как TDLib с кэшем ChannelFull сразу после init или размонтирования (SPEC 2).
class StaleReadStore final : public store::Store {
  public:
    StaleReadStore(store::Store& inner, int hidden, std::optional<store::HeadInfo> stale)
        : inner_(inner), hidden_(hidden), stale_(std::move(stale)) {}

    [[nodiscard]] int head_reads() const { return reads_; }

    store::ChunkRef put_chunk(const std::filesystem::path& f) override {
        return inner_.put_chunk(f);
    }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        inner_.read_range(r, off, out);
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_.delete_messages(ids); }
    std::optional<store::HeadInfo> read_head() override {
        ++reads_;
        if (hidden_ > 0) {
            --hidden_;
            return stale_;
        }
        return inner_.read_head();
    }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const std::filesystem::path& f) override { return inner_.put_index(f); }
    void set_head(int64_t new_id, std::optional<int64_t> old_id) override {
        inner_.set_head(new_id, old_id);
    }

  private:
    store::Store& inner_;
    int hidden_;
    std::optional<store::HeadInfo> stale_;
    int reads_ = 0;
};

class Mount : public CommitEnv {
  protected:
    static void commit_change(MountedFs& fs, std::string_view name) {
        mkfile(fs, name);
        fs.committer().commit_now();
    }

    // Подменить список закреплённых сообщений канала (откат головы, пустая голова).
    void set_pinned(const std::vector<int64_t>& ids) {
        std::string text;
        for (const int64_t id : ids) {
            text += std::to_string(id) + "\n";
        }
        write_file_atomic(channel_dir() / "pinned", as_bytes(text));
    }

    MountError mount_error(const FsPaths& paths, const std::optional<MountOptions>& o = {}) {
        try {
            mount(paths, o);
        } catch (const MountError& e) {
            return e;
        }
        ADD_FAILURE() << "mount did not fail";
        return {MountErrorKind::local_state, "none"};
    }

    static MountError mount_error_with(store::Store& store, const FsPaths& paths,
                                       const MountOptions& o) {
        try {
            mount_fs(store, paths, o);
        } catch (const MountError& e) {
            return e;
        }
        ADD_FAILURE() << "mount did not fail";
        return {MountErrorKind::local_state, "none"};
    }

    MountOptions with(bool MountOptions::*flag) {
        MountOptions o = mount_options();
        o.*flag = true;
        return o;
    }

    // Цепочка: снимок (init, поколение 1) и три дельты. Вернуть msg_id по поколениям.
    std::vector<int64_t> make_chain_of_four() {
        init();
        auto fs = mount(a_);
        std::vector<int64_t> ids{test::must(store_->read_head()).msg_id};
        for (int i = 0; i < 3; ++i) {
            commit_change(*fs, "f" + std::to_string(i));
            ids.push_back(test::must(store_->read_head()).msg_id);
        }
        dump_a_ = normalized(test::dump_of(*fs));
        keys_ = fs->keys();
        fs_uuid_ = fs->fs_uuid();
        fs->unmount();
        return ids;
    }

    std::string dump_a_;
    crypto::SubKeys keys_;
    FsUuid fs_uuid_{};
};

// --- init и «та же» / «другая» машина ---

TEST_F(Mount, SameAndOtherMachineSeeTheSameIndex) {
    init();
    {
        auto fs = mount(b_);  // сразу после init на другой машине
        EXPECT_EQ(fs->report().situation, MountSituation::built_fresh);
        auto local = mount(machine("c"));
        EXPECT_EQ(test::dump_of(*fs), test::dump_of(*local));
    }
    std::filesystem::remove_all(b_.data_dir);

    std::string dump_a;
    {
        auto fs = mount(a_);
        EXPECT_EQ(fs->report().situation, MountSituation::same_generation);
        EXPECT_FALSE(fs->report().unclean_shutdown);
        const int64_t dir = mkdir(*fs, "dir");
        mkfile(*fs, "file", dir);
        const int64_t x = mkfile(*fs, "x", dir);
        fs->meta().write([&](Db& db) { set_inline(db, x, test::bytes_of("hi"), 1); });
        fs->committer().commit_now();
        commit_change(*fs, "more");
        dump_a = test::dump_of(*fs);
        fs->unmount();
        EXPECT_EQ(state_int(*fs, state_key::clean_shutdown), 1);
    }
    auto fb = mount(b_);
    EXPECT_EQ(fb->report().situation, MountSituation::built_fresh);
    EXPECT_EQ(fb->report().remote_generation, 3U);
    EXPECT_EQ(test::dump_of(*fb), dump_a);  // включая index_history
    EXPECT_EQ(fb->state().list_chain().size(), 3U);
    EXPECT_EQ(state_int(*fb, state_key::base_generation), 3);
    EXPECT_EQ(fb->params().chunk_size, 1 << 16);
    EXPECT_TRUE(std::filesystem::exists(b_.keyring()));
    // keyring новой машины: тот же master (тот же wrapped_master), свой tdlib_db_key.
    EXPECT_EQ(crypto::load_keyring(b_.keyring()).wrapped_master,
              crypto::load_keyring(a_.keyring()).wrapped_master);
}

// --- G_r == G_l ---

TEST_F(Mount, SameGenerationWithForeignHeadHashIsCorrupt) {
    init();
    {
        StateStore state{a_.state_db()};
        Bytes bad(32, std::byte{7});
        state.set_blob(state_key::head_hash, bad);
    }
    EXPECT_EQ(mount_error(a_).kind(), MountErrorKind::corrupt);
}

// --- G_r > G_l без локальных изменений ---

TEST_F(Mount, RemoteAheadInSameChainCatchesUp) {
    init();
    {
        auto fa = mount(a_);
        commit_change(*fa, "one");
        fa->unmount();
    }
    {
        auto fb = mount(b_);
        EXPECT_EQ(fb->report().situation, MountSituation::built_fresh);
        fb->unmount();
    }
    std::string dump_a;
    {
        auto fa = mount(a_);
        commit_change(*fa, "two");
        commit_change(*fa, "three");
        dump_a = test::dump_of(*fa);
        fa->unmount();
    }
    auto fb = mount(b_);
    EXPECT_EQ(fb->report().situation, MountSituation::caught_up);
    EXPECT_EQ(fb->report().local_generation, 2U);
    EXPECT_EQ(fb->report().remote_generation, 4U);
    EXPECT_EQ(test::dump_of(*fb), dump_a);
    EXPECT_EQ(state_int(*fb, state_key::base_generation), 4);
    EXPECT_EQ(fb->state().list_chain().size(), 4U);
    // И дальше коммиты B ложатся на ту же цепочку.
    commit_change(*fb, "from-b");
    EXPECT_EQ(head_header().kind, crypto::BlobKind::delta);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fb)));
}

TEST_F(Mount, RemoteAheadInNewChainRebuilds) {
    commit_config_.max_chain = 1;
    init();
    {
        auto fb = mount(b_);
        fb->unmount();
    }
    std::string dump_a;
    {
        auto fa = mount(a_);
        commit_change(*fa, "one");  // дельта 2
        commit_change(*fa, "two");  // снимок 3: G_l = 1 уже не в цепочке головы
        dump_a = test::dump_of(*fa);
        fa->unmount();
    }
    auto fb = mount(b_);
    EXPECT_EQ(fb->report().situation, MountSituation::rebuilt);
    EXPECT_EQ(test::dump_of(*fb), dump_a);
}

TEST_F(Mount, CatchUpFallsBackToRebuildWhenDeltasConflict) {
    init();
    {
        auto fb = mount(b_);
        fb->unmount();
    }
    // Локальный индекс B отличается от блоба G_l служебной записью, которую везёт и дельта A
    // (строка index_history с поколением < G_l): INSERT существующей строки — конфликт.
    {
        MetaStore meta{b_.index_db()};
        meta.write([](Db& db) { add_history(db, 0, 999, BlobKind::snapshot); },
                   WriteKind::housekeeping);
    }
    std::string dump_a;
    {
        auto fa = mount(a_);
        fa->meta().write([](Db& db) { add_history(db, 0, 999, BlobKind::snapshot); },
                         WriteKind::housekeeping);
        commit_change(*fa, "one");
        commit_change(*fa, "two");
        dump_a = test::dump_of(*fa);
        fa->unmount();
    }
    auto fb = mount(b_);
    EXPECT_EQ(fb->report().situation, MountSituation::rebuilt);
    EXPECT_EQ(test::dump_of(*fb), dump_a);
}

// --- G_r > G_l с локальными изменениями ---

TEST_F(Mount, RemoteAheadWithLocalChangesIsAConflict) {
    init();
    std::string dump_b;
    {
        store::LocalDirStore offline{channel_dir()};
        offline.set_faults({store::FaultRule{
            .op = StoreOp::put_index, .nth = std::nullopt, .action = FaultAction::transient}});
        auto fb = mount_fs(offline, b_, mount_options());
        mkfile(*fb, "local-only");
        EXPECT_THROW(fb->unmount(), CommitError);  // финальный коммит не удаётся: патч в pending
        dump_b = test::dump_of(*fb);
        EXPECT_EQ(fb->state().list_pending().size(), 1U);
    }
    std::string dump_a;
    {
        auto fa = mount(a_);
        commit_change(*fa, "remote");
        dump_a = test::dump_of(*fa);
        fa->unmount();
    }
    const MountError e = mount_error(b_);
    EXPECT_EQ(e.kind(), MountErrorKind::local_conflict);
    EXPECT_NE(std::string(e.what()).find("--discard-local"), std::string::npos);
    EXPECT_NE(std::string(e.what()).find("--ro"), std::string::npos);

    {
        auto ro = mount(b_, with(&MountOptions::read_only));
        EXPECT_EQ(ro->report().situation, MountSituation::local_read_only);
        EXPECT_TRUE(ro->read_only());
        EXPECT_EQ(test::dump_of(*ro), dump_b);  // локальные изменения видны, ничего не потеряно
        ro->unmount();
    }
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::local_conflict);

    // Грязный чанк overlay тоже считается локальным изменением и выбрасывается.
    {
        StateStore state{b_.state_db()};
        state.upsert_dirty_chunk(2, 0, "dirty-2-0", 1);
        state.set_dirty_attrs(DirtyAttrs{2, 10, 1});
    }
    write_file_atomic(b_.staging() / "dirty-2-0", test::bytes_of("data"));
    auto fb = mount(b_, with(&MountOptions::discard_local));
    EXPECT_EQ(fb->report().situation, MountSituation::discarded_local);
    EXPECT_EQ(test::dump_of(*fb), dump_a);
    EXPECT_TRUE(fb->state().list_pending().empty());
    EXPECT_TRUE(fb->state().list_dirty_chunks().empty());
    EXPECT_TRUE(fb->state().list_dirty_attrs().empty());
    EXPECT_FALSE(std::filesystem::exists(b_.staging() / "dirty-2-0"));
    EXPECT_EQ(state_int(*fb, state_key::index_dirty), 0);
}

// Баг 3: --discard-local не должен терять состояние, если пересборка индекса упала. Старый
// порядок стирал overlay, pending и index_dirty до пересборки, и следующее монтирование
// накатывало дельты на индекс с «выброшенными», но не откатанными изменениями.
TEST_F(Mount, DiscardLocalFailureInTheMiddleKeepsTheConflict) {
    init();
    {
        store::LocalDirStore offline{channel_dir()};
        offline.set_faults({store::FaultRule{
            .op = StoreOp::put_index, .nth = std::nullopt, .action = FaultAction::transient}});
        auto fb = mount_fs(offline, b_, mount_options());
        mkfile(*fb, "local-only");
        EXPECT_THROW(fb->unmount(), CommitError);  // патч остался в pending
    }
    std::string dump_a;
    {
        auto fa = mount(a_);
        commit_change(*fa, "remote");
        dump_a = test::dump_of(*fa);
        fa->unmount();
    }
    {
        StateStore state{b_.state_db()};
        state.upsert_dirty_chunk(2, 0, "dirty-2-0", 1);
    }
    write_file_atomic(b_.staging() / "dirty-2-0", test::bytes_of("data"));

    // Сбой сборки: на месте временного снимка непустой каталог.
    const auto blocker = b_.tmp() / "chain-snapshot.sqlite";
    std::filesystem::create_directories(blocker);
    write_file_atomic(blocker / "x", test::bytes_of("x"));
    EXPECT_ANY_THROW(mount(b_, with(&MountOptions::discard_local)));
    std::filesystem::remove_all(blocker);

    {
        StateStore state{b_.state_db()};
        EXPECT_EQ(state.list_pending().size(), 1U);
        EXPECT_EQ(state.list_dirty_chunks().size(), 1U);
    }
    EXPECT_TRUE(std::filesystem::exists(b_.staging() / "dirty-2-0"));
    // Конфликт никуда не делся: молчаливого догона дельтами на испорченный индекс нет.
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::local_conflict);

    auto fb = mount(b_, with(&MountOptions::discard_local));
    EXPECT_EQ(fb->report().situation, MountSituation::discarded_local);
    EXPECT_EQ(test::dump_of(*fb), dump_a);
    EXPECT_TRUE(fb->state().list_pending().empty());
    EXPECT_TRUE(fb->state().list_dirty_chunks().empty());
    EXPECT_FALSE(std::filesystem::exists(b_.staging() / "dirty-2-0"));
}

// Баг 3, родственный: индекса нет, а локальные изменения (overlay) есть. Молча выбрасывать их
// нельзя, и особенно при --ro (ничего не меняет локально): отказ, как local_conflict. Выбросить
// можно только явным --discard-local.
TEST_F(Mount, MissingLocalIndexWithLocalChangesIsAConflict) {
    init();
    {
        auto fb = mount(b_);
        fb->unmount();
    }
    {
        StateStore state{b_.state_db()};
        state.upsert_dirty_chunk(1, 0, "dirty-1-0", 1);
        state.set_dirty_attrs(DirtyAttrs{1, 10, 1});
    }
    write_file_atomic(b_.staging() / "dirty-1-0", test::bytes_of("data"));
    for (const char* suffix : {"", "-wal", "-shm"}) {
        std::filesystem::remove(b_.data_dir / (std::string("index.sqlite") + suffix));
    }
    const std::string remote = remote_state();

    const auto untouched = [&] {
        StateStore state{b_.state_db()};
        EXPECT_EQ(state.list_dirty_chunks().size(), 1U);
        EXPECT_EQ(state.list_dirty_attrs().size(), 1U);
        EXPECT_TRUE(std::filesystem::exists(b_.staging() / "dirty-1-0"));
        EXPECT_FALSE(std::filesystem::exists(b_.index_db()));
    };
    const MountError ro = mount_error(b_, with(&MountOptions::read_only));
    EXPECT_EQ(ro.kind(), MountErrorKind::local_conflict);
    EXPECT_NE(std::string(ro.what()).find("--discard-local"), std::string::npos);
    untouched();
    MountOptions ro_discard = with(&MountOptions::read_only);
    ro_discard.discard_local = true;  // --ro сильнее: ничего не меняет
    EXPECT_EQ(mount_error(b_, ro_discard).kind(), MountErrorKind::local_conflict);
    untouched();
    const MountError rw = mount_error(b_);
    EXPECT_EQ(rw.kind(), MountErrorKind::local_conflict);
    EXPECT_NE(std::string(rw.what()).find("--discard-local"), std::string::npos);
    untouched();

    auto fb = mount(b_, with(&MountOptions::discard_local));
    EXPECT_EQ(fb->report().situation, MountSituation::discarded_local);
    EXPECT_EQ(normalized(test::dump_of(*fb)), remote);
    EXPECT_TRUE(fb->state().list_dirty_chunks().empty());
    EXPECT_TRUE(fb->state().list_dirty_attrs().empty());
    EXPECT_FALSE(std::filesystem::exists(b_.staging() / "dirty-1-0"));
}

// N2: индекса нет, --discard-local. Сначала транзакция state.sqlite (сброс локальных строк),
// потом пересборка индекса: сбой между ними не должен дать свежий индекс при старом
// base_generation и локальных строках (при G_l == G_r следующее монтирование накатило бы
// pending и dirty_chunks на чужой индекс).
class DiscardWithoutIndex : public Mount {
  protected:
    // Машина b: поколение совпадает с каналом, индекс удалён, остались локальные строки.
    void make_orphaned_local_state() {
        init();
        {
            auto fb = mount(b_);
            fb->unmount();
        }
        {
            StateStore state{b_.state_db()};
            state.upsert_dirty_chunk(1, 0, "dirty-1-0", 1);
            state.set_dirty_attrs(DirtyAttrs{1, 10, 1});
        }
        write_file_atomic(b_.staging() / "dirty-1-0", test::bytes_of("data"));
        for (const char* suffix : {"", "-wal", "-shm"}) {
            std::filesystem::remove(b_.data_dir / (std::string("index.sqlite") + suffix));
        }
        remote_ = remote_state();
    }

    std::string remote_;
};

// Сбой после транзакции state (пересборка упала): состояние штатное «индекса нет, изменений
// нет», обычное монтирование без --discard-local собирает индекс сам.
TEST_F(DiscardWithoutIndex, CrashAfterStateTransactionLeavesNoIndexAndNoLocalChanges) {
    make_orphaned_local_state();
    const auto blocker = b_.tmp() / "chain-snapshot.sqlite";  // сбой сборки
    std::filesystem::create_directories(blocker);
    write_file_atomic(blocker / "x", test::bytes_of("x"));
    EXPECT_ANY_THROW(mount(b_, with(&MountOptions::discard_local)));
    std::filesystem::remove_all(blocker);
    EXPECT_FALSE(std::filesystem::exists(b_.index_db()));

    auto fb = mount(b_);
    EXPECT_EQ(fb->report().situation, MountSituation::built_fresh);
    EXPECT_EQ(normalized(test::dump_of(*fb)), remote_);
    EXPECT_TRUE(fb->state().list_dirty_chunks().empty());
    EXPECT_TRUE(fb->state().list_dirty_attrs().empty());
    EXPECT_TRUE(fb->state().list_pending().empty());
    EXPECT_EQ(state_int(*fb, state_key::index_dirty), 0);
}

// Сбой самой транзакции state: до неё индекса не должно быть, иначе следующее монтирование
// увидело бы свежий индекс того же поколения и локальные строки поверх него.
TEST_F(DiscardWithoutIndex, FailedStateTransactionDoesNotLeaveFreshIndexOverLocalRows) {
    make_orphaned_local_state();
    {
        Db db = Db::open(b_.state_db());
        db.exec(
            "CREATE TRIGGER boom BEFORE DELETE ON dirty_chunks BEGIN "
            "SELECT RAISE(ABORT, 'boom'); END");
    }
    EXPECT_ANY_THROW(mount(b_, with(&MountOptions::discard_local)));
    {
        Db db = Db::open(b_.state_db());
        db.exec("DROP TRIGGER boom");
    }
    EXPECT_FALSE(std::filesystem::exists(b_.index_db()))
        << "a fresh index was built before the state transaction";
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::local_conflict);
    {
        StateStore state{b_.state_db()};
        EXPECT_EQ(state.list_dirty_chunks().size(), 1U);
    }

    auto fb = mount(b_, with(&MountOptions::discard_local));
    EXPECT_EQ(fb->report().situation, MountSituation::discarded_local);
    EXPECT_EQ(normalized(test::dump_of(*fb)), remote_);
    EXPECT_TRUE(fb->state().list_dirty_chunks().empty());
}

// --- G_r < G_l ---

TEST_F(Mount, RolledBackHeadIsRefused) {
    init();
    std::vector<int64_t> head_of(5);
    std::string dump_a;
    {
        auto fa = mount(a_);
        head_of[1] = test::must(store_->read_head()).msg_id;
        for (int g = 2; g <= 4; ++g) {
            commit_change(*fa, "f" + std::to_string(g));
            head_of[static_cast<size_t>(g)] = test::must(store_->read_head()).msg_id;
        }
        dump_a = normalized(test::dump_of(*fa));
        fa->unmount();
    }
    set_pinned({head_of[2]});  // злоумышленник вернул старую голову
    EXPECT_EQ(head_header().generation, 2U);
    const MountError e = mount_error(a_);
    EXPECT_EQ(e.kind(), MountErrorKind::rollback);
    EXPECT_NE(std::string(e.what()).find("--force-local"), std::string::npos);

    {
        auto ro = mount(a_, with(&MountOptions::read_only));
        EXPECT_EQ(ro->report().situation, MountSituation::local_read_only);
        EXPECT_EQ(normalized(test::dump_of(*ro)), dump_a);
        ro->unmount();
    }

    auto fa = mount(a_, with(&MountOptions::force_local));
    EXPECT_EQ(fa->report().situation, MountSituation::forced_local);
    fa->committer().commit_now();  // force_local ставит index_dirty: снимок уходит сразу
    const auto h = head_header();
    EXPECT_EQ(h.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(h.generation, 5U);  // больше и G_l = 4, и G_r = 2
    EXPECT_EQ(h.prev_msg_id, head_of[2]);  // CAS и prev_msg_id — против реальной головы
    EXPECT_EQ(store_->pinned().size(), 1U);
    EXPECT_EQ(remote_state(), dump_a);
    fa->unmount();
    EXPECT_EQ(mount(a_)->report().situation, MountSituation::same_generation);
}

// --- кэш TDLib: голова не видна сразу после init или размонтирования (SPEC 2, 7.2 шаг 4) ---

TEST_F(Mount, StaleEmptyHeadIsRetriedWhenTheLocalHeadIsKnown) {
    init();
    {
        auto fs = mount(a_);  // локальное состояние с головой поколения 1
        fs->unmount();
    }
    StaleReadStore stale{*store_, 3, std::nullopt};
    MountOptions o = mount_options();
    o.stale_head_window = std::chrono::seconds{5};
    o.stale_head_period = std::chrono::milliseconds{1};
    auto fs = mount_fs(stale, a_, o);
    EXPECT_EQ(fs->report().situation, MountSituation::same_generation);
    EXPECT_EQ(stale.head_reads(), 4);  // три пустых ответа и настоящая голова
    fs->unmount();
}

TEST_F(Mount, StaleOlderHeadIsRetriedWhenTheLocalHeadIsKnown) {
    init();
    int64_t first = 0;
    std::optional<store::HeadInfo> old_head;
    {
        auto fs = mount(a_);
        old_head = test::must(store_->read_head());
        first = old_head->msg_id;
        commit_change(*fs, "f1");
        fs->unmount();
    }
    ASSERT_GT(test::must(store_->read_head()).msg_id, first);
    StaleReadStore stale{*store_, 2, old_head};
    MountOptions o = mount_options();
    o.stale_head_window = std::chrono::seconds{5};
    o.stale_head_period = std::chrono::milliseconds{1};
    auto fs = mount_fs(stale, a_, o);
    EXPECT_EQ(fs->report().situation, MountSituation::same_generation);
    EXPECT_EQ(fs->report().remote_generation, 2U);
    EXPECT_EQ(stale.head_reads(), 3);
    fs->unmount();
}

TEST_F(Mount, StaleHeadThatNeverClearsFailsAfterTheWindow) {
    init();
    {
        auto fs = mount(a_);
        fs->unmount();
    }
    StaleReadStore stale{*store_, 1'000'000, std::nullopt};
    MountOptions o = mount_options();
    o.stale_head_window = std::chrono::milliseconds{50};
    o.stale_head_period = std::chrono::milliseconds{5};
    try {
        mount_fs(stale, a_, o);
        FAIL() << "mount did not fail";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::no_head);
    }
    EXPECT_GT(stale.head_reads(), 2);
    EXPECT_LT(stale.head_reads(), 1000);
}

TEST_F(Mount, StaleHeadIsNotRetriedWithoutAWindowOrALocalHead) {
    init();
    {
        auto fs = mount(a_);
        fs->unmount();
    }
    {  // окно 0 (по умолчанию, LocalDirStore): ответ принимается как есть
        StaleReadStore stale{*store_, 1'000'000, std::nullopt};
        EXPECT_EQ(mount_error_with(stale, a_, mount_options()).kind(), MountErrorKind::no_head);
        EXPECT_EQ(stale.head_reads(), 1);
    }
    {  // новая машина: локальной головы нет, ждать нечего
        StaleReadStore stale{*store_, 1'000'000, std::nullopt};
        MountOptions o = mount_options();
        o.stale_head_window = std::chrono::seconds{5};
        o.stale_head_period = std::chrono::milliseconds{1};
        EXPECT_EQ(mount_error_with(stale, b_, o).kind(), MountErrorKind::no_head);
        EXPECT_EQ(stale.head_reads(), 1);
    }
}

// --- голова пуста или не блоб ---

TEST_F(Mount, EmptyChannelHasNoHead) {
    EXPECT_EQ(mount_error(a_).kind(), MountErrorKind::no_head);
    init();
    set_pinned({});
    const MountError e = mount_error(b_);
    EXPECT_EQ(e.kind(), MountErrorKind::no_head);
    EXPECT_NE(std::string(e.what()).find("init"), std::string::npos);
    EXPECT_NE(std::string(e.what()).find("recover --scan"), std::string::npos);
}

TEST_F(Mount, HeadThatIsNotAnIndexBlobIsRefused) {
    init();
    const auto file = tmp_ / "junk";
    write_file_atomic(file, Bytes(300, std::byte{0x42}));
    const auto ref = store_->put_chunk(file);
    set_pinned({ref.msg_id});
    EXPECT_EQ(mount_error(a_).kind(), MountErrorKind::not_index_blob);
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::not_index_blob);
}

// --- ключи ---

TEST_F(Mount, WrongPassphrase) {
    InitOptions o = init_options();
    o.key = KeySource{"right", {}};
    o.kdf = crypto::passphrase_kdf_params(crypto_pwhash_OPSLIMIT_MIN, crypto_pwhash_MEMLIMIT_MIN);
    init_fs(*store_, a_, o);

    MountOptions m = mount_options();
    m.key = KeySource{"wrong", {}};
    const MountError local = mount_error(a_, m);  // локальный keyring
    EXPECT_EQ(local.kind(), MountErrorKind::wrong_password);
    EXPECT_EQ(std::string(local.what()), "wrong passphrase or keyfile");
    EXPECT_EQ(mount_error(b_, m).kind(), MountErrorKind::wrong_password);  // ключи из головы
    EXPECT_FALSE(std::filesystem::exists(b_.keyring()));

    EXPECT_EQ(mount_error(b_, mount_options()).kind(), MountErrorKind::bad_key_source);
    m.key = KeySource{std::nullopt, {}};
    EXPECT_EQ(mount_error(b_, m).kind(), MountErrorKind::bad_key_source);

    m.key = KeySource{"right", {}};
    auto fb = mount(b_, m);
    EXPECT_EQ(fb->report().situation, MountSituation::built_fresh);
    EXPECT_EQ(crypto::load_keyring(b_.keyring()).kdf, crypto::load_keyring(a_.keyring()).kdf);
}

// Параметры KDF из неаутентифицированного заголовка головы (машина без keyring) ограничены
// пределами SENSITIVE (SPEC 11): подложенный заголовок не должен заставлять считать Argon2
// с произвольными параметрами. Отказ приходит до вычисления KDF.
TEST_F(Mount, ForgedKdfLimitsInHeadHeaderAreRefused) {
    InitOptions o = init_options();
    o.key = KeySource{"right", {}};
    o.kdf = crypto::passphrase_kdf_params(crypto_pwhash_OPSLIMIT_MIN, crypto_pwhash_MEMLIMIT_MIN);
    init_fs(*store_, a_, o);
    MountOptions m = mount_options();
    m.key = KeySource{"right", {}};

    const auto plant = [&](uint64_t ops, uint64_t mem) {
        const auto file = msg_file(test::must(store_->read_head()).msg_id);
        Bytes blob = read_file(file);
        constexpr size_t kOpsOffset = 97;  // kdf_opslimit, затем kdf_memlimit (SPEC 6.3)
        for (size_t i = 0; i < 8; ++i) {
            blob[kOpsOffset + i] = static_cast<std::byte>(ops >> (8 * i));
            blob[kOpsOffset + 8 + i] = static_cast<std::byte>(mem >> (8 * i));
        }
        write_file_atomic(file, blob);
    };
    plant(crypto_pwhash_OPSLIMIT_SENSITIVE + 1, crypto_pwhash_MEMLIMIT_MIN);
    EXPECT_EQ(mount_error(b_, m).kind(), MountErrorKind::corrupt);
    plant(crypto_pwhash_OPSLIMIT_MIN, crypto_pwhash_MEMLIMIT_SENSITIVE + 1);
    EXPECT_EQ(mount_error(b_, m).kind(), MountErrorKind::corrupt);
    plant(~uint64_t{0}, ~uint64_t{0});
    EXPECT_EQ(mount_error(b_, m).kind(), MountErrorKind::corrupt);
    EXPECT_FALSE(std::filesystem::exists(b_.keyring()));
}

TEST_F(Mount, WrongKeyfile) {
    init();
    const auto other = tmp_ / "other-keyfile";
    write_file_atomic(other, crypto::Key::random().span());
    MountOptions m = mount_options();
    m.key = KeySource{std::nullopt, other};
    EXPECT_EQ(mount_error(a_, m).kind(), MountErrorKind::wrong_password);
    EXPECT_EQ(mount_error(b_, m).kind(), MountErrorKind::wrong_password);
    m.key = KeySource{"a passphrase", {}};
    EXPECT_EQ(mount_error(b_, m).kind(), MountErrorKind::bad_key_source);
}

TEST_F(Mount, KeyringOfAnotherFileSystemIsRefused) {
    init();
    store_ = std::make_unique<store::LocalDirStore>(tmp_ / "channel2");
    init(b_);
    EXPECT_EQ(mount_error(a_).kind(), MountErrorKind::foreign_fs);
}

// --- порча цепочки (SPEC 6.3: prev_hash и generation проверяются всегда) ---

TEST_F(Mount, CorruptChainElementIsRefused) {
    const auto ids = make_chain_of_four();
    Bytes blob = read_file(msg_file(ids[1]));
    blob[blob.size() / 2] ^= std::byte{1};  // тег не сойдётся
    write_file_atomic(msg_file(ids[1]), blob);
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::corrupt);
    // Своя машина цепочку не скачивает: G_r == G_l, проверяется хеш головы.
    EXPECT_EQ(mount(a_)->report().situation, MountSituation::same_generation);
}

TEST_F(Mount, CorruptHeadIsRefusedEverywhere) {
    const auto ids = make_chain_of_four();
    Bytes blob = read_file(msg_file(ids[3]));
    blob.back() ^= std::byte{1};
    write_file_atomic(msg_file(ids[3]), blob);
    EXPECT_EQ(mount_error(a_).kind(), MountErrorKind::corrupt);
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::corrupt);
}

TEST_F(Mount, MissingChainElementIsRefused) {
    const auto ids = make_chain_of_four();
    std::filesystem::remove(msg_file(ids[2]));
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::corrupt);
}

TEST_F(Mount, SwappedChainElementsAreRefused) {
    const auto ids = make_chain_of_four();
    const Bytes one = read_file(msg_file(ids[1]));
    const Bytes two = read_file(msg_file(ids[2]));
    write_file_atomic(msg_file(ids[1]), two);
    write_file_atomic(msg_file(ids[2]), one);
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::corrupt);
}

TEST_F(Mount, ElementFromAnotherBranchIsRefused) {
    // Подмена элемента подлинным блобом той же ФС с тем же поколением, но из другой ветки.
    const auto ids = make_chain_of_four();
    Bytes forged;
    {
        const ChainBlob el =
            open_chain_blob(keys_.index, fs_uuid_, ids[2], read_file(msg_file(ids[2])));
        forged = crypto::seal_index_blob(
            keys_.index, el.header,
            crypto::zstd_compress(encode_delta_body(el.listed_chain, {}), kDeltaZstdLevel));
    }
    write_file_atomic(msg_file(ids[2]), forged);
    EXPECT_EQ(mount_error(b_).kind(), MountErrorKind::corrupt);
}

// Проверки fetch_chain по отдельности: блобы собирает тот, у кого есть K_index, поэтому
// каждая проверка ловит именно свою несостыковку.
class ChainChecks : public Mount {
  protected:
    void SetUp() override {
        Mount::SetUp();
        ids_ = make_chain_of_four();
        for (const int64_t id : ids_) {
            blobs_.push_back(read_file(msg_file(id)));
            elements_.push_back(open_chain_blob(keys_.index, fs_uuid_, id, blobs_.back()));
        }
    }

    // Новая голова-дельта поверх заданного списка цепочки с правкой заголовка.
    template <typename Edit>
    std::pair<int64_t, Bytes> forge_head(const std::vector<int64_t>& chain, Edit edit) {
        crypto::IndexHeader h = elements_.back().header;
        h.generation = elements_.back().header.generation + 1;
        h.prev_msg_id = chain.back();
        const auto pos =
            static_cast<size_t>(std::find(ids_.begin(), ids_.end(), chain.back()) - ids_.begin());
        h.prev_hash = crypto::blake2b256(blobs_.at(pos));
        edit(h);
        Bytes blob = crypto::seal_index_blob(
            keys_.index, h, crypto::zstd_compress(encode_delta_body(chain, {}), kDeltaZstdLevel));
        const auto file = tmp_ / "forged";
        write_file_atomic(file, blob);
        return {store_->put_index(file), std::move(blob)};
    }

    void expect_corrupt(const std::pair<int64_t, Bytes>& head) {
        EXPECT_THROW(fetch_chain(*store_, keys_.index, fs_uuid_, head.first, head.second),
                     ChainCorrupt);
    }

    std::vector<int64_t> ids_;
    std::vector<Bytes> blobs_;
    std::vector<ChainBlob> elements_;
};

TEST_F(ChainChecks, ValidChainIsAccepted) {
    const auto chain = fetch_chain(*store_, keys_.index, fs_uuid_, ids_.back(), blobs_.back());
    ASSERT_EQ(chain.size(), 4U);
    for (size_t i = 0; i < chain.size(); ++i) {
        EXPECT_EQ(chain[i].msg_id, ids_[i]);
        EXPECT_EQ(chain[i].header.generation, i + 1);
    }
    const auto ok = forge_head(ids_, [](crypto::IndexHeader&) {});
    EXPECT_EQ(fetch_chain(*store_, keys_.index, fs_uuid_, ok.first, ok.second).size(), 5U);
}

TEST_F(ChainChecks, SkippedElementIsCaughtByGeneration) {
    // Список без элемента 2, prev_hash честный: ловит непрерывность generation.
    expect_corrupt(
        forge_head({ids_[0], ids_[1], ids_[3]}, [](crypto::IndexHeader& h) { h.generation = 4; }));
    expect_corrupt(forge_head({ids_[0], ids_[1], ids_[3]}, [](crypto::IndexHeader&) {}));
}

TEST_F(ChainChecks, ReorderedElementsAreCaught) {
    expect_corrupt(forge_head({ids_[0], ids_[2], ids_[1], ids_[3]}, [](crypto::IndexHeader&) {}));
}

TEST_F(ChainChecks, WrongPrevHashIsCaught) {
    expect_corrupt(
        forge_head(ids_, [](crypto::IndexHeader& h) { h.prev_hash[0] ^= std::byte{1}; }));
}

TEST_F(ChainChecks, WrongPrevMsgIdIsCaught) {
    expect_corrupt(forge_head(ids_, [&](crypto::IndexHeader& h) { h.prev_msg_id = ids_[2]; }));
}

TEST_F(ChainChecks, WrongSnapshotFieldsAreCaught) {
    expect_corrupt(forge_head(ids_, [&](crypto::IndexHeader& h) { h.snapshot_msg_id = ids_[1]; }));
    expect_corrupt(forge_head(ids_, [](crypto::IndexHeader& h) { h.snapshot_gen = 2; }));
}

TEST_F(ChainChecks, ForeignFsUuidIsCaught) {
    expect_corrupt(forge_head(ids_, [](crypto::IndexHeader& h) { h.fs_uuid[0] ^= std::byte{1}; }));
}

TEST_F(ChainChecks, SnapshotHeadMustBeConsistent) {
    crypto::IndexHeader h = elements_.front().header;
    h.generation = 9;  // snapshot_gen остался 1
    const Bytes blob = crypto::seal_index_blob(keys_.index, h,
                                               crypto::zstd_compress(elements_.front().payload, 3));
    const auto file = tmp_ / "forged-snapshot";
    write_file_atomic(file, blob);
    const int64_t id = store_->put_index(file);
    EXPECT_THROW(fetch_chain(*store_, keys_.index, fs_uuid_, id, blob), ChainCorrupt);
}

// --- шаги 5, 7, 8 ---

TEST_F(Mount, UncleanShutdownForcesSnapshot) {
    init();
    {
        auto fs = mount(a_);
        commit_change(*fs, "one");
        mkfile(*fs, "two");  // не закоммичено, процесс «упал»
    }
    auto fs = mount(a_);
    EXPECT_TRUE(fs->report().unclean_shutdown);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_EQ(state_int(*fs, state_key::clean_shutdown), 0);
    EXPECT_TRUE(fs->meta().read(
        [](Db& db) { return lookup(db, kRootIno, test::bytes_of("two")).has_value(); }));
    fs->committer().commit_now();
    EXPECT_EQ(head_header().kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Mount, OrphanUploadsAreCleanedUp) {
    init();
    int64_t orphan_msg = 0;
    int64_t indexed_msg = 0;
    {
        auto fs = mount(a_);
        const int64_t ino = mkfile(*fs, "data");
        const int64_t indexed = put_real_chunk(*fs, ino, 0);
        indexed_msg = fs->meta().read([&](Db& db) { return get_chunk(db, indexed)->msg_id; });
        // Загрузка дошла до канала, но не до индекса.
        const auto file = tmp_ / "orphan";
        write_file_atomic(file, Bytes(50, std::byte{1}));
        orphan_msg = store_->put_chunk(file).msg_id;
        fs->state().upload_started(100, 1);
        fs->state().upload_finished(100, orphan_msg);
        fs->state().upload_started(101, 1);  // загрузка не завершилась
        fs->state().upload_started(indexed, 1);
        fs->state().upload_finished(indexed, indexed_msg);
        fs->committer().commit_now();
    }
    auto fs = mount(a_);
    EXPECT_EQ(fs->report().orphans_deleted, 1U);
    EXPECT_EQ(fs->report().orphans_forgotten, 2U);
    EXPECT_FALSE(has_message(orphan_msg));
    EXPECT_TRUE(has_message(indexed_msg));
    EXPECT_TRUE(fs->state().list_uploads().empty());
}

TEST_F(Mount, OrphanWithReusedChunkIdIsDeleted) {
    init();
    int64_t stray_msg = 0;
    int64_t indexed_msg = 0;
    {
        auto fs = mount(a_);
        const int64_t indexed = put_real_chunk(*fs, mkfile(*fs, "data"), 0);
        indexed_msg = fs->meta().read([&](Db& db) { return get_chunk(db, indexed)->msg_id; });
        const auto file = tmp_ / "stray";
        write_file_atomic(file, Bytes(50, std::byte{3}));
        stray_msg = store_->put_chunk(file).msg_id;
        fs->state().upload_started(indexed, 1);
        fs->state().upload_finished(indexed, stray_msg);
        fs->committer().commit_now();
        fs->unmount();
    }
    auto fs = mount(a_);
    EXPECT_EQ(fs->report().orphans_deleted, 1U);
    EXPECT_FALSE(has_message(stray_msg));
    EXPECT_TRUE(has_message(indexed_msg));
}

TEST_F(Mount, OrphanDeletionFailureIsRetriedNextMount) {
    init();
    int64_t orphan_msg = 0;
    {
        auto fs = mount(a_);
        const auto file = tmp_ / "orphan";
        write_file_atomic(file, Bytes(50, std::byte{1}));
        orphan_msg = store_->put_chunk(file).msg_id;
        fs->state().upload_started(100, 1);
        fs->state().upload_finished(100, orphan_msg);
        fs->unmount();
    }
    fault_next(StoreOp::delete_messages, FaultAction::transient);
    {
        auto fs = mount(a_);
        EXPECT_EQ(fs->report().orphans_deleted, 0U);
        EXPECT_EQ(fs->state().list_uploads().size(), 1U);
        fs->unmount();
    }
    auto fs = mount(a_);
    EXPECT_EQ(fs->report().orphans_deleted, 1U);
    EXPECT_FALSE(has_message(orphan_msg));
}

TEST_F(Mount, DetachedInodesAreDeleted) {
    init();
    int64_t chunk_id = 0;
    int64_t ino = 0;
    {
        auto fs = mount(a_);
        ino = mkfile(*fs, "open-and-unlinked");
        chunk_id = put_real_chunk(*fs, ino, 0);
        mkfile(*fs, "kept");
        fs->meta().write([&](Db& db) { detach_inode(db, ino); });
        fs->state().upsert_dirty_chunk(ino, 1, "dirty-x", 1);
        write_file_atomic(a_.staging() / "dirty-x", test::bytes_of("x"));
        fs->committer().commit_now();
        fs->unmount();
    }
    auto fs = mount(a_);
    EXPECT_EQ(fs->report().detached_deleted, 1U);
    EXPECT_FALSE(fs->meta().read([&](Db& db) { return get_inode(db, ino).has_value(); }));
    EXPECT_EQ(fs->meta().read([&](Db& db) { return get_chunk(db, chunk_id)->refcount; }), 0);
    EXPECT_TRUE(fs->state().list_dirty_chunks().empty());
    EXPECT_FALSE(std::filesystem::exists(a_.staging() / "dirty-x"));
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);  // удаление закоммитится
    fs->committer().commit_now();
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Mount, ReadOnlyMountChangesNothing) {
    init();
    {
        auto fs = mount(a_);
        mkfile(*fs, "x");  // «упал» с изменениями
    }
    auto ro = mount_ro(a_);
    EXPECT_EQ(ro->report().situation, MountSituation::same_generation);
    EXPECT_TRUE(ro->report().unclean_shutdown);
    ro->unmount();
    ro.reset();
    StateStore state{a_.state_db()};
    EXPECT_EQ(state.get_int(state_key::clean_shutdown), 0);
    EXPECT_EQ(state.get_int(state_key::force_snapshot), 0);
}

}  // namespace
}  // namespace telega::meta
