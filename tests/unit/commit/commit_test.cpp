// CommitManager (SPEC 6.6, 7.1, 7.4) и init (SPEC 12.3): вид блоба, заголовок, склейка
// pending, неудачный снимок, CAS, фоновый поток, уборка, замеры.

#include <gtest/gtest.h>
#include <sodium.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <future>
#include <map>
#include <random>
#include <thread>

#include "commit_test_util.h"
#include "crypto/keyring.h"
#include "meta/chain.h"

namespace telega::meta {
namespace {

using namespace std::chrono_literals;
using store::FaultAction;
using store::StoreOp;
using test::CommitEnv;
using test::FakeClock;
using test::normalized;

class Commit : public CommitEnv {
  protected:
    std::unique_ptr<MountedFs> init_and_mount() {
        init();
        return mount(a_);
    }

    static crypto::BlobKind last_kind(MountedFs& fs) {
        return test::must(test::must(fs.committer().stats().last).kind);
    }

    // Изменить индекс и закоммитить; вернуть вид блоба.
    crypto::BlobKind change_and_commit(MountedFs& fs) {
        mkfile(fs, "f" + std::to_string(++files_));
        const auto r = fs.committer().commit();
        EXPECT_TRUE(r.committed);
        return test::must(r.metrics.kind);
    }

    int files_ = 0;
};

// --- тело блоба ---

TEST(DeltaBody, RoundTrip) {
    const std::vector<int64_t> chain{5, 9, -1, INT64_MAX};
    const Bytes patch = test::bytes_of("patchset bytes");
    const DeltaBody d = decode_delta_body(encode_delta_body(chain, patch));
    EXPECT_EQ(d.chain, chain);
    EXPECT_EQ(d.patchset, patch);

    const DeltaBody empty = decode_delta_body(encode_delta_body({}, {}));
    EXPECT_TRUE(empty.chain.empty());
    EXPECT_TRUE(empty.patchset.empty());
}

TEST(DeltaBody, TruncatedIsCorrupt) {
    Bytes body = encode_delta_body({1, 2, 3}, {});
    EXPECT_THROW(decode_delta_body(ByteSpan{body}.first(3)), crypto::CorruptError);
    body.pop_back();
    EXPECT_THROW(decode_delta_body(body), crypto::CorruptError);
    // n огромное: длина не сходится, без выхода за буфер.
    Bytes huge(4, std::byte{0xFF});
    EXPECT_THROW(decode_delta_body(huge), crypto::CorruptError);
}

// --- init ---

TEST_F(Commit, InitCommitsPinnedSnapshotOfGenerationOne) {
    init();
    const auto head = test::must(store_->read_head());
    const auto h = crypto::parse_index_header(head.header);
    EXPECT_EQ(h.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(h.generation, 1U);
    EXPECT_EQ(h.snapshot_gen, 1U);
    EXPECT_EQ(h.snapshot_msg_id, 0);
    EXPECT_EQ(h.prev_msg_id, 0);
    EXPECT_EQ(h.prev_hash, crypto::Hash32{});
    EXPECT_EQ(store_->pinned(), std::vector<int64_t>{head.msg_id});

    // kdf и wrapped_master в заголовке — те же, что в локальном keyring.
    const auto keyring = crypto::load_keyring(a_.keyring());
    EXPECT_EQ(h.kdf, keyring.kdf);
    EXPECT_EQ(h.wrapped_master, keyring.wrapped_master);
    EXPECT_EQ(h.fs_uuid, keyring.fs_uuid);
    EXPECT_EQ(std::filesystem::status(a_.keyring()).permissions() & std::filesystem::perms::all,
              std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);

    StateStore state{a_.state_db()};
    EXPECT_EQ(state.get_int(state_key::base_generation), 1);
    EXPECT_EQ(state.get_int(state_key::head_msg_id), head.msg_id);
    EXPECT_EQ(state.get_int(state_key::index_dirty), 0);
    EXPECT_EQ(state.get_int(state_key::clean_shutdown), 1);
    ASSERT_EQ(state.list_chain().size(), 1U);
    EXPECT_EQ(state.list_chain()[0].msg_id, head.msg_id);

    MetaStore meta{a_.index_db()};
    const Inode root = meta.read([](Db& db) { return test::must(get_inode(db, kRootIno)); });
    EXPECT_EQ(root.uid, ::getuid());
    EXPECT_EQ(root.gid, ::getgid());
    EXPECT_EQ(root.mode, 0755U);
    const auto history = meta.read([](Db& db) { return list_history(db); });
    ASSERT_EQ(history.size(), 1U);
    EXPECT_EQ(history[0].generation, 1);
    EXPECT_EQ(history[0].msg_id, head.msg_id);
}

TEST_F(Commit, InitRefusesNonEmptyChannelAndExistingProfile) {
    init();
    EXPECT_THROW(init(b_), InitError);  // канал не пуст
    EXPECT_FALSE(std::filesystem::exists(b_.keyring()));

    store_ = std::make_unique<store::LocalDirStore>(tmp_ / "channel2");
    EXPECT_THROW(init(a_), InitError);  // профиль уже занят
}

TEST_F(Commit, InitFailureLeavesNoLocalFiles) {
    fault_next(StoreOp::put_index, FaultAction::permanent);
    EXPECT_THROW(init(), CommitError);
    EXPECT_FALSE(std::filesystem::exists(a_.keyring()));
    EXPECT_FALSE(std::filesystem::exists(a_.index_db()));
    EXPECT_FALSE(std::filesystem::exists(a_.state_db()));
    store_->set_faults({});
    init();  // повтор проходит
    EXPECT_EQ(head_header().generation, 1U);
}

// Store, у которого set_head закрепляет сообщение, но отвечает ошибкой (потерянный ответ).
class LostPinReplyStore final : public store::Store {
  public:
    explicit LostPinReplyStore(store::Store& inner) : inner_(inner) {}
    store::ChunkRef put_chunk(const std::filesystem::path& f) override {
        return inner_.put_chunk(f);
    }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        inner_.read_range(r, off, out);
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_.delete_messages(ids); }
    std::optional<store::HeadInfo> read_head() override { return inner_.read_head(); }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const std::filesystem::path& b) override { return inner_.put_index(b); }
    void set_head(int64_t id, std::optional<int64_t> old) override {
        inner_.set_head(id, old);
        throw store::StoreError(store::StoreErrorKind::transient, "reply lost");
    }

  private:
    store::Store& inner_;
};

// Ошибка после pin: в канале закреплён наш блоб, keyring и базы удалять нельзя. Это успех init:
// монтирование узнаёт свой снимок и доделывает шаг 8.
TEST_F(Commit, InitFailureAfterPinKeepsLocalStateAndMountResumesIt) {
    LostPinReplyStore lossy{*store_};
    const InitResult r = init_fs(lossy, a_, init_options());
    const auto head = test::must(store_->read_head());  // pin состоялся
    EXPECT_EQ(r.head_msg_id, head.msg_id);
    EXPECT_TRUE(std::filesystem::exists(a_.keyring()));
    EXPECT_TRUE(std::filesystem::exists(a_.index_db()));
    EXPECT_TRUE(std::filesystem::exists(a_.state_db()));

    auto fs = mount(a_);
    EXPECT_TRUE(fs->report().resumed_commit);
    EXPECT_EQ(fs->report().situation, MountSituation::same_generation);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 1);
    mkfile(*fs, "after-init");
    const auto r2 = fs->committer().commit();
    ASSERT_TRUE(r2.committed);
    EXPECT_EQ(r2.metrics.generation, 2U);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    fs->unmount();
}

// set_head упал, а следующее чтение головы возвращает кэш TDLib («нет головы»): одно чтение
// приняло бы это за «pin не прошёл» и откатило init вместе с ключом к закреплённому блобу.
TEST_F(Commit, InitRollbackDoesNotTrustASingleStaleHeadRead) {
    test::MaskedHeadStore masked{*store_};
    masked.arm_after_pin(3, std::nullopt, /*lose_reply=*/true);
    InitOptions o = init_options();
    o.stale_head_window = 200ms;
    o.stale_head_period = 10ms;
    const InitResult r = init_fs(masked, a_, o);
    EXPECT_GE(masked.head_reads(), 4);
    EXPECT_EQ(r.head_msg_id, test::must(store_->read_head()).msg_id);
    EXPECT_TRUE(std::filesystem::exists(a_.keyring()));
    EXPECT_TRUE(std::filesystem::exists(a_.state_db()));
    auto fs = mount(a_);
    EXPECT_TRUE(fs->report().resumed_commit);
    fs->unmount();
}

// Pin не прошёл (голова пуста всё окно): откат, а залитый блоб удаляется.
TEST_F(Commit, InitRollbackAfterWindowRemovesFilesAndTheUnpinnedBlob) {
    test::MaskedHeadStore masked{*store_};
    InitOptions o = init_options();
    o.stale_head_window = 40ms;
    o.stale_head_period = 10ms;
    fault_next(StoreOp::set_head, FaultAction::permanent);
    EXPECT_THROW(init_fs(masked, a_, o), CommitError);
    EXPECT_FALSE(std::filesystem::exists(a_.keyring()));
    EXPECT_FALSE(std::filesystem::exists(a_.index_db()));
    EXPECT_FALSE(std::filesystem::exists(a_.state_db()));
    EXPECT_TRUE(store_->list_messages().empty());
    EXPECT_GE(masked.head_reads(), 3);
    store_->set_faults({});
    init();  // и init повторяется
}

TEST_F(Commit, InitWithPassphrase) {
    InitOptions o = init_options();
    o.key = KeySource{"correct horse", {}};
    o.kdf = crypto::passphrase_kdf_params(crypto_pwhash_OPSLIMIT_MIN, crypto_pwhash_MEMLIMIT_MIN);
    init_fs(*store_, a_, o);
    const auto h = head_header();
    EXPECT_EQ(h.kdf.alg, crypto::KdfAlg::argon2id13);
    EXPECT_EQ(h.kdf.opslimit, crypto_pwhash_OPSLIMIT_MIN);
}

// --- вид блоба (SPEC 6.6) ---

TEST_F(Commit, DeltaHeaderAndBody) {
    auto fs = init_and_mount();
    const auto first = test::must(store_->read_head());
    const Bytes first_blob = store_->read_index(first.msg_id);
    ASSERT_EQ(change_and_commit(*fs), crypto::BlobKind::delta);

    const auto head = test::must(store_->read_head());
    const Bytes blob = store_->read_index(head.msg_id);
    const auto chain_blob = open_chain_blob(fs->keys().index, fs->fs_uuid(), head.msg_id, blob);
    const auto& h = chain_blob.header;
    EXPECT_EQ(h.generation, 2U);
    EXPECT_EQ(h.snapshot_gen, 1U);
    EXPECT_EQ(h.snapshot_msg_id, first.msg_id);
    EXPECT_EQ(h.prev_msg_id, first.msg_id);
    EXPECT_EQ(h.prev_hash, crypto::blake2b256(first_blob));
    EXPECT_EQ(chain_blob.listed_chain, std::vector<int64_t>{first.msg_id});
    EXPECT_FALSE(chain_blob.payload.empty());
    // Старая голова откреплена (шаг 7).
    EXPECT_EQ(store_->pinned(), std::vector<int64_t>{head.msg_id});

    EXPECT_EQ(fs->committer().base_generation(), 2U);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    EXPECT_EQ(fs->state().list_chain().size(), 2U);
    EXPECT_TRUE(fs->state().list_pending().empty());
}

TEST_F(Commit, NothingChangedNothingCommitted) {
    auto fs = init_and_mount();
    const auto before = store_->list_messages();
    EXPECT_FALSE(fs->committer().commit().committed);
    fs->committer().commit_now();
    EXPECT_EQ(store_->list_messages(), before);
    EXPECT_EQ(fs->committer().base_generation(), 1U);

    // Откатившаяся запись ставит dirty, но патч пуст: коммита нет, dirty сбрасывается.
    EXPECT_THROW(fs->meta().write([](Db& db) {
        insert_inode(db, test::new_node(kRootIno, "rolled-back"));
        throw std::runtime_error("rollback");
    }),
                 std::runtime_error);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);
    EXPECT_FALSE(fs->committer().commit().committed);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    EXPECT_EQ(fs->committer().base_generation(), 1U);
    EXPECT_EQ(store_->list_messages(), before);

    // Служебная строка index_history шага 8 сама коммит не вызывает, но уходит со следующим.
    change_and_commit(*fs);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    EXPECT_FALSE(fs->committer().commit().committed);
    EXPECT_EQ(fs->committer().base_generation(), 2U);
}

TEST_F(Commit, SnapshotAfterMaxChainDeltas) {
    commit_config_.max_chain = 2;
    auto fs = init_and_mount();
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::delta);
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::delta);
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::snapshot);
    EXPECT_EQ(fs->state().list_chain().size(), 1U);
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::delta);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Commit, SnapshotWhenDeltasOutgrowCheckpointRatio) {
    commit_config_.checkpoint_ratio = 0.0;
    auto fs = init_and_mount();
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::delta);  // дельт ещё нет: 0 > 0 ложно
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::snapshot);
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::delta);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Commit, ForcedSnapshot) {
    auto fs = init_and_mount();
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::delta);

    fs->state().set_int(state_key::force_snapshot, 1);
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::snapshot);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 0);

    // Явный запрос снимка коммитит даже без изменений.
    const auto r = fs->committer().commit(true);
    EXPECT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(fs->committer().base_generation(), 4U);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// --- ошибки шагов 3–6 ---

TEST_F(Commit, FailedDeltaStaysPendingAndMergesWithNext) {
    auto fs = init_and_mount();
    const auto committed = normalized(test::dump_of(*fs));
    mkfile(*fs, "one");
    fault_next(StoreOp::put_index, FaultAction::transient);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    EXPECT_FALSE(fs->read_only());
    EXPECT_EQ(fs->state().list_pending().size(), 1U);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 1);
    EXPECT_EQ(fs->committer().stats().failures, 1U);
    EXPECT_EQ(remote_state(), committed);

    fault_next(StoreOp::set_head, FaultAction::transient);
    mkfile(*fs, "two");
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    EXPECT_EQ(fs->state().list_pending().size(), 2U);

    mkfile(*fs, "three");
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::delta);
    EXPECT_EQ(r.metrics.pending_merged, 3U);
    EXPECT_EQ(r.metrics.generation, 2U);
    EXPECT_TRUE(fs->state().list_pending().empty());
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    // Блоб неудавшейся попытки (set_head не прошёл) убран.
    EXPECT_EQ(store_->list_messages().size(), 2U);
}

TEST_F(Commit, FailedSnapshotForcesNextSnapshot) {
    auto fs = init_and_mount();
    change_and_commit(*fs);
    mkfile(*fs, "lost-from-session");
    fault_next(StoreOp::put_index, FaultAction::transient);
    EXPECT_THROW(fs->committer().commit(true), CommitError);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_FALSE(std::filesystem::exists(snapshot_tmp_path(a_.tmp(), 3)));

    // Следующий коммит обязан быть снимком: изменения до VACUUM INTO ушли из сессии.
    EXPECT_EQ(change_and_commit(*fs), crypto::BlobKind::snapshot);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 0);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Commit, BlobOfFailedPinIsDeletedByNextAttempt) {
    auto fs = init_and_mount();
    // set_head не прошёл: следующая попытка видит, что голова не её блоб, и удаляет его.
    mkfile(*fs, "x");
    fault_next(StoreOp::set_head, FaultAction::transient);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    const auto msgs = store_->list_messages();
    ASSERT_EQ(msgs.size(), 2U);  // снимок init и незакреплённый блоб
    mkfile(*fs, "y");
    fs->committer().commit_now();
    EXPECT_FALSE(has_message(msgs.back()));
    EXPECT_EQ(head_header().generation, 2U);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Commit, CasConflictSwitchesToReadOnly) {
    init();
    auto fa = mount(a_);
    // Вторая «машина» с тем же каналом через свой объект Store.
    store::LocalDirStore other{channel_dir()};
    auto fb = mount_fs(other, b_, mount_options());

    mkfile(*fa, "from-a");
    fa->committer().commit_now();
    const auto head_after_a = test::must(store_->read_head()).msg_id;

    mkfile(*fb, "from-b");
    EXPECT_THROW(fb->committer().commit_now(), CommitError);
    EXPECT_TRUE(fb->read_only());
    EXPECT_TRUE(fb->committer().read_only());
    EXPECT_EQ(test::must(store_->read_head()).msg_id, head_after_a);
    EXPECT_THROW(fb->committer().commit_now(), CommitError);
    // Патч B остался в pending: recover --export-dirty (M4) сможет его достать.
    EXPECT_EQ(fb->state().list_pending().size(), 1U);
    // Финальный коммит не удаётся: ошибка после полного разбора (SPEC 7.2).
    EXPECT_THROW(fb->unmount(), CommitError);
    EXPECT_EQ(fb->state().get_int(state_key::clean_shutdown), 1);
    EXPECT_EQ(fb->state().get_int(state_key::force_snapshot), 1);

    EXPECT_FALSE(fa->read_only());
    mkfile(*fa, "more");
    fa->committer().commit_now();
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fa)));
}

// --- параллельность ---

TEST_F(Commit, WritesDuringCommitAreNotLost) {
    auto fs = init_and_mount();
    mkfile(*fs, "before");
    store_->set_faults({store::FaultRule{.op = StoreOp::put_index,
                                         .nth = store_->call_count(StoreOp::put_index) + 1,
                                         .action = FaultAction::delay,
                                         .delay = 100ms}});
    const uint64_t put_calls = store_->call_count(StoreOp::put_index);
    std::thread committer([&] { fs->committer().commit_now(); });
    // Патч уже забран, идёт заливка: запись попадает в следующий коммит.
    while (store_->call_count(StoreOp::put_index) == put_calls) {
        std::this_thread::yield();
    }
    mkfile(*fs, "during");
    committer.join();
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 2);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);
    // Запись после забора патча видит поколение коммита в полёте (dead_since_gen, SPEC 7.4).
    EXPECT_EQ(fs->committer().base_generation(), 2U);
    fs->committer().commit_now();
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 3);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Commit, ConcurrentCommitNowCalls) {
    auto fs = init_and_mount();
    constexpr int kThreads = 4;
    constexpr int kRounds = 5;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int r = 0; r < kRounds; ++r) {
                mkfile(*fs, "t" + std::to_string(t) + "-" + std::to_string(r));
                fs->committer().commit_now();
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    EXPECT_LE(fs->committer().base_generation(), 1U + kThreads * kRounds);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// --- фоновый поток ---

class Background : public Commit {
  protected:
    void SetUp() override {
        Commit::SetUp();
        commit_config_.clock = &clock_;
        commit_config_.commit_interval = 60s;
        commit_config_.commit_every_chunks = 3;
        commit_config_.retry_min = 1s;
        commit_config_.retry_max = 4s;
    }
    FakeClock clock_;
};

TEST_F(Background, CommitsAfterInterval) {
    auto fs = init_and_mount();
    fs->committer().start();
    clock_.advance(120s);
    fs->committer().poke();
    // Индекс не менялся: коммита нет.
    EXPECT_FALSE(fs->committer().wait_for_generation(2, 50ms));

    mkfile(*fs, "x");
    clock_.advance(30s);
    fs->committer().poke();
    EXPECT_TRUE(fs->committer().wait_for_generation(2, 10s));  // прошло > 60 с с init
    mkfile(*fs, "y");
    clock_.advance(59s);
    fs->committer().poke();
    EXPECT_FALSE(fs->committer().wait_for_generation(3, 50ms));
    clock_.advance(2s);
    fs->committer().poke();
    EXPECT_TRUE(fs->committer().wait_for_generation(3, 10s));
    fs->unmount();
    EXPECT_EQ(fs->committer().stats().commits, 2U);
}

TEST_F(Background, CommitsAfterEveryNChunks) {
    auto fs = init_and_mount();
    fs->committer().start();
    mkfile(*fs, "x");
    fs->committer().chunk_uploaded();
    fs->committer().chunk_uploaded();
    EXPECT_FALSE(fs->committer().wait_for_generation(2, 50ms));
    fs->committer().chunk_uploaded();
    EXPECT_TRUE(fs->committer().wait_for_generation(2, 10s));
    fs->unmount();
}

TEST_F(Background, RetriesWithExponentialBackoff) {
    auto fs = init_and_mount();
    fault_next(StoreOp::put_index, FaultAction::transient, 3);
    fs->committer().start();
    mkfile(*fs, "x");
    clock_.advance(61s);
    fs->committer().poke();
    auto failures = [&] { return fs->committer().stats().failures; };
    auto wait_failures = [&](uint64_t n) {
        for (int i = 0; i < 5000 && failures() < n; ++i) {
            std::this_thread::sleep_for(1ms);
        }
        return failures();
    };
    EXPECT_EQ(wait_failures(1), 1U);
    clock_.advance(999ms);  // задержка 1 с ещё не прошла
    fs->committer().poke();
    EXPECT_FALSE(fs->committer().wait_for_generation(2, 50ms));
    EXPECT_EQ(failures(), 1U);
    clock_.advance(1ms);
    EXPECT_EQ(wait_failures(2), 2U);
    clock_.advance(2s);  // вторая задержка — 2 с
    EXPECT_EQ(wait_failures(3), 3U);
    clock_.advance(3s);  // третья — 4 с, ещё рано
    fs->committer().poke();
    EXPECT_FALSE(fs->committer().wait_for_generation(2, 50ms));
    clock_.advance(1s);
    EXPECT_TRUE(fs->committer().wait_for_generation(2, 10s));
    fs->unmount();
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Background, CommitNowFailureIsThrownAndRetriedInBackground) {
    auto fs = init_and_mount();
    fs->committer().start();
    mkfile(*fs, "x");
    fault_next(StoreOp::read_head, FaultAction::transient);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    clock_.advance(1s);
    EXPECT_TRUE(fs->committer().wait_for_generation(2, 10s));
    fs->unmount();
}

TEST_F(Background, StopWaitsForCommitInFlight) {
    auto fs = init_and_mount();
    store_->set_faults({store::FaultRule{.op = StoreOp::put_index,
                                         .nth = std::nullopt,
                                         .action = FaultAction::delay,
                                         .delay = 50ms}});
    fs->committer().start();
    mkfile(*fs, "x");
    const uint64_t cas_calls = store_->call_count(StoreOp::read_head);
    fs->committer().chunk_uploaded();
    fs->committer().chunk_uploaded();
    fs->committer().chunk_uploaded();
    while (store_->call_count(StoreOp::read_head) == cas_calls) {  // CAS фонового коммита
        std::this_thread::yield();
    }
    fs->committer().stop();
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 2);
}

// --- отложенная уборка (SPEC 7.4) ---

TEST_F(Commit, DeadChunkDeletedExactlyAfterRetainGenerations) {
    commit_config_.retain_generations = 2;
    auto fs = init_and_mount();
    const int64_t ino = mkfile(*fs, "data");
    const int64_t chunk_id = put_real_chunk(*fs, ino, 0);
    const int64_t msg =
        fs->meta().read([&](Db& db) { return test::must(get_chunk(db, chunk_id)).msg_id; });
    fs->committer().commit_now();  // G = 2
    fs->meta().write([&](Db& db) {
        delete_inode(db, ino, static_cast<int64_t>(fs->committer().base_generation()));
    });
    EXPECT_EQ(fs->meta().read([&](Db& db) { return get_chunk(db, chunk_id)->dead_since_gen; }), 2);

    change_and_commit(*fs);  // G = 3: 2 + 2 > 3
    EXPECT_TRUE(has_message(msg));
    EXPECT_TRUE(fs->meta().read([&](Db& db) { return get_chunk(db, chunk_id).has_value(); }));

    change_and_commit(*fs);  // G = 4: удаляется
    EXPECT_FALSE(has_message(msg));
    EXPECT_FALSE(fs->meta().read([&](Db& db) { return get_chunk(db, chunk_id).has_value(); }));
    EXPECT_TRUE(fs->state().list_uploads().empty());
    EXPECT_EQ(test::must(fs->committer().stats().last).gc_chunks_deleted, 1U);
    // Удаление строки — служебная запись: коммита сама не вызывает, уходит со следующим.
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    change_and_commit(*fs);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Commit, FailedChunkDeletionIsRetried) {
    commit_config_.retain_generations = 1;
    auto fs = init_and_mount();
    const int64_t ino = mkfile(*fs, "data");
    const int64_t chunk_id = put_real_chunk(*fs, ino, 0);
    const int64_t msg = fs->meta().read([&](Db& db) { return get_chunk(db, chunk_id)->msg_id; });
    fs->committer().commit_now();
    fs->meta().write([&](Db& db) { delete_inode(db, ino, 2); });
    fault_next(StoreOp::delete_messages, FaultAction::transient);
    fs->committer().commit_now();  // G = 3: строка удалена, сообщение — нет
    EXPECT_TRUE(has_message(msg));
    EXPECT_FALSE(fs->meta().read([&](Db& db) { return get_chunk(db, chunk_id).has_value(); }));
    ASSERT_EQ(fs->state().list_uploads().size(), 1U);
    EXPECT_EQ(fs->state().list_uploads()[0].msg_id, msg);

    change_and_commit(*fs);
    EXPECT_FALSE(has_message(msg));
    EXPECT_TRUE(fs->state().list_uploads().empty());
}

TEST_F(Commit, CrashBetweenRowAndMessageDeletionIsCleanedAtMount) {
    commit_config_.retain_generations = 1;
    auto fs = init_and_mount();
    const int64_t ino = mkfile(*fs, "data");
    const int64_t chunk_id = put_real_chunk(*fs, ino, 0);
    const int64_t msg = fs->meta().read([&](Db& db) { return get_chunk(db, chunk_id)->msg_id; });
    fs->committer().commit_now();
    fs->meta().write([&](Db& db) { delete_inode(db, ino, 2); });
    fault_next(StoreOp::delete_messages, FaultAction::crash_before);
    EXPECT_THROW(fs->committer().commit_now(), store::CrashInjected);
    fs.reset();
    reopen_store();
    EXPECT_TRUE(has_message(msg));

    fs = mount(a_);
    EXPECT_EQ(fs->report().orphans_deleted, 1U);
    EXPECT_FALSE(has_message(msg));
    EXPECT_TRUE(fs->state().list_uploads().empty());
}

TEST_F(Commit, OldChainsAreDeletedCurrentChainIsKept) {
    commit_config_.max_chain = 1;
    commit_config_.retain_generations = 2;
    auto fs = init_and_mount();
    std::map<uint64_t, int64_t> msg_of;  // generation → msg_id
    msg_of[1] = test::must(store_->read_head()).msg_id;
    for (uint64_t g = 2; g <= 7; ++g) {
        change_and_commit(*fs);
        msg_of[g] = test::must(store_->read_head()).msg_id;
        // Цепочки: {1,2} {3,4} {5,6} {7}; цепочка уходит при G ≥ g_last + 2.
        const auto expect_gone = [&](uint64_t gen) {
            const uint64_t g_last = gen % 2 == 1 ? gen + 1 : gen;
            return g >= g_last + 2;
        };
        for (uint64_t gen = 1; gen <= g; ++gen) {
            EXPECT_EQ(has_message(msg_of[gen]), !expect_gone(gen)) << "G " << g << ", gen " << gen;
        }
        for (const auto& el : fs->state().list_chain()) {
            EXPECT_TRUE(has_message(el.msg_id));
        }
    }
    const auto history = fs->meta().read([](Db& db) { return list_history(db); });
    ASSERT_FALSE(history.empty());
    EXPECT_EQ(history.front().generation, 5);  // строки удалённых цепочек тоже убраны
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(Commit, MetricsForE1) {
    auto fs = init_and_mount();
    for (int i = 0; i < 20; ++i) {
        mkfile(*fs, "file" + std::to_string(i));
    }
    const auto d = fs->committer().commit();
    ASSERT_TRUE(d.committed);
    EXPECT_EQ(d.metrics.kind, crypto::BlobKind::delta);
    EXPECT_GT(d.metrics.patch_bytes, 0U);
    EXPECT_EQ(d.metrics.take.patch_bytes, d.metrics.patch_bytes);
    EXPECT_GT(d.metrics.raw_bytes, d.metrics.patch_bytes);
    EXPECT_GT(d.metrics.compressed_bytes, 0U);
    EXPECT_EQ(d.metrics.blob_bytes,
              static_cast<size_t>(std::filesystem::file_size(msg_file(d.metrics.msg_id))));
    EXPECT_GT(d.metrics.total.count(), 0);

    const auto s = fs->committer().commit(true);
    EXPECT_EQ(s.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(s.metrics.patch_bytes, 0U);
    EXPECT_GT(s.metrics.raw_bytes, s.metrics.compressed_bytes);
    EXPECT_GT(s.metrics.vacuum.count(), 0);
    const auto stats = fs->committer().stats();
    EXPECT_EQ(stats.commits, 2U);
    EXPECT_EQ(stats.snapshots, 1U);
    EXPECT_EQ(stats.deltas, 1U);
    EXPECT_EQ(test::must(stats.last).generation, 3U);
}

TEST_F(Commit, ReadOnlyMountDoesNotCommit) {
    init();
    auto fs = mount_ro(a_);
    EXPECT_TRUE(fs->read_only());
    fs->committer().commit_now();  // изменений нет: не ошибка
    EXPECT_THROW(fs->committer().commit(true), CommitError);
    fs->unmount();
    EXPECT_EQ(head_header().generation, 1U);
}

// Свойство SPEC 7.4: для каждого из последних retain_generations поколений блоб на месте, а
// индекс, собранный из его цепочки, не ссылается на удалённые сообщения чанков.
TEST_F(Commit, RetentionInvariantRandom) {
    constexpr uint64_t kRetain = 3;
    for (uint32_t seed = 1; seed <= 2; ++seed) {
        for (const uint32_t max_chain : {2U, 64U}) {
            std::mt19937 rng(seed);
            commit_config_.retain_generations = kRetain;
            commit_config_.max_chain = max_chain;
            std::filesystem::remove_all(tmp_ / "a");
            std::filesystem::remove_all(tmp_ / "channel");
            reopen_store();
            init();
            auto fs = mount(a_);
            std::map<uint64_t, int64_t> head_msg;  // поколение -> msg_id блоба
            std::vector<int64_t> files;
            int n = 0;
            const auto record = [&] {
                const auto h = test::must(store_->read_head());
                head_msg[crypto::parse_index_header(h.header).generation] = h.msg_id;
            };
            // Индекс на поколении g, собранный из канала: все его чанки должны быть на месте.
            const auto check_generation = [&](uint64_t g, const std::string& where) {
                const int64_t msg = head_msg.at(g);
                ASSERT_TRUE(has_message(msg))
                    << where << ": blob of retained gen " << g << " deleted";
                const Bytes blob = store_->read_index(msg);
                const auto chain = fetch_chain(*store_, fs->keys().index, fs->fs_uuid(), msg, blob);
                const auto snap = tmp_ / "rs.sqlite";
                const auto out = tmp_ / "ro.sqlite";
                std::filesystem::remove(snap);
                std::filesystem::remove(out);
                write_file_atomic(snap, chain.front().payload);
                std::vector<Bytes> patches;
                for (size_t i = 1; i < chain.size(); ++i) {
                    patches.push_back(chain[i].payload);
                }
                MetaStore::build_from_chain(snap, patches, out);
                Db db = Db::open(out);
                Stmt st{db,
                        "SELECT DISTINCT c.chunk_id, c.msg_id FROM extents e JOIN chunks c "
                        "ON c.chunk_id = e.chunk_id"};
                while (st.step()) {
                    ASSERT_TRUE(has_message(st.column_int64(1)))
                        << where << ": gen " << g << " references deleted chunk msg "
                        << st.column_int64(1);
                }
            };
            record();
            for (int step = 0; step < 40; ++step) {
                const uint32_t op = rng() % 4;
                if (op <= 1 || files.empty()) {
                    const int64_t ino = mkfile(*fs, "f" + std::to_string(n++));
                    put_real_chunk(*fs, ino, 0);
                    files.push_back(ino);
                } else if (op == 2) {
                    const size_t i = rng() % files.size();
                    const int64_t ino = files[i];
                    files.erase(files.begin() + static_cast<std::ptrdiff_t>(i));
                    fs->meta().write([&](Db& db) {
                        delete_inode(db, ino,
                                     static_cast<int64_t>(fs->committer().base_generation()));
                    });
                } else {
                    put_real_chunk(*fs, files[rng() % files.size()], 0);  // освобождает старый чанк
                }
                if (rng() % 3 == 0) {
                    continue;
                }
                fs->committer().commit_now();
                record();
                const uint64_t newest = head_msg.rbegin()->first;
                const std::string where = "seed " + std::to_string(seed) + " max_chain " +
                                          std::to_string(max_chain) + " step " +
                                          std::to_string(step);
                for (uint64_t g = (newest >= kRetain ? newest - kRetain + 1 : 1); g <= newest;
                     ++g) {
                    if (head_msg.contains(g)) {
                        ASSERT_NO_FATAL_FAILURE(check_generation(g, where));
                    }
                }
            }
            fs->unmount();
        }
    }
}

// --- запись pending вне блокировки и промежуточный забор (SPEC 6.6) ---

// Хук, на котором тест держит запись патча в pending: pass() входит и ждёт release().
class Gate {
  public:
    void pass() {
        std::unique_lock lock(mu_);
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }
    bool wait_entered(std::chrono::milliseconds timeout) {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, timeout, [&] { return entered_; });
    }
    void release() {
        {
            const std::lock_guard lock(mu_);
            released_ = true;
        }
        cv_.notify_all();
    }

  private:
    std::mutex mu_;
    std::condition_variable cv_;
    bool entered_ = false;
    bool released_ = false;
};

class IntermediateTake : public Commit {
  protected:
    void break_pending() {
        Db db = Db::open(a_.state_db());
        db.exec(
            "CREATE TRIGGER boom BEFORE INSERT ON pending_patchsets "
            "BEGIN SELECT RAISE(ABORT, 'boom'); END");
    }
    void repair_pending() {
        Db db = Db::open(a_.state_db());
        db.exec("DROP TRIGGER boom");
    }
    static size_t pending_count(MountedFs& fs) { return fs.state().list_pending().size(); }
};

// Пока коммит пишет забранный патч в pending, блокировка MetaStore свободна.
TEST_F(IntermediateTake, CommitWritesPendingOutsideMetaStoreLock) {
    Gate gate;
    commit_config_.before_append_pending = [&] { gate.pass(); };
    auto fs = init_and_mount();
    mkfile(*fs, "one");
    std::thread committer([&] { fs->committer().commit_now(); });
    const bool entered = gate.wait_entered(10s);
    auto writer = std::async(std::launch::async, [&] { return mkfile(*fs, "two"); });
    const bool not_blocked = writer.wait_for(5s) == std::future_status::ready;
    gate.release();
    committer.join();
    ASSERT_TRUE(entered);
    ASSERT_TRUE(not_blocked) << "MetaStore::write waited for the append to pending_patchsets";
    writer.get();
    fs->committer().commit_now();
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(IntermediateTake, IntermediateTakeWritesPendingOutsideMetaStoreLock) {
    Gate gate;
    commit_config_.before_append_pending = [&] { gate.pass(); };
    auto fs = init_and_mount();
    mkfile(*fs, "one");
    std::thread taker([&] { EXPECT_TRUE(fs->committer().take_intermediate().has_value()); });
    const bool entered = gate.wait_entered(10s);
    auto writer = std::async(std::launch::async, [&] { return mkfile(*fs, "two"); });
    const bool not_blocked = writer.wait_for(5s) == std::future_status::ready;
    gate.release();
    taker.join();
    ASSERT_TRUE(entered);
    ASSERT_TRUE(not_blocked);
    writer.get();
    EXPECT_EQ(pending_count(*fs), 1U);
    fs->committer().commit_now();
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Порядок записи в pending = порядок забора. Первый патч (INSERT x) забран промежуточным
// забором и ещё не записан; коммит забирает второй (UPDATE x) и обязан ждать очередь. Если
// бы второй лёг в pending раньше первого, склейка дала бы неверное состояние.
TEST_F(IntermediateTake, ConcurrentTakesKeepTakeOrderInPending) {
    Gate gate;
    commit_config_.before_append_pending = [&] { gate.pass(); };
    auto fs = init_and_mount();
    const int64_t x = mkfile(*fs, "x");
    std::thread taker([&] { EXPECT_TRUE(fs->committer().take_intermediate().has_value()); });
    const bool entered = gate.wait_entered(10s);
    touch(*fs, x, 777);
    std::atomic<bool> commit_done{false};
    CommitResult result;
    std::thread committer([&] {
        result = fs->committer().commit();
        commit_done.store(true);
    });
    std::this_thread::sleep_for(200ms);
    const bool waited = !commit_done.load() && pending_count(*fs) == 0;
    gate.release();
    taker.join();
    committer.join();
    ASSERT_TRUE(entered);
    EXPECT_TRUE(waited) << "the commit overtook the intermediate take";
    ASSERT_TRUE(result.committed);
    EXPECT_EQ(result.metrics.pending_merged, 2U);
    EXPECT_TRUE(fs->state().list_pending().empty());
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Сбой записи патча коммитом: патч забран из сессии и потерян, следующий коммит — снимок.
TEST_F(IntermediateTake, FailedPendingWriteInCommitForcesSnapshot) {
    auto fs = init_and_mount();
    change_and_commit(*fs);
    break_pending();
    mkfile(*fs, "lost-from-session");
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_EQ(pending_count(*fs), 0U);
    EXPECT_EQ(fs->committer().stats().failures, 1U);
    EXPECT_FALSE(fs->read_only());
    repair_pending();

    mkfile(*fs, "next");
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 0);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

TEST_F(IntermediateTake, FailedPendingWriteInIntermediateTakeForcesSnapshot) {
    auto fs = init_and_mount();
    change_and_commit(*fs);
    break_pending();
    mkfile(*fs, "lost-from-session");
    EXPECT_THROW((void)fs->committer().take_intermediate(), CommitError);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_EQ(fs->committer().stats().failures, 1U);
    repair_pending();
    // Пока следующий блоб обязан быть снимком, забирать патчи в pending незачем.
    mkfile(*fs, "more");
    EXPECT_FALSE(fs->committer().take_intermediate().has_value());
    EXPECT_EQ(pending_count(*fs), 0U);

    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Коммит, которому предшествовал только промежуточный забор (сессия пуста, pending не пуст),
// не пустой, а index_dirty после него снова 0.
TEST_F(IntermediateTake, CommitAfterOnlyIntermediateTakeIsNotEmpty) {
    auto fs = init_and_mount();
    mkfile(*fs, "one");
    mkfile(*fs, "two");
    ASSERT_TRUE(fs->committer().take_intermediate().has_value());
    EXPECT_EQ(pending_count(*fs), 1U);
    EXPECT_FALSE(fs->meta().dirty_since_take());
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);  // ещё не закоммичено
    EXPECT_FALSE(fs->committer().take_intermediate().has_value());  // сессия пуста

    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::delta);
    EXPECT_EQ(r.metrics.pending_merged, 1U);
    EXPECT_EQ(r.metrics.generation, 2U);
    EXPECT_TRUE(fs->state().list_pending().empty());
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    EXPECT_FALSE(fs->committer().commit().committed);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Записи до и после промежуточного забора уходят одним коммитом.
TEST_F(IntermediateTake, CommitMergesIntermediatePatchWithSession) {
    auto fs = init_and_mount();
    const int64_t x = mkfile(*fs, "x");
    ASSERT_TRUE(fs->committer().take_intermediate().has_value());
    touch(*fs, x, 5);
    mkfile(*fs, "y");
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.pending_merged, 2U);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    // Промежуточный забор не сдвигает поколение для записей (dead_since_gen).
    EXPECT_EQ(fs->committer().base_generation(), 2U);
}

// Служебные записи не ставят index_dirty, но патч в pending без него коммит бы не запустил.
TEST_F(IntermediateTake, HousekeepingOnlyPatchMakesIndexDirty) {
    auto fs = init_and_mount();
    const int64_t x = mkfile(*fs, "x");
    fs->committer().commit_now();
    ASSERT_EQ(state_int(*fs, state_key::index_dirty), 0);
    fs->meta().write(
        [&](Db& db) {
            AttrUpdate u;
            u.mtime_ns = 9;
            update_attrs(db, x, u);
        },
        WriteKind::housekeeping);
    ASSERT_TRUE(fs->committer().take_intermediate().has_value());
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Промежуточный забор во время заливки блоба: коммит удаляет только свои патчи, чужой остаётся
// и уходит со следующим коммитом.
TEST_F(IntermediateTake, TakeDuringUploadKeepsItsPatchPending) {
    auto fs = init_and_mount();
    mkfile(*fs, "before");
    store_->set_faults({store::FaultRule{.op = StoreOp::put_index,
                                         .nth = store_->call_count(StoreOp::put_index) + 1,
                                         .action = FaultAction::delay,
                                         .delay = 200ms}});
    const uint64_t put_calls = store_->call_count(StoreOp::put_index);
    std::thread committer([&] { fs->committer().commit_now(); });
    while (store_->call_count(StoreOp::put_index) == put_calls) {
        std::this_thread::yield();
    }
    mkfile(*fs, "during");
    const bool taken = fs->committer().take_intermediate().has_value();
    committer.join();
    ASSERT_TRUE(taken);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 2);
    ASSERT_EQ(pending_count(*fs), 1U);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);

    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    // Патч «during» и строка index_history, которую шаг 8 первого коммита записал в сессию.
    EXPECT_EQ(r.metrics.pending_merged, 2U);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::delta);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// То же для снимка: патч, забранный после VACUUM INTO, шаг 8 снимка не удаляет.
TEST_F(IntermediateTake, SnapshotKeepsPatchTakenAfterVacuum) {
    auto fs = init_and_mount();
    mkfile(*fs, "before");
    store_->set_faults({store::FaultRule{.op = StoreOp::put_index,
                                         .nth = store_->call_count(StoreOp::put_index) + 1,
                                         .action = FaultAction::delay,
                                         .delay = 200ms}});
    const uint64_t put_calls = store_->call_count(StoreOp::put_index);
    std::thread committer([&] { (void)fs->committer().commit(true); });
    while (store_->call_count(StoreOp::put_index) == put_calls) {
        std::this_thread::yield();
    }
    mkfile(*fs, "during");  // уже после VACUUM INTO: в снимок не вошло
    const bool taken = fs->committer().take_intermediate().has_value();
    committer.join();
    ASSERT_TRUE(taken);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 2);
    EXPECT_EQ(pending_count(*fs), 1U);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::delta);
    EXPECT_EQ(r.metrics.pending_merged, 2U);  // «during» и index_history шага 8 снимка
    EXPECT_TRUE(fs->state().list_pending().empty());
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Пока есть неподтверждённый pin, промежуточный забор не делается ни вручную, ни фоном.
TEST_F(IntermediateTake, NoTakeWhileUnconfirmed) {
    FakeClock clock;
    commit_config_.clock = &clock;
    commit_config_.commit_interval = 3600s;
    commit_config_.patch_take_rows = 3;
    auto fs = init_and_mount();
    fs->committer().start();
    mkfile(*fs, "x");
    fault_next(StoreOp::set_head, FaultAction::transient);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);  // unconfirmed_, патч в pending
    EXPECT_EQ(pending_count(*fs), 1U);
    EXPECT_FALSE(fs->committer().take_intermediate().has_value());
    // Порог пройден с запасом: фон не должен ничего забрать.
    fs->meta().write([&](Db& db) {
        for (int i = 0; i < 20; ++i) {
            insert_inode(db, test::new_node(kRootIno, "g" + std::to_string(i)));
        }
    });
    std::this_thread::sleep_for(150ms);
    EXPECT_EQ(fs->committer().stats().intermediate_takes, 0U);
    EXPECT_EQ(pending_count(*fs), 1U);
    EXPECT_TRUE(fs->meta().dirty_since_take());

    // Следующий коммит выясняет судьбу pin и забирает всё.
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.pending_merged, 2U);  // патч неподтверждённой попытки и сессия
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    fs->unmount();
}

// Фоновый поток забирает патч сам, когда набралось patch_take_rows строк; коммита нет.
TEST_F(IntermediateTake, BackgroundTakesAtRowThreshold) {
    FakeClock clock;
    commit_config_.clock = &clock;
    commit_config_.commit_interval = 3600s;
    commit_config_.patch_take_rows = 10;
    auto fs = init_and_mount();
    fs->committer().start();
    const auto wait_takes = [&](uint64_t n) {
        for (int i = 0; i < 1000 && fs->committer().stats().intermediate_takes < n; ++i) {
            std::this_thread::sleep_for(10ms);
        }
        return fs->committer().stats().intermediate_takes >= n;
    };
    const auto add_files = [&](int count, const std::string& prefix) {
        fs->meta().write([&](Db& db) {
            for (int i = 0; i < count; ++i) {
                insert_inode(db, test::new_node(kRootIno, prefix + std::to_string(i)));
            }
        });
    };
    add_files(3, "a");  // порог не пройден
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(fs->committer().stats().intermediate_takes, 0U);
    add_files(10, "b");
    ASSERT_TRUE(wait_takes(1));
    EXPECT_EQ(pending_count(*fs), 1U);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);
    add_files(10, "c");  // счётчик начался заново
    ASSERT_TRUE(wait_takes(2));
    EXPECT_EQ(pending_count(*fs), 2U);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 1);  // коммита не было

    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::delta);
    EXPECT_EQ(r.metrics.pending_merged, 2U);  // третий патч — из хвоста «a»
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    fs->unmount();
}

TEST_F(IntermediateTake, ZeroThresholdDisablesBackgroundTakes) {
    FakeClock clock;
    commit_config_.clock = &clock;
    commit_config_.commit_interval = 3600s;
    commit_config_.patch_take_rows = 0;
    auto fs = init_and_mount();
    fs->committer().start();
    fs->meta().write([&](Db& db) {
        for (int i = 0; i < 100; ++i) {
            insert_inode(db, test::new_node(kRootIno, "f" + std::to_string(i)));
        }
    });
    std::this_thread::sleep_for(150ms);
    EXPECT_EQ(fs->committer().stats().intermediate_takes, 0U);
    EXPECT_EQ(pending_count(*fs), 0U);
    fs->unmount();
}

// Замер для E1/SPEC 6.6: время под блокировкой на забор 20 000 новых строк. Без промежуточного
// забора коммит берёт всё разом, с patch_take_rows = 2000 — не больше порога за раз.
TEST_F(IntermediateTake, TakeUnderLockIsShorterWithIntermediateTakes) {
    commit_config_.patch_take_rows = 2000;
    auto fs = init_and_mount();
    constexpr int kFiles = 20'000;
    constexpr int kBatch = 150;
    int next = 0;
    const auto add_batch = [&] {
        fs->meta().write([&](Db& db) {
            for (int i = 0; i < kBatch; ++i) {
                insert_inode(db, test::new_node(kRootIno, "f" + std::to_string(next++)));
            }
        });
    };
    const auto us = [](std::chrono::nanoseconds d) {
        return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
    };

    // Без промежуточного забора.
    int64_t rows_without = 0;
    while (next < kFiles) {
        add_batch();
    }
    rows_without = fs->meta().rows_since_take();
    const auto without = fs->committer().commit();
    ASSERT_TRUE(without.committed);
    const auto locked_without = without.metrics.take.locked;

    // С промежуточным забором (его делает фоновый поток, здесь — тот же вызов по порогу).
    std::chrono::nanoseconds max_intermediate{0};
    int takes = 0;
    const int second_start = next;
    while (next < second_start + kFiles) {
        add_batch();
        if (fs->meta().rows_since_take() >= 2000) {
            const TakeStats stats = test::must(fs->committer().take_intermediate());
            max_intermediate = std::max(max_intermediate, stats.locked);
            ++takes;
        }
    }
    const int64_t rows_tail = fs->meta().rows_since_take();
    const auto with = fs->committer().commit();
    ASSERT_TRUE(with.committed);
    const auto locked_with = std::max(with.metrics.take.locked, max_intermediate);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));

    std::printf(
        "[ E1 ] take under lock, %d new files: without intermediate takes %lld us (%lld rows); "
        "with patch_take_rows = 2000: max %lld us over %d takes + final %lld us (tail %lld rows)\n",
        kFiles, static_cast<long long>(us(locked_without)), static_cast<long long>(rows_without),
        static_cast<long long>(us(locked_with)), takes,
        static_cast<long long>(us(with.metrics.take.locked)), static_cast<long long>(rows_tail));
    RecordProperty("take_locked_without_us", std::to_string(us(locked_without)));
    RecordProperty("take_locked_with_us", std::to_string(us(locked_with)));
    EXPECT_GE(takes, 5);
    EXPECT_LT(locked_with * 2, locked_without);
}

}  // namespace
}  // namespace telega::meta
