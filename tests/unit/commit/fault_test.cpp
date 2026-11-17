// Инъекция сбоев (SPEC 7.3, 14): падение до и после каждого шага коммита, который трогает
// Store, затем «перезапуск» — монтирование на том же локальном каталоге и на пустом.
// Плюс property-тест: случайные операции и коммиты со сбоями, канал всегда равен состоянию
// последнего успешного коммита.

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <random>
#include <thread>
#include <tuple>

#include "commit_test_util.h"

namespace telega::meta {
namespace {

using namespace std::chrono_literals;
using store::FaultAction;
using store::StoreOp;
using test::CommitEnv;
using test::normalized;

const char* op_name(StoreOp op) {
    switch (op) {
        case StoreOp::read_head:
            return "read_head";
        case StoreOp::put_index:
            return "put_index";
        case StoreOp::set_head:
            return "set_head";
        default:
            return "other";
    }
}

// (операция, crash_after, снимок)
class CrashAtStep : public CommitEnv,
                    public ::testing::WithParamInterface<std::tuple<StoreOp, bool, bool>> {};

TEST_P(CrashAtStep, RestartSeesOldOrNewCommitNeverAMix) {
    const auto [op, after, snapshot] = GetParam();
    init();
    auto fs = mount(a_);
    const int64_t dir = mkdir(*fs, "dir");
    mkfile(*fs, "old", dir);
    fs->committer().commit_now();  // поколение 2
    const std::string old_state = normalized(test::dump_of(*fs));

    // Изменения коммита, который упадёт: создание, переименование, удаление.
    mkfile(*fs, "new", dir);
    fs->meta().write([&](Db& db) {
        const int64_t new_parent = kRootIno;
        move_inode(db, dir, new_parent, test::bytes_of("renamed"));
        delete_inode(db, test::must(lookup(db, dir, test::bytes_of("old"))).ino, 2);
    });
    const std::string new_state = normalized(test::dump_of(*fs));
    ASSERT_NE(old_state, new_state);

    fault_next(op, after ? FaultAction::crash_after : FaultAction::crash_before);
    EXPECT_THROW(fs->committer().commit(snapshot), store::CrashInjected);
    fs.reset();  // процесс умер: без unmount, без финального коммита
    reopen_store();

    // Точка коммита одна — pin (set_head): до неё виден старый индекс, после — новый.
    const bool committed = op == StoreOp::set_head && after;
    EXPECT_EQ(head_header().generation, committed ? 3U : 2U);
    EXPECT_EQ(store_->pinned().size(), 1U);
    EXPECT_EQ(remote_state(), committed ? new_state : old_state);

    // Тот же локальный каталог: ничего не потеряно, сбой разобран.
    fs = mount(a_);
    EXPECT_TRUE(fs->report().unclean_shutdown);
    EXPECT_EQ(fs->report().resumed_commit, committed);
    EXPECT_EQ(fs->report().situation, MountSituation::same_generation);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), committed ? 3 : 2);
    EXPECT_EQ(normalized(test::dump_of(*fs)), new_state);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_FALSE(std::filesystem::exists(snapshot_tmp_path(a_.tmp(), 3)));

    // Следующий коммит — снимок (сессия потеряна), после него всё сходится.
    mkfile(*fs, "after-restart");
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(r.metrics.generation, committed ? 4U : 3U);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    EXPECT_TRUE(fs->state().list_pending().empty());
    const std::string final_state = test::dump_of(*fs);
    fs->unmount();
    fs.reset();

    // Пустой локальный каталог: тот же индекс, включая index_history.
    auto other = mount(b_);
    EXPECT_EQ(test::dump_of(*other), final_state);
}

INSTANTIATE_TEST_SUITE_P(
    Steps, CrashAtStep,
    ::testing::Combine(::testing::Values(StoreOp::read_head, StoreOp::put_index, StoreOp::set_head),
                       ::testing::Bool(), ::testing::Bool()),
    [](const auto& param_info) {
        const auto& p = param_info.param;
        return std::string(op_name(std::get<0>(p))) + (std::get<1>(p) ? "_after" : "_before") +
               (std::get<2>(p) ? "_snapshot" : "_delta");
    });

// Сбой между шагом 6 и 8 разбирается и на пустом каталоге, и при удачном следующем коммите
// с того же каталога без перемонтирования другой машины.
TEST_F(CommitEnv, PinnedButStateNotWrittenIsResumedAtMount) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "one");
    fs->committer().commit_now();
    mkfile(*fs, "two");
    fault_next(StoreOp::set_head, FaultAction::crash_after);
    EXPECT_THROW(fs->committer().commit_now(), store::CrashInjected);
    fs.reset();
    reopen_store();
    {
        StateStore state{a_.state_db()};
        EXPECT_EQ(state.get_int(state_key::base_generation), 2);  // G_r = G_l + 1
        EXPECT_EQ(state.list_pending().size(), 1U);
    }
    fs = mount(a_);
    EXPECT_TRUE(fs->report().resumed_commit);
    EXPECT_EQ(fs->report().local_generation, 2U);
    EXPECT_EQ(fs->report().remote_generation, 3U);
    EXPECT_TRUE(fs->state().list_pending().empty());
    EXPECT_EQ(fs->state().list_chain().size(), 3U);
    const auto history = fs->meta().read([](Db& db) { return list_history(db); });
    EXPECT_EQ(history.back().generation, 3);
}

// Чужой блоб того же поколения поверх G_l не принимается за свой: конфликт, а не порча.
TEST_F(CommitEnv, ForeignCommitOfNextGenerationIsNotResumed) {
    init();
    {
        auto fa = mount(a_);
        mkfile(*fa, "from-a");
        // Процесс A падает после того, как забрал патч, но до CAS.
        fault_next(StoreOp::read_head, FaultAction::crash_before);
        EXPECT_THROW(fa->committer().commit_now(), store::CrashInjected);
    }
    reopen_store();
    {
        auto fb = mount(b_);
        mkfile(*fb, "from-b");
        fb->committer().commit_now();  // поколение 2, prev_msg_id = голова A
        fb->unmount();
    }
    try {
        mount(a_);
        ADD_FAILURE() << "mount succeeded";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::local_conflict);
    }
}

// --- property-тест ---

// Случайные операции над индексом (подмножество из теста дельта-журнала). Чанки фиктивные,
// с msg_id далеко за пределами настоящих сообщений канала.
class RandomOps {
  public:
    explicit RandomOps(uint64_t seed) : rng_(seed) {}

    [[nodiscard]] int rolled_back() const { return rolled_back_; }

    int pick(int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(rng_); }
    bool chance(int percent) { return pick(0, 99) < percent; }

    void step(MountedFs& fs) {
        const auto g = static_cast<int64_t>(fs.committer().base_generation());
        try {
            fs.meta().write([&](Db& db) {
                const int n = pick(1, 3);
                for (int i = 0; i < n; ++i) {
                    one_op(db, g);
                }
            });
        } catch (const FsError&) {
            ++rolled_back_;  // EEXIST и подобные откатывают транзакцию целиком
        }
    }

  private:
    std::string name() {
        return std::string(1, static_cast<char>('a' + pick(0, 7))) + std::to_string(pick(0, 2));
    }

    int64_t random_ino(Db& db, const std::string& where) {
        Stmt count{db, "SELECT COUNT(*) FROM inodes WHERE " + where};
        count.step();
        const int64_t n = count.column_int64(0);
        if (n == 0) {
            return 0;
        }
        Stmt st{db, "SELECT ino FROM inodes WHERE " + where + " ORDER BY ino LIMIT 1 OFFSET ?"};
        st.bind(1, static_cast<int64_t>(pick(0, static_cast<int>(n - 1))));
        st.step();
        return st.column_int64(0);
    }

    static bool is_ancestor(Db& db, int64_t ancestor, int64_t ino) {
        for (std::optional<int64_t> cur = ino; cur;) {
            if (*cur == ancestor) {
                return true;
            }
            cur = test::must(get_inode(db, *cur)).parent;
        }
        return false;
    }

    void one_op(Db& db, int64_t g) {
        switch (pick(0, 7)) {
            case 0:
            case 1: {
                const int64_t parent = random_ino(db, "kind = 2");
                insert_inode(db,
                             test::new_node(parent, name(),
                                            pick(0, 3) == 0 ? InodeKind::dir : InodeKind::file));
                break;
            }
            case 2: {
                const int64_t ino = random_ino(db, "ino <> 1 AND parent IS NOT NULL");
                const int64_t to = random_ino(db, "kind = 2");
                if (ino != 0 && !is_ancestor(db, ino, to)) {
                    move_inode(db, ino, to, test::bytes_of(name()));
                }
                break;
            }
            case 3: {
                const int64_t ino = random_ino(db, "kind = 1 AND parent IS NOT NULL");
                if (ino != 0) {
                    if (pick(0, 1) == 0) {
                        delete_inode(db, ino, g);
                    } else {
                        detach_inode(db, ino);
                    }
                }
                break;
            }
            case 4:
            case 5: {
                const int64_t ino = random_ino(db, "kind = 1");
                if (ino == 0) {
                    break;
                }
                if (test::must(get_inode(db, ino)).inline_len) {
                    clear_inline(db, ino);
                }
                const int64_t id = alloc_chunk_id(db);
                NewChunk c;
                c.chunk_id = id;
                c.msg_id = 1'000'000'000 + id;
                c.plain_size = pick(1, 5000);
                c.enc_size = c.plain_size + 40;
                c.content_mac = test::mac_of(id);
                insert_chunk(db, c, g);
                put_extent(
                    db, Extent{ino, pick(0, 3), id, 0, pick(1, static_cast<int>(c.plain_size))}, g);
                break;
            }
            case 6: {
                const int64_t ino = random_ino(db, "kind = 1");
                if (ino != 0) {
                    Bytes data(static_cast<size_t>(pick(0, 60)),
                               std::byte{static_cast<unsigned char>(pick(0, 255))});
                    set_inline(db, ino, data, g);
                }
                break;
            }
            default: {
                AttrUpdate u;
                u.mtime_ns = pick(1, 1'000'000);
                u.mode = static_cast<uint32_t>(pick(0, 0777));
                update_attrs(db, random_ino(db, "1"), u);
                break;
            }
        }
    }

    std::mt19937_64 rng_;
    int rolled_back_ = 0;
};

class CommitProperty : public CommitEnv, public ::testing::WithParamInterface<uint64_t> {};

TEST_P(CommitProperty, ChannelEqualsLastSuccessfulCommit) {
    const uint64_t seed = GetParam();
    commit_config_.max_chain = 4;
    commit_config_.retain_generations = 3;
    RandomOps ops{seed};
    init();
    auto fs = mount(a_);
    std::string committed = normalized(test::dump_of(*fs));
    uint64_t generation = 1;
    int crashes = 0;
    int failures = 0;
    int successes = 0;
    int verified = 0;

    auto verify = [&] {
        ++verified;
        ASSERT_EQ(remote_state(), committed) << "seed " << seed << ", generation " << generation;
    };

    constexpr int kOps = 200;
    for (int i = 0; i < kOps; ++i) {
        ops.step(*fs);
        if (!ops.chance(20)) {
            continue;
        }
        const std::string candidate = normalized(test::dump_of(*fs));
        const int f = ops.pick(0, 99);
        const StoreOp op = std::array{StoreOp::read_head, StoreOp::put_index,
                                      StoreOp::set_head}[static_cast<size_t>(ops.pick(0, 2))];
        if (f < 15) {
            fault_next(op, FaultAction::transient);
        } else if (f < 30) {
            fault_next(op, ops.chance(50) ? FaultAction::crash_before : FaultAction::crash_after);
        }
        try {
            fs->committer().commit(ops.chance(10));
            committed = candidate;
            generation = static_cast<uint64_t>(state_int(*fs, state_key::base_generation));
            ++successes;
        } catch (const CommitError&) {
            ++failures;
            ASSERT_FALSE(fs->read_only()) << "seed " << seed;
        } catch (const store::CrashInjected&) {
            ++crashes;
            fs.reset();
            reopen_store();
            const uint64_t head_gen = head_header().generation;
            ASSERT_TRUE(head_gen == generation || head_gen == generation + 1) << "seed " << seed;
            if (head_gen == generation + 1) {
                committed = candidate;
                generation = head_gen;
            }
            ASSERT_NO_FATAL_FAILURE(verify());
            fs = mount(a_);
            ASSERT_EQ(normalized(test::dump_of(*fs)).empty(), false);
        }
        store_->set_faults({});
        if (ops.chance(20)) {
            ASSERT_NO_FATAL_FAILURE(verify());
        }
    }
    fs->committer().commit_now();
    committed = normalized(test::dump_of(*fs));
    ASSERT_NO_FATAL_FAILURE(verify());
    fs->unmount();

    EXPECT_GT(successes, 5);
    RecordProperty("crashes", crashes);
    RecordProperty("rolled_back", ops.rolled_back());
    RecordProperty("failures", failures);
    RecordProperty("verified", verified);
}

INSTANTIATE_TEST_SUITE_P(Seeds, CommitProperty, ::testing::Values(1, 2, 3, 4, 5, 6));

// --- неподтверждённый pin и размонтирование (SPEC 7.1, 7.2) ---

class Unconfirmed : public CommitEnv {
  protected:
    // Сбой шага 8: триггер во втором соединении к state.sqlite не даёт записать chain.
    // Pin к этому моменту уже сделан, коммит остаётся неподтверждённым.
    void break_step8() {
        Db db = Db::open(a_.state_db());
        db.exec(
            "CREATE TRIGGER boom BEFORE INSERT ON chain BEGIN SELECT RAISE(ABORT, 'boom'); END");
    }
    void repair_step8() {
        Db db = Db::open(a_.state_db());
        db.exec("DROP TRIGGER boom");
    }
    static bool has_name(MountedFs& fs, std::string_view name) {
        return fs.meta().read(
            [&](Db& db) { return lookup(db, kRootIno, test::bytes_of(name)).has_value(); });
    }
};

// Баг 1: записи после неподтверждённого pin живут только в сессии. Чистое размонтирование без
// сети теряет сессию, поэтому следующий коммит обязан быть снимком, иначе дельта не будет
// содержать этих записей и канал разойдётся с локальным индексом.
TEST_F(Unconfirmed, UnmountWithoutNetworkAfterUnconfirmedDeltaForcesSnapshot) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "x");
    fault_next(StoreOp::set_head, FaultAction::transient);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);  // патч в pending, unconfirmed_
    const int64_t y = mkfile(*fs, "y");                       // только в сессии
    fault_next(StoreOp::read_head, FaultAction::transient);  // повтор упирается в read_head
    EXPECT_THROW(fs->unmount(), CommitError);
    EXPECT_EQ(state_int(*fs, state_key::clean_shutdown), 1);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    fs.reset();

    fs = mount(a_);
    EXPECT_FALSE(fs->report().unclean_shutdown);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    touch(*fs, y, 12345);
    fs->committer().commit_now();  // дельта с UPDATE строки, которую канал уже знает
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    EXPECT_TRUE(has_name(*fs, "y"));
}

// Баг 1(б): блоб головы — наш снимок (pin прошёл, шаг 8 упал). Монтирование доделывает шаг 8,
// но не имеет права сбрасывать force_snapshot: записи после VACUUM INTO ушли с сессией.
TEST_F(Unconfirmed, UnmountAfterPinnedSnapshotKeepsForceSnapshotAcrossReplay) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "x");
    break_step8();
    EXPECT_THROW(fs->committer().commit(true), CommitError);  // снимок закреплён, state нет
    mkfile(*fs, "y");  // после VACUUM INTO: только в сессии
    fault_next(StoreOp::read_head, FaultAction::transient);
    EXPECT_THROW(fs->unmount(), CommitError);
    EXPECT_EQ(state_int(*fs, state_key::clean_shutdown), 1);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    fs.reset();
    repair_step8();

    fs = mount(a_);
    EXPECT_TRUE(fs->report().resumed_commit);
    EXPECT_FALSE(fs->report().unclean_shutdown);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    EXPECT_TRUE(has_name(*fs, "y"));
}

// SPEC 7.2: неудачный финальный коммит (нет сети) — это ошибка размонтирования, но разбор
// доводится до конца: force_snapshot = 1 строго до clean_shutdown = 1, фоновый поток остановлен.
TEST_F(Unconfirmed, UnmountReportsFailedFinalCommitAfterTeardown) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "x");
    store_->set_faults({store::FaultRule{
        .op = StoreOp::put_index, .nth = std::nullopt, .action = FaultAction::permanent}});
    EXPECT_THROW(fs->unmount(), CommitError);
    EXPECT_EQ(state_int(*fs, state_key::clean_shutdown), 1);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_NO_THROW(fs->unmount());  // повторный вызов ничего не делает
    store_->set_faults({});
    fs.reset();

    fs = mount(a_);
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// N4: повтор неподтверждённого коммита упал не StoreError, а ошибкой state.sqlite (шаг 8: диск
// заполнен). Сбой обязан попасть в record_failure и сдвинуть retry_at_: иначе фоновый цикл
// крутился бы без задержки.
class UnconfirmedBackground : public Unconfirmed {
  protected:
    void SetUp() override {
        Unconfirmed::SetUp();
        commit_config_.clock = &clock_;
        commit_config_.commit_interval = 60s;
        commit_config_.retry_min = 1s;
        commit_config_.retry_max = 8s;
    }
    test::FakeClock clock_;
};

TEST_F(UnconfirmedBackground, NonStoreFailureOfUnconfirmedRetryIsBackedOff) {
    init();
    auto fs = mount(a_);
    fs->committer().start();
    mkfile(*fs, "x");
    break_step8();
    EXPECT_THROW(fs->committer().commit_now(), CommitError);  // pin есть, state нет
    const auto failures = [&] { return fs->committer().stats().failures; };
    const auto wait_failures = [&](uint64_t n) {
        for (int i = 0; i < 5000 && failures() < n; ++i) {
            std::this_thread::sleep_for(1ms);
        }
        return failures();
    };
    ASSERT_EQ(failures(), 1U);

    // Первый повтор (через 1 с): разбор неподтверждённого коммита падает на state.sqlite.
    clock_.advance(1s);
    ASSERT_EQ(wait_failures(2), 2U) << "the failure of the retry was not recorded";
    // Следующая задержка — 2 с: пока она не прошла, попыток нет (ни обращений к голове канала).
    const uint64_t heads = store_->call_count(StoreOp::read_head);
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(store_->call_count(StoreOp::read_head), heads) << "the retry loop spins";
    EXPECT_EQ(failures(), 2U);
    clock_.advance(1999ms);
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(failures(), 2U);
    clock_.advance(1ms);
    ASSERT_EQ(wait_failures(3), 3U);

    repair_step8();
    clock_.advance(4s);  // третья задержка
    EXPECT_TRUE(fs->committer().wait_for_generation(2, 10s));
    fs->unmount();
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Успешное размонтирование force_snapshot не ставит.
TEST_F(Unconfirmed, SuccessfulUnmountDoesNotForceSnapshot) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "x");
    EXPECT_NO_THROW(fs->unmount());
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 0);
    EXPECT_EQ(state_int(*fs, state_key::clean_shutdown), 1);
}

// Баг 2: --force-local, шаг 8 упал, следующий коммит доделал неподтверждённый. CAS-подмена
// должна сброситься, иначе следующий коммит сверяется со старой головой и ложно уходит в
// read-only.
TEST_F(Unconfirmed, ForceLocalCasOverrideIsResetWhenUnconfirmedCommitIsFinished) {
    init();
    int64_t m2 = 0;
    {
        auto fa = mount(a_);
        mkfile(*fa, "f1");
        fa->committer().commit_now();
        m2 = test::must(store_->read_head()).msg_id;
        mkfile(*fa, "f2");
        fa->committer().commit_now();
        fa->unmount();
    }
    // Откат головы на поколение 2.
    write_file_atomic(channel_dir() / "pinned", test::bytes_of(std::to_string(m2) + "\n"));
    MountOptions o = mount_options();
    o.force_local = true;
    auto fa = mount(a_, o);
    ASSERT_EQ(fa->report().situation, MountSituation::forced_local);

    break_step8();
    mkfile(*fa, "f3");
    EXPECT_THROW(fa->committer().commit_now(), CommitError);  // pin есть, state нет
    repair_step8();
    fa->committer().commit_now();  // доделывает неподтверждённый коммит
    EXPECT_FALSE(fa->committer().read_only());
    mkfile(*fa, "f4");
    EXPECT_NO_THROW(fa->committer().commit_now());
    EXPECT_FALSE(fa->committer().read_only()) << "CAS override still points at the old head";
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fa)));
}

// --- промежуточный забор патча (SPEC 6.6, 7.2) ---

// Блоб головы — наш, шаг 8 упал, потом процесс умер. Пока pin неподтверждён, промежуточный забор
// ничего не дописывает в pending, поэтому патч блоба равен склейке pending, и монтирование
// узнаёт свой блоб (G_r = G_l + 1) и доделывает шаг 8.
TEST_F(Unconfirmed, OwnBlobIsRecognizedAtMountAfterRowsPassedTheTakeThreshold) {
    commit_config_.patch_take_rows = 3;
    init();
    auto fs = mount(a_);
    mkfile(*fs, "x");
    break_step8();
    EXPECT_THROW(fs->committer().commit_now(), CommitError);  // pin прошёл, state нет
    for (int i = 0; i < 10; ++i) {
        mkfile(*fs, "later" + std::to_string(i));
    }
    EXPECT_FALSE(fs->committer().take_intermediate().has_value());
    EXPECT_EQ(fs->state().list_pending().size(), 1U);
    fs.reset();  // процесс умер: clean_shutdown = 0
    repair_step8();

    fs = mount(a_);
    EXPECT_TRUE(fs->report().resumed_commit);
    EXPECT_TRUE(fs->report().unclean_shutdown);
    EXPECT_EQ(fs->report().local_generation, 1U);
    EXPECT_EQ(fs->report().remote_generation, 2U);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Промежуточный забор успел дописать патч, пока блоб заливался, и процесс упал после pin: в pending
// блоб головы — это префикс. Монтирование всё равно узнаёт его как свой.
TEST_F(CommitEnv, OwnBlobIsRecognizedWhenIntermediateTakeAppendedDuringUpload) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "one");
    const uint64_t put_calls = store_->call_count(StoreOp::put_index);
    const uint64_t head_calls = store_->call_count(StoreOp::set_head);
    store_->set_faults(
        {store::FaultRule{.op = StoreOp::put_index,
                          .nth = put_calls + 1,
                          .action = FaultAction::delay,
                          .delay = 200ms},
         store::FaultRule{
             .op = StoreOp::set_head, .nth = head_calls + 1, .action = FaultAction::crash_after}});
    std::atomic<bool> crashed{false};
    std::thread committer([&] {
        try {
            fs->committer().commit_now();
        } catch (const store::CrashInjected&) {
            crashed.store(true);
        }
    });
    while (store_->call_count(StoreOp::put_index) == put_calls) {
        std::this_thread::yield();
    }
    mkfile(*fs, "during");
    const bool taken = fs->committer().take_intermediate().has_value();
    committer.join();
    ASSERT_TRUE(crashed.load());
    ASSERT_TRUE(taken);
    fs.reset();
    reopen_store();
    {
        StateStore state{a_.state_db()};
        EXPECT_EQ(state.get_int(state_key::base_generation), 1);  // G_r = G_l + 1
        EXPECT_EQ(state.list_pending().size(), 2U);
    }
    fs = mount(a_);
    EXPECT_TRUE(fs->report().resumed_commit);
    EXPECT_EQ(fs->report().local_generation, 1U);
    EXPECT_EQ(fs->report().remote_generation, 2U);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    // «during» есть в локальном индексе, но не в блобе головы: следующий коммит — снимок.
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// Случайные операции параллельно с промежуточными заборами, коммитами и фоновым потоком с
// крошечным patch_take_rows: итоговая цепочка в канале равна живому индексу (порядок патчей
// в pending совпадает с порядком забора, коммит удаляет только свои).
class CommitPropertyConcurrent : public CommitEnv,
                                 public ::testing::WithParamInterface<uint64_t> {};

TEST_P(CommitPropertyConcurrent, ChainEqualsLiveIndexWithConcurrentTakes) {
    const uint64_t seed = GetParam();
    commit_config_.max_chain = 4;
    commit_config_.retain_generations = 3;
    commit_config_.patch_take_rows = 3;
    commit_config_.commit_interval = 5ms;
    commit_config_.retry_min = 1ms;
    commit_config_.retry_max = 4ms;
    MountOptions o = mount_options();
    o.start_background = true;
    init();
    auto fs = mount(a_, o);
    RandomOps ops{seed};

    std::atomic<bool> done{false};
    std::atomic<int> errors{0};
    std::atomic<int> takes{0};
    std::atomic<int> commits{0};
    std::thread taker([&] {
        while (!done.load()) {
            try {
                if (fs->committer().take_intermediate()) {
                    ++takes;
                }
            } catch (const CommitError&) {
                ++errors;
            }
            std::this_thread::sleep_for(150us);
        }
    });
    std::thread committer([&] {
        std::mt19937_64 rng{seed * 7919};
        while (!done.load()) {
            try {
                if (fs->committer().commit(rng() % 8 == 0).committed) {
                    ++commits;
                }
            } catch (const CommitError&) {
                ++errors;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(500 + rng() % 2000));
        }
    });
    for (int i = 0; i < 400; ++i) {
        ops.step(*fs);
        if (ops.chance(30)) {
            std::this_thread::sleep_for(100us);
        }
    }
    done.store(true);
    taker.join();
    committer.join();
    EXPECT_EQ(errors.load(), 0) << "seed " << seed;

    // Фон остановлен: иначе он забирал бы в pending служебные записи самого коммита и уборки.
    fs->committer().stop();
    fs->committer().commit_now();
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0) << "seed " << seed;
    EXPECT_TRUE(fs->state().list_pending().empty()) << "seed " << seed;
    ASSERT_EQ(remote_state(), normalized(test::dump_of(*fs))) << "seed " << seed;
    fs->unmount();
    RecordProperty("takes", takes.load());
    RecordProperty("commits", commits.load());
    EXPECT_GT(takes.load() + static_cast<int>(fs->committer().stats().intermediate_takes), 0);
}

INSTANTIATE_TEST_SUITE_P(Seeds, CommitPropertyConcurrent,
                         ::testing::Values(11, 12, 13, 14, 15, 16));

}  // namespace
}  // namespace telega::meta
