// Потеря головы (SPEC 7.2, таблица, две последние строки, и «Особые случаи», --force-local):
// закреплённого нет или закреплено не блоб индекса, а локальный индекс жив. Отказ с подсказкой
// про --ro и --force-local; --ro монтирует локальный индекс; --force-local коммитит его снимком
// G_l + 1 с CAS «голова пуста» (или «именно это чужое сообщение»), prev_msg_id = 0, и шаг 7 чужое
// сообщение не открепляет. Блоб индекса другой ФС — отказ без вариантов. Падение на шагах
// коммита после --force-local.

#include <gtest/gtest.h>

#include <algorithm>
#include <string>

#include "commit_test_util.h"
#include "meta/chain.h"

namespace telega::meta {
namespace {

using store::FaultAction;
using store::StoreOp;
using test::CommitEnv;
using test::normalized;

class HeadLoss : public CommitEnv {
  protected:
    // Подменить список закреплённых сообщений канала (LocalDirStore хранит его в файле pinned).
    void set_pinned(const std::vector<int64_t>& ids) {
        std::string text;
        for (const int64_t id : ids) {
            text += std::to_string(id) + "\n";
        }
        write_file_atomic(channel_dir() / "pinned", as_bytes(text));
    }

    MountOptions with(bool MountOptions::*flag) {
        MountOptions o = mount_options();
        o.*flag = true;
        return o;
    }

    MountError mount_error(const FsPaths& paths, const MountOptions& o) {
        try {
            mount(paths, o);
        } catch (const MountError& e) {
            return e;
        }
        ADD_FAILURE() << "mount did not fail";
        return {MountErrorKind::local_state, "none"};
    }

    MountError mount_error_with(store::Store& s, const FsPaths& paths) {
        try {
            mount_fs(s, paths, mount_options());
        } catch (const MountError& e) {
            return e;
        }
        ADD_FAILURE() << "mount did not fail";
        return {MountErrorKind::local_state, "none"};
    }

    // Чужой документ в канале (не блоб индекса).
    int64_t put_foreign_document() {
        const auto file = tmp_ / "foreign.bin";
        write_file_atomic(file, Bytes(500, std::byte{0x42}));
        return store_->put_chunk(file).msg_id;
    }

    // Три коммита; unmount. Возвращает msg_id голов по поколениям (с 1) и запоминает дамп.
    std::vector<int64_t> make_history() {
        init();
        auto fs = mount(a_);
        std::vector<int64_t> heads{0, test::must(store_->read_head()).msg_id};
        for (int i = 0; i < 3; ++i) {
            const int64_t ino = mkfile(*fs, "f" + std::to_string(i));
            fs->meta().write([&](Db& db) {
                set_inline(db, ino, test::bytes_of("data" + std::to_string(i)), 1);
            });
            fs->committer().commit_now();
            heads.push_back(test::must(store_->read_head()).msg_id);
        }
        dump_ = normalized(test::dump_of(*fs));
        fs->unmount();
        return heads;
    }

    // Отказ без побочных эффектов: ни state.sqlite (флаги), ни канал не изменились.
    void expect_untouched(const std::vector<int64_t>& pins) {
        EXPECT_EQ(store_->pinned(), pins);
        StateStore state{a_.state_db()};
        EXPECT_EQ(state.get_int_or(state_key::force_snapshot, 0), 0);
        EXPECT_EQ(state.get_int_or(state_key::base_generation, 0), 4);
    }

    // Проверка результата --force-local: снимок G_l + 1, prev_msg_id = 0, данные на месте.
    void expect_forced_snapshot(MountedFs& fs, uint64_t g_l) {
        EXPECT_EQ(fs.report().situation, MountSituation::forced_local);
        fs.committer().commit_now();  // force_local ставит index_dirty: снимок уходит сразу
        const auto h = head_header();
        EXPECT_EQ(h.kind, crypto::BlobKind::snapshot);
        EXPECT_EQ(h.generation, g_l + 1);
        EXPECT_EQ(h.prev_msg_id, 0);
        EXPECT_FALSE(fs.committer().read_only());
        EXPECT_EQ(remote_state(), dump_);
    }

    std::string dump_;
};

TEST_F(HeadLoss, UnpinnedHeadIsRefusedThenRoMountsLocalThenForceLocalCommits) {
    const auto heads = make_history();
    set_pinned({});
    ASSERT_FALSE(store_->read_head().has_value());

    const MountError e = mount_error(a_, mount_options());
    EXPECT_EQ(e.kind(), MountErrorKind::head_lost);
    EXPECT_NE(std::string(e.what()).find("--ro"), std::string::npos);
    EXPECT_NE(std::string(e.what()).find("--force-local"), std::string::npos);
    expect_untouched({});

    {
        auto ro = mount(a_, with(&MountOptions::read_only));
        EXPECT_EQ(ro->report().situation, MountSituation::local_read_only);
        EXPECT_EQ(normalized(test::dump_of(*ro)), dump_);
        EXPECT_TRUE(ro->read_only());  // --ro ничего не коммитит
        ro->unmount();
    }
    EXPECT_FALSE(store_->read_head().has_value());
    expect_untouched({});

    auto fs = mount(a_, with(&MountOptions::force_local));
    expect_forced_snapshot(*fs, 4);
    EXPECT_EQ(store_->pinned(), (std::vector<int64_t>{test::must(store_->read_head()).msg_id}));
    EXPECT_EQ(head_header().generation, 5U);
    fs->unmount();
    // Новая голова — обычная: монтируется без флагов, и другая машина её видит.
    EXPECT_EQ(mount(a_)->report().situation, MountSituation::same_generation);
    EXPECT_EQ(mount(b_)->report().situation, MountSituation::built_fresh);
    (void)heads;
}

TEST_F(HeadLoss, ForeignPinnedMessageIsKeptWhenForceLocalCommits) {
    const auto heads = make_history();
    const int64_t foreign = put_foreign_document();
    set_pinned({heads[4], foreign});  // чужое новее всех: оно и есть «голова»
    ASSERT_EQ(test::must(store_->read_head()).msg_id, foreign);

    const MountError e = mount_error(a_, mount_options());
    EXPECT_EQ(e.kind(), MountErrorKind::head_lost);
    EXPECT_NE(std::string(e.what()).find("--force-local"), std::string::npos);
    expect_untouched({heads[4], foreign});

    {
        auto ro = mount(a_, with(&MountOptions::read_only));
        EXPECT_EQ(ro->report().situation, MountSituation::local_read_only);
        EXPECT_EQ(normalized(test::dump_of(*ro)), dump_);
        ro->unmount();
    }
    expect_untouched({heads[4], foreign});

    auto fs = mount(a_, with(&MountOptions::force_local));
    expect_forced_snapshot(*fs, 4);
    const int64_t new_head = test::must(store_->read_head()).msg_id;
    EXPECT_GT(new_head, foreign);
    // Чужой закреп остался: шаг 7 открепляет только свою бывшую голову. Старая голова G4 тоже
    // осталась закреплённой (до неё коммит не дотягивается), главное — новая голова наш снимок.
    const auto pins = store_->pinned();
    EXPECT_NE(std::find(pins.begin(), pins.end(), foreign), pins.end());
    EXPECT_NE(std::find(pins.begin(), pins.end(), new_head), pins.end());
    EXPECT_TRUE(has_message(foreign));
    fs->unmount();
    EXPECT_EQ(mount(a_)->report().situation, MountSituation::same_generation);
}

TEST_F(HeadLoss, OnlyForeignPinWithNoOldPinsAlsoWorks) {
    make_history();
    const int64_t foreign = put_foreign_document();
    set_pinned({foreign});
    EXPECT_EQ(mount_error(a_, mount_options()).kind(), MountErrorKind::head_lost);
    auto fs = mount(a_, with(&MountOptions::force_local));
    expect_forced_snapshot(*fs, 4);
    EXPECT_EQ(store_->pinned().size(), 2U);
    EXPECT_TRUE(has_message(foreign));
}

TEST_F(HeadLoss, DeletedHeadMessageWithNoOldPinsIsALostHead) {
    const auto heads = make_history();
    store_->delete_messages(std::vector<int64_t>{heads[4]});
    ASSERT_FALSE(store_->read_head().has_value());
    EXPECT_EQ(mount_error(a_, mount_options()).kind(), MountErrorKind::head_lost);
    auto fs = mount(a_, with(&MountOptions::force_local));
    expect_forced_snapshot(*fs, 4);
    EXPECT_EQ(store_->pinned().size(), 1U);
}

TEST_F(HeadLoss, DeletedHeadMessageWithAnOldPinLeftIsARollbackNotALostHead) {
    const auto heads = make_history();
    set_pinned({heads[2], heads[4]});  // старый закреп остался
    store_->delete_messages(std::vector<int64_t>{heads[4]});
    ASSERT_EQ(test::must(store_->read_head()).msg_id, heads[2]);
    EXPECT_EQ(mount_error(a_, mount_options()).kind(), MountErrorKind::rollback);
}

TEST_F(HeadLoss, WithoutLocalIndexTheHintIsInit) {
    make_history();
    set_pinned({});
    // Другая машина, где никогда не монтировали.
    {
        const MountError e = mount_error(b_, mount_options());
        EXPECT_EQ(e.kind(), MountErrorKind::no_head);
        EXPECT_NE(std::string(e.what()).find("init"), std::string::npos);
        EXPECT_EQ(std::string(e.what()).find("--force-local"), std::string::npos);
        EXPECT_EQ(mount_error(b_, with(&MountOptions::force_local)).kind(),
                  MountErrorKind::no_head);
        EXPECT_EQ(mount_error(b_, with(&MountOptions::read_only)).kind(), MountErrorKind::no_head);
    }
    // Свой профиль, но index.sqlite потерян: восстанавливать не из чего.
    std::filesystem::remove(a_.index_db());
    EXPECT_EQ(mount_error(a_, with(&MountOptions::force_local)).kind(), MountErrorKind::no_head);
    // То же с закреплённым чужим сообщением.
    set_pinned({put_foreign_document()});
    const MountError e = mount_error(a_, with(&MountOptions::force_local));
    EXPECT_EQ(e.kind(), MountErrorKind::not_index_blob);
    EXPECT_NE(std::string(e.what()).find("init"), std::string::npos);
}

TEST_F(HeadLoss, IndexBlobOfAnotherFileSystemIsRefusedWithoutOptions) {
    make_history();
    // Блоб чужой ФС в нашем канале: другой канал, своя ФС, голова копируется сообщением.
    store::LocalDirStore other{tmp_ / "other-channel"};
    init_fs(other, machine("other"), init_options());
    const Bytes blob = other.read_index(test::must(other.read_head()).msg_id);
    const auto file = tmp_ / "foreign.idx";
    write_file_atomic(file, blob);
    const int64_t foreign = store_->put_index(file);
    set_pinned({foreign});

    for (const auto flag :
         {&MountOptions::read_only, &MountOptions::force_local, &MountOptions::discard_local}) {
        EXPECT_EQ(mount_error(a_, with(flag)).kind(), MountErrorKind::foreign_fs);
    }
    EXPECT_EQ(mount_error(a_, mount_options()).kind(), MountErrorKind::foreign_fs);
    expect_untouched({foreign});
    EXPECT_FALSE(HasFailure());
}

TEST_F(HeadLoss, LocalIndexOfAnotherFileSystemUnderOurKeyringIsRefused) {
    make_history();
    set_pinned({});
    // Чужой индекс под нашим keyring: отказ до любых изменений.
    store::LocalDirStore other{tmp_ / "other-channel"};
    init_fs(other, machine("other"), init_options());
    std::filesystem::copy_file(machine("other").index_db(), a_.index_db(),
                               std::filesystem::copy_options::overwrite_existing);
    std::filesystem::remove(a_.index_db().string() + "-wal");
    std::filesystem::remove(a_.index_db().string() + "-shm");
    EXPECT_EQ(mount_error(a_, with(&MountOptions::force_local)).kind(), MountErrorKind::foreign_fs);
    StateStore state{a_.state_db()};
    EXPECT_EQ(state.get_int_or(state_key::force_snapshot, 0), 0);
}

// Новое окно кэша TDLib (SPEC 7.2, шаг 4): «головы нет» выдерживается, только если у хранилища
// есть кэш; для LocalDirStore (окно 0) повтора нет — это проверяет число чтений головы.
class CountingHeadStore final : public store::Store {
  public:
    explicit CountingHeadStore(store::Store& inner) : inner_(inner) {}
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
        // Первые `hidden_` ответов — «нет головы», как кэш TDLib; потом настоящая голова.
        if (hidden_ > 0) {
            --hidden_;
            return std::nullopt;
        }
        return inner_.read_head();
    }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const std::filesystem::path& f) override { return inner_.put_index(f); }
    void set_head(int64_t n, std::optional<int64_t> o) override { inner_.set_head(n, o); }
    void hide(int n) { hidden_ = n; }

  private:
    store::Store& inner_;
    int hidden_ = 0;
    int reads_ = 0;
};

TEST_F(HeadLoss, TdlibCacheWindowIsKeptBeforeDecidingThatTheHeadIsGone) {
    make_history();
    {
        // Кэш TDLib показывает «нет головы», потом настоящую: монтирование проходит как обычное.
        CountingHeadStore cached{*store_};
        cached.hide(3);
        MountOptions o = mount_options();
        o.stale_head_window = std::chrono::seconds{5};
        o.stale_head_period = std::chrono::milliseconds{1};
        auto fs = mount_fs(cached, a_, o);
        EXPECT_EQ(fs->report().situation, MountSituation::same_generation);
        EXPECT_EQ(cached.head_reads(), 4);
    }
    set_pinned({});
    {
        // Голова действительно пропала: окно выдержано целиком, и только потом отказ.
        CountingHeadStore gone{*store_};
        MountOptions o = mount_options();
        o.stale_head_window = std::chrono::milliseconds{60};
        o.stale_head_period = std::chrono::milliseconds{5};
        const MountError e = [&] {
            const auto began = std::chrono::steady_clock::now();
            try {
                mount_fs(gone, a_, o);
            } catch (const MountError& err) {
                EXPECT_GE(std::chrono::steady_clock::now() - began, std::chrono::milliseconds{60});
                return err;
            }
            ADD_FAILURE() << "mount did not fail";
            return MountError{MountErrorKind::local_state, "none"};
        }();
        EXPECT_EQ(e.kind(), MountErrorKind::head_lost);
        EXPECT_GT(gone.head_reads(), 3);
    }
    {
        // Без окна (LocalDirStore): одно чтение.
        CountingHeadStore plain{*store_};
        EXPECT_EQ(mount_error_with(plain, a_).kind(), MountErrorKind::head_lost);
        EXPECT_EQ(plain.head_reads(), 1);
    }
}

// --- сбои на шагах коммита после --force-local ---

enum class LostKind : uint8_t { empty, foreign };

struct CrashCase {
    const char* name;
    StoreOp op;
    FaultAction action;
    bool pinned;  // pin дошёл: после «перезапуска» голова — наш снимок
};

class ForceLocalCrash : public HeadLoss,
                        public ::testing::WithParamInterface<std::tuple<CrashCase, LostKind>> {};

TEST_P(ForceLocalCrash, RemountIsConsistent) {
    const auto [c, kind] = GetParam();
    make_history();
    std::optional<int64_t> foreign;
    if (kind == LostKind::foreign) {
        foreign = put_foreign_document();
        set_pinned({*foreign});
    } else {
        set_pinned({});
    }
    {
        auto fs = mount(a_, with(&MountOptions::force_local));
        ASSERT_EQ(fs->report().situation, MountSituation::forced_local);
        fault_next(c.op, c.action);
        EXPECT_THROW(fs->committer().commit_now(), store::CrashInjected);
        // «Процесс упал»: без unmount, без финального коммита.
    }
    reopen_store();

    if (!c.pinned) {
        // Голова осталась потерянной: перемонтирование консистентно, ничего не испортилось, а
        // повторный --force-local доводит дело до конца.
        const auto pins_before = store_->pinned();
        EXPECT_EQ(mount_error(a_, mount_options()).kind(), MountErrorKind::head_lost);
        EXPECT_EQ(store_->pinned(), pins_before);
        {
            auto ro = mount(a_, with(&MountOptions::read_only));
            EXPECT_EQ(normalized(test::dump_of(*ro)), dump_);
        }
        auto fs = mount(a_, with(&MountOptions::force_local));
        expect_forced_snapshot(*fs, 4);
        if (foreign) {
            EXPECT_TRUE(has_message(*foreign));
        }
        return;
    }
    // pin дошёл, шаг 8 нет: головой стал наш снимок, он узнаётся как свой, шаг 8 доделывается.
    auto fs = mount(a_);
    EXPECT_TRUE(fs->report().resumed_commit);
    EXPECT_EQ(fs->report().situation, MountSituation::same_generation);
    EXPECT_EQ(fs->report().local_generation, 4U);  // до доделывания шага 8
    EXPECT_EQ(fs->report().remote_generation, 5U);
    EXPECT_EQ(normalized(test::dump_of(*fs)), dump_);
    EXPECT_EQ(remote_state(), dump_);
    EXPECT_EQ(head_header().generation, 5U);
    if (foreign) {
        EXPECT_TRUE(has_message(*foreign));
    }
    // Дальше ФС живёт как обычно.
    mkfile(*fs, "after");
    fs->committer().commit_now();
    EXPECT_FALSE(fs->committer().read_only());
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

INSTANTIATE_TEST_SUITE_P(
    Steps, ForceLocalCrash,
    ::testing::Combine(
        ::testing::Values(
            CrashCase{"put_index_before", StoreOp::put_index, FaultAction::crash_before, false},
            CrashCase{"put_index_after", StoreOp::put_index, FaultAction::crash_after, false},
            CrashCase{"set_head_before", StoreOp::set_head, FaultAction::crash_before, false},
            CrashCase{"set_head_after", StoreOp::set_head, FaultAction::crash_after, true}),
        ::testing::Values(LostKind::empty, LostKind::foreign)),
    [](const auto& param_info) {
        return std::string(std::get<0>(param_info.param).name) +
               (std::get<1>(param_info.param) == LostKind::empty ? "_empty" : "_foreign");
    });

// Неподтверждённый pin (SPEC 7.1, конец): set_head упал транзиентно, pin не прошёл. Следующий
// коммит видит прежнюю голову (пусто или чужое), удаляет залитый блоб и повторяет с тем же CAS.
class UnconfirmedForcePin : public HeadLoss, public ::testing::WithParamInterface<LostKind> {};

TEST_P(UnconfirmedForcePin, PinThatDidNotPassIsRetriedAgainstTheSameHead) {
    make_history();
    std::optional<int64_t> foreign;
    if (GetParam() == LostKind::foreign) {
        foreign = put_foreign_document();
        set_pinned({*foreign});
    } else {
        set_pinned({});
    }
    auto fs = mount(a_, with(&MountOptions::force_local));
    fault_next(StoreOp::set_head, FaultAction::transient);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    const auto blobs_after_failure = store_->list_messages();
    // Голова не изменилась, блоб залит без pin.
    if (foreign) {
        EXPECT_EQ(test::must(store_->read_head()).msg_id, *foreign);
    } else {
        EXPECT_FALSE(store_->read_head().has_value());
    }
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 4);

    fs->committer().commit_now();  // окно кэша 0: «пусто» или «чужое» — pin не прошёл, блоб удалён
    EXPECT_FALSE(fs->committer().read_only());
    const auto h = head_header();
    EXPECT_EQ(h.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(h.generation, 5U);
    EXPECT_EQ(h.prev_msg_id, 0);
    EXPECT_EQ(remote_state(), dump_);
    // Первый залитый блоб — сирота этой попытки — удалён.
    const int64_t first_blob = blobs_after_failure.back();
    EXPECT_FALSE(has_message(first_blob));
    if (foreign) {
        EXPECT_TRUE(has_message(*foreign));
        const auto pins = store_->pinned();
        EXPECT_NE(std::find(pins.begin(), pins.end(), *foreign), pins.end());
    }
    mkfile(*fs, "later");
    EXPECT_NO_THROW(fs->committer().commit_now());
    EXPECT_FALSE(fs->committer().read_only());
}

INSTANTIATE_TEST_SUITE_P(Heads, UnconfirmedForcePin,
                         ::testing::Values(LostKind::empty, LostKind::foreign),
                         [](const auto& param_info) {
                             return param_info.param == LostKind::empty ? "empty" : "foreign";
                         });

}  // namespace
}  // namespace telega::meta
