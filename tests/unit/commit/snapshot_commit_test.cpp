// Снимок в коммите без долгой блокировки записи: блокировка
// MetaStore не держится на время копирования, граница pending_patchsets по max_pending_seq,
// сбои на каждом шаге снимка, узнавание своего снимка при монтировании.
// Синхронизация идёт через хук этапов копирования (CommitConfig::snapshot_hook).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <thread>

#include "commit_test_util.h"

namespace telega::meta {
namespace {

using namespace std::chrono_literals;
using store::FaultAction;
using store::StoreOp;
using test::CommitEnv;
using test::normalized;

class SnapshotCommit : public CommitEnv {
  protected:
    void SetUp() override {
        CommitEnv::SetUp();
        commit_config_.snapshot_hook = [this](SnapshotStage stage, Db& db) {
            if (hook_) {
                hook_(stage, db);
            }
        };
    }

    // Записать из другого потока и дождаться: если бы блокировка MetaStore ещё держалась, запись
    // не прошла бы никогда (мьютекс рекурсивный, поэтому из самого потока коммита это не
    // доказательство). Предел ожидания нужен только затем, чтобы сломанная реализация роняла
    // тест, а не вешала его.
    static bool write_from_other_thread(MountedFs& fs, const std::string& name) {
        std::promise<void> done;
        auto future = done.get_future();
        std::thread t([&] {
            mkfile(fs, name);
            done.set_value();
        });
        const bool ok = future.wait_for(60s) == std::future_status::ready;
        if (ok) {
            t.join();
        } else {
            t.detach();
        }
        return ok;
    }

    static std::vector<std::string> tmp_leftovers(const FsPaths& paths) {
        std::vector<std::string> out;
        for (const auto& e : std::filesystem::directory_iterator(paths.tmp())) {
            const std::string name = e.path().filename().string();
            if (name.starts_with("index-")) {
                out.push_back(name);
            }
        }
        return out;
    }

    static void expect_wal_not_held(const FsPaths& paths) {
        Db db = Db::open(paths.index_db());
        Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
        ASSERT_TRUE(st.step());
        EXPECT_EQ(st.column_int64(0), 0) << "the snapshot reader transaction is still open";
    }

    std::function<void(SnapshotStage, Db&)> hook_;
};

// Блокировка не держится на время копирования, а записи и промежуточный забор во время копии
// попадают в следующую дельту и не теряются.
TEST_F(SnapshotCommit, WritesDuringCopyGoToTheNextDelta) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();

    int hook_calls = 0;
    bool intermediate_taken = false;
    hook_ = [&](SnapshotStage stage, Db&) {
        if (stage != SnapshotStage::backup) {
            return;
        }
        ++hook_calls;
        EXPECT_TRUE(write_from_other_thread(*fs, "during-1"));
        // Промежуточный забор идёт, пока копия не готова: очередь патчей уже свободна.
        intermediate_taken = fs->committer().take_intermediate().has_value();
        EXPECT_TRUE(write_from_other_thread(*fs, "during-2"));
    };
    mkfile(*fs, "before-cut");
    const auto s = fs->committer().commit(true);
    hook_ = {};
    ASSERT_TRUE(s.committed);
    EXPECT_EQ(s.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(hook_calls, 1);
    EXPECT_TRUE(intermediate_taken);
    EXPECT_GT(s.metrics.snapshot_hold.count(), 0);
    EXPECT_GT(s.metrics.snapshot_backup.count(), 0);
    EXPECT_GT(s.metrics.vacuum.count(), 0);
    EXPECT_EQ(s.metrics.patch_bytes, 0U);
    EXPECT_TRUE(tmp_leftovers(a_).empty());

    // Снимок — состояние на срез, без «during»; записи после него ждут дельты.
    EXPECT_EQ(fs->state().list_pending().size(), 1U);  // патч промежуточного забора после среза
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);
    EXPECT_NE(remote_state(), normalized(test::dump_of(*fs)));

    const auto d = fs->committer().commit();
    ASSERT_TRUE(d.committed);
    EXPECT_EQ(d.metrics.kind, crypto::BlobKind::delta);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    EXPECT_TRUE(fs->state().list_pending().empty());
    expect_wal_not_held(a_);
}

// Шаг 8: патч, забранный до фиксации среза, входит в срез и удаляется; забранный после — остаётся.
TEST_F(SnapshotCommit, PendingBoundaryIsMaxPendingSeqAtTheCut) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();

    mkfile(*fs, "taken-before-cut");
    ASSERT_TRUE(fs->committer().take_intermediate().has_value());
    ASSERT_EQ(fs->state().list_pending().size(), 1U);
    const int64_t seq_before = fs->state().max_pending_seq();

    hook_ = [&](SnapshotStage stage, Db&) {
        if (stage == SnapshotStage::backup) {
            mkfile(*fs, "taken-after-cut");
            EXPECT_TRUE(fs->committer().take_intermediate().has_value());
        }
    };
    const auto s = fs->committer().commit(true);
    hook_ = {};
    ASSERT_TRUE(s.committed);
    const auto pending = fs->state().list_pending();
    ASSERT_EQ(pending.size(), 1U) << "only the patch taken after the cut survives step 8";
    EXPECT_GT(pending.front().seq, seq_before);
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 1);

    ASSERT_TRUE(fs->committer().commit().committed);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    EXPECT_TRUE(fs->state().list_pending().empty());
    EXPECT_EQ(state_int(*fs, state_key::index_dirty), 0);
}

// --- сбой на каждом шаге снимка ---

struct StepFault {
    const char* name;
    SnapshotStage stage;
    std::function<void(const FsPaths&, Db& copy)> inject;
};

class SnapshotStepFault : public SnapshotCommit, public ::testing::WithParamInterface<StepFault> {};

TEST_P(SnapshotStepFault, ForcesSnapshotClosesReaderAndRemovesRaw) {
    const StepFault& fault = GetParam();
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();
    mkfile(*fs, "changed");

    bool injected = false;
    hook_ = [&](SnapshotStage stage, Db& db) {
        if (stage == fault.stage) {
            injected = true;
            mkfile(*fs, "after-cut");  // запись после среза: срез от неё не меняется
            fault.inject(a_, db);
        }
    };
    EXPECT_THROW(fs->committer().commit(true), CommitError);
    hook_ = {};
    EXPECT_TRUE(injected);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_TRUE(tmp_leftovers(a_).empty()) << "raw or snapshot file left in tmp/";
    expect_wal_not_held(a_);
    EXPECT_EQ(head_header().generation, 2U) << "a failed snapshot must not reach the channel";

    // Следующий коммит — снимок, и канал после него равен живому индексу (в том числе записям,
    // сделанным после среза упавшего снимка).
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 0);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    EXPECT_TRUE(tmp_leftovers(a_).empty());
}

INSTANTIATE_TEST_SUITE_P(
    Steps, SnapshotStepFault,
    ::testing::Values(
        StepFault{"before_backup", SnapshotStage::backup,
                  [](const FsPaths&, Db&) { throw std::runtime_error("injected backup failure"); }},
        // Нехватка места: копия не вмещает страницы среза (SQLITE_FULL в backup).
        StepFault{"no_space_in_backup", SnapshotStage::backup,
                  [](const FsPaths&, Db& copy) { copy.exec("PRAGMA max_page_count = 3"); }},
        StepFault{"before_vacuum_into", SnapshotStage::vacuum,
                  [](const FsPaths&, Db&) { throw std::runtime_error("injected vacuum failure"); }},
        // Настоящая ошибка SQLite: VACUUM INTO не пишет поверх существующего файла.
        StepFault{"vacuum_into_target_exists", SnapshotStage::vacuum,
                  [](const FsPaths& paths, Db&) {
                      std::ofstream(snapshot_tmp_path(paths.tmp(), 3)) << "squatter";
                  }}),
    [](const auto& param_info) { return std::string(param_info.param.name); });

// Сбой после среза до чтения max_pending_seq и так же после CAS/заливки: снимок всегда
// помечает force_snapshot. Здесь — сбой put_index (шаг 5) после успешной копии.
TEST_F(SnapshotCommit, UploadFailureAfterCopyForcesSnapshotAndRemovesFiles) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();
    mkfile(*fs, "changed");
    fault_next(StoreOp::put_index, FaultAction::permanent);
    EXPECT_THROW(fs->committer().commit(true), CommitError);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_TRUE(tmp_leftovers(a_).empty());
    expect_wal_not_held(a_);
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// --- узнавание своего снимка при монтировании ---

// Процесс упал после pin снимка (между шагами 6 и 8), записи после среза сделаны уже во время
// копирования. Блоб головы — состояние на срез; монтирование узнаёт его по tmp/index-<G>.sqlite
// (файл после VACUUM INTO копии), доделывает шаг 8, а записи после среза остаются в локальном
// индексе и уходят следующим снимком.
TEST_F(SnapshotCommit, OwnSnapshotIsRecognizedAfterCrashBetweenSteps6And8) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();  // поколение 2
    mkfile(*fs, "in-snapshot");

    std::string state_at_cut;
    hook_ = [&](SnapshotStage stage, Db&) {
        if (stage == SnapshotStage::backup) {
            state_at_cut = normalized(test::dump_of(*fs));
            mkfile(*fs, "after-cut");
        }
    };
    fault_next(StoreOp::set_head, FaultAction::crash_after);
    EXPECT_THROW(fs->committer().commit(true), store::CrashInjected);
    hook_ = {};
    // Свой снимок на месте до перезапуска; сырой копии нет.
    EXPECT_TRUE(std::filesystem::exists(snapshot_tmp_path(a_.tmp(), 3)));
    EXPECT_FALSE(std::filesystem::exists(snapshot_raw_path(a_.tmp(), 3)));
    fs.reset();
    reopen_store();
    EXPECT_EQ(head_header().generation, 3U);
    EXPECT_EQ(remote_state(), state_at_cut);

    fs = mount(a_);
    EXPECT_TRUE(fs->report().unclean_shutdown);
    EXPECT_TRUE(fs->report().resumed_commit);
    EXPECT_EQ(fs->report().situation, MountSituation::same_generation);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 3);
    EXPECT_EQ(state_int(*fs, state_key::force_snapshot), 1);
    EXPECT_TRUE(tmp_leftovers(a_).empty());
    const std::string live = normalized(test::dump_of(*fs));
    EXPECT_NE(live, state_at_cut);

    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot);
    EXPECT_EQ(remote_state(), live);
}

// Падение процесса во время копирования оставляет в tmp/ сырую копию (и её журнал): монтирование
// убирает их (префикс «index-») и не принимает за свой снимок.
TEST_F(SnapshotCommit, RawCopyLeftByCrashIsCleanedAtMount) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();
    mkfile(*fs, "changed");
    fs.reset();  // процесс умер
    std::ofstream(snapshot_raw_path(a_.tmp(), 3)) << "partial copy";
    std::ofstream(snapshot_raw_path(a_.tmp(), 3).string() + "-journal") << "journal";
    ASSERT_EQ(tmp_leftovers(a_).size(), 2U);

    fs = mount(a_);
    EXPECT_TRUE(fs->report().unclean_shutdown);
    EXPECT_FALSE(fs->report().resumed_commit);
    EXPECT_TRUE(tmp_leftovers(a_).empty());
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
}

// force_snapshot, поставленный после среза снимка, снимок не сбрасывает: промежуточный
// забор во время копии забрал патч с записями после среза, не смог положить его в
// pending_patchsets, и без снимка следующая дельта эти записи потеряла бы.
TEST_F(SnapshotCommit, ForceSnapshotMarkedAfterTheCutSurvivesTheSnapshotCommit) {
    std::atomic<bool> fail_append{false};
    commit_config_.before_append_pending = [&] {
        if (fail_append.load()) {
            throw std::runtime_error("injected append failure");
        }
    };
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();
    hook_ = [&](SnapshotStage stage, Db&) {
        if (stage != SnapshotStage::backup) {
            return;
        }
        ASSERT_TRUE(write_from_other_thread(*fs, "after-cut"));
        fail_append = true;
        EXPECT_THROW(fs->committer().take_intermediate(), CommitError);  // патч забран, не сохранён
        fail_append = false;
    };
    const auto snap = fs->committer().commit(true);
    hook_ = {};
    ASSERT_TRUE(snap.committed);
    EXPECT_EQ(snap.metrics.kind, crypto::BlobKind::snapshot);
    // Флаг после среза остаётся: и в state.sqlite (переживёт перезапуск), и в памяти.
    EXPECT_EQ(fs->state().get_int_or(state_key::force_snapshot, 0), 1);

    mkfile(*fs, "later");
    const auto next = fs->committer().commit();
    ASSERT_TRUE(next.committed);
    EXPECT_EQ(next.metrics.kind, crypto::BlobKind::snapshot) << "a delta would lose 'after-cut'";
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    // Теперь флаг сброшен: потерянного патча больше нет.
    EXPECT_EQ(fs->state().get_int_or(state_key::force_snapshot, 0), 0);
}

// Контроль: флаг, поставленный до среза, снимок сбрасывает.
TEST_F(SnapshotCommit, ForceSnapshotMarkedBeforeTheCutIsClearedBySnapshot) {
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();
    fs->state().set_int(state_key::force_snapshot, 1);
    const auto snap = fs->committer().commit(true);
    ASSERT_TRUE(snap.committed);
    EXPECT_EQ(fs->state().get_int_or(state_key::force_snapshot, 0), 0);
}

// Щель до очереди патчей: коммит выбрал дельту, а фоновый промежуточный забор в этот момент забрал
// патч, не смог его сохранить и поставил force_snapshot. Под очередью коммит перепроверяет флаг и
// уходит на снимок, иначе дельта без потерянного патча разошлась бы с локальным индексом.
TEST_F(SnapshotCommit, ForceSnapshotSetBetweenKindChoiceAndPatchQueueTurnsTheDeltaIntoASnapshot) {
    std::atomic<bool> fail_append{false};
    commit_config_.before_append_pending = [&] {
        if (fail_append.load()) {
            throw std::runtime_error("injected append failure");
        }
    };
    std::function<void()> gap;
    std::atomic<bool> gap_used{false};
    commit_config_.before_patch_queue = [&] {
        if (gap && !gap_used.exchange(true)) {
            gap();
        }
    };
    init();
    auto fs = mount(a_);
    mkfile(*fs, "base");
    fs->committer().commit_now();
    mkfile(*fs, "unsaved");
    gap = [&] {
        fail_append = true;
        EXPECT_THROW(fs->committer().take_intermediate(), CommitError);  // патч забран, не сохранён
        fail_append = false;
    };
    const auto r = fs->committer().commit();
    ASSERT_TRUE(r.committed);
    EXPECT_EQ(r.metrics.kind, crypto::BlobKind::snapshot) << "a delta would lose 'unsaved'";
    EXPECT_EQ(remote_state(), normalized(test::dump_of(*fs)));
    EXPECT_EQ(fs->state().get_int_or(state_key::force_snapshot, 0), 0);
}

}  // namespace
}  // namespace telega::meta
