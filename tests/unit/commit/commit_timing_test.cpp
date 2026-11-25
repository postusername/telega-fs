// Коммит: отсрочка по commit_interval, пока очередь заливки не пуста, и CAS с повтором
// после своего pin (кэш TDLib показывает старую голову).

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "commit_test_util.h"

namespace telega::meta {
namespace {

using namespace std::chrono_literals;
using store::FaultAction;
using store::StoreOp;
using test::CommitEnv;
using test::FakeClock;

// Store, который после set_head несколько раз отвечает на read_head старой головой или 404,
// как TDLib с кэшем ChannelFull. Всё остальное пробрасывает.
class StaleHeadStore final : public store::Store {
  public:
    StaleHeadStore(store::Store& inner, FakeClock& clock) : inner_(inner), clock_(clock) {}

    // Следующий set_head: reads раз вернуть старую голову (или 404, если as_404).
    void arm(int reads, bool as_404 = false) {
        const std::lock_guard lock(mu_);
        arm_reads_ = reads;
        arm_404_ = as_404;
    }
    [[nodiscard]] std::vector<FakeClock::time_point> read_times() const {
        const std::lock_guard lock(mu_);
        return read_times_;
    }

    store::ChunkRef put_chunk(const std::filesystem::path& f) override {
        return inner_.put_chunk(f);
    }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        inner_.read_range(r, off, out);
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_.delete_messages(ids); }
    std::optional<store::HeadInfo> read_head() override {
        {
            const std::lock_guard lock(mu_);
            read_times_.push_back(clock_.now());
            if (stale_reads_ > 0) {
                --stale_reads_;
                return stale_head_;
            }
        }
        return inner_.read_head();
    }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const std::filesystem::path& f) override { return inner_.put_index(f); }
    void set_head(int64_t new_id, std::optional<int64_t> old_id) override {
        const auto before = inner_.read_head();
        inner_.set_head(new_id, old_id);
        const std::lock_guard lock(mu_);
        stale_reads_ = arm_reads_;
        stale_head_ = arm_404_ ? std::nullopt : before;
        arm_reads_ = 0;
    }

  private:
    store::Store& inner_;
    FakeClock& clock_;
    mutable std::mutex mu_;
    int arm_reads_ = 0;
    bool arm_404_ = false;
    int stale_reads_ = 0;
    std::optional<store::HeadInfo> stale_head_;
    std::vector<FakeClock::time_point> read_times_;
};

class Timing : public CommitEnv {
  protected:
    void SetUp() override {
        CommitEnv::SetUp();
        commit_config_.clock = &clock_;
        commit_config_.cas_retry_window = 90s;  // значения по умолчанию
        commit_config_.commit_interval = 60s;
        commit_config_.commit_every_chunks = 3;
        commit_config_.retry_min = 1s;
        commit_config_.retry_max = 4s;
        init();
    }

    // Выполнить commit_now в другом потоке, двигая часы, пока он не закончится. Часы идут шагами
    // по 500 мс, чтобы ожидания по 10 с не перескакивали. Возвращает исключение коммита.
    std::exception_ptr commit_advancing(MountedFs& fs, std::chrono::milliseconds limit = 200s) {
        auto done = std::async(std::launch::async, [&] {
            try {
                fs.committer().commit_now();
            } catch (...) {
                return std::current_exception();
            }
            return std::exception_ptr{};
        });
        std::chrono::milliseconds spent{0};
        while (done.wait_for(3ms) != std::future_status::ready) {
            clock_.advance(500ms);
            spent += 500ms;
            if (spent > limit) {
                ADD_FAILURE() << "commit did not finish while the clock advanced " << limit.count()
                              << " ms";
                break;
            }
        }
        return done.get();
    }

    FakeClock clock_;
};

// --- CAS с повтором ---

class CasRetry : public Timing {
  protected:
    // Смонтировать a_ поверх обёртки; первый коммит сделан (свой pin только что состоялся).
    std::unique_ptr<MountedFs> mount_and_pin() {
        auto fs = mount_fs(stale(), a_, mount_options());
        mkfile(*fs, "first");
        fs->committer().commit_now();
        return fs;
    }
    // Обёртка создаётся после SetUp: store_ появляется только в нём.
    StaleHeadStore& stale() {
        if (!stale_) {
            stale_ = std::make_unique<StaleHeadStore>(*store_, clock_);
        }
        return *stale_;
    }
    std::unique_ptr<StaleHeadStore> stale_;
};

TEST_F(CasRetry, StaleOlderHeadIsRecheckedUntilItCatchesUp) {
    stale().arm(3);  // после первого pin три чтения вернут старую голову (init)
    auto fs = mount_and_pin();
    mkfile(*fs, "second");
    const auto before = stale().read_times().size();
    EXPECT_EQ(commit_advancing(*fs), nullptr);
    EXPECT_FALSE(fs->read_only());
    EXPECT_EQ(head_header().generation, 3U);
    EXPECT_EQ(remote_state(), test::normalized(test::dump_of(*fs)));

    const auto times = stale().read_times();
    ASSERT_EQ(times.size() - before, 4U);  // три устаревших ответа и четвёртый настоящий
    for (size_t i = before + 1; i < times.size(); ++i) {
        const auto gap = times[i] - times[i - 1];
        EXPECT_GE(gap, 10s);  // не чаще раза в 10 с
        EXPECT_LE(gap, 12s);
    }
    fs->unmount();
}

TEST_F(CasRetry, Missing404HeadIsRecheckedToo) {
    stale().arm(2, /*as_404=*/true);
    auto fs = mount_and_pin();
    mkfile(*fs, "second");
    const auto before = stale().read_times().size();
    EXPECT_EQ(commit_advancing(*fs), nullptr);
    EXPECT_EQ(stale().read_times().size() - before, 3U);
    EXPECT_EQ(head_header().generation, 3U);
    fs->unmount();
}

TEST_F(CasRetry, StaleHeadForWholeWindowIsConflict) {
    stale().arm(1000);
    auto fs = mount_and_pin();
    mkfile(*fs, "second");
    const auto start = clock_.now();
    const auto before = stale().read_times().size();
    const std::exception_ptr err = commit_advancing(*fs);
    ASSERT_NE(err, nullptr);
    EXPECT_THROW(std::rethrow_exception(err), CommitError);
    EXPECT_TRUE(fs->read_only());
    EXPECT_GE(clock_.now() - start, 85s);   // окно 90 с отсчитано от pin
    EXPECT_LE(clock_.now() - start, 100s);  // и не дольше
    const size_t reads = stale().read_times().size() - before;
    EXPECT_GE(reads, 6U);
    EXPECT_LE(reads, 10U);
    EXPECT_EQ(head_header().generation, 2U);  // голова канала не тронута
    EXPECT_THROW(fs->unmount(), CommitError);
}

TEST_F(CasRetry, NoRetryWhenOurLastPinIsOlderThanTheWindow) {
    auto fs = mount_and_pin();
    mkfile(*fs, "second");
    stale().arm(1000);
    // Свой pin был давно: устаревший ответ — не кэш, а настоящий откат головы.
    // arm действует со следующего set_head, поэтому сделаем его и состарим.
    fs->committer().commit_now();
    clock_.advance(100s);
    mkfile(*fs, "third");
    const auto before = stale().read_times().size();
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    EXPECT_EQ(stale().read_times().size() - before, 1U);  // одна проверка, без повторов
    EXPECT_TRUE(fs->read_only());
}

TEST_F(CasRetry, NoRetryForForeignHead) {
    auto fs = mount_and_pin();
    // Вторая машина обгоняет нас: голова канала новее ожидаемой.
    store::LocalDirStore other{channel_dir()};
    auto fb = mount_fs(other, b_, mount_options());
    mkfile(*fb, "from-b");
    fb->committer().commit_now();

    mkfile(*fs, "second");
    const auto before = stale().read_times().size();
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    EXPECT_EQ(stale().read_times().size() - before, 1U);
    EXPECT_TRUE(fs->read_only());
}

TEST_F(CasRetry, FirstCommitOfAProcessHasNoRetry) {
    // Своего pin в этом процессе не было: «старая голова» — это чужое изменение.
    auto fs = mount_fs(stale(), a_, mount_options());
    store::LocalDirStore other{channel_dir()};
    auto fb = mount_fs(other, b_, mount_options());
    mkfile(*fb, "from-b");
    fb->committer().commit_now();
    stale().arm(0);
    mkfile(*fs, "from-a");
    const auto before = stale().read_times().size();
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    EXPECT_EQ(stale().read_times().size() - before, 1U);
}

TEST_F(CasRetry, ShutdownInterruptsTheWaitWithoutReadOnly) {
    stale().arm(1000);
    auto fs = mount_and_pin();
    mkfile(*fs, "second");
    auto commit = std::async(std::launch::async, [&] {
        try {
            fs->committer().commit_now();
        } catch (...) {
            return std::current_exception();
        }
        return std::exception_ptr{};
    });
    // Дождаться, пока коммит уснул в ожидании (часы стоят).
    const auto before = stale().read_times().size();
    while (stale().read_times().size() == before) {
        std::this_thread::sleep_for(1ms);
    }
    std::this_thread::sleep_for(30ms);
    fs->committer().stop();
    const std::exception_ptr err = commit.get();
    ASSERT_NE(err, nullptr);
    EXPECT_THROW(std::rethrow_exception(err), CommitError);
    EXPECT_FALSE(fs->read_only());  // остановка — не конфликт
}

// --- неподтверждённый pin и кэш TDLib ---

class UnconfirmedWindow : public Timing {
  protected:
    // Сбой шага 8: триггер во втором соединении к state.sqlite не даёт записать chain. Pin к
    // этому моменту уже сделан.
    void break_step8() {
        Db db = Db::open(a_.state_db());
        db.exec(
            "CREATE TRIGGER boom BEFORE INSERT ON chain BEGIN SELECT RAISE(ABORT, 'boom'); END");
    }
    void repair_step8() {
        Db db = Db::open(a_.state_db());
        db.exec("DROP TRIGGER boom");
    }
    // Последнее сообщение канала, кроме головы init: блоб неподтверждённой попытки.
    int64_t newest_message() { return store_->list_messages().back(); }
};

// Упал шаг 8 после pin, а кэш ещё отдаёт старую голову: «старая голова» — неизвестность, блоб
// (он закреплён!) не удаляется, чтение повторяется и находит наш блоб.
TEST_F(UnconfirmedWindow, StaleHeadAfterPinDoesNotDeleteThePinnedBlob) {
    const auto old_head = test::must(store_->read_head());
    test::MaskedHeadStore masked{*store_};
    auto fs = mount_fs(masked, a_, mount_options());
    mkfile(*fs, "x");
    break_step8();
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    const int64_t blob = newest_message();
    ASSERT_NE(blob, old_head.msg_id);
    ASSERT_EQ(test::must(store_->read_head()).msg_id, blob);  // pin состоялся
    repair_step8();

    masked.mask_next(3, old_head);  // кэш: три чтения отдают старую голову
    const int reads_before = masked.head_reads();
    EXPECT_EQ(commit_advancing(*fs), nullptr);
    EXPECT_GE(masked.head_reads() - reads_before, 4);
    EXPECT_TRUE(has_message(blob));  // не удалён
    EXPECT_EQ(test::must(store_->read_head()).msg_id, blob);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 2);
    EXPECT_EQ(state_int(*fs, state_key::head_msg_id), blob);
    fs->unmount();
}

// То же с «нет головы» (404).
TEST_F(UnconfirmedWindow, MissingHeadAfterPinDoesNotDeleteThePinnedBlob) {
    test::MaskedHeadStore masked{*store_};
    auto fs = mount_fs(masked, a_, mount_options());
    mkfile(*fs, "x");
    break_step8();
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    const int64_t blob = newest_message();
    repair_step8();
    masked.mask_next(2);
    EXPECT_EQ(commit_advancing(*fs), nullptr);
    EXPECT_TRUE(has_message(blob));
    EXPECT_EQ(state_int(*fs, state_key::head_msg_id), blob);
    fs->unmount();
}

// pin не прошёл: до конца окна блоб не трогаем (мог быть кэш), после окна — удаляем и коммитим.
TEST_F(UnconfirmedWindow, BlobOfAPinThatNeverHappenedIsDeletedOnlyAfterTheWindow) {
    test::MaskedHeadStore masked{*store_};
    auto fs = mount_fs(masked, a_, mount_options());
    mkfile(*fs, "x");
    fault_next(StoreOp::set_head, FaultAction::transient);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    const int64_t orphan = newest_message();
    const auto before = clock_.now();

    auto done = std::async(std::launch::async, [&] {
        try {
            fs->committer().commit_now();
        } catch (...) {
            return std::current_exception();
        }
        return std::exception_ptr{};
    });
    // Идём по часам до 80 с с последнего pin: блоб ещё на месте, коммит ждёт.
    while (clock_.now() - before < 80s && done.wait_for(3ms) != std::future_status::ready) {
        clock_.advance(500ms);
        EXPECT_TRUE(has_message(orphan)) << "deleted before the window ended";
    }
    EXPECT_NE(done.wait_for(0ms), std::future_status::ready);
    while (done.wait_for(3ms) != std::future_status::ready) {
        clock_.advance(500ms);
        ASSERT_LT(clock_.now() - before, 200s);
    }
    EXPECT_EQ(done.get(), nullptr);
    EXPECT_FALSE(has_message(orphan));  // после окна блоб убран
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 2);
    EXPECT_NE(head_header().generation, 1U);
    fs->unmount();
}

// Окончательная ошибка pin (permanent): pin не прошёл, кэш TDLib ничего не скрывает, и
// неподтверждённая попытка разбирается одним чтением. Часы стоят: окно ожидания (до 90 с) повисло
// бы навсегда и держало commit_mu_ (fsync, размонтирование).
TEST_F(UnconfirmedWindow, PermanentPinErrorIsResolvedWithASingleHeadRead) {
    test::MaskedHeadStore masked{*store_};
    auto fs = mount_fs(masked, a_, mount_options());
    mkfile(*fs, "x");
    fault_next(StoreOp::set_head, FaultAction::permanent);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    const int64_t orphan = newest_message();

    const int reads_before = masked.head_reads();
    auto done = std::async(std::launch::async, [&] {
        try {
            fs->committer().commit_now();
        } catch (...) {
            return std::current_exception();
        }
        return std::exception_ptr{};
    });
    ASSERT_EQ(done.wait_for(10s), std::future_status::ready) << "waited for the TDLib cache window";
    EXPECT_EQ(done.get(), nullptr);
    // Одно чтение для разбора и одно для CAS нового коммита.
    EXPECT_EQ(masked.head_reads() - reads_before, 2);
    EXPECT_FALSE(has_message(orphan));
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 2);
    fs->unmount();
}

TEST_F(UnconfirmedWindow, UnmountAfterAPermanentPinErrorDoesNotWaitForTheWindow) {
    auto fs = mount(a_);
    mkfile(*fs, "x");
    fault_next(StoreOp::set_head, FaultAction::permanent);
    EXPECT_THROW(fs->committer().commit_now(), CommitError);
    auto done = std::async(std::launch::async, [&] { fs->unmount(); });
    ASSERT_EQ(done.wait_for(10s), std::future_status::ready) << "unmount waited for the window";
    done.get();
    EXPECT_EQ(head_header().generation, 2U);
}

// --- отсрочка по commit_interval ---

class Defer : public Timing {
  protected:
    void SetUp() override {
        Timing::SetUp();
        tune();
        fs_ = mount(a_);
        fs_->committer().set_upload_probe([this] { return busy_.load(); });
        fs_->committer().start();
    }
    void TearDown() override {
        if (fs_) {
            fs_->committer().set_upload_probe({});
            fs_->unmount();
        }
    }

    virtual void tune() {}

    // Подвинуть часы и убедиться, что коммита не было.
    void expect_no_commit_after(std::chrono::milliseconds advance) {
        clock_.advance(advance);
        fs_->committer().poke();
        EXPECT_FALSE(fs_->committer().wait_for_generation(2, 100ms));
    }

    std::atomic<bool> busy_{false};
    std::unique_ptr<MountedFs> fs_;
};

TEST_F(Defer, IntervalCommitWaitsWhileUploadsAreQueued) {
    mkfile(*fs_, "x");
    busy_ = true;
    expect_no_commit_after(61s);  // интервал вышел, но очередь не пуста
    expect_no_commit_after(120s);  // 181 с
    expect_no_commit_after(118s);  // 299 с: до 5 · 60 с ещё секунда
    clock_.advance(2s);            // 301 с: предел отсрочки
    fs_->committer().poke();
    EXPECT_TRUE(fs_->committer().wait_for_generation(2, 10s));
}

// Пробник (Vfs::status()) дорог, а цикл просыпается от каждой записи: в окне отсрочки проба идёт
// не чаще раза в defer_recheck.
TEST_F(Defer, ProbeIsRateLimitedInTheDeferWindow) {
    std::atomic<int> probes{0};
    fs_->committer().set_upload_probe([&] {
        ++probes;
        return true;
    });
    mkfile(*fs_, "x");
    clock_.advance(61s);  // интервал вышел, очередь не пуста: отсрочка
    for (int i = 0; i < 50; ++i) {
        mkfile(*fs_, "f" + std::to_string(i));  // каждая запись будит фоновый поток
        fs_->committer().poke();
        std::this_thread::sleep_for(2ms);
    }
    EXPECT_FALSE(fs_->committer().wait_for_generation(2, 100ms));
    EXPECT_GE(probes.load(), 1);
    EXPECT_LE(probes.load(), 3);  // без ограничения были бы десятки
    // Время идёт — проба повторяется.
    const int seen = probes.load();
    clock_.advance(5s);
    fs_->committer().poke();
    EXPECT_FALSE(fs_->committer().wait_for_generation(2, 100ms));
    EXPECT_GT(probes.load(), seen);
    fs_->committer().set_upload_probe({});
}

// Результат пробы относится к своему окну отсрочки: после коммита новое окно начинается с
// первой пробы, даже если прошлая проба была недавно (по часам defer_recheck).
class DeferSlowRecheck : public Defer {
  protected:
    void tune() override { commit_config_.defer_recheck = 1000s; }
};

TEST_F(DeferSlowRecheck, ProbeResultDoesNotLeakIntoTheNextDeferWindow) {
    mkfile(*fs_, "x");
    busy_ = true;
    expect_no_commit_after(61s);  // проба «занято», окно отсрочки
    fs_->committer().commit_now();  // fsync: коммит без отсрочки, окно закончилось
    ASSERT_TRUE(fs_->committer().wait_for_generation(2, 10s));
    busy_ = false;
    mkfile(*fs_, "y");
    clock_.advance(61s);  // интервал нового окна вышел, очередь пуста
    fs_->committer().poke();
    EXPECT_TRUE(fs_->committer().wait_for_generation(3, 10s));
}

TEST_F(Defer, CommitsAsSoonAsTheQueueEmpties) {
    mkfile(*fs_, "x");
    busy_ = true;
    expect_no_commit_after(90s);
    busy_ = false;
    fs_->committer().chunk_uploaded();  // последняя заливка закончилась
    EXPECT_TRUE(fs_->committer().wait_for_generation(2, 10s));
}

TEST_F(Defer, EmptyQueueCommitsAtTheInterval) {
    mkfile(*fs_, "x");
    expect_no_commit_after(59s);
    clock_.advance(2s);
    fs_->committer().poke();
    EXPECT_TRUE(fs_->committer().wait_for_generation(2, 10s));
}

TEST_F(Defer, ChunkCountCommitsWithoutDeferral) {
    mkfile(*fs_, "x");
    busy_ = true;
    fs_->committer().chunk_uploaded();
    fs_->committer().chunk_uploaded();
    EXPECT_FALSE(fs_->committer().wait_for_generation(2, 50ms));
    fs_->committer().chunk_uploaded();  // commit_every_chunks = 3
    EXPECT_TRUE(fs_->committer().wait_for_generation(2, 10s));
}

TEST_F(Defer, SyncCommitsIgnoreTheQueue) {
    busy_ = true;
    mkfile(*fs_, "x");
    fs_->committer().commit_now();  // fsync, fsyncdir, sync
    EXPECT_EQ(head_header().generation, 2U);
}

TEST_F(Defer, UnmountIgnoresTheQueue) {
    busy_ = true;
    mkfile(*fs_, "x");
    fs_->committer().set_upload_probe({});
    fs_->unmount();
    EXPECT_EQ(head_header().generation, 2U);
    fs_.reset();
}

TEST_F(Defer, ClearedProbeMeansNoDeferral) {
    mkfile(*fs_, "x");
    busy_ = true;
    expect_no_commit_after(90s);
    fs_->committer().set_upload_probe({});
    EXPECT_TRUE(fs_->committer().wait_for_generation(2, 10s));
}

}  // namespace
}  // namespace telega::meta
