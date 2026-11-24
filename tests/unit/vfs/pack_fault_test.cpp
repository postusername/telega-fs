// Инъекция сбоев в заливке и применении пакета (SPEC 6.5, 7.3, 14): настоящее монтирование
// (MountedFs с CommitManager) на LocalDirStore, Vfs поверх него. Падение на каждом шаге
// заливки и применения пакета, затем «перезапуск процесса»: монтирование на том же каталоге
// (сироты upload_journal, SPEC 7.2 шаг 7), дозаливка overlay и чтение на пустом каталоге.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <string_view>
#include <thread>

#include "../commit/commit_test_util.h"
#include "vfs/vfs.h"
#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

using namespace std::chrono_literals;
using meta::MountedFs;
using meta::test::CommitEnv;
using store::FaultAction;
using store::StoreOp;

// Размеры по init_options() CommitEnv: inline_max 64, pack_threshold 4096, chunk_size 64 КиБ.
constexpr size_t kSmall = 1500;  // класс «в пакете»

struct Machine {
    std::unique_ptr<MountedFs> fs;
    std::unique_ptr<Vfs> vfs;  // объявлен после fs: уничтожается раньше
};

class PackCrash : public CommitEnv {
  protected:
    static VfsOptions options(std::function<void(std::string_view)> probe = {}) {
        VfsOptions o = fast_options();
        o.pack_max_age = 3'600'000ms;
        o.pack_probe = std::move(probe);
        o.sync_timeout = 20s;
        return o;
    }

    Machine open(const meta::FsPaths& paths, const VfsOptions& o = options()) {
        Machine m;
        m.fs = mount(paths);
        m.vfs = std::make_unique<Vfs>(m.fs->meta(), m.fs->state(), m.fs->store(), m.fs->committer(),
                                      m.fs->keys(), VfsPaths{paths.staging(), paths.tmp()}, o);
        return m;
    }

    static int64_t put(Vfs& v, const std::string& name, const Bytes& data) {
        const Created c = v.create(meta::kRootIno, bytes_of(name), 0644, O_RDWR, 1000, 1000);
        EXPECT_EQ(v.write(c.attr.ino, 0, data), data.size());
        v.release(c.fh);
        v.forget(c.attr.ino, 1);  // ядро забыло inode: тест не держит ссылок
        return c.attr.ino;
    }
    static bool wait_open(Vfs& v, size_t n) {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (v.pack_status().open_objects != n) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(1ms);
        }
        return true;
    }
    static Bytes read_all(Vfs& v, int64_t ino) {
        const Attr a = v.getattr(ino);
        return v.read(ino, 0, static_cast<size_t>(a.size) + 1);
    }

    // chunk-сообщения, на которые не ссылается ни индекс, ни история блобов: сироты канала.
    static size_t unreferenced_messages(store::LocalDirStore& channel, MountedFs& fs) {
        std::set<int64_t> known;
        fs.meta().read([&](meta::Db& db) {
            meta::Stmt st{db, "SELECT msg_id FROM chunks"};
            while (st.step()) {
                known.insert(st.column_int64(0));
            }
            for (const auto& h : meta::list_history(db)) {
                known.insert(h.msg_id);
            }
        });
        for (const auto& c : fs.state().list_chain()) {
            known.insert(c.msg_id);
        }
        if (const auto head = channel.read_head()) {
            known.insert(head->msg_id);
        }
        size_t n = 0;
        for (const int64_t id : channel.list_messages()) {
            n += known.contains(id) ? 0 : 1;
        }
        return n;
    }

    static ::testing::AssertionResult accounting_ok(MountedFs& fs) {
        return fs.meta().read(
            [](meta::Db& db) { return meta::test::chunk_accounting_consistent(db); });
    }
};

// Падение на каждом шаге заливки и применения пакета (probe) и на put_chunk до и после.
struct Step {
    const char* name;
    bool probe;  // true — probe(name), false — отказ Store
    FaultAction fault;
    bool message_may_exist;  // сообщение дошло в канал, а журнал без msg_id: сирота до gc --deep
};

class PackCrashAtStep : public PackCrash, public ::testing::WithParamInterface<Step> {};

TEST_P(PackCrashAtStep, RestartIsConsistentAndNothingIsLost) {
    const Step step = GetParam();
    init();
    std::vector<Bytes> data;
    std::vector<int64_t> inos;
    const Bytes old_data = pattern(kSmall, 99);
    int64_t old_ino = 0;
    {
        std::atomic<bool> armed{false};
        auto m = open(a_, options([&](std::string_view s) {
                          if (step.probe && armed.load() && s == step.name) {
                              armed.store(false);
                              throw store::CrashInjected();
                          }
                      }));
        Vfs& v = *m.vfs;
        // Закоммиченное до сбоя: fsync вернулся — оно переживёт всё.
        old_ino = put(v, "old", old_data);
        ASSERT_TRUE(wait_open(v, 1));
        v.fsync(old_ino);

        for (uint64_t i = 0; i < 5; ++i) {
            data.push_back(pattern(kSmall + i * 100, i + 1));
            inos.push_back(put(v, "f" + std::to_string(i), data.back()));
        }
        ASSERT_TRUE(wait_open(v, 5));
        if (step.probe) {
            armed.store(true);
        } else {
            fault_next(StoreOp::put_chunk, step.fault);
        }
        // Заливка пакета упала вместе с «процессом»: fsync не вернул успех. Исключение —
        // падение после очистки overlay: пакет к этому моменту применён, и fsync, дождавшись
        // его и коммита, вправе вернуть успех (данные уже надёжны).
        bool threw = false;
        try {
            v.fsync(inos[0]);
        } catch (const meta::FsError&) {
            threw = true;
        }
        EXPECT_TRUE(threw || std::string_view(step.name) == "overlay_cleared");
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (!v.status().fatal && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(1ms);
        }
        EXPECT_TRUE(v.status().fatal);
        // Дальше процесс не живёт: без размонтирования, без финального коммита.
    }
    reopen_store();

    // Перезапуск на том же локальном каталоге.
    {
        auto m = open(a_);
        Vfs& v = *m.vfs;
        EXPECT_TRUE(m.fs->report().unclean_shutdown);
        EXPECT_EQ(read_all(v, old_ino), old_data);
        // Незафиксированное, но принятое write, лежит в overlay (SPEC 7.3).
        for (size_t i = 0; i < inos.size(); ++i) {
            EXPECT_EQ(read_all(v, inos[i]), data[i]) << i;
        }
        v.sync();
        EXPECT_EQ(v.status().dirty_chunks, 0U);
        EXPECT_TRUE(m.fs->state().list_dirty_chunks().empty());
        EXPECT_TRUE(m.fs->state().list_dirty_attrs().empty());
        EXPECT_TRUE(m.fs->state().list_open_pack().empty());
        EXPECT_TRUE(accounting_ok(*m.fs));
        for (size_t i = 0; i < inos.size(); ++i) {
            EXPECT_EQ(read_all(v, inos[i]), data[i]) << i;
        }
        // Сироты: всё, что осталось без ссылки, — не больше одного сообщения, дошедшего в канал
        // без записи msg_id в журнале (его убирает только gc --deep, SPEC 7.2 шаг 7).
        EXPECT_LE(unreferenced_messages(*store_, *m.fs), step.message_may_exist ? 1U : 0U)
            << "orphans_deleted=" << m.fs->report().orphans_deleted;
        v.shutdown();
        m.fs->unmount();
    }

    // Другая машина с пустым локальным каталогом видит то же.
    {
        auto m = open(b_);
        Vfs& v = *m.vfs;
        EXPECT_EQ(read_all(v, old_ino), old_data);
        for (size_t i = 0; i < inos.size(); ++i) {
            EXPECT_EQ(read_all(v, inos[i]), data[i]) << i;
        }
        EXPECT_TRUE(accounting_ok(*m.fs));
    }
}

INSTANTIATE_TEST_SUITE_P(
    Steps, PackCrashAtStep,
    ::testing::Values(Step{"run", true, FaultAction::transient, false},
                      Step{"assembled", true, FaultAction::transient, false},
                      Step{"encrypted", true, FaultAction::transient, false},
                      Step{"journal_started", true, FaultAction::transient, false},
                      Step{"put_chunk_before", false, FaultAction::crash_before, false},
                      Step{"put_chunk_after", false, FaultAction::crash_after, true},
                      Step{"uploaded", true, FaultAction::transient, true},
                      Step{"journal_finished", true, FaultAction::transient, false},
                      Step{"index_applied", true, FaultAction::transient, false},
                      Step{"overlay_cleared", true, FaultAction::transient, false}),
    [](const auto& param_info) { return std::string(param_info.param.name); });

// Успешный fsync файла из пакета: данные и метаданные переживают сбой (SPEC 7.3).
TEST_F(PackCrash, DataOfSucceededFsyncSurvivesACrash) {
    init();
    std::vector<Bytes> data;
    std::vector<int64_t> inos;
    {
        auto m = open(a_);
        Vfs& v = *m.vfs;
        for (uint64_t i = 0; i < 4; ++i) {
            data.push_back(pattern(kSmall + i, i + 1));
            inos.push_back(put(v, "f" + std::to_string(i), data.back()));
        }
        ASSERT_TRUE(wait_open(v, 4));
        v.fsync(inos[2]);  // пакет одной заливкой, затем коммит
        EXPECT_EQ(store_->call_count(StoreOp::put_chunk), 1U);
        // Незакоммиченное после fsync: файл записан, но не fsync-нут.
        data.push_back(pattern(kSmall, 77));
        inos.push_back(put(v, "late", data.back()));
        // Падение: без unmount.
    }
    reopen_store();
    {
        auto m = open(b_);  // пустой каталог, только то, что в канале
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_EQ(read_all(*m.vfs, inos[i]), data[i]) << i;
        }
        EXPECT_TRUE(accounting_ok(*m.fs));
    }
}

// Сбой на заливке пакета не оставляет открытый пакет без объектов: после transient-ошибок
// пакет заливается, а строки open_pack и overlay пусты.
TEST_F(PackCrash, TransientErrorsOnPackUploadAreRetried) {
    init();
    auto m = open(a_);
    Vfs& v = *m.vfs;
    fault_next(StoreOp::put_chunk, FaultAction::transient, 3);
    std::vector<int64_t> inos;
    for (uint64_t i = 0; i < 3; ++i) {
        inos.push_back(put(v, "f" + std::to_string(i), pattern(kSmall, i + 1)));
    }
    ASSERT_TRUE(wait_open(v, 3));
    v.fsync(inos[0]);
    EXPECT_EQ(store_->call_count(StoreOp::put_chunk), 4U);
    EXPECT_TRUE(m.fs->state().list_open_pack().empty());
    EXPECT_TRUE(m.fs->state().list_dirty_chunks().empty());
    EXPECT_TRUE(accounting_ok(*m.fs));
}

// Постоянная ошибка: fsync — EIO, объекты остаются в overlay с ошибкой, не в открытом пакете;
// повторный fsync пробует снова и проходит.
TEST_F(PackCrash, PermanentErrorLeavesObjectsInTheOverlayUntilTheNextFsync) {
    init();
    auto m = open(a_);
    Vfs& v = *m.vfs;
    std::vector<Bytes> data;
    std::vector<int64_t> inos;
    for (uint64_t i = 0; i < 3; ++i) {
        data.push_back(pattern(kSmall, i + 1));
        inos.push_back(put(v, "f" + std::to_string(i), data.back()));
    }
    ASSERT_TRUE(wait_open(v, 3));
    fault_next(StoreOp::put_chunk, FaultAction::permanent);
    EXPECT_THROW(v.fsync(inos[1]), meta::FsError);
    // fsync просыпается, как только помечен его объект; остальные объекты пакета помечаются
    // следом тем же потоком заливки.
    const auto marked_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (v.status().failed < 3U && std::chrono::steady_clock::now() < marked_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(v.status().failed, 3U);
    EXPECT_EQ(v.status().dirty_chunks, 3U);
    EXPECT_FALSE(v.status().fatal);
    EXPECT_TRUE(m.fs->state().list_open_pack().empty());
    EXPECT_EQ(v.pack_status().open_objects, 0U);
    EXPECT_TRUE(m.fs->state().list_uploads().empty());
    for (size_t i = 0; i < inos.size(); ++i) {
        EXPECT_EQ(read_all(v, inos[i]), data[i]);
    }
    v.fsync(inos[1]);  // явный запрос — новая попытка, но только для этого файла
    EXPECT_EQ(v.status().failed, 2U);
    EXPECT_EQ(v.status().dirty_chunks, 2U);
    v.sync();
    EXPECT_EQ(v.status().failed, 0U);
    EXPECT_EQ(v.status().dirty_chunks, 0U);
    for (size_t i = 0; i < inos.size(); ++i) {
        EXPECT_EQ(read_all(v, inos[i]), data[i]);
    }
    EXPECT_TRUE(accounting_ok(*m.fs));
}

}  // namespace
}  // namespace telega::vfs::test
