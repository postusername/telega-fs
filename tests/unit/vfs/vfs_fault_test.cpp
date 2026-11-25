// Vfs: инъекция сбоев Store на шагах записи и чтения — транзиентные и постоянные ошибки
// заливки, «падение» до и после put_chunk с перезапуском, таймаут чтения, порча чанка.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <vector>

#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

using store::FaultAction;
using store::FaultRule;
using store::StoreOp;

class VfsFaults : public ::testing::Test {
  protected:
    void SetUp() override { env.start(); }
    [[nodiscard]] Vfs& v() const { return *env.vfs; }

    // Файл из нескольких позиций, записанных с конца (без потоковой заливки).
    int64_t make_file(const Bytes& data) {
        const int64_t f = env.mkfile("f");
        const auto n = static_cast<int64_t>(data.size());
        for (int64_t idx = (n - 1) / kChunk; idx >= 0; --idx) {
            const int64_t end = std::min(n, (idx + 1) * kChunk);
            const ByteSpan part{data.data() + idx * kChunk,
                                static_cast<size_t>(end - idx * kChunk)};
            env.write(f, static_cast<uint64_t>(idx * kChunk), part);
        }
        return f;
    }

    [[nodiscard]] std::filesystem::path message_path(int64_t msg_id) const {
        return env.store->dir() / "msgs" / std::to_string(msg_id);
    }

    [[nodiscard]] meta::Chunk chunk_of(int64_t ino, int64_t idx) const {
        return env.meta->read([&](meta::Db& db) {
            const auto e = meta::get_extent(db, ino, idx);
            return *meta::get_chunk(db, e->chunk_id);
        });
    }

    VfsEnv env;
};

TEST_F(VfsFaults, TransientUploadErrorsAreRetried) {
    env.store->set_faults({FaultRule{StoreOp::put_chunk, 1, FaultAction::transient, {}},
                           FaultRule{StoreOp::put_chunk, 2, FaultAction::transient, {}},
                           FaultRule{StoreOp::put_chunk, 3, FaultAction::transient, {}}});
    const Bytes data = pattern(100'000, 1);
    const int64_t f = make_file(data);
    v().fsync(f);
    EXPECT_EQ(env.store->call_count(StoreOp::put_chunk), 4U);
    EXPECT_EQ(env.extents(f).size(), 1U);
    EXPECT_FALSE(v().status().last_error.empty());
    EXPECT_EQ(v().status().failed, 0U);
    EXPECT_TRUE(env.state->list_uploads().empty());
    env.restart();
    EXPECT_EQ(env.read_all(f), data);
}

TEST_F(VfsFaults, PermanentUploadErrorKeepsChunkInOverlay) {
    env.store->set_faults({FaultRule{StoreOp::put_chunk, 1, FaultAction::permanent, {}}});
    const Bytes data = pattern(100'000, 2);
    const int64_t f = make_file(data);
    EXPECT_EQ(err_of([&] { v().fsync(f); }), EIO);
    const VfsStatus st = v().status();
    EXPECT_EQ(st.failed, 1U);
    EXPECT_EQ(st.dirty_chunks, 1U);
    EXPECT_FALSE(st.last_error.empty());
    EXPECT_EQ(env.store->call_count(StoreOp::put_chunk), 1U);  // не повторялось
    EXPECT_TRUE(env.extents(f).empty());
    EXPECT_TRUE(env.state->list_uploads().empty());
    EXPECT_EQ(env.read_all(f), data);  // данные в overlay
    // Таймер такой чанк сам не берёт; явный fsync пробует снова.
    v().fsync(f);
    EXPECT_EQ(env.extents(f).size(), 1U);
    EXPECT_EQ(v().status().failed, 0U);
    env.restart();
    EXPECT_EQ(env.read_all(f), data);
}

TEST_F(VfsFaults, PermanentErrorIsRetriedAfterRestart) {
    env.store->set_faults({FaultRule{StoreOp::put_chunk, 1, FaultAction::permanent, {}}});
    const Bytes data = pattern(50'000, 3);
    const int64_t f = make_file(data);
    EXPECT_EQ(err_of([&] { v().flush_all(); }), EIO);
    env.restart();
    env.vfs->flush_all();
    EXPECT_EQ(env.extents(f).size(), 1U);
    EXPECT_EQ(env.read_all(f), data);
}

struct CrashCase {
    FaultAction action;
    uint64_t nth;
};

class VfsCrash : public VfsFaults, public ::testing::WithParamInterface<CrashCase> {};

TEST_P(VfsCrash, DataSurvivesCrashDuringUpload) {
    const CrashCase cc = GetParam();
    env.store->set_faults({FaultRule{StoreOp::put_chunk, cc.nth, cc.action, {}}});
    VfsOptions o = fast_options();
    o.upload_concurrency = 1;  // n-й вызов put_chunk — n-й чанк
    env.restart(o);
    const Bytes data = pattern(3 * kChunk - 1000, 4);
    const int64_t f = make_file(data);
    EXPECT_EQ(err_of([&] { env.vfs->fsync(f); }), EIO);
    EXPECT_TRUE(env.vfs->status().fatal);
    EXPECT_EQ(err_of([&] { env.vfs->write(f, 0, bytes_of("x")); }), EIO);

    env.store->set_faults({});
    Vfs& r = env.restart(fast_options());
    EXPECT_EQ(env.read_all(f), data);  // до дозаливки — из overlay и индекса
    r.flush_all();
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
    EXPECT_TRUE(env.state->list_dirty_attrs().empty());
    EXPECT_EQ(env.extents(f).size(), 3U);
    EXPECT_EQ(env.must_inode(f).size, static_cast<int64_t>(data.size()));
    EXPECT_TRUE(env.accounting_ok());
    env.restart();
    EXPECT_EQ(env.read_all(f), data);
    // Сироты остаются только в журнале (их убирает монтирование).
    for (const auto& rec : env.state->list_uploads()) {
        EXPECT_FALSE(rec.msg_id.has_value());
    }
}

INSTANTIATE_TEST_SUITE_P(Steps, VfsCrash,
                         ::testing::Values(CrashCase{FaultAction::crash_before, 1},
                                           CrashCase{FaultAction::crash_after, 1},
                                           CrashCase{FaultAction::crash_before, 2},
                                           CrashCase{FaultAction::crash_after, 3}));

TEST_F(VfsFaults, ReadTimesOutWithEio) {
    const Bytes data = pattern(200'000, 5);
    const int64_t f = make_file(data);
    v().fsync(f);
    VfsOptions o = fast_options();
    o.read_timeout = milliseconds(100);
    o.retry_initial = milliseconds(5);
    o.retry_max = milliseconds(20);
    Vfs& r = env.restart(o);
    env.store->set_faults(
        {FaultRule{StoreOp::read_range, std::nullopt, FaultAction::transient, {}}});
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(err_of([&] { r.read(f, 0, 100); }), EIO);
    const auto spent = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(spent, milliseconds(100));
    EXPECT_LT(spent, milliseconds(2'000));
    EXPECT_GT(env.store->call_count(StoreOp::read_range), 2U);
    // Транзиентная ошибка однажды — чтение проходит.
    const uint64_t calls = env.store->call_count(StoreOp::read_range);
    env.store->set_faults({FaultRule{StoreOp::read_range, calls + 1, FaultAction::transient, {}}});
    EXPECT_EQ(r.read(f, 0, data.size()), data);
    // Постоянная — сразу EIO.
    env.restart(o);
    env.store->set_faults(
        {FaultRule{StoreOp::read_range, std::nullopt, FaultAction::permanent, {}}});
    EXPECT_EQ(err_of([&] { env.vfs->read(f, 0, 10); }), EIO);
}

TEST_F(VfsFaults, CorruptChunkGivesEio) {
    const Bytes data = pattern(kChunk + 200'000, 6);
    const int64_t f = make_file(data);
    v().fsync(f);
    const meta::Chunk c1 = chunk_of(f, 1);
    {
        std::fstream file(message_path(c1.msg_id), std::ios::in | std::ios::out | std::ios::binary);
        file.seekp(40 + 70'000);
        char b = 0;
        file.seekg(40 + 70'000);
        file.read(&b, 1);
        b = static_cast<char>(b ^ 1);
        file.seekp(40 + 70'000);
        file.write(&b, 1);
    }
    Vfs& r = env.restart();
    // Повреждён второй сегмент позиции 1: он — EIO, остальное читается.
    EXPECT_EQ(err_of([&] { r.read(f, kChunk + 65536, 10); }), EIO);
    EXPECT_EQ(r.read(f, kChunk, 100), Bytes(data.begin() + kChunk, data.begin() + kChunk + 100));
    EXPECT_EQ(r.read(f, 0, 100), Bytes(data.begin(), data.begin() + 100));
    // RMW поверх испорченного чанка тоже EIO, и файл не меняется.
    EXPECT_EQ(err_of([&] { r.write(f, kChunk + 65536 + 5, bytes_of("zz")); }), EIO);
    EXPECT_EQ(env.staging_files(), 0U);
}

TEST_F(VfsFaults, TruncatedChunkGivesEio) {
    const Bytes data = pattern(200'000, 7);
    const int64_t f = make_file(data);
    v().fsync(f);
    const meta::Chunk c = chunk_of(f, 0);
    // Отрезать последний сегмент целиком: decrypt этого не увидит, но размер не сойдётся.
    const auto path = message_path(c.msg_id);
    const auto size = std::filesystem::file_size(path);
    std::filesystem::resize_file(path, size - (200'000 - 3 * 65536) - 16);
    Vfs& r = env.restart();
    EXPECT_EQ(err_of([&] { r.read(f, 0, data.size()); }), EIO);
    EXPECT_EQ(err_of([&] { r.write(f, 10, bytes_of("x")); }), EIO);
}

TEST_F(VfsFaults, SubstitutedChunkGivesEio) {
    const Bytes a = pattern(100'000, 8);
    const int64_t f = make_file(a);
    v().fsync(f);
    const int64_t g = env.mkfile("g");
    env.write(g, 0, pattern(100'000, 9));
    v().fsync(g);
    // Подмена сообщения одного чанка другим (AD содержит chunk_id).
    std::filesystem::copy_file(message_path(chunk_of(g, 0).msg_id),
                               message_path(chunk_of(f, 0).msg_id),
                               std::filesystem::copy_options::overwrite_existing);
    Vfs& r = env.restart();
    EXPECT_EQ(err_of([&] { r.read(f, 0, 10); }), EIO);
}

TEST_F(VfsFaults, CommitFailureMakesFsyncFail) {
    const int64_t f = make_file(pattern(10'000, 10));
    env.committer.fail_commit = true;
    EXPECT_EQ(err_of([&] { v().fsync(f); }), EIO);
    EXPECT_EQ(err_of([&] { v().sync(); }), EIO);
    env.committer.fail_commit = false;
    v().sync();
}

// --- sync_timeout: без сети fsync и размонтирование не ждут вечно ---

TEST_F(VfsFaults, FsyncTimesOutWithoutNetworkAndUploadsResumeAfter) {
    VfsOptions o = fast_options();
    o.sync_timeout = std::chrono::milliseconds(300);
    env.restart(o);
    env.store->set_faults(
        {FaultRule{StoreOp::put_chunk, std::nullopt, FaultAction::transient, {}}});
    const Bytes data = pattern(100'000, 7);
    const int64_t f = make_file(data);

    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(err_of([&] { v().fsync(f); }), EIO);
    const auto waited = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(waited, std::chrono::milliseconds(300));
    EXPECT_LT(waited, std::chrono::seconds(10));
    EXPECT_EQ(err_of([&] { v().flush_all(); }), EIO);
    EXPECT_EQ(err_of([&] { v().sync(); }), EIO);

    // Состояние overlay не изменилось, данные читаются, повторы заливки продолжаются.
    EXPECT_EQ(v().status().dirty_chunks, 1U);
    EXPECT_EQ(v().status().failed, 0U);
    EXPECT_FALSE(v().status().fatal);
    EXPECT_EQ(env.read_all(f), data);
    EXPECT_GT(env.store->call_count(StoreOp::put_chunk), 1U);

    // Сеть вернулась: чанк дозаливается, следующий fsync успешен.
    env.store->set_faults({});
    v().fsync(f);
    EXPECT_EQ(v().status().dirty_chunks, 0U);
    env.restart(o);
    EXPECT_EQ(env.read_all(f), data);
}

TEST_F(VfsFaults, ShutdownWithTimeoutDoesNotHangAndKeepsOverlay) {
    VfsOptions o = fast_options();
    o.sync_timeout = std::chrono::milliseconds(300);
    env.restart(o);
    env.store->set_faults(
        {FaultRule{StoreOp::put_chunk, std::nullopt, FaultAction::transient, {}}});
    const Bytes data = pattern(100'000, 8);
    const int64_t f = make_file(data);

    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(err_of([&] { v().shutdown(); }), EIO);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(10));
    EXPECT_NO_THROW(v().shutdown());  // повторный вызов безопасен

    // Незалитое осталось в overlay и дозаливается после перезапуска.
    env.store->set_faults({});
    env.restart(o);
    EXPECT_EQ(env.read_all(f), data);
    v().fsync(f);
    EXPECT_EQ(v().status().dirty_chunks, 0U);
}

}  // namespace
}  // namespace telega::vfs::test
