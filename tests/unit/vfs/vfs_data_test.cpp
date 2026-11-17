// Vfs: пути данных — запись и чтение через границы чанков, разреженные файлы, RMW, truncate,
// потоковая заливка, классы хранения, изменения во время заливки, unlink открытого файла.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

#include "common/files.h"
#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

using store::FaultAction;
using store::FaultRule;
using store::StoreOp;

template <typename P>
bool wait_until(P pred, milliseconds timeout = milliseconds(10'000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(milliseconds(1));
    }
    return true;
}

Bytes slice(const Bytes& b, size_t off, size_t len) {
    return {b.begin() + static_cast<std::ptrdiff_t>(off),
            b.begin() + static_cast<std::ptrdiff_t>(off + len)};
}

// Модель содержимого файла: запись с расширением нулями.
void model_write(Bytes& m, size_t off, const Bytes& data) {
    if (m.size() < off + data.size()) {
        m.resize(off + data.size(), std::byte{0});
    }
    std::copy(data.begin(), data.end(), m.begin() + static_cast<std::ptrdiff_t>(off));
}

class VfsData : public ::testing::Test {
  protected:
    void SetUp() override { env.start(); }
    [[nodiscard]] Vfs& v() const { return *env.vfs; }
    [[nodiscard]] size_t messages() const { return env.store->list_messages().size(); }

    VfsEnv env;
};

TEST_F(VfsData, WriteReadAcrossChunkBoundaries) {
    const int64_t f = env.mkfile("f");
    Bytes model;
    const Bytes a = pattern(kChunk + 1000, 1);
    env.write(f, kChunk - 500, a);  // позиции 0, 1, 2
    model_write(model, kChunk - 500, a);
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_EQ(slice(v().read(f, kChunk - 10, 20), 0, 20), slice(model, kChunk - 10, 20));
    // Чтение за концом.
    EXPECT_TRUE(v().read(f, model.size(), 10).empty());
    EXPECT_EQ(v().read(f, model.size() - 3, 10).size(), 3U);

    v().fsync(f);
    EXPECT_EQ(env.extents(f).size(), 3U);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_GE(env.committer.commits.load(), 1U);
    EXPECT_EQ(env.committer.uploads.load(), 3U);
    // Холодное чтение после перезапуска (пустой кэш).
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsData, SparseFile) {
    const int64_t f = env.mkfile("f");
    const Bytes tail = pattern(100, 2);
    env.write(f, 3 * kChunk + kChunk / 2, tail);
    EXPECT_EQ(v().getattr(f).size, 3 * kChunk + kChunk / 2 + 100);
    v().fsync(f);
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 1U);
    EXPECT_EQ(exts[0].idx, 3);
    EXPECT_EQ(exts[0].len, kChunk / 2 + 100);
    env.restart();
    const Bytes all = env.read_all(f);
    ASSERT_EQ(all.size(), static_cast<size_t>(3 * kChunk + kChunk / 2 + 100));
    EXPECT_TRUE(
        std::all_of(all.begin(), all.end() - 100, [](std::byte b) { return b == std::byte{0}; }));
    EXPECT_EQ(slice(all, all.size() - 100, 100), tail);
}

TEST_F(VfsData, OverwriteMiddleAfterUploadDoesReadModifyWrite) {
    const int64_t f = env.mkfile("f");
    Bytes model = pattern(2 * kChunk + kChunk / 2, 3);
    env.write(f, 0, model);
    v().fsync(f);
    const auto before = env.extents(f);
    ASSERT_EQ(before.size(), 3U);
    env.restart();  // без кэша: RMW действительно скачивает чанк
    const Bytes patch = pattern(10, 4);
    env.write(f, kChunk + kChunk / 2, patch);
    model_write(model, kChunk + kChunk / 2, patch);
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_EQ(env.staging_files(), 1U);
    env.vfs->fsync(f);
    const auto after = env.extents(f);
    ASSERT_EQ(after.size(), 3U);
    EXPECT_EQ(after[0].chunk_id, before[0].chunk_id);
    EXPECT_NE(after[1].chunk_id, before[1].chunk_id);
    EXPECT_EQ(after[1].len, kChunk);
    EXPECT_EQ(after[2].chunk_id, before[2].chunk_id);
    EXPECT_EQ(env.live_chunks(), 3);
    EXPECT_TRUE(env.accounting_ok());
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
}

TEST_F(VfsData, WholePositionOverwriteSkipsDownload) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, pattern(kChunk, 5));
    v().fsync(f);
    env.restart();
    const uint64_t reads = env.store->call_count(StoreOp::read_range);
    const Bytes repl = pattern(kChunk, 6);
    env.write(f, 0, repl);
    EXPECT_EQ(env.store->call_count(StoreOp::read_range), reads);
    EXPECT_EQ(env.read_all(f), repl);
}

TEST_F(VfsData, TruncateUploadedFileDownAndUp) {
    const int64_t f = env.mkfile("f");
    Bytes model = pattern(2 * kChunk + 300, 7);
    env.write(f, 0, model);
    v().fsync(f);
    SetAttr s;
    s.size = kChunk + 200;
    v().setattr(f, s);
    model.resize(kChunk + 200);
    // Без грязных чанков размер идёт прямо в inodes, экстенты обрезаны.
    EXPECT_EQ(env.must_inode(f).size, kChunk + 200);
    auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 2U);
    EXPECT_EQ(exts[1].len, 200);
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_EQ(env.live_chunks(), 2);
    // Обратное расширение: хвост — нули.
    s.size = 3 * kChunk;
    v().setattr(f, s);
    model.resize(3 * kChunk, std::byte{0});
    EXPECT_EQ(env.read_all(f), model);
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsData, TruncateDirtyFileDownAndUp) {
    const int64_t f = env.mkfile("f");
    Bytes model = pattern(2 * kChunk + 300, 8);
    // Позиции пишутся с конца: без потоковой заливки все три остаются грязными.
    for (int64_t idx = 2; idx >= 0; --idx) {
        const auto off = static_cast<size_t>(idx * kChunk);
        env.write(f, off, slice(model, off, std::min<size_t>(kChunk, model.size() - off)));
    }
    EXPECT_EQ(env.staging_files(), 3U);
    EXPECT_EQ(env.committer.uploads.load(), 0U);
    SetAttr s;
    s.size = kChunk / 2;
    v().setattr(f, s);
    model.resize(kChunk / 2);
    EXPECT_EQ(env.staging_files(), 1U);
    EXPECT_EQ(std::filesystem::file_size(env.staging() / (std::to_string(f) + "-0")),
              static_cast<uintmax_t>(kChunk / 2));
    EXPECT_EQ(env.read_all(f), model);
    s.size = 2 * kChunk;
    v().setattr(f, s);
    model.resize(2 * kChunk, std::byte{0});
    EXPECT_EQ(env.read_all(f), model);
    // Размер грязного файла — в dirty_attrs, не в inodes.
    EXPECT_EQ(env.must_inode(f).size, 0);
    v().fsync(f);
    EXPECT_EQ(env.must_inode(f).size, 2 * kChunk);
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 1U);
    EXPECT_EQ(exts[0].len, kChunk / 2);
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
}

TEST_F(VfsData, TruncateMixedDirtyAndUploaded) {
    const int64_t f = env.mkfile("f");
    Bytes model = pattern(3 * kChunk, 9);
    env.write(f, 0, model);
    v().fsync(f);
    const Bytes p = pattern(100, 10);
    env.write(f, 2 * kChunk + 50, p);  // грязная позиция 2 (RMW)
    model_write(model, 2 * kChunk + 50, p);
    SetAttr s;
    s.size = kChunk + 10;  // позиция 2 уходит, 1 обрезается в индексе
    v().setattr(f, s);
    model.resize(kChunk + 10);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(env.read_all(f), model);
    s.size = 3 * kChunk;
    v().setattr(f, s);
    model.resize(3 * kChunk, std::byte{0});
    EXPECT_EQ(env.read_all(f), model);
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsData, OTruncFreesChunks) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, pattern(kChunk + 5, 11));
    v().fsync(f);
    EXPECT_EQ(env.live_chunks(), 2);
    const uint64_t fh = v().open(f, O_WRONLY | O_TRUNC);
    EXPECT_EQ(v().getattr(f).size, 0);
    EXPECT_EQ(env.live_chunks(), 0);
    env.write(f, 0, bytes_of("fresh"));
    v().release(fh);
    v().flush_all();
    EXPECT_EQ(str_of(env.read_all(f)), "fresh");
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsData, StreamingUploadOfLargeFile) {
    const Created c = v().create(kRoot, bytes_of("big"), 0644, O_WRONLY, 1000, 1000);
    const int64_t f = c.attr.ino;
    const size_t total = 5 * kChunk + kChunk / 3;
    const Bytes data = pattern(total, 12);
    constexpr size_t kWrite = 128 << 10;
    for (size_t off = 0; off < total; off += kWrite) {
        const size_t n = std::min(kWrite, total - off);
        env.write(f, off, slice(data, off, n));
    }
    // Пять полных позиций уходят, пока файл ещё открыт.
    EXPECT_TRUE(wait_until([&] { return env.committer.uploads.load() >= 5; }));
    EXPECT_EQ(env.staging_files(), 1U);
    v().release(c.fh);  // последняя позиция — асинхронно
    EXPECT_TRUE(wait_until([&] { return env.staging_files() == 0; }));
    v().flush_all();
    EXPECT_EQ(env.extents(f).size(), 6U);
    EXPECT_EQ(env.must_inode(f).size, static_cast<int64_t>(total));
    env.restart();
    EXPECT_EQ(env.read_all(f), data);
}

TEST_F(VfsData, SealDelayTimerUploads) {
    VfsOptions o = fast_options();
    o.seal_delay = milliseconds(20);
    env.restart(o);
    const int64_t f = env.mkfile("f");
    const Created keep = v().create(kRoot, bytes_of("open"), 0644, O_RDWR, 0, 0);
    env.write(keep.attr.ino, 0, pattern(10'000, 13));
    EXPECT_TRUE(wait_until([&] { return env.staging_files() == 0; }));
    EXPECT_EQ(env.extents(keep.attr.ino).size(), 1U);
    (void)f;
    v().release(keep.fh);
}

TEST_F(VfsData, UnlinkOpenFileKeepsDataUntilRelease) {
    const Created c = v().create(kRoot, bytes_of("f"), 0644, O_RDWR, 0, 0);
    const int64_t f = c.attr.ino;
    const Bytes data = pattern(kChunk + 100, 14);
    env.write(f, 0, data);
    v().fsync(f);
    env.write(f, 10, bytes_of("dirty"));  // ещё и грязная позиция
    Bytes model = data;
    model_write(model, 10, bytes_of("dirty"));
    v().unlink(kRoot, bytes_of("f"));
    EXPECT_EQ(err_of([&] { v().lookup(kRoot, bytes_of("f")); }), ENOENT);
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_EQ(v().getattr(f).nlink, 0U);
    env.write(f, 0, bytes_of("x"));  // запись в удалённый открытый файл работает
    model_write(model, 0, bytes_of("x"));
    EXPECT_EQ(env.read_all(f), model);
    v().flush_all();  // удалённый файл не заливается
    EXPECT_EQ(env.staging_files(), 1U);
    EXPECT_EQ(env.live_chunks(), 2);
    v().release(c.fh);
    EXPECT_TRUE(env.inode(f));  // lookup ядра ещё жив
    v().forget(f, 1);
    EXPECT_FALSE(env.inode(f));
    EXPECT_EQ(env.live_chunks(), 0);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
    EXPECT_TRUE(env.state->list_dirty_attrs().empty());
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsData, UnlinkFileWithUnuploadedChunks) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, pattern(2 * kChunk, 15));
    EXPECT_EQ(env.staging_files(), 2U);
    v().forget(f, 1);
    v().unlink(kRoot, bytes_of("f"));
    EXPECT_FALSE(env.inode(f));
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
    EXPECT_TRUE(env.state->list_dirty_attrs().empty());
    v().flush_all();
    // Загрузка, начатая до unlink, может ещё идти: её сообщение удаляется сразу после put
    // (discard_upload), уже без участия flush_all. Ждём, а не сверяем мгновенно.
    EXPECT_TRUE(wait_until([&] { return messages() == 0; }));
}

TEST_F(VfsData, WriteDuringUploadKeepsChunkDirty) {
    const Created c = v().create(kRoot, bytes_of("f"), 0644, O_RDWR, 0, 0);
    const int64_t f = c.attr.ino;
    Bytes model = pattern(kChunk / 2, 16);
    env.write(f, 0, model);
    env.store->set_faults(
        {FaultRule{StoreOp::put_chunk, 1, FaultAction::delay, milliseconds(200)}});
    v().release(c.fh);  // асинхронное запечатывание
    ASSERT_TRUE(wait_until([&] { return env.store->call_count(StoreOp::put_chunk) >= 1; }));
    const Bytes p = pattern(1000, 17);
    env.write(f, 5000, p);  // во время put_chunk: version растёт
    model_write(model, 5000, p);
    ASSERT_TRUE(wait_until([&] { return env.committer.uploads.load() >= 1; }));
    // Первая заливка применена, но чанк остался грязным: читаем новое.
    EXPECT_EQ(env.extents(f).size(), 1U);
    EXPECT_EQ(env.staging_files(), 1U);
    EXPECT_EQ(env.read_all(f), model);
    v().flush_all();
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(env.live_chunks(), 1);
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsData, UnlinkDuringUploadDiscardsChunk) {
    const Created c = v().create(kRoot, bytes_of("f"), 0644, O_RDWR, 0, 0);
    const int64_t f = c.attr.ino;
    env.write(f, 0, pattern(kChunk / 2, 18));
    env.store->set_faults(
        {FaultRule{StoreOp::put_chunk, 1, FaultAction::delay, milliseconds(200)}});
    v().release(c.fh);
    ASSERT_TRUE(wait_until([&] { return env.store->call_count(StoreOp::put_chunk) >= 1; }));
    v().forget(f, 1);
    v().unlink(kRoot, bytes_of("f"));
    // Залитое сообщение удаляется, в индекс ничего не попадает.
    ASSERT_TRUE(wait_until([&] { return messages() == 0 && env.state->list_uploads().empty(); }));
    EXPECT_EQ(env.committer.uploads.load(), 0U);
    EXPECT_FALSE(env.inode(f));
    EXPECT_EQ(env.live_chunks(), 0);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsData, TruncateDuringUploadClampsExtent) {
    const Created c = v().create(kRoot, bytes_of("f"), 0644, O_RDWR, 0, 0);
    const int64_t f = c.attr.ino;
    Bytes model = pattern(kChunk, 19);
    env.write(f, 0, model);
    env.store->set_faults(
        {FaultRule{StoreOp::put_chunk, 1, FaultAction::delay, milliseconds(200)}});
    v().release(c.fh);
    ASSERT_TRUE(wait_until([&] { return env.store->call_count(StoreOp::put_chunk) >= 1; }));
    SetAttr s;
    s.size = 1000;
    v().setattr(f, s);  // позиция 0 обрезана в staging
    s.size = kChunk;
    v().setattr(f, s);  // и снова расширена: хвост — нули
    model.resize(1000);
    model.resize(kChunk, std::byte{0});
    ASSERT_TRUE(wait_until([&] { return env.committer.uploads.load() >= 1; }));
    // Снимок до обрезки применён не длиннее обрезки.
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 1U);
    EXPECT_LE(exts[0].len, 1000);
    EXPECT_EQ(env.read_all(f), model);
    v().flush_all();
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
}

// --- восстановление после перезапуска ---

TEST_F(VfsData, RestartPicksUpAndUploadsOverlay) {
    const int64_t f = env.mkfile("f");
    const Bytes data = pattern(kChunk + 1234, 26);
    for (int64_t idx = 1; idx >= 0; --idx) {  // без потоковой заливки
        const auto off = static_cast<size_t>(idx * kChunk);
        env.write(f, off, slice(data, off, std::min<size_t>(kChunk, data.size() - off)));
    }
    ASSERT_EQ(env.staging_files(), 2U);
    // Мусор прошлого сеанса: staging-файл без строки и недолитый файл чанка в tmp/.
    { const Fd stray = Fd::open(env.staging() / "999-0", O_WRONLY | O_CREAT, 0600); }
    { const Fd stray = Fd::open(env.tmp() / "chunk-77", O_WRONLY | O_CREAT, 0600); }
    env.restart();
    // Дозаливка overlay начинается сама, без release/fsync.
    EXPECT_TRUE(wait_until([&] { return env.staging_files() == 0; }));
    EXPECT_FALSE(std::filesystem::exists(env.tmp() / "chunk-77"));
    v().flush_all();
    EXPECT_EQ(env.extents(f).size(), 2U);
    EXPECT_EQ(env.must_inode(f).size, static_cast<int64_t>(data.size()));
    EXPECT_TRUE(env.state->list_dirty_attrs().empty());
    EXPECT_EQ(env.read_all(f), data);
}

TEST_F(VfsData, RestartRepairsHalfAppliedState) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, pattern(3 * kChunk, 27));
    v().fsync(f);
    env.vfs.reset();
    // Сбой после транзакции индекса, но до уборки dirty_attrs: строка атрибутов без чанков.
    env.state->set_dirty_attrs(meta::DirtyAttrs{f, kChunk + 10, 9'000'000'000});
    // И overlay удалённого из индекса файла.
    env.state->upsert_dirty_chunk(4242, 0, "4242-0", 1);
    env.state->set_dirty_attrs(meta::DirtyAttrs{4242, 10, 1});
    { const Fd stray = Fd::open(env.staging() / "4242-0", O_WRONLY | O_CREAT, 0600); }
    env.restart();
    // Атрибуты перенесены в индекс, экстенты за размером обрезаны (SPEC 6.2).
    EXPECT_EQ(env.must_inode(f).size, kChunk + 10);
    EXPECT_EQ(env.must_inode(f).mtime_ns, 9'000'000'000);
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 2U);
    EXPECT_EQ(exts[1].len, 10);
    EXPECT_TRUE(env.state->list_dirty_attrs().empty());
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(env.read_all(f), slice(pattern(3 * kChunk, 27), 0, kChunk + 10));
    EXPECT_TRUE(env.accounting_ok());
}

// --- классы хранения (SPEC 6.5) ---

TEST_F(VfsData, SmallFileBecomesInline) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, bytes_of("tiny"));
    v().fsync(f);
    EXPECT_EQ(str_of(env.inline_data(f).value_or(Bytes{})), "tiny");
    EXPECT_TRUE(env.extents(f).empty());
    EXPECT_EQ(messages(), 0U);  // ничего не загружалось
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(env.must_inode(f).size, 4);
    env.restart();
    EXPECT_EQ(str_of(env.read_all(f)), "tiny");
}

TEST_F(VfsData, InlineToPositionalAndBack) {
    const int64_t f = env.mkfile("f");
    Bytes model = pattern(3000, 20);
    env.write(f, 0, model);
    v().fsync(f);
    ASSERT_TRUE(env.inline_data(f));
    // Запись в инлайн-файл: частичная перезапись (копия инлайна в staging).
    env.write(f, 100, bytes_of("patch"));
    model_write(model, 100, bytes_of("patch"));
    EXPECT_EQ(env.read_all(f), model);
    // Рост за inline_max — позиционный.
    const Bytes more = pattern(5000, 21);
    env.write(f, 3000, more);
    model_write(model, 3000, more);
    v().fsync(f);
    EXPECT_FALSE(env.inline_data(f));
    EXPECT_EQ(env.extents(f).size(), 1U);
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_EQ(env.meta->read([](meta::Db& db) { return meta::inline_total(db); }), 0);
    // Обратно: усечь и переписать — снова инлайн, чанк освобождён.
    SetAttr s;
    s.size = 10;
    v().setattr(f, s);
    model.resize(10);
    env.write(f, 0, bytes_of("01234"));
    model_write(model, 0, bytes_of("01234"));
    v().fsync(f);
    EXPECT_EQ(env.inline_data(f).value_or(Bytes{}), model);
    EXPECT_TRUE(env.extents(f).empty());
    EXPECT_EQ(env.live_chunks(), 0);
    EXPECT_TRUE(env.accounting_ok());
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
}

TEST_F(VfsData, WriteBeyondFirstPositionOfInlineFile) {
    const int64_t f = env.mkfile("f");
    Bytes model = bytes_of("inline-head");
    env.write(f, 0, model);
    v().fsync(f);
    ASSERT_TRUE(env.inline_data(f));
    const Bytes far = pattern(100, 22);
    env.write(f, 2 * kChunk, far);  // позиция 0 переезжает в overlay
    model_write(model, 2 * kChunk, far);
    EXPECT_EQ(env.read_all(f), model);
    v().fsync(f);
    EXPECT_FALSE(env.inline_data(f));
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 2U);
    EXPECT_EQ(exts[0].idx, 0);
    EXPECT_EQ(exts[1].idx, 2);
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
}

TEST_F(VfsData, InlineTruncateCutsInlineData) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, bytes_of("0123456789"));
    v().fsync(f);
    SetAttr s;
    s.size = 4;
    v().setattr(f, s);
    EXPECT_EQ(str_of(env.inline_data(f).value_or(Bytes{})), "0123");
    s.size = 8;
    v().setattr(f, s);
    Bytes expect = bytes_of("0123");
    expect.resize(8, std::byte{0});
    EXPECT_EQ(env.read_all(f), expect);
}

// Находка 1: экстент позиции ≥ 1 инлайн-файла не применяется раньше позиции 0. Иначе индекс
// (и коммит в этом окне) остался бы без начала файла: ни inline_data, ни экстента 0.
TEST_F(VfsData, InlineFileFarWriteKeepsStartOfFileInIndexUntilPosition0) {
    const Created c = v().create(kRoot, bytes_of("f"), 0644, O_RDWR, 0, 0);
    const int64_t f = c.attr.ino;
    ASSERT_EQ(v().write(f, 0, bytes_of("abc")), 3U);
    v().fsync(f);
    ASSERT_TRUE(env.inline_data(f));
    ASSERT_EQ(v().write(f, kChunk, bytes_of("tail")), 4U);
    // Первый put_chunk (позиция 0, она в очереди первой) задерживается, остальные идут.
    env.store->set_faults(
        {FaultRule{StoreOp::put_chunk, 1, FaultAction::delay, milliseconds(500)}});
    v().release(c.fh);
    ASSERT_TRUE(wait_until([&] { return env.store->call_count(StoreOp::put_chunk) >= 1; }));
    // За время задержки позиция 1 успела бы примениться: проверяем инвариант всё это время.
    const auto until = std::chrono::steady_clock::now() + milliseconds(300);
    while (std::chrono::steady_clock::now() < until) {
        const bool has_inline = env.inline_data(f).has_value();
        const auto exts = env.extents(f);
        const bool has_ext0 = !exts.empty() && exts.front().idx == 0;
        ASSERT_TRUE(has_inline || has_ext0)
            << "index lost the first bytes of the file until position 0 is applied";
        std::this_thread::sleep_for(milliseconds(5));
    }
    v().flush_all();
    EXPECT_FALSE(env.inline_data(f));
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 2U);
    EXPECT_EQ(exts[0].idx, 0);
    EXPECT_EQ(exts[1].idx, 1);
    Bytes model = bytes_of("abc");
    model_write(model, kChunk, bytes_of("tail"));
    EXPECT_EQ(env.read_all(f), model);
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_TRUE(env.accounting_ok());
}

// Находка 2: если staging-файла нет, строка dirty_chunks уходит, а закоммиченные данные
// позиции остаются читаемыми из индекса (а не нулями из заново созданного пустого файла).
TEST_F(VfsData, MissingStagingFileFallsBackToIndex) {
    const int64_t f = env.mkfile("f");
    const Bytes orig = pattern(100'000, 5);
    env.write(f, 0, orig);
    v().fsync(f);
    ASSERT_EQ(env.extents(f).size(), 1U);
    env.write(f, 10, bytes_of("XYZ"));  // RMW: позиция грязная
    env.vfs.reset();                    // падение
    for (const auto& de : std::filesystem::directory_iterator(env.staging())) {
        std::filesystem::remove(de.path());  // потерянная запись каталога после сбоя питания
    }
    env.restart();
    EXPECT_FALSE(env.vfs->status().last_error.empty()) << "the loss must be visible in status";
    EXPECT_EQ(env.vfs->status().dirty_chunks, 0U);
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
    EXPECT_TRUE(env.state->list_dirty_attrs().empty());
    EXPECT_EQ(env.must_inode(f).size, 100'000);
    env.vfs->flush_all();
    EXPECT_EQ(env.read_all(f), orig);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_TRUE(env.accounting_ok());
}

// Находка 2 (сопутствующее): пустой staging-файл тоже не затирает закоммиченное нулями.
TEST_F(VfsData, EmptyStagingFileKeepsCommittedData) {
    const int64_t f = env.mkfile("f");
    const Bytes orig = pattern(100'000, 6);
    env.write(f, 0, orig);
    v().fsync(f);
    env.write(f, 10, bytes_of("XYZ"));
    env.vfs.reset();
    for (const auto& de : std::filesystem::directory_iterator(env.staging())) {
        std::filesystem::resize_file(de.path(), 0);  // строка пережила данные файла
    }
    env.restart();
    env.vfs->flush_all();
    EXPECT_EQ(env.read_all(f), orig);
    EXPECT_EQ(env.extents(f).size(), 1U);
    EXPECT_EQ(env.store->call_count(StoreOp::put_chunk), 1U) << "nothing is uploaded again";
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
}

// N1: у инлайн-файла с непустым inline_data записаны позиции 0 и 1, а staging позиции 0 после
// сбоя пропал или пуст. Позиция 0 заново создаётся из inline_data (SPEC 8.1): иначе позиция 1
// застревает навсегда (inline_conflict), а fsync вечно даёт EIO.
class VfsInlineFarWriteLoss : public VfsData {
  protected:
    // Инлайн-файл «abc», затем запись за первую позицию. Vfs «падает», staging позиции 0
    // портится (loss), Vfs стартует заново.
    template <typename Loss>
    void crash_with_lost_position0(Loss loss) {
        f = env.mkfile("f");
        env.write(f, 0, bytes_of("abc"));
        v().fsync(f);
        ASSERT_TRUE(env.inline_data(f));
        env.write(f, kChunk + 5, bytes_of("tail"));
        model = bytes_of("abc");
        model_write(model, kChunk + 5, bytes_of("tail"));
        env.vfs.reset();
        loss(env.staging() / (std::to_string(f) + "-0"));
        env.restart();
    }

    void expect_consistent_after_fsync() {
        ASSERT_NO_THROW(v().fsync(f)) << "position 1 is stuck behind the lost position 0";
        EXPECT_EQ(env.read_all(f), model);
        EXPECT_FALSE(env.inline_data(f));
        const auto exts = env.extents(f);
        ASSERT_EQ(exts.size(), 2U);
        EXPECT_EQ(exts[0].idx, 0);
        EXPECT_EQ(exts[1].idx, 1);
        EXPECT_EQ(env.must_inode(f).size, kChunk + 9);
        EXPECT_EQ(env.staging_files(), 0U);
        EXPECT_TRUE(env.state->list_dirty_chunks().empty());
        EXPECT_TRUE(env.state->list_dirty_attrs().empty());
        EXPECT_EQ(env.store->call_count(StoreOp::put_chunk), 2U)
            << "a chunk was uploaded and deleted again";
        EXPECT_TRUE(env.accounting_ok());
        env.restart();  // индекс без overlay согласован
        EXPECT_EQ(env.read_all(f), model);
    }

    int64_t f = 0;
    Bytes model;
};

TEST_F(VfsInlineFarWriteLoss, LostStagingOfPosition0IsRestoredFromInline) {
    crash_with_lost_position0([](const std::filesystem::path& p) { std::filesystem::remove(p); });
    EXPECT_FALSE(env.vfs->status().last_error.empty()) << "the loss must be visible in status";
    // До заливки позиция 0 снова в overlay, и файл читается целиком.
    EXPECT_EQ(env.read_all(f), model);
    expect_consistent_after_fsync();
}

TEST_F(VfsInlineFarWriteLoss, EmptyStagingOfPosition0IsRestoredFromInline) {
    crash_with_lost_position0(
        [](const std::filesystem::path& p) { std::filesystem::resize_file(p, 0); });
    expect_consistent_after_fsync();
}

// N3: сбой посреди truncate при оставшихся грязных позициях. dirty_attrs уже с новым размером, а
// индекс ещё со старым: recover обязан понизить inodes.size вместе с обрезкой экстентов, не
// дожидаясь заливки последней грязной позиции (иначе коммит в этом окне покажет старый размер с
// обнулённым хвостом).
TEST_F(VfsData, RecoverLowersIndexSizeWhileDirtyPositionsRemain) {
    const int64_t f = env.mkfile("f");
    Bytes model = pattern(2 * kChunk + kChunk / 2, 9);
    env.write(f, 0, model);
    v().fsync(f);
    ASSERT_EQ(env.must_inode(f).size, static_cast<int64_t>(model.size()));
    env.write(f, 10, bytes_of("XYZ"));          // грязная позиция 0
    env.write(f, kChunk + 5, bytes_of("QQQ"));  // грязная позиция 1 (RMW)
    model_write(model, 10, bytes_of("XYZ"));
    model_write(model, kChunk + 5, bytes_of("QQQ"));
    env.vfs.reset();
    // Падение посреди truncate: размер в dirty_attrs уже новый, индекс и staging — нет.
    const int64_t new_size = kChunk + 100;
    env.state->set_dirty_attrs(meta::DirtyAttrs{f, new_size, 5});
    model.resize(static_cast<size_t>(new_size));
    // Заливка не доходит: позиции остаются грязными, пока идёт проверка индекса.
    env.store->set_faults(
        {FaultRule{StoreOp::put_chunk, std::nullopt, FaultAction::permanent, milliseconds(0)}});
    env.restart();
    EXPECT_EQ(env.vfs->status().dirty_chunks, 2U);
    EXPECT_EQ(env.must_inode(f).size, new_size) << "the index keeps the old larger size";
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 2U);
    EXPECT_EQ(exts[1].idx, 1);
    EXPECT_EQ(exts[1].len, 100);
    EXPECT_EQ(env.read_all(f), model);
    env.store->set_faults({});
    v().fsync(f);
    EXPECT_EQ(env.must_inode(f).size, new_size);
    EXPECT_EQ(env.read_all(f), model);
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_TRUE(env.accounting_ok());
}

// Находка 6: чанк из одних нулей (пустой staging после усечения инлайн-файла) не заливается,
// позиция — дыра.
TEST_F(VfsData, EmptyInlineThenFarWriteUploadsNoZeroChunk) {
    const Created c = v().create(kRoot, bytes_of("f"), 0644, O_RDWR, 0, 0);
    const int64_t f = c.attr.ino;
    ASSERT_EQ(v().write(f, 0, bytes_of("abc")), 3U);
    v().fsync(f);
    SetAttr s;
    s.size = 0;
    v().setattr(f, s);
    ASSERT_EQ(v().write(f, kChunk + 10, bytes_of("tail")), 4U);
    v().fsync(f);
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 1U) << "position 0 of pure zeros was uploaded";
    EXPECT_EQ(exts[0].idx, 1);
    EXPECT_FALSE(env.inline_data(f));
    EXPECT_EQ(env.store->call_count(StoreOp::put_chunk), 1U);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(env.must_inode(f).size, kChunk + 14);
    Bytes model(static_cast<size_t>(kChunk) + 10, std::byte{0});
    model_write(model, kChunk + 10, bytes_of("tail"));
    EXPECT_EQ(env.read_all(f), model);
    v().release(c.fh);
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_TRUE(env.accounting_ok());
}

// Находка 5б: усечение грязного файла вниз понижает inodes.size в той же транзакции, где
// обрезаются экстенты: коммит до конца заливки не покажет старый размер с обнулённым хвостом.
TEST_F(VfsData, TruncateDirtyFileLowersIndexSize) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, pattern(100'000, 7));
    v().fsync(f);
    ASSERT_EQ(env.must_inode(f).size, 100'000);
    env.write(f, 10, bytes_of("XYZ"));  // позиция 0 снова грязная
    SetAttr s;
    s.size = 50;
    v().setattr(f, s);
    EXPECT_EQ(env.must_inode(f).size, 50);
    const auto exts = env.extents(f);
    ASSERT_EQ(exts.size(), 1U);
    EXPECT_EQ(exts[0].len, 50);
    EXPECT_EQ(v().getattr(f).size, 50);
}

// Находка 5а: сбой между обрезкой overlay и записью индекса. Запись индекса падает
// (хук грязи MetaStore бросает), Vfs «падает», перезапуск: размер должен быть новый.
TEST_F(VfsData, TruncateCrashAfterOverlayDropKeepsNewSize) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, pattern(100'000, 8));
    v().fsync(f);
    env.write(f, 10, bytes_of("XYZ"));  // грязная позиция 0 и dirty_attrs со старым размером
    env.meta->take_patchset();  // следующая запись индекса снова вызовет хук
    env.meta->set_dirty_hook([] { throw std::runtime_error("injected crash"); });
    SetAttr s;
    s.size = 0;
    EXPECT_NE(err_of([&] { v().setattr(f, s); }), 0);
    env.restart();
    EXPECT_EQ(env.must_inode(f).size, 0);
    EXPECT_TRUE(env.extents(f).empty());
    EXPECT_TRUE(env.read_all(f).empty());
}

class VfsBudget : public ::testing::Test {
  protected:
    VfsBudget() : env(4096, 5000) { env.start(); }
    VfsEnv env;
};

TEST_F(VfsBudget, ExhaustedBudgetGoesPositional) {
    const int64_t a = env.mkfile("a");
    const int64_t b = env.mkfile("b");
    env.write(a, 0, pattern(3000, 23));
    env.vfs->fsync(a);
    EXPECT_TRUE(env.inline_data(a));
    env.write(b, 0, pattern(3000, 24));
    env.vfs->fsync(b);
    EXPECT_FALSE(env.inline_data(b));  // 6000 > 5000
    EXPECT_EQ(env.extents(b).size(), 1U);
    EXPECT_EQ(env.read_all(b), pattern(3000, 24));
    // Перезапись инлайн-файла тем же размером бюджет не тратит повторно.
    env.write(a, 0, pattern(3000, 25));
    env.vfs->fsync(a);
    EXPECT_TRUE(env.inline_data(a));
    EXPECT_EQ(env.meta->read([](meta::Db& db) { return meta::inline_total(db); }), 3000);
    env.restart();
    EXPECT_EQ(env.read_all(a), pattern(3000, 25));
    EXPECT_EQ(env.read_all(b), pattern(3000, 24));
}

}  // namespace
}  // namespace telega::vfs::test
