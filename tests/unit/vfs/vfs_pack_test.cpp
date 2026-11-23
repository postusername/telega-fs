// PackWriter (SPEC 6.5, 8.1): выбор класса хранения, закрытие пакета по каждому условию,
// fsync нескольких файлов одного пакета, мёртвые и изменившиеся объекты, чтение из пакета,
// перезапуск с открытым пакетом, параллельность с позиционными загрузками.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "crypto/chunk.h"
#include "vfs/pack_compact.h"
#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

using store::FaultAction;
using store::FaultRule;
using store::StoreOp;

constexpr int64_t kInlineMax = 4096;
constexpr int64_t kPackThreshold = 1 << 16;  // 64 КиБ
constexpr int64_t kPackTarget = 256 << 10;  // 256 КиБ: пакет закрывается на шестом файле по 50 КБ

// Размеры файлов трёх классов (при inline_max = 4096, pack_threshold = 64 КиБ).
constexpr size_t kInlineSize = 1000;
constexpr size_t kPackSize = 20'000;
constexpr size_t kBigSize = 100'000;

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

enum class Klass : uint8_t { inline_data, pack, positional, none };

const char* name_of(Klass k) {
    switch (k) {
        case Klass::inline_data:
            return "inline";
        case Klass::pack:
            return "pack";
        case Klass::positional:
            return "positional";
        case Klass::none:
            return "none";
    }
    return "?";
}

class VfsPack : public ::testing::Test {
  protected:
    VfsPack() : env(kInlineMax, 16 << 20, kPackThreshold, kPackTarget) {}

    void SetUp() override { env.start(options()); }

    static VfsOptions options() {
        VfsOptions o = fast_options();
        o.pack_max_age = milliseconds(3'600'000);  // по возрасту закрывают только тесты возраста
        return o;
    }
    [[nodiscard]] Vfs& v() const { return *env.vfs; }

    // Файл с данными: create, write, release (release запечатывает файл асинхронно).
    // NOLINTNEXTLINE(modernize-use-nodiscard): тестам часто нужен только сам файл
    int64_t put(const std::string& name, const Bytes& data) const {
        const Created c = v().create(kRoot, bytes_of(name), 0644, O_RDWR, 1000, 1000);
        EXPECT_EQ(v().write(c.attr.ino, 0, data), data.size());
        v().release(c.fh);
        return c.attr.ino;
    }
    // Дождаться, пока в открытом пакете будет ровно n объектов.
    [[nodiscard]] bool wait_open(size_t n) const {
        return wait_until([&] { return v().pack_status().open_objects == n; });
    }
    [[nodiscard]] bool wait_applied(uint64_t packs) const {
        return wait_until([&] {
            const PackStatus st = v().pack_status();
            return st.packs_uploaded >= packs && st.closing_packs == 0;
        });
    }
    [[nodiscard]] Klass klass(int64_t ino) const {
        if (env.inline_data(ino)) {
            return Klass::inline_data;
        }
        const auto exts = env.extents(ino);
        if (exts.empty()) {
            return Klass::none;
        }
        const auto c = chunk_row(exts.front().chunk_id);
        return c.kind == meta::ChunkKind::pack ? Klass::pack : Klass::positional;
    }
    [[nodiscard]] meta::Chunk chunk_row(int64_t chunk_id) const {
        return meta::test::must(
            env.meta->read([&](meta::Db& db) { return meta::get_chunk(db, chunk_id); }));
    }
    [[nodiscard]] meta::Chunk chunk_of(int64_t ino) const {
        return chunk_row(env.extents(ino).at(0).chunk_id);
    }
    // unlink и forget ядра: файл удалён целиком, а не отсоединён как открытый.
    void rm(const std::string& name, int64_t ino) const {
        v().unlink(kRoot, bytes_of(name));
        v().forget(ino, 1);
    }
    void truncate(int64_t ino, int64_t size) const {
        SetAttr s;
        s.size = size;
        v().setattr(ino, s);
    }
    [[nodiscard]] uint64_t puts() const { return env.store->call_count(StoreOp::put_chunk); }

    VfsEnv env;
};

// --- классы хранения (SPEC 6.5) ---

struct Transition {
    size_t from;
    size_t to;
};

class VfsPackTransitions : public VfsPack, public ::testing::WithParamInterface<Transition> {};

Klass klass_of_size(size_t n) {
    if (n <= static_cast<size_t>(kInlineMax)) {
        return Klass::inline_data;
    }
    return n <= static_cast<size_t>(kPackThreshold) ? Klass::pack : Klass::positional;
}

// Все девять переходов между тремя классами одной транзакцией запечатывания: новые данные
// записаны, старые освобождены, учёт чанков сходится, после перезапуска читается то же.
TEST_P(VfsPackTransitions, ClassIsChosenAgainOnEverySeal) {
    const auto [from, to] = GetParam();
    const Bytes a = pattern(from, 1);
    const int64_t f = put("f", a);
    v().fsync(f);
    ASSERT_EQ(klass(f), klass_of_size(from)) << name_of(klass_of_size(from));
    ASSERT_EQ(env.read_all(f), a);
    std::optional<meta::Chunk> old;
    if (!env.extents(f).empty()) {
        old = chunk_of(f);
    }

    // Новое содержимое поверх старого, без truncate(0): экстент прежнего класса меняется самим
    // запечатыванием, а не обнулением файла.
    const Bytes b = pattern(to, 2);
    if (to < from) {
        truncate(f, static_cast<int64_t>(to));
    }
    env.write(f, 0, b);
    v().fsync(f);
    EXPECT_EQ(klass(f), klass_of_size(to));
    EXPECT_EQ(env.read_all(f), b);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_TRUE(env.accounting_ok());
    if (old && klass_of_size(to) != klass_of_size(from)) {
        // Единственный объект ушёл из чанка: он больше не жив.
        const meta::Chunk now_old = chunk_row(old->chunk_id);
        EXPECT_EQ(now_old.refcount, 0);
        EXPECT_EQ(now_old.live_bytes, 0);
        EXPECT_TRUE(now_old.dead_since_gen.has_value());
    }
    if (klass_of_size(to) == Klass::inline_data) {
        EXPECT_TRUE(env.extents(f).empty());
    }
    env.restart();
    EXPECT_EQ(env.read_all(f), b);
    EXPECT_TRUE(env.accounting_ok());
}

INSTANTIATE_TEST_SUITE_P(
    AllPairs, VfsPackTransitions,
    ::testing::Values(Transition{kInlineSize, kInlineSize}, Transition{kInlineSize, kPackSize},
                      Transition{kInlineSize, kBigSize}, Transition{kPackSize, kInlineSize},
                      Transition{kPackSize, kPackSize}, Transition{kPackSize, kBigSize},
                      Transition{kBigSize, kInlineSize}, Transition{kBigSize, kPackSize},
                      Transition{kBigSize, kBigSize}),
    [](const auto& param_info) {
        return std::string(name_of(klass_of_size(param_info.param.from))) + "_to_" +
               name_of(klass_of_size(param_info.param.to)) + "_" + std::to_string(param_info.index);
    });

TEST(VfsPackBudget, ExhaustedInlineBudgetSendsFilesToPacks) {
    VfsEnv env(kInlineMax, 8'000, kPackThreshold, kPackTarget);
    VfsOptions o = fast_options();
    o.pack_max_age = milliseconds(3'600'000);
    Vfs& v = env.start(o);
    std::vector<int64_t> inos;
    std::vector<Bytes> data;
    for (uint64_t i = 0; i < 4; ++i) {
        data.push_back(pattern(4'000, i + 1));  // два инлайна по 4000 и бюджет 8000 заполнен
        const Created c = v.create(kRoot, bytes_of("f" + std::to_string(i)), 0644, O_RDWR, 1, 1);
        v.write(c.attr.ino, 0, data.back());
        v.release(c.fh);
        inos.push_back(c.attr.ino);
        v.fsync(c.attr.ino);
    }
    EXPECT_TRUE(env.inline_data(inos[0]));
    EXPECT_TRUE(env.inline_data(inos[1]));
    for (size_t i = 2; i < 4; ++i) {
        EXPECT_FALSE(env.inline_data(inos[i])) << i;
        const auto exts = env.extents(inos[i]);
        ASSERT_EQ(exts.size(), 1U) << i;
        const auto c = meta::test::must(
            env.meta->read([&](meta::Db& db) { return meta::get_chunk(db, exts[0].chunk_id); }));
        EXPECT_EQ(c.kind, meta::ChunkKind::pack) << i;
    }
    // Уже лежащие инлайн-файлы не выселяются.
    EXPECT_TRUE(env.inline_data(inos[0]));
    EXPECT_EQ(env.meta->read([](meta::Db& db) { return meta::inline_total(db); }), 8'000);
    env.restart(o);
    for (size_t i = 0; i < inos.size(); ++i) {
        EXPECT_EQ(env.read_all(inos[i]), data[i]) << i;
    }
}

// Файл в пакете, ставший инлайн (после освобождения бюджета), освобождает свой объект.
TEST_F(VfsPack, PackFileShrunkToInlineFreesItsObject) {
    const int64_t a = put("a", pattern(kPackSize, 1));
    const int64_t b = put("b", pattern(kPackSize, 2));
    ASSERT_TRUE(wait_open(2));
    v().fsync(a);
    ASSERT_EQ(klass(a), Klass::pack);
    ASSERT_EQ(klass(b), Klass::pack);
    EXPECT_EQ(chunk_of(a).chunk_id, chunk_of(b).chunk_id);
    const meta::Chunk pack = chunk_of(a);
    EXPECT_EQ(pack.refcount, 2);
    EXPECT_EQ(pack.live_bytes, static_cast<int64_t>(2 * kPackSize));
    truncate(a, 100);
    env.write(a, 0, bytes_of("tiny"));
    v().fsync(a);
    EXPECT_EQ(klass(a), Klass::inline_data);
    const meta::Chunk after = chunk_row(pack.chunk_id);
    EXPECT_EQ(after.refcount, 1);
    EXPECT_EQ(after.live_bytes, static_cast<int64_t>(kPackSize));
    EXPECT_FALSE(after.dead_since_gen.has_value());
    EXPECT_TRUE(env.accounting_ok());
}

// --- закрытие пакета ---

TEST_F(VfsPack, ClosesWhenSizeReachesTarget) {
    std::vector<Bytes> data;
    std::vector<int64_t> inos;
    for (uint64_t i = 0; i < 5; ++i) {  // 5 · 50 000 = 250 000 < 262 144
        data.push_back(pattern(50'000, i + 1));
        inos.push_back(put("f" + std::to_string(i), data.back()));
    }
    ASSERT_TRUE(wait_open(5));
    EXPECT_EQ(v().pack_status().open_bytes, 250'000);
    EXPECT_EQ(puts(), 0U);

    data.push_back(pattern(50'000, 6));  // 300 000 ≥ 262 144: закрытие
    inos.push_back(put("f5", data.back()));
    ASSERT_TRUE(wait_applied(1));
    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(v().pack_status().open_objects, 0U);
    EXPECT_EQ(env.staging_files(), 0U);
    const meta::Chunk c = chunk_of(inos[0]);
    EXPECT_EQ(c.kind, meta::ChunkKind::pack);
    EXPECT_EQ(c.plain_size, 300'000);
    EXPECT_EQ(c.refcount, 6);
    EXPECT_EQ(c.live_bytes, 300'000);
    std::set<int64_t> offs;
    for (const int64_t ino : inos) {
        const auto exts = env.extents(ino);
        ASSERT_EQ(exts.size(), 1U);
        EXPECT_EQ(exts[0].chunk_id, c.chunk_id);
        EXPECT_EQ(exts[0].idx, 0);
        EXPECT_EQ(exts[0].len, 50'000);
        offs.insert(exts[0].chunk_off);
    }
    EXPECT_EQ(offs.size(), 6U);  // у каждого объекта своё смещение
    EXPECT_TRUE(offs.contains(0));
    // Холодное чтение по chunk_off через ChunkReader.
    env.restart(options());
    for (size_t i = 0; i < inos.size(); ++i) {
        EXPECT_EQ(env.read_all(inos[i]), data[i]) << i;
    }
    EXPECT_EQ(v().read(inos[3], 100, 50),
              Bytes(data[3].begin() + 100, data[3].begin() + 150));  // диапазон внутри объекта
    EXPECT_TRUE(env.accounting_ok());
}

// Пакет не больше chunk_size: последний объект, не влезший в чанк, уходит в следующий пакет.
TEST(VfsPackCap, ObjectThatDoesNotFitMovesToTheNextPack) {
    VfsEnv env(kInlineMax, 16 << 20, kPackThreshold, kChunk);  // pack_target = chunk_size = 1 МиБ
    VfsOptions o = fast_options();
    o.pack_max_age = milliseconds(3'600'000);
    Vfs& v = env.start(o);
    std::vector<Bytes> data;
    std::vector<int64_t> inos;
    for (uint64_t i = 0; i < 18; ++i) {  // 18 · 60 000 = 1 080 000 ≥ 1 048 576
        data.push_back(pattern(60'000, i + 1));
        const Created c = v.create(kRoot, bytes_of("f" + std::to_string(i)), 0644, O_RDWR, 1, 1);
        v.write(c.attr.ino, 0, data.back());
        v.release(c.fh);
        inos.push_back(c.attr.ino);
    }
    v.flush_all();
    EXPECT_EQ(env.store->call_count(StoreOp::put_chunk), 2U);
    std::set<int64_t> chunks;
    for (const int64_t ino : inos) {
        const auto exts = env.extents(ino);
        ASSERT_EQ(exts.size(), 1U);
        chunks.insert(exts[0].chunk_id);
    }
    EXPECT_EQ(chunks.size(), 2U);
    for (const int64_t id : chunks) {
        const auto c =
            meta::test::must(env.meta->read([&](meta::Db& db) { return meta::get_chunk(db, id); }));
        EXPECT_LE(c.plain_size, kChunk);
    }
    env.restart(o);
    for (size_t i = 0; i < inos.size(); ++i) {
        EXPECT_EQ(env.read_all(inos[i]), data[i]) << i;
    }
}

TEST(VfsPackAge, ClosesWhenOldestObjectIsOldEnough) {
    VfsEnv env(kInlineMax, 16 << 20, kPackThreshold, kPackTarget);
    VfsOptions o = fast_options();
    o.pack_max_age = milliseconds(50);
    Vfs& v = env.start(o);
    const Bytes data = pattern(kPackSize, 1);
    const Created c = v.create(kRoot, bytes_of("f"), 0644, O_RDWR, 1, 1);
    v.write(c.attr.ino, 0, data);
    v.release(c.fh);
    EXPECT_TRUE(wait_until([&] { return v.pack_status().packs_uploaded == 1; }));
    EXPECT_EQ(env.store->call_count(StoreOp::put_chunk), 1U);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(env.extents(c.attr.ino).size(), 1U);
}

// Возраст пакета — от самого старого объекта: повторное запечатывание файла из открытого пакета
// его не сбрасывает, у нового объекта отсчёт свой.
TEST_F(VfsPack, AgeCountsFromTheOldestObject) {
    const int64_t a = put("a", pattern(kPackSize, 1));
    ASSERT_TRUE(wait_open(1));
    const auto rows1 = env.state->list_open_pack();
    ASSERT_EQ(rows1.size(), 1U);
    const int64_t a_added = rows1[0].added_ns;
    const int64_t b = put("b", pattern(kPackSize, 2));
    ASSERT_TRUE(wait_open(2));
    // Файл a переписан и запечатан снова: version новая, возраст прежний.
    const auto oldv = static_cast<uint64_t>(rows1[0].version);
    const uint64_t fh = v().open(a, O_RDWR);
    v().write(a, 0, bytes_of("changed"));
    v().release(fh);  // release запечатывает файл заново
    ASSERT_TRUE(wait_until([&] {
        for (const auto& r : env.state->list_open_pack()) {
            if (r.ino == a) {
                return static_cast<uint64_t>(r.version) > oldv;
            }
        }
        return false;
    }));
    for (const auto& r : env.state->list_open_pack()) {
        if (r.ino == a) {
            EXPECT_EQ(r.added_ns, a_added);
            EXPECT_EQ(r.len, static_cast<int64_t>(kPackSize));  // перезапись не удлинила файл
        } else {
            EXPECT_EQ(r.ino, b);
            EXPECT_GT(r.added_ns, a_added);
        }
    }
    EXPECT_EQ(v().pack_status().open_objects, 2U);
}

TEST_F(VfsPack, FsyncOfAPackedFileClosesThePackAndWaitsForCommit) {
    std::vector<Bytes> data;
    std::vector<int64_t> inos;
    for (uint64_t i = 0; i < 3; ++i) {
        data.push_back(pattern(kPackSize, i + 1));
        inos.push_back(put("f" + std::to_string(i), data.back()));
    }
    ASSERT_TRUE(wait_open(3));
    const uint64_t commits = env.committer.commits.load();
    v().fsync(inos[1]);
    // fsync вернулся: пакет залит и применён для всех троих, коммит сделан.
    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_GT(env.committer.commits.load(), commits);
    EXPECT_EQ(v().status().dirty_chunks, 0U);
    for (const int64_t ino : inos) {
        EXPECT_EQ(klass(ino), Klass::pack);
    }
    EXPECT_TRUE(env.state->list_open_pack().empty());
    // Последовательный fsync уже закоммиченных файлов нового пакета не создаёт.
    v().fsync(inos[0]);
    v().fsync(inos[2]);
    EXPECT_EQ(puts(), 1U);
}

TEST_F(VfsPack, ShutdownUploadsTheOpenPack) {
    const Bytes a = pattern(kPackSize, 1);
    const Bytes b = pattern(kPackSize, 2);
    const int64_t fa = put("a", a);
    const int64_t fb = put("b", b);
    ASSERT_TRUE(wait_open(2));
    v().shutdown();
    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(klass(fa), Klass::pack);
    EXPECT_EQ(klass(fb), Klass::pack);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_TRUE(env.state->list_open_pack().empty());
    env.restart(options());
    EXPECT_EQ(env.read_all(fa), a);
    EXPECT_EQ(env.read_all(fb), b);
}

// Давление на staging (SPEC 8.3) закрывает открытый пакет публичным методом.
TEST_F(VfsPack, CloseOpenPackForStagingPressure) {
    EXPECT_FALSE(v().close_open_pack());  // пакет пуст
    const Bytes a = pattern(kPackSize, 1);
    const int64_t fa = put("a", a);
    ASSERT_TRUE(wait_open(1));
    EXPECT_TRUE(v().close_open_pack());
    ASSERT_TRUE(wait_applied(1));
    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(klass(fa), Klass::pack);
    EXPECT_FALSE(v().close_open_pack());
}

// --- fsync нескольких файлов одного пакета: одна заливка ---

TEST_F(VfsPack, ConcurrentFsyncsOfOnePackAreOneUpload) {
    constexpr int kFiles = 6;
    std::vector<Bytes> data;
    std::vector<int64_t> inos;
    for (int i = 0; i < kFiles; ++i) {
        data.push_back(pattern(10'000, static_cast<uint64_t>(i) + 1));
        inos.push_back(put("f" + std::to_string(i), data.back()));
    }
    ASSERT_TRUE(wait_open(kFiles));

    // Ворота держат заливку, пока все fsync не войдут: барьер вместо счётчиков под нагрузкой.
    GateStore gate(*env.store);
    env.vfs.reset();
    env.override_store = &gate;
    env.start(options());
    ASSERT_TRUE(wait_open(kFiles));
    gate.close_gate();
    std::atomic<int> done{0};
    std::vector<std::thread> threads;
    threads.reserve(kFiles);
    for (int i = 0; i < kFiles; ++i) {
        threads.emplace_back([&, i] {
            v().fsync(inos[static_cast<size_t>(i)]);
            done.fetch_add(1);
        });
    }
    ASSERT_TRUE(gate.wait_entered(1));
    // Заливка идёт, а fsync остальных стоят в ожидании той же заливки, нового пакета нет.
    EXPECT_TRUE(wait_until([&] { return v().pack_status().closing_packs == 1; }));
    EXPECT_EQ(done.load(), 0);
    gate.open_gate();
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(done.load(), kFiles);
    EXPECT_EQ(gate.entered(), 1);
    EXPECT_EQ(v().pack_status().packs_uploaded, 1U);
    EXPECT_EQ(env.staging_files(), 0U);
    for (int i = 0; i < kFiles; ++i) {
        EXPECT_EQ(env.read_all(inos[static_cast<size_t>(i)]), data[static_cast<size_t>(i)]);
        EXPECT_EQ(klass(inos[static_cast<size_t>(i)]), Klass::pack);
    }
    env.vfs.reset();
    env.override_store = nullptr;
}

// --- мёртвые объекты ---

TEST_F(VfsPack, DeadObjectsAreNotUploaded) {
    const Bytes a = pattern(10'000, 1);
    const Bytes c = pattern(12'000, 3);
    const int64_t fa = put("a", a);
    const int64_t fb = put("b", pattern(11'000, 2));
    const int64_t fc = put("c", c);
    const int64_t fd = put("d", pattern(9'000, 4));
    ASSERT_TRUE(wait_open(4));
    rm("b", fb);
    EXPECT_EQ(v().pack_status().open_objects, 3U);  // мёртвого объекта в открытом пакете нет
    truncate(fd, 0);
    EXPECT_EQ(v().pack_status().open_objects, 2U);
    EXPECT_EQ(env.state->list_open_pack().size(), 2U);
    v().fsync(fa);
    ASSERT_EQ(puts(), 1U);
    const meta::Chunk pack = chunk_of(fa);
    EXPECT_EQ(pack.plain_size, 22'000);  // a и c: залито только живое
    EXPECT_EQ(pack.live_bytes, 22'000);
    EXPECT_EQ(pack.refcount, 2);
    EXPECT_EQ(env.read_all(fc), c);
    (void)fb;
}

TEST_F(VfsPack, ObjectsThatDieAfterTheCloseAreSkippedAtAssembly) {
    // Пакет закрыт, но сборка ещё не началась (probe держит её): файл удаляется и обрезается.
    std::mutex mu;
    std::condition_variable cv;
    bool at_run = false;
    bool go = false;
    VfsOptions o = options();
    o.pack_probe = [&](std::string_view step) {
        if (step != "run") {
            return;
        }
        std::unique_lock lock(mu);
        at_run = true;
        cv.notify_all();
        cv.wait(lock, [&] { return go; });
    };
    env.restart(o);
    const Bytes a = pattern(10'000, 1);
    const int64_t fa = put("a", a);
    const int64_t fb = put("b", pattern(10'000, 2));
    const int64_t fc = put("c", pattern(10'000, 3));
    ASSERT_TRUE(wait_open(3));
    ASSERT_TRUE(v().close_open_pack());
    {
        std::unique_lock lock(mu);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] { return at_run; }));
    }
    rm("b", fb);
    truncate(fc, 0);
    {
        const std::lock_guard lock(mu);
        go = true;
    }
    cv.notify_all();
    ASSERT_TRUE(wait_applied(1));
    ASSERT_EQ(puts(), 1U);
    EXPECT_EQ(chunk_of(fa).plain_size, 10'000);
    EXPECT_EQ(chunk_of(fa).refcount, 1);
    EXPECT_EQ(env.read_all(fa), a);
    EXPECT_EQ(env.must_inode(fc).size, 0);
    (void)fb;
}

TEST_F(VfsPack, PackOfOnlyDeadObjectsIsNotUploaded) {
    const int64_t fa = put("a", pattern(10'000, 1));
    const int64_t fb = put("b", pattern(10'000, 2));
    ASSERT_TRUE(wait_open(2));
    rm("a", fa);
    rm("b", fb);
    EXPECT_FALSE(v().close_open_pack());
    EXPECT_EQ(puts(), 0U);
    EXPECT_TRUE(env.state->list_open_pack().empty());
    EXPECT_EQ(env.staging_files(), 0U);
}

// --- файл изменился во время заливки его пакета ---

TEST_F(VfsPack, FileChangedDuringUploadIsNotAppliedAndGoesToTheNextPack) {
    GateStore gate(*env.store);
    env.vfs.reset();
    env.override_store = &gate;
    env.start(options());
    const Bytes a1 = pattern(10'000, 1);
    const Bytes b = pattern(10'000, 2);
    const Created ca = v().create(kRoot, bytes_of("a"), 0644, O_RDWR, 1, 1);
    v().write(ca.attr.ino, 0, a1);
    const int64_t fb = put("b", b);
    v().release(ca.fh);
    ASSERT_TRUE(wait_open(2));
    gate.close_gate();
    ASSERT_TRUE(v().close_open_pack());
    ASSERT_TRUE(gate.wait_entered(1));  // пакет собран и стоит в put_chunk

    // Файл a меняется, пока заливается пакет с его прежней версией.
    const Bytes a2 = pattern(10'000, 9);
    const uint64_t fh = v().open(ca.attr.ino, O_RDWR);
    v().write(ca.attr.ino, 0, a2);
    gate.open_gate();
    ASSERT_TRUE(wait_applied(1));
    v().release(fh);

    // Пакет применён только для b; байты a в нём мёртвые с самого начала.
    EXPECT_EQ(klass(fb), Klass::pack);
    EXPECT_EQ(klass(ca.attr.ino), Klass::none);
    const meta::Chunk first = chunk_of(fb);
    EXPECT_EQ(first.plain_size, 20'000);
    EXPECT_EQ(first.live_bytes, 10'000);
    EXPECT_EQ(first.refcount, 1);
    EXPECT_EQ(env.read_all(ca.attr.ino), a2);  // новая версия читается из overlay

    // Новая версия a уходит в следующий пакет.
    v().fsync(ca.attr.ino);
    EXPECT_EQ(gate.entered(), 2);
    EXPECT_EQ(klass(ca.attr.ino), Klass::pack);
    const meta::Chunk second = chunk_of(ca.attr.ino);
    EXPECT_NE(second.chunk_id, first.chunk_id);
    EXPECT_EQ(second.plain_size, 10'000);
    EXPECT_EQ(chunk_row(first.chunk_id).live_bytes, 10'000);
    EXPECT_EQ(env.read_all(ca.attr.ino), a2);
    EXPECT_EQ(env.read_all(fb), b);
    EXPECT_TRUE(env.accounting_ok());
    env.vfs.reset();
    env.override_store = nullptr;
}

// fsync файла, который изменился за время заливки явного пакета, не зависает до закрытия по
// возрасту: объект запечатывается заново и уходит следующим пакетом.
TEST_F(VfsPack, FsyncSurvivesAFileChangedWhileItsPackIsUploading) {
    GateStore gate(*env.store);
    env.vfs.reset();
    env.override_store = &gate;
    env.start(options());
    const Created ca = v().create(kRoot, bytes_of("a"), 0644, O_RDWR, 1, 1);
    v().write(ca.attr.ino, 0, pattern(10'000, 1));
    const int64_t fb = put("b", pattern(10'000, 2));
    ASSERT_TRUE(wait_open(1));  // b запечатан, a ещё открыт и не запечатан
    gate.close_gate();
    std::thread syncer([&] { v().fsync(ca.attr.ino); });
    ASSERT_TRUE(gate.wait_entered(1));
    const Bytes a2 = pattern(10'000, 7);
    v().write(ca.attr.ino, 0, a2);  // изменился, пока пакет в put_chunk
    gate.open_gate();
    syncer.join();
    v().release(ca.fh);
    EXPECT_EQ(gate.entered(), 2);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(klass(ca.attr.ino), Klass::pack);
    EXPECT_EQ(env.read_all(ca.attr.ino), a2);
    env.restart(options());
    EXPECT_EQ(env.read_all(ca.attr.ino), a2);
    EXPECT_EQ(klass(fb), Klass::pack);
    EXPECT_TRUE(env.accounting_ok());
}

// --- чтение, перезапись и перезапуск ---

TEST_F(VfsPack, PartialRewriteOfAPackedFileGoesThroughReadModifyWrite) {
    const Bytes a = pattern(30'000, 1);
    const Bytes b = pattern(30'000, 2);
    const int64_t fa = put("a", a);
    const int64_t fb = put("b", b);
    ASSERT_TRUE(wait_open(2));
    v().fsync(fa);
    const meta::Chunk pack = chunk_of(fb);
    ASSERT_GT(env.extents(fb).at(0).chunk_off + env.extents(fa).at(0).chunk_off, 0);
    env.restart(options());  // холодный кэш: RMW читает объект из пакета по chunk_off
    Bytes model = b;
    const Bytes patch = pattern(100, 5);
    std::copy(patch.begin(), patch.end(), model.begin() + 12'345);
    env.write(fb, 12'345, patch);
    EXPECT_EQ(env.read_all(fb), model);
    v().fsync(fb);
    EXPECT_EQ(klass(fb), Klass::pack);
    EXPECT_EQ(env.read_all(fb), model);
    EXPECT_EQ(env.read_all(fa), a);
    EXPECT_NE(chunk_of(fb).chunk_id, pack.chunk_id);
    EXPECT_EQ(chunk_row(pack.chunk_id).live_bytes, 30'000);  // остался только a
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsPack, OpenPackSurvivesARestartWithoutFlush) {
    const Bytes a = pattern(kPackSize, 1);
    const Bytes b = pattern(kPackSize, 2);
    const int64_t fa = put("a", a);
    const int64_t fb = put("b", b);
    ASSERT_TRUE(wait_open(2));
    ASSERT_EQ(env.state->list_open_pack().size(), 2U);
    Vfs& r = env.restart(options());  // падение: данные в overlay, строки open_pack на месте
    EXPECT_EQ(r.pack_status().open_objects, 2U);
    EXPECT_EQ(env.read_all(fa), a);
    EXPECT_EQ(env.read_all(fb), b);
    r.flush_all();
    EXPECT_EQ(puts(), 1U);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_TRUE(env.state->list_open_pack().empty());
    EXPECT_EQ(klass(fa), Klass::pack);
    env.restart(options());
    EXPECT_EQ(env.read_all(fa), a);
    EXPECT_EQ(env.read_all(fb), b);
}

TEST_F(VfsPack, OpenPackRowsWithoutDirtyChunksAreDroppedAtStart) {
    put("a", pattern(kPackSize, 1));
    ASSERT_TRUE(wait_open(1));
    // Строка без данных (сбой между удалением грязного чанка и строки).
    env.vfs.reset();
    env.state->upsert_open_pack({4242, 1, 100, 1});
    EXPECT_EQ(env.state->list_open_pack().size(), 2U);
    const Vfs& r = env.start(options());
    EXPECT_EQ(r.pack_status().open_objects, 1U);
    EXPECT_EQ(env.state->list_open_pack().size(), 1U);
}

TEST_F(VfsPack, UnlinkRemovesTheObjectFromTheOpenPack) {
    const int64_t fa = put("a", pattern(kPackSize, 1));
    const int64_t fb = put("b", pattern(kPackSize, 2));
    ASSERT_TRUE(wait_open(2));
    rm("a", fa);
    EXPECT_EQ(v().pack_status().open_objects, 1U);
    const auto rows = env.state->list_open_pack();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0].ino, fb);
    EXPECT_EQ(v().pack_status().open_bytes, static_cast<int64_t>(kPackSize));
}

// Файл вырос из пакета (стал позиционным до закрытия): строка убирается, в пакет он не попадает.
TEST_F(VfsPack, FileThatOutgrowsThePackLeavesTheOpenPack) {
    const int64_t fa = put("a", pattern(kPackSize, 1));
    const int64_t fb = put("b", pattern(kPackSize, 2));
    ASSERT_TRUE(wait_open(2));
    const Bytes big = pattern(kBigSize, 3);
    const uint64_t fh = v().open(fa, O_RDWR);
    v().write(fa, 0, big);
    v().release(fh);
    ASSERT_TRUE(wait_until([&] { return v().pack_status().open_objects == 1; }));
    v().fsync(fb);
    v().fsync(fa);
    EXPECT_EQ(klass(fa), Klass::positional);
    EXPECT_EQ(klass(fb), Klass::pack);
    EXPECT_EQ(chunk_of(fb).plain_size, static_cast<int64_t>(kPackSize));
    EXPECT_EQ(env.read_all(fa), big);
    EXPECT_TRUE(env.accounting_ok());
}

// --- параллельность ---

// Пакет заливается параллельно с позиционным чанком: оба вызова put_chunk внутри одновременно.
TEST(VfsPackParallel, PackAndPositionalChunkUploadConcurrently) {
    VfsEnv env(kInlineMax, 16 << 20, kPackThreshold, kPackTarget);
    GateStore gate(*env.store);
    env.override_store = &gate;
    VfsOptions o = fast_options();
    o.upload_concurrency = 2;
    o.pack_max_age = milliseconds(3'600'000);
    Vfs& v = env.start(o);
    gate.close_gate();
    const Bytes small = pattern(kPackSize, 1);
    const Bytes big = pattern(kBigSize, 2);
    const Created cs = v.create(kRoot, bytes_of("small"), 0644, O_RDWR, 1, 1);
    v.write(cs.attr.ino, 0, small);
    v.release(cs.fh);
    ASSERT_TRUE(wait_until([&] { return v.pack_status().open_objects == 1; }));
    ASSERT_TRUE(v.close_open_pack());
    ASSERT_TRUE(gate.wait_entered(1));  // пакет внутри put_chunk
    const Created cb = v.create(kRoot, bytes_of("big"), 0644, O_RDWR, 1, 1);
    v.write(cb.attr.ino, 0, big);
    v.release(cb.fh);
    ASSERT_TRUE(gate.wait_entered(2));  // позиционный чанк тоже, пока пакет не вернулся
    const auto sizes = gate.put_sizes();
    ASSERT_EQ(sizes.size(), 2U);
    gate.open_gate();
    v.flush_all();
    EXPECT_EQ(env.read_all(cs.attr.ino), small);
    EXPECT_EQ(env.read_all(cb.attr.ino), big);
    EXPECT_EQ(env.staging_files(), 0U);
}

// Пакет заливается обычным put_chunk, поэтому годится для альбома: размер файла пакета — это
// размер чанка по формату раздела 5.
TEST_F(VfsPack, PackIsAnOrdinaryChunk) {
    GateStore gate(*env.store);
    env.vfs.reset();
    env.override_store = &gate;
    env.start(options());
    put("a", pattern(10'000, 1));
    put("b", pattern(20'000, 2));
    ASSERT_TRUE(wait_open(2));
    v().flush_all();
    const auto sizes = gate.put_sizes();
    ASSERT_EQ(sizes.size(), 1U);
    EXPECT_EQ(sizes[0], 40U + 30'000U + 16U);  // заголовок, открытый текст, один тег сегмента
    env.vfs.reset();
    env.override_store = nullptr;
}

// --- уплотнение пакетов (SPEC 6.5) ---

class VfsCompact : public VfsPack {
  protected:
    // n файлов по 30 000 байт в одном пакете, закоммиченном fsync.
    std::vector<int64_t> make_pack(const std::string& prefix, int n,
                                   std::vector<Bytes>* data = nullptr) {
        std::vector<int64_t> inos;
        for (int i = 0; i < n; ++i) {
            Bytes d = pattern(30'000, static_cast<uint64_t>(i) + 100 + prefix.size() * 1000);
            inos.push_back(put(prefix + std::to_string(i), d));
            if (data != nullptr) {
                data->push_back(std::move(d));
            }
        }
        EXPECT_TRUE(
            wait_until([&] { return v().pack_status().open_objects == static_cast<size_t>(n); }));
        v().fsync(inos[0]);
        return inos;
    }
    [[nodiscard]] CompactContext context(store::Store& st) {
        return CompactContext{*env.meta, *env.state, st,      env.committer,
                              env.keys,  env.tmp(),  nullptr, {}};
    }
    [[nodiscard]] std::array<std::byte, 16> nonce_of(const meta::Chunk& c) const {
        std::ifstream in(env.store->dir() / "msgs" / std::to_string(c.msg_id), std::ios::binary);
        std::array<char, 40> head{};
        in.read(head.data(), 40);
        std::array<std::byte, 16> out{};
        std::memcpy(out.data(), head.data() + 16, 16);
        return out;
    }
    [[nodiscard]] size_t messages() const { return env.store->list_messages().size(); }
    [[nodiscard]] size_t tmp_files() const {
        size_t n = 0;
        for (const auto& de : std::filesystem::directory_iterator(env.tmp())) {
            (void)de;
            ++n;
        }
        return n;
    }
};

TEST_F(VfsCompact, RepacksLiveObjectsIntoANewPackWithANewNonce) {
    std::vector<Bytes> data;
    const auto inos = make_pack("f", 6, &data);
    const meta::Chunk old = chunk_of(inos[3]);
    ASSERT_EQ(old.refcount, 6);
    for (const size_t i : {0U, 1U, 2U, 4U, 5U}) {
        rm("f" + std::to_string(i), inos[i]);
    }
    const meta::Chunk thin = chunk_row(old.chunk_id);
    EXPECT_EQ(thin.refcount, 1);
    EXPECT_EQ(thin.live_bytes, 30'000);
    EXPECT_EQ(thin.plain_size, 180'000);
    const uint64_t commits = env.committer.commits.load();

    const CompactResult r = compact_packs(context(*env.store));
    EXPECT_EQ(r.packs_selected, 1U);
    EXPECT_EQ(r.objects_moved, 1U);
    EXPECT_EQ(r.objects_skipped, 0U);
    EXPECT_EQ(r.bytes_moved, 30'000U);
    EXPECT_EQ(r.new_packs, 1U);
    EXPECT_TRUE(r.committed);
    EXPECT_GT(env.committer.commits.load(), commits);

    const meta::Chunk fresh = chunk_of(inos[3]);
    EXPECT_NE(fresh.chunk_id, old.chunk_id);
    EXPECT_NE(fresh.msg_id, old.msg_id);
    EXPECT_EQ(fresh.kind, meta::ChunkKind::pack);
    EXPECT_EQ(fresh.plain_size, 30'000);
    EXPECT_EQ(fresh.live_bytes, 30'000);
    EXPECT_EQ(fresh.refcount, 1);
    EXPECT_EQ(env.extents(inos[3]).at(0).chunk_off, 0);
    EXPECT_NE(nonce_of(fresh), nonce_of(old));
    // Старый пакет мёртв и уходит в обычную отложенную уборку.
    const meta::Chunk dead = chunk_row(old.chunk_id);
    EXPECT_EQ(dead.refcount, 0);
    EXPECT_EQ(dead.live_bytes, 0);
    EXPECT_TRUE(dead.dead_since_gen.has_value());
    EXPECT_TRUE(env.accounting_ok());
    EXPECT_EQ(tmp_files(), 0U);
    EXPECT_TRUE(env.state->list_uploads().empty());

    env.restart(options());  // холодное чтение из нового пакета
    EXPECT_EQ(env.read_all(inos[3]), data[3]);
}

TEST_F(VfsCompact, PacksAtOrAboveTheRatioAreLeftAlone) {
    const auto inos = make_pack("f", 4);
    for (const size_t i : {0U, 1U, 2U}) {
        rm("f" + std::to_string(i), inos[i]);  // живо 30 000 из 120 000: ровно 0,25
    }
    const uint64_t puts_before = puts();
    CompactResult r = compact_packs(context(*env.store));
    EXPECT_EQ(r.packs_selected, 0U);  // строго меньше порога
    EXPECT_FALSE(r.committed);
    EXPECT_EQ(puts(), puts_before);
    CompactOptions o;
    o.ratio = 0.26;
    r = compact_packs(context(*env.store), o);
    EXPECT_EQ(r.packs_selected, 1U);
    EXPECT_EQ(r.new_packs, 1U);
    EXPECT_EQ(puts(), puts_before + 1);
    // Позиционные чанки не уплотняются, сколько бы в них ни было мёртвого.
    const int64_t big = put("big", pattern(kBigSize, 5));
    v().fsync(big);
    truncate(big, 10);
    EXPECT_EQ(compact_packs(context(*env.store), o).packs_selected, 0U);
}

TEST_F(VfsCompact, SurvivorsOfSeveralPacksAreMergedIntoOne) {
    std::vector<Bytes> da;
    std::vector<Bytes> db;
    const auto a = make_pack("a", 6, &da);
    const auto b = make_pack("b", 6, &db);
    ASSERT_NE(chunk_of(a[0]).chunk_id, chunk_of(b[0]).chunk_id);
    for (size_t i = 0; i < 6; ++i) {
        if (i != 2) {
            rm("a" + std::to_string(i), a[i]);
        }
        if (i != 5) {
            rm("b" + std::to_string(i), b[i]);
        }
    }
    const CompactResult r = compact_packs(context(*env.store));
    EXPECT_EQ(r.packs_selected, 2U);
    EXPECT_EQ(r.objects_moved, 2U);
    EXPECT_EQ(r.new_packs, 1U);
    const meta::Chunk merged = chunk_of(a[2]);
    EXPECT_EQ(chunk_of(b[5]).chunk_id, merged.chunk_id);
    EXPECT_EQ(merged.plain_size, 60'000);
    EXPECT_EQ(merged.refcount, 2);
    EXPECT_NE(env.extents(a[2]).at(0).chunk_off, env.extents(b[5]).at(0).chunk_off);
    env.restart(options());
    EXPECT_EQ(env.read_all(a[2]), da[2]);
    EXPECT_EQ(env.read_all(b[5]), db[5]);
    EXPECT_TRUE(env.accounting_ok());
}

// Конкурентная запись во время перепаковки: экстент, изменившийся с начала перепаковки, не
// переставляется (его байты в новом пакете мёртвые), остальные переставляются.
TEST_F(VfsCompact, ExtentChangedDuringRepackIsNotMoved) {
    GateStore gate(*env.store);
    env.vfs.reset();
    env.override_store = &gate;
    env.start(options());
    std::vector<Bytes> data;
    const auto inos = make_pack("f", 6, &data);
    for (const size_t i : {0U, 1U, 2U, 3U}) {
        rm("f" + std::to_string(i), inos[i]);
    }
    const meta::Chunk old = chunk_of(inos[4]);
    ASSERT_EQ(old.live_bytes, 60'000);
    CompactOptions o;
    o.ratio = 0.5;
    gate.close_gate();
    const int before = gate.entered();
    CompactResult r;
    std::thread compactor([&] { r = compact_packs(context(gate), o); });
    ASSERT_TRUE(gate.wait_entered(before + 1));  // новый пакет собран и стоит в put_chunk
    // Пока он заливается, файл f5 усечён: его экстент изменился.
    truncate(inos[5], 1000);
    gate.open_gate();
    compactor.join();

    EXPECT_EQ(r.objects_moved, 1U);
    EXPECT_EQ(r.objects_skipped, 1U);
    EXPECT_EQ(r.new_packs, 1U);
    const meta::Chunk fresh = chunk_of(inos[4]);
    EXPECT_NE(fresh.chunk_id, old.chunk_id);
    EXPECT_EQ(fresh.plain_size, 60'000);
    EXPECT_EQ(fresh.live_bytes, 30'000);  // перенесён один объект, байты второго мёртвые
    EXPECT_EQ(fresh.refcount, 1);
    // f5 остался в старом пакете с новой длиной.
    EXPECT_EQ(chunk_of(inos[5]).chunk_id, old.chunk_id);
    EXPECT_EQ(env.extents(inos[5]).at(0).len, 1000);
    EXPECT_EQ(chunk_row(old.chunk_id).live_bytes, 1000);
    EXPECT_TRUE(env.accounting_ok());
    EXPECT_EQ(env.read_all(inos[4]), data[4]);
    EXPECT_EQ(env.read_all(inos[5]), Bytes(data[5].begin(), data[5].begin() + 1000));
}

TEST_F(VfsCompact, NothingIsMovedWhenEveryExtentChangedAndTheNewMessageIsDeleted) {
    GateStore gate(*env.store);
    env.vfs.reset();
    env.override_store = &gate;
    env.start(options());
    const auto inos = make_pack("f", 6);
    for (const size_t i : {0U, 1U, 2U, 3U, 4U}) {
        rm("f" + std::to_string(i), inos[i]);
    }
    const size_t msgs = messages();
    gate.close_gate();
    const int before = gate.entered();
    CompactResult r;
    std::thread compactor([&] { r = compact_packs(context(gate)); });
    ASSERT_TRUE(gate.wait_entered(before + 1));
    rm("f5", inos[5]);  // единственный живой объект удалён
    gate.open_gate();
    compactor.join();
    EXPECT_EQ(r.objects_moved, 0U);
    EXPECT_EQ(r.objects_skipped, 1U);
    EXPECT_EQ(r.new_packs, 0U);
    EXPECT_EQ(messages(), msgs);  // залитое сообщение нового пакета убрано
    EXPECT_TRUE(env.state->list_uploads().empty());
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsCompact, TransientUploadErrorsAreRetriedPermanentOnesAbort) {
    const auto inos = make_pack("f", 6);
    for (const size_t i : {0U, 1U, 2U, 3U, 4U}) {
        rm("f" + std::to_string(i), inos[i]);
    }
    const uint64_t base = env.store->call_count(StoreOp::put_chunk);
    env.store->set_faults({FaultRule{StoreOp::put_chunk, base + 1, FaultAction::permanent, {}}});
    const meta::Chunk old = chunk_of(inos[5]);
    CompactOptions o;
    o.retry_initial = milliseconds(1);
    o.retry_max = milliseconds(2);
    EXPECT_THROW((void)compact_packs(context(*env.store), o), store::StoreError);
    EXPECT_EQ(chunk_of(inos[5]).chunk_id, old.chunk_id);  // индекс не тронут
    EXPECT_EQ(tmp_files(), 0U);
    EXPECT_TRUE(env.state->list_uploads().empty());
    EXPECT_TRUE(env.accounting_ok());

    env.store->set_faults({FaultRule{StoreOp::put_chunk, base + 2, FaultAction::transient, {}},
                           FaultRule{StoreOp::put_chunk, base + 3, FaultAction::transient, {}}});
    const CompactResult r = compact_packs(context(*env.store), o);
    EXPECT_EQ(r.new_packs, 1U);
    EXPECT_NE(chunk_of(inos[5]).chunk_id, old.chunk_id);

    // Бесконечные транзиентные ошибки: после max_upload_attempts ошибка выходит наружу.
    const auto more = make_pack("g", 6);
    for (const size_t i : {0U, 1U, 2U, 3U, 4U}) {
        rm("g" + std::to_string(i), more[i]);
    }
    env.store->set_faults(
        {FaultRule{StoreOp::put_chunk, std::nullopt, FaultAction::transient, {}}});
    o.max_upload_attempts = 3;
    EXPECT_THROW((void)compact_packs(context(*env.store), o), store::StoreError);
    EXPECT_EQ(tmp_files(), 0U);
}

// Открывает ворота и останавливает Vfs до уничтожения GateStore: упавшая проверка не должна
// оставить заливку висеть в закрытых воротах (деструктор Vfs ждёт её).
struct GateGuard {
    GateGuard(VfsEnv& e, GateStore& g) : env(e), gate(g) {}
    GateGuard(const GateGuard&) = delete;
    GateGuard& operator=(const GateGuard&) = delete;
    ~GateGuard() {
        gate.open_gate();
        env.vfs.reset();
        env.override_store = nullptr;
    }
    VfsEnv& env;
    GateStore& gate;
};

// O_TRUNC + запись, пока прежний объект файла лежит в закрытом заливаемом пакете: новый объект
// не должен принять версию прежнего (раньше version начиналась с 1 заново), иначе после
// применения пакета finalize удалял строку open_pack нового объекта, и он зависал в overlay
// вне пакетов.
TEST_F(VfsPack, RewriteAfterTruncateIsNotMistakenForTheObjectOfTheClosedPack) {
    GateStore gate(*env.store);
    const GateGuard guard(env, gate);
    env.vfs.reset();
    env.override_store = &gate;
    VfsOptions o = options();
    o.pack_max_age = milliseconds(300);
    env.start(o);
    const Created c = v().create(kRoot, bytes_of("a"), 0644, O_RDWR, 1, 1);
    v().write(c.attr.ino, 0, pattern(10'000, 1));
    v().release(c.fh);
    ASSERT_TRUE(wait_open(1));
    gate.close_gate();
    ASSERT_TRUE(v().close_open_pack());
    ASSERT_TRUE(gate.wait_entered(1));  // пакет с прежним объектом стоит в put_chunk

    truncate(c.attr.ino, 0);
    const Bytes a2 = pattern(10'000, 7);
    const uint64_t fh = v().open(c.attr.ino, O_RDWR);
    v().write(c.attr.ino, 0, a2);
    v().release(fh);  // запечатывание асинхронное: новый объект встаёт в открытый пакет
    ASSERT_TRUE(wait_open(1));
    gate.open_gate();
    // Новый объект уходит вторым пакетом по возрасту, а не висит в overlay.
    EXPECT_TRUE(wait_until([&] { return env.staging_files() == 0; }, milliseconds(10'000)))
        << "the object is stuck in staging: it was dropped from the open pack";
    EXPECT_EQ(klass(c.attr.ino), Klass::pack);
    EXPECT_EQ(env.read_all(c.attr.ino), a2);
    EXPECT_TRUE(env.state->list_open_pack().empty());
    env.restart(options());
    EXPECT_EQ(env.read_all(c.attr.ino), a2);
    EXPECT_TRUE(env.accounting_ok());
}

// Спрос от fsync привязан к файлу (SPEC 6.5): fsync одного файла закрытого пакета не заставляет
// заново запечатывать и сразу заливать другой его файл, изменившийся за время заливки.
TEST_F(VfsPack, FsyncOfOneFileDoesNotForceTheChangedNeighbourOutOfTheOpenPack) {
    GateStore gate(*env.store);
    const GateGuard guard(env, gate);
    env.vfs.reset();
    env.override_store = &gate;
    env.start(options());
    const int64_t fa = put("a", pattern(10'000, 1));
    const Created cb = v().create(kRoot, bytes_of("b"), 0644, O_RDWR, 1, 1);
    v().write(cb.attr.ino, 0, pattern(10'000, 2));
    v().release(cb.fh);
    ASSERT_TRUE(wait_open(2));
    gate.close_gate();
    ASSERT_TRUE(v().close_open_pack());  // закрытие по давлению или таймеру: без спроса
    ASSERT_TRUE(gate.wait_entered(1));
    std::thread syncer([&] { v().fsync(fa); });  // ждёт заливку пакета, спрос только у a
    // b меняется, пока его пакет в put_chunk, и запечатывается в новый открытый пакет.
    const uint64_t fh = v().open(cb.attr.ino, O_RDWR);
    v().write(cb.attr.ino, 0, pattern(10'000, 9));
    v().release(fh);
    ASSERT_TRUE(wait_open(1));
    gate.open_gate();
    syncer.join();
    // Пакет с a применён; b никто не ждёт и до закрытия пакета по возрасту (1 ч) он остаётся
    // открытым: второй put_chunk не должен появиться.
    EXPECT_FALSE(wait_until([&] { return gate.entered() >= 2; }, milliseconds(1'000)))
        << "an unrelated fsync forced the neighbour out of the open pack";
    EXPECT_EQ(v().pack_status().open_objects, 1U);
    EXPECT_EQ(klass(fa), Klass::pack);
}

TEST_F(VfsCompact, StagingFilesOfACompactionAreLeasedFromTheBudget) {
    std::vector<Bytes> data;
    const auto inos = make_pack("f", 6, &data);
    for (const size_t i : {0U, 1U, 2U, 4U, 5U}) {
        rm("f" + std::to_string(i), inos[i]);
    }
    std::atomic<uint64_t> peak{0};
    StagingBudget budget(StagingConfig{64ULL << 20U, 48ULL << 20U, 0},
                         [&](uint64_t used) { peak = std::max(peak.load(), used); });
    CompactContext ctx = context(*env.store);
    ctx.budget = &budget;
    ctx.abort = [] { return false; };
    const CompactResult r = compact_packs(ctx);
    EXPECT_EQ(r.objects_moved, 1U);
    EXPECT_FALSE(r.aborted);
    // Открытый текст и шифртекст нового пакета были учтены, после работы аренда возвращена.
    EXPECT_GE(peak.load(), 30'000U + crypto::chunk_encrypted_size(30'000));
    EXPECT_EQ(budget.used(), 0U);
    EXPECT_EQ(budget.leased(), 0U);
    EXPECT_EQ(tmp_files(), 0U);
}

TEST_F(VfsCompact, AbortWhileWaitingForStagingSpaceStopsTheCompaction) {
    const auto inos = make_pack("f", 6);
    for (const size_t i : {0U, 1U, 2U, 4U, 5U}) {
        rm("f" + std::to_string(i), inos[i]);
    }
    // Лимит мал, и держатель аренды занимает его: уплотнение ждёт места, пока не сработает abort.
    StagingBudget budget(StagingConfig{100'000, 100'000, 0});
    auto holder = budget.acquire_internal(90'000, [] { return false; });
    ASSERT_TRUE(holder.has_value());
    std::atomic<bool> stop{false};
    CompactContext ctx = context(*env.store);
    ctx.budget = &budget;
    ctx.abort = [&] { return stop.load(); };
    CompactResult r;
    std::thread compactor([&] { r = compact_packs(ctx); });
    std::this_thread::sleep_for(milliseconds(200));
    stop = true;
    budget.notify_all();
    compactor.join();
    EXPECT_TRUE(r.aborted);
    EXPECT_EQ(r.new_packs, 0U);
    EXPECT_EQ(tmp_files(), 0U);
}

// SQLITE_BUSY от соседнего соединения к state.sqlite (ReadCache) транзиентна: запись журнала
// загрузок и строк overlay повторяется с паузой, а не останавливает ФС (set_fatal). Размер файла:
// 300 000 — позиционная заливка, 10 000 — заливка пакета.
class VfsStateBusy : public VfsPack, public ::testing::WithParamInterface<size_t> {};

TEST_P(VfsStateBusy, UploadsRetryWhileStateDbIsLockedByAnotherConnection) {
    GateStore gate(*env.store);
    const GateGuard guard(env, gate);
    env.vfs.reset();
    env.override_store = &gate;
    env.start(options());
    env.state->set_busy_timeout(milliseconds(20));
    const Created c = v().create(kRoot, bytes_of("busy"), 0644, O_RDWR, 1, 1);
    const Bytes data = pattern(GetParam(), 3);
    gate.close_gate();
    v().write(c.attr.ino, 0, data);
    v().release(c.fh);
    std::atomic<bool> threw{false};
    std::thread syncer([&] {
        try {
            v().fsync(c.attr.ino);
        } catch (const std::exception&) {
            threw = true;
        }
    });
    ASSERT_TRUE(gate.wait_entered(1));  // журнал начат, заливка стоит в put_chunk
    {
        meta::Db other = meta::Db::open(env.state_path());
        other.exec("BEGIN IMMEDIATE");  // чужая запись держит state.sqlite
        gate.open_gate();
        std::this_thread::sleep_for(milliseconds(400));
        other.exec("COMMIT");
    }
    syncer.join();
    EXPECT_FALSE(threw.load()) << "SQLITE_BUSY stopped the upload";
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_FALSE(v().status().fatal);
    EXPECT_EQ(env.read_all(c.attr.ino), data);
}

INSTANTIATE_TEST_SUITE_P(Sizes, VfsStateBusy, ::testing::Values(size_t{300'000}, size_t{10'000}));

}  // namespace
}  // namespace telega::vfs::test
