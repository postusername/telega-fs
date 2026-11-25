// Учёт staging, лимиты и обратное давление, запечатывание выше
// staging_high_watermark, RMW без буфера на чанк.
//
// Тесты на параллельность синхронизируются барьерами и условиями (GateStore держит заливку на
// put_chunk, ожидания идут по условию, а не по времени); паузы нужны только в проверках «ничего
// не происходит»: они могут пропустить ошибку, но не дать ложного падения.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <latch>
#include <thread>
#include <vector>

#include "crypto/chunk.h"
#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

constexpr uint64_t kMiB = uint64_t{1} << 20U;
constexpr uint64_t kKiB = 1024;

// Резерв заливки R для тестового окружения: upload_concurrency = 2, LocalDirStore (без
// альбомов), плюс открытый текст одного пакета (chunk_size).
uint64_t reserve() {
    return 2 * crypto::chunk_encrypted_size(static_cast<uint64_t>(kChunk)) +
           static_cast<uint64_t>(kChunk);
}

// Проверка инварианта «занятое ≤ staging_limit» на каждом изменении счётчика.
struct InvariantProbe {
    explicit InvariantProbe(uint64_t lim) : limit(lim) {}
    void operator()(uint64_t used) {
        uint64_t prev = max_used.load();
        while (used > prev && !max_used.compare_exchange_weak(prev, used)) {
        }
        if (used > limit) {
            violations.fetch_add(1);
        }
    }
    uint64_t limit;
    std::atomic<uint64_t> max_used{0};
    std::atomic<uint64_t> violations{0};
};

// Параметры: staging_limit = R + extra, пользовательский предел (limit − R) = extra.
VfsOptions staging_options(uint64_t extra, InvariantProbe* probe = nullptr) {
    VfsOptions o = fast_options();
    o.staging_limit = reserve() + extra;
    o.staging_probe = [probe](uint64_t used) {
        if (probe != nullptr) {
            (*probe)(used);
        }
    };
    return o;
}

bool wait_until(const std::function<bool()>& pred, milliseconds timeout = milliseconds(10'000)) {
    const auto deadline = Clock::now() + timeout;
    while (!pred()) {
        if (Clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(milliseconds(2));
    }
    return true;
}

// Сумма длин файлов каталога.
uint64_t dir_bytes(const fs::path& dir) {
    uint64_t total = 0;
    for (const auto& de : fs::directory_iterator(dir)) {
        if (de.is_regular_file()) {
            total += de.file_size();
        }
    }
    return total;
}

// Store, у которого чтение чанков можно сломать.
class ReadFailStore final : public store::Store {
  public:
    explicit ReadFailStore(store::Store& inner) : inner_(inner) {}
    std::atomic<bool> fail_reads{false};

    store::ChunkRef put_chunk(const fs::path& file) override { return inner_.put_chunk(file); }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        if (fail_reads.load()) {
            throw store::StoreError(store::StoreErrorKind::permanent, "injected read failure");
        }
        inner_.read_range(r, off, out);
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_.delete_messages(ids); }
    std::optional<store::HeadInfo> read_head() override { return inner_.read_head(); }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const fs::path& f) override { return inner_.put_index(f); }
    void set_head(int64_t n, std::optional<int64_t> o) override { inner_.set_head(n, o); }

  private:
    store::Store& inner_;
};

class VfsStaging : public ::testing::Test {
  protected:
    // Заглушить все заливки до открытия ворот. Ворота должны быть открыты до разрушения Vfs.
    void SetUp() override {
        gate = std::make_unique<GateStore>(*env.store);
        env.override_store = gate.get();
    }
    void TearDown() override {
        gate->open_gate();
        env.vfs.reset();
    }
    VfsEnv env;
    std::unique_ptr<GateStore> gate;
};

// --- обратное давление ---

TEST_F(VfsStaging, WriteBlocksAtLimitMinusReserveAndResumesWhenUploadsFreeSpace) {
    InvariantProbe probe(reserve() + 6 * kMiB);
    gate->close_gate();
    Vfs& vfs = env.start(staging_options(6 * kMiB, &probe));
    const uint64_t cap = vfs.staging().user_cap();
    ASSERT_EQ(cap, 6 * kMiB);

    const int64_t other = env.mkfile("other");
    env.write(other, 0, bytes_of("small file"));

    const int64_t big = env.mkfile("big");
    constexpr size_t kPiece = 512 * kKiB;
    constexpr int kPieces = 20;  // 10 МиБ при пределе 6 МиБ: без заливки не поместится
    Bytes model;
    for (int i = 0; i < kPieces; ++i) {
        const Bytes p = pattern(kPiece, static_cast<uint64_t>(i) + 1);
        model.insert(model.end(), p.begin(), p.end());
    }
    std::atomic<int> done{0};
    std::atomic<int> failed{0};
    std::thread writer([&] {
        for (int i = 0; i < kPieces; ++i) {
            try {
                const ByteSpan piece{model.data() + static_cast<size_t>(i) * kPiece, kPiece};
                vfs.write(big, static_cast<uint64_t>(i) * kPiece, piece);
                done.fetch_add(1);
            } catch (const std::exception&) {
                failed.fetch_add(1);
                return;
            }
        }
    });

    // Писатель упирается в предел: занято + ещё один кусок не помещается.
    ASSERT_TRUE(gate->wait_entered(2));  // обе заливки стоят в put_chunk, аренда уменьшена
    ASSERT_TRUE(wait_until([&] { return vfs.staging().used() + kPiece > cap; }));
    std::this_thread::sleep_for(milliseconds(100));  // done отстаёт от учёта на один write
    const int stuck_at = done.load();
    EXPECT_LT(stuck_at, kPieces);

    // Пока write ждёт места, другие операции отвечают.
    EXPECT_EQ(vfs.getattr(other).size, 10);
    EXPECT_EQ(str_of(vfs.read(other, 0, 100)), "small file");
    EXPECT_EQ(vfs.getattr(big).ino, big);
    EXPECT_EQ(vfs.read(big, 0, 16).size(), 16U);
    EXPECT_NO_THROW(vfs.fsync(other));
    EXPECT_NO_THROW((void)vfs.lookup(kRoot, bytes_of("other")));

    // Негативная проверка: за паузу писатель не продвинулся.
    std::this_thread::sleep_for(milliseconds(150));
    EXPECT_EQ(done.load(), stuck_at);
    EXPECT_EQ(failed.load(), 0);

    gate->open_gate();
    writer.join();
    EXPECT_EQ(done.load(), kPieces);
    EXPECT_EQ(failed.load(), 0);
    vfs.flush_all();
    EXPECT_EQ(env.read_all(big), model);
    EXPECT_EQ(probe.violations.load(), 0U);
    EXPECT_LE(probe.max_used.load(), vfs.staging().config().limit);
    EXPECT_EQ(vfs.staging().overcommits(), 0U);
}

TEST_F(VfsStaging, EnospcAfterTimeoutWhileUploadsAreStopped) {
    VfsOptions o = staging_options(3 * kMiB);
    o.staging_full_timeout = milliseconds(400);
    gate->close_gate();
    Vfs& vfs = env.start(o);
    const int64_t ino = env.mkfile("f");
    constexpr size_t kPiece = 512 * kKiB;
    Bytes model;
    int err = 0;
    const auto begin = Clock::now();
    auto elapsed = Clock::duration::zero();
    for (int i = 0; i < 40 && err == 0; ++i) {
        const Bytes p = pattern(kPiece, static_cast<uint64_t>(i) + 1);
        const auto t0 = Clock::now();
        err = err_of([&] { vfs.write(ino, model.size(), p); });
        if (err == 0) {
            model.insert(model.end(), p.begin(), p.end());
        } else {
            elapsed = Clock::now() - t0;
        }
    }
    EXPECT_EQ(err, ENOSPC);
    EXPECT_GE(elapsed, milliseconds(400));
    EXPECT_LT(Clock::now() - begin, std::chrono::seconds(30));
    // Принятые записи целы, размер файла не вырос на отказавшем write.
    EXPECT_EQ(vfs.getattr(ino).size, static_cast<int64_t>(model.size()));
    EXPECT_EQ(str_of({vfs.read(ino, 0, 16).data(), 16}), str_of({model.data(), 16}));

    // Когда заливка возобновилась, запись снова проходит.
    gate->open_gate();
    const Bytes more = pattern(kPiece, 999);
    EXPECT_TRUE(
        wait_until([&] { return err_of([&] { vfs.write(ino, model.size(), more); }) == 0; }));
}

TEST_F(VfsStaging, BlockedWriteDoesNotHoldInodeStripeSoPackApplyFreesSpace) {
    // Пользовательского места 60 000 байт. Объекты P (30 000) и P2 (20 000) лежат в закрытом
    // пакете, который стоит в put_chunk (ворота закрыты): место освободит только применение
    // пакета, которому нужны полосы P и P2. Полная перезапись P на 45 000 байт требует ещё
    // 15 000 и ждёт места вне полосы P: иначе она держала бы полосу, а пакет — место.
    VfsOptions o = staging_options(60'000);
    o.staging_full_timeout = milliseconds(8'000);
    gate->close_gate();
    Vfs& vfs = env.start(o);
    const Bytes p_data = pattern(30'000, 1);
    const Bytes p2_data = pattern(20'000, 2);
    const Created c1 = vfs.create(kRoot, bytes_of("p"), 0644, O_RDWR, 1000, 1000);
    const Created c2 = vfs.create(kRoot, bytes_of("p2"), 0644, O_RDWR, 1000, 1000);
    const int64_t p = c1.attr.ino;
    ASSERT_EQ(vfs.write(p, 0, p_data), p_data.size());
    ASSERT_EQ(vfs.write(c2.attr.ino, 0, p2_data), p2_data.size());
    vfs.release(c1.fh);  // последний release запечатывает: объект уходит в открытый пакет
    vfs.release(c2.fh);
    ASSERT_TRUE(wait_until([&] { return vfs.pack_status().open_objects == 2; }));
    ASSERT_TRUE(vfs.close_open_pack());
    ASSERT_TRUE(gate->wait_entered(1));  // пакет в put_chunk

    const Bytes rewrite = pattern(45'000, 3);  // покрывает весь P: старое читать не нужно
    std::atomic<bool> finished{false};
    std::atomic<int> werr{0};
    std::thread writer([&] {
        werr.store(err_of([&] { vfs.write(p, 0, rewrite); }));
        finished.store(true);
    });
    // Писатель в ожидании места: ждём, пока он войдёт в wait_for_space (место не освободится).
    std::this_thread::sleep_for(milliseconds(150));
    EXPECT_FALSE(finished.load());
    // Полоса P свободна: чтение и getattr того же файла отвечают.
    EXPECT_EQ(vfs.read(p, 0, 100).size(), 100U);
    EXPECT_EQ(vfs.getattr(p).size, 30'000);

    gate->open_gate();  // пакет применяется, место освобождается, запись проходит
    writer.join();
    EXPECT_EQ(werr.load(), 0);
    EXPECT_EQ(env.read_all(p), rewrite);
    EXPECT_EQ(env.read_all(vfs.lookup(kRoot, bytes_of("p2")).ino), p2_data);
}

// --- запечатывание выше staging_high_watermark ---

TEST_F(VfsStaging, AboveHighWatermarkSealsOldestWithoutDelayAndClosesOpenPack) {
    VfsOptions o = staging_options(64 * kMiB);
    o.staging_high_watermark = 2'621'440;  // 2,5 МиБ
    // seal_delay и pack_max_age велики: ничто, кроме порога, ничего не запечатывает.
    o.pack_max_age = milliseconds(3'600'000);
    Vfs& vfs = env.start(o);

    const int64_t small = env.mkfile("small");
    env.write(small, 0, pattern(10 * kKiB, 100));  // пакетный класс: > inline_max, ≤ pack_threshold
    std::vector<int64_t> files;
    files.reserve(9);
    for (int i = 0; i < 9; ++i) {
        files.push_back(env.mkfile("f" + std::to_string(i)));
    }
    // 8 файлов по 300 КиБ вместе с small — ниже порога: никто не запечатан.
    for (int i = 0; i < 8; ++i) {
        env.write(files[static_cast<size_t>(i)], 0, pattern(300 * kKiB, static_cast<uint64_t>(i)));
    }
    EXPECT_LT(vfs.staging().used(), o.staging_high_watermark);
    std::this_thread::sleep_for(milliseconds(50));
    EXPECT_EQ(gate->entered(), 0);
    EXPECT_EQ(vfs.status().queued + vfs.status().uploading, 0U);
    EXPECT_EQ(vfs.pack_status().open_objects, 0U);

    // Девятый файл переводит занятое через порог: запечатываются самые старые (small, f0), а
    // открытый пакет закрывается сразу.
    env.write(files[8], 0, pattern(300 * kKiB, 8));
    ASSERT_TRUE(wait_until([&] { return !env.extents(files[0]).empty(); }));
    ASSERT_TRUE(wait_until([&] { return vfs.pack_status().packs_uploaded == 1; }));
    EXPECT_FALSE(env.extents(small).empty());
    for (int i = 1; i < 9; ++i) {
        EXPECT_TRUE(env.extents(files[static_cast<size_t>(i)]).empty()) << "file " << i;
    }
    ASSERT_TRUE(wait_until([&] { return env.staging_files() == 8; }));
    EXPECT_TRUE(wait_until([&] { return vfs.staging().used() == uint64_t{8} * 300 * kKiB; }));
}

TEST_F(VfsStaging, PressureCountsInFlightUploadsBeforeSealingMore) {
    // Выше порога идёт заливка одного файла, которой хватает, чтобы вернуться под порог:
    // следующие записи не запечатывают лишнего.
    VfsOptions o = staging_options(64 * kMiB);
    o.staging_high_watermark = 1 * kMiB;
    gate->close_gate();
    env.start(o);
    std::vector<int64_t> files;
    files.reserve(6);
    for (int i = 0; i < 6; ++i) {
        files.push_back(env.mkfile("g" + std::to_string(i)));
    }
    env.write(files[0], 0, pattern(800 * kKiB, 1));
    EXPECT_EQ(gate->entered(), 0);
    env.write(files[1], 0, pattern(800 * kKiB, 2));  // 1,6 МиБ > 1 МиБ: нужно освободить 0,6 МиБ
    ASSERT_TRUE(gate->wait_entered(1));  // самый старый ушёл в заливку (остановлен воротами)
    // Ещё записи при уже идущей заливке: она покрывает превышение, f1 не трогают.
    std::this_thread::sleep_for(milliseconds(50));
    EXPECT_EQ(gate->entered(), 1);
    EXPECT_TRUE(env.extents(files[1]).empty());
    gate->open_gate();
    ASSERT_TRUE(wait_until([&] { return !env.extents(files[0]).empty(); }));
}

// --- учёт ---

TEST_F(VfsStaging, UsedEqualsFileLengthsAndInvariantHoldsWithParallelWriters) {
    constexpr int kThreads = 4;
    constexpr size_t kPiece = 256 * kKiB;
    constexpr size_t kPerFile = 6 * kMiB;
    InvariantProbe probe(reserve() + 8 * kMiB);
    gate->close_gate();
    Vfs& vfs = env.start(staging_options(8 * kMiB, &probe));
    const uint64_t cap = vfs.staging().user_cap();

    std::vector<int64_t> inos;
    std::vector<Bytes> models(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        inos.push_back(env.mkfile("w" + std::to_string(t)));
        models[static_cast<size_t>(t)] = pattern(kPerFile, static_cast<uint64_t>(t) + 7);
    }
    std::latch start(kThreads + 1);
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            const Bytes& m = models[static_cast<size_t>(t)];
            start.arrive_and_wait();
            try {
                for (size_t off = 0; off < kPerFile; off += kPiece) {
                    vfs.write(inos[static_cast<size_t>(t)], off, {m.data() + off, kPiece});
                }
            } catch (const std::exception&) {
                failures.fetch_add(1);
            }
        });
    }
    start.arrive_and_wait();
    // 24 МиБ данных при пределе 8 МиБ: писатели упираются, заливка стоит на воротах.
    ASSERT_TRUE(wait_until([&] { return vfs.staging().used() + kPiece > cap; }));
    ASSERT_TRUE(gate->wait_entered(2));
    // Занятое равно длинам файлов в staging/ и tmp/ (логическая длина).
    EXPECT_TRUE(wait_until(
        [&] { return vfs.staging().used() == dir_bytes(env.staging()) + dir_bytes(env.tmp()); }));
    gate->open_gate();
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(failures.load(), 0);
    vfs.flush_all();
    for (int t = 0; t < kThreads; ++t) {
        EXPECT_EQ(env.read_all(inos[static_cast<size_t>(t)]), models[static_cast<size_t>(t)]);
    }
    EXPECT_EQ(probe.violations.load(), 0U);
    EXPECT_LE(probe.max_used.load(), vfs.staging().config().limit);
    EXPECT_EQ(vfs.staging().overcommits(), 0U);
    // Всё залито: учёт вернулся к нулю, файлов нет.
    EXPECT_TRUE(wait_until([&] { return vfs.staging().used() == 0; }));
    EXPECT_EQ(env.staging_files(), 0U);
}

TEST_F(VfsStaging, UsedIsRecountedAfterRestart) {
    env.override_store = nullptr;  // первый запуск: заливки нет, seal_delay огромен
    const Vfs& first = env.start(staging_options(64 * kMiB));
    const int64_t a = env.mkfile("a");
    const int64_t b = env.mkfile("b");
    const int64_t s = env.mkfile("s");
    env.write(a, 0, pattern(700 * kKiB, 1));
    env.write(b, 0, pattern(300 * kKiB, 2));
    env.write(s, 0, pattern(10 * kKiB, 3));
    env.write(b, 400 * kKiB, pattern(1000, 4));  // дыра: логическая длина файла b — 400 КиБ + 1000
    const uint64_t expected = 700 * kKiB + (400 * kKiB + 1000) + 10 * kKiB;
    EXPECT_EQ(first.staging().used(), expected);
    EXPECT_EQ(dir_bytes(env.staging()), expected);

    // «Падение» и перезапуск; заливки встают на воротах, так что состояние устойчиво.
    env.override_store = gate.get();
    gate->close_gate();
    Vfs& second = env.restart(staging_options(64 * kMiB));
    ASSERT_TRUE(gate->wait_entered(2));  // a и b ушли в заливку; s ждёт в открытом пакете
    EXPECT_TRUE(wait_until([&] {
        return second.staging().used() == dir_bytes(env.staging()) + dir_bytes(env.tmp());
    }));
    EXPECT_GE(second.staging().used(), expected);
}

TEST_F(VfsStaging, TruncateUpTakesNoSpace) {
    Vfs& vfs = env.start(staging_options(4 * kMiB));
    const int64_t ino = env.mkfile("t");
    env.write(ino, 0, pattern(100 * kKiB, 1));
    const uint64_t before = vfs.staging().used();
    EXPECT_EQ(before, 100 * kKiB);
    SetAttr up;
    up.size = int64_t{4} << 30;
    vfs.setattr(ino, up);
    EXPECT_EQ(vfs.getattr(ino).size, int64_t{4} << 30);
    EXPECT_EQ(vfs.staging().used(), before);
    // Запись далеко за концом занимает только записанное: файл разреженный.
    env.write(ino, (int64_t{3} << 30) + 5, pattern(1000, 2));
    EXPECT_EQ(vfs.staging().used(), before + 5 + 1000);
    // Усечение возвращает место.
    SetAttr down;
    down.size = 40 * static_cast<int64_t>(kKiB);
    vfs.setattr(ino, down);
    EXPECT_EQ(vfs.staging().used(), 40 * kKiB);
}

TEST_F(VfsStaging, OverwriteInsideStagingNeedsNoSpaceEvenAtTheLimit) {
    VfsOptions o = staging_options(300 * kKiB);
    o.staging_full_timeout = milliseconds(200);
    Vfs& vfs = env.start(o);
    const int64_t ino = env.mkfile("o");
    env.write(ino, 0, pattern(300 * kKiB, 1));  // занято ровно до предела
    EXPECT_EQ(vfs.staging().used(), 300 * kKiB);
    const Bytes patch = pattern(1000, 2);
    EXPECT_EQ(err_of([&] { vfs.write(ino, 100, patch); }), 0);
    // Запись, которой места не хватает, ждёт и получает ENOSPC (даже запечатав и залив файл,
    // RMW старого чанка требует больше предела).
    EXPECT_EQ(err_of([&] { vfs.write(ino, 300 * kKiB, patch); }), ENOSPC);
    // Запечатанный из-за нехватки места файл заливается, аренда возвращается.
    EXPECT_TRUE(wait_until([&] { return vfs.staging().used() <= 300 * kKiB; }));
}

// --- параметры ---

TEST_F(VfsStaging, LimitNotAboveReserveIsRejected) {
    VfsOptions o = fast_options();
    o.staging_limit = reserve();
    EXPECT_THROW(env.start(o), std::invalid_argument);
    o.staging_limit = reserve() + kMiB;
    o.staging_high_watermark = o.staging_limit + 1;
    EXPECT_THROW(env.start(o), std::invalid_argument);
}

TEST_F(VfsStaging, DefaultHighWatermarkIsThreeQuartersOfLimit) {
    VfsOptions o = fast_options();
    o.staging_limit = reserve() + 400 * kMiB;
    Vfs& vfs = env.start(o);
    const auto& cfg = vfs.staging().config();
    EXPECT_EQ(cfg.high, o.staging_limit / 4 * 3);
    EXPECT_EQ(cfg.reserve, reserve());
    EXPECT_EQ(vfs.status().staging_limit, o.staging_limit);
}

// --- RMW без буфера на чанк ---

class VfsRmw : public ::testing::Test {
  protected:
    void SetUp() override {
        failing = std::make_unique<ReadFailStore>(*env.store);
        env.override_store = failing.get();
    }
    VfsEnv env;
    std::unique_ptr<ReadFailStore> failing;
};

TEST_F(VfsRmw, DecryptsChunkIntoStagingInSegmentsWithoutChunkSizedBuffer) {
    VfsOptions o = staging_options(64 * kMiB);
    o.rmw_segment = 64 * kKiB;
    Vfs& vfs = env.start(o);
    const int64_t ino = env.mkfile("r");
    // Не кратно сегменту: последний кусок короче.
    Bytes model = pattern(static_cast<size_t>(kChunk) - 1000, 5);
    env.write(ino, 0, model);
    vfs.flush_all();
    ASSERT_EQ(env.staging_files(), 0U);
    ASSERT_EQ(vfs.staging().used(), 0U);

    const Bytes patch = pattern(10, 6);
    env.write(ino, 500'000, patch);  // не покрывает позицию: RMW
    std::copy(patch.begin(), patch.end(), model.begin() + 500'000);
    EXPECT_EQ(vfs.rmw_peak_buffer(), 64 * kKiB);
    EXPECT_LT(vfs.rmw_peak_buffer(), static_cast<uint64_t>(kChunk));
    EXPECT_EQ(vfs.staging().used(), model.size());
    EXPECT_EQ(env.read_all(ino), model);
    vfs.flush_all();
    env.vfs->shutdown();
    EXPECT_EQ(env.read_all(ino), model);
}

TEST_F(VfsRmw, FailedRmwLeavesNoStagingFileAndReturnsSpace) {
    VfsOptions o = staging_options(64 * kMiB);
    o.rmw_segment = 64 * kKiB;
    Vfs& vfs = env.start(o);
    const int64_t ino = env.mkfile("r");
    const Bytes model = pattern(static_cast<size_t>(kChunk), 8);
    env.write(ino, 0, model);
    vfs.flush_all();
    failing->fail_reads.store(true);
    EXPECT_EQ(err_of([&] { vfs.write(ino, 1000, pattern(10, 9)); }), EIO);
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(vfs.staging().used(), 0U);
    failing->fail_reads.store(false);
    EXPECT_EQ(err_of([&] { vfs.write(ino, 1000, pattern(10, 9)); }), 0);
    EXPECT_EQ(vfs.staging().used(), model.size());
}

TEST_F(VfsRmw, ConcurrentRmwBuffersAreSegmentsNotChunks) {
    VfsOptions o = staging_options(64 * kMiB);
    o.rmw_segment = 32 * kKiB;
    Vfs& vfs = env.start(o);
    constexpr int kFiles = 4;
    std::vector<int64_t> inos;
    std::vector<Bytes> models;
    for (int i = 0; i < kFiles; ++i) {
        inos.push_back(env.mkfile("c" + std::to_string(i)));
        models.push_back(pattern(static_cast<size_t>(kChunk), static_cast<uint64_t>(i) + 20));
        env.write(inos.back(), 0, models.back());
    }
    vfs.flush_all();
    std::latch start(kFiles);
    std::vector<std::thread> threads;
    threads.reserve(kFiles);
    for (int i = 0; i < kFiles; ++i) {
        threads.emplace_back([&, i] {
            start.arrive_and_wait();
            const Bytes patch = pattern(100, static_cast<uint64_t>(i) + 50);
            vfs.write(inos[static_cast<size_t>(i)], 12'345, patch);
            std::copy(patch.begin(), patch.end(), models[static_cast<size_t>(i)].begin() + 12'345);
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(vfs.rmw_peak_buffer(), 32 * kKiB);
    for (int i = 0; i < kFiles; ++i) {
        EXPECT_EQ(env.read_all(inos[static_cast<size_t>(i)]), models[static_cast<size_t>(i)]);
    }
}

// --- аренда StagingBudget: внутренняя аренда остаётся внутренней после перемещения ---

TEST(StagingBudgetLease, InternalLeaseKeepsItsKindWhenMoved) {
    StagingBudget b(StagingConfig{1000, 750, 100});
    {
        // acquire_internal возвращает optional<Lease> перемещением.
        auto l = b.acquire_internal(100, [] { return false; });
        if (!l.has_value()) {
            FAIL() << "no lease";
        }
        EXPECT_EQ(b.leased(), 100U);
        l->shrink_to(40);
        EXPECT_EQ(b.leased(), 40U);
        StagingBudget::Lease moved = std::move(*l);
        EXPECT_EQ(b.leased(), 40U);
        StagingBudget::Lease assigned;
        assigned = std::move(moved);
        EXPECT_EQ(assigned.bytes(), 40U);
    }
    EXPECT_EQ(b.used(), 0U);
    EXPECT_EQ(b.leased(), 0U);
}

TEST(StagingBudgetLease, PassesOverTheLimitWhenThereAreNoHoldersAfterALeaseWasReleased) {
    StagingBudget b(StagingConfig{1000, 750, 100});
    {
        auto l = b.acquire_internal(100, [] { return false; });
        ASSERT_TRUE(l);
    }
    b.charge_unchecked(950);  // staging занят пользовательскими данными, держателей нет
    std::atomic<bool> stop{false};
    std::atomic<bool> got{false};
    std::thread t([&] {
        const auto l = b.acquire_internal(100, [&] { return stop.load(); });
        got = l.has_value();
        stop = true;
    });
    EXPECT_TRUE(wait_until([&] { return stop.load(); }, milliseconds(5'000)))
        << "acquire_internal waits although nobody holds a lease";
    stop = true;
    b.notify_all();
    t.join();
    EXPECT_TRUE(got.load());
    EXPECT_EQ(b.overcommits(), 1U);
}

TEST(StagingBudgetLease, UserLeaseGivenToTheOverlayDoesNotCountAsLeased) {
    StagingBudget b(StagingConfig{1000, 750, 100});
    auto u = b.try_acquire_user(200);
    if (!u.has_value()) {
        FAIL() << "no lease";
    }
    EXPECT_EQ(b.leased(), 0U);
    u->keep();
    EXPECT_EQ(b.used(), 200U);
    EXPECT_EQ(b.leased(), 0U);
}

TEST(VfsStagingLease, NothingIsLeasedWhenUploadsAreIdle) {
    VfsEnv env(4096, 16 << 20, 1 << 16, 256 << 10);
    env.start(fast_options());
    Vfs& v = *env.vfs;
    for (int i = 0; i < 5; ++i) {
        const Created c = v.create(kRoot, bytes_of("big" + std::to_string(i)), 0644, O_RDWR, 1, 1);
        v.write(c.attr.ino, 0, pattern(300'000, static_cast<uint64_t>(i)));
        v.release(c.fh);
        v.fsync(c.attr.ino);
    }
    EXPECT_TRUE(wait_until([&] { return v.staging().used() == 0; }));
    EXPECT_EQ(v.staging().leased(), 0U);
}

// Давление и запечатывание должны работать и после первой заливки: раньше аренда терялась при
// перемещении, leased росло навсегда, давление обнулялось (used = total - leased), и писатели
// получали ENOSPC вместо запечатывания.
TEST(VfsStagingLease, PressureSealsOpenFilesAfterTheFirstUpload) {
    VfsEnv env(4096, 16 << 20, 1 << 16, 256 << 10);
    VfsOptions o = fast_options();
    o.staging_limit = reserve() + 6 * kMiB;
    o.staging_full_timeout = milliseconds(3000);
    env.start(o);
    Vfs& v = *env.vfs;
    const Created prime = v.create(kRoot, bytes_of("prime"), 0644, O_RDWR, 1, 1);
    v.write(prime.attr.ino, 0, pattern(300'000, 1));
    v.release(prime.fh);
    v.fsync(prime.attr.ino);
    // Двенадцать файлов с полным чанком, открытые: вместе они втрое больше пользовательского
    // предела, так что без запечатывания под давлением запись упёрлась бы в ENOSPC.
    for (int i = 0; i < 12; ++i) {
        const Created c = v.create(kRoot, bytes_of("f" + std::to_string(i)), 0644, O_RDWR, 1, 1);
        EXPECT_NO_THROW(v.write(
            c.attr.ino, 0, pattern(static_cast<size_t>(kChunk), static_cast<uint64_t>(10 + i))))
            << "write " << i;
    }
}

}  // namespace
}  // namespace telega::vfs::test
