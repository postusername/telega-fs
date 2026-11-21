// Vfs под нагрузкой из нескольких потоков (для TSan): запись и чтение разных файлов и одного
// файла, параллельные заливки по таймеру и потоковые, fsync и операции с именами.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

class VfsConcurrency : public ::testing::Test {
  protected:
    void SetUp() override {
        VfsOptions o = fast_options();
        o.seal_delay = milliseconds(2);
        o.upload_concurrency = 3;
        o.download_concurrency = 2;
        o.plain_cache = 1U << 20U;
        env.start(o);
    }
    VfsEnv env;
};

TEST_F(VfsConcurrency, WritersOnDifferentFiles) {
    constexpr int kThreads = 4;
    std::vector<int64_t> inos;
    inos.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        inos.push_back(env.mkfile("t" + std::to_string(t)));
    }
    std::vector<Bytes> models(kThreads);
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937_64 rng(static_cast<uint64_t>(t) + 100);
            Bytes& m = models[static_cast<size_t>(t)];
            const int64_t ino = inos[static_cast<size_t>(t)];
            try {
                for (int i = 0; i < 60; ++i) {
                    const uint64_t off = rng() % (2 * kChunk + 1000);
                    const auto len = static_cast<size_t>(rng() % 70'000 + 1);
                    const Bytes data = pattern(len, rng());
                    env.vfs->write(ino, off, data);
                    if (m.size() < off + len) {
                        m.resize(off + len, std::byte{0});
                    }
                    std::copy(data.begin(), data.end(),
                              m.begin() + static_cast<std::ptrdiff_t>(off));
                    if (i % 7 == 0) {
                        const Bytes got = env.vfs->read(ino, 0, m.size() + 1);
                        if (got != m) {
                            failures.fetch_add(1);
                        }
                    }
                    if (i % 20 == 19) {
                        env.vfs->fsync(ino);
                    }
                }
            } catch (const std::exception& e) {
                ADD_FAILURE() << "thread " << t << ": " << e.what();
                failures.fetch_add(1);
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(failures.load(), 0);
    env.vfs->flush_all();
    env.restart();
    for (int t = 0; t < kThreads; ++t) {
        EXPECT_EQ(env.read_all(inos[static_cast<size_t>(t)]), models[static_cast<size_t>(t)]);
    }
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsConcurrency, WritersAndReadersOnOneFile) {
    const int64_t f = env.mkfile("shared");
    // Каждый писатель владеет своей полосой байт (с пересечением границ позиций),
    // читатели читают всё подряд.
    constexpr int kWriters = 3;
    constexpr uint64_t kStripe = kChunk / 2 + 777;
    std::atomic<bool> done{false};
    std::atomic<int> failures{0};
    std::vector<Bytes> last(kWriters);
    std::vector<std::thread> threads;
    threads.reserve(kWriters + 2);
    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w] {
            try {
                for (int i = 0; i < 25; ++i) {
                    const Bytes data = pattern(
                        kStripe, static_cast<uint64_t>(w) * 1000 + static_cast<uint64_t>(i));
                    env.vfs->write(f, static_cast<uint64_t>(w) * kStripe, data);
                    last[static_cast<size_t>(w)] = data;
                    if (i % 10 == 9) {
                        env.vfs->fsync(f);
                    }
                }
            } catch (const std::exception& e) {
                ADD_FAILURE() << "writer " << w << ": " << e.what();
                failures.fetch_add(1);
            }
        });
    }
    for (int r = 0; r < 2; ++r) {
        threads.emplace_back([&] {
            try {
                while (!done.load()) {
                    const Attr a = env.vfs->getattr(f);
                    const Bytes got = env.vfs->read(f, 0, static_cast<size_t>(a.size));
                    if (static_cast<int64_t>(got.size()) > a.size) {
                        failures.fetch_add(1);
                    }
                }
            } catch (const std::exception& e) {
                ADD_FAILURE() << "reader: " << e.what();
                failures.fetch_add(1);
            }
        });
    }
    for (int w = 0; w < kWriters; ++w) {
        threads[static_cast<size_t>(w)].join();
    }
    done = true;
    for (size_t i = kWriters; i < threads.size(); ++i) {
        threads[i].join();
    }
    EXPECT_EQ(failures.load(), 0);
    Bytes model;
    for (const Bytes& b : last) {
        model.insert(model.end(), b.begin(), b.end());
    }
    EXPECT_EQ(env.read_all(f), model);
    env.vfs->flush_all();
    env.restart();
    EXPECT_EQ(env.read_all(f), model);
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsConcurrency, NamespaceChurnWithUploads) {
    constexpr int kThreads = 4;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            try {
                const int64_t dir =
                    env.vfs->mkdir(kRoot, bytes_of("d" + std::to_string(t)), 0755, 0, 0).ino;
                for (int i = 0; i < 30; ++i) {
                    const std::string n = "f" + std::to_string(i % 5);
                    int64_t ino = 0;
                    try {
                        ino = env.vfs->lookup(dir, bytes_of(n)).ino;
                    } catch (const meta::FsError&) {
                        const Created c = env.vfs->create(dir, bytes_of(n), 0644, O_RDWR, 0, 0);
                        env.vfs->release(c.fh);
                        ino = c.attr.ino;
                    }
                    env.vfs->write(
                        ino, static_cast<uint64_t>(i) * 1000,
                        pattern(static_cast<size_t>(i) * 300 + 1, static_cast<uint64_t>(i)));
                    switch (i % 6) {
                        case 0:
                            env.vfs->unlink(dir, bytes_of(n));
                            break;
                        case 1:
                            env.vfs->rename(dir, bytes_of(n), dir, bytes_of("r" + n), 0);
                            break;
                        case 2:
                            env.vfs->fsync(ino);
                            break;
                        default: {
                            SetAttr s;
                            s.size = static_cast<int64_t>(i) * 100;
                            env.vfs->setattr(ino, s);
                            break;
                        }
                    }
                }
                env.vfs->fsyncdir(dir);
            } catch (const std::exception& e) {
                ADD_FAILURE() << "thread " << t << ": " << e.what();
                failures.fetch_add(1);
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(failures.load(), 0);
    env.vfs->shutdown();
    env.restart();
    EXPECT_TRUE(env.state->list_dirty_chunks().empty());
    EXPECT_TRUE(env.accounting_ok());
}

// Store, который считает одновременные put_chunk и держит каждый вызов некоторое время.
class CountingStore final : public store::Store {
  public:
    // expect — сколько одновременных put_chunk ждёт тест. Каждый вызов держится, пока их не
    // наберётся столько (или не выйдет 2 с): иначе под нагрузкой (ctest -j) потоки не успевали
    // совпасть по времени, и максимум выходил меньше настоящей параллельности.
    CountingStore(store::Store& inner, bool albums, int expect)
        : inner_(inner), albums_(albums), expect_(expect) {}
    [[nodiscard]] bool gathers_albums() const noexcept override { return albums_; }
    store::ChunkRef put_chunk(const std::filesystem::path& file) override {
        {
            std::unique_lock lock(mu_);
            ++inflight_;
            max_inflight_ = std::max(max_inflight_, inflight_);
            cv_.notify_all();
            cv_.wait_for(lock, std::chrono::seconds(2), [&] { return inflight_ >= expect_; });
        }
        auto ref = inner_.put_chunk(file);
        {
            const std::lock_guard lock(mu_);
            --inflight_;
        }
        return ref;
    }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        inner_.read_range(r, off, out);
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_.delete_messages(ids); }
    std::optional<store::HeadInfo> read_head() override { return inner_.read_head(); }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const std::filesystem::path& f) override { return inner_.put_index(f); }
    void set_head(int64_t n, std::optional<int64_t> o) override { inner_.set_head(n, o); }
    [[nodiscard]] int max_inflight() const {
        const std::lock_guard lock(mu_);
        return max_inflight_;
    }

  private:
    store::Store& inner_;
    bool albums_;
    int expect_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    int inflight_ = 0;
    int max_inflight_ = 0;
};

// SPEC 8.1, «Параллельность загрузок»: одновременных put_chunk ровно upload_concurrency, если
// Store не собирает альбомы (LocalDirStore); с альбомами — min(10 · upload_concurrency, 32).
int max_put_chunk_calls(VfsEnv& env, bool albums, uint32_t upload_concurrency, int files,
                        int expect) {
    CountingStore counting(*env.store, albums, expect);
    env.override_store = &counting;
    VfsOptions o = fast_options();
    o.upload_concurrency = upload_concurrency;
    env.start(o);
    for (int i = 0; i < files; ++i) {
        const int64_t ino = env.mkfile("f" + std::to_string(i));
        env.vfs->write(ino, 0, pattern(6000, static_cast<uint64_t>(i) + 1));
    }
    env.vfs->flush_all();
    env.vfs->shutdown();
    env.override_store = nullptr;
    return counting.max_inflight();
}

TEST(VfsUploadParallelism, StoreWithoutAlbumsGetsExactlyUploadConcurrency) {
    {
        VfsEnv env;
        EXPECT_EQ(max_put_chunk_calls(env, false, 1, 12, 1), 1);
    }
    {
        VfsEnv env;
        EXPECT_EQ(max_put_chunk_calls(env, false, 3, 12, 3), 3);
    }
}

TEST(VfsUploadParallelism, LocalDirStoreIsNotAnAlbumStore) {
    VfsEnv env;
    EXPECT_FALSE(env.store->gathers_albums());
}

TEST(VfsUploadParallelism, AlbumStoreGetsTenPerSlotUpToThirtyTwo) {
    {
        VfsEnv env;
        EXPECT_EQ(max_put_chunk_calls(env, true, 2, 40, 20), 20);
    }
    {
        VfsEnv env;
        EXPECT_EQ(max_put_chunk_calls(env, true, 100, 60, 32), 32);
    }
    {
        VfsEnv env;  // переполнение 10 · upload_concurrency в 32 битах не даёт мало потоков
        EXPECT_EQ(max_put_chunk_calls(env, true, 0x7fffffffU, 60, 32), 32);
    }
}

}  // namespace
}  // namespace telega::vfs::test
