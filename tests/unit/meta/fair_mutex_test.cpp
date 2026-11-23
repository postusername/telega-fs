// FairRecursiveMutex (meta/fair_mutex.h, SPEC 9): строгий порядок FIFO, повторный вход
// владельца, взаимное исключение. Порядок задаётся не таймингами, а счётчиком очереди
// waiting(): следующий поток запускается, только когда предыдущий уже стоит в очереди.

#include "meta/fair_mutex.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "meta/index_db.h"
#include "test_util.h"

namespace telega::meta {
namespace {

using namespace std::chrono_literals;

// Ждать условия без расчёта на время; предел нужен только затем, чтобы сломанная реализация
// роняла тест, а не вешала его.
bool wait_until(const std::function<bool()>& pred) {
    const auto deadline = std::chrono::steady_clock::now() + 60s;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

TEST(FairMutex, GrantsInArrivalOrder) {
    constexpr int kThreads = 8;
    FairRecursiveMutex mutex;
    std::vector<int> order;  // под mutex
    std::vector<std::thread> threads;

    mutex.lock();
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            const std::lock_guard lock(mutex);
            order.push_back(i);
        });
        // Следующий поток стартует, когда этот уже взял билет и ждёт.
        ASSERT_TRUE(wait_until([&] { return mutex.waiting() == static_cast<uint64_t>(i) + 1; }));
    }
    mutex.unlock();
    for (auto& t : threads) {
        t.join();
    }
    std::vector<int> expected(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        expected[static_cast<size_t>(i)] = i;
    }
    EXPECT_EQ(order, expected);
    EXPECT_EQ(mutex.waiting(), 0U);
}

// Владелец, отпустивший мьютекс и сразу взявший его снова, не обгоняет ждущего (у
// std::recursive_mutex обгоняет, и писатель в цикле морит коммит).
TEST(FairMutex, ReleaserDoesNotBargeAheadOfWaiter) {
    FairRecursiveMutex mutex;
    for (int round = 0; round < 200; ++round) {
        std::atomic<bool> waiter_ran{false};
        mutex.lock();
        std::thread waiter([&] {
            const std::lock_guard lock(mutex);
            waiter_ran.store(true);
        });
        ASSERT_TRUE(wait_until([&] { return mutex.waiting() == 1; }));
        mutex.unlock();
        mutex.lock();  // сразу снова
        EXPECT_TRUE(waiter_ran.load()) << "round " << round << ": the waiter was overtaken";
        mutex.unlock();
        waiter.join();
    }
}

TEST(FairMutex, OwnerReentersWithoutQueueing) {
    FairRecursiveMutex mutex;
    std::thread other;
    mutex.lock();
    other = std::thread([&] { const std::lock_guard lock(mutex); });
    ASSERT_TRUE(wait_until([&] { return mutex.waiting() == 1; }));
    // Ждущий в очереди не мешает владельцу входить повторно, и очередь при этом не растёт.
    mutex.lock();
    mutex.lock();
    EXPECT_EQ(mutex.waiting(), 1U);
    mutex.unlock();
    mutex.unlock();
    // Пока не отпущен внешний уровень, ждущий мьютекса не получит.
    EXPECT_EQ(mutex.waiting(), 1U);
    mutex.unlock();
    other.join();
    EXPECT_EQ(mutex.waiting(), 0U);
}

TEST(FairMutex, MutualExclusionUnderContention) {
    constexpr int kThreads = 4;
    constexpr int kIterations = 3000;
    FairRecursiveMutex mutex;
    int64_t counter = 0;  // не атомарный: защищён мьютексом (TSan проверит)
    std::atomic<int> inside{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < kIterations; ++i) {
                const std::lock_guard outer(mutex);
                EXPECT_EQ(inside.fetch_add(1), 0);
                {
                    const std::lock_guard inner(mutex);  // повторный вход
                    ++counter;
                }
                inside.fetch_sub(1);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(counter, int64_t{kThreads} * kIterations);
}

// MetaStore работает на честном мьютексе: запись, забор патча и ещё одна запись, вставшие в
// очередь в таком порядке, выполняются в нём же.
TEST(FairMutex, MetaStoreServesWaitersInOrder) {
    const test::TempDir dir;
    MetaStore::create(dir / "index.sqlite", test::test_params());
    MetaStore store(dir / "index.sqlite");

    std::vector<char> order;  // под мьютексом MetaStore
    std::vector<std::thread> threads;
    store.with_lock([&] {
        threads.emplace_back([&] {
            store.write([&](Db& db) {
                order.push_back('w');
                insert_inode(db, test::new_node(kRootIno, "a"));
            });
        });
        ASSERT_TRUE(wait_until([&] { return store.lock_waiters() == 1; }));
        threads.emplace_back([&] {
            store.with_lock([&] {
                order.push_back('t');
                (void)store.take_patchset();
            });
        });
        ASSERT_TRUE(wait_until([&] { return store.lock_waiters() == 2; }));
        threads.emplace_back([&] {
            store.write([&](Db& db) {
                order.push_back('v');
                insert_inode(db, test::new_node(kRootIno, "b"));
            });
        });
        ASSERT_TRUE(wait_until([&] { return store.lock_waiters() == 3; }));
    });
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(order, (std::vector<char>{'w', 't', 'v'}));
}

}  // namespace
}  // namespace telega::meta
