// RateLimiter: два бакета, пауза FLOOD_WAIT, deadline, stop.

#include "store/rate_limiter.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <thread>
#include <vector>

namespace telega::store {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;
using Clock = RateLimiter::Clock;

constexpr uint64_t kMiB = 1ULL << 20;

// Ручные часы для расчётов лимитера без ожидания.
class FakeClock {
  public:
    FakeClock()
        : ns_(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch())
                  .count()) {}
    [[nodiscard]] Clock::time_point now() const {
        return Clock::time_point{std::chrono::nanoseconds{ns_.load()}};
    }
    void advance(std::chrono::milliseconds d) {
        ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
    }
    [[nodiscard]] std::function<Clock::time_point()> fn() {
        return [this] { return now(); };
    }

  private:
    std::atomic<int64_t> ns_;
};

double elapsed_ms(Clock::time_point since) {
    return std::chrono::duration<double, std::milli>(Clock::now() - since).count();
}

RateLimiterOptions fast_actions(double per_minute, double burst) {
    RateLimiterOptions o;
    o.actions_per_minute = per_minute;
    o.actions_burst = burst;
    o.bytes_per_minute = 0;  // байты не ограничены
    o.flood_jitter = 0;
    return o;
}

TEST(RateLimiter, BurstPassesImmediately) {
    RateLimiter limiter(fast_actions(60, 3));
    const auto t0 = Clock::now();
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(limiter.acquire(1, 0));
    }
    EXPECT_LT(elapsed_ms(t0), 200);
}

TEST(RateLimiter, ActionsBucketRefillsAtTheConfiguredRate) {
    RateLimiter limiter(fast_actions(600, 2));  // 10 в секунду
    ASSERT_TRUE(limiter.acquire(1, 0));
    ASSERT_TRUE(limiter.acquire(1, 0));
    const auto t0 = Clock::now();
    ASSERT_TRUE(limiter.acquire(1, 0));
    EXPECT_GE(elapsed_ms(t0), 80);
    EXPECT_LT(elapsed_ms(t0), 1000);
}

TEST(RateLimiter, AlbumCostsMoreActions) {
    RateLimiter limiter(fast_actions(600, 2));
    ASSERT_TRUE(limiter.acquire(kAlbumActions, 0));
    const auto t0 = Clock::now();
    ASSERT_TRUE(limiter.acquire(1, 0));
    EXPECT_GE(elapsed_ms(t0), 80);  // бакет после альбома пуст
}

TEST(RateLimiter, BytesBucketLimitsIndependently) {
    RateLimiterOptions o;
    o.actions_per_minute = 0;      // действия не ограничены
    o.bytes_per_minute = 600'000;  // 10 000 байт в секунду
    o.bytes_burst = 1000;
    RateLimiter limiter(o);
    ASSERT_TRUE(limiter.acquire(1, 1000));
    const auto t0 = Clock::now();
    ASSERT_TRUE(limiter.acquire(1, 1000));
    EXPECT_GE(elapsed_ms(t0), 80);
    EXPECT_LT(elapsed_ms(t0), 1000);
}

TEST(RateLimiter, WaitsForTheSlowerOfTheTwoBuckets) {
    RateLimiterOptions o;
    o.actions_per_minute = 6000;  // 100 в секунду: действия почти не ждут
    o.actions_burst = 1;
    o.bytes_per_minute = 600'000;  // 10 000 байт в секунду
    o.bytes_burst = 2000;
    RateLimiter limiter(o);
    ASSERT_TRUE(limiter.acquire(1, 2000));
    const auto t0 = Clock::now();
    ASSERT_TRUE(limiter.acquire(1, 1500));  // байты: 0,15 с
    EXPECT_GE(elapsed_ms(t0), 120);
}

TEST(RateLimiter, DocumentLargerThanCapacityPassesAndLeavesDebt) {
    RateLimiterOptions o;
    o.actions_per_minute = 0;
    o.bytes_per_minute = 6'000'000;  // 100 000 байт в секунду
    o.bytes_burst = 1000;
    RateLimiter limiter(o);
    const auto t0 = Clock::now();
    ASSERT_TRUE(limiter.acquire(1, 50'000));  // бакет полон: проходит сразу
    EXPECT_LT(elapsed_ms(t0), 200);
    const auto t1 = Clock::now();
    ASSERT_TRUE(limiter.acquire(1, 1));  // долг 49 000 байт гасится 0,49 с
    EXPECT_GE(elapsed_ms(t1), 400);
}

TEST(RateLimiter, DocumentLargerThanCapacityWaitsForFullBucket) {
    RateLimiterOptions o;
    o.actions_per_minute = 0;
    o.bytes_per_minute = 6'000'000;  // 100 000 байт в секунду
    o.bytes_burst = 10'000;
    RateLimiter limiter(o);
    ASSERT_TRUE(limiter.acquire(1, 10'000));  // бакет пуст
    const auto t0 = Clock::now();
    ASSERT_TRUE(limiter.acquire(1, 1'000'000));  // ждёт полного бакета (0,1 с), а не 10 с
    EXPECT_GE(elapsed_ms(t0), 80);
    EXPECT_LT(elapsed_ms(t0), 2000);
}

TEST(RateLimiter, DeadlineExpiresWithoutSpendingTokens) {
    RateLimiter limiter(fast_actions(120, 1));  // 0,5 с на токен
    ASSERT_TRUE(limiter.acquire(1, 0));
    const auto t0 = Clock::now();
    EXPECT_FALSE(limiter.acquire(1, 0, t0 + milliseconds{50}));
    EXPECT_GE(elapsed_ms(t0), 40);
    // Неудачная попытка токенов не списала: следующий проходит ко времени 0,5 с от начала.
    EXPECT_TRUE(limiter.acquire(1, 0, t0 + seconds{3}));
    EXPECT_LT(elapsed_ms(t0), 900);
}

TEST(RateLimiter, DeadlineInThePastActsAsTryAcquire) {
    RateLimiter limiter(fast_actions(1, 1));
    const auto past = Clock::now() - seconds{1};
    EXPECT_TRUE(limiter.acquire(1, 0, past));   // токен есть
    EXPECT_FALSE(limiter.acquire(1, 0, past));  // токена нет
}

TEST(RateLimiter, StopReleasesWaitersAndRefusesLater) {
    RateLimiter limiter(fast_actions(1, 1));  // следующий токен через минуту
    ASSERT_TRUE(limiter.acquire(1, 0));
    std::vector<std::future<bool>> waiters;
    waiters.reserve(3);
    for (int i = 0; i < 3; ++i) {
        waiters.push_back(std::async(std::launch::async, [&] { return limiter.acquire(1, 0); }));
    }
    std::this_thread::sleep_for(milliseconds{100});
    for (auto& w : waiters) {
        EXPECT_EQ(w.wait_for(milliseconds{10}), std::future_status::timeout);
    }
    limiter.stop();
    for (auto& w : waiters) {
        ASSERT_EQ(w.wait_for(seconds{5}), std::future_status::ready);
        EXPECT_FALSE(w.get());
    }
    EXPECT_FALSE(limiter.acquire(0, 0));
}

TEST(RateLimiter, FloodWaitPausesEveryone) {
    RateLimiter limiter(fast_actions(6000, 5));
    const auto t0 = Clock::now();
    limiter.on_flood_wait(seconds{1});
    ASSERT_TRUE(limiter.paused_until().has_value());
    std::vector<std::future<bool>> waiters;
    waiters.reserve(2);
    for (int i = 0; i < 2; ++i) {
        waiters.push_back(std::async(std::launch::async, [&] { return limiter.acquire(1, 0); }));
    }
    for (auto& w : waiters) {
        EXPECT_TRUE(w.get());
    }
    EXPECT_GE(elapsed_ms(t0), 900);
    EXPECT_FALSE(limiter.paused_until().has_value());
}

TEST(RateLimiter, DeadlineShorterThanFloodWaitFails) {
    RateLimiter limiter(fast_actions(6000, 5));
    limiter.on_flood_wait(seconds{30});
    const auto t0 = Clock::now();
    EXPECT_FALSE(limiter.acquire(1, 0, t0 + milliseconds{100}));
    EXPECT_GE(elapsed_ms(t0), 80);
    EXPECT_LT(elapsed_ms(t0), 2000);
}

TEST(RateLimiter, FloodWaitOnFakeClock) {
    FakeClock clock;
    RateLimiter limiter(fast_actions(6000, 5), clock.fn());
    EXPECT_FALSE(limiter.paused_until().has_value());
    limiter.on_flood_wait(seconds{10});
    EXPECT_EQ(limiter.paused_until(), clock.now() + seconds{10});
    // Более короткий retry_after паузу не сокращает.
    limiter.on_flood_wait(seconds{1});
    EXPECT_EQ(limiter.paused_until(), clock.now() + seconds{10});
    // Более длинный продлевает.
    limiter.on_flood_wait(seconds{20});
    EXPECT_EQ(limiter.paused_until(), clock.now() + seconds{20});
    // На паузе acquire с прошедшим deadline сразу отказывает, без блокировки.
    EXPECT_FALSE(limiter.acquire(1, 0, clock.now()));
    clock.advance(milliseconds{19'999});
    EXPECT_TRUE(limiter.paused_until().has_value());
    clock.advance(milliseconds{2});
    EXPECT_FALSE(limiter.paused_until().has_value());
    EXPECT_TRUE(limiter.acquire(1, 0, clock.now()));
}

TEST(RateLimiter, FloodWaitJitterStaysWithinTenPercent) {
    FakeClock clock;
    double max_extra = 0;
    for (int i = 0; i < 200; ++i) {
        RateLimiterOptions o = fast_actions(6000, 5);
        o.flood_jitter = 0.10;
        RateLimiter limiter(o, clock.fn());
        limiter.on_flood_wait(seconds{100});
        const auto paused = limiter.paused_until();
        ASSERT_TRUE(paused.has_value());
        const auto until = paused.value_or(Clock::time_point{});
        const double extra = std::chrono::duration<double>(until - clock.now()).count() - 100.0;
        EXPECT_GE(extra, 0.0);
        EXPECT_LE(extra, 10.0);
        max_extra = std::max(max_extra, extra);
    }
    EXPECT_GT(max_extra, 0.5);  // джиттер действительно добавляется
}

TEST(RateLimiter, BucketsRefillOnFakeClock) {
    FakeClock clock;
    RateLimiter limiter(fast_actions(60, 2), clock.fn());  // 1 в секунду
    EXPECT_TRUE(limiter.acquire(1, 0, clock.now()));
    EXPECT_TRUE(limiter.acquire(1, 0, clock.now()));
    EXPECT_FALSE(limiter.acquire(1, 0, clock.now()));
    clock.advance(milliseconds{999});
    EXPECT_FALSE(limiter.acquire(1, 0, clock.now()));
    clock.advance(milliseconds{2});
    EXPECT_TRUE(limiter.acquire(1, 0, clock.now()));
    // Накопление не превышает ёмкость.
    clock.advance(seconds{3600});
    EXPECT_TRUE(limiter.acquire(1, 0, clock.now()));
    EXPECT_TRUE(limiter.acquire(1, 0, clock.now()));
    EXPECT_FALSE(limiter.acquire(1, 0, clock.now()));
}

TEST(RateLimiter, DefaultsMatchTheSpec) {
    const RateLimiterOptions o;
    EXPECT_DOUBLE_EQ(o.actions_per_minute, 12);
    EXPECT_DOUBLE_EQ(o.actions_burst, 5);
    EXPECT_EQ(o.bytes_per_minute, 224 * kMiB);
    EXPECT_EQ(o.bytes_burst, 64 * kMiB);
}

TEST(RateLimiter, WaitersAreServedInOrder) {
    RateLimiter limiter(fast_actions(600, 2));  // 10 в секунду
    ASSERT_TRUE(limiter.acquire(2, 0));
    std::atomic<int> order{0};
    std::atomic<int> album_position{0};
    std::atomic<int> single_position{0};
    std::thread album([&] {
        EXPECT_TRUE(limiter.acquire(2, 0));
        album_position = ++order;
    });
    std::this_thread::sleep_for(milliseconds{30});
    std::thread single([&] {
        EXPECT_TRUE(limiter.acquire(1, 0));
        single_position = ++order;
    });
    album.join();
    single.join();
    // Одиночное действие не обгоняет альбом, пришедший раньше.
    EXPECT_EQ(album_position.load(), 1);
    EXPECT_EQ(single_position.load(), 2);
}

TEST(RateLimiter, ManyThreadsShareTheBudget) {
    RateLimiter limiter(fast_actions(60'000, 10));  // 1000 в секунду
    std::atomic<int> granted{0};
    std::vector<std::thread> threads;
    threads.reserve(8);
    const auto t0 = Clock::now();
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < 25; ++i) {
                if (limiter.acquire(1, 0)) {
                    ++granted;
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(granted.load(), 200);
    // 200 действий при ёмкости 10 и 1000 в секунду: не меньше 0,19 с.
    EXPECT_GE(elapsed_ms(t0), 150);
}

// pin, unpin и удаление идут без байтов: долг бакета байтов после крупного документа их не держит.
TEST(RateLimiter, ActionWithoutBytesIgnoresTheBytesDebt) {
    RateLimiterOptions o;
    o.actions_per_minute = 600;
    o.actions_burst = 5;
    o.bytes_per_minute = 6'000'000;  // 100 000 байт в секунду
    o.bytes_burst = 1000;
    o.flood_jitter = 0;
    RateLimiter limiter(o);
    ASSERT_TRUE(limiter.acquire(1, 500'000));  // долг около 499 000 байт: 5 секунд
    const auto t0 = Clock::now();
    ASSERT_TRUE(limiter.acquire(1, 0, Clock::now() + milliseconds{500}));
    ASSERT_TRUE(limiter.acquire(1, 0, Clock::now() + milliseconds{500}));
    EXPECT_LT(elapsed_ms(t0), 300);
    // С байтами тот же запрос ждёт погашения долга и не укладывается в deadline.
    EXPECT_FALSE(limiter.acquire(1, 1, Clock::now() + milliseconds{100}));
}

TEST(RateLimiter, ZeroRatesMeanUnlimited) {
    RateLimiterOptions o;
    o.actions_per_minute = 0;
    o.bytes_per_minute = 0;
    RateLimiter limiter(o);
    const auto t0 = Clock::now();
    for (int i = 0; i < 1000; ++i) {
        ASSERT_TRUE(limiter.acquire(10, 1ULL << 40));
    }
    EXPECT_LT(elapsed_ms(t0), 1000);
}

}  // namespace
}  // namespace telega::store
