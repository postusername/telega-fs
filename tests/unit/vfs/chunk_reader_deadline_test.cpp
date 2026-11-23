// read_timeout ChunkReader (SPEC 8.2 «Без сети»): один срок от постановки чтения на заголовок,
// данные, повторы и ожидание места среди скачиваний; Store получает его как дедлайн, повтор без
// остатка срока не начинается.

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "crypto/chunk.h"
#include "crypto/keys.h"
#include "meta/errors.h"
#include "store/store.h"
#include "vfs/chunk_reader.h"

namespace telega::vfs {
namespace {

using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;

// Store, у которого каждое чтение заканчивается transient. read_range (прежний вход без срока)
// ждёт собственные own_wait, read_range_until — не дольше срока вызывающего. Если закрыт gate,
// вызов стоит до его открытия, не глядя на срок (застрявшее чтение, занявшее слот).
class FailingStore final : public store::Store {
  public:
    milliseconds own_wait{0};

    store::ChunkRef put_chunk(const std::filesystem::path& /*f*/) override { unsupported(); }
    void read_range(const store::ChunkRef& /*r*/, uint64_t /*off*/,
                    std::span<std::byte> /*out*/) override {
        record(std::nullopt);
        wait_until(Clock::now() + own_wait);
        throw store::StoreError(store::StoreErrorKind::transient, "no network");
    }
    void read_range_until(const store::ChunkRef& /*r*/, uint64_t /*off*/,
                          std::span<std::byte> /*out*/, store::Deadline until) override {
        record(until);
        wait_until(std::min(until, Clock::now() + own_wait));
        throw store::StoreError(store::StoreErrorKind::transient, "no network");
    }
    void delete_messages(std::span<const int64_t> /*ids*/) override { unsupported(); }
    std::optional<store::HeadInfo> read_head() override { unsupported(); }
    Bytes read_index(int64_t /*id*/) override { unsupported(); }
    int64_t put_index(const std::filesystem::path& /*f*/) override { unsupported(); }
    void set_head(int64_t /*n*/, std::optional<int64_t> /*o*/) override { unsupported(); }

    void open_gate() {
        {
            const std::lock_guard lock(mu_);
            gate_open_ = true;
        }
        cv_.notify_all();
    }
    void close_gate_for_calls() { gated_ = true; }
    [[nodiscard]] size_t calls() const {
        const std::lock_guard lock(mu_);
        return calls_;
    }
    [[nodiscard]] std::vector<std::optional<store::Deadline>> deadlines() const {
        const std::lock_guard lock(mu_);
        return deadlines_;
    }

  private:
    [[noreturn]] static void unsupported() { throw std::logic_error("not used"); }
    void record(std::optional<store::Deadline> d) {
        const std::lock_guard lock(mu_);
        ++calls_;
        deadlines_.push_back(d);
    }
    void wait_until(Clock::time_point t) {
        std::unique_lock lock(mu_);
        if (gated_) {
            cv_.wait(lock, [&] { return gate_open_; });  // срок игнорируется: чужое чтение застряло
        } else {
            cv_.wait_until(lock, t, [] { return false; });
        }
    }

    mutable std::mutex mu_;
    std::condition_variable cv_;
    bool gated_ = false;
    bool gate_open_ = false;
    size_t calls_ = 0;
    std::vector<std::optional<store::Deadline>> deadlines_;
};

class ChunkReaderDeadlineTest : public ::testing::Test {
  protected:
    void SetUp() override {
        crypto::init();
        key = crypto::Key::random();
    }

    static meta::Chunk chunk(int64_t id) {
        constexpr uint64_t kPlain = 1000;
        meta::Chunk c;
        c.chunk_id = id;
        c.msg_id = 100 + id;
        c.plain_size = static_cast<int64_t>(kPlain);
        c.enc_size = static_cast<int64_t>(crypto::chunk_encrypted_size(kPlain));
        return c;
    }
    ChunkReader make_reader(milliseconds read_timeout, milliseconds retry_initial,
                            milliseconds retry_max, uint32_t concurrency = 4) {
        ReaderOptions o;
        o.read_timeout = read_timeout;
        o.retry_initial = retry_initial;
        o.retry_max = retry_max;
        o.concurrency = concurrency;
        return ChunkReader(store, key, FsUuid{}, o);
    }
    static int errno_of(ChunkReader& r, const meta::Chunk& c) {
        Bytes buf(100);
        try {
            r.read(c, 0, buf);
        } catch (const meta::FsError& e) {
            return e.err();
        }
        return 0;
    }

    FailingStore store;
    crypto::Key key;
};

// Store ждёт не дольше срока вызывающего: прежний read_range со своими 60 с (здесь own_wait) не
// продлевает чтение сверх read_timeout.
TEST_F(ChunkReaderDeadlineTest, StoreWaitIsLimitedByTheReadDeadline) {
    store.own_wait = milliseconds{2'000};
    auto r = make_reader(milliseconds{300}, milliseconds{50}, milliseconds{50});
    const auto began = Clock::now();
    EXPECT_EQ(errno_of(r, chunk(1)), EIO);
    const auto took = Clock::now() - began;
    EXPECT_GE(took, milliseconds{290});
    EXPECT_LT(took, milliseconds{900});
    EXPECT_EQ(store.calls(), 1U);  // повтора с новым сроком нет
}

// Повтор после transient получает остаток срока, а не новый срок, и не начинается, если остатка
// нет: при сроке 200 мс и паузе 150 мс попыток ровно две (0 и 150 мс), третьей на пороге срока нет.
// Все вызовы получают один и тот же дедлайн.
TEST_F(ChunkReaderDeadlineTest, RetriesShareOneDeadlineAndNoRetryStartsWithoutTimeLeft) {
    auto r = make_reader(milliseconds{200}, milliseconds{150}, milliseconds{150});
    const auto before = Clock::now();
    EXPECT_EQ(errno_of(r, chunk(2)), EIO);
    const auto after = Clock::now();
    EXPECT_EQ(store.calls(), 2U);
    const auto ds = store.deadlines();
    ASSERT_EQ(ds.size(), 2U);
    ASSERT_TRUE(ds[0].has_value());
    ASSERT_TRUE(ds[1].has_value());
    // NOLINTBEGIN(bugprone-unchecked-optional-access): clang-tidy не видит ASSERT_TRUE выше
    EXPECT_EQ(*ds[0], *ds[1]);
    EXPECT_GE(*ds[0], before + milliseconds{200});
    EXPECT_LE(*ds[0], after + milliseconds{200});
    // NOLINTEND(bugprone-unchecked-optional-access)
}

// Ожидание места среди скачиваний входит в тот же срок: читатель, не получивший слот, отвечает EIO
// по сроку, а не ждёт застрявшее чужое чтение.
TEST_F(ChunkReaderDeadlineTest, WaitingForADownloadSlotIsLimitedByTheDeadline) {
    store.close_gate_for_calls();
    store.own_wait = milliseconds{5'000};
    auto r = make_reader(milliseconds{300}, milliseconds{50}, milliseconds{50}, /*concurrency=*/1);
    std::thread holder([&] { (void)errno_of(r, chunk(3)); });  // занимает единственный слот
    while (store.calls() < 1) {
        std::this_thread::yield();
    }
    const auto began = Clock::now();
    EXPECT_EQ(errno_of(r, chunk(4)), EIO);
    EXPECT_LT(Clock::now() - began, milliseconds{800});
    store.open_gate();
    holder.join();
}

}  // namespace
}  // namespace telega::vfs
