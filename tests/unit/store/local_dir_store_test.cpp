// LocalDirStore: раскладка на диске, голова, удаление, инъекция сбоев, потокобезопасность.

#include "store/local_dir_store.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "common/bytes.h"
#include "common/files.h"

namespace telega::store {
namespace {

namespace fs = std::filesystem;
using std::chrono::milliseconds;

Bytes make_bytes(size_t n, uint8_t seed) {
    Bytes b(n);
    for (size_t i = 0; i < n; ++i) {
        b[i] = static_cast<std::byte>((i * 31U + seed) & 0xFFU);
    }
    return b;
}

Bytes text_bytes(const std::string& s) {
    const auto span = as_bytes(s);
    return {span.begin(), span.end()};
}

StoreErrorKind error_kind_of(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const StoreError& e) {
        return e.kind();
    }
    ADD_FAILURE() << "expected StoreError";
    return StoreErrorKind::permanent;
}

// Голова, которая обязана быть; иначе исключение (gtest засчитает его как провал теста).
HeadInfo require_head(LocalDirStore& store) {
    auto head = store.read_head();
    if (!head) {
        throw std::runtime_error("read_head returned nullopt");
    }
    return *std::move(head);
}

class LocalDirStoreTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::string tmpl = (fs::temp_directory_path() / "telega-store-XXXXXX").string();
        ASSERT_NE(::mkdtemp(tmpl.data()), nullptr);
        root_ = tmpl;
        dir_ = root_ / "channel";
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(root_, ec);
    }

    // Файл-источник для put_*.
    fs::path source(const Bytes& data) {
        const auto path = root_ / ("src-" + std::to_string(next_src_++));
        write_file_atomic(path, data);
        return path;
    }

    static Bytes read_all(LocalDirStore& store, int64_t msg_id, size_t size) {
        Bytes out(size);
        store.read_range(ChunkRef{.msg_id = msg_id, .remote_file_id = {}}, 0, out);
        return out;
    }

    static void throwing_crash_handler(LocalDirStore& store) {
        store.set_crash_handler([] { throw CrashInjected(); });
    }

    fs::path root_;
    fs::path dir_;
    std::atomic<int> next_src_{0};
};

TEST_F(LocalDirStoreTest, CreatesLayout) {
    const LocalDirStore store(dir_);
    EXPECT_TRUE(fs::is_directory(dir_ / "msgs"));
    EXPECT_TRUE(fs::exists(dir_ / "next_id"));
    EXPECT_TRUE(store.list_messages().empty());
    EXPECT_TRUE(store.pinned().empty());
    EXPECT_EQ(store.dir(), dir_);
}

TEST_F(LocalDirStoreTest, PutReadRoundTrip) {
    LocalDirStore store(dir_);
    const Bytes data = make_bytes(3 * 1024 * 1024 + 17, 7);  // больше блока копирования
    const auto src = source(data);
    const ChunkRef ref = store.put_chunk(src);
    EXPECT_EQ(ref.msg_id, 1);
    EXPECT_EQ(ref.remote_file_id, "local:1");
    fs::remove(src);  // после возврата источник можно удалять

    EXPECT_EQ(read_all(store, ref.msg_id, data.size()), data);

    Bytes part(100);
    store.read_range(ref, 1000, part);
    EXPECT_TRUE(std::equal(part.begin(), part.end(), data.begin() + 1000));

    // Пустой диапазон на самом конце допустим.
    store.read_range(ref, data.size(), MutableByteSpan{});

    EXPECT_EQ(store.read_index(ref.msg_id), data);
    EXPECT_FALSE(fs::exists(dir_ / "msgs" / "1.tmp"));
}

TEST_F(LocalDirStoreTest, PutIndexRoundTripAndEmptyMessage) {
    LocalDirStore store(dir_);
    const Bytes blob = make_bytes(1000, 3);
    const int64_t id = store.put_index(source(blob));
    EXPECT_EQ(store.read_index(id), blob);

    const int64_t empty_id = store.put_index(source(Bytes{}));
    EXPECT_TRUE(store.read_index(empty_id).empty());
}

TEST_F(LocalDirStoreTest, ReadErrors) {
    LocalDirStore store(dir_);
    const auto ref = store.put_chunk(source(make_bytes(100, 1)));
    Bytes buf(10);

    EXPECT_EQ(error_kind_of(
                  [&] { store.read_range(ChunkRef{.msg_id = 42, .remote_file_id = {}}, 0, buf); }),
              StoreErrorKind::not_found);
    EXPECT_EQ(error_kind_of([&] { (void)store.read_index(42); }), StoreErrorKind::not_found);
    EXPECT_EQ(error_kind_of([&] { store.read_range(ref, 95, buf); }), StoreErrorKind::permanent);
    EXPECT_EQ(error_kind_of([&] { store.read_range(ref, 101, MutableByteSpan{}); }),
              StoreErrorKind::permanent);
    EXPECT_EQ(error_kind_of([&] { store.read_range(ref, UINT64_MAX, buf); }),
              StoreErrorKind::permanent);
    store.read_range(ref, 90, buf);
}

TEST_F(LocalDirStoreTest, PutMissingSourceIsPermanentAndLeavesNoHole) {
    LocalDirStore store(dir_);
    EXPECT_EQ(error_kind_of([&] { (void)store.put_chunk(root_ / "missing"); }),
              StoreErrorKind::permanent);
    EXPECT_EQ(store.put_chunk(source(make_bytes(1, 1))).msg_id, 1);
}

TEST_F(LocalDirStoreTest, IdsIncreaseAndSurviveReopen) {
    {
        LocalDirStore store(dir_);
        EXPECT_EQ(store.put_chunk(source(make_bytes(10, 1))).msg_id, 1);
        EXPECT_EQ(store.put_index(source(make_bytes(10, 2))), 2);
        EXPECT_EQ(store.put_chunk(source(make_bytes(10, 3))).msg_id, 3);
        store.set_head(2, std::nullopt);
    }
    LocalDirStore store(dir_);
    EXPECT_EQ(store.list_messages(), (std::vector<int64_t>{1, 2, 3}));
    EXPECT_EQ(store.pinned(), (std::vector<int64_t>{2}));
    EXPECT_EQ(read_all(store, 3, 10), make_bytes(10, 3));
    EXPECT_EQ(store.put_chunk(source(make_bytes(10, 4))).msg_id, 4);

    // Удалённые id не выдаются повторно.
    const std::vector<int64_t> del{4};
    store.delete_messages(del);
    EXPECT_EQ(LocalDirStore(dir_).put_chunk(source(make_bytes(1, 5))).msg_id, 5);
}

TEST_F(LocalDirStoreTest, NextIdNeverBelowExistingMessages) {
    {
        LocalDirStore store(dir_);
        (void)store.put_chunk(source(make_bytes(10, 1)));
        (void)store.put_chunk(source(make_bytes(10, 2)));
    }
    fs::remove(dir_ / "next_id");
    LocalDirStore store(dir_);
    EXPECT_EQ(store.put_chunk(source(make_bytes(10, 3))).msg_id, 3);
}

TEST_F(LocalDirStoreTest, ListIgnoresLeftoverTmp) {
    LocalDirStore store(dir_);
    (void)store.put_chunk(source(make_bytes(10, 1)));
    write_file_atomic(dir_ / "msgs" / "7.tmp", make_bytes(3, 0));
    EXPECT_EQ(store.list_messages(), (std::vector<int64_t>{1}));
}

TEST_F(LocalDirStoreTest, DeleteMessages) {
    LocalDirStore store(dir_);
    for (uint8_t i = 1; i <= 4; ++i) {
        (void)store.put_chunk(source(make_bytes(10, i)));
    }
    const std::vector<int64_t> del{2, 3, 99};  // 99 нет — не ошибка
    store.delete_messages(del);
    EXPECT_EQ(store.list_messages(), (std::vector<int64_t>{1, 4}));
    EXPECT_EQ(error_kind_of([&] { (void)store.read_index(2); }), StoreErrorKind::not_found);

    store.delete_messages(del);  // повторно — тоже не ошибка
    store.delete_messages(std::span<const int64_t>{});
    EXPECT_EQ(store.list_messages(), (std::vector<int64_t>{1, 4}));
}

TEST_F(LocalDirStoreTest, DeletePinnedUnpinsIt) {
    LocalDirStore store(dir_);
    for (uint8_t i = 1; i <= 3; ++i) {
        (void)store.put_index(source(make_bytes(10, i)));
    }
    store.set_head(1, std::nullopt);
    store.set_head(3, std::nullopt);  // без unpin: закреплены 1 и 3
    EXPECT_EQ(store.pinned(), (std::vector<int64_t>{1, 3}));

    const std::vector<int64_t> del{3};
    store.delete_messages(del);
    EXPECT_EQ(store.pinned(), (std::vector<int64_t>{1}));
    const HeadInfo head = require_head(store);
    EXPECT_EQ(head.msg_id, 1);
}

TEST_F(LocalDirStoreTest, HeadEmpty) {
    LocalDirStore store(dir_);
    EXPECT_FALSE(store.read_head().has_value());
    (void)store.put_index(source(make_bytes(10, 1)));
    EXPECT_FALSE(store.read_head().has_value());
}

TEST_F(LocalDirStoreTest, SetHeadPinsNewUnpinsOld) {
    LocalDirStore store(dir_);
    const Bytes first = make_bytes(1000, 1);
    const Bytes second = make_bytes(1000, 2);
    const int64_t a = store.put_index(source(first));
    const int64_t b = store.put_index(source(second));

    store.set_head(a, std::nullopt);
    HeadInfo head = require_head(store);
    EXPECT_EQ(head.msg_id, a);
    EXPECT_EQ(head.header, Bytes(first.begin(), first.begin() + kHeadHeaderSize));

    store.set_head(b, a);
    EXPECT_EQ(store.pinned(), (std::vector<int64_t>{b}));
    head = require_head(store);
    EXPECT_EQ(head.msg_id, b);
    EXPECT_EQ(head.header, Bytes(second.begin(), second.begin() + kHeadHeaderSize));

    // Повторный set_head того же id и unpin незакреплённого — без ошибок.
    store.set_head(b, b);
    store.set_head(b, 12345);
    EXPECT_EQ(store.pinned(), (std::vector<int64_t>{b}));
}

TEST_F(LocalDirStoreTest, HeadIsLargestPinned) {
    LocalDirStore store(dir_);
    for (uint8_t i = 1; i <= 3; ++i) {
        (void)store.put_index(source(make_bytes(10, i)));
    }
    store.set_head(3, std::nullopt);
    store.set_head(2, std::nullopt);  // старее головы: голова остаётся 3
    const HeadInfo head = require_head(store);
    EXPECT_EQ(head.msg_id, 3);
    EXPECT_EQ(store.pinned(), (std::vector<int64_t>{2, 3}));
}

TEST_F(LocalDirStoreTest, SetHeadMissingIsNotFoundAndKeepsHead) {
    LocalDirStore store(dir_);
    const int64_t a = store.put_index(source(make_bytes(10, 1)));
    store.set_head(a, std::nullopt);
    EXPECT_EQ(error_kind_of([&] { store.set_head(77, a); }), StoreErrorKind::not_found);
    EXPECT_EQ(store.pinned(), (std::vector<int64_t>{a}));
    const HeadInfo head = require_head(store);
    EXPECT_EQ(head.msg_id, a);
}

TEST_F(LocalDirStoreTest, ShortMessageHeader) {
    LocalDirStore store(dir_);
    const Bytes data = text_bytes("short head");
    const int64_t id = store.put_index(source(data));
    store.set_head(id, std::nullopt);
    const HeadInfo head = require_head(store);
    EXPECT_EQ(head.header, data);
}

// ---------------------------------------------------------------------------------------------
// Инъекция сбоев

TEST_F(LocalDirStoreTest, TransientAndPermanentFaultsSkipOperation) {
    LocalDirStore store(dir_);
    store.set_faults({
        FaultRule{.op = StoreOp::put_chunk, .nth = 1, .action = FaultAction::transient},
        FaultRule{.op = StoreOp::put_chunk, .nth = 2, .action = FaultAction::permanent},
    });
    const auto src = source(make_bytes(10, 1));
    EXPECT_EQ(error_kind_of([&] { (void)store.put_chunk(src); }), StoreErrorKind::transient);
    EXPECT_EQ(error_kind_of([&] { (void)store.put_chunk(src); }), StoreErrorKind::permanent);
    EXPECT_TRUE(store.list_messages().empty());
    // Операция не выполнялась, id не расходовались.
    EXPECT_EQ(store.put_chunk(src).msg_id, 1);
    EXPECT_EQ(store.call_count(StoreOp::put_chunk), 3U);
}

TEST_F(LocalDirStoreTest, FaultOnEveryCallAndOtherOpsUnaffected) {
    LocalDirStore store(dir_);
    const int64_t id = store.put_index(source(make_bytes(10, 1)));
    store.set_faults({FaultRule{
        .op = StoreOp::read_index, .nth = std::nullopt, .action = FaultAction::transient}});
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(error_kind_of([&] { (void)store.read_index(id); }), StoreErrorKind::transient);
    }
    Bytes buf(10);
    store.read_range(ChunkRef{.msg_id = id, .remote_file_id = {}}, 0, buf);
    EXPECT_EQ(store.call_count(StoreOp::read_index), 3U);
    EXPECT_EQ(store.call_count(StoreOp::read_range), 1U);

    store.set_faults({});
    EXPECT_EQ(store.read_index(id).size(), 10U);
}

TEST_F(LocalDirStoreTest, FaultsForEachOperation) {
    LocalDirStore store(dir_);
    const int64_t id = store.put_index(source(make_bytes(10, 1)));
    std::vector<FaultRule> rules;
    for (const auto op :
         {StoreOp::put_chunk, StoreOp::read_range, StoreOp::delete_messages, StoreOp::read_head,
          StoreOp::read_index, StoreOp::put_index, StoreOp::set_head}) {
        rules.push_back(FaultRule{.op = op, .nth = std::nullopt, .action = FaultAction::permanent});
    }
    store.set_faults(rules);
    const auto src = source(make_bytes(1, 1));
    Bytes buf(1);
    const std::vector<int64_t> ids{id};
    const std::vector<std::function<void()>> calls{
        [&] { (void)store.put_chunk(src); },
        [&] { store.read_range(ChunkRef{.msg_id = id, .remote_file_id = {}}, 0, buf); },
        [&] { store.delete_messages(ids); },
        [&] { (void)store.read_head(); },
        [&] { (void)store.read_index(id); },
        [&] { (void)store.put_index(src); },
        [&] { store.set_head(id, std::nullopt); },
    };
    for (const auto& call : calls) {
        EXPECT_EQ(error_kind_of(call), StoreErrorKind::permanent);
    }
    EXPECT_EQ(store.list_messages(), (std::vector<int64_t>{id}));
    EXPECT_TRUE(store.pinned().empty());
}

TEST_F(LocalDirStoreTest, CrashBeforeSkipsOperation) {
    LocalDirStore store(dir_);
    throwing_crash_handler(store);
    const int64_t id = store.put_index(source(make_bytes(10, 1)));
    store.set_faults(
        {FaultRule{.op = StoreOp::set_head, .nth = 1, .action = FaultAction::crash_before}});
    EXPECT_THROW(store.set_head(id, std::nullopt), CrashInjected);
    EXPECT_TRUE(store.pinned().empty());
    EXPECT_FALSE(LocalDirStore(dir_).read_head().has_value());

    // Второй вызов правилом nth = 1 не затронут.
    store.set_head(id, std::nullopt);
    EXPECT_EQ(store.pinned(), (std::vector<int64_t>{id}));
}

TEST_F(LocalDirStoreTest, CrashAfterPerformsOperation) {
    LocalDirStore store(dir_);
    throwing_crash_handler(store);
    store.set_faults(
        {FaultRule{.op = StoreOp::put_index, .nth = 2, .action = FaultAction::crash_after}});
    EXPECT_EQ(store.put_index(source(make_bytes(10, 1))), 1);
    EXPECT_THROW((void)store.put_index(source(make_bytes(10, 2))), CrashInjected);
    // Сообщение записано и видно после «перезапуска».
    LocalDirStore reopened(dir_);
    EXPECT_EQ(reopened.list_messages(), (std::vector<int64_t>{1, 2}));
    EXPECT_EQ(reopened.read_index(2), make_bytes(10, 2));
}

TEST_F(LocalDirStoreTest, CrashHandlerThatReturnsStillAborts) {
    LocalDirStore store(dir_);
    int crashes = 0;
    store.set_crash_handler([&crashes] { ++crashes; });
    store.set_faults(
        {FaultRule{
             .op = StoreOp::put_chunk, .nth = std::nullopt, .action = FaultAction::crash_after},
         FaultRule{
             .op = StoreOp::read_head, .nth = std::nullopt, .action = FaultAction::crash_before}});
    EXPECT_THROW((void)store.put_chunk(source(make_bytes(10, 1))), CrashInjected);
    EXPECT_THROW((void)store.read_head(), CrashInjected);
    EXPECT_EQ(crashes, 2);
    EXPECT_EQ(store.list_messages(), (std::vector<int64_t>{1}));
}

TEST_F(LocalDirStoreTest, DelayThenPerform) {
    LocalDirStore store(dir_);
    store.set_faults({FaultRule{.op = StoreOp::put_chunk,
                                .nth = 1,
                                .action = FaultAction::delay,
                                .delay = milliseconds{100}}});
    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(store.put_chunk(source(make_bytes(10, 1))).msg_id, 1);
    EXPECT_GE(std::chrono::steady_clock::now() - start, milliseconds{100});
}

TEST_F(LocalDirStoreTest, DelayDoesNotHoldLock) {
    LocalDirStore store(dir_);
    store.set_faults({FaultRule{.op = StoreOp::read_head,
                                .nth = std::nullopt,
                                .action = FaultAction::delay,
                                .delay = milliseconds{1000}}});
    std::atomic<bool> done{false};
    std::thread slow([&] {
        (void)store.read_head();
        done = true;
    });
    while (store.call_count(StoreOp::read_head) == 0) {
        std::this_thread::yield();
    }
    // Если бы задержка шла под мьютексом, эти вызовы ждали бы конца read_head.
    EXPECT_TRUE(store.pinned().empty());
    EXPECT_EQ(store.put_chunk(source(make_bytes(10, 1))).msg_id, 1);
    EXPECT_FALSE(done.load());
    slow.join();
    EXPECT_TRUE(done.load());
}

// Настоящий _exit(86). Не фикстура: в стиле threadsafe дочерний процесс заново выполняет тест,
// и SetUp фикстуры создал бы в нём свой каталог, который после _exit некому убрать. Каталог
// вычисляется из pid родителя, поэтому в родителе и в дочернем процессе он один и тот же.
fs::path death_test_dir(pid_t parent) {
    return fs::temp_directory_path() / ("telega-store-death-" + std::to_string(parent));
}

TEST(LocalDirStoreDeathTest, DefaultCrashHandlerExits) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto dir = death_test_dir(::getpid());
    std::error_code ec;
    fs::remove_all(dir, ec);
    EXPECT_EXIT(
        {
            const auto child_dir = death_test_dir(::getppid());
            const auto src = child_dir / "src";
            fs::create_directories(child_dir);
            write_file_atomic(src, make_bytes(10, 1));
            LocalDirStore store(child_dir / "channel");
            store.set_faults({FaultRule{
                .op = StoreOp::put_index, .nth = 1, .action = FaultAction::crash_after}});
            (void)store.put_index(src);
        },
        ::testing::ExitedWithCode(86), "");
    // crash_after: операция выполнилась до выхода дочернего процесса.
    EXPECT_EQ(LocalDirStore(dir / "channel").list_messages(), (std::vector<int64_t>{1}));
    fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------------------------
// Разбор TELEGA_FS_FAULTS

TEST(ParseFaultRules, Valid) {
    EXPECT_TRUE(parse_fault_rules("").empty());
    EXPECT_TRUE(parse_fault_rules("   ").empty());
    EXPECT_TRUE(parse_fault_rules(" ; ;").empty());

    const auto rules = parse_fault_rules(
        " put_chunk:3:transient ; read_range:*:permanent;delete_messages:1:crash_before;"
        "read_head:2:crash_after; read_index : * : delay=250 ;put_index:10:permanent;"
        "set_head:1:crash_before;");
    ASSERT_EQ(rules.size(), 7U);
    EXPECT_EQ(rules[0].op, StoreOp::put_chunk);
    EXPECT_EQ(rules[0].nth, 3U);
    EXPECT_EQ(rules[0].action, FaultAction::transient);
    EXPECT_EQ(rules[1].op, StoreOp::read_range);
    EXPECT_EQ(rules[1].nth, std::nullopt);
    EXPECT_EQ(rules[1].action, FaultAction::permanent);
    EXPECT_EQ(rules[2].op, StoreOp::delete_messages);
    EXPECT_EQ(rules[2].action, FaultAction::crash_before);
    EXPECT_EQ(rules[3].op, StoreOp::read_head);
    EXPECT_EQ(rules[3].action, FaultAction::crash_after);
    EXPECT_EQ(rules[4].op, StoreOp::read_index);
    EXPECT_EQ(rules[4].nth, std::nullopt);
    EXPECT_EQ(rules[4].action, FaultAction::delay);
    EXPECT_EQ(rules[4].delay, milliseconds{250});
    EXPECT_EQ(rules[5].op, StoreOp::put_index);
    EXPECT_EQ(rules[5].nth, 10U);
    EXPECT_EQ(rules[6].op, StoreOp::set_head);
}

TEST(ParseFaultRules, Invalid) {
    for (const char* bad : {
             "put_chunk",
             "put_chunk:1",
             "put_chunk:1:transient:extra",
             "write:1:transient",
             "PUT_CHUNK:1:transient",
             "put_chunk:0:transient",
             "put_chunk:-1:transient",
             "put_chunk:+1:transient",
             "put_chunk:x:transient",
             "put_chunk::transient",
             "put_chunk:1.5:transient",
             "put_chunk:99999999999999999999999:transient",
             "put_chunk:1:explode",
             "put_chunk:1:",
             "put_chunk:1:delay",
             "put_chunk:1:delay=",
             "put_chunk:1:delay=-5",
             "put_chunk:1:delay=5ms",
             "put_chunk:1:transient;bogus",
         }) {
        EXPECT_THROW((void)parse_fault_rules(bad), std::invalid_argument) << bad;
    }
    try {
        (void)parse_fault_rules("put_chunk:1:explode");
        FAIL();
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("explode"), std::string::npos) << e.what();
    }
}

// setenv/unsetenv не потокобезопасны, но в этом тесте других потоков нет.
TEST(FaultRulesFromEnv, ReadsVariable) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe): тест однопоточный
    ASSERT_EQ(::unsetenv(kFaultsEnvVar), 0);
    EXPECT_TRUE(fault_rules_from_env().empty());

    // NOLINTNEXTLINE(concurrency-mt-unsafe): тест однопоточный
    ASSERT_EQ(::setenv(kFaultsEnvVar, "set_head:2:crash_before;put_chunk:*:delay=5", 1), 0);
    const auto rules = fault_rules_from_env();
    ASSERT_EQ(rules.size(), 2U);
    EXPECT_EQ(rules[0].op, StoreOp::set_head);
    EXPECT_EQ(rules[0].nth, 2U);
    EXPECT_EQ(rules[0].action, FaultAction::crash_before);
    EXPECT_EQ(rules[1].delay, milliseconds{5});

    // NOLINTNEXTLINE(concurrency-mt-unsafe): тест однопоточный
    ASSERT_EQ(::setenv(kFaultsEnvVar, "nonsense", 1), 0);
    EXPECT_THROW((void)fault_rules_from_env(), std::invalid_argument);

    // NOLINTNEXTLINE(concurrency-mt-unsafe): тест однопоточный
    ASSERT_EQ(::unsetenv(kFaultsEnvVar), 0);
}

// ---------------------------------------------------------------------------------------------
// Потокобезопасность

TEST_F(LocalDirStoreTest, ConcurrentPutAndRead) {
    LocalDirStore store(dir_);
    constexpr int kThreads = 6;
    constexpr int kPerThread = 15;
    std::vector<std::vector<int64_t>> ids(kThreads);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    std::atomic<int> failures{0};
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                const auto seed = static_cast<uint8_t>(t * kPerThread + i);
                const Bytes data = make_bytes(4096 + static_cast<size_t>(i), seed);
                const auto ref = store.put_chunk(source(data));
                ids[static_cast<size_t>(t)].push_back(ref.msg_id);
                Bytes back(data.size());
                store.read_range(ref, 0, back);
                if (back != data) {
                    ++failures;
                }
                if (i % 5 == 0) {
                    store.set_head(ref.msg_id, std::nullopt);
                    (void)store.read_head();
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(failures.load(), 0);

    std::set<int64_t> all;
    for (const auto& per_thread : ids) {
        // Внутри одного потока id строго возрастают.
        EXPECT_TRUE(std::is_sorted(per_thread.begin(), per_thread.end()));
        all.insert(per_thread.begin(), per_thread.end());
    }
    constexpr size_t kTotal = static_cast<size_t>(kThreads) * kPerThread;
    EXPECT_EQ(all.size(), kTotal);
    EXPECT_EQ(*all.begin(), 1);
    EXPECT_EQ(*all.rbegin(), static_cast<int64_t>(kTotal));
    EXPECT_EQ(store.list_messages().size(), kTotal);
    EXPECT_EQ(store.call_count(StoreOp::put_chunk), kTotal);
    const HeadInfo head = require_head(store);
    EXPECT_EQ(head.msg_id, store.pinned().back());
}

}  // namespace
}  // namespace telega::store
