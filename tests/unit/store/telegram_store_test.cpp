// TelegramStore на фейковом транспорте TDLib (SPEC 3, 4, 7.1, 8.2, 10.1, 10.2).

#include "store/telegram_store.h"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "fake_telegram.h"
#include "meta/sqlite.h"
#include "td/td_runtime.h"

namespace telega::store {
namespace {

// Анализ clang-tidy не понимает ASSERT_TRUE(opt.has_value()) в gtest: доступ к optional после
// утверждения помечается как непроверенный.
// NOLINTBEGIN(bugprone-unchecked-optional-access)

namespace fs = std::filesystem;
using std::chrono::milliseconds;
using std::chrono::seconds;
using td::Json;
using test::FakeTelegram;
using test::kChatId;
using test::td_error;
using Clock = std::chrono::steady_clock;

constexpr const char* kFlood0 = "Too Many Requests: retry after 0";

// Содержимое, зависящее от (seed, позиция): ошибка смещения не пройдёт незамеченной.
std::string pattern(size_t size, uint32_t seed) {
    std::string s(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        s[i] = static_cast<char>((uint64_t{i} * 131U + uint64_t{seed} * 17U + (i >> 8U)) & 0xFFU);
    }
    return s;
}

// Опрос наблюдаемого условия (барьер для тестов с потоками: ждём события, а не время).
template <class P>
bool wait_for(P pred, milliseconds timeout = seconds{10}) {
    const auto deadline = Clock::now() + timeout;
    while (!pred()) {
        if (Clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(milliseconds{1});
    }
    return true;
}

RateLimiterOptions unlimited() {
    RateLimiterOptions o;
    o.actions_per_minute = 0;
    o.bytes_per_minute = 0;
    o.flood_jitter = 0;
    return o;
}

class TelegramStoreTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::string tmpl = "/tmp/telega_tgstore_XXXXXX";
        ASSERT_NE(::mkdtemp(tmpl.data()), nullptr);
        dir = tmpl;
        fs::create_directories(dir / "files");
        fake = std::make_unique<FakeTelegram>(*transport, dir / "dl");
        client = runtime.create_client();
        limiter = std::make_unique<RateLimiter>(unlimited());
    }
    void TearDown() override {
        store.reset();
        runtime.stop();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    static TelegramStoreOptions default_options() {
        TelegramStoreOptions o;
        o.chat_id = kChatId;
        o.album_max_doc_size = 1000;  // порог альбома: 1000 + 40 + 16 = 1056 байт
        o.album_gather = milliseconds{100};
        o.read_timeout = milliseconds{3000};
        o.send_timeout = milliseconds{3000};
        o.call_timeout = milliseconds{1500};
        o.download_failure_guard = milliseconds{20};
        return o;
    }

    TelegramStore& make(const std::optional<TelegramStoreOptions>& options = std::nullopt) {
        store =
            std::make_unique<TelegramStore>(*client, *limiter, options.value_or(default_options()));
        return *store;
    }

    fs::path write_file(const std::string& name, const std::string& content) {
        const fs::path p = dir / "files" / name;
        std::ofstream(p, std::ios::binary) << content;
        return p;
    }
    // Файл размером size (мелкий: ниже порога альбомов, крупный: выше).
    fs::path small_file(const std::string& name, uint32_t seed, size_t size = 200) {
        return write_file(name, pattern(size, seed));
    }
    fs::path big_file(const std::string& name, uint32_t seed, size_t size = 3000) {
        return write_file(name, pattern(size, seed));
    }

    size_t sends() const { return fake->count("sendMessage") + fake->count("sendMessageAlbum"); }
    size_t files_left() const {
        size_t n = 0;
        for (const auto& e : fs::directory_iterator(dir / "files")) {
            n += e.is_regular_file() ? 1 : 0;
        }
        return n;
    }

    template <class F>
    static std::optional<StoreError> error_of(F&& fn) {
        try {
            fn();
        } catch (const StoreError& e) {
            return e;
        }
        return std::nullopt;
    }

    fs::path dir;
    std::shared_ptr<test::FakeTransport> transport = std::make_shared<test::FakeTransport>();
    td::TdRuntime runtime{transport, milliseconds{10}};
    std::unique_ptr<FakeTelegram> fake;
    std::shared_ptr<td::TdClient> client;
    std::unique_ptr<RateLimiter> limiter;
    std::unique_ptr<TelegramStore> store;
};

// ---- open ---------------------------------------------------------------------------------

TEST_F(TelegramStoreTest, OpenAcceptsChannelOwner) {
    auto& s = make();
    EXPECT_NO_THROW(s.open());
    const auto seq = fake->sequence();
    ASSERT_FALSE(seq.empty());
    EXPECT_EQ(seq.front(), "getChat");  // первым запросом по каналу (SPEC 2)
}

TEST_F(TelegramStoreTest, OpenAcceptsAdministratorWithRights) {
    fake->set_member_status(Json{{"@type", "chatMemberStatusAdministrator"},
                                 {"rights",
                                  {{"can_post_messages", true},
                                   {"can_edit_messages", true},
                                   {"can_delete_messages", true}}}});
    EXPECT_NO_THROW(make().open());
}

TEST_F(TelegramStoreTest, OpenRejectsAdministratorWithoutDeleteRight) {
    fake->set_member_status(Json{{"@type", "chatMemberStatusAdministrator"},
                                 {"rights",
                                  {{"can_post_messages", true},
                                   {"can_edit_messages", true},
                                   {"can_delete_messages", false}}}});
    const auto e = error_of([&] { make().open(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    EXPECT_NE(std::string{e->what()}.find("delete messages"), std::string::npos);
}

TEST_F(TelegramStoreTest, OpenRejectsOrdinaryMember) {
    fake->set_member_status(Json{{"@type", "chatMemberStatusMember"}});
    const auto e = error_of([&] { make().open(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
}

TEST_F(TelegramStoreTest, OpenRejectsAutoDelete) {
    fake->set_auto_delete(86400);
    const auto e = error_of([&] { make().open(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    EXPECT_NE(std::string{e->what()}.find("auto-delete"), std::string::npos);
}

TEST_F(TelegramStoreTest, OpenRejectsNonChannel) {
    fake->set_is_channel(false);
    const auto e = error_of([&] { make().open(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
}

TEST_F(TelegramStoreTest, OpenMapsChatErrors) {
    fake->fail_next("getChat", td_error(400, "Chat not found"));
    auto e = error_of([&] { make().open(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);

    fake->fail_next("getChat", td_error(502, "Bad Gateway"));
    e = error_of([&] { make().open(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
}

TEST_F(TelegramStoreTest, MethodsCallGetChatLazilyWithoutOpen) {
    auto& s = make();
    EXPECT_FALSE(s.read_head().has_value());  // 404 — закреплённых нет, а не «Chat not found»
    EXPECT_EQ(fake->sequence().front(), "getChat");
    EXPECT_EQ(fake->count("getChat"), 1U);
    s.read_head();
    EXPECT_EQ(fake->count("getChat"), 1U);
}

// ---- put_chunk / put_index ----------------------------------------------------------------

TEST_F(TelegramStoreTest, PutChunkSingleDocumentRequestFollowsTheSpec) {
    auto& s = make();
    const auto path = big_file("chunk-17.tmp", 1);
    const ChunkRef ref = s.put_chunk(path);

    const auto sent = fake->requests("sendMessage");
    ASSERT_EQ(sent.size(), 1U);
    EXPECT_EQ(fake->count("sendMessageAlbum"), 0U);
    const auto& r = sent[0];
    EXPECT_EQ(r["chat_id"], kChatId);
    EXPECT_EQ(r["options"]["disable_notification"], true);
    const auto& content = r["input_message_content"];
    EXPECT_EQ(content["@type"], "inputMessageDocument");
    EXPECT_FALSE(content.contains("caption"));
    EXPECT_EQ(content["document"]["@type"], "inputDocument");
    EXPECT_EQ(content["document"]["disable_content_type_detection"], true);
    EXPECT_EQ(content["document"]["document"]["@type"], "inputFileLocal");
    const std::string name =
        fs::path(content["document"]["document"]["path"].get<std::string>()).filename().string();
    EXPECT_TRUE(std::regex_match(name, std::regex("[0-9a-f]{16}\\.bin"))) << name;
    EXPECT_NE(name, "chunk-17.tmp");  // имя файла чанка в канал не попадает

    const auto m = fake->message(ref.msg_id);
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->name, name);
    EXPECT_EQ(m->content, pattern(3000, 1));
    EXPECT_EQ(ref.remote_file_id, m->remote_id);
    // Исходный файл цел, временная ссылка убрана.
    EXPECT_TRUE(fs::exists(path));
    EXPECT_EQ(files_left(), 1U);
}

TEST_F(TelegramStoreTest, PutIndexUsesIdxNameAndReturnsMessageId) {
    auto& s = make();
    const int64_t id = s.put_index(big_file("blob", 2, 5000));
    const auto m = fake->message(id);
    ASSERT_TRUE(m.has_value());
    EXPECT_TRUE(std::regex_match(m->name, std::regex("[0-9a-f]{16}\\.idx"))) << m->name;
    EXPECT_EQ(m->content, pattern(5000, 2));
}

TEST_F(TelegramStoreTest, LoneSmallDocumentIsSentAsSingleMessageAfterGathering) {
    auto& s = make();
    const auto t0 = Clock::now();
    const ChunkRef ref = s.put_chunk(small_file("small", 3));
    EXPECT_GE(Clock::now() - t0, milliseconds{80});  // ждал попутчиков album_gather
    EXPECT_EQ(fake->count("sendMessageAlbum"), 0U);
    EXPECT_EQ(fake->count("sendMessage"), 1U);
    EXPECT_EQ(fake->message(ref.msg_id)->content, pattern(200, 3));
}

TEST_F(TelegramStoreTest, ThresholdAccountsForChunkFormatOverhead) {
    auto& s = make();
    // 1000 + 40 + 16 = 1056 байт ещё мелкий документ (альбом), 1057 — уже крупный.
    std::thread other([&] { (void)s.put_chunk(small_file("other", 4, 1056)); });
    (void)s.put_chunk(small_file("edge", 5, 1056));
    other.join();
    EXPECT_EQ(fake->count("sendMessageAlbum"), 1U);
    (void)s.put_chunk(small_file("over", 6, 1057));
    EXPECT_EQ(fake->count("sendMessage"), 1U);  // сразу, без ожидания попутчиков
}

// Запускает по одному потоку на файл и возвращает ChunkRef в порядке файлов.
std::vector<ChunkRef> put_concurrently(TelegramStore& s, const std::vector<fs::path>& files,
                                       std::vector<std::optional<StoreError>>* errors = nullptr) {
    std::vector<ChunkRef> refs(files.size());
    std::vector<std::optional<StoreError>> errs(files.size());
    std::vector<std::thread> threads;
    for (size_t i = 0; i < files.size(); ++i) {
        threads.emplace_back([&, i] {
            try {
                refs[i] = s.put_chunk(files[i]);
            } catch (const StoreError& e) {
                errs[i] = e;
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    if (errors != nullptr) {
        *errors = errs;
    }
    return refs;
}

TEST_F(TelegramStoreTest, ConcurrentSmallDocumentsGoAsOneAlbumWithRightMessageIds) {
    auto o = default_options();
    o.album_gather = milliseconds{500};
    auto& s = make(o);
    std::vector<fs::path> files;
    for (uint32_t i = 0; i < 7; ++i) {
        files.push_back(small_file("c" + std::to_string(i), 10 + i, 150 + i * 13));
    }
    const auto refs = put_concurrently(s, files);
    const auto albums = fake->requests("sendMessageAlbum");
    ASSERT_EQ(albums.size(), 1U);
    EXPECT_EQ(albums[0]["input_message_contents"].size(), 7U);
    EXPECT_EQ(fake->count("sendMessage"), 0U);
    EXPECT_EQ(albums[0]["options"]["disable_notification"], true);
    std::set<int64_t> ids;
    for (uint32_t i = 0; i < files.size(); ++i) {
        const auto m = fake->message(refs[i].msg_id);
        ASSERT_TRUE(m.has_value()) << i;
        EXPECT_EQ(m->content, pattern(150 + i * 13, 10 + i)) << i;  // свой документ у каждого
        EXPECT_EQ(refs[i].remote_file_id, m->remote_id);
        ids.insert(refs[i].msg_id);
    }
    EXPECT_EQ(ids.size(), 7U);
    // У каждого документа альбома имя `<16 hex>.bin`, все разные.
    std::set<std::string> names;
    for (const auto& c : albums[0]["input_message_contents"]) {
        const std::string n =
            fs::path(c["document"]["document"]["path"].get<std::string>()).filename().string();
        EXPECT_TRUE(std::regex_match(n, std::regex("[0-9a-f]{16}\\.bin"))) << n;
        names.insert(n);
    }
    EXPECT_EQ(names.size(), 7U);
    EXPECT_EQ(files_left(), 7U);
}

TEST_F(TelegramStoreTest, FullAlbumOfTenIsSentWithoutWaitingForTheGatherTimeout) {
    auto o = default_options();
    o.album_gather = milliseconds{20'000};
    auto& s = make(o);
    s.open();
    std::vector<fs::path> files;
    for (uint32_t i = 0; i < 10; ++i) {
        files.push_back(small_file("ten" + std::to_string(i), 150 + i, 130 + i));
    }
    const auto t0 = Clock::now();
    const auto refs = put_concurrently(s, files);
    EXPECT_LT(Clock::now() - t0, seconds{10});
    ASSERT_EQ(fake->count("sendMessageAlbum"), 1U);
    EXPECT_EQ(fake->requests("sendMessageAlbum")[0]["input_message_contents"].size(), 10U);
    for (uint32_t i = 0; i < 10; ++i) {
        EXPECT_EQ(fake->message(refs[i].msg_id)->content, pattern(130 + i, 150 + i));
    }
}

TEST_F(TelegramStoreTest, MoreThanTenDocumentsSplitIntoAlbumsOfAtMostTen) {
    auto o = default_options();
    o.album_gather = milliseconds{400};
    auto& s = make(o);
    std::vector<fs::path> files;
    for (uint32_t i = 0; i < 13; ++i) {
        files.push_back(small_file("m" + std::to_string(i), 40 + i, 120 + i));
    }
    const auto refs = put_concurrently(s, files);
    size_t docs = 0;
    for (const auto& a : fake->requests("sendMessageAlbum")) {
        EXPECT_LE(a["input_message_contents"].size(), 10U);
        docs += a["input_message_contents"].size();
    }
    docs += fake->count("sendMessage");
    EXPECT_EQ(docs, 13U);
    for (uint32_t i = 0; i < files.size(); ++i) {
        const auto m = fake->message(refs[i].msg_id);
        ASSERT_TRUE(m.has_value()) << i;
        EXPECT_EQ(m->content, pattern(120 + i, 40 + i)) << i;
    }
    EXPECT_EQ(fake->messages().size(), 13U);
}

TEST_F(TelegramStoreTest, EarlySendResultBeforeTheResponseIsNotLost) {
    fake->set_early_updates(true);
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    const auto single = s.put_chunk(big_file("early-big", 7));
    EXPECT_EQ(fake->message(single.msg_id)->content, pattern(3000, 7));
    const auto refs = put_concurrently(s, {small_file("e1", 8), small_file("e2", 9, 210)});
    EXPECT_EQ(fake->message(refs[0].msg_id)->content, pattern(200, 8));
    EXPECT_EQ(fake->message(refs[1].msg_id)->content, pattern(210, 9));
    EXPECT_EQ(fake->count("sendMessageAlbum"), 1U);
}

TEST_F(TelegramStoreTest, AlbumRejectedWith429IsRepeatedWhole) {
    fake->fail_next("sendMessageAlbum", td_error(429, kFlood0));
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    const auto refs =
        put_concurrently(s, {small_file("a1", 20), small_file("a2", 21), small_file("a3", 22)});
    EXPECT_EQ(fake->count("sendMessageAlbum"), 2U);
    EXPECT_EQ(fake->count("sendMessage"), 0U);
    EXPECT_EQ(fake->messages().size(), 3U);
    for (uint32_t i = 0; i < 3; ++i) {
        EXPECT_EQ(fake->message(refs[i].msg_id)->content, pattern(200, 20 + i));
    }
}

TEST_F(TelegramStoreTest, AlbumWhereEveryDocumentFailsWith429IsRepeatedWhole) {
    std::atomic<int> round{0};
    std::mutex mu;
    std::set<std::string> seen;
    fake->set_verdict([&](const std::string& name, size_t) -> std::optional<Json> {
        const std::lock_guard<std::mutex> lock(mu);
        // Первый раунд: все три документа получают 429 (лимит по действиям, SPEC 10.2).
        if (seen.size() < 3) {
            seen.insert(name);
            round = 1;
            return td_error(429, kFlood0);
        }
        return std::nullopt;
    });
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    const auto refs =
        put_concurrently(s, {small_file("w1", 23), small_file("w2", 24), small_file("w3", 25)});
    EXPECT_EQ(round.load(), 1);
    EXPECT_EQ(fake->count("sendMessageAlbum"), 2U);  // повтор целиком
    EXPECT_EQ(fake->count("sendMessage"), 0U);
    EXPECT_EQ(fake->messages().size(), 3U);
    for (uint32_t i = 0; i < 3; ++i) {
        EXPECT_EQ(fake->message(refs[i].msg_id)->content, pattern(200, 23 + i));
    }
}

TEST_F(TelegramStoreTest, PartialBytesLimitRepeatsOnlyFailedDocumentsOneByOne) {
    // Документ в позиции 1 первым раундом получает 400 retry after (лимит по байтам), остальные
    // доставлены: повтор только неудавшегося, иначе в канале будут дубли (E6).
    std::mutex mu;
    std::string failed_name;
    bool first_round_failed = false;
    fake->set_verdict([&](const std::string& name, size_t index) -> std::optional<Json> {
        const std::lock_guard<std::mutex> lock(mu);
        if (!first_round_failed && index == 1) {
            first_round_failed = true;
            failed_name = name;
            return td_error(400, kFlood0);
        }
        return std::nullopt;
    });
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    std::vector<fs::path> files;
    for (uint32_t i = 0; i < 4; ++i) {
        files.push_back(small_file("p" + std::to_string(i), 30 + i, 100 + i));
    }
    const auto refs = put_concurrently(s, files);
    EXPECT_EQ(fake->count("sendMessageAlbum"), 1U);
    EXPECT_EQ(fake->count("sendMessage"), 1U);  // только неудавшийся, отдельным сообщением
    EXPECT_EQ(fake->messages().size(), 4U);  // без дублей
    std::set<std::string> contents;
    for (uint32_t i = 0; i < 4; ++i) {
        const auto m = fake->message(refs[i].msg_id);
        ASSERT_TRUE(m.has_value()) << i;
        EXPECT_EQ(m->content, pattern(100 + i, 30 + i)) << i;
        contents.insert(m->content);
    }
    EXPECT_EQ(contents.size(), 4U);
}

TEST_F(TelegramStoreTest, FloodWaitOnAlbumPausesTheLimiterForRetryAfter) {
    fake->fail_next("sendMessageAlbum", td_error(429, "Too Many Requests: retry after 1"));
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    const auto t0 = Clock::now();
    (void)put_concurrently(s, {small_file("f1", 50), small_file("f2", 51)});
    EXPECT_GE(Clock::now() - t0, milliseconds{900});
    EXPECT_EQ(fake->count("sendMessageAlbum"), 2U);
}

TEST_F(TelegramStoreTest, FloodWaitOnSingleDocumentIsRetriedTransparently) {
    fake->fail_next("sendMessage", td_error(400, kFlood0));  // код 400 тоже лимит (E5)
    auto& s = make();
    const ChunkRef ref = s.put_chunk(big_file("flood-big", 52));
    EXPECT_EQ(fake->count("sendMessage"), 2U);
    EXPECT_EQ(fake->messages().size(), 1U);
    EXPECT_EQ(fake->message(ref.msg_id)->content, pattern(3000, 52));
}

TEST_F(TelegramStoreTest, FloodWaitInUpdateOfSingleDocumentIsRetried) {
    std::atomic<int> n{0};
    fake->set_verdict([&](const std::string&, size_t) -> std::optional<Json> {
        return n++ == 0 ? std::optional<Json>(td_error(429, kFlood0)) : std::nullopt;
    });
    auto& s = make();
    const ChunkRef ref = s.put_chunk(big_file("flood-upd", 53));
    EXPECT_EQ(fake->count("sendMessage"), 2U);
    EXPECT_EQ(fake->message(ref.msg_id)->content, pattern(3000, 53));
}

TEST_F(TelegramStoreTest, UnusableFilesAreRejectedBeforeAnyCall) {
    auto& s = make();
    const fs::path missing = dir / "files" / "missing";
    const fs::path empty = write_file("empty", "");
    fs::create_directories(dir / "files" / "subdir");
    for (const fs::path& p : {missing, empty, dir / "files" / "subdir"}) {
        const auto e = error_of([&] { (void)s.put_chunk(p); });
        ASSERT_TRUE(e.has_value()) << p;
        EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
        EXPECT_EQ(std::string{e->what()}.find(dir.string()),
                  std::string::npos);  // пути в тексте нет
    }
    const auto e = error_of([&] { (void)s.put_index(missing); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    EXPECT_EQ(sends(), 0U);
}

TEST_F(TelegramStoreTest, FileSpoiledWhileGatheringFailsAloneAndNeverReachesTheAlbumCall) {
    auto o = default_options();
    o.album_gather = milliseconds{600};
    auto& s = make(o);
    const auto victim = small_file("victim", 60);
    std::optional<StoreError> victim_error;
    std::thread a([&] { victim_error = error_of([&] { (void)s.put_chunk(victim); }); });
    std::this_thread::sleep_for(milliseconds{150});
    // Файл портится после проверки, но до отправки (тот же inode, что и у ссылки).
    fs::resize_file(victim, 0);
    ChunkRef good_ref;
    std::thread b([&] { good_ref = s.put_chunk(small_file("good", 61)); });
    a.join();
    b.join();
    ASSERT_TRUE(victim_error.has_value());
    EXPECT_EQ(victim_error->kind(), StoreErrorKind::permanent);
    EXPECT_EQ(fake->count("sendMessageAlbum"), 0U);  // альбом с негодным файлом не отправлялся
    EXPECT_EQ(fake->count("sendMessage"), 1U);
    EXPECT_EQ(fake->message(good_ref.msg_id)->content, pattern(200, 61));
}

TEST_F(TelegramStoreTest, FileThatFailsInsideTheAlbumFailsOnlyItsOwnCaller) {
    // TDLib принял альбом, но один документ дал updateMessageSendFailed (файл испорчен после
    // вызова).
    fake->set_verdict([&](const std::string&, size_t index) -> std::optional<Json> {
        return index == 0 ? std::optional<Json>(td_error(400, "Wrong file")) : std::nullopt;
    });
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    std::vector<std::optional<StoreError>> errors;
    const auto refs = put_concurrently(
        s, {small_file("g1", 62), small_file("g2", 63), small_file("g3", 64)}, &errors);
    const auto failed = static_cast<size_t>(
        std::count_if(errors.begin(), errors.end(), [](const auto& e) { return e.has_value(); }));
    EXPECT_EQ(failed, 1U);
    for (size_t i = 0; i < 3; ++i) {
        const auto& err = errors[i];
        if (err.has_value()) {
            EXPECT_EQ(err->kind(), StoreErrorKind::permanent);
        } else {
            EXPECT_TRUE(fake->message(refs[i].msg_id));
        }
    }
    EXPECT_EQ(fake->messages().size(), 2U);
}

TEST_F(TelegramStoreTest, AlbumRejectedWith400FallsBackToSingleMessages) {
    fake->fail_next("sendMessageAlbum", td_error(400, "InputFile is not specified"));
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    const auto refs = put_concurrently(s, {small_file("r1", 65), small_file("r2", 66)});
    EXPECT_EQ(fake->count("sendMessageAlbum"), 1U);
    EXPECT_EQ(fake->count("sendMessage"), 2U);
    EXPECT_EQ(fake->message(refs[0].msg_id)->content, pattern(200, 65));
    EXPECT_EQ(fake->message(refs[1].msg_id)->content, pattern(200, 66));
}

TEST_F(TelegramStoreTest, SendErrorsAreMapped) {
    auto& s = make();
    fake->set_verdict([](const std::string&, size_t) -> std::optional<Json> {
        return td_error(400, "FILE_PART_INVALID");
    });
    auto e = error_of([&] { (void)s.put_chunk(big_file("perm", 70)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    EXPECT_EQ(fake->count("sendMessage"), 1U);  // 4xx не повторяется

    fake->set_verdict([](const std::string&, size_t) -> std::optional<Json> {
        return td_error(502, "Bad Gateway");
    });
    e = error_of([&] { (void)s.put_chunk(big_file("trans", 71)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);

    fake->set_verdict(nullptr);
    fake->fail_next("sendMessage", td_error(500, "Internal"));
    e = error_of([&] { (void)s.put_chunk(big_file("trans2", 72)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    EXPECT_EQ(files_left(), 3U);  // временные ссылки убраны и после ошибок
}

TEST_F(TelegramStoreTest, SendTimeoutsAreTransient) {
    auto o = default_options();
    o.call_timeout = milliseconds{200};
    o.send_timeout = milliseconds{300};
    auto& s = make(o);
    s.open();
    fake->drop_requests("sendMessage");
    auto e = error_of([&] { (void)s.put_chunk(big_file("t1", 73)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    fake->drop_requests("sendMessage", false);
    fake->set_suppress_updates(true);  // сообщение отправлено, итог не приходит
    e = error_of([&] { (void)s.put_chunk(big_file("t2", 74)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    EXPECT_EQ(files_left(), 2U);
    // Потерянный итог: временное сообщение удалено ДО того, как вызывающий получил transient
    // (SPEC 10.2), иначе поздно дошедшая отправка оставила бы дубль.
    const auto deletes = fake->requests("deleteMessages");
    ASSERT_EQ(deletes.size(), 1U);
    EXPECT_EQ(deletes[0]["message_ids"].size(), 1U);
    EXPECT_GE(deletes[0]["message_ids"][0].get<int64_t>(), int64_t{1} << 40);  // временный id
    EXPECT_TRUE(deletes[0]["revoke"].get<bool>());
}

// Итог отправки пришёл после таймаута. Заливку отменяет удаление временного сообщения, и в канале
// не остаётся ни дубля, ни сироты.
TEST_F(TelegramStoreTest, LateSendResultAfterTimeoutLeavesNoMessage) {
    auto o = default_options();
    o.send_timeout = milliseconds{200};
    auto& s = make(o);
    s.open();
    fake->set_update_delay(milliseconds{600});
    const auto e = error_of([&] { (void)s.put_chunk(big_file("late1", 75)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    std::this_thread::sleep_for(milliseconds{900});  // срок, когда итог всё равно пришёл бы
    EXPECT_TRUE(fake->messages().empty());
    EXPECT_EQ(files_left(), 1U);
}

// Худший случай: итог уже в пути, и удаление временного id ничего не отменяет. Итог оседает в
// трекере, и ближайший вызов (здесь повтор отправки) удаляет настоящее сообщение.
TEST_F(TelegramStoreTest, LateResultThatCannotBeCancelledIsDeletedByTheNextCall) {
    auto o = default_options();
    o.send_timeout = milliseconds{200};
    auto& s = make(o);
    s.open();
    fake->set_cancel_on_delete(false);
    fake->set_update_delay(milliseconds{500});
    const auto e = error_of([&] { (void)s.put_chunk(big_file("late2", 76)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    std::this_thread::sleep_for(milliseconds{800});
    ASSERT_EQ(fake->messages().size(), 1U);  // поздний итог создал сообщение

    fake->set_update_delay(milliseconds{0});
    const ChunkRef ref = s.put_chunk(big_file("late2-retry", 77));  // повтор вызывающего
    const auto left = fake->messages();
    ASSERT_EQ(left.size(), 1U);  // осталось только сообщение повтора
    EXPECT_EQ(left[0].id, ref.msg_id);
    EXPECT_EQ(left[0].content, pattern(3000, 77));
}

// Ответ на sendMessage потерян (таймаут вызова), id временного сообщения неизвестен: итог,
// пришедший апдейтом, опознаётся по имени файла.
TEST_F(TelegramStoreTest, SendWhoseResponseIsLostIsCleanedUpByDocumentName) {
    auto o = default_options();
    o.call_timeout = milliseconds{200};
    auto& s = make(o);
    s.open();
    fake->set_lose_send_response(true);
    const auto e = error_of([&] { (void)s.put_chunk(big_file("lost", 78)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    EXPECT_TRUE(fake->messages().empty());  // доставленный документ удалён
    EXPECT_EQ(fake->count("deleteMessages"), 1U);
}

TEST_F(TelegramStoreTest, AlbumDocumentsWithoutResultAreDeletedBeforeTransient) {
    auto o = default_options();
    o.send_timeout = milliseconds{200};
    o.album_gather = milliseconds{200};
    auto& s = make(o);
    s.open();
    fake->set_suppress_updates(true);
    std::vector<std::optional<StoreError>> errors;
    (void)put_concurrently(s, {small_file("a1", 79), small_file("a2", 80), small_file("a3", 81)},
                           &errors);
    for (const auto& e : errors) {
        ASSERT_TRUE(e.has_value());
        EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    }
    ASSERT_EQ(fake->count("sendMessageAlbum"), 1U);
    const auto deletes = fake->requests("deleteMessages");
    ASSERT_EQ(deletes.size(), 1U);
    EXPECT_EQ(deletes[0]["message_ids"].size(), 3U);
}

// send_timeout растёт с размером документа (SPEC 10.2): 3000 байт при 2000 Б/с дают 1,5 с
// к базе 150 мс, итог через 600 мс ещё вовремя. На быстром канале (1 МиБ/с) тот же итог опоздал бы.
TEST_F(TelegramStoreTest, SendTimeoutGrowsWithTheDocumentSize) {
    auto o = default_options();
    o.send_timeout = milliseconds{150};
    o.send_min_rate = 2000;
    auto& s = make(o);
    s.open();
    fake->set_update_delay(milliseconds{600});
    const ChunkRef ref = s.put_chunk(big_file("slow", 82));
    EXPECT_EQ(fake->message(ref.msg_id)->content, pattern(3000, 82));

    o.send_min_rate = 1ULL << 20;
    auto& fast = make(o);
    const auto e = error_of([&] { (void)fast.put_chunk(big_file("slow2", 83)); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
}

// Исключение посреди deliver(): в catch нельзя трогать задания, которым итог уже отдан, их
// UploadJob (на стеке вызывающего put_chunk) к этому моменту может быть уничтожен. Ломаем
// лимитер после второй отправки, пока первые два вызывающих уже вернулись.
TEST_F(TelegramStoreTest, ExceptionInDeliverNeverTouchesFinishedJobs) {
    std::atomic<bool> armed{false};
    limiter = std::make_unique<RateLimiter>(unlimited(), [&]() -> RateLimiter::Clock::time_point {
        if (armed.load()) {
            std::this_thread::sleep_for(milliseconds{150});  // вызывающие успевают вернуться
            throw std::runtime_error("clock failure");
        }
        return Clock::now();
    });
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    s.open();
    // Альбом отвергнут: задания идут по одному, и итог каждого отдаётся по мере отправки.
    fake->fail_next("sendMessageAlbum", td_error(400, "album rejected"));
    std::atomic<int> sends{0};
    fake->set_verdict([&](const std::string&, size_t) -> std::optional<Json> {
        if (++sends == 2) {
            armed = true;  // третья отправка не начнётся: лимитер бросит
        }
        return std::nullopt;
    });
    std::vector<std::optional<StoreError>> errors;
    const auto refs = put_concurrently(
        s, {small_file("x1", 84), small_file("x2", 85), small_file("x3", 86)}, &errors);
    int failed = 0;
    for (size_t i = 0; i < errors.size(); ++i) {
        if (errors[i]) {
            ++failed;
            EXPECT_EQ(errors[i]->kind(), StoreErrorKind::permanent);
            EXPECT_NE(std::string{errors[i]->what()}.find("internal error"), std::string::npos);
        } else {
            EXPECT_TRUE(fake->message(refs[i].msg_id).has_value());
        }
    }
    EXPECT_EQ(failed, 1);
    armed = false;
}

TEST_F(TelegramStoreTest, UploadConcurrencyIsBounded) {
    fake->set_send_delay(milliseconds{150});
    auto o = default_options();
    o.upload_concurrency = 2;
    auto& s = make(o);
    s.open();
    std::vector<fs::path> files;
    for (uint32_t i = 0; i < 6; ++i) {
        files.push_back(big_file("u" + std::to_string(i), 80 + i));
    }
    const auto refs = put_concurrently(s, files);
    EXPECT_EQ(fake->max_inflight_sends(), 2);
    for (uint32_t i = 0; i < 6; ++i) {
        EXPECT_EQ(fake->message(refs[i].msg_id)->content, pattern(3000, 80 + i));
    }
}

TEST_F(TelegramStoreTest, UploadConcurrencyAppliesToAlbumsToo) {
    fake->set_send_delay(milliseconds{150});
    auto o = default_options();
    o.upload_concurrency = 1;
    o.album_gather = milliseconds{50};
    auto& s = make(o);
    s.open();
    std::vector<fs::path> files;
    for (uint32_t i = 0; i < 25; ++i) {
        files.push_back(small_file("v" + std::to_string(i), 90 + i, 120 + i));
    }
    const auto refs = put_concurrently(s, files);
    EXPECT_EQ(fake->max_inflight_sends(), 1);
    for (uint32_t i = 0; i < 25; ++i) {
        EXPECT_EQ(fake->message(refs[i].msg_id)->content, pattern(120 + i, 90 + i));
    }
}

TEST_F(TelegramStoreTest, LimiterChargesOneActionPerSingleAndTwoPerAlbumPlusBytes) {
    RateLimiterOptions lo;
    lo.actions_per_minute = 60;
    lo.actions_burst = 2;
    lo.bytes_per_minute = 6000;  // 100 байт в секунду
    lo.bytes_burst = 100'000;
    lo.flood_jitter = 0;
    limiter = std::make_unique<RateLimiter>(lo);
    auto o = default_options();
    o.album_gather = milliseconds{300};
    auto& s = make(o);
    s.open();
    // Альбом из трёх документов по 200 байт: 2 действия и 600 байт.
    (void)put_concurrently(s,
                           {small_file("l1", 100), small_file("l2", 101), small_file("l3", 102)});
    ASSERT_EQ(fake->count("sendMessageAlbum"), 1U);
    EXPECT_FALSE(limiter->acquire(1, 0, Clock::now() + milliseconds{50}));  // бакет действий пуст
    EXPECT_TRUE(limiter->acquire(0, 99'000, Clock::now()));  // 100 000 − 600 байт осталось
    EXPECT_FALSE(limiter->acquire(0, 100'000, Clock::now() + milliseconds{50}));
}

TEST_F(TelegramStoreTest, LimiterChargesOneActionAndFileSizeForSingleDocument) {
    RateLimiterOptions lo;
    lo.actions_per_minute = 60;
    lo.actions_burst = 2;
    lo.bytes_per_minute = 0;
    lo.flood_jitter = 0;
    limiter = std::make_unique<RateLimiter>(lo);
    auto& s = make();
    (void)s.put_chunk(big_file("one", 103));
    EXPECT_TRUE(limiter->acquire(1, 0, Clock::now()));  // остался один токен
    EXPECT_FALSE(limiter->acquire(1, 0, Clock::now()));
}

TEST_F(TelegramStoreTest, BlockedPutChunkIsReleasedByLimiterStop) {
    RateLimiterOptions lo;
    lo.actions_per_minute = 1;
    lo.actions_burst = 1;
    lo.bytes_per_minute = 0;
    limiter = std::make_unique<RateLimiter>(lo);
    auto& s = make();
    (void)s.put_chunk(big_file("first", 104));
    std::optional<StoreError> error;
    std::thread blocked(
        [&] { error = error_of([&] { (void)s.put_chunk(big_file("second", 105)); }); });
    std::this_thread::sleep_for(milliseconds{200});
    limiter->stop();
    blocked.join();
    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(error->kind(), StoreErrorKind::transient);
    EXPECT_EQ(fake->count("sendMessage"), 1U);
}

// ---- read_range ---------------------------------------------------------------------------

class ReadTest : public TelegramStoreTest {
  protected:
    static constexpr size_t kSize = 5000;
    void SetUp() override {
        TelegramStoreTest::SetUp();
        content = pattern(kSize, 200);
        msg_id = fake->seed_document(content);
        remote_id = fake->message(msg_id)->remote_id;
        make();
    }

    std::string read(const ChunkRef& ref, uint64_t offset, size_t len) {
        std::string out(len, '\0');
        store->read_range(ref, offset, std::as_writable_bytes(std::span{out}));
        return out;
    }

    std::string content;
    int64_t msg_id = 0;
    std::string remote_id;
};

TEST_F(ReadTest, RemoteFileIdWithKnownSizeSkipsGetMessages) {
    const ChunkRef ref{msg_id, remote_id};
    EXPECT_EQ(read(ref, 1234, 777), content.substr(1234, 777));
    EXPECT_EQ(fake->count("getRemoteFile"), 1U);
    EXPECT_EQ(fake->count("getMessages"), 0U);
    const auto d = fake->requests("downloadFile");
    ASSERT_EQ(d.size(), 1U);
    EXPECT_EQ(d[0]["offset"], 1234);
    EXPECT_EQ(d[0]["limit"], 777);
    EXPECT_EQ(d[0]["synchronous"], false);  // планировщик качает только асинхронно (SPEC 8.2)
    // Второе чтение берёт file_id из кэша процесса.
    EXPECT_EQ(read(ref, 0, 100), content.substr(0, 100));
    EXPECT_EQ(fake->count("getRemoteFile"), 1U);
}

TEST_F(ReadTest, RemoteFileWithZeroSizeFallsBackToGetMessages) {
    fake->set_remote_size_zero(true);  // пустая БД TDLib: размер 0 и негодный file_id (E4)
    const ChunkRef ref{msg_id, remote_id};
    EXPECT_EQ(read(ref, kSize - 100, 100), content.substr(kSize - 100));
    EXPECT_EQ(fake->count("getRemoteFile"), 1U);
    EXPECT_EQ(fake->count("getMessages"), 1U);
    const auto seq = fake->sequence();
    const auto rf = std::find(seq.begin(), seq.end(), "getRemoteFile");
    const auto gm = std::find(seq.begin(), seq.end(), "getMessages");
    EXPECT_LT(rf, gm);
    // file_id из getMessages запомнен: повторное чтение обходится без новых запросов.
    EXPECT_EQ(read(ref, 10, 20), content.substr(10, 20));
    EXPECT_EQ(fake->count("getMessages"), 1U);
    EXPECT_EQ(fake->count("getRemoteFile"), 1U);
}

TEST_F(ReadTest, EmptyRemoteFileIdUsesGetMessages) {
    const ChunkRef ref{msg_id, ""};
    EXPECT_EQ(read(ref, 3, 4000), content.substr(3, 4000));
    EXPECT_EQ(fake->count("getRemoteFile"), 0U);
    EXPECT_EQ(fake->count("getMessages"), 1U);
}

TEST_F(ReadTest, UnknownRemoteFileIdFallsBackToGetMessages) {
    const ChunkRef ref{msg_id, "stale-remote-id"};
    EXPECT_EQ(read(ref, 0, 50), content.substr(0, 50));
    EXPECT_EQ(fake->count("getRemoteFile"), 1U);
    EXPECT_EQ(fake->count("getMessages"), 1U);
}

TEST_F(ReadTest, LimitNeverExceedsTheRemainderAndReadingPastTheEndIsPermanent) {
    const ChunkRef ref{msg_id, remote_id};
    EXPECT_EQ(read(ref, kSize - 10, 10), content.substr(kSize - 10));  // ровно до конца
    const auto d = fake->requests("downloadFile");
    ASSERT_EQ(d.size(), 1U);
    EXPECT_EQ(d[0]["offset"].get<int64_t>() + d[0]["limit"].get<int64_t>(),
              static_cast<int64_t>(kSize));

    const size_t before = fake->count("downloadFile");
    auto e = error_of([&] { (void)read(ref, kSize - 10, 11); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    e = error_of([&] { (void)read(ref, kSize + 1, 1); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    EXPECT_EQ(fake->count("downloadFile"), before);  // до скачивания не дошло
}

TEST_F(ReadTest, EmptyReadDoesNothing) {
    const ChunkRef ref{msg_id, remote_id};
    store->read_range(ref, 0, {});
    EXPECT_TRUE(fake->sequence().empty());
}

TEST_F(ReadTest, DownloadErrorRefreshesFileIdFromTheMessageAndRetries) {
    const ChunkRef ref{msg_id, remote_id};
    fake->fail_next("downloadFile", td_error(400, "FILE_REFERENCE_EXPIRED"));
    EXPECT_EQ(read(ref, 100, 200), content.substr(100, 200));
    EXPECT_EQ(fake->count("downloadFile"), 2U);
    EXPECT_EQ(fake->count("getMessages"), 1U);
}

TEST_F(ReadTest, TwoDownloadErrorsInARowAreReportedByKind) {
    const ChunkRef ref{msg_id, remote_id};
    fake->fail_next("downloadFile", td_error(500, "Internal"), 2);
    auto e = error_of([&] { (void)read(ref, 0, 10); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    fake->fail_next("downloadFile", td_error(400, "bad"), 2);
    e = error_of([&] { (void)read(ref, 0, 10); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    EXPECT_EQ(read(ref, 0, 10), content.substr(0, 10));  // и дальше всё работает
}

// Отмена скачивания (TDLib, E2E, E7) transient: чтение повторяет её с новым file_id (до 5
// кругов и до своего дедлайна), а не отдаёт EIO после второй.
TEST_F(ReadTest, CanceledDownloadsAreRetriedWithFreshFileIdUntilTheyPass) {
    const ChunkRef ref{msg_id, remote_id};
    fake->fail_next_windows(4);  // асинхронный обрыв: апдейт «не качается», без ошибки
    EXPECT_EQ(read(ref, 100, 200), content.substr(100, 200));
    EXPECT_EQ(fake->count("downloadFile"), 5U);
    EXPECT_GE(fake->count("getMessages"), 1U);  // file_id обновлён из сообщения
    fake->fail_next_windows(3);
    EXPECT_EQ(read(ref, 300, 100), content.substr(300, 100));
}

// «File download has failed or was canceled» TDLib отдаёт на любой неуспех, поэтому повторов
// не больше 5 кругов, даже если до дедлайна далеко (SPEC 10.2).
TEST_F(ReadTest, EndlessCancelsEndAsTransientAfterTheRoundLimit) {
    auto o = default_options();
    o.read_timeout = seconds{30};
    auto& s = make(o);
    fake->fail_next_windows(1000);
    std::string out(10, '\0');
    const auto t0 = Clock::now();
    const auto e = error_of(
        [&] { s.read_range({msg_id, remote_id}, 0, std::as_writable_bytes(std::span{out})); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    EXPECT_EQ(fake->count("downloadFile"), 5U);
    EXPECT_LT(Clock::now() - t0, seconds{5});
}

TEST_F(ReadTest, CancelsStopAtTheDeadlineOfTheCallBeforeTheRoundLimit) {
    auto o = default_options();
    o.read_timeout = milliseconds{200};
    auto& s = make(o);
    fake->fail_next_windows(1000);
    std::string out(10, '\0');
    const auto t0 = Clock::now();
    const auto e = error_of(
        [&] { s.read_range({msg_id, remote_id}, 0, std::as_writable_bytes(std::span{out})); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    EXPECT_LT(fake->count("downloadFile"), 5U);
    EXPECT_LT(Clock::now() - t0, seconds{3});
}

TEST_F(ReadTest, ReadHeadAndReadIndexRetryCanceledDownloadsToo) {
    auto& s = *store;
    const std::string blob = pattern(2000, 305);
    const int64_t id = fake->seed_document(blob);
    fake->seed_pin(id);
    fake->fail_next_windows(3);
    const auto head = s.read_head();
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(head->header.data()), head->header.size()),
              blob.substr(0, 256));
    fake->fail_next_windows(3);
    const Bytes data = s.read_index(id);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.data()), data.size()), blob);
}

// Чанк, который этот процесс только что залил: для TDLib он локальный, а ссылку, с которой он
// ушёл, уже убрали. TDLib сам замечает это перед downloadFile и качает с сервера (E2E), а
// deleteFile для такого файла ничего не делает, поэтому Store его не зовёт.
TEST_F(ReadTest, ChunkJustSentByThisProcessIsReadableAfterTheLinkIsGone) {
    fake->set_sent_files_stale(true);
    const ChunkRef ref = store->put_chunk(big_file("own", 87));
    EXPECT_EQ(read(ref, 100, 200), pattern(3000, 87).substr(100, 200));
    EXPECT_EQ(fake->count("downloadFile"), 1U);
    EXPECT_EQ(fake->count("deleteFile"), 0U);
}

// Если TDLib исчезновение не заметил (гонка), pread даёт ENOENT, и чтение просто повторяет
// downloadFile, не трогая deleteFile.
TEST_F(ReadTest, VanishedLocalFileIsDownloadedAgain) {
    fake->set_sent_files_stale(true);
    const ChunkRef ref = store->put_chunk(big_file("own2", 88));
    fake->set_probe_lies(2);  // «скачано», а файла нет: pread даёт ENOENT, и так дважды
    EXPECT_EQ(read(ref, 100, 200), pattern(3000, 88).substr(100, 200));
    EXPECT_EQ(fake->count("downloadFile"), 1U);

    const int64_t idx = store->put_index(big_file("own-idx", 89, 1500));
    fake->set_probe_lies(1);
    const Bytes data = store->read_index(idx);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.data()), data.size()),
              pattern(1500, 89));
    EXPECT_EQ(fake->count("deleteFile"), 0U);
}

// Файл, который этот процесс отправил, TDLib считает локальным и готовым целиком (getFile и
// getFileDownloadedPrefixSize диск не проверяют), а временной ссылки, с которой он ушёл, уже нет.
// pread даёт ENOENT, и диапазон идёт обычным downloadFile: TDLib сам проверяет путь и качает с
// сервера (FileManager::download -> check_local_location_async). Это чанк, голова и блоб индекса.
TEST_F(ReadTest, FileSentByThisProcessIsDownloadedWhenItsLinkIsGone) {
    const ChunkRef ref = store->put_chunk(big_file("sent-chunk", 90));
    EXPECT_EQ(read(ref, 100, 200), pattern(3000, 90).substr(100, 200));
    EXPECT_EQ(fake->count("downloadFile"), 1U);
    EXPECT_EQ(fake->count("getMessages"), 0U);  // file_id не менялся: повторов по кругам не было
    EXPECT_EQ(fake->count("deleteFile"), 0U);

    const std::string blob = pattern(1500, 91);
    const int64_t idx = store->put_index(big_file("sent-idx", 91, 1500));
    const Bytes data = store->read_index(idx);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.data()), data.size()), blob);
    store->set_head(idx, std::nullopt);
    const auto head = store->read_head();
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(head->msg_id, idx);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(head->header.data()), head->header.size()),
              blob.substr(0, kHeadHeaderSize));
}

// «Скачано», а файла нет и после ответа TDLib на prefix (гонка): диапазон скачивается заново, а
// getFileDownloadedPrefixSize второй раз не спрашивается.
TEST_F(ReadTest, ReadyRangeWithoutLocalFileIsDownloadedOnce) {
    const ChunkRef ref{msg_id, remote_id};
    fake->set_probe_lies(1000);
    EXPECT_EQ(read(ref, 0, 10), content.substr(0, 10));
    EXPECT_EQ(fake->count("getFileDownloadedPrefixSize"), 1U);
    EXPECT_EQ(fake->count("downloadFile"), 1U);
}

TEST_F(ReadTest, DeletedMessageIsNotFound) {
    const int64_t gone = fake->seed_document("x");
    std::vector<int64_t> ids{gone};
    store->delete_messages(ids);
    const ChunkRef ref{gone, ""};
    const auto e = error_of([&] { (void)read(ref, 0, 1); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::not_found);
}

TEST_F(ReadTest, FloodWaitOnDownloadIsWaitedOutAndRetried) {
    const ChunkRef ref{msg_id, remote_id};
    fake->fail_next("downloadFile", td_error(429, kFlood0));
    EXPECT_EQ(read(ref, 7, 7), content.substr(7, 7));
    EXPECT_EQ(fake->count("downloadFile"), 2U);
    EXPECT_EQ(fake->count("getMessages"), 0U);  // flood не повод менять file_id
}

TEST_F(ReadTest, FloodWaitLongerThanReadTimeoutIsTransient) {
    auto o = default_options();
    o.read_timeout = milliseconds{400};
    auto& s = make(o);
    fake->fail_next("downloadFile", td_error(429, "Too Many Requests: retry after 30"));
    std::string out(10, '\0');
    const auto t0 = Clock::now();
    const auto e = error_of(
        [&] { s.read_range({msg_id, remote_id}, 0, std::as_writable_bytes(std::span{out})); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    EXPECT_LT(Clock::now() - t0, seconds{3});
}

TEST_F(ReadTest, FloodWaitIsRespectedByOtherReaders) {
    // После FLOOD_WAIT другие чтения тоже ждут паузу, а не бьют в лимит.
    fake->fail_next("downloadFile", td_error(429, "Too Many Requests: retry after 1"));
    const ChunkRef ref{msg_id, remote_id};
    std::thread first([&] { (void)read(ref, 0, 10); });
    std::this_thread::sleep_for(milliseconds{300});
    const auto t0 = Clock::now();
    (void)read(ref, 20, 10);
    first.join();
    EXPECT_GE(Clock::now() - t0, milliseconds{500});
}

TEST_F(ReadTest, NoAnswerIsTransientTimeout) {
    auto o = default_options();
    o.call_timeout = milliseconds{200};
    o.read_timeout = milliseconds{300};
    auto& s = make(o);
    fake->drop_requests("downloadFile");
    std::string out(10, '\0');
    const auto e = error_of(
        [&] { s.read_range({msg_id, remote_id}, 0, std::as_writable_bytes(std::span{out})); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
}

TEST_F(ReadTest, DownloadConcurrencyIsBounded) {
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    auto o = default_options();
    o.download_concurrency = 2;
    o.read_timeout = seconds{20};
    auto& s = make(o);
    constexpr int kFiles = 6;
    std::vector<ChunkRef> refs;
    std::vector<int32_t> file_ids;
    for (int t = 0; t < kFiles; ++t) {
        const int64_t id = fake->seed_document(content);
        refs.push_back({id, fake->message(id)->remote_id});
        file_ids.push_back(fake->message(id)->file_id);
    }
    std::atomic<int> finished{0};
    std::vector<std::thread> threads;
    threads.reserve(kFiles);
    for (int t = 0; t < kFiles; ++t) {
        threads.emplace_back([&, t] {
            std::string out(10, '\0');
            s.read_range(refs[static_cast<size_t>(t)], static_cast<uint64_t>(t) * 10,
                         std::as_writable_bytes(std::span{out}));
            EXPECT_EQ(out, content.substr(static_cast<size_t>(t) * 10, 10));
            ++finished;
        });
    }
    // Барьер: все шесть читателей ждут, а окон открыто ровно два.
    ASSERT_TRUE(wait_for([&] { return s.download_stats().waiting == kFiles; }));
    // Счётчик запросов растёт при получении, а окно открывается позже: барьер — два одновременно
    // открытых окна у самого TDLib (иначе run_downloads мог бы закончить первое окно до открытия
    // второго, и пик одновременности остался бы 1).
    ASSERT_TRUE(wait_for([&] { return fake->max_active_windows() == 2; }));
    ASSERT_TRUE(wait_for([&] { return s.download_stats().plans_since_event >= 1; }));
    EXPECT_EQ(fake->count("downloadFile"), 2U);
    while (finished.load() < kFiles) {
        fake->run_downloads();
        std::this_thread::yield();
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(fake->max_active_windows(), 2);
    EXPECT_EQ(s.download_stats().max_active, 2U);
}

// Пока общая пауза FLOOD_WAIT не кончилась, планировщик спит до её конца (или до события), а не
// крутит проходы: ждущему читателю с нулевым retry_at раньше «пора проснуться» было в прошлом.
TEST_F(ReadTest, FloodPauseDoesNotMakeTheSchedulerSpin) {
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    auto o = default_options();
    o.download_concurrency = 1;
    o.read_timeout = seconds{20};
    auto& s = make(o);
    std::vector<ChunkRef> refs;
    for (int i = 0; i < 3; ++i) {
        const int64_t id = fake->seed_document(content);
        refs.push_back({id, fake->message(id)->remote_id});
    }
    std::vector<std::thread> threads;
    threads.emplace_back([&] { (void)error_of([&] { (void)read(refs[0], 0, 10); }); });
    ASSERT_TRUE(wait_for([&] { return s.download_stats().active_now == 1; }));
    threads.emplace_back([&] { (void)error_of([&] { (void)read(refs[1], 0, 10); }); });
    ASSERT_TRUE(wait_for([&] { return s.download_stats().waiting == 2; }));
    fake->fail_next("getRemoteFile", td_error(429, "Too Many Requests: retry after 3"));
    threads.emplace_back([&] { (void)error_of([&] { (void)read(refs[2], 0, 10); }); });
    // Пауза началась (флуд пришёл на getRemoteFile третьего читателя).
    ASSERT_TRUE(wait_for([&] { return fake->count("getRemoteFile") >= 3; }));
    std::this_thread::sleep_for(milliseconds{300});
    const uint64_t before = s.download_stats().plans;
    std::this_thread::sleep_for(milliseconds{1500});
    const uint64_t passes = s.download_stats().plans - before;
    // Проход раз в секунду (потолок ожидания) и по событиям; прежде было десятки тысяч.
    EXPECT_LT(passes, 20U);
    std::atomic<bool> done{false};
    std::thread driver([&] {
        while (!done) {
            fake->run_downloads();
            std::this_thread::sleep_for(milliseconds{5});
        }
    });
    for (auto& t : threads) {
        t.join();
    }
    done = true;
    driver.join();
}

TEST_F(ReadTest, ConcurrentReadsReturnTheirOwnRanges) {
    auto o = default_options();
    o.download_concurrency = 3;
    auto& s = make(o);
    const ChunkRef ref{msg_id, remote_id};
    std::vector<std::thread> threads;
    threads.reserve(8);
    std::atomic<int> bad{0};
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < 20; ++i) {
                const auto off = static_cast<size_t>((t * 613 + i * 97) % 4000);
                const auto len = 100 + static_cast<size_t>((t + i) % 7) * 50;
                std::string out(len, '\0');
                s.read_range(ref, off, std::as_writable_bytes(std::span{out}));
                if (out != content.substr(off, len)) {
                    ++bad;
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(bad.load(), 0);
}

// TDLib отменяет прежний downloadFile файла, если пришёл новый запрос на тот же файл («Canceled by
// another downloadFile request», E2E): пользовательское чтение и readahead одного чанка не должны
// идти параллельно, иначе они отменяют друг друга и чтение отдаёт EIO.
TEST_F(ReadTest, ReadsOfTheSameFileDoNotCancelEachOther) {
    fake->set_part_size(256);
    fake->set_part_delay(milliseconds{2});
    auto o = default_options();
    o.download_concurrency = 4;
    auto& s = make(o);
    const ChunkRef ref{msg_id, remote_id};
    std::vector<std::thread> threads;
    threads.reserve(6);
    std::atomic<int> bad{0};
    for (int t = 0; t < 6; ++t) {
        threads.emplace_back([&, t] {
            std::string out(100, '\0');
            try {
                s.read_range(ref, static_cast<uint64_t>(t) * 700,
                             std::as_writable_bytes(std::span{out}));
                if (out != content.substr(static_cast<size_t>(t) * 700, 100)) {
                    ++bad;
                }
            } catch (const StoreError&) {
                ++bad;
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(bad.load(), 0);
}

// ---- read_head / read_index ---------------------------------------------------------------

TEST_F(TelegramStoreTest, ReadHeadWithoutPinnedMessageIsNullopt) {
    auto& s = make();
    EXPECT_FALSE(s.read_head().has_value());
}

TEST_F(TelegramStoreTest, ReadHeadReturnsFirst256BytesOfTheNewestPinned) {
    auto& s = make();
    const int64_t older = fake->seed_document(pattern(4000, 300));
    const std::string newest_content = pattern(6000, 301);
    const int64_t newest = fake->seed_document(newest_content);
    fake->seed_pin(older);
    fake->seed_pin(newest);
    const auto head = s.read_head();
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(head->msg_id, newest);
    ASSERT_EQ(head->header.size(), 256U);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(head->header.data()), head->header.size()),
              newest_content.substr(0, 256));
    const auto d = fake->requests("downloadFile");
    ASSERT_EQ(d.size(), 1U);
    EXPECT_EQ(d[0]["offset"], 0);
    EXPECT_EQ(d[0]["limit"], 256);
}

TEST_F(TelegramStoreTest, ReadHeadOfShortDocumentIsShort) {
    auto& s = make();
    const int64_t id = fake->seed_document(pattern(100, 302));
    fake->seed_pin(id);
    const auto head = s.read_head();
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(head->header.size(), 100U);
}

TEST_F(TelegramStoreTest, ReadHeadMapsErrors) {
    auto& s = make();
    fake->fail_next("getChatPinnedMessage", td_error(500, "Internal"));
    auto e = error_of([&] { (void)s.read_head(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    fake->fail_next("getChatPinnedMessage", td_error(403, "CHAT_ADMIN_REQUIRED"));
    e = error_of([&] { (void)s.read_head(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
}

TEST_F(TelegramStoreTest, ReadIndexReturnsWholeDocument) {
    auto& s = make();
    const std::string blob = pattern(70'000, 303);
    const int64_t id = fake->seed_document(blob);
    const Bytes data = s.read_index(id);
    ASSERT_EQ(data.size(), blob.size());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.data()), data.size()), blob);
    const auto d = fake->requests("downloadFile");
    ASSERT_EQ(d.size(), 1U);
    EXPECT_EQ(d[0]["offset"], 0);
    EXPECT_EQ(d[0]["limit"], 70'000);  // блоб целиком: limit равен размеру файла
}

TEST_F(TelegramStoreTest, ReadIndexOfMissingMessageIsNotFound) {
    auto& s = make();
    const auto e = error_of([&] { (void)s.read_index(999 * test::kShift); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::not_found);
}

TEST_F(TelegramStoreTest, ReadIndexMapsErrorsAndScrubsPaths) {
    auto& s = make();
    const int64_t id = fake->seed_document(pattern(300, 304));
    fake->fail_next("getMessages", td_error(500, "Internal"));
    auto e = error_of([&] { (void)s.read_index(id); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    fake->fail_next("getMessages", td_error(401, "Unauthorized"));
    e = error_of([&] { (void)s.read_index(id); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    fake->fail_next("getMessages",
                    td_error(400, "Can't open file \"/home/someone/secret/dir/x.bin\""));
    e = error_of([&] { (void)s.read_index(id); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    EXPECT_EQ(std::string{e->what()}.find("someone"), std::string::npos);
    EXPECT_NE(std::string{e->what()}.find("<path>"), std::string::npos);
}

TEST_F(TelegramStoreTest, StoppedRuntimeIsPermanent) {
    auto& s = make();
    runtime.stop();
    const auto e = error_of([&] { (void)s.read_head(); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
}

// ---- set_head -----------------------------------------------------------------------------

TEST_F(TelegramStoreTest, SetHeadPinsNewThenUnpinsOldWithoutReloadingKnownMessage) {
    auto& s = make();
    const int64_t old_head = s.put_index(big_file("old", 400));
    s.set_head(old_head, std::nullopt);
    const int64_t new_head = s.put_index(big_file("new", 401));
    s.set_head(new_head, old_head);

    EXPECT_EQ(fake->pinned(), std::vector<int64_t>{new_head});
    const auto seq = fake->sequence();
    const auto pins = fake->requests("pinChatMessage");
    ASSERT_EQ(pins.size(), 2U);
    EXPECT_EQ(pins[1]["message_id"], new_head);
    EXPECT_EQ(pins[1]["disable_notification"], true);
    const auto unpins = fake->requests("unpinChatMessage");
    ASSERT_EQ(unpins.size(), 1U);
    EXPECT_EQ(unpins[0]["message_id"], old_head);
    // pin раньше unpin, getMessages не нужен: сообщение знает этот процесс.
    EXPECT_LT(std::find(seq.rbegin(), seq.rend(), "unpinChatMessage") - seq.rbegin(),
              std::find(seq.rbegin(), seq.rend(), "pinChatMessage") - seq.rbegin());
    EXPECT_EQ(fake->count("getMessages"), 0U);
}

TEST_F(TelegramStoreTest, SetHeadLoadsOldHeadBeforeUnpinAfterRestart) {
    auto& s = make();
    const int64_t old_head = fake->seed_document(pattern(300, 402));
    fake->seed_pin(old_head);  // закреплён прошлым процессом, этому TDLib его не знает
    const int64_t new_head = s.put_index(big_file("new2", 403));
    s.set_head(new_head, old_head);
    EXPECT_EQ(fake->pinned(), std::vector<int64_t>{new_head});
    const auto seq = fake->sequence();
    const auto gm = std::find(seq.begin(), seq.end(), "getMessages");
    const auto un = std::find(seq.begin(), seq.end(), "unpinChatMessage");
    ASSERT_NE(gm, seq.end());
    ASSERT_NE(un, seq.end());
    EXPECT_LT(gm, un);
    EXPECT_LT(std::find(seq.begin(), seq.end(), "pinChatMessage"), gm);  // сперва точка коммита
}

TEST_F(TelegramStoreTest, SetHeadSwallowsUnpinFailure) {
    auto& s = make();
    const int64_t old_head = s.put_index(big_file("o3", 404));
    const int64_t new_head = s.put_index(big_file("n3", 405));
    s.set_head(old_head, std::nullopt);
    fake->fail_next("unpinChatMessage", td_error(400, "Message not found"));
    EXPECT_NO_THROW(s.set_head(new_head, old_head));
    const auto pinned = fake->pinned();
    EXPECT_EQ(pinned.size(), 2U);  // старый остался закреплённым, головой считается новый
    EXPECT_EQ(pinned.back(), new_head);
    fake->fail_next("unpinChatMessage", td_error(500, "Internal"));
    EXPECT_NO_THROW(s.set_head(new_head, old_head));
}

TEST_F(TelegramStoreTest, SetHeadSkipsUnpinOfVanishedOldHead) {
    auto& s = make();
    const int64_t gone = fake->seed_document(pattern(100, 406));
    fake->seed_pin(gone);
    const int64_t new_head = s.put_index(big_file("n4", 407));
    std::vector<int64_t> ids{gone};
    s.delete_messages(ids);
    EXPECT_NO_THROW(s.set_head(new_head, gone));
    EXPECT_EQ(fake->count("unpinChatMessage"), 0U);
    EXPECT_EQ(fake->pinned(), std::vector<int64_t>{new_head});
}

TEST_F(TelegramStoreTest, SetHeadPinFailureIsReportedAndOldIsKept) {
    auto& s = make();
    const int64_t old_head = s.put_index(big_file("o5", 408));
    const int64_t new_head = s.put_index(big_file("n5", 409));
    s.set_head(old_head, std::nullopt);
    fake->fail_next("pinChatMessage", td_error(400, "Message not found"));
    auto e = error_of([&] { s.set_head(new_head, old_head); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    fake->fail_next("pinChatMessage", td_error(500, "Internal"));
    e = error_of([&] { s.set_head(new_head, old_head); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    EXPECT_EQ(fake->count("unpinChatMessage"), 0U);  // unpin только после успешного pin
    EXPECT_EQ(fake->pinned(), std::vector<int64_t>{old_head});
}

TEST_F(TelegramStoreTest, SetHeadWithoutOldHeadDoesNotUnpin) {
    auto& s = make();
    s.set_head(s.put_index(big_file("n6", 410)), std::nullopt);
    EXPECT_EQ(fake->count("unpinChatMessage"), 0U);
    EXPECT_EQ(fake->count("pinChatMessage"), 1U);
}

TEST_F(TelegramStoreTest, SetHeadFloodWaitIsRetried) {
    auto& s = make();
    const int64_t head = s.put_index(big_file("n7", 411));
    fake->fail_next("pinChatMessage", td_error(429, kFlood0));
    EXPECT_NO_THROW(s.set_head(head, std::nullopt));
    EXPECT_EQ(fake->count("pinChatMessage"), 2U);
    EXPECT_EQ(fake->pinned(), std::vector<int64_t>{head});
}

TEST_F(TelegramStoreTest, PinAndUnpinCostOneActionEach) {
    RateLimiterOptions lo;
    lo.actions_per_minute = 60;
    lo.actions_burst = 5;
    lo.bytes_per_minute = 0;
    limiter = std::make_unique<RateLimiter>(lo);
    auto& s = make();
    const int64_t a = s.put_index(big_file("a8", 412));  // 1 действие
    const int64_t b = s.put_index(big_file("b8", 413));  // 1
    s.set_head(a, std::nullopt);                         // 1
    s.set_head(b, a);                                    // pin + unpin = 2: всего 5
    EXPECT_FALSE(limiter->acquire(1, 0, Clock::now()));
}

TEST_F(TelegramStoreTest, SetHeadDoesNotWaitForTheLimiterForever) {
    RateLimiterOptions lo;
    lo.actions_per_minute = 1;
    lo.actions_burst = 1;
    lo.bytes_per_minute = 0;
    limiter = std::make_unique<RateLimiter>(lo);
    auto o = default_options();
    o.send_timeout = milliseconds{300};
    auto& s = make(o);
    const int64_t head = s.put_index(big_file("n9", 414));  // токен потрачен
    const auto e = error_of([&] { s.set_head(head, std::nullopt); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    EXPECT_EQ(fake->count("pinChatMessage"), 0U);
}

// ---- delete_messages ----------------------------------------------------------------------

TEST_F(TelegramStoreTest, DeleteSplitsIntoBatchesOf100WithRevoke) {
    auto& s = make();
    std::vector<int64_t> ids;
    ids.reserve(250);
    for (int i = 0; i < 250; ++i) {
        ids.push_back(fake->seed_document("d" + std::to_string(i)));
    }
    s.delete_messages(ids);
    const auto dels = fake->requests("deleteMessages");
    ASSERT_EQ(dels.size(), 3U);
    EXPECT_EQ(dels[0]["message_ids"].size(), 100U);
    EXPECT_EQ(dels[1]["message_ids"].size(), 100U);
    EXPECT_EQ(dels[2]["message_ids"].size(), 50U);
    for (const auto& d : dels) {
        EXPECT_EQ(d["revoke"], true);
        EXPECT_EQ(d["chat_id"], kChatId);
    }
    EXPECT_TRUE(fake->messages().empty());
    // Сообщения прошлых процессов предварительно загружаются getMessages (SPEC 7.1, E5).
    EXPECT_EQ(fake->count("getMessages"), 3U);
}

TEST_F(TelegramStoreTest, DeleteOfMessagesSentByThisProcessNeedsNoGetMessages) {
    auto& s = make();
    std::vector<int64_t> ids;
    for (uint32_t i = 0; i < 4; ++i) {
        ids.push_back(s.put_chunk(big_file("k" + std::to_string(i), 500 + i)).msg_id);
    }
    s.delete_messages(ids);
    EXPECT_EQ(fake->count("getMessages"), 0U);
    EXPECT_EQ(fake->count("deleteMessages"), 1U);
    EXPECT_TRUE(fake->messages().empty());
}

TEST_F(TelegramStoreTest, DeleteIgnoresMissingMessages) {
    auto& s = make();
    const int64_t alive = fake->seed_document("alive");
    const std::vector<int64_t> mixed{alive, 777 * test::kShift, 778 * test::kShift};
    EXPECT_NO_THROW(s.delete_messages(mixed));
    EXPECT_TRUE(fake->messages().empty());
    ASSERT_EQ(fake->requests("deleteMessages").size(), 1U);
    EXPECT_EQ(fake->requests("deleteMessages")[0]["message_ids"].size(), 1U);

    // Все отсутствуют: удалять нечего, deleteMessages не вызывается.
    const std::vector<int64_t> none{900 * test::kShift};
    EXPECT_NO_THROW(s.delete_messages(none));
    EXPECT_EQ(fake->count("deleteMessages"), 1U);
    EXPECT_NO_THROW(s.delete_messages({}));
}

TEST_F(TelegramStoreTest, DeleteToleratesNotFoundFromTheServer) {
    auto& s = make();
    const int64_t id = s.put_chunk(big_file("nf", 510)).msg_id;
    fake->fail_next("deleteMessages", td_error(400, "Message not found"));
    const std::vector<int64_t> ids{id};
    EXPECT_NO_THROW(s.delete_messages(ids));
}

TEST_F(TelegramStoreTest, DeleteDeduplicatesIds) {
    auto& s = make();
    const int64_t id = s.put_chunk(big_file("dup", 511)).msg_id;
    const std::vector<int64_t> ids{id, id, id};
    s.delete_messages(ids);
    ASSERT_EQ(fake->requests("deleteMessages").size(), 1U);
    EXPECT_EQ(fake->requests("deleteMessages")[0]["message_ids"].size(), 1U);
}

TEST_F(TelegramStoreTest, DeleteMapsErrorsAndRetriesFloodWait) {
    auto& s = make();
    const int64_t id = s.put_chunk(big_file("de", 512)).msg_id;
    const std::vector<int64_t> ids{id};
    fake->fail_next("deleteMessages", td_error(429, kFlood0));
    EXPECT_NO_THROW(s.delete_messages(ids));
    EXPECT_EQ(fake->count("deleteMessages"), 2U);

    const int64_t id2 = s.put_chunk(big_file("de2", 513)).msg_id;
    const std::vector<int64_t> ids2{id2};
    fake->fail_next("deleteMessages", td_error(500, "Internal"));
    auto e = error_of([&] { s.delete_messages(ids2); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::transient);
    fake->fail_next("deleteMessages", td_error(403, "CHAT_ADMIN_REQUIRED"));
    e = error_of([&] { s.delete_messages(ids2); });
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->kind(), StoreErrorKind::permanent);
    EXPECT_NO_THROW(s.delete_messages(ids2));  // повтор после ошибки проходит
}

TEST_F(TelegramStoreTest, DeleteCostsOneActionPerBatch) {
    RateLimiterOptions lo;
    lo.actions_per_minute = 60;
    lo.actions_burst = 3;
    lo.bytes_per_minute = 0;
    limiter = std::make_unique<RateLimiter>(lo);
    auto& s = make();
    std::vector<int64_t> ids;
    ids.reserve(250);
    for (int i = 0; i < 250; ++i) {
        ids.push_back(fake->seed_document("z"));
    }
    s.delete_messages(ids);  // три пачки
    EXPECT_FALSE(limiter->acquire(1, 0, Clock::now()));
}

// ---- Сквозное -----------------------------------------------------------------------------

TEST_F(TelegramStoreTest, RoundTripChunkIndexHeadAndDelete) {
    auto& s = make();
    s.open();
    const std::string plain = pattern(3000, 600);
    const ChunkRef chunk = s.put_chunk(write_file("rt", plain));
    std::string out(500, '\0');
    s.read_range(chunk, 2000, std::as_writable_bytes(std::span{out}));
    EXPECT_EQ(out, plain.substr(2000, 500));

    const std::string blob = pattern(1500, 601);
    const int64_t index = s.put_index(write_file("idx", blob));
    s.set_head(index, std::nullopt);
    const auto head = s.read_head();
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(head->msg_id, index);
    EXPECT_EQ(s.read_index(index).size(), blob.size());

    const std::vector<int64_t> both{chunk.msg_id, index};
    s.delete_messages(both);
    EXPECT_TRUE(fake->messages().empty());
    EXPECT_FALSE(s.read_head().has_value());
}

// ---- Планировщик скачиваний (SPEC 8.2, 10.2) ----------------------------------------------

// Скачиванием фейка управляет тест (manual): порядок исполнения и приоритеты детерминированы.
// Ожидания — барьеры на наблюдаемые события (запрос дошёл до TDLib, читатель встал в очередь,
// планировщик прошёл после постановки запроса), а не паузы.
class SchedTest : public TelegramStoreTest {
  protected:
    static constexpr size_t kSize = 20'000;

    struct Doc {
        ChunkRef ref;
        int32_t fid = 0;
        std::string content;
    };
    struct Reader {
        std::thread thread;
        std::string out;
        std::optional<StoreError> error;
        Clock::time_point began;
        Clock::time_point ended;
        void join() {
            if (thread.joinable()) {
                thread.join();
            }
        }
    };

    void SetUp() override {
        TelegramStoreTest::SetUp();
        fake->set_download_mode(FakeTelegram::DownloadMode::manual);
        fake->set_part_size(1000);
        auto o = default_options();
        o.download_concurrency = 8;
        o.read_timeout = seconds{20};
        o.call_timeout = seconds{5};
        make(o);
    }
    void TearDown() override {
        // Зависшие читатели (тест упал на полпути) дорабатывают в автоматическом режиме.
        fake->set_download_mode(FakeTelegram::DownloadMode::automatic);
        for (auto& r : readers) {
            r->join();
        }
        TelegramStoreTest::TearDown();
    }

    Doc add_doc(uint32_t seed, size_t size = kSize) {
        Doc d;
        d.content = pattern(size, seed);
        const int64_t id = fake->seed_document(d.content);
        d.ref = ChunkRef{id, fake->message(id)->remote_id};
        d.fid = fake->message(id)->file_id;
        return d;
    }
    Reader& start_read(const Doc& d, uint64_t off, size_t len) {
        readers.push_back(std::make_unique<Reader>());
        Reader& r = *readers.back();
        r.out.assign(len, '\0');
        r.began = Clock::now();
        r.thread = std::thread([this, &r, ref = d.ref, off] {
            r.error = error_of(
                [&] { store->read_range(ref, off, std::as_writable_bytes(std::span{r.out})); });
            r.ended = Clock::now();
        });
        return r;
    }
    bool wait_downloads(size_t n) {
        return wait_for([&] { return fake->count("downloadFile") >= n; });
    }
    bool wait_waiting(uint64_t n) {
        return wait_for([&] { return store->download_stats().waiting == n; });
    }
    // Планировщик прошёл хотя бы раз после последней постановки запроса.
    bool settle() {
        return store->wait_prefetch_idle(seconds{10}) &&
               wait_for([&] { return store->download_stats().plans_since_event >= 1; });
    }
    Json download(size_t index) const { return fake->requests("downloadFile").at(index); }

    std::vector<std::unique_ptr<Reader>> readers;
};

TEST_F(SchedTest, ReadyRangeIsReadWithoutDownloadFile) {
    const Doc d = add_doc(1);
    Reader& first = start_read(d, 0, 3000);
    ASSERT_TRUE(wait_downloads(1));
    fake->run_downloads();
    first.join();
    EXPECT_FALSE(first.error.has_value());
    EXPECT_EQ(first.out, d.content.substr(0, 3000));

    const size_t calls = fake->count("downloadFile");
    const uint64_t hits = store->download_stats().prefix_hits;
    Reader& again = start_read(d, 1000, 1000);
    again.join();  // без шагов фейка: данные уже на диске
    EXPECT_FALSE(again.error.has_value());
    EXPECT_EQ(again.out, d.content.substr(1000, 1000));
    EXPECT_EQ(fake->count("downloadFile"), calls);
    EXPECT_EQ(store->download_stats().prefix_hits, hits + 1);
}

TEST_F(SchedTest, ReadInsideTheActiveWindowWaitsWithoutNewRequest) {
    const Doc d = add_doc(2);
    Reader& a = start_read(d, 0, 10'000);
    ASSERT_TRUE(wait_downloads(1));
    Reader& b = start_read(d, 2000, 1000);
    ASSERT_TRUE(wait_waiting(2));
    ASSERT_TRUE(settle());
    EXPECT_EQ(fake->count("downloadFile"), 1U);
    fake->run_downloads();
    a.join();
    b.join();
    EXPECT_EQ(a.out, d.content.substr(0, 10'000));
    EXPECT_EQ(b.out, d.content.substr(2000, 1000));
    EXPECT_EQ(fake->count("downloadFile"), 1U);
}

TEST_F(SchedTest, UserReadDisplacesReadaheadAndCapturesTheRestOfTheWindow) {
    const Doc d = add_doc(3);
    store->prefetch(d.ref, 4000, 8000, 0);
    ASSERT_TRUE(wait_downloads(1));
    EXPECT_EQ(download(0)["offset"], 4000);
    EXPECT_EQ(download(0)["limit"], 8000);
    EXPECT_EQ(download(0)["priority"], 16);
    fake->step(2);                          // готово [4000, 6000)
    Reader& r = start_read(d, 3000, 1000);  // перед окном: не покрыт, вытесняет readahead
    ASSERT_TRUE(wait_downloads(2));
    EXPECT_EQ(download(1)["offset"], 3000);
    EXPECT_EQ(download(1)["limit"], 9000);  // захвачен остаток [6000, 12000) за запросом
    EXPECT_EQ(download(1)["priority"], 32);
    fake->run_downloads();
    r.join();
    EXPECT_FALSE(r.error.has_value());
    EXPECT_EQ(r.out, d.content.substr(3000, 1000));
    EXPECT_EQ(store->download_stats().displaced, 1U);
    EXPECT_EQ(fake->downloaded_bytes(d.fid), 9000 + 0);  // [3000, 12000)
}

TEST_F(SchedTest, AdjacentReadaheadExtendsTheActiveWindowAndOthersWait) {
    const Doc d = add_doc(4);
    store->prefetch(d.ref, 0, 4000, 0);
    ASSERT_TRUE(wait_downloads(1));
    store->prefetch(d.ref, 4000, 4000, 0);  // смежный: продлевает окно
    ASSERT_TRUE(wait_downloads(2));
    EXPECT_EQ(download(1)["offset"], 0);    // тот же offset
    EXPECT_EQ(download(1)["limit"], 8000);  // больший limit
    ASSERT_TRUE(fake->window(d.fid).has_value());
    EXPECT_EQ(fake->window(d.fid)->limit, 8000);
    EXPECT_EQ(store->download_stats().extended, 1U);

    store->prefetch(d.ref, 12'000, 2000, 0);  // несмежный: встаёт в очередь файла
    ASSERT_TRUE(settle());
    EXPECT_EQ(fake->count("downloadFile"), 2U);
    fake->run_downloads();  // окно [0, 8000) завершено, очередь свободна
    ASSERT_TRUE(wait_downloads(3));
    EXPECT_EQ(download(2)["offset"], 12'000);
    EXPECT_EQ(download(2)["limit"], 2000);
}

TEST_F(SchedTest, WaitingReaderRaisesTheReadaheadPriority) {
    const Doc d = add_doc(5);
    store->prefetch(d.ref, 0, 8000, 0);
    ASSERT_TRUE(wait_downloads(1));
    EXPECT_EQ(download(0)["priority"], 16);
    Reader& r = start_read(d, 1000, 1000);  // обслуживается окном readahead
    ASSERT_TRUE(wait_downloads(2));
    EXPECT_EQ(download(1)["offset"], 0);  // те же offset и limit, другой приоритет
    EXPECT_EQ(download(1)["limit"], 8000);
    EXPECT_EQ(download(1)["priority"], 32);
    EXPECT_GE(store->download_stats().reprioritized, 1U);
    fake->run_downloads();
    r.join();
    EXPECT_EQ(r.out, d.content.substr(1000, 1000));
}

TEST_F(SchedTest, ReadaheadPriorityFollowsTheDistance) {
    const std::vector<uint32_t> distances{0, 1, 3, 40};
    const std::vector<int> expected{16, 15, 13, 1};
    std::vector<Doc> docs;
    for (size_t i = 0; i < distances.size(); ++i) {
        docs.push_back(add_doc(10 + static_cast<uint32_t>(i)));
        store->prefetch(docs.back().ref, 0, 4000, distances[i]);
    }
    ASSERT_TRUE(wait_downloads(distances.size()));
    for (size_t i = 0; i < docs.size(); ++i) {
        ASSERT_TRUE(fake->window(docs[i].fid).has_value());
        EXPECT_EQ(fake->window(docs[i].fid)->priority, expected[i]) << "distance " << distances[i];
    }
}

TEST_F(SchedTest, ReadaheadTakesAtMostHalfOfTheSlots) {
    auto o = default_options();
    o.download_concurrency = 4;
    o.read_timeout = seconds{20};
    store.reset();
    auto& s = make(o);
    std::vector<Doc> docs;
    for (uint32_t i = 0; i < 4; ++i) {
        docs.push_back(add_doc(20 + i));
        s.prefetch(docs.back().ref, 0, 4000, i);
    }
    ASSERT_TRUE(wait_downloads(2));
    ASSERT_TRUE(settle());
    EXPECT_EQ(fake->count("downloadFile"), 2U);  // половина от 4
    // Пользовательское чтение занимает свободный слот.
    Reader& r = start_read(docs[3], 0, 500);
    ASSERT_TRUE(wait_downloads(3));
    fake->run_downloads();
    r.join();
    EXPECT_EQ(r.out, docs[3].content.substr(0, 500));
}

TEST_F(SchedTest, UserPriorityFollowsAgeAcrossFilesAndIsRecomputedOnCompletion) {
    const Doc a = add_doc(30);
    const Doc b = add_doc(31);
    const Doc c = add_doc(32);
    Reader& ra = start_read(a, 0, 3000);
    ASSERT_TRUE(wait_downloads(1));
    Reader& rb = start_read(b, 0, 3000);
    ASSERT_TRUE(wait_downloads(2));
    Reader& rc = start_read(c, 0, 3000);
    ASSERT_TRUE(wait_downloads(3));
    EXPECT_EQ(fake->window(a.fid)->priority, 32);
    EXPECT_EQ(fake->window(b.fid)->priority, 31);
    EXPECT_EQ(fake->window(c.fid)->priority, 30);

    while (fake->completion_order().empty()) {  // TDLib берёт старший приоритет: файл a
        ASSERT_GT(fake->step(), 0);
    }
    EXPECT_EQ(fake->completion_order().front(), a.fid);
    ra.join();
    // Приоритеты оставшихся пересчитаны повторным вызовом с теми же offset и limit.
    ASSERT_TRUE(wait_for([&] {
        const auto wb = fake->window(b.fid);
        const auto wc = fake->window(c.fid);
        return wb && wc && wb->priority == 32 && wc->priority == 31;
    }));
    const auto recalculated = fake->requests("downloadFile").back();
    EXPECT_EQ(recalculated["limit"], 3000);
    EXPECT_EQ(recalculated["offset"], 0);
    fake->run_downloads();
    rb.join();
    rc.join();
    EXPECT_EQ(fake->completion_order(), (std::vector<int32_t>{a.fid, b.fid, c.fid}));
    EXPECT_EQ(rb.out, b.content.substr(0, 3000));
    EXPECT_EQ(rc.out, c.content.substr(0, 3000));
}

TEST_F(SchedTest, UserReadsOfOneFileAreServedOldestFirst) {
    const Doc d = add_doc(40);
    Reader& first = start_read(d, 0, 1000);
    ASSERT_TRUE(wait_downloads(1));
    Reader& second = start_read(d, 10'000, 1000);  // другое место: ждёт очереди файла
    ASSERT_TRUE(wait_waiting(2));
    ASSERT_TRUE(settle());
    EXPECT_EQ(fake->count("downloadFile"), 1U);  // старый запрос не вытеснен
    fake->run_downloads();
    first.join();
    ASSERT_TRUE(wait_downloads(2));
    EXPECT_EQ(download(1)["offset"], 10'000);
    fake->run_downloads();
    second.join();
    EXPECT_EQ(first.out, d.content.substr(0, 1000));
    EXPECT_EQ(second.out, d.content.substr(10'000, 1000));
}

// local.path меняется при завершении файла: апдейт с прогрессом ещё несёт старый путь, а по нему
// файла уже нет. Чтение берёт путь заново (getFile) и не качает ничего повторно.
TEST_F(SchedTest, PathChangeOnCompletionIsNotAnError) {
    const Doc d = add_doc(50, 3000);
    fake->hold_final_update(true);
    Reader& r = start_read(d, 0, 3000);
    ASSERT_TRUE(wait_downloads(1));
    fake->run_downloads();
    r.join();
    EXPECT_FALSE(r.error.has_value());
    EXPECT_EQ(r.out, d.content);
    EXPECT_GE(fake->count("getFile"), 1U);
    EXPECT_EQ(fake->count("downloadFile"), 1U);
    fake->release_held_updates();
}

TEST_F(SchedTest, DisplacingOwnWindowIsNotAnError) {
    const Doc d = add_doc(60);
    store->prefetch(d.ref, 0, 8000, 0);
    ASSERT_TRUE(wait_downloads(1));
    fake->step(1);
    Reader& r = start_read(d, 9000, 1000);  // вне окна: вытесняет его
    ASSERT_TRUE(wait_downloads(2));
    fake->run_downloads();
    r.join();
    EXPECT_FALSE(r.error.has_value());
    EXPECT_EQ(r.out, d.content.substr(9000, 1000));
    EXPECT_EQ(fake->count("getMessages"), 0U);  // повторов с новым file_id не было
}

// read_timeout отсчитывается от постановки запроса, а не от его очереди в TDLib: второй читатель
// не получает новый срок, когда доходит до головы очереди файла.
TEST_F(SchedTest, ReadTimeoutCountsFromTheMomentOfQueueing) {
    auto o = default_options();
    o.download_concurrency = 8;
    o.read_timeout = milliseconds{600};
    o.call_timeout = seconds{5};
    store.reset();
    make(o);
    const Doc d = add_doc(70);
    Reader& first = start_read(d, 0, 1000);  // окно открыто, но TDLib не качает
    ASSERT_TRUE(wait_downloads(1));
    std::this_thread::sleep_for(milliseconds{300});
    Reader& second = start_read(d, 10'000, 1000);
    first.join();
    second.join();
    ASSERT_TRUE(first.error.has_value());
    ASSERT_TRUE(second.error.has_value());
    EXPECT_EQ(first.error->kind(), StoreErrorKind::transient);
    EXPECT_EQ(second.error->kind(), StoreErrorKind::transient);
    const auto first_took = first.ended - first.began;
    const auto second_took = second.ended - second.began;
    EXPECT_GE(first_took, milliseconds{600});
    EXPECT_GE(second_took, milliseconds{550});
    EXPECT_LT(second_took, milliseconds{800});  // со свежим сроком было бы около 900 мс
}

TEST_F(SchedTest, StalledDownloadIsRepeatedWithTheSameCall) {
    auto o = default_options();
    o.download_concurrency = 8;
    o.read_timeout = seconds{20};
    o.download_stall = milliseconds{100};
    store.reset();
    make(o);
    const Doc d = add_doc(80);
    Reader& r = start_read(d, 0, 2000);
    ASSERT_TRUE(wait_downloads(1));
    ASSERT_TRUE(wait_downloads(2));  // без прогресса вызов повторён тем же offset и limit
    EXPECT_EQ(download(1)["offset"], download(0)["offset"]);
    EXPECT_EQ(download(1)["limit"], download(0)["limit"]);
    fake->run_downloads();
    r.join();
    EXPECT_EQ(r.out, d.content.substr(0, 2000));
    EXPECT_GE(store->download_stats().stalls, 1U);
}

// ---- Кэш чтения (SPEC 8.2) ------------------------------------------------------------------

class CacheTest : public TelegramStoreTest {
  protected:
    void SetUp() override {
        TelegramStoreTest::SetUp();
        // Размер кэшированного файла на диске по st_blocks: файл в 5000 байт.
        const fs::path probe = dir / "probe";
        std::ofstream(probe, std::ios::binary) << pattern(kSize, 1);
        struct stat st {};
        ASSERT_EQ(::stat(probe.c_str(), &st), 0);
        file_bytes = static_cast<uint64_t>(st.st_blocks) * 512;
        fs::remove(probe);
        state_db = dir / "state.sqlite";
        meta::Db::open(state_db, meta::OpenOptions{.create = true}).exec("SELECT 1");
    }

    TelegramStore& make_cached(uint64_t limit) {
        auto o = default_options();
        o.state_db = state_db;
        o.tdfiles_dir = dir / "dl";
        o.cache_limit = limit;
        return make(o);
    }
    struct Doc {
        ChunkRef ref;
        int32_t fid = 0;
        std::string content;
    };
    Doc add_doc(uint32_t seed, size_t size = kSize) {
        Doc d;
        d.content = pattern(size, seed);
        const int64_t id = fake->seed_document(d.content);
        d.ref = ChunkRef{id, fake->message(id)->remote_id};
        d.fid = fake->message(id)->file_id;
        return d;
    }
    std::string read_all(const Doc& d) {
        std::string out(d.content.size(), '\0');
        store->read_range(d.ref, 0, std::as_writable_bytes(std::span{out}));
        return out;
    }
    [[nodiscard]] bool on_disk(const Doc& d) const {
        const std::string p = fake->local_path(d.fid);
        return !p.empty() && fs::exists(p);
    }

    static constexpr size_t kSize = 5000;
    uint64_t file_bytes = 0;
    fs::path state_db;
};

TEST_F(CacheTest, EvictsLeastRecentlyUsedFilesDownTo90PercentOfTheLimit) {
    auto& s = make_cached(file_bytes * 7 / 2);  // 3,5 файла
    std::vector<Doc> docs;
    for (uint32_t i = 0; i < 5; ++i) {
        docs.push_back(add_doc(100 + i));
        EXPECT_EQ(read_all(docs.back()), docs.back().content);
    }
    ASSERT_NE(s.read_cache(), nullptr);
    EXPECT_EQ(s.read_cache()->total_bytes(), 5 * file_bytes);
    s.maintain_read_cache();
    // Цель: не больше 90 % лимита (3,15 файла): остаются три самых свежих файла.
    EXPECT_EQ(s.read_cache()->total_bytes(), 3 * file_bytes);
    EXPECT_FALSE(on_disk(docs[0]));
    EXPECT_FALSE(on_disk(docs[1]));
    EXPECT_TRUE(on_disk(docs[2]));
    EXPECT_TRUE(on_disk(docs[3]));
    EXPECT_TRUE(on_disk(docs[4]));
    EXPECT_EQ(fake->count("deleteFile"), 2U);  // через TDLib: getRemoteFile и deleteFile
    // Вытесненный файл читается заново и верно.
    EXPECT_EQ(read_all(docs[0]), docs[0].content);
    EXPECT_EQ(fake->count("downloadFile"), 6U);
}

TEST_F(CacheTest, FileWithActiveDownloadIsNotEvicted) {
    fake->set_part_size(1000);
    auto& s = make_cached(file_bytes / 2);  // всё сверх половины файла лишнее
    const Doc quiet = add_doc(110);
    EXPECT_EQ(read_all(quiet), quiet.content);
    const Doc busy = add_doc(111, 20'000);
    std::string head(1000, '\0');
    s.read_range(busy.ref, 0, std::as_writable_bytes(std::span{head}));  // файл учтён
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    std::string tail(1000, '\0');
    std::thread reader(
        [&] { s.read_range(busy.ref, 15'000, std::as_writable_bytes(std::span{tail})); });
    ASSERT_TRUE(wait_for([&] { return fake->window(busy.fid).has_value(); }));
    s.maintain_read_cache();
    EXPECT_FALSE(on_disk(quiet));
    EXPECT_TRUE(on_disk(busy));  // его окно активно
    fake->run_downloads();
    reader.join();
    EXPECT_EQ(tail, busy.content.substr(15'000, 1000));
}

TEST_F(CacheTest, FileDeletedBehindTdlibIsDownloadedAgainAndAccountedOnce) {
    auto& s = make_cached(file_bytes * 100);
    const Doc d = add_doc(120);
    EXPECT_EQ(read_all(d), d.content);
    fake->remove_local_file(d.fid);  // за спиной TDLib: getFile ещё считает его скачанным
    EXPECT_EQ(read_all(d), d.content);
    EXPECT_EQ(fake->count("downloadFile"), 2U);
    EXPECT_EQ(s.read_cache()->entries().size(), 1U);
    EXPECT_EQ(s.read_cache()->total_bytes(), file_bytes);
}

// Ошибка при удалении жертвы (любого типа, не только TdException) не оставляет файлы навсегда
// помеченными как вытесняемые: читатели не ждут до таймаута.
TEST_F(CacheTest, FailureDuringEvictionReleasesTheClaims) {
    auto o = default_options();
    o.state_db = state_db;
    o.tdfiles_dir = dir / "dl";
    o.cache_limit = file_bytes * 7 / 2;
    o.eviction_test_hook = [](const std::string&) { throw std::runtime_error("injected"); };
    auto& s = make(o);
    std::vector<Doc> docs;
    for (uint32_t i = 0; i < 5; ++i) {
        docs.push_back(add_doc(140 + i));
        EXPECT_EQ(read_all(docs.back()), docs.back().content);
    }
    EXPECT_THROW(s.maintain_read_cache(), std::runtime_error);
    for (const Doc& d : docs) {
        const auto began = Clock::now();
        EXPECT_EQ(read_all(d), d.content);  // без снятия пометки ждало бы read_timeout и упало
        EXPECT_LT(Clock::now() - began, milliseconds{1000});
    }
}

// Файл, отправленный этим процессом, лежит вне tdfiles (временная ссылка): учёт кэша его не ведёт и
// вытеснение его не трогает; после докачки в tdfiles учитывается уже скачанная копия.
TEST_F(CacheTest, FilesSentByThisProcessAreNotAccountedOutsideTdfiles) {
    auto& s = make_cached(file_bytes * 100);
    const ChunkRef ref = s.put_chunk(write_file("own-chunk", pattern(kSize, 160)));
    std::string out(kSize, '\0');
    s.read_range(ref, 0, std::as_writable_bytes(std::span{out}));
    EXPECT_EQ(out, pattern(kSize, 160));
    s.maintain_read_cache();
    ASSERT_NE(s.read_cache(), nullptr);
    for (const ReadCacheEntry& e : s.read_cache()->entries()) {
        EXPECT_TRUE(e.path.starts_with((dir / "dl").string())) << e.path;
    }
    EXPECT_EQ(s.read_cache()->entries().size(), 1U);
    EXPECT_EQ(fake->count("deleteFile"), 0U);
}

TEST_F(CacheTest, AccountingIsSavedToTheStateTableAndSurvivesRestart) {
    {
        auto& s = make_cached(file_bytes * 100);
        for (uint32_t i = 0; i < 3; ++i) {
            const Doc d = add_doc(130 + i);
            EXPECT_EQ(read_all(d), d.content);
        }
        s.maintain_read_cache();  // сброс в таблицу
    }
    store.reset();
    auto& again = make_cached(file_bytes * 100);
    again.start_read_cache();  // сверка при монтировании
    ASSERT_NE(again.read_cache(), nullptr);
    EXPECT_EQ(again.read_cache()->entries().size(), 3U);
    EXPECT_EQ(again.read_cache()->total_bytes(), 3 * file_bytes);
    for (const ReadCacheEntry& e : again.read_cache()->entries()) {
        EXPECT_FALSE(e.remote_file_id.empty());  // remote_file_id пережил перезапуск
    }
}

// В --ro (SPEC 7.2) state_db не передаётся: учёт кэша идёт в памяти, вытеснение работает, а
// state.sqlite не меняется ни байтом (ни CREATE TABLE, ни сброса учёта, ни WAL).
TEST_F(CacheTest, WithoutStateDbTheAccountingStaysInMemoryAndStateSqliteIsUntouched) {
    const auto snapshot = [&] {
        std::ifstream in(state_db, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    };
    const std::string before = snapshot();
    ASSERT_FALSE(before.empty());
    auto o = default_options();
    o.tdfiles_dir = dir / "dl";  // state_db не задан
    o.cache_limit = file_bytes * 7 / 2;
    auto& s = make(o);
    std::vector<Doc> docs;
    for (uint32_t i = 0; i < 5; ++i) {
        docs.push_back(add_doc(150 + i));
        EXPECT_EQ(read_all(docs.back()), docs.back().content);
    }
    s.start_read_cache();     // сверка с каталогом
    s.maintain_read_cache();  // вытеснение и «сброс»
    ASSERT_NE(s.read_cache(), nullptr);
    EXPECT_EQ(s.read_cache()->total_bytes(), 3 * file_bytes);  // вытеснение работает
    EXPECT_FALSE(on_disk(docs[0]));
    store.reset();  // деструктор ReadCache тоже сбрасывает учёт
    EXPECT_EQ(snapshot(), before);
    EXPECT_FALSE(fs::exists(state_db.string() + "-wal"));
    EXPECT_FALSE(fs::exists(state_db.string() + "-shm"));
}

TEST_F(CacheTest, WithoutPathsThereIsNoAccounting) {
    auto& s = make();
    s.start_read_cache();
    s.maintain_read_cache();
    EXPECT_EQ(s.read_cache(), nullptr);
}

// ---- DownloadScheduler напрямую (SPEC 8.2): вытеснение, апдейты, readahead ----------------------

class DirectSchedTest : public TelegramStoreTest {
  protected:
    static constexpr size_t kSize = 20'000;
    static constexpr int32_t kAlias = 5000;

    struct Doc {
        int32_t fid = 0;
        std::string content;
        [[nodiscard]] FileRef ref() const { return FileRef{fid, kSize, {}}; }
    };

    void SetUp() override {
        TelegramStoreTest::SetUp();
        fake->set_part_size(1000);
    }
    void TearDown() override {
        fake->set_download_mode(FakeTelegram::DownloadMode::automatic);
        for (auto& t : threads) {
            t.join();
        }
        TelegramStoreTest::TearDown();
    }

    static SchedulerOptions options() {
        SchedulerOptions o;
        o.call_timeout = seconds{5};
        o.failure_guard = milliseconds{30};
        return o;
    }
    std::unique_ptr<DownloadScheduler> make_sched(const SchedulerOptions& o = options()) {
        return std::make_unique<DownloadScheduler>(*client, o, SchedulerHooks{});
    }
    Doc add_doc(uint32_t seed) {
        Doc d;
        d.content = pattern(kSize, seed);
        const int64_t id = fake->seed_document(d.content);
        d.fid = fake->message(id)->file_id;
        return d;
    }
    static Clock::time_point in(milliseconds d) { return Clock::now() + d; }
    static std::string read(DownloadScheduler& sched, const FileRef& f, uint64_t off, size_t len,
                            Clock::time_point deadline) {
        std::string out(len, '\0');
        sched.read(f, off, std::as_writable_bytes(std::span{out}), ReadKind::user, deadline);
        return out;
    }
    // Чтение, которое должно упереться в дедлайн (transient StoreError).
    static bool times_out(DownloadScheduler& sched, const FileRef& f, uint64_t off, size_t len) {
        try {
            (void)read(sched, f, off, len, in(milliseconds{300}));
        } catch (const StoreError& e) {
            return e.kind() == StoreErrorKind::transient;
        }
        return false;
    }

    std::vector<std::thread> threads;
};

// Один путь известен планировщику под двумя file_id (после getMessages): вытеснение помечает оба,
// чтение под любым из них ждёт его конца.
TEST_F(DirectSchedTest, EvictionBarrierCoversEveryFileIdKnownUnderThePath) {
    const Doc d = add_doc(1);
    fake->add_alias(kAlias, d.fid);
    auto sched = make_sched();
    const FileRef alias{kAlias, kSize, {}};
    EXPECT_EQ(read(*sched, d.ref(), 0, 1000, in(seconds{10})), d.content.substr(0, 1000));
    EXPECT_EQ(read(*sched, alias, 0, 1000, in(seconds{10})), d.content.substr(0, 1000));
    const std::string path = fake->local_path(d.fid);
    ASSERT_FALSE(path.empty());
    ASSERT_TRUE(sched->begin_evict(path));
    EXPECT_TRUE(times_out(*sched, alias, 0, 1000));  // второй file_id тоже помечен
    EXPECT_TRUE(times_out(*sched, d.ref(), 0, 1000));
    sched->end_evict(path, false);
    EXPECT_EQ(read(*sched, alias, 0, 1000, in(seconds{10})), d.content.substr(0, 1000));
    EXPECT_EQ(read(*sched, d.ref(), 0, 1000, in(seconds{10})), d.content.substr(0, 1000));
}

// Занят любой из file_id пути: вытеснение отказывает, даже если первый из них свободен.
TEST_F(DirectSchedTest, EvictionIsRefusedWhileAnotherFileIdOfThePathIsBusy) {
    const Doc d = add_doc(2);
    fake->add_alias(kAlias, d.fid);
    auto sched = make_sched();
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    threads.emplace_back([&] { (void)read(*sched, d.ref(), 0, 1000, in(seconds{10})); });
    ASSERT_TRUE(wait_for([&] { return fake->window(d.fid).has_value(); }));
    fake->run_downloads();
    ASSERT_TRUE(wait_for([&] { return !fake->local_path(d.fid).empty(); }));
    const std::string path = fake->local_path(d.fid);
    threads.back().join();
    threads.pop_back();
    // Под вторым file_id идёт скачивание того же файла.
    threads.emplace_back(
        [&] { (void)read(*sched, FileRef{kAlias, kSize, {}}, 10'000, 1000, in(seconds{10})); });
    ASSERT_TRUE(wait_for([&] { return sched->stats().waiting == 1; }));
    // Путь второй состояние получает с ответом на downloadFile: опрашиваем, пока отказ не придёт.
    bool refused = false;
    const auto until = in(seconds{2});
    while (Clock::now() < until && !refused) {
        if (sched->begin_evict(path)) {
            sched->end_evict(path, false);
            std::this_thread::sleep_for(milliseconds{2});
        } else {
            refused = true;
        }
    }
    EXPECT_TRUE(refused);
    fake->run_downloads();
    threads.back().join();
    threads.pop_back();
}

// Файл, которого планировщик не знал при begin_evict, получает барьер, когда узнаёт вытесняемый
// путь: pread не читает файл, который сейчас удаляют.
TEST_F(DirectSchedTest, FileUnknownToTheSchedulerWaitsForTheEvictionOfItsPath) {
    const Doc d = add_doc(3);
    std::string path;
    {
        auto first = make_sched();
        EXPECT_EQ(read(*first, d.ref(), 0, 1000, in(seconds{10})), d.content.substr(0, 1000));
        path = fake->local_path(d.fid);
    }
    auto sched = make_sched();  // не знает файл
    ASSERT_TRUE(sched->begin_evict(path));
    EXPECT_TRUE(times_out(*sched, d.ref(), 0, 1000));
    std::string got;
    std::atomic<bool> finished{false};
    threads.emplace_back([&] {
        got = read(*sched, d.ref(), 0, 1000, in(seconds{10}));
        finished = true;
    });
    // Читатель стоит у барьера: вытеснение ещё не кончилось, данные он не получил.
    std::this_thread::sleep_for(milliseconds{100});
    EXPECT_FALSE(finished.load());
    sched->end_evict(path, false);
    threads.back().join();
    threads.pop_back();
    EXPECT_EQ(got, d.content.substr(0, 1000));
}

// Читатель, ждавший конца вытеснения, держит ссылку на состояние файла: уборка простаивающих файлов
// (idle_forget) не стирает его, пока читатель не проснулся. Без счётчика читателей до ожидания
// ссылка повисала (ловится ASan, на обычной сборке исход случаен).
TEST_F(DirectSchedTest, ReaderWaitingForEvictionKeepsTheFileState) {
    const Doc d = add_doc(7);
    auto o = options();
    o.idle_forget = milliseconds{10};
    auto sched = make_sched(o);
    EXPECT_EQ(read(*sched, d.ref(), 0, 1000, in(seconds{10})), d.content.substr(0, 1000));
    const std::string path = fake->local_path(d.fid);
    for (int round = 0; round < 15; ++round) {
        if (sched->tracked_files() == 0) {
            // Состояние забыто простоем: читатель заводит его заново и снова получает путь.
            EXPECT_EQ(read(*sched, d.ref(), 0, 1000, in(seconds{10})), d.content.substr(0, 1000));
        }
        // Окно скачивания закрывается проходом планировщика уже после ответа читателю.
        ASSERT_TRUE(wait_for([&] { return sched->stats().active_now == 0; }));
        ASSERT_TRUE(sched->begin_evict(path));
        std::string got;
        std::thread reader([&] { got = read(*sched, d.ref(), 0, 1000, in(seconds{10})); });
        std::this_thread::sleep_for(milliseconds{30});  // втрое дольше idle_forget
        sched->end_evict(path, false);
        reader.join();
        EXPECT_EQ(got, d.content.substr(0, 1000));
    }
}

// Запоздалый апдейт «не качается» о прежнем окне (другой download_offset) не обрывает новое.
TEST_F(DirectSchedTest, LateInactiveUpdateOfThePreviousWindowIsNotAnAbort) {
    const Doc d = add_doc(4);
    auto sched = make_sched();
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    std::string got;
    std::optional<StoreError> error;
    threads.emplace_back([&] {
        error = error_of([&] { got = read(*sched, d.ref(), 10'000, 1000, in(seconds{10})); });
    });
    ASSERT_TRUE(wait_for([&] { return fake->window(d.fid).has_value(); }));
    ASSERT_TRUE(wait_for([&] { return !fake->local_path(d.fid).empty(); }));
    // Прежнее окно (offset 0) сообщает «не качается», когда уже идёт окно с offset 10000.
    Json local{{"path", fake->local_path(d.fid)},
               {"is_downloading_active", false},
               {"is_downloading_completed", false},
               {"download_offset", 0},
               {"downloaded_prefix_size", 0}};
    fake->push_update(
        Json{{"@type", "updateFile"},
             {"file", {{"id", d.fid}, {"size", kSize}, {"local", std::move(local)}}}});
    std::this_thread::sleep_for(milliseconds{300});  // в десять раз больше failure_guard
    EXPECT_EQ(fake->count("downloadFile"), 1U);
    fake->run_downloads();
    threads.back().join();
    threads.pop_back();
    EXPECT_FALSE(error.has_value());
    EXPECT_EQ(got, d.content.substr(10'000, 1000));
}

// Скачивание после сброса локального расположения (файл отправлен этим процессом): TDLib сначала
// шлёт «не качается», потом «качается», а первая часть приходит дольше failure_guard. Это не
// обрыв: окно не отменяется и читатель получает данные тем же downloadFile.
TEST_F(DirectSchedTest, SlowFirstPartAfterLocalCheckIsNotAnAbort) {
    const Doc d = add_doc(8);
    fake->set_stale_local(d.fid);
    auto sched = make_sched();
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    std::string got;
    std::optional<StoreError> error;
    threads.emplace_back(
        [&] { error = error_of([&] { got = read(*sched, d.ref(), 0, 1000, in(seconds{10})); }); });
    ASSERT_TRUE(wait_for([&] { return fake->window(d.fid).has_value(); }));
    std::this_thread::sleep_for(milliseconds{300});  // в десять раз дольше failure_guard
    EXPECT_EQ(fake->count("downloadFile"), 1U);
    fake->run_downloads();
    threads.back().join();
    threads.pop_back();
    EXPECT_FALSE(error.has_value());
    EXPECT_EQ(got, d.content.substr(0, 1000));
}

// Начало диапазона ждущего уже скачано (дошёл readahead), окно начинается с первого непокрытого
// байта. Такое окно обслуживает ждущего: планировщик не выдаёт тот же downloadFile на каждом
// проходе (в живом прогоне это были тысячи вызовов в секунду, они отменяли друг друга, и чтение
// вставало).
TEST_F(DirectSchedTest, WindowStartedAtTheFirstGapServesTheWaiterWithoutRepeatedCalls) {
    const Doc d = add_doc(10);
    auto sched = make_sched();
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    threads.emplace_back([&] { (void)read(*sched, d.ref(), 0, 1000, in(seconds{10})); });
    ASSERT_TRUE(wait_for([&] { return fake->window(d.fid).has_value(); }));
    fake->run_downloads();
    threads.back().join();
    threads.pop_back();
    ASSERT_TRUE(wait_for([&] { return sched->stats().active_now == 0; }));
    const size_t before = fake->count("downloadFile");
    std::string got;
    threads.emplace_back([&] { got = read(*sched, d.ref(), 500, 3000, in(seconds{10})); });
    ASSERT_TRUE(wait_for([&] { return fake->count("downloadFile") > before; }));
    EXPECT_EQ(fake->window(d.fid)->offset, 1000);  // с первого непокрытого байта
    std::this_thread::sleep_for(milliseconds{300});
    EXPECT_LE(fake->count("downloadFile"), before + 2) << "downloadFile is repeated on every pass";
    EXPECT_LE(sched->stats().calls, uint64_t{before + 2});
    fake->run_downloads();
    threads.back().join();
    threads.pop_back();
    EXPECT_EQ(got, d.content.substr(500, 3000));
}

// Диагностика (SPEC 8.2): состояние файла в планировщике показывает окно, ждущих и последние
// апдейты; то же уходит в лог (warning) при таймауте ожидания окна.
TEST_F(DirectSchedTest, DescribeShowsTheWindowWaitersAndLastUpdates) {
    const Doc d = add_doc(9);
    auto sched = make_sched();
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    EXPECT_TRUE(times_out(*sched, d.ref(), 2000, 1000));  // окно открыто, TDLib не качает
    const std::string text = sched->describe(d.fid);
    EXPECT_NE(text.find("active window [2000, +1000)"), std::string::npos) << text;
    EXPECT_NE(text.find("prio 32"), std::string::npos) << text;
    EXPECT_NE(text.find("ready {}"), std::string::npos) << text;
    EXPECT_NE(text.find("last updates:"), std::string::npos) << text;
    EXPECT_NE(text.find("reply offset 2000"), std::string::npos) << text;
    EXPECT_NE(sched->describe(424242).find("unknown"), std::string::npos);
}

// Срок readahead считается от каждого запроса, а не от последней подсказки файла: запрос, который
// не стартовал за readahead_ttl, забывается, даже если рядом поставили новый.
TEST_F(DirectSchedTest, ReadaheadTtlCountsFromEachRequest) {
    const Doc busy = add_doc(5);
    const Doc waiting = add_doc(6);
    auto o = options();
    o.download_concurrency = 1;
    o.readahead_ttl = milliseconds{1000};
    auto sched = make_sched(o);
    fake->set_download_mode(FakeTelegram::DownloadMode::manual);
    sched->prefetch(busy.ref(), 0, 1000, 0);  // занимает единственный слот
    ASSERT_TRUE(wait_for([&] { return fake->window(busy.fid).has_value(); }));
    const auto t0 = Clock::now();
    sched->prefetch(waiting.ref(), 0, 1000, 0);  // ждёт слот
    std::this_thread::sleep_until(t0 + milliseconds{600});
    sched->prefetch(waiting.ref(), 5000, 1000, 0);  // новая подсказка того же файла
    std::this_thread::sleep_until(t0 + milliseconds{1250});  // первая подсказка просрочена
    fake->run_downloads();                                   // слот освободился
    ASSERT_TRUE(wait_for([&] { return fake->window(waiting.fid).has_value(); }));
    EXPECT_EQ(fake->window(waiting.fid)->offset, 5000);
    fake->run_downloads();
}

// NOLINTEND(bugprone-unchecked-optional-access)

}  // namespace
}  // namespace telega::store
