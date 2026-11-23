// Vfs поверх TelegramStore на фейковом транспорте TDLib: параллельность загрузок (SPEC 8.1) и
// чтение чанка, который этот же процесс только что залил (SPEC 8.2).

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "../store/fake_telegram.h"
#include "store/telegram_store.h"
#include "td/td_runtime.h"
#include "vfs_test_util.h"

namespace telega::vfs {
namespace {

using std::chrono::milliseconds;
using test::bytes_of;
using test::kChunk;
using test::kRoot;
using test::pattern;

class VfsTelegram : public ::testing::Test {
  protected:
    void SetUp() override {
        fake = std::make_unique<store::test::FakeTelegram>(*transport, env.dir.path() / "dl");
        client = runtime.create_client();
        store::RateLimiterOptions lo;
        lo.actions_per_minute = 0;
        lo.bytes_per_minute = 0;
        lo.flood_jitter = 0;
        limiter = std::make_unique<store::RateLimiter>(lo);
        store::TelegramStoreOptions so;
        so.chat_id = store::test::kChatId;
        so.upload_concurrency = 2;
        so.album_gather = milliseconds{3000};  // полный альбом уходит без ожидания
        tg = std::make_unique<store::TelegramStore>(*client, *limiter, so);
        env.override_store = tg.get();
    }
    void TearDown() override {
        env.vfs.reset();
        tg.reset();
        runtime.stop();
    }

    // Файл больше pack_threshold (64 КиБ в тестах): позиционный чанк, далеко до порога альбомов.
    int64_t put_file(const std::string& name, const Bytes& data) const {
        const Created c = env.vfs->create(kRoot, bytes_of(name), 0644, O_RDWR, 1000, 1000);
        EXPECT_EQ(env.vfs->write(c.attr.ino, 0, data), data.size());
        env.vfs->release(c.fh);
        return c.attr.ino;
    }

    test::VfsEnv env;
    std::shared_ptr<store::test::FakeTransport> transport =
        std::make_shared<store::test::FakeTransport>();
    td::TdRuntime runtime{transport, milliseconds{10}};
    std::unique_ptr<store::test::FakeTelegram> fake;
    std::shared_ptr<td::TdClient> client;
    std::unique_ptr<store::RateLimiter> limiter;
    std::unique_ptr<store::TelegramStore> tg;
};

// Двадцать грязных мелких чанков: Vfs держит 10 · upload_concurrency вызовов put_chunk, поэтому
// TelegramStore набирает альбомы по 10 (раньше — upload_concurrency вызовов, альбомы по 2).
TEST_F(VfsTelegram, TwentySmallDirtyChunksReachTheStoreAsAlbumsOfTen) {
    VfsOptions o = test::fast_options();
    o.upload_concurrency = 2;
    env.start(o);
    std::vector<int64_t> inos;
    std::vector<Bytes> data;
    for (uint64_t i = 0; i < 20; ++i) {
        data.push_back(pattern(70'000 + i * 10, 100 + i));
        inos.push_back(put_file("f" + std::to_string(i), data.back()));
    }
    env.vfs->flush_all();

    const auto albums = fake->requests("sendMessageAlbum");
    ASSERT_EQ(albums.size(), 2U);
    EXPECT_EQ(albums[0]["input_message_contents"].size(), 10U);
    EXPECT_EQ(albums[1]["input_message_contents"].size(), 10U);
    EXPECT_EQ(fake->count("sendMessage"), 0U);
    EXPECT_LE(fake->max_inflight_sends(), 2);  // реальных отправок не больше upload_concurrency
    EXPECT_EQ(env.staging_files(), 0U);
    for (size_t i = 0; i < inos.size(); ++i) {
        EXPECT_EQ(env.read_all(inos[i]), data[i]) << i;
    }
}

// Чанк, залитый этим процессом, читается сразу: для TDLib он локальный, а ссылки, с которой он
// ушёл, уже нет. TDLib сам замечает это перед downloadFile и качает с сервера (SPEC 10.2), а
// deleteFile Store не зовёт: для файла вне files_directory он ничего не делает.
TEST_F(VfsTelegram, ChunkUploadedByThisProcessIsReadBackAfterTheLinkIsGone) {
    fake->set_sent_files_stale(true);
    env.start(test::fast_options());
    const Bytes data = pattern(3 * kChunk / 2, 7);  // два чанка: 1 МиБ и 0,5 МиБ
    const int64_t ino = put_file("big", data);
    env.vfs->flush_all();
    EXPECT_EQ(env.staging_files(), 0U);
    EXPECT_EQ(env.read_all(ino), data);
    EXPECT_EQ(fake->count("deleteFile"), 0U);
}

}  // namespace
}  // namespace telega::vfs
