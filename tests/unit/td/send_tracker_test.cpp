// SendTracker: итог sendMessage приходит апдейтом по (chat_id, old_message_id) (SPEC 2, 10.1).

#include "td/send_tracker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include "fake_transport.h"
#include "td/td_runtime.h"

namespace telega::td::test {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;

constexpr int64_t kChat = -1001234567890;

Json temp_message(int64_t id, int64_t chat = kChat) {
    return Json{{"@type", "message"},
                {"id", id},
                {"chat_id", chat},
                {"sending_state", {{"@type", "messageSendingStatePending"}}}};
}

Json succeeded(int64_t old_id, int64_t new_id, int64_t chat = kChat) {
    return Json{{"@type", "updateMessageSendSucceeded"},
                {"message", {{"@type", "message"}, {"id", new_id}, {"chat_id", chat}}},
                {"old_message_id", old_id}};
}

Json failed(int64_t old_id, int code, const std::string& text, int64_t chat = kChat) {
    return Json{{"@type", "updateMessageSendFailed"},
                {"message", {{"@type", "message"}, {"id", old_id}, {"chat_id", chat}}},
                {"old_message_id", old_id},
                {"error", {{"@type", "error"}, {"code", code}, {"message", text}}}};
}

TdError error_of(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const TdException& e) {
        return e.error();
    }
    ADD_FAILURE() << "TdException expected";
    return {};
}

class SendTrackerTest : public ::testing::Test {
  protected:
    std::shared_ptr<FakeTransport> transport = std::make_shared<FakeTransport>();
    TdRuntime runtime{transport, milliseconds{10}};
    std::shared_ptr<TdClient> client = runtime.create_client();
};

TEST_F(SendTrackerTest, SendReturnsFinalMessage) {
    transport->set_responder([](FakeTransport& t, int id, const Json& r) {
        t.respond(id, r, temp_message(1048576));
        t.push(id, succeeded(1048576, int64_t{5} * 1048576));
    });
    SendTracker tracker(*client);
    const Json m = tracker.send(Json{{"@type", "sendMessage"}}, seconds{5}, seconds{5});
    EXPECT_EQ(m["id"], int64_t{5} * 1048576);
    EXPECT_EQ(tracker.buffered(), 0U);
}

TEST_F(SendTrackerTest, EarlyUpdateBeforeResponseIsBuffered) {
    // Итог приходит раньше ответа на sendMessage.
    transport->set_responder([](FakeTransport& t, int id, const Json& r) {
        t.push(id, succeeded(2097152, int64_t{6} * 1048576));
        t.respond(id, r, temp_message(2097152));
    });
    SendTracker tracker(*client);
    const Json m = tracker.send(Json{{"@type", "sendMessage"}}, seconds{5}, seconds{5});
    EXPECT_EQ(m["id"], int64_t{6} * 1048576);
}

TEST_F(SendTrackerTest, ResultsBeforeTrackerWaitAreKept) {
    SendTracker tracker(*client);
    transport->push(client->id(), succeeded(10, 20));
    transport->push(client->id(), succeeded(11, 21));
    EXPECT_EQ(tracker.wait(kChat, 11, seconds{5})["id"], 21);
    EXPECT_EQ(tracker.wait(kChat, 10, seconds{5})["id"], 20);
}

TEST_F(SendTrackerTest, FailureCarriesErrorAndRetryAfter) {
    SendTracker tracker(*client);
    transport->push(client->id(), failed(30, 429, "Too Many Requests: retry after 12"));
    const TdError e = error_of([&] { (void)tracker.wait(kChat, 30, seconds{5}); });
    EXPECT_EQ(e.code, 429);
    EXPECT_EQ(e.retry_after, seconds{12});
}

TEST_F(SendTrackerTest, OtherChatsAndIdsDoNotMix) {
    SendTracker tracker(*client);
    transport->push(client->id(), succeeded(40, 41, /*chat=*/-5));
    const TdError e = error_of([&] { (void)tracker.wait(kChat, 40, milliseconds{100}); });
    EXPECT_TRUE(e.is_timeout());
    EXPECT_EQ(tracker.wait(-5, 40, seconds{5})["id"], 41);
}

TEST_F(SendTrackerTest, TimesOutWithoutResult) {
    SendTracker tracker(*client);
    const auto started = std::chrono::steady_clock::now();
    const TdError e = error_of([&] { (void)tracker.wait(kChat, 50, milliseconds{150}); });
    EXPECT_TRUE(e.is_timeout());
    EXPECT_GE(std::chrono::steady_clock::now() - started, milliseconds{140});
}

TEST_F(SendTrackerTest, ResultArrivingDuringWaitWakesWaiter) {
    SendTracker tracker(*client);
    std::thread t([&] {
        std::this_thread::sleep_for(milliseconds{50});
        transport->push(client->id(), succeeded(60, 61));
    });
    EXPECT_EQ(tracker.wait(kChat, 60, seconds{5})["id"], 61);
    t.join();
}

TEST_F(SendTrackerTest, BufferIsBounded) {
    SendTracker tracker(*client, /*max_buffered=*/3);
    for (int i = 1; i <= 10; ++i) {
        transport->push(client->id(), succeeded(i, 100 + i));
    }
    // Ждём, пока апдейты не разберёт поток приёма.
    for (int i = 0; i < 500 && client->stats().updates < 10; ++i) {
        std::this_thread::sleep_for(milliseconds{2});
    }
    ASSERT_EQ(client->stats().updates, 10U);
    EXPECT_EQ(tracker.buffered(), 3U);
    EXPECT_EQ(tracker.wait(kChat, 10, seconds{1})["id"], 110);
    EXPECT_TRUE(error_of([&] { (void)tracker.wait(kChat, 1, milliseconds{20}); }).is_timeout());
}

TEST_F(SendTrackerTest, SendRejectsMalformedResponse) {
    transport->set_responder(
        [](FakeTransport& t, int id, const Json& r) { t.respond(id, r, Json{{"@type", "ok"}}); });
    SendTracker tracker(*client);
    EXPECT_EQ(error_of([&] {
                  (void)tracker.send(Json{{"@type", "sendMessage"}}, seconds{5}, seconds{5});
              }).code,
              TdError::kUnexpected);
}

TEST_F(SendTrackerTest, SendPropagatesCallError) {
    transport->set_responder([](FakeTransport& t, int id, const Json& r) {
        t.respond(id, r,
                  Json{{"@type", "error"}, {"code", 400}, {"message", "CHAT_WRITE_FORBIDDEN"}});
    });
    SendTracker tracker(*client);
    const TdError e = error_of(
        [&] { (void)tracker.send(Json{{"@type", "sendMessage"}}, seconds{5}, seconds{5}); });
    EXPECT_EQ(e.code, 400);
}

TEST_F(SendTrackerTest, AlbumWaitsForEveryMessage) {
    transport->set_responder([](FakeTransport& t, int id, const Json& r) {
        t.respond(id, r,
                  Json{{"@type", "messages"},
                       {"total_count", 2},
                       {"messages", Json::array({temp_message(7), temp_message(8)})}});
        t.push(id, succeeded(8, 18));
        t.push(id, succeeded(7, 17));
    });
    SendTracker tracker(*client);
    const auto done =
        tracker.send_album(Json{{"@type", "sendMessageAlbum"}}, seconds{5}, seconds{5});
    ASSERT_EQ(done.size(), 2U);
    EXPECT_EQ(done[0]["id"], 17);
    EXPECT_EQ(done[1]["id"], 18);
}

TEST_F(SendTrackerTest, WaiterGetsClosedWhenClientCloses) {
    SendTracker tracker(*client);
    std::thread t([&] {
        std::this_thread::sleep_for(milliseconds{50});
        transport->push(client->id(),
                        Json{{"@type", "updateAuthorizationState"},
                             {"authorization_state", {{"@type", "authorizationStateClosed"}}}});
    });
    const TdError e = error_of([&] { (void)tracker.wait(kChat, 70, seconds{30}); });
    EXPECT_EQ(e.code, TdError::kClosed);
    t.join();
}

TEST_F(SendTrackerTest, WaiterGetsStoppedWhenRuntimeStops) {
    SendTracker tracker(*client);
    std::thread t([&] {
        std::this_thread::sleep_for(milliseconds{50});
        runtime.stop();
    });
    const TdError e = error_of([&] { (void)tracker.wait(kChat, 71, seconds{30}); });
    EXPECT_EQ(e.code, TdError::kStopped);
    t.join();
}

TEST_F(SendTrackerTest, DestroyedTrackerUnsubscribes) {
    { const SendTracker tracker(*client); }
    transport->push(client->id(), succeeded(80, 81));
    for (int i = 0; i < 500 && client->stats().dropped_updates < 1; ++i) {
        std::this_thread::sleep_for(milliseconds{2});
    }
    EXPECT_EQ(client->stats().dropped_updates, 1U);
}

}  // namespace
}  // namespace telega::td::test
