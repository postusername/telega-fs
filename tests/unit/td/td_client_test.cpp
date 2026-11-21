// TdRuntime и TdClient на фейковом транспорте: сопоставление ответов, таймауты, несколько
// клиентов, подписки, остановка (SPEC 10.1).

#include "td/td_client.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "fake_transport.h"
#include "td/td_runtime.h"

namespace telega::td::test {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;

bool wait_until(const std::function<bool()>& pred, milliseconds timeout = seconds{5}) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(milliseconds{2});
    }
    return true;
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

// Ответчик «эхо»: на любой запрос отвечает {"@type":"echo","v":<request.v>}.
void echo(FakeTransport& t, int client_id, const Json& request) {
    t.respond(client_id, request, Json{{"@type", "echo"}, {"v", request.value("v", Json{})}});
}

class TdClientTest : public ::testing::Test {
  protected:
    std::shared_ptr<FakeTransport> transport = std::make_shared<FakeTransport>();
    TdRuntime runtime{transport, milliseconds{10}};
};

TEST_F(TdClientTest, MatchesResponsesToRequests) {
    transport->set_responder(echo);
    auto client = runtime.create_client();
    std::vector<std::thread> threads;
    threads.reserve(8);
    std::atomic<int> bad{0};
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < 50; ++i) {
                const int v = t * 1000 + i;
                const Json r = client->call(Json{{"@type", "ping"}, {"v", v}});
                if (r.value("v", -1) != v || r.contains("@extra") || r.contains("@client_id")) {
                    ++bad;
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(client->stats().requests, 400U);
    EXPECT_EQ(client->stats().timeouts, 0U);
}

TEST_F(TdClientTest, ExtraIsUniqueUnsignedAndOverwritten) {
    transport->set_responder(echo);
    auto client = runtime.create_client();
    client->call(Json{{"@type", "a"}, {"@extra", "mine"}});
    client->call(Json{{"@type", "b"}});
    const auto sent = transport->sent(client->id());
    ASSERT_EQ(sent.size(), 2U);
    EXPECT_TRUE(sent[0]["@extra"].is_number_unsigned());
    EXPECT_NE(sent[0]["@extra"], sent[1]["@extra"]);
}

TEST_F(TdClientTest, ErrorResponseBecomesTdException) {
    transport->set_responder([](FakeTransport& t, int id, const Json& r) {
        t.respond(id, r,
                  Json{{"@type", "error"},
                       {"code", 429},
                       {"message", "Too Many Requests: retry after 9"}});
    });
    auto client = runtime.create_client();
    const TdError e = error_of([&] { client->call(Json{{"@type", "sendMessage"}}); });
    EXPECT_EQ(e.code, 429);
    EXPECT_EQ(e.retry_after, seconds{9});

    auto future = client->call_async(Json{{"@type", "sendMessage"}});
    EXPECT_THROW((void)future.get(), TdException);
}

TEST_F(TdClientTest, TimeoutThenLateResponseIsDropped) {
    auto client = runtime.create_client();
    const TdError e = error_of([&] { client->call(Json{{"@type", "slow"}}, milliseconds{50}); });
    EXPECT_TRUE(e.is_timeout());
    EXPECT_EQ(client->stats().timeouts, 1U);

    const auto sent = transport->sent(client->id());
    ASSERT_EQ(sent.size(), 1U);
    transport->respond(client->id(), sent[0], Json{{"@type", "ok"}});
    ASSERT_TRUE(wait_until([&] { return client->stats().late_responses == 1; }));

    // Следующий запрос получает свой ответ, а не запоздавший.
    transport->set_responder(echo);
    const Json r = client->call(Json{{"@type", "ping"}, {"v", 7}});
    EXPECT_EQ(r["v"], 7);
}

TEST_F(TdClientTest, AsyncTimeoutFiresInReceiveThread) {
    auto client = runtime.create_client();
    auto future = client->call_async(Json{{"@type", "slow"}}, milliseconds{30});
    ASSERT_EQ(future.wait_for(seconds{5}), std::future_status::ready);
    EXPECT_TRUE(error_of([&] { (void)future.get(); }).is_timeout());
}

TEST_F(TdClientTest, TwoClientsInOneRuntime) {
    transport->set_responder(echo);
    auto a = runtime.create_client();
    auto b = runtime.create_client();
    EXPECT_NE(a->id(), b->id());

    std::atomic<int> a_updates{0};
    std::atomic<int> b_updates{0};
    auto sa = a->subscribe("updateX", [&](const Json&) { ++a_updates; });
    auto sb = b->subscribe("updateX", [&](const Json&) { ++b_updates; });
    transport->push(a->id(), Json{{"@type", "updateX"}});
    transport->push(b->id(), Json{{"@type", "updateX"}});
    transport->push(b->id(), Json{{"@type", "updateX"}});
    ASSERT_TRUE(wait_until([&] { return a_updates == 1 && b_updates == 2; }));

    EXPECT_EQ(a->call(Json{{"@type", "p"}, {"v", 1}})["v"], 1);
    EXPECT_EQ(b->call(Json{{"@type", "p"}, {"v", 2}})["v"], 2);
    EXPECT_EQ(transport->sent(a->id()).size(), 1U);
    EXPECT_EQ(transport->sent(b->id()).size(), 1U);
}

TEST_F(TdClientTest, UnknownClientAndGarbageAreCounted) {
    auto client = runtime.create_client();
    transport->push(999, Json{{"@type", "updateX"}});
    transport->push_raw("not json");
    transport->push_raw("[1,2]");
    ASSERT_TRUE(wait_until([&] { return runtime.stats().malformed == 2; }));
    EXPECT_EQ(runtime.stats().unknown_client, 1U);
}

TEST_F(TdClientTest, SubscriptionsDeliverByTypeAndUnsubscribe) {
    auto client = runtime.create_client();
    std::atomic<int> a{0};
    std::atomic<int> b{0};
    auto sub_a = client->subscribe("updateA", [&](const Json& u) {
        if (u.value("n", 0) == 1) {
            ++a;
        }
    });
    auto sub_b = client->subscribe("updateB", [&](const Json&) { ++b; });
    transport->push(client->id(), Json{{"@type", "updateA"}, {"n", 1}});
    transport->push(client->id(), Json{{"@type", "updateB"}});
    ASSERT_TRUE(wait_until([&] { return a == 1 && b == 1; }));

    sub_a.reset();
    EXPECT_FALSE(sub_a.active());
    transport->push(client->id(), Json{{"@type", "updateA"}, {"n", 1}});
    transport->push(client->id(), Json{{"@type", "updateB"}});
    ASSERT_TRUE(wait_until([&] { return b == 2; }));
    EXPECT_EQ(a.load(), 1);
}

TEST_F(TdClientTest, UpdatesWithoutSubscribersAreDropped) {
    auto client = runtime.create_client();
    transport->push(client->id(), Json{{"@type", "updateNobody"}});
    transport->push(client->id(), Json{{"@type", "updateNobody"}});
    ASSERT_TRUE(wait_until([&] { return client->stats().dropped_updates == 2; }));
    EXPECT_EQ(client->stats().updates, 2U);
}

TEST_F(TdClientTest, SeveralSubscribersAndMove) {
    auto client = runtime.create_client();
    std::atomic<int> n{0};
    auto s1 = client->subscribe("u", [&](const Json&) { ++n; });
    Subscription s2 = client->subscribe("u", [&](const Json&) { ++n; });
    const Subscription moved = std::move(s2);
    EXPECT_TRUE(moved.active());
    transport->push(client->id(), Json{{"@type", "u"}});
    ASSERT_TRUE(wait_until([&] { return n == 2; }));
}

TEST_F(TdClientTest, ThrowingHandlerDoesNotKillReceiveThread) {
    transport->set_responder(echo);
    auto client = runtime.create_client();
    std::atomic<int> after{0};
    auto bad = client->subscribe("u", [](const Json&) { throw std::runtime_error("boom"); });
    auto good = client->subscribe("u", [&](const Json&) { ++after; });
    transport->push(client->id(), Json{{"@type", "u"}});
    ASSERT_TRUE(wait_until([&] { return after == 1; }));
    EXPECT_EQ(client->stats().handler_failures, 1U);
    EXPECT_EQ(client->call(Json{{"@type", "p"}, {"v", 3}})["v"], 3);
}

TEST_F(TdClientTest, HandlerCanUnsubscribeItself) {
    auto client = runtime.create_client();
    std::atomic<int> n{0};
    Subscription sub;
    sub = client->subscribe("u", [&](const Json&) {
        ++n;
        sub.reset();
    });
    transport->push(client->id(), Json{{"@type", "u"}});
    ASSERT_TRUE(wait_until([&] { return n == 1; }));
    transport->push(client->id(), Json{{"@type", "u"}});
    transport->push(client->id(), Json{{"@type", "other"}});
    ASSERT_TRUE(wait_until([&] { return client->stats().updates == 3; }));
    EXPECT_EQ(n.load(), 1);
}

TEST_F(TdClientTest, UnsubscribeWaitsForRunningHandler) {
    auto client = runtime.create_client();
    std::atomic<bool> inside{false};
    std::atomic<bool> release{false};
    std::atomic<bool> finished{false};
    auto sub = client->subscribe("u", [&](const Json&) {
        inside = true;
        while (!release) {
            std::this_thread::sleep_for(milliseconds{1});
        }
        finished = true;
    });
    transport->push(client->id(), Json{{"@type", "u"}});
    ASSERT_TRUE(wait_until([&] { return inside.load(); }));
    std::atomic<bool> unsubscribed{false};
    std::thread t([&] {
        sub.reset();
        unsubscribed = true;
        EXPECT_TRUE(finished.load());  // отписка вернулась только после конца обработчика
    });
    std::this_thread::sleep_for(milliseconds{50});
    EXPECT_FALSE(unsubscribed.load());
    release = true;
    t.join();
    EXPECT_TRUE(unsubscribed.load());
}

TEST_F(TdClientTest, StopFailsPendingRequests) {
    auto client = runtime.create_client();
    auto future = client->call_async(Json{{"@type", "hang"}}, seconds{60});
    std::promise<TdError> blocked;
    std::thread t([&] {
        blocked.set_value(error_of([&] { client->call(Json{{"@type", "hang"}}, seconds{60}); }));
    });
    ASSERT_TRUE(transport->wait_sent(client->id(), 2, seconds{5}));

    const auto started = std::chrono::steady_clock::now();
    runtime.stop();
    EXPECT_LT(std::chrono::steady_clock::now() - started, seconds{5});

    auto fut = blocked.get_future();
    ASSERT_EQ(fut.wait_for(seconds{5}), std::future_status::ready);
    EXPECT_EQ(fut.get().code, TdError::kStopped);
    t.join();
    EXPECT_EQ(error_of([&] { (void)future.get(); }).code, TdError::kStopped);

    // После остановки новые запросы и клиенты отказывают сразу; stop идемпотентна.
    EXPECT_EQ(error_of([&] { client->call(Json{{"@type", "x"}}); }).code, TdError::kStopped);
    EXPECT_THROW((void)runtime.create_client(), TdException);
    runtime.stop();
}

TEST_F(TdClientTest, StopWithoutClientsAndWaitingForAuthState) {
    TdRuntime idle{transport};
    idle.stop();  // потока приёма ещё нет

    auto client = runtime.create_client();
    std::promise<TdError> waiting;
    std::thread t([&] {
        waiting.set_value(error_of([&] { (void)client->wait_auth_state(0, seconds{60}); }));
    });
    std::this_thread::sleep_for(milliseconds{30});
    runtime.stop();
    auto fut = waiting.get_future();
    ASSERT_EQ(fut.wait_for(seconds{5}), std::future_status::ready);
    EXPECT_EQ(fut.get().code, TdError::kStopped);
    t.join();
}

// Ответ getAuthorizationState мог опередить апдейт с тем же состоянием: тот не должен считаться
// новым состоянием, иначе авторизация обработает его второй раз.
TEST_F(TdClientTest, UpdateRepeatingTheSeededStateIsNotANewState) {
    auto client = runtime.create_client();
    const Json initial{{"@type", "authorizationStateWaitTdlibParameters"}};
    client->seed_auth_state(initial);
    const auto seeded = client->auth_state();
    EXPECT_EQ(seeded.has_value() ? seeded->seq : 0U, 1U);
    transport->push(client->id(),
                    Json{{"@type", "updateAuthorizationState"}, {"authorization_state", initial}});
    transport->push(client->id(), Json{{"@type", "updateAuthorizationState"},
                                       {"authorization_state",
                                        {{"@type", "authorizationStateWaitPhoneNumber"}}}});
    const AuthState next = client->wait_auth_state(1, seconds{5});
    EXPECT_EQ(next.seq, 2U);  // а не 3: повтор начального состояния seq не занял
    EXPECT_EQ(next.type(), "authorizationStateWaitPhoneNumber");
}

TEST_F(TdClientTest, SecondUpdateWithTheSeededStateIsAnOrdinaryUpdate) {
    // Дубликатом считается только первый апдейт после seed.
    auto client = runtime.create_client();
    const Json initial{{"@type", "authorizationStateWaitPhoneNumber"}};
    client->seed_auth_state(initial);
    const Json update{{"@type", "updateAuthorizationState"}, {"authorization_state", initial}};
    transport->push(client->id(), update);
    transport->push(client->id(), update);
    EXPECT_EQ(client->wait_auth_state(1, seconds{5}).seq, 2U);
}

TEST_F(TdClientTest, ClosedStateFailsPendingAndRejectsNewRequests) {
    auto client = runtime.create_client();
    auto future = client->call_async(Json{{"@type", "hang"}}, seconds{60});
    transport->push(client->id(),
                    Json{{"@type", "updateAuthorizationState"},
                         {"authorization_state", {{"@type", "authorizationStateClosed"}}}});
    ASSERT_EQ(future.wait_for(seconds{5}), std::future_status::ready);
    EXPECT_EQ(error_of([&] { (void)future.get(); }).code, TdError::kClosed);
    EXPECT_TRUE(client->closed());
    const auto reason = client->dead_reason();
    EXPECT_EQ(reason.has_value() ? reason->code : 0, TdError::kClosed);
    EXPECT_EQ(error_of([&] { client->call(Json{{"@type", "x"}}); }).code, TdError::kClosed);
}

TEST_F(TdClientTest, CloseWaitsForClosedState) {
    transport->set_responder([](FakeTransport& t, int id, const Json& r) {
        t.respond(id, r, Json{{"@type", "ok"}});
        if (r["@type"] == "close") {
            t.push(id, Json{{"@type", "updateAuthorizationState"},
                            {"authorization_state", {{"@type", "authorizationStateClosed"}}}});
        }
    });
    auto client = runtime.create_client();
    EXPECT_TRUE(client->close(seconds{5}));
    EXPECT_TRUE(client->close(seconds{5}));  // повторное закрытие — без запросов
    EXPECT_EQ(transport->sent(client->id()).size(), 1U);
}

TEST_F(TdClientTest, CloseTimesOutWithoutClosedState) {
    transport->set_responder(echo);
    auto client = runtime.create_client();
    EXPECT_FALSE(client->close(milliseconds{100}));
    EXPECT_FALSE(client->closed());
}

TEST_F(TdClientTest, LogOutSendsLogOut) {
    transport->set_responder([](FakeTransport& t, int id, const Json& r) {
        t.respond(id, r, Json{{"@type", "ok"}});
        if (r["@type"] == "logOut") {
            t.push(id, Json{{"@type", "updateAuthorizationState"},
                            {"authorization_state", {{"@type", "authorizationStateClosed"}}}});
        }
    });
    auto client = runtime.create_client();
    EXPECT_TRUE(client->log_out(seconds{5}));
    EXPECT_EQ(transport->sent(client->id())[0]["@type"], "logOut");
}

}  // namespace
}  // namespace telega::td::test
