// Авторизация: машина состояний, подсказчик, параметры (SPEC 10.1, 10.3).

#include "td/auth.h"

#include <gtest/gtest.h>

#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "fake_transport.h"
#include "td/td_runtime.h"

namespace telega::td::test {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;

Json state_update(const std::string& type, Json extra = Json::object()) {
    extra["@type"] = type;
    return Json{{"@type", "updateAuthorizationState"}, {"authorization_state", std::move(extra)}};
}

// Сервер TDLib по части авторизации.
class FakeAuthServer {
  public:
    struct Config {
        bool has_account = true;  // false: после кода нужна регистрация
        bool kick_with_update = true;  // слать первое состояние апдейтом (как TDLib)
        std::string stop_after_phone;  // не «Ready», а это состояние после телефона/токена
        // Ответ на getAuthorizationState приходит РАНЬШЕ апдейта с тем же состоянием, а переход
        // после setTdlibParameters — с задержкой (в окне между ними видно повтор начального).
        bool answer_before_update = false;
    };

    explicit FakeAuthServer(Config config) : config_(std::move(config)) {}
    ~FakeAuthServer() {
        for (auto& t : threads_) {
            t.join();
        }
    }
    FakeAuthServer(const FakeAuthServer&) = delete;
    FakeAuthServer& operator=(const FakeAuthServer&) = delete;

    // Для answer_before_update: апдейт с начальным состоянием шлётся, когда клиент уже принял
    // ответ.
    void set_seeded(std::function<bool()> seeded) { seeded_ = std::move(seeded); }

    void attach(FakeTransport& transport) {
        transport.set_responder([this](FakeTransport& t, int id, const Json& r) { on(t, id, r); });
    }

    std::vector<std::string> requests() {
        const std::lock_guard<std::mutex> lock(mu_);
        return types_;
    }
    int parameters_requests() {
        const std::lock_guard<std::mutex> lock(mu_);
        return parameters_;
    }
    Json last(const std::string& type) {
        const std::lock_guard<std::mutex> lock(mu_);
        return last_[type];
    }

  private:
    void to(FakeTransport& t, int id, const std::string& type, Json extra = Json::object()) {
        state_ = type;
        t.push(id, state_update(type, std::move(extra)));
    }
    static void ok(FakeTransport& t, int id, const Json& r) {
        t.respond(id, r, Json{{"@type", "ok"}});
    }
    static void err(FakeTransport& t, int id, const Json& r, int code, const std::string& text) {
        t.respond(id, r, Json{{"@type", "error"}, {"code", code}, {"message", text}});
    }
    void finish(FakeTransport& t, int id) {
        if (!config_.stop_after_phone.empty()) {
            to(t, id, config_.stop_after_phone);
        } else {
            to(t, id, "authorizationStateReady");
        }
    }

    void on(FakeTransport& t, int id, const Json& r) {
        const std::lock_guard<std::mutex> lock(mu_);
        const std::string type = r.value("@type", std::string{});
        types_.push_back(type);
        last_[type] = r;
        if (!started_) {
            started_ = true;
            state_ = "authorizationStateWaitTdlibParameters";
            if (config_.kick_with_update && !config_.answer_before_update) {
                t.push(id, state_update(state_));
            }
        }
        if (type == "getAuthorizationState") {
            t.respond(id, r, Json{{"@type", state_}});
            if (config_.answer_before_update && !late_update_sent_) {
                late_update_sent_ = true;
                threads_.emplace_back([this, &t, id] {
                    while (!seeded_()) {
                        std::this_thread::sleep_for(std::chrono::milliseconds{1});
                    }
                    t.push(id, state_update("authorizationStateWaitTdlibParameters"));
                });
            }
        } else if (type == "setTdlibParameters") {
            ++parameters_;
            if (state_ != "authorizationStateWaitTdlibParameters") {
                err(t, id, r, 400, "Unexpected setTdlibParameters");
                return;
            }
            ok(t, id, r);
            if (config_.answer_before_update) {
                state_ = "authorizationStateWaitPhoneNumber";
                threads_.emplace_back([&t, id] {
                    std::this_thread::sleep_for(std::chrono::milliseconds{150});
                    t.push(id, state_update("authorizationStateWaitPhoneNumber"));
                });
            } else {
                to(t, id, "authorizationStateWaitPhoneNumber");
            }
        } else if (type == "setAuthenticationPhoneNumber") {
            ok(t, id, r);
            to(t, id, "authorizationStateWaitCode",
               {{"code_info", {{"@type", "authenticationCodeInfo"}, {"timeout", 0}}}});
        } else if (type == "checkAuthenticationBotToken") {
            if (r.value("token", "") != "123:good") {
                err(t, id, r, 400, "ACCESS_TOKEN_INVALID");
                return;
            }
            ok(t, id, r);
            to(t, id, "authorizationStateReady");
        } else if (type == "checkAuthenticationCode") {
            if (r.value("code", "") != "22222") {
                err(t, id, r, 400, "PHONE_CODE_INVALID");
                return;
            }
            ok(t, id, r);
            if (!config_.has_account) {
                to(t, id, "authorizationStateWaitRegistration");
            } else {
                finish(t, id);
            }
        } else if (type == "registerUser") {
            ok(t, id, r);
            finish(t, id);
        } else if (type == "checkAuthenticationPassword") {
            if (r.value("password", "") != "hunter2") {
                err(t, id, r, 400, "PASSWORD_HASH_INVALID");
                return;
            }
            ok(t, id, r);
            to(t, id, "authorizationStateReady");
        } else {
            err(t, id, r, 400, "Unexpected request " + type);
        }
    }

    std::mutex mu_;
    Config config_;
    std::function<bool()> seeded_;
    std::vector<std::thread> threads_;
    bool late_update_sent_ = false;
    int parameters_ = 0;
    bool started_ = false;
    std::string state_;
    std::vector<std::string> types_;
    std::map<std::string, Json> last_;
};

class ScriptedPrompter final : public AuthPrompter {
  public:
    std::string phone_number() override {
        calls.emplace_back("phone");
        return "+99966X0000";
    }
    std::string code(const Json& info) override {
        calls.emplace_back("code");
        code_info = info;
        std::string c = codes.empty() ? "22222" : codes.front();
        if (!codes.empty()) {
            codes.pop_front();
        }
        return c;
    }
    std::string password(const std::string& hint) override {
        calls.emplace_back("password");
        password_hint = hint;
        return "hunter2";
    }
    RegistrationName registration() override {
        calls.emplace_back("registration");
        return {"tgfs", "bench"};
    }
    std::string bot_token() override {
        calls.emplace_back("token");
        return token;
    }
    bool retry_after_error(AuthStep step, const TdError& error) override {
        retried.emplace_back(step, error.code);
        return allow_retry;
    }

    std::vector<std::string> calls;
    std::deque<std::string> codes;
    std::string token = "123:good";
    std::string password_hint;
    Json code_info;
    bool allow_retry = false;
    std::vector<std::pair<AuthStep, int>> retried;
};

TdError error_of(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const TdException& e) {
        return e.error();
    }
    ADD_FAILURE() << "TdException expected";
    return {};
}

class AuthTest : public ::testing::Test {
  protected:
    void SetUp() override {
        params.use_test_dc = true;
        params.database_directory = "/tmp/x/db";
        params.files_directory = "/tmp/x/files";
        params.api_id = 12345;
        params.api_hash = "hash-not-a-secret";
        params.application_version = "0.0.1";
    }
    void start(FakeAuthServer::Config config) {
        server = std::make_unique<FakeAuthServer>(std::move(config));
        server->attach(*transport);
        client = runtime.create_client();
    }

    std::shared_ptr<FakeTransport> transport = std::make_shared<FakeTransport>();
    TdRuntime runtime{transport, milliseconds{10}};
    std::unique_ptr<FakeAuthServer> server;
    std::shared_ptr<TdClient> client;
    TdlibParameters params;
    ScriptedPrompter prompter;
};

TEST_F(AuthTest, BotLogin) {
    start({});
    AuthOptions options;
    options.mode = AuthMode::kBot;
    authorize(*client, params, prompter, options);
    EXPECT_EQ(prompter.calls, std::vector<std::string>{"token"});
    const auto state = client->auth_state();
    EXPECT_EQ(state.has_value() ? state->type() : std::string{}, "authorizationStateReady");

    const Json p = server->last("setTdlibParameters");
    EXPECT_TRUE(p["use_test_dc"].get<bool>());
    EXPECT_EQ(p["api_id"], 12345);
    EXPECT_EQ(p["database_directory"], "/tmp/x/db");
    EXPECT_FALSE(p["use_message_database"].get<bool>());
    EXPECT_FALSE(p["use_secret_chats"].get<bool>());
    EXPECT_EQ(p["device_model"], "telega-fs");
    EXPECT_EQ(p["database_encryption_key"], "");
}

TEST_F(AuthTest, BotWithInvalidTokenFails) {
    start({});
    prompter.token = "bad";
    AuthOptions options;
    options.mode = AuthMode::kBot;
    const TdError e = error_of([&] { authorize(*client, params, prompter, options); });
    EXPECT_EQ(e.code, 400);
    EXPECT_EQ(e.message, "ACCESS_TOKEN_INVALID");
    ASSERT_EQ(prompter.retried.size(), 1U);
    EXPECT_EQ(prompter.retried[0].first, AuthStep::kBotToken);
}

TEST_F(AuthTest, UserLogin) {
    start({});
    authorize(*client, params, prompter);
    EXPECT_EQ(prompter.calls, (std::vector<std::string>{"phone", "code"}));
    EXPECT_EQ(prompter.code_info.value("@type", ""), "authenticationCodeInfo");
    EXPECT_EQ(server->last("setAuthenticationPhoneNumber")["phone_number"], "+99966X0000");
}

TEST_F(AuthTest, UserRegistration) {
    FakeAuthServer::Config config;
    config.has_account = false;
    start(config);
    authorize(*client, params, prompter);
    EXPECT_EQ(prompter.calls, (std::vector<std::string>{"phone", "code", "registration"}));
    const Json reg = server->last("registerUser");
    EXPECT_EQ(reg["first_name"], "tgfs");
    EXPECT_EQ(reg["last_name"], "bench");
}

// Вход с 2FA: отдельный ответчик со своей цепочкой состояний.
TEST_F(AuthTest, PasswordFlow) {
    std::mutex mu;
    std::string state = "start";
    transport->set_responder([&](FakeTransport& t, int id, const Json& r) {
        const std::lock_guard<std::mutex> lock(mu);
        const std::string type = r["@type"];
        auto go = [&](const std::string& s, Json extra = Json::object()) {
            state = s;
            t.push(id, state_update(s, std::move(extra)));
        };
        if (state == "start") {
            go("authorizationStateWaitTdlibParameters");
        }
        if (type == "getAuthorizationState") {
            t.respond(id, r, Json{{"@type", state}});
        } else if (type == "setTdlibParameters") {
            t.respond(id, r, Json{{"@type", "ok"}});
            go("authorizationStateWaitPhoneNumber");
        } else if (type == "setAuthenticationPhoneNumber") {
            t.respond(id, r, Json{{"@type", "ok"}});
            go("authorizationStateWaitCode");
        } else if (type == "checkAuthenticationCode") {
            t.respond(id, r, Json{{"@type", "ok"}});
            go("authorizationStateWaitPassword", {{"password_hint", "pet name"}});
        } else if (type == "checkAuthenticationPassword") {
            t.respond(id, r, Json{{"@type", "ok"}});
            go("authorizationStateReady");
        }
    });
    client = runtime.create_client();
    authorize(*client, params, prompter);
    EXPECT_EQ(prompter.calls, (std::vector<std::string>{"phone", "code", "password"}));
    EXPECT_EQ(prompter.password_hint, "pet name");
}

TEST_F(AuthTest, WrongCodeIsRetriedWhenPrompterAgrees) {
    start({});
    prompter.codes = {"00000", "22222"};
    prompter.allow_retry = true;
    authorize(*client, params, prompter);
    EXPECT_EQ(prompter.calls, (std::vector<std::string>{"phone", "code", "code"}));
    ASSERT_EQ(prompter.retried.size(), 1U);
    EXPECT_EQ(prompter.retried[0].first, AuthStep::kCode);
}

TEST_F(AuthTest, WrongCodeIsFatalByDefault) {
    start({});
    prompter.codes = {"00000"};
    const TdError e = error_of([&] { authorize(*client, params, prompter); });
    EXPECT_EQ(e.message, "PHONE_CODE_INVALID");
    EXPECT_EQ(prompter.calls.size(), 2U);
}

TEST_F(AuthTest, RetriesAreCapped) {
    start({});
    prompter.codes = {"1", "2", "3", "4", "5", "6", "7", "8", "9"};
    prompter.allow_retry = true;
    AuthOptions options;
    options.max_retries = 2;
    const TdError e = error_of([&] { authorize(*client, params, prompter, options); });
    EXPECT_EQ(e.message, "PHONE_CODE_INVALID");
    EXPECT_EQ(prompter.retried.size(), 2U);
}

TEST_F(AuthTest, ClosedIsAnError) {
    FakeAuthServer::Config config;
    config.stop_after_phone = "authorizationStateClosed";
    start(config);
    const TdError e = error_of([&] { authorize(*client, params, prompter); });
    EXPECT_EQ(e.code, TdError::kClosed);
}

TEST_F(AuthTest, LoggingOutIsAnError) {
    FakeAuthServer::Config config;
    config.stop_after_phone = "authorizationStateLoggingOut";
    start(config);
    const TdError e = error_of([&] { authorize(*client, params, prompter); });
    EXPECT_EQ(e.code, TdError::kClosed);
}

TEST_F(AuthTest, UnsupportedStateIsAnError) {
    FakeAuthServer::Config config;
    config.stop_after_phone = "authorizationStateWaitEmailAddress";
    start(config);
    const TdError e = error_of([&] { authorize(*client, params, prompter); });
    EXPECT_EQ(e.code, TdError::kUnexpected);
}

TEST_F(AuthTest, FirstStateFromExplicitQuery) {
    // Первого апдейта нет: начальное состояние приходит только ответом getAuthorizationState.
    FakeAuthServer::Config config;
    config.kick_with_update = false;
    start(config);
    AuthOptions options;
    options.timeout = seconds{5};
    options.mode = AuthMode::kBot;
    EXPECT_NO_THROW(authorize(*client, params, prompter, options));
    EXPECT_EQ(server->requests().front(), "getAuthorizationState");
}

// Ответ getAuthorizationState раньше апдейта: то же состояние дважды дало бы второй
// setTdlibParameters и 400.
TEST_F(AuthTest, ResponseBeforeTheUpdateDoesNotRepeatTheInitialState) {
    FakeAuthServer::Config config;
    config.answer_before_update = true;
    start(config);
    server->set_seeded([&] { return client->auth_state().has_value(); });
    AuthOptions options;
    options.timeout = seconds{5};
    options.mode = AuthMode::kBot;
    EXPECT_NO_THROW(authorize(*client, params, prompter, options));
    EXPECT_EQ(server->parameters_requests(), 1);
}

TEST_F(AuthTest, AlreadyReadyClientReturnsAtOnce) {
    start({});
    AuthOptions options;
    options.mode = AuthMode::kBot;
    authorize(*client, params, prompter, options);
    const size_t before = server->requests().size();
    authorize(*client, params, prompter, options);
    EXPECT_EQ(server->requests().size(), before);
}

TEST_F(AuthTest, TimesOutWhenServerIsSilent) {
    client = runtime.create_client();
    AuthOptions options;
    options.timeout = milliseconds{200};
    options.call_timeout = milliseconds{100};
    const TdError e = error_of([&] { authorize(*client, params, prompter, options); });
    EXPECT_TRUE(e.is_timeout());
}

TEST(TdlibParametersTest, EncodesKeyAsBase64) {
    TdlibParameters p;
    for (const auto& [raw, encoded] : std::vector<std::pair<std::string, std::string>>{
             {"", ""}, {"f", "Zg=="}, {"fo", "Zm8="}, {"foo", "Zm9v"}, {"foob", "Zm9vYg=="}}) {
        p.database_encryption_key = raw;
        EXPECT_EQ(p.to_request()["database_encryption_key"], encoded);
    }
    p.database_encryption_key = std::string("\xff\x00\xfe", 3);
    EXPECT_EQ(p.to_request()["database_encryption_key"], "/wD+");
}

}  // namespace
}  // namespace telega::td::test
