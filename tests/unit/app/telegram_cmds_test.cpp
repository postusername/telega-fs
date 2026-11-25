// login, logout, init и mount для store = "telegram" на фейковом транспорте TDLib. Сеть и настоящая
// TDLib не нужны.

#include "app/telegram_cmds.h"

#include <gtest/gtest.h>
#include <stdlib.h>  // NOLINT(modernize-deprecated-headers): setenv/unsetenv только в <stdlib.h>
#include <unistd.h>

#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "../td/fake_transport.h"
#include "app/cli_util.h"
#include "app/commands.h"
#include "app/config.h"
#include "app/terminal_prompter.h"
#include "app/tg_session.h"
#include "common/files.h"
#include "crypto/keyring.h"
#include "meta/state_db.h"

namespace telega::app {
namespace {

namespace fs = std::filesystem;
using td::Json;
using td::test::FakeTransport;

void set_env(const char* name, const std::string& value) {
    ::setenv(name, value.c_str(), 1);  // NOLINT(concurrency-mt-unsafe)
}
void unset_env(const char* name) {
    ::unsetenv(name);  // NOLINT(concurrency-mt-unsafe)
}

std::string base64_of(const std::string& raw) {
    static constexpr std::string_view kAlphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    for (size_t i = 0; i < raw.size(); i += 3) {
        uint32_t n = static_cast<uint32_t>(static_cast<uint8_t>(raw[i])) << 16U;
        if (i + 1 < raw.size()) {
            n |= static_cast<uint32_t>(static_cast<uint8_t>(raw[i + 1])) << 8U;
        }
        if (i + 2 < raw.size()) {
            n |= static_cast<uint32_t>(static_cast<uint8_t>(raw[i + 2]));
        }
        out += kAlphabet[(n >> 18U) & 63U];
        out += kAlphabet[(n >> 12U) & 63U];
        out += i + 1 < raw.size() ? kAlphabet[(n >> 6U) & 63U] : '=';
        out += i + 2 < raw.size() ? kAlphabet[n & 63U] : '=';
    }
    return out;
}

// Сервер TDLib: авторизация, getMe, logOut, close, createNewSupergroupChat. Сессия (authorized_)
// живёт дольше клиентов, как база TDLib на диске.
class FakeAccount {
  public:
    struct Options {
        bool bot = true;
        bool need_password = false;
    };
    explicit FakeAccount(Options options) : options_(options) {}

    void attach(FakeTransport& transport) {
        transport.set_responder([this](FakeTransport& t, int id, const Json& r) { on(t, id, r); });
    }

    [[nodiscard]] std::vector<std::string> types() {
        const std::lock_guard<std::mutex> lock(mu_);
        return types_;
    }
    [[nodiscard]] Json last(const std::string& type) {
        const std::lock_guard<std::mutex> lock(mu_);
        return last_[type];
    }
    [[nodiscard]] bool authorized() {
        const std::lock_guard<std::mutex> lock(mu_);
        return authorized_;
    }
    void set_authorized(bool v) {
        const std::lock_guard<std::mutex> lock(mu_);
        authorized_ = v;
    }

  private:
    static Json update(const std::string& type, Json extra = Json::object()) {
        extra["@type"] = type;
        return Json{{"@type", "updateAuthorizationState"}, {"authorization_state", extra}};
    }
    void to(FakeTransport& t, int id, const std::string& type, Json extra = Json::object()) {
        state_[id] = type;
        t.push(id, update(type, std::move(extra)));
    }
    static void ok(FakeTransport& t, int id, const Json& r) {
        t.respond(id, r, Json{{"@type", "ok"}});
    }
    static void err(FakeTransport& t, int id, const Json& r, int code, const std::string& text) {
        t.respond(id, r, Json{{"@type", "error"}, {"code", code}, {"message", text}});
    }
    [[nodiscard]] static Json code_info() {
        return {{"code_info",
                 {{"@type", "authenticationCodeInfo"},
                  {"type", {{"@type", "authenticationCodeTypeSms"}, {"length", 5}}},
                  {"next_type", {{"@type", "authenticationCodeTypeCall"}}},
                  {"timeout", 60}}}};
    }
    void ready(FakeTransport& t, int id) {
        authorized_ = true;
        to(t, id, "authorizationStateReady");
    }

    void on(FakeTransport& t, int id, const Json& r) {
        const std::lock_guard<std::mutex> lock(mu_);
        const std::string type = r.value("@type", std::string{});
        types_.push_back(type);
        last_[type] = r;
        if (started_.insert(id).second) {
            state_[id] = "authorizationStateWaitTdlibParameters";
            t.push(id, update(state_[id]));
        }
        if (type == "getAuthorizationState") {
            t.respond(id, r, Json{{"@type", state_[id]}});
        } else if (type == "setTdlibParameters") {
            ok(t, id, r);
            if (authorized_) {
                to(t, id, "authorizationStateReady");
            } else {
                to(t, id, "authorizationStateWaitPhoneNumber");
            }
        } else if (type == "checkAuthenticationBotToken") {
            if (r.value("token", "") != "123:good") {
                err(t, id, r, 400, "ACCESS_TOKEN_INVALID");
                return;
            }
            ok(t, id, r);
            ready(t, id);
        } else if (type == "setAuthenticationPhoneNumber" || type == "resendAuthenticationCode") {
            ok(t, id, r);
            to(t, id, "authorizationStateWaitCode", code_info());
        } else if (type == "checkAuthenticationCode") {
            if (r.value("code", "") != "12345") {
                err(t, id, r, 400, "PHONE_CODE_INVALID");
                return;
            }
            ok(t, id, r);
            if (options_.need_password) {
                to(t, id, "authorizationStateWaitPassword", {{"password_hint", "pet"}});
            } else {
                ready(t, id);
            }
        } else if (type == "checkAuthenticationPassword") {
            if (r.value("password", "") != "hunter2") {
                err(t, id, r, 400, "PASSWORD_HASH_INVALID");
                return;
            }
            ok(t, id, r);
            ready(t, id);
        } else if (type == "getMe") {
            t.respond(
                id, r,
                Json{{"@type", "user"},
                     {"id", 4242},
                     {"phone_number", "15550001"},
                     {"usernames", {{"active_usernames", Json::array({"somebody"})}}},
                     {"type", {{"@type", options_.bot ? "userTypeBot" : "userTypeRegular"}}}});
        } else if (type == "createNewSupergroupChat") {
            t.respond(id, r, Json{{"@type", "chat"}, {"id", -1001000000777}});
        } else if (type == "logOut" || type == "close") {
            ok(t, id, r);
            if (type == "logOut") {
                authorized_ = false;
            }
            t.push(id, update("authorizationStateClosed"));
        } else {
            err(t, id, r, 400, "unexpected request " + type);
        }
    }

    std::mutex mu_;
    Options options_;
    bool authorized_ = false;
    std::set<int> started_;
    std::map<int, std::string> state_;
    std::vector<std::string> types_;
    std::map<std::string, Json> last_;
};

// Подмена std::cin на время теста.
class StdinScript {
  public:
    explicit StdinScript(const std::string& text) : in_(text), old_(std::cin.rdbuf(in_.rdbuf())) {}
    StdinScript(const StdinScript&) = delete;
    StdinScript& operator=(const StdinScript&) = delete;
    ~StdinScript() { std::cin.rdbuf(old_); }

  private:
    std::istringstream in_;
    std::streambuf* old_;
};

class TelegramCmds : public ::testing::Test {
  protected:
    void SetUp() override {
        root_ = fs::temp_directory_path() / ("telega-tgcmd-" + std::to_string(::getpid()));
        fs::remove_all(root_);
        fs::create_directories(root_ / "data");
        set_env("XDG_CONFIG_HOME", (root_ / "config").string());
        set_env("XDG_DATA_HOME", (root_ / "data").string());
        set_env("XDG_CACHE_HOME", (root_ / "cache").string());
        unset_env("TELEGA_API_ID");
        unset_env("TELEGA_API_HASH");
        keyfile_ = root_ / "keyfile";
        write_file_atomic(keyfile_, crypto::Key::random().span(), 0600);
        global_.config = config_path();
        global_.profile = "main";
        use_transport();
    }
    void TearDown() override {
        set_transport_factory({});
        unset_env("XDG_CONFIG_HOME");
        unset_env("XDG_DATA_HOME");
        unset_env("XDG_CACHE_HOME");
        fs::remove_all(root_);
    }

    [[nodiscard]] fs::path config_path() const { return root_ / "config.toml"; }

    // Фабрика отдаёт один и тот же фейк всем сессиям теста.
    void use_transport() {
        transport_ = std::make_shared<FakeTransport>();
        account_ = std::make_unique<FakeAccount>(options_);
        account_->attach(*transport_);
        set_transport_factory([this](const fs::path& log) {
            log_files_.push_back(log);
            return std::static_pointer_cast<td::TdTransport>(transport_);
        });
    }

    void write_config(const std::string& profile_body, bool with_keyfile = true) {
        std::string text =
            "api_id = 12345\napi_hash = \"0123456789abcdef0123456789abcdef\"\n\n"
            "[profiles.main]\n" +
            profile_body;
        if (with_keyfile) {
            text += "keyfile = \"" + keyfile_.string() + "\"\n";
        }
        write_file_atomic(config_path(), as_bytes(text), 0600);
    }

    [[nodiscard]] fs::path data_dir() const { return root_ / "data" / "telega-fs" / "main"; }
    [[nodiscard]] fs::path cache_dir() const { return root_ / "cache" / "telega-fs" / "main"; }

    int login(bool bot = false, bool qr = false, bool test_dc = false) {
        LoginArgs a;
        a.global = global_;
        a.bot = bot;
        a.qr = qr;
        a.test_dc = test_dc;
        return run_login(a);
    }

    FakeAccount::Options options_;
    GlobalArgs global_;
    fs::path root_;
    fs::path keyfile_;
    std::shared_ptr<FakeTransport> transport_;
    std::unique_ptr<FakeAccount> account_;
    std::vector<fs::path> log_files_;
};

TEST_F(TelegramCmds, BotLoginCreatesBareKeyringAndPassesTdlibParameters) {
    write_config("auth = \"bot\"\n");
    const StdinScript in{"123:good\n"};
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    EXPECT_EQ(login(/*bot=*/true), 0);
    const std::string out = testing::internal::GetCapturedStdout();
    const std::string err = testing::internal::GetCapturedStderr();

    // Keyring без ФС: 0600, настоящий wrapped_tdlib; предупреждение о потере данных.
    const fs::path keyring_path = data_dir() / "keyring";
    ASSERT_TRUE(fs::exists(keyring_path));
    EXPECT_EQ(fs::status(keyring_path).permissions() & fs::perms::all,
              fs::perms::owner_read | fs::perms::owner_write);
    const crypto::KeyringFile keyring = crypto::load_keyring(keyring_path);
    EXPECT_FALSE(crypto::keyring_has_fs(keyring));
    EXPECT_EQ(keyring.kdf.alg, crypto::KdfAlg::keyfile);
    EXPECT_NE(out.find("lost for good"), std::string::npos);
    EXPECT_EQ(fs::status(data_dir()).permissions() & fs::perms::all, fs::perms::owner_all);

    // setTdlibParameters.
    const Json p = account_->last("setTdlibParameters");
    EXPECT_EQ(p["database_directory"], (data_dir() / "tdlib").string());
    EXPECT_EQ(p["files_directory"], (cache_dir() / "tdfiles").string());
    EXPECT_EQ(p["use_test_dc"], false);
    EXPECT_EQ(p["use_message_database"], false);
    EXPECT_EQ(p["use_secret_chats"], false);
    EXPECT_EQ(p["device_model"], "telega-fs");
    EXPECT_EQ(p["api_id"], 12345);
    EXPECT_FALSE(p["application_version"].get<std::string>().empty());
    const auto key = crypto::unlock_tdlib_key(keyring, crypto::read_keyfile(keyfile_));
    if (!key.has_value()) {
        FAIL() << "keyring does not unlock";
    }
    EXPECT_EQ(p["database_encryption_key"],
              base64_of(std::string(reinterpret_cast<const char*>(key->data()), 32)));
    EXPECT_EQ(account_->last("checkAuthenticationBotToken")["token"], "123:good");
    ASSERT_EQ(log_files_.size(), 1U);
    EXPECT_EQ(log_files_[0], data_dir() / "td.log");  // лог TDLib в data-каталоге

    // Секреты не напечатаны: ни токен, ни api_hash, ни телефон, ни ключ.
    for (const std::string& text : {out, err}) {
        EXPECT_EQ(text.find("123:good"), std::string::npos);
        EXPECT_EQ(text.find("0123456789abcdef"), std::string::npos);
        EXPECT_EQ(text.find("15550001"), std::string::npos);
    }
    EXPECT_NE(out.find("bot @somebody (id 4242)"), std::string::npos);
    // Клиент закрыт до выхода (close раньше остановки рантайма).
    EXPECT_EQ(account_->types().back(), "close");
}

TEST_F(TelegramCmds, BotLoginAsksForTokenAgainOnRejection) {
    write_config("auth = \"bot\"\n");
    const StdinScript in{"999:bad\n123:good\n"};
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    EXPECT_EQ(login(true), 0);
    (void)testing::internal::GetCapturedStdout();
    const std::string err = testing::internal::GetCapturedStderr();
    EXPECT_NE(err.find("ACCESS_TOKEN_INVALID"), std::string::npos);
    EXPECT_EQ(err.find("999:bad"), std::string::npos);  // сам токен не печатается
    EXPECT_TRUE(account_->authorized());
}

TEST_F(TelegramCmds, UserLoginWithResendWrongCodeAndPassword) {
    options_.bot = false;
    options_.need_password = true;
    use_transport();
    write_config("auth = \"user\"\n");
    // телефон, пустая строка (отправить код заново), неверный код, верный код, пароль 2FA
    const StdinScript in{"+15550001\n\n99999\n12345\nhunter2\n"};
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    EXPECT_EQ(login(), 0);
    const std::string out = testing::internal::GetCapturedStdout();
    const std::string err = testing::internal::GetCapturedStderr();
    const auto types = account_->types();
    const auto count = [&](const char* t) { return std::count(types.begin(), types.end(), t); };
    EXPECT_EQ(count("setAuthenticationPhoneNumber"), 1);
    EXPECT_EQ(count("resendAuthenticationCode"), 1);
    EXPECT_EQ(count("checkAuthenticationCode"), 2);
    EXPECT_EQ(count("checkAuthenticationPassword"), 1);
    EXPECT_NE(err.find("sent via Sms"), std::string::npos);  // куда ушёл код
    EXPECT_NE(err.find("next method: Call"), std::string::npos);
    EXPECT_NE(err.find("PHONE_CODE_INVALID"), std::string::npos);
    EXPECT_NE(err.find("Password hint: pet"), std::string::npos);
    EXPECT_NE(out.find("user @somebody (id 4242)"), std::string::npos);
    for (const std::string& text : {out, err}) {
        EXPECT_EQ(text.find("hunter2"), std::string::npos);
        EXPECT_EQ(text.find("+15550001"), std::string::npos);
        EXPECT_EQ(text.find("12345\n"), std::string::npos);
    }
}

TEST_F(TelegramCmds, QrLoginRequestsQrAuthentication) {
    options_.bot = false;
    use_transport();
    write_config("auth = \"user\"\n");
    // Сервер не умеет QR: проверяем, что запрос именно QR, а не телефон.
    const StdinScript in{""};
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    EXPECT_EQ(login(false, /*qr=*/true), 1);  // фейк отвечает на неизвестный запрос ошибкой
    (void)testing::internal::GetCapturedStdout();
    (void)testing::internal::GetCapturedStderr();
    const auto types = account_->types();
    EXPECT_NE(std::find(types.begin(), types.end(), "requestQrCodeAuthentication"), types.end());
    EXPECT_EQ(std::find(types.begin(), types.end(), "setAuthenticationPhoneNumber"), types.end());
}

TEST_F(TelegramCmds, BotAndQrTogetherAreRefused) {
    write_config("auth = \"bot\"\n");
    testing::internal::CaptureStderr();
    EXPECT_EQ(login(true, true), 1);
    EXPECT_NE(testing::internal::GetCapturedStderr().find("cannot be combined"), std::string::npos);
    EXPECT_TRUE(account_->types().empty());
}

TEST_F(TelegramCmds, TestDcFlagIsSavedInProfileAndPassedToTdlib) {
    write_config("auth = \"bot\"\n# my comment\n");
    const StdinScript in{"123:good\n"};
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    EXPECT_EQ(login(true, false, /*test_dc=*/true), 0);
    (void)testing::internal::GetCapturedStdout();
    (void)testing::internal::GetCapturedStderr();
    EXPECT_EQ(account_->last("setTdlibParameters")["use_test_dc"], true);
    EXPECT_TRUE(load_config(config_path()).profile("main").use_test_dc);
    // Остальное содержимое файла сохранено.
    const Bytes raw = read_file(config_path());
    const std::string text(reinterpret_cast<const char*>(raw.data()), raw.size());
    EXPECT_NE(text.find("# my comment"), std::string::npos);
    EXPECT_NE(text.find("api_hash"), std::string::npos);
}

TEST_F(TelegramCmds, PassphraseIsAskedTwiceAndMismatchCreatesNothing) {
    write_config("auth = \"bot\"\n", /*with_keyfile=*/false);
    {
        const StdinScript in{"first\nsecond\n"};
        testing::internal::CaptureStderr();
        EXPECT_EQ(login(true), 1);
        EXPECT_NE(testing::internal::GetCapturedStderr().find("do not match"), std::string::npos);
        EXPECT_FALSE(fs::exists(data_dir() / "keyring"));
        EXPECT_TRUE(account_->types().empty());  // до TDLib дело не дошло
    }
    {
        const StdinScript in{"open sesame\nopen sesame\n123:good\n"};
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        EXPECT_EQ(login(true), 0);
        (void)testing::internal::GetCapturedStdout();
        (void)testing::internal::GetCapturedStderr();
    }
    const crypto::KeyringFile keyring = crypto::load_keyring(data_dir() / "keyring");
    EXPECT_EQ(keyring.kdf.alg, crypto::KdfAlg::argon2id13);
    EXPECT_FALSE(crypto::keyring_has_fs(keyring));
    const crypto::Key kek = crypto::derive_kek("open sesame", keyring.kdf);
    EXPECT_TRUE(crypto::unlock_tdlib_key(keyring, kek).has_value());
    EXPECT_FALSE(
        crypto::unlock_tdlib_key(keyring, crypto::derive_kek("nope", keyring.kdf)).has_value());
}

TEST_F(TelegramCmds, SecondLoginKeepsKeyringAndSessionAndRefusesWrongPassword) {
    write_config("auth = \"bot\"\n", /*with_keyfile=*/false);
    {
        const StdinScript in{"pw\npw\n123:good\n"};
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        ASSERT_EQ(login(true), 0);
        (void)testing::internal::GetCapturedStdout();
        (void)testing::internal::GetCapturedStderr();
    }
    const Bytes before = read_file(data_dir() / "keyring");
    {
        const StdinScript in{"wrong\n"};
        testing::internal::CaptureStderr();
        EXPECT_EQ(login(true), 1);
        EXPECT_NE(testing::internal::GetCapturedStderr().find("wrong passphrase"),
                  std::string::npos);
    }
    {
        const StdinScript in{"pw\n"};  // токен не нужен: сессия уже авторизована
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        EXPECT_EQ(login(true), 0);
        EXPECT_NE(testing::internal::GetCapturedStdout().find("Logged in as bot"),
                  std::string::npos);
        (void)testing::internal::GetCapturedStderr();
    }
    EXPECT_EQ(read_file(data_dir() / "keyring"), before);
}

TEST_F(TelegramCmds, LoginIsNotForLocalProfiles) {
    write_config("store = \"local\"\nlocal_dir = \"" + (root_ / "chan").string() + "\"\n");
    testing::internal::CaptureStderr();
    EXPECT_EQ(login(true), 1);
    EXPECT_NE(testing::internal::GetCapturedStderr().find("store = \"local\""), std::string::npos);
    EXPECT_FALSE(fs::exists(data_dir() / "keyring"));
}

TEST_F(TelegramCmds, LoginNeedsApiCredentials) {
    write_file_atomic(config_path(), as_bytes(std::string("[profiles.main]\nauth = \"bot\"\n")),
                      0600);
    testing::internal::CaptureStderr();
    EXPECT_EQ(login(true), 1);
    EXPECT_NE(testing::internal::GetCapturedStderr().find("api_id"), std::string::npos);
}

TEST_F(TelegramCmds, LogoutLogsOutRemovesSessionAndKeepsKeyring) {
    write_config("auth = \"bot\"\n");
    {
        const StdinScript in{"123:good\n"};
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        ASSERT_EQ(login(true), 0);
        (void)testing::internal::GetCapturedStdout();
        (void)testing::internal::GetCapturedStderr();
    }
    ASSERT_TRUE(fs::exists(data_dir() / "tdlib"));
    fs::create_directories(cache_dir() / "tdfiles");
    const Bytes keyring = read_file(data_dir() / "keyring");

    LogoutArgs a;
    a.global = global_;
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    EXPECT_EQ(run_logout(a), 0);
    const std::string out = testing::internal::GetCapturedStdout();
    (void)testing::internal::GetCapturedStderr();
    const auto types = account_->types();
    EXPECT_NE(std::find(types.begin(), types.end(), "logOut"), types.end());
    EXPECT_FALSE(account_->authorized());
    EXPECT_FALSE(fs::exists(data_dir() / "tdlib"));
    EXPECT_FALSE(fs::exists(cache_dir() / "tdfiles"));
    EXPECT_EQ(read_file(data_dir() / "keyring"), keyring);  // keyring не тронут
    EXPECT_NE(out.find("Logged out"), std::string::npos);

    // Повторный logout — «сессии нет», без ошибки.
    testing::internal::CaptureStdout();
    EXPECT_EQ(run_logout(a), 0);
    EXPECT_NE(testing::internal::GetCapturedStdout().find("no Telegram session"),
              std::string::npos);
}

TEST_F(TelegramCmds, LogoutWithWrongPasswordKeepsTheSession) {
    write_config("auth = \"bot\"\n", /*with_keyfile=*/false);
    {
        const StdinScript in{"pw\npw\n123:good\n"};
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        ASSERT_EQ(login(true), 0);
        (void)testing::internal::GetCapturedStdout();
        (void)testing::internal::GetCapturedStderr();
    }
    LogoutArgs a;
    a.global = global_;
    const StdinScript in{"nope\n"};
    testing::internal::CaptureStderr();
    EXPECT_EQ(run_logout(a), 1);
    (void)testing::internal::GetCapturedStderr();
    EXPECT_TRUE(fs::exists(data_dir() / "tdlib"));
    EXPECT_TRUE(account_->authorized());
}

// --- сессия без интерактива (mount, init) ---

TEST_F(TelegramCmds, BackendOfAnUnauthorizedSessionPointsToLogin) {
    write_config("auth = \"bot\"\n");
    const Loaded l = load_profile(global_);
    ensure_profile_dirs(l.paths);
    TelegramBackend backend{l, crypto::Key::random()};
    try {
        backend.connect();
        FAIL() << "connect must fail";
    } catch (const CommandError& e) {
        EXPECT_NE(std::string(e.what()).find("telega-fs login"), std::string::npos);
    }
}

TEST_F(TelegramCmds, BackendOfAnAuthorizedSessionConnectsWithoutPrompts) {
    write_config("auth = \"bot\"\n");
    account_->set_authorized(true);
    const Loaded l = load_profile(global_);
    ensure_profile_dirs(l.paths);
    const StdinScript in{""};  // читать нечего: любой вопрос был бы ошибкой
    TelegramBackend backend{l, crypto::Key::random()};
    EXPECT_NO_THROW(backend.connect());
    EXPECT_EQ(describe_account(backend.session().me()), "bot @somebody (id 4242)");
}

// --ro ничего не меняет локально, в том числе учёт кэша чтения не пишется в state.sqlite:
// TelegramStore в --ro получает tdfiles_dir (учёт в памяти), но не state_db.
TEST_F(TelegramCmds, ReadOnlyMountDoesNotGiveTheReadCacheAStateDatabase) {
    write_config("auth = \"bot\"\n");
    const Loaded l = load_profile(global_);
    const store::TelegramStoreOptions rw = mount_store_options(l, 1, /*read_only=*/false);
    EXPECT_EQ(rw.state_db, l.paths.state_db());
    EXPECT_EQ(rw.tdfiles_dir, l.paths.tdfiles());
    const store::TelegramStoreOptions ro = mount_store_options(l, 1, /*read_only=*/true);
    EXPECT_TRUE(ro.state_db.empty());
    EXPECT_EQ(ro.tdfiles_dir, l.paths.tdfiles());  // кэш и его вытеснение работают, но в памяти
    EXPECT_EQ(ro.cache_limit, rw.cache_limit);
}

TEST_F(TelegramCmds, MountWithoutKeyringPointsToLogin) {
    write_config("auth = \"bot\"\nchat_id = -1001\n");
    fs::create_directories(root_ / "mnt");
    MountArgs a;
    a.global = global_;
    a.mountpoint = root_ / "mnt";
    a.foreground = true;
    testing::internal::CaptureStderr();
    EXPECT_EQ(run_mount(a), 1);
    const std::string err = testing::internal::GetCapturedStderr();
    EXPECT_NE(err.find("telega-fs login"), std::string::npos);
    EXPECT_TRUE(account_->types().empty());
}

TEST_F(TelegramCmds, MountOfAnUnauthorizedSessionExitsWithLoginHintWithoutPrompts) {
    write_config("auth = \"bot\"\nchat_id = -1001\n");
    {
        // keyring без ФС, но сессия не авторизована.
        const StdinScript in{"123:good\n"};
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        ASSERT_EQ(login(true), 0);
        (void)testing::internal::GetCapturedStdout();
        (void)testing::internal::GetCapturedStderr();
    }
    account_->set_authorized(false);
    fs::create_directories(root_ / "mnt");
    MountArgs a;
    a.global = global_;
    a.mountpoint = root_ / "mnt";
    a.foreground = true;
    const StdinScript in{""};  // keyfile профиля: пароль не спрашивается, больше вводить нечего
    testing::internal::CaptureStderr();
    EXPECT_EQ(run_mount(a), 1);
    EXPECT_NE(testing::internal::GetCapturedStderr().find("telega-fs login"), std::string::npos);
}

TEST_F(TelegramCmds, MountNeedsChatId) {
    write_config("auth = \"bot\"\n");
    {
        const StdinScript in{"123:good\n"};
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        ASSERT_EQ(login(true), 0);
        (void)testing::internal::GetCapturedStdout();
        (void)testing::internal::GetCapturedStderr();
    }
    fs::create_directories(root_ / "mnt");
    MountArgs a;
    a.global = global_;
    a.mountpoint = root_ / "mnt";
    a.foreground = true;
    testing::internal::CaptureStderr();
    EXPECT_EQ(run_mount(a), 1);
    EXPECT_NE(testing::internal::GetCapturedStderr().find("chat_id"), std::string::npos);
}

// --- init: проверки до обращения к каналу ---

class TelegramInit : public TelegramCmds {
  protected:
    int init(std::optional<int64_t> chat, std::optional<std::string> title) {
        InitArgs a;
        a.global = global_;
        a.chat = chat;
        a.title = std::move(title);
        return run_init(a);
    }
    void login_bot() {
        const StdinScript in{"123:good\n"};
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        ASSERT_EQ(login(true), 0);
        (void)testing::internal::GetCapturedStdout();
        (void)testing::internal::GetCapturedStderr();
    }
    std::string run_init_capturing(std::optional<int64_t> chat, std::optional<std::string> title,
                                   int expect_code) {
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        EXPECT_EQ(init(chat, std::move(title)), expect_code);
        (void)testing::internal::GetCapturedStdout();
        return testing::internal::GetCapturedStderr();
    }
};

TEST_F(TelegramInit, NeedsExactlyOneOfChatAndTitle) {
    write_config("auth = \"bot\"\n");
    login_bot();
    EXPECT_NE(run_init_capturing({}, {}, 1).find("--chat ID"), std::string::npos);
    EXPECT_NE(run_init_capturing(-100, "T", 1).find("--chat ID"), std::string::npos);
}

TEST_F(TelegramInit, RequiresLoginFirst) {
    write_config("auth = \"bot\"\n");
    EXPECT_NE(run_init_capturing(-100, {}, 1).find("telega-fs login"), std::string::npos);
}

TEST_F(TelegramInit, BotCannotCreateChannels) {
    write_config("auth = \"bot\"\n");
    login_bot();
    const std::string err = run_init_capturing({}, "my channel", 1);
    EXPECT_NE(err.find("bot cannot create channels"), std::string::npos);
    const auto types = account_->types();
    EXPECT_EQ(std::find(types.begin(), types.end(), "createNewSupergroupChat"), types.end());
    EXPECT_FALSE(crypto::keyring_has_fs(crypto::load_keyring(data_dir() / "keyring")));
}

TEST_F(TelegramInit, InitWithoutSessionPointsToLogin) {
    write_config("auth = \"bot\"\n");
    login_bot();
    account_->set_authorized(false);
    const std::string err = run_init_capturing(-100, {}, 1);
    EXPECT_NE(err.find("telega-fs login"), std::string::npos);
}

TEST_F(TelegramInit, LocalProfileRejectsChatAndTitle) {
    write_config("store = \"local\"\nlocal_dir = \"" + (root_ / "chan").string() + "\"\n");
    EXPECT_NE(run_init_capturing(-100, {}, 1).find("store = \"telegram\""), std::string::npos);
    EXPECT_NE(run_init_capturing({}, "T", 1).find("store = \"telegram\""), std::string::npos);
}

// Keyring с ФС, как после init, который упал между записью keyring и pin.
void attach_fake_fs(const fs::path& data_dir, const fs::path& keyfile) {
    const crypto::KeyringFile k = crypto::load_keyring(data_dir / "keyring");
    const crypto::Key kek = crypto::read_keyfile(keyfile);
    crypto::save_keyring(data_dir / "keyring", crypto::attach_fs(k, crypto::random_fs_uuid(), kek,
                                                                 crypto::Key::random()));
}

TEST_F(TelegramInit, CommittedFileSystemIsRefusedBeforeTelegram) {
    write_config("auth = \"bot\"\n");
    login_bot();
    attach_fake_fs(data_dir(), keyfile_);
    // ФС уже закоммичена: init отказывает до обращения к Telegram.
    meta::StateStore::create(data_dir() / "state.sqlite");
    {
        meta::StateStore state{data_dir() / "state.sqlite"};
        state.set_int(meta::state_key::base_generation, 1);
    }
    const size_t before = account_->types().size();
    EXPECT_NE(run_init_capturing(-100, {}, 1).find("already has a file system"), std::string::npos);
    EXPECT_EQ(account_->types().size(), before);
}

// Состояние профиля проверяется под его блокировкой: пока профиль занят другим процессом, init
// отказывает из-за блокировки, не заглядывая в базы (иначе проверка и init гонялись бы с
// параллельным mount).
TEST_F(TelegramInit, ProfileIsInspectedOnlyUnderItsLock) {
    write_config("auth = \"bot\"\n");
    login_bot();
    attach_fake_fs(data_dir(), keyfile_);
    meta::StateStore::create(data_dir() / "state.sqlite");
    {
        meta::StateStore state{data_dir() / "state.sqlite"};
        state.set_int(meta::state_key::base_generation, 1);
    }
    const ProfileLock held{data_dir() / "lock"};
    const std::string err = run_init_capturing(-100, {}, 1);
    EXPECT_NE(err.find("in use by another"), std::string::npos) << err;
    EXPECT_EQ(err.find("already has a file system"), std::string::npos) << err;
}

// Прерванный init (keyring с ФС, базы нет или base_generation = 0) не блокирует профиль: init
// идёт дальше, к Telegram, а решает судьба канала.
TEST_F(TelegramInit, InterruptedInitIsNotRefusedUpFront) {
    write_config("auth = \"bot\"\n");
    login_bot();
    attach_fake_fs(data_dir(), keyfile_);
    const size_t before = account_->types().size();
    const std::string err = run_init_capturing(-100, {}, 1);  // фейк не умеет getChat
    EXPECT_EQ(err.find("already has a file system"), std::string::npos) << err;
    const auto types = account_->types();
    EXPECT_NE(std::find(types.begin() + static_cast<std::ptrdiff_t>(before), types.end(), "getMe"),
              types.end());

    // То же с базой, которую успели создать, но не закоммитить.
    meta::StateStore::create(data_dir() / "state.sqlite");
    {
        meta::StateStore state{data_dir() / "state.sqlite"};
        state.set_int(meta::state_key::base_generation, 0);
    }
    const size_t before2 = account_->types().size();
    const std::string err2 = run_init_capturing(-100, {}, 1);
    EXPECT_EQ(err2.find("already has a file system"), std::string::npos) << err2;
    EXPECT_GT(account_->types().size(), before2);
}

// --- подсказчик ---

TEST(TerminalPrompterTest, AsksWithTheRightPromptsAndHidesSecrets) {
    std::vector<std::pair<std::string, bool>> asked;
    std::ostringstream out;
    TerminalPrompter::Options o;
    o.out = &out;
    o.ask = [&](const std::string& prompt, bool secret) {
        asked.emplace_back(prompt, secret);
        return std::string{"x"};
    };
    TerminalPrompter p{std::move(o)};
    (void)p.phone_number();
    (void)p.code(Json{{"type", {{"@type", "authenticationCodeTypeSms"}}}});
    (void)p.password("hint");
    (void)p.bot_token();
    ASSERT_EQ(asked.size(), 4U);
    EXPECT_FALSE(asked[0].second);  // телефон виден
    EXPECT_FALSE(asked[1].second);  // код виден
    EXPECT_TRUE(asked[2].second);   // пароль 2FA без эха
    EXPECT_TRUE(asked[3].second);   // токен бота без эха
    EXPECT_NE(asked[1].first.find("empty line"), std::string::npos);  // пустая строка = повтор
    EXPECT_NE(out.str().find("sent via Sms"), std::string::npos);
    EXPECT_NE(out.str().find("hint"), std::string::npos);
}

TEST(TerminalPrompterTest, CodeDeliveryDescription) {
    const Json info{{"type", {{"@type", "authenticationCodeTypeTelegramMessage"}}},
                    {"next_type", {{"@type", "authenticationCodeTypeSms"}}},
                    {"timeout", 30}};
    const std::string text = describe_code_delivery(info);
    EXPECT_NE(text.find("TelegramMessage"), std::string::npos);
    EXPECT_NE(text.find("next method: Sms"), std::string::npos);
    EXPECT_NE(text.find("30 s"), std::string::npos);
    EXPECT_EQ(describe_code_delivery(Json::object()), "the code was sent via -");
}

TEST(TerminalPrompterTest, FloodWaitIsWaitedOutUpToTheLimit) {
    std::ostringstream out;
    std::vector<std::chrono::seconds> slept;
    TerminalPrompter::Options o;
    o.out = &out;
    o.max_flood_wait = std::chrono::seconds{600};
    o.sleep = [&](std::chrono::seconds s) { slept.push_back(s); };
    TerminalPrompter p{std::move(o)};
    EXPECT_TRUE(p.retry_after_error(td::AuthStep::kPhoneNumber,
                                    td::TdError::make(429, "Too Many Requests: retry after 20")));
    ASSERT_EQ(slept.size(), 1U);
    EXPECT_GE(slept[0], std::chrono::seconds{20});
    EXPECT_LE(slept[0], std::chrono::seconds{30});
    EXPECT_FALSE(p.retry_after_error(
        td::AuthStep::kPhoneNumber, td::TdError::make(429, "Too Many Requests: retry after 9000")));
    EXPECT_EQ(slept.size(), 1U);
    // Неверный ввод — спросить ещё раз; локальные ошибки и прочее — нет.
    EXPECT_TRUE(
        p.retry_after_error(td::AuthStep::kCode, td::TdError::make(400, "PHONE_CODE_INVALID")));
    EXPECT_FALSE(p.retry_after_error(td::AuthStep::kParameters, td::TdError::make(400, "x")));
    EXPECT_FALSE(p.retry_after_error(td::AuthStep::kCode,
                                     td::TdError::make(td::TdError::kTimeout, "timeout")));
}

TEST(TerminalPrompterTest, QrLinkFallsBackToPrintingIt) {
    std::ostringstream out;
    std::string drawn;
    TerminalPrompter::Options o;
    o.qr = true;
    o.out = &out;
    o.draw = [&](const std::string& link) {
        drawn = link;
        return false;  // python3-qrcode нет
    };
    TerminalPrompter p{std::move(o)};
    EXPECT_TRUE(p.use_qr());
    p.show_qr_link("tg://login?token=abc");
    EXPECT_EQ(drawn, "tg://login?token=abc");
    EXPECT_NE(out.str().find("tg://login?token=abc"), std::string::npos);
    EXPECT_NE(out.str().find("Link Desktop Device"), std::string::npos);
}

}  // namespace
}  // namespace telega::app
