#pragma once

// Сессия TDLib в приложении: сборка setTdlibParameters из профиля, TdRuntime,
// авторизация и закрытие. Библиотека не линкуется с TDLib: настоящий транспорт
// (td_json_client) приходит из main через set_transport_factory, в тестах — фейк.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

#include "app/config.h"
#include "crypto/keys.h"
#include "td/auth.h"
#include "td/td_runtime.h"
#include "td/td_transport.h"

namespace telega::app {

// Создаёт транспорт и направляет лог TDLib в log_file (setLogStream, 16 МиБ, уровень
// 1). Зовётся один раз на процесс, до первого клиента.
using TransportFactory =
    std::function<std::shared_ptr<td::TdTransport>(const std::filesystem::path& log_file)>;
void set_transport_factory(TransportFactory factory);

// Сессия не авторизована, а команда не интерактивная (mount, init, logout): надо запустить login.
class NotLoggedIn : public std::runtime_error {
  public:
    NotLoggedIn() : std::runtime_error("the Telegram session is not logged in") {}
};

struct ApiCredentials {
    int32_t api_id = 0;
    std::string api_hash;  // секрет: в логи не пишется
};
// api_id и api_hash из конфига (или TELEGA_API_ID / TELEGA_API_HASH). ConfigError, если их нет.
ApiCredentials api_credentials(const Config& config);

// Подсказчик без ввода: любой вопрос — NotLoggedIn.
class NoInteractionPrompter final : public td::AuthPrompter {
  public:
    std::string phone_number() override { throw NotLoggedIn(); }
    std::string code(const td::Json& /*code_info*/) override { throw NotLoggedIn(); }
    std::string password(const std::string& /*hint*/) override { throw NotLoggedIn(); }
    td::RegistrationName registration() override { throw NotLoggedIn(); }
    std::string bot_token() override { throw NotLoggedIn(); }
};

// Один клиент TDLib профиля. Деструктор закрывает клиента (close) и только потом останавливает
// TdRuntime, затирает ключ и api_hash.
class TgSession {
  public:
    TgSession(const ProfilePaths& paths, bool use_test_dc, const ApiCredentials& api,
              const crypto::Key& tdlib_db_key);
    TgSession(const TgSession&) = delete;
    TgSession& operator=(const TgSession&) = delete;
    ~TgSession();

    // Довести клиента до authorizationStateReady (setTdlibParameters и дальше по подсказчику).
    // Ошибки — TdException, NotLoggedIn от подсказчика без ввода.
    void authorize(td::AuthPrompter& prompter, td::AuthMode mode,
                   std::chrono::milliseconds timeout = std::chrono::minutes{10});
    // То же без ввода: NotLoggedIn, если сессия не авторизована.
    void authorize_existing(std::chrono::milliseconds timeout = std::chrono::seconds{60});

    [[nodiscard]] td::TdClient& client() { return *client_; }
    // getMe: id, тип и username, без телефона.
    td::Json me();
    // logOut и ожидание закрытия. false — не успело (сети нет).
    bool log_out(std::chrono::milliseconds timeout = std::chrono::seconds{60});
    // Закрыть клиента (до остановки рантайма) и остановить рантайм. Идемпотентно, не бросает.
    void close() noexcept;

  private:
    td::TdlibParameters params_;
    std::unique_ptr<td::TdRuntime> runtime_;
    std::shared_ptr<td::TdClient> client_;
    bool closed_ = false;
};

}  // namespace telega::app
