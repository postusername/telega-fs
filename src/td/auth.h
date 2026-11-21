#pragma once

// Авторизация (SPEC 10.1, 10.3): машина состояний по updateAuthorizationState. Что спросить у
// человека (или ответить самому), решает AuthPrompter: `telega-fs login` спрашивает пользователя,
// `tgfs-bench` отвечает по правилам тестового DC (SPEC 17.2). Параметры setTdlibParameters
// приходят извне.

#include <chrono>
#include <cstdint>
#include <string>

#include "td/td_client.h"

namespace telega::td {

// Поля setTdlibParameters. Значения по умолчанию — из SPEC 10.1.
struct TdlibParameters {
    bool use_test_dc = false;
    std::string database_directory;
    std::string files_directory;
    // Сырые байты ключа (в JSON уходят в base64). Пустой — БД TDLib не шифруется.
    std::string database_encryption_key;
    bool use_file_database = true;
    bool use_chat_info_database = true;
    bool use_message_database = false;
    bool use_secret_chats = false;
    int32_t api_id = 0;
    std::string api_hash;  // секрет: в логи не попадает
    std::string system_language_code = "en";
    std::string device_model = "telega-fs";
    std::string system_version;
    std::string application_version;

    // Запрос setTdlibParameters.
    [[nodiscard]] Json to_request() const;
};

enum class AuthMode : uint8_t { kUser, kBot };

struct RegistrationName {
    std::string first_name;
    std::string last_name;
};

// Шаги, на которых можно получить ошибку (для retry_after_error).
enum class AuthStep : uint8_t {
    kParameters,
    kPhoneNumber,
    kBotToken,
    kCode,
    kPassword,
    kRegistration
};

class AuthPrompter {
  public:
    AuthPrompter() = default;
    AuthPrompter(const AuthPrompter&) = delete;
    AuthPrompter& operator=(const AuthPrompter&) = delete;
    virtual ~AuthPrompter() = default;

    virtual std::string phone_number() = 0;
    // code_info — объект authenticationCodeInfo (куда ушёл код, длина и так далее).
    virtual std::string code(const Json& code_info) = 0;
    virtual std::string password(const std::string& hint) = 0;
    virtual RegistrationName registration() = 0;
    virtual std::string bot_token() = 0;

    // Шаг закончился ошибкой TDLib (неверный код, FLOOD_WAIT и так далее). Состояние при этом
    // не меняется. true — спросить и отправить ещё раз (не более max_retries раз на состояние),
    // false — authorize() бросит эту ошибку. Здесь можно, например, выждать retry_after.
    virtual bool retry_after_error(AuthStep step, const TdError& error);

    // Вход пользователя по QR-коду (requestQrCodeAuthentication) вместо кода подтверждения.
    virtual bool use_qr() { return false; }
    // Новая ссылка tg://login?token=... для показа QR-кодом; TDLib обновляет её по истечении.
    virtual void show_qr_link(const std::string& /*link*/) {}
};

struct AuthOptions {
    AuthMode mode = AuthMode::kUser;
    // Общее время на всю авторизацию, включая ввод человеком.
    std::chrono::milliseconds timeout = std::chrono::minutes{10};
    // Таймаут отдельного запроса к TDLib.
    std::chrono::milliseconds call_timeout = std::chrono::seconds{60};
    int max_retries = 5;
};

// Доводит клиента до authorizationStateReady. Ошибки — TdException: ошибка TDLib на шаге (если
// подсказчик не просит повтор), kClosed при Closed/Closing/LoggingOut, kUnexpected для
// состояний, которые не поддерживаются (почта, QR с другого устройства, покупка Premium),
// kTimeout при исчерпании options.timeout. Подписка на апдейты ставится внутри, до первого
// запроса.
void authorize(TdClient& client, const TdlibParameters& params, AuthPrompter& prompter,
               const AuthOptions& options = {});

}  // namespace telega::td
