#include "td/auth.h"

#include <algorithm>
#include <cstddef>
#include <utility>

namespace telega::td {

namespace {

std::string base64(const std::string& raw) {
    static constexpr std::string_view kAlphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((raw.size() + 2) / 3 * 4);
    size_t i = 0;
    for (; i + 2 < raw.size(); i += 3) {
        const uint32_t n = (static_cast<uint32_t>(static_cast<uint8_t>(raw[i])) << 16U) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(raw[i + 1])) << 8U) |
                           static_cast<uint32_t>(static_cast<uint8_t>(raw[i + 2]));
        out += kAlphabet[(n >> 18U) & 63U];
        out += kAlphabet[(n >> 12U) & 63U];
        out += kAlphabet[(n >> 6U) & 63U];
        out += kAlphabet[n & 63U];
    }
    if (i + 1 == raw.size()) {
        const uint32_t n = static_cast<uint32_t>(static_cast<uint8_t>(raw[i])) << 16U;
        out += kAlphabet[(n >> 18U) & 63U];
        out += kAlphabet[(n >> 12U) & 63U];
        out += "==";
    } else if (i + 2 == raw.size()) {
        const uint32_t n = (static_cast<uint32_t>(static_cast<uint8_t>(raw[i])) << 16U) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(raw[i + 1])) << 8U);
        out += kAlphabet[(n >> 18U) & 63U];
        out += kAlphabet[(n >> 12U) & 63U];
        out += kAlphabet[(n >> 6U) & 63U];
        out += '=';
    }
    return out;
}

[[noreturn]] void fail(int code, const std::string& message) {
    throw TdException(TdError::make(code, message));
}

class Authorizer {
  public:
    Authorizer(TdClient& client, const TdlibParameters& params, AuthPrompter& prompter,
               const AuthOptions& options)
        : client_(client),
          params_(params),
          prompter_(prompter),
          options_(options),
          deadline_(TdClient::Clock::now() + options.timeout) {}

    void run() {
        // Подписка на апдейты встроена в TdClient (auth_state), она стоит с создания клиента.
        // TDLib шлёт первое состояние только после первого запроса, поэтому, если ничего ещё не
        // пришло, спрашиваем состояние явно.
        if (!client_.auth_state()) {
            const Json state =
                client_.call(Json{{"@type", "getAuthorizationState"}}, call_timeout());
            client_.seed_auth_state(state);
        }
        uint64_t handled = 0;
        for (;;) {
            const AuthState current = client_.wait_auth_state(handled, remaining());
            handled = current.seq;
            const std::string type = current.type();
            if (type == "authorizationStateReady") {
                return;
            }
            handle(type, current.state);
        }
    }

  private:
    [[nodiscard]] std::chrono::milliseconds remaining() const {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline_ - TdClient::Clock::now());
        if (left <= std::chrono::milliseconds{0}) {
            fail(TdError::kTimeout, "authorization timed out");
        }
        return left;
    }
    [[nodiscard]] std::chrono::milliseconds call_timeout() const {
        return std::min(options_.call_timeout, remaining());
    }

    // Один шаг; при ошибке TDLib и согласии подсказчика повторяет с новым вводом.
    template <typename Make>
    void step(AuthStep which, Make&& make_request) {
        for (int attempt = 0;; ++attempt) {
            try {
                client_.call(make_request(), call_timeout());
                return;
            } catch (const TdException& e) {
                if (e.error().is_local() || attempt >= options_.max_retries ||
                    !prompter_.retry_after_error(which, e.error())) {
                    throw;
                }
            }
        }
    }

    void handle(const std::string& type, const Json& state) {
        if (type == "authorizationStateWaitTdlibParameters") {
            step(AuthStep::kParameters, [&] { return params_.to_request(); });
        } else if (type == "authorizationStateWaitPhoneNumber") {
            if (options_.mode == AuthMode::kBot) {
                step(AuthStep::kBotToken, [&] {
                    return Json{{"@type", "checkAuthenticationBotToken"},
                                {"token", prompter_.bot_token()}};
                });
            } else if (prompter_.use_qr() && !qr_requested_) {
                request_qr();
            } else {
                step(AuthStep::kPhoneNumber, [&] {
                    return Json{{"@type", "setAuthenticationPhoneNumber"},
                                {"phone_number", prompter_.phone_number()}};
                });
            }
        } else if (type == "authorizationStateWaitOtherDeviceConfirmation") {
            prompter_.show_qr_link(state.value("link", std::string{}));
        } else if (type == "authorizationStateWaitCode" && prompter_.use_qr() && !qr_requested_) {
            request_qr();  // код уже запрошен раньше; QR допустим и из WaitCode
        } else if (type == "authorizationStateWaitCode") {
            const Json info = state.contains("code_info") ? state["code_info"] : Json::object();
            // Пустой код от подсказчика — просьба отправить код заново (следующим способом).
            std::string code = prompter_.code(info);
            if (code.empty()) {
                step(AuthStep::kCode, [&] {
                    return Json{{"@type", "resendAuthenticationCode"}, {"reason", nullptr}};
                });
                return;
            }
            step(AuthStep::kCode, [&] {
                if (code.empty()) {
                    code = prompter_.code(info);  // повтор после ошибки: новый ввод
                }
                Json r{{"@type", "checkAuthenticationCode"}, {"code", code}};
                code.clear();
                return r;
            });
        } else if (type == "authorizationStateWaitPassword") {
            const std::string hint = state.value("password_hint", std::string{});
            step(AuthStep::kPassword, [&] {
                return Json{{"@type", "checkAuthenticationPassword"},
                            {"password", prompter_.password(hint)}};
            });
        } else if (type == "authorizationStateWaitRegistration") {
            step(AuthStep::kRegistration, [&] {
                const RegistrationName name = prompter_.registration();
                return Json{{"@type", "registerUser"},
                            {"first_name", name.first_name},
                            {"last_name", name.last_name}};
            });
        } else if (type == "authorizationStateClosed" || type == "authorizationStateClosing" ||
                   type == "authorizationStateLoggingOut") {
            fail(TdError::kClosed, "client is closing (" + type + ")");
        } else {
            fail(TdError::kUnexpected, "unsupported authorization state " + type);
        }
    }

    void request_qr() {
        qr_requested_ = true;
        step(AuthStep::kPhoneNumber, [] {
            return Json{{"@type", "requestQrCodeAuthentication"},
                        {"other_user_ids", Json::array()}};
        });
    }

    bool qr_requested_ = false;
    TdClient& client_;
    const TdlibParameters& params_;
    AuthPrompter& prompter_;
    const AuthOptions& options_;
    const TdClient::Clock::time_point deadline_;
};

}  // namespace

Json TdlibParameters::to_request() const {
    Json r{{"@type", "setTdlibParameters"},
           {"use_test_dc", use_test_dc},
           {"database_directory", database_directory},
           {"files_directory", files_directory},
           {"database_encryption_key", base64(database_encryption_key)},
           {"use_file_database", use_file_database},
           {"use_chat_info_database", use_chat_info_database},
           {"use_message_database", use_message_database},
           {"use_secret_chats", use_secret_chats},
           {"api_id", api_id},
           {"api_hash", api_hash},
           {"system_language_code", system_language_code},
           {"device_model", device_model},
           {"system_version", system_version},
           {"application_version", application_version}};
    return r;
}

bool AuthPrompter::retry_after_error(AuthStep /*step*/, const TdError& /*error*/) { return false; }

void authorize(TdClient& client, const TdlibParameters& params, AuthPrompter& prompter,
               const AuthOptions& options) {
    Authorizer(client, params, prompter, options).run();
}

}  // namespace telega::td
