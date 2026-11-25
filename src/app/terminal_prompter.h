#pragma once

// Интерактивный подсказчик входа для `telega-fs login`: телефон, код с
// выводом способа доставки и повторной отправкой по пустой строке, пароль 2FA без эха, имя при
// регистрации, токен бота без эха, QR-код.

#include <functional>
#include <iosfwd>
#include <string>

#include "td/auth.h"

namespace telega::app {

class TerminalPrompter final : public td::AuthPrompter {
  public:
    // Ввод одной строки; secret = true — без эха. По умолчанию терминал и стандартный ввод.
    using Ask = std::function<std::string(const std::string& prompt, bool secret)>;
    // Рисует QR-код ссылки. false — не получилось (тогда выводится сама ссылка).
    using DrawQr = std::function<bool(const std::string& link)>;
    // Ожидание FLOOD_WAIT на входе; в тестах подменяется.
    using Sleep = std::function<void(std::chrono::seconds)>;

    struct Options {
        bool qr = false;
        Ask ask;                      // пусто — терминал
        DrawQr draw;                  // пусто — python3 -m qrcode по stdin
        Sleep sleep;                  // пусто — std::this_thread::sleep_for
        std::ostream* out = nullptr;  // сообщения; nullptr — std::cerr
        // FLOOD_WAIT дольше этого не ждём (как у tgfs-bench: 30 минут).
        std::chrono::seconds max_flood_wait{30 * 60};
    };

    explicit TerminalPrompter(Options options);

    std::string phone_number() override;
    std::string code(const td::Json& code_info) override;
    std::string password(const std::string& hint) override;
    td::RegistrationName registration() override;
    std::string bot_token() override;
    bool retry_after_error(td::AuthStep step, const td::TdError& error) override;
    bool use_qr() override { return options_.qr; }
    void show_qr_link(const std::string& link) override;

  private:
    [[nodiscard]] std::string ask(const std::string& prompt, bool secret) const;
    [[nodiscard]] std::ostream& out() const;

    Options options_;
};

// Описание способа доставки кода по authenticationCodeInfo: «code sent via ..., next: ...».
std::string describe_code_delivery(const td::Json& code_info);

}  // namespace telega::app
