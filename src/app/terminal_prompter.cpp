#include "app/terminal_prompter.h"

#include <cstdio>
#include <iostream>
#include <thread>
#include <utility>

#include "app/cli_util.h"

namespace telega::app {

namespace {

std::string type_name(const td::Json& info, const char* key) {
    if (info.contains(key) && info[key].is_object()) {
        std::string t = info[key].value("@type", std::string{"?"});
        constexpr std::string_view kPrefix = "authenticationCodeType";
        if (t.starts_with(kPrefix)) {
            t = t.substr(kPrefix.size());
        }
        return t;
    }
    return "-";
}

std::string terminal_ask(const std::string& prompt, bool secret) {
    if (secret) {
        Secret s;
        read_secret(prompt.c_str(), s);
        return s.value();
    }
    return read_line(prompt);
}

// QR рисует python3-qrcode; ссылка уходит через stdin, а не через argv (в ps не видна).
bool draw_qr_with_python(const std::string& link) {
    FILE* py = ::popen(
        "python3 -I -c 'import sys,qrcode; q=qrcode.QRCode(border=2); "
        "q.add_data(sys.stdin.read().strip()); q.print_ascii(out=sys.stderr, invert=True)'",
        "w");
    if (py == nullptr) {
        return false;
    }
    std::fputs(link.c_str(), py);
    return ::pclose(py) == 0;
}

}  // namespace

std::string describe_code_delivery(const td::Json& code_info) {
    std::string s = "the code was sent via " + type_name(code_info, "type");
    const std::string next = type_name(code_info, "next_type");
    if (next != "-") {
        s += ", next method: " + next;
    }
    if (const int timeout = code_info.value("timeout", 0); timeout > 0) {
        s += ", can be resent in " + std::to_string(timeout) + " s";
    }
    return s;
}

TerminalPrompter::TerminalPrompter(Options options) : options_(std::move(options)) {}

std::ostream& TerminalPrompter::out() const {
    return options_.out != nullptr ? *options_.out : std::cerr;
}

std::string TerminalPrompter::ask(const std::string& prompt, bool secret) const {
    return options_.ask ? options_.ask(prompt, secret) : terminal_ask(prompt, secret);
}

std::string TerminalPrompter::phone_number() {
    return ask("Phone number (international format, e.g. +15551234567): ", false);
}

std::string TerminalPrompter::code(const td::Json& code_info) {
    out() << describe_code_delivery(code_info) << '\n';
    return ask("Login code (empty line = send the code again): ", false);
}

std::string TerminalPrompter::password(const std::string& hint) {
    if (!hint.empty()) {
        out() << "Password hint: " << hint << '\n';
    }
    return ask("Two-step verification password: ", true);
}

td::RegistrationName TerminalPrompter::registration() {
    out() << "This phone number is not registered in Telegram; creating a new account.\n";
    td::RegistrationName name;
    name.first_name = ask("First name: ", false);
    name.last_name = ask("Last name (optional): ", false);
    return name;
}

std::string TerminalPrompter::bot_token() { return ask("Bot token (from @BotFather): ", true); }

void TerminalPrompter::show_qr_link(const std::string& link) {
    out() << "\nScan with the Telegram app on your phone: Settings > Devices > Link Desktop "
             "Device\n";
    const bool drawn = options_.draw ? options_.draw(link) : draw_qr_with_python(link);
    if (!drawn) {
        out() << "Cannot draw the QR code (install python3-qrcode). Open this link in the "
                 "Telegram app instead: "
              << link << '\n';
    }
}

bool TerminalPrompter::retry_after_error(td::AuthStep step, const td::TdError& error) {
    if (error.is_local()) {
        return false;
    }
    if (error.retry_after) {
        // FLOOD_WAIT на входе: выждать с запасом и повторить тот же шаг.
        if (*error.retry_after > options_.max_flood_wait) {
            out() << "Telegram asks to wait " << error.retry_after->count()
                  << " s, which is too long; try again later.\n";
            return false;
        }
        const auto wait = *error.retry_after + *error.retry_after / 10 + std::chrono::seconds{2};
        out() << "Telegram rate limit at the login step: waiting " << wait.count() << " s...\n";
        if (options_.sleep) {
            options_.sleep(wait);
        } else {
            std::this_thread::sleep_for(wait);
        }
        return true;
    }
    if (error.code == 400 && (step == td::AuthStep::kPhoneNumber || step == td::AuthStep::kCode ||
                              step == td::AuthStep::kPassword || step == td::AuthStep::kBotToken)) {
        // Неверный ввод: спросить ещё раз. Текст ошибки TDLib секретов не содержит.
        out() << "Telegram rejected the input: " << error.message << "\n";
        return true;
    }
    return false;
}

}  // namespace telega::app
