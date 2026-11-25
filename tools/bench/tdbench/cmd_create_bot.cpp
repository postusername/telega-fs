// create-bot: создаёт тестового бота у @BotFather тестового DC от имени тестового пользователя и
// дописывает токен в creds. Токен не печатается: тексты BotFather с токеном в вывод
// не попадают (в подробном режиме выводятся только с замаскированным токеном).

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include "commands.h"

namespace telega::bench {

namespace {

using std::chrono::seconds;

constexpr seconds kStepTimeout{60};
constexpr seconds kMaxFloodWait{30 * 60};
constexpr int kMaxUsernameAttempts = 5;
constexpr int kMaxFloodRestarts = 3;

const std::regex& token_regex() {
    static const std::regex re{R"(\d+:[A-Za-z0-9_-]{30,})"};
    return re;
}

std::string lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

std::string redact(const std::string& text) {
    std::string out = std::regex_replace(text, token_regex(), "<token>");
    if (out.size() > 300) {
        out.resize(300);
        out += "...";
    }
    std::replace(out.begin(), out.end(), '\n', ' ');
    return out;
}

std::string random_hex8() {
    std::random_device rd;
    std::uniform_int_distribution<int> nibble(0, 15);
    std::string s;
    for (int i = 0; i < 8; ++i) {
        s += "0123456789abcdef"[nibble(rd)];
    }
    return s;
}

// Входящие сообщения BotFather. Заполняется обработчиком в потоке приёма: только кладёт текст.
class Inbox {
  public:
    void push(std::string text) {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            queue_.push_back(std::move(text));
        }
        cv_.notify_all();
    }
    void clear() {
        const std::lock_guard<std::mutex> lock(mu_);
        queue_.clear();
    }
    std::optional<std::string> next(seconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        if (!cv_.wait_for(lock, timeout, [&] { return !queue_.empty(); })) {
            return std::nullopt;
        }
        std::string s = std::move(queue_.front());
        queue_.pop_front();
        return s;
    }

  private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::string> queue_;
};

std::optional<seconds> botfather_flood(const std::string& text) {
    static const std::regex re{R"(try again in (\d+) second)", std::regex::icase};
    std::smatch m;
    if (std::regex_search(text, m, re)) {
        return seconds{std::stol(m[1].str())};
    }
    return std::nullopt;
}

bool username_rejected(const std::string& text) {
    const std::string t = lower(text);
    constexpr std::array<std::string_view, 4> kMarkers{"sorry", "taken", "invalid",
                                                       "try something different"};
    return std::ranges::any_of(
        kMarkers, [&](std::string_view marker) { return t.find(marker) != std::string::npos; });
}

class BotFatherDialog {
  public:
    BotFatherDialog(BenchAccount& user, bool verbose) : user_(user), verbose_(verbose) {
        const td::Json chat =
            user_.client().call(td::Json{{"@type", "searchPublicChat"}, {"username", "BotFather"}});
        chat_id_ = chat.value("id", int64_t{0});
        if (chat_id_ == 0) {
            throw std::runtime_error("@BotFather was not found on the test DC");
        }
        // Подписка ставится до первой отправки: ответ BotFather может прийти сразу.
        subscription_ = user_.client().subscribe("updateNewMessage", [this](const td::Json& u) {
            if (!u.contains("message")) {
                return;
            }
            const td::Json& m = u["message"];
            if (m.value("chat_id", int64_t{0}) != chat_id_ || m.value("is_outgoing", false)) {
                return;
            }
            if (m.contains("content") && m["content"].value("@type", "") == "messageText") {
                inbox_.push(m["content"]["text"].value("text", ""));
            }
        });
    }

    void send(const std::string& text) {
        inbox_.clear();
        if (verbose_) {
            std::cerr << ">> " << text << "\n";
        }
        td::Json req{{"@type", "sendMessage"},
                     {"chat_id", chat_id_},
                     {"input_message_content",
                      {{"@type", "inputMessageText"},
                       {"text", {{"@type", "formattedText"}, {"text", text}}}}}};
        user_.tracker().send(std::move(req), seconds{30}, seconds{60});
    }

    std::optional<std::string> next() {
        auto m = inbox_.next(kStepTimeout);
        if (m && verbose_) {
            std::cerr << "<< " << redact(*m) << "\n";
        }
        return m;
    }

  private:
    BenchAccount& user_;
    bool verbose_;
    int64_t chat_id_ = 0;
    Inbox inbox_;
    td::Subscription subscription_;
};

struct Created {
    std::string username;
    std::string token;
};

// Один проход диалога. nullopt и flood != nullopt — BotFather просит подождать.
std::optional<Created> run_dialog(BotFatherDialog& d, std::optional<seconds>* flood) {
    d.send("/cancel");  // сбросить недоговорённый диалог прошлого запуска
    (void)d.next();  // ответ может и не прийти, это не ошибка
    d.send("/newbot");
    auto reply = d.next();
    if (!reply) {
        throw std::runtime_error("@BotFather did not answer /newbot in 60 s");
    }
    if (const auto wait = botfather_flood(*reply)) {
        *flood = wait;
        return std::nullopt;
    }
    d.send("tgfs bench");
    reply = d.next();
    if (!reply) {
        throw std::runtime_error("@BotFather did not answer the bot name in 60 s");
    }
    if (const auto wait = botfather_flood(*reply)) {
        *flood = wait;
        return std::nullopt;
    }
    for (int attempt = 1; attempt <= kMaxUsernameAttempts; ++attempt) {
        const std::string username = "tgfs_" + random_hex8() + "_bot";
        d.send(username);
        for (;;) {
            reply = d.next();
            if (!reply) {
                throw std::runtime_error("@BotFather did not answer the username in 60 s");
            }
            std::smatch m;
            if (std::regex_search(*reply, m, token_regex())) {
                return Created{username, m[0].str()};
            }
            if (const auto wait = botfather_flood(*reply)) {
                *flood = wait;
                return std::nullopt;
            }
            if (username_rejected(*reply)) {
                break;  // другой username
            }
            // Прочее (дополнительное сообщение) — ждём дальше.
        }
    }
    throw std::runtime_error("@BotFather rejected " + std::to_string(kMaxUsernameAttempts) +
                             " usernames in a row");
}

}  // namespace

void add_create_bot_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand(
        "create-bot", "Create a test bot at @BotFather (as the test user) and save its token");
    auto verbose = std::make_shared<bool>(false);
    cmd->add_flag("-v,--verbose", *verbose, "Print the dialog with @BotFather (tokens are masked)");
    cmd->callback([&options, exit_code, verbose] {
        if (!options.use_test_dc) {
            // На боевых серверах бота создаёт пользователь сам, не бенч.
            throw std::runtime_error("create-bot works only with --test-dc");
        }
        BenchEnv env(options);
        if (env.creds().test_bot_token) {
            std::cout << "test-bot-token is already set in " << options.creds.string()
                      << ", nothing to do\n";
            *exit_code = 0;
            return;
        }
        const auto user = env.open(Account::kUser, std::cerr);
        BotFatherDialog dialog(*user, *verbose);
        for (int restart = 0;; ++restart) {
            std::optional<seconds> flood;
            if (const auto created = run_dialog(dialog, &flood)) {
                append_cred(options.creds, "test-bot-token", created->token);
                std::cout << "created bot @" << created->username << "\n"
                          << "token saved to " << options.creds.string() << "\n";
                *exit_code = 0;
                return;
            }
            if (!flood || *flood > kMaxFloodWait || restart >= kMaxFloodRestarts) {
                throw std::runtime_error("@BotFather asks to wait " +
                                         std::to_string(flood ? flood->count() : 0) +
                                         " s, giving up");
            }
            const seconds wait = *flood + *flood / 10 + seconds{5};
            std::cerr << "@BotFather rate limit: waiting " << wait.count() << " s\n";
            std::this_thread::sleep_for(wait);
        }
    });
}

}  // namespace telega::bench
