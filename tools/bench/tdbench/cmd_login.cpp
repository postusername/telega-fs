// login и whoami: вход тестового аккаунта и getMe.

#include <iostream>
#include <map>
#include <string>

#include "commands.h"

namespace telega::bench {

CLI::Option* add_account_option(CLI::App& cmd, Account* account) {
    static const std::map<std::string, Account> kNames{{"user", Account::kUser},
                                                       {"bot", Account::kBot}};
    return cmd.add_option("--as", *account, "Account to use: user or bot")
        ->required()
        ->transform(CLI::CheckedTransformer(kNames, CLI::ignore_case));
}

namespace {

std::string username_of(const td::Json& user) {
    if (user.contains("usernames") && user["usernames"].is_object()) {
        const auto& names = user["usernames"];
        if (names.contains("active_usernames") && names["active_usernames"].is_array() &&
            !names["active_usernames"].empty()) {
            return names["active_usernames"][0].get<std::string>();
        }
    }
    return "-";
}

std::string type_of(const td::Json& user) {
    const std::string t = user.contains("type") ? user["type"].value("@type", "") : "";
    if (t == "userTypeBot") {
        return "bot";
    }
    return t == "userTypeRegular" ? "user" : t;
}

// Печатает id, тип и username. Номер телефона не печатаем.
void print_me(const td::Json& me) {
    std::cout << "id: " << me.value("id", int64_t{0}) << "\n"
              << "type: " << type_of(me) << "\n"
              << "username: " << username_of(me) << "\n";
}

}  // namespace

void add_login_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("login", "Log in a test account and keep its session");
    auto account = std::make_shared<Account>(Account::kUser);
    add_account_option(*cmd, account.get());
    cmd->callback([&options, exit_code, account] {
        BenchEnv env(options);
        const auto acc = env.open(*account, std::cerr);
        std::cout << "logged in\n";
        print_me(acc->me());
        *exit_code = 0;
    });
}

void add_whoami_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("whoami", "Print id, type and username of a test account");
    auto account = std::make_shared<Account>(Account::kUser);
    add_account_option(*cmd, account.get());
    cmd->callback([&options, exit_code, account] {
        BenchEnv env(options);
        const auto acc = env.open(*account, std::cerr);
        print_me(acc->me());
        *exit_code = 0;
    });
}

void add_logout_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("logout", "Log out an account and drop its TDLib session");
    auto account = std::make_shared<Account>(Account::kUser);
    add_account_option(*cmd, account.get());
    cmd->callback([&options, exit_code, account] {
        BenchEnv env(options);
        const auto acc = env.open(*account, std::cerr);
        if (!acc->client().log_out()) {
            std::cerr << "logOut did not finish in time\n";
            *exit_code = 1;
            return;
        }
        std::cout << "logged out\n";
        *exit_code = 0;
    });
}

}  // namespace telega::bench
