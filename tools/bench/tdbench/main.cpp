// tgfs-bench: замеры через TdClient против тестового DC (SPEC 17.2).
// По умолчанию боевые серверы, --test-dc включает тестовый DC (SPEC 17.2). Секреты не печатаются.

#include <CLI/CLI.hpp>

#include <exception>
#include <iostream>

#include "commands.h"
#include "td/td_error.h"

int main(int argc, char** argv) {
    using namespace telega::bench;
    BenchOptions options = BenchOptions::defaults();
    int exit_code = 0;

    CLI::App app{"tgfs-bench: experiments against the Telegram test DC (SPEC 17.2)"};
    app.require_subcommand(1);
    app.fallthrough();
    app.add_option("--creds", options.creds,
                   "File with api-id, api-hash, bot-token, test-bot-token (key=value)")
        ->capture_default_str()
        ->type_name("FILE");
    app.add_option("--state-dir", options.state_dir,
                   "TDLib sessions, files and logs (outside the repository)")
        ->capture_default_str()
        ->type_name("DIR");
    app.add_option("--dc", options.test_dc, "Test DC (1-3) for a newly created test user number")
        ->capture_default_str()
        ->check(CLI::Range(1, 3));
    app.add_flag("--test-dc", options.use_test_dc,
                 "Use the Telegram test DC instead of production");
    app.add_flag("--qr", options.qr,
                 "Log in the user by scanning a QR code (implies --interactive)");
    app.add_flag("--interactive", options.interactive,
                 "Ask for the phone number, login code and 2FA password in the terminal");

    add_login_command(app, options, &exit_code);
    add_whoami_command(app, options, &exit_code);
    add_create_bot_command(app, options, &exit_code);
    add_logout_command(app, options, &exit_code);
    add_e2_command(app, options, &exit_code);
    add_e3_command(app, options, &exit_code);
    add_e4_command(app, options, &exit_code);
    add_e5_command(app, options, &exit_code);
    add_e6_command(app, options, &exit_code);
    add_e7_command(app, options, &exit_code);
    add_e9_command(app, options, &exit_code);
    add_e10_command(app, options, &exit_code);
    add_e2e_command(app, options, &exit_code);

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        return app.exit(e);
    } catch (const telega::td::TdException& e) {
        std::cerr << "error: " << e.error().to_string() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
    return exit_code;
}
