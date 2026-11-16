#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#include <td/telegram/td_json_client.h>

#include <exception>
#include <iostream>
#include <string>

#include "app/version.h"

namespace {

// Версия TDLib через синхронный td_execute: клиент и сеть для этого не нужны.
std::string tdlib_version() {
    const char* raw = td_execute(R"({"@type":"getOption","name":"version"})");
    if (raw == nullptr) {
        return "unknown";
    }
    const auto reply = nlohmann::json::parse(raw, nullptr, /*allow_exceptions=*/false);
    if (!reply.is_object() || reply.value("@type", "") != "optionValueString") {
        return "unknown";
    }
    return reply.value("value", "unknown");
}

int run(int argc, char** argv) {
    CLI::App app{"telega-fs: encrypted FUSE filesystem on top of a private channel"};
    app.set_version_flag("-V,--version", [] {
        return "telega-fs " + std::string{telega::app::version()} + "\nTDLib " + tdlib_version() +
               "\n" + telega::app::core_library_versions();
    });
    app.require_subcommand(0);

    CLI11_PARSE(app, argc, argv);

    std::cout << app.help();
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << e.what() << '\n';
    } catch (...) {
        std::cerr << "telega-fs: unknown error\n";
    }
    return 1;
}
