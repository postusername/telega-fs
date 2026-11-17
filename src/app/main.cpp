#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#include <td/telegram/td_json_client.h>

#include <exception>
#include <iostream>
#include <string>

#include "app/commands.h"
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
    app.require_subcommand(0, 1);
    app.fallthrough();  // глобальные опции можно писать и после имени команды

    telega::app::GlobalArgs global;
    app.add_option("--config", global.config, "Use FILE instead of ~/.config/telega-fs/config.toml")
        ->type_name("FILE");
    app.add_option("-p,--profile", global.profile, "Profile name")
        ->capture_default_str()
        ->type_name("NAME");
    app.add_flag("-v,--verbose", global.verbose, "Debug logging");

    int exit_code = 0;

    telega::app::InitArgs init_args;
    auto* init = app.add_subcommand("init", "Create a new file system in the profile's store");
    init->add_option("--keyfile", init_args.keyfile,
                     "Use a 32-byte key file instead of a passphrase")
        ->type_name("FILE");
    init->add_option("--chunk-size", init_args.chunk_size,
                     "Chunk size, e.g. 16M (default: from the profile)")
        ->type_name("SIZE");
    init->callback([&] {
        init_args.global = global;
        exit_code = telega::app::run_init(init_args);
    });

    telega::app::MountArgs mount_args;
    auto* mount = app.add_subcommand("mount", "Mount the file system");
    mount->add_option("DIR", mount_args.mountpoint, "Mount point")->required();
    mount->add_flag("-f,--foreground", mount_args.foreground, "Stay in the foreground");
    mount->add_flag("--ro", mount_args.read_only, "Mount read-only");
    mount->add_flag("--discard-local", mount_args.discard_local,
                    "Drop local changes if the channel is newer");
    mount->add_flag("--force-local", mount_args.force_local,
                    "Accept the local state if the channel head was rolled back");
    mount->add_option("-o", mount_args.options, "Extra FUSE mount options (comma separated)")
        ->type_name("OPTS");
    mount->callback([&] {
        mount_args.global = global;
        exit_code = telega::app::run_mount(mount_args);
    });

    CLI11_PARSE(app, argc, argv);

    if (app.get_subcommands().empty()) {
        std::cout << app.help();
    }
    return exit_code;
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
