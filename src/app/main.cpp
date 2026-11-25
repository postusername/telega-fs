#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#ifndef TELEGA_FS_HAVE_TDLIB
#error "TELEGA_FS_HAVE_TDLIB must be defined by CMake (0 or 1)"
#endif
#if TELEGA_FS_HAVE_TDLIB
#include <td/telegram/td_json_client.h>
#endif

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "app/cmd_maint.h"
#include "app/commands.h"
#include "app/maint_cmds.h"
#include "app/tg_session.h"
#include "app/version.h"
#if TELEGA_FS_HAVE_TDLIB
#include "td/tdjson_transport.h"
#endif

namespace {

#if TELEGA_FS_HAVE_TDLIB
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

// Настоящий транспорт TDLib: единственное место, где создаётся
// TdJsonTransport. Лог TDLib идёт в файл в data-каталоге профиля.
std::shared_ptr<telega::td::TdTransport> make_tdjson_transport(
    const std::filesystem::path& log_file) {
    auto transport = std::make_shared<telega::td::TdJsonTransport>();
    std::string error;
    // Уровень лога TDLib: по умолчанию 1. Для диагностического прогона его поднимает
    // TELEGA_FS_TDLIB_LOG_VERBOSITY (0..5; 4 — отладка загрузок, лог файла TDLib растёт быстро).
    int verbosity = 1;
    // NOLINTNEXTLINE(concurrency-mt-unsafe): читается один раз при старте, до потоков
    if (const char* env = std::getenv("TELEGA_FS_TDLIB_LOG_VERBOSITY");
        env != nullptr && *env >= '0' && *env <= '9' && env[1] == '\0') {
        verbosity = *env - '0';
    }
    if (!telega::td::set_tdlib_log_file(*transport, log_file, int64_t{16} << 20U, verbosity,
                                        &error)) {
        throw std::runtime_error("cannot open the TDLib log " + log_file.string() + ": " + error);
    }
    return transport;
}
#else
// Сборка без TDLib (TELEGA_FS_WITH_TDLIB=OFF): работают только профили с store = "local".
std::string tdlib_version() { return "none"; }

std::shared_ptr<telega::td::TdTransport> make_tdjson_transport(
    const std::filesystem::path& /*log_file*/) {
    throw std::runtime_error(
        "this telega-fs is built without TDLib: only profiles with store = \"local\" work "
        "(rebuild with -DTELEGA_FS_WITH_TDLIB=ON)");
}
#endif

int run(int argc, char** argv) {
    telega::app::set_transport_factory(make_tdjson_transport);

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

    telega::app::LoginArgs login_args;
    auto* login = app.add_subcommand("login", "Log in to Telegram (bot token, user or QR code)");
    login->add_flag("--bot", login_args.bot, "Log in a bot: the token is read from standard input");
    login->add_flag("--qr", login_args.qr, "Log in a user account by QR code instead of a code");
    login->add_flag("--test-dc", login_args.test_dc,
                    "Use the Telegram test servers (saved as use_test_dc in the profile)");
    login->callback([&] {
        login_args.global = global;
        exit_code = telega::app::run_login(login_args);
    });

    telega::app::LogoutArgs logout_args;
    auto* logout = app.add_subcommand("logout", "Log out and delete the Telegram session");
    logout->callback([&] {
        logout_args.global = global;
        exit_code = telega::app::run_logout(logout_args);
    });

    telega::app::InitArgs init_args;
    auto* init = app.add_subcommand("init", "Create a new file system in the profile's store");
    auto* init_chat = init->add_option("--chat", init_args.chat,
                                       "Empty existing channel (the only option for a bot)")
                          ->type_name("ID");
    init->add_option("--title", init_args.title, "Create a new channel (user accounts only)")
        ->type_name("TITLE")
        ->excludes(init_chat);
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
                    "Commit the local state over a rolled back or lost channel head");
    mount->add_option("-o", mount_args.options, "Extra FUSE mount options (comma separated)")
        ->type_name("OPTS");
    mount->callback([&] {
        mount_args.global = global;
        exit_code = telega::app::run_mount(mount_args);
    });

    telega::app::FsckArgs fsck_args;
    auto* fsck = app.add_subcommand(
        "fsck", "Check the index and the channel messages; with --repair fix the index");
    fsck->add_flag("--repair", fsck_args.repair,
                   "Fix the index only (nothing is deleted in the channel) and commit it");
    fsck->add_flag("--verify-data", fsck_args.verify_data,
                   "Download every chunk and verify its authentication tags");
    fsck->callback([&] {
        fsck_args.global = global;
        exit_code = telega::app::run_fsck(fsck_args);
    });

    telega::app::RecoverArgs recover_args;
    auto* recover = app.add_subcommand(
        "recover", "Save uncommitted local data when the channel is newer than the local state");
    recover
        ->add_option("--export-dirty", recover_args.export_dir,
                     "Write the uncommitted files of the local overlay into DIR")
        ->type_name("DIR")
        ->required();
    recover->callback([&] {
        recover_args.global = global;
        exit_code = telega::app::run_recover(recover_args);
    });

    telega::app::StatusArgs status_args;
    auto* status = app.add_subcommand("status", "Show generation, dirty data and upload queue");
    status->add_flag("--json", status_args.json, "Print the status as JSON");
    status->callback([&] {
        status_args.global = global;
        exit_code = telega::app::run_status(status_args);
    });

    telega::app::SyncArgs sync_args;
    auto* sync =
        app.add_subcommand("sync", "Upload all dirty data and commit (mounted file system)");
    sync->callback([&] {
        sync_args.global = global;
        exit_code = telega::app::run_sync(sync_args);
    });

    telega::app::GcArgs gc_args;
    auto* gc = app.add_subcommand("gc", "Delete expired chunks and index blobs (not mounted)");
    gc->add_flag("--compact", gc_args.compact, "Also compact mostly dead packs");
    gc->callback([&] {
        gc_args.global = global;
        exit_code = telega::app::run_gc(gc_args);
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
