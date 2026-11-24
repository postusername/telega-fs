#include <spdlog/spdlog.h>

#include <iostream>
#include <memory>
#include <string>

#include "app/cli_util.h"
#include "app/maint_cmds.h"
#include "app/maint_session.h"
#include "common/bytes.h"
#include "crypto/keyring.h"
#include "crypto/keys.h"
#include "vfs/export_dirty.h"

namespace telega::app {

namespace fs = std::filesystem;

namespace {

// Ключи ФС из keyring профиля: выгрузка идёт офлайн и не ходит за головой канала.
crypto::SubKeys unlock_fs_keys(const MaintSession& session) {
    const Loaded& l = session.loaded();
    crypto::KeyringFile keyring;
    try {
        if (!std::filesystem::exists(l.paths.keyring())) {
            throw CommandError("no keyring in profile '" + session.global().profile +
                               "': there is no local state to export");
        }
        keyring = crypto::load_keyring(l.paths.keyring());
    } catch (const crypto::CorruptError& e) {
        throw CommandError(std::string("the local keyring is damaged: ") + e.what());
    }
    if (!crypto::keyring_has_fs(keyring)) {
        throw CommandError("the keyring has no file system: there is no local state to export");
    }
    crypto::Key kek;
    try {
        kek = meta::kek_from(session.key(), keyring.kdf);
    } catch (const meta::MountError& e) {
        throw CommandError(describe_exception(e));
    }
    const auto unlocked = crypto::unlock_keyring(keyring, kek);
    if (!unlocked) {
        throw CommandError("wrong passphrase or keyfile");
    }
    return crypto::derive_subkeys(unlocked->master);
}

int recover_impl(const RecoverArgs& args) {
    crypto::init();
    if (args.export_dir.empty()) {
        throw CommandError("recover needs --export-dirty DIR");
    }
    MaintSession session{args.global};
    const crypto::SubKeys keys = unlock_fs_keys(session);

    // Залитые части файлов читаются через Store. Если его нет (сети нет, сессия не открывается),
    // выгружается то, что есть локально, а неполные файлы перечисляются.
    store::Store* store = nullptr;
    try {
        store = &session.open_store(/*read_only=*/true);
        session.start_read_cache();
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: the channel is not available (" << describe_exception(e)
                  << "): only local data is exported\n";
    }
    // Пароль нужен и open_store (ключ базы TDLib), поэтому затирается только после него.
    session.wipe_secrets();

    const fs::path out = absolute_path(args.export_dir);
    const vfs::ExportReport report =
        vfs::export_dirty(session.paths(), store, keys, out, vfs::ExportOptions{});

    std::cout << "Exported " << report.files.size() << " file(s), " << report.bytes_written
              << " bytes, to " << out.string() << '\n';
    for (const vfs::ExportedFile& f : report.files) {
        std::cout << "  " << f.path << "  " << f.size
                  << (f.symlink ? " bytes (symlink)" : " bytes");
        if (!f.complete()) {
            std::cout << "  INCOMPLETE, unreadable positions:";
            for (const int64_t idx : f.missing) {
                std::cout << ' ' << idx;
            }
        }
        std::cout << '\n';
    }
    if (!report.empty_dirs.empty()) {
        std::cout << "Directories changed after the last commit (created empty):\n";
        for (const std::string& d : report.empty_dirs) {
            std::cout << "  " << d << '\n';
        }
    }
    for (const std::string& w : report.warnings) {
        std::cout << "warning: " << w << '\n';
    }
    const size_t incomplete = report.incomplete_count();
    if (incomplete > 0) {
        std::cout << incomplete << " file(s) are incomplete: their unreadable parts are zeros\n";
        return 2;
    }
    return 0;
}

}  // namespace

int run_recover(const RecoverArgs& args) {
    try {
        return recover_impl(args);
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        return 1;
    }
}

}  // namespace telega::app
