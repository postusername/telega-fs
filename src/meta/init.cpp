#include "meta/init.h"

#include <spdlog/spdlog.h>
#include <unistd.h>

#include <chrono>
#include <system_error>

#include "crypto/keyring.h"
#include "meta/commit.h"
#include "meta/index_db.h"
#include "meta/state_db.h"
#include "store/local_dir_store.h"

namespace telega::meta {

namespace {

void remove_db(const std::filesystem::path& path) {
    std::error_code ignored;
    for (const char* suffix : {"", "-wal", "-shm", "-journal"}) {
        auto p = path;
        p += suffix;
        std::filesystem::remove(p, ignored);
    }
}

// true только если канал достоверно пуст: чтение головы удалось и закреплённого нет.
bool channel_is_empty(store::Store& store) {
    try {
        return !store.read_head().has_value();
    } catch (const std::exception& e) {
        spdlog::warn("init: cannot check the channel after a failed commit: {}", e.what());
        return false;
    }
}

}  // namespace

InitResult init_fs(store::Store& store, const FsPaths& paths, const InitOptions& options) {
    crypto::init();
    if (store.read_head()) {
        throw InitError("the channel is not empty: it already has a pinned message");
    }
    for (const auto& p : {paths.keyring(), paths.index_db(), paths.state_db()}) {
        if (std::filesystem::exists(p)) {
            throw InitError("the profile already has a file system (" + p.filename().string() +
                            " exists)");
        }
    }
    ensure_fs_dirs(paths);

    crypto::KdfParams kdf;
    crypto::Key kek;
    if (!options.key.keyfile.empty()) {
        kdf = crypto::keyfile_kdf_params();
        kek = kek_from(options.key, kdf);
    } else if (options.key.passphrase) {
        kdf = options.kdf.value_or(crypto::passphrase_kdf_params());
        if (kdf.alg != crypto::KdfAlg::argon2id13) {
            throw std::invalid_argument("init: passphrase requires argon2id KDF parameters");
        }
        kek = kek_from(options.key, kdf);
    } else {
        throw InitError("a passphrase or a keyfile is required");
    }

    const FsUuid fs_uuid = crypto::random_fs_uuid();
    const crypto::Key master = crypto::Key::random();
    const crypto::Key tdlib_db = crypto::Key::random();
    const crypto::KeyringFile keyring = crypto::make_keyring(fs_uuid, kdf, kek, master, tdlib_db);

    InitResult result;
    result.fs_uuid = fs_uuid;
    bool committed = false;
    try {
        crypto::save_keyring(paths.keyring(), keyring);

        InitParams params;
        params.fs_uuid = fs_uuid;
        params.chunk_size = options.chunk_size;
        params.inline_max = options.inline_max;
        params.inline_budget = options.inline_budget;
        params.pack_threshold = options.pack_threshold;
        params.pack_target = options.pack_target;
        params.root_uid = ::getuid();
        params.root_gid = ::getgid();
        params.root_mode = 0755;
        params.now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
        MetaStore::create(paths.index_db(), params);

        StateStore::create(paths.state_db());
        StateStore state{paths.state_db()};
        state.transaction([&] {
            state.set_blob(state_key::fs_uuid, fs_uuid);
            state.set_int(state_key::base_generation, 0);
            state.set_int(state_key::head_msg_id, 0);
            state.set_int(state_key::clean_shutdown, 1);
            state.set_int(state_key::force_snapshot, 0);
            state.set_int(state_key::index_dirty, 1);  // первый коммит обязан состояться
        });

        MetaStore meta{paths.index_db()};
        CommitConfig config;
        config.tmp_dir = paths.tmp();
        CommitManager committer{meta, state, store,
                                BlobKeys{.k_index = crypto::derive_subkeys(master).index,
                                         .fs_uuid = fs_uuid,
                                         .kdf = keyring.kdf,
                                         .wrapped_master = keyring.wrapped_master},
                                config};
        // Цепочки ещё нет, поэтому это снимок поколения 1; set_head закрепляет его.
        const CommitResult commit = committer.commit(true);
        committed = commit.committed;
        result.head_msg_id = commit.metrics.msg_id;
        committer.stop();
    } catch (const store::CrashInjected&) {
        throw;  // имитация падения: файлы остаются, как после настоящего сбоя
    } catch (...) {
        // Если ошибка пришла уже после pin (set_head с потерянным ответом или сбой шага 8),
        // в канале закреплён наш блоб. Удалить keyring и базы значило бы потерять ключ к нему.
        // Тогда локальное состояние остаётся: монтирование узнаёт свой блоб (G_r = G_l + 1,
        // SPEC 7.2) и доделывает шаг 8. Удаляем только если канал точно остался без головы;
        // если проверить не удалось, тоже оставляем (ключи нужнее, чем чистый профиль).
        if (!committed && channel_is_empty(store)) {
            std::error_code ignored;
            std::filesystem::remove(paths.keyring(), ignored);
            remove_db(paths.index_db());
            remove_db(paths.state_db());
        }
        throw;
    }
    spdlog::warn("init: losing the passphrase or keyfile means losing all data");
    return result;
}

}  // namespace telega::meta
