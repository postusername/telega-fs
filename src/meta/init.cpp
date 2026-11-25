#include "meta/init.h"

#include <spdlog/spdlog.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

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

// Store, который запоминает msg_id залитого блоба: по нему после сбоя коммита видно, наш ли
// блоб закреплён (сам CommitManager при ошибке msg_id не отдаёт).
class TrackingStore final : public store::Store {
  public:
    explicit TrackingStore(store::Store& inner) : inner_(inner) {}

    [[nodiscard]] std::optional<int64_t> uploaded() const { return uploaded_; }

    store::ChunkRef put_chunk(const std::filesystem::path& f) override {
        return inner_.put_chunk(f);
    }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        inner_.read_range(r, off, out);
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_.delete_messages(ids); }
    std::optional<store::HeadInfo> read_head() override { return inner_.read_head(); }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const std::filesystem::path& blob) override {
        const int64_t id = inner_.put_index(blob);
        uploaded_ = id;
        return id;
    }
    void set_head(int64_t id, std::optional<int64_t> old) override { inner_.set_head(id, old); }

  private:
    store::Store& inner_;
    std::optional<int64_t> uploaded_;
};

// Читать голову, пока она пуста, но не дольше окна (кэш TDLib: после pin голова может
// до полутора минут читаться пустой). Окно 0 — одно чтение. Ошибка чтения пробрасывается.
std::optional<store::HeadInfo> read_head_over_window(store::Store& store,
                                                     const InitOptions& options,
                                                     std::string_view why) {
    auto head = store.read_head();
    const auto until = std::chrono::steady_clock::now() + options.stale_head_window;
    while (!head) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
            until - std::chrono::steady_clock::now());
        if (left <= std::chrono::milliseconds{0}) {
            break;
        }
        const auto pause = std::min(left, options.stale_head_period);
        spdlog::info("init: the channel head is empty, checking again in {} s ({}; TDLib cache?)",
                     pause.count() / 1000, why);
        std::this_thread::sleep_for(pause);
        head = store.read_head();
    }
    return head;
}

enum class RollbackVerdict : uint8_t {
    pinned,  // голова — наш блоб: pin состоялся, откатывать нельзя
    unpinned,  // голова пуста всё окно: pin не состоялся, можно откатывать
    unknown,  // чужая голова или чтение не удалось: ключи нужнее, ничего не трогаем
};

// Что с каналом после сбоя первого коммита. Блоб не залит — pin невозможен.
RollbackVerdict judge_failed_commit(store::Store& store, std::optional<int64_t> uploaded,
                                    const InitOptions& options) {
    if (!uploaded) {
        return RollbackVerdict::unpinned;
    }
    try {
        const auto head = read_head_over_window(store, options, "our pin may be hidden by a cache");
        if (!head) {
            return RollbackVerdict::unpinned;
        }
        return head->msg_id == *uploaded ? RollbackVerdict::pinned : RollbackVerdict::unknown;
    } catch (const std::exception& e) {
        spdlog::warn("init: cannot check the channel after a failed commit: {}", e.what());
        return RollbackVerdict::unknown;
    }
}

}  // namespace

InitProfileState inspect_profile_for_init(const FsPaths& paths) {
    std::optional<crypto::KeyringFile> keyring;
    if (std::filesystem::exists(paths.keyring())) {
        try {
            keyring = crypto::load_keyring(paths.keyring());
        } catch (const crypto::CorruptError& e) {
            throw InitError(std::string("the local keyring is damaged: ") + e.what());
        }
    }
    const bool dbs =
        std::filesystem::exists(paths.index_db()) || std::filesystem::exists(paths.state_db());
    if (!keyring || !crypto::keyring_has_fs(*keyring)) {
        if (dbs) {
            throw InitError("the profile already has a file system (" +
                            std::string(std::filesystem::exists(paths.index_db())
                                            ? paths.index_db().filename().string()
                                            : paths.state_db().filename().string()) +
                            " exists)");
        }
        return InitProfileState::fresh;
    }
    // Keyring с ФС. init пишет его раньше pin, поэтому такой профиль мог остаться от
    // прерванного init: повторить можно, пока ФС ни разу не закоммичена.
    if (std::filesystem::exists(paths.state_db())) {
        try {
            StateStore state{paths.state_db()};
            if (state.get_int_or(state_key::base_generation, 0) > 0) {
                throw InitError("the profile already has a file system (it has been committed)");
            }
        } catch (const InitError&) {
            throw;
        } catch (const std::exception& e) {
            throw InitError(std::string("the local state of an earlier init is damaged: ") +
                            e.what());
        }
    }
    return InitProfileState::resumable;
}

InitResult init_fs(store::Store& store, const FsPaths& paths, const InitOptions& options) {
    crypto::init();
    // Keyring, созданный login до init: без ФС. init дописывает
    // в него ФС тем же KEK. Keyring с ФС и незакоммиченной ФС — прерванный init.
    const InitProfileState profile_state = inspect_profile_for_init(paths);
    const bool resuming = profile_state == InitProfileState::resumable;
    std::optional<crypto::KeyringFile> existing;
    if (std::filesystem::exists(paths.keyring())) {
        existing = crypto::load_keyring(paths.keyring());
    }

    if (resuming && existing) {
        // Пароль проверяется до ожидания: с чужим паролем ждать нечего.
        const crypto::Key kek = kek_from(options.key, existing->kdf);
        if (!crypto::unlock_tdlib_key(*existing, kek)) {
            throw InitError("wrong passphrase or keyfile");
        }
        spdlog::warn("init: repeating an interrupted init: the file system is generated anew");
    }
    // При повторе прерванного init pin мог состояться, а кэш TDLib — его скрывать: пустой
    // ответ принимается только после окна. Новому профилю окно не нужно.
    const auto head =
        resuming ? read_head_over_window(store, options, "repeating init") : store.read_head();
    if (head) {
        throw InitError(
            resuming ? "the channel is not empty: it has a pinned message (an earlier init may "
                       "have reached the pin: try `telega-fs mount`)"
                     : "the channel is not empty: it already has a pinned message");
    }
    ensure_fs_dirs(paths);

    crypto::KdfParams kdf;
    crypto::Key kek;
    if (existing) {
        // KDF и KEK те же, что у keyring; пароль проверяется по wrapped_tdlib.
        kdf = existing->kdf;
        kek = kek_from(options.key, kdf);
    } else if (!options.key.keyfile.empty()) {
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
    crypto::KeyringFile keyring;
    if (existing) {
        if (!crypto::unlock_tdlib_key(*existing, kek)) {
            throw InitError("wrong passphrase or keyfile");
        }
        keyring = crypto::attach_fs(*existing, fs_uuid, kek, master);
    } else {
        keyring = crypto::make_keyring(fs_uuid, kdf, kek, master, crypto::Key::random());
    }

    InitResult result;
    result.fs_uuid = fs_uuid;
    bool committed = false;
    TrackingStore tracked{store};
    try {
        if (resuming) {
            // Базы прерванного init (ФС ни разу не закоммичена) заменяются новыми.
            remove_db(paths.index_db());
            remove_db(paths.state_db());
        }
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
        CommitManager committer{meta, state, tracked,
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
        // в канале закреплён наш блоб, и без keyring с базами ключ к нему потерян. Тогда
        // локальное состояние остаётся, и это успех: монтирование узнаёт свой блоб (G_r = G_l + 1)
        // и доделывает шаг 8. Откат только если блоб не залит или голова пуста всё
        // окно кэша TDLib. Чужая голова или сбой проверки — ключи нужнее, остаётся.
        if (!committed) {
            const RollbackVerdict verdict = judge_failed_commit(store, tracked.uploaded(), options);
            if (verdict == RollbackVerdict::pinned) {
                spdlog::warn(
                    "init: the commit failed after the pin; the file system is kept and the next "
                    "mount finishes it");
                result.head_msg_id = *tracked.uploaded();
                spdlog::warn("init: losing the passphrase or keyfile means losing all data");
                return result;
            }
            if (verdict == RollbackVerdict::unpinned) {
                std::error_code ignored;
                if (existing) {
                    crypto::save_keyring(paths.keyring(), *existing);  // как было до init
                } else {
                    std::filesystem::remove(paths.keyring(), ignored);
                }
                remove_db(paths.index_db());
                remove_db(paths.state_db());
                if (tracked.uploaded()) {
                    // Залитый, но не закреплённый блоб — мусор канала.
                    try {
                        const std::vector<int64_t> orphan{*tracked.uploaded()};
                        store.delete_messages(orphan);
                    } catch (const std::exception& e) {
                        spdlog::warn("init: cannot delete the unpinned blob: {}", e.what());
                    }
                }
            }
        }
        throw;
    }
    spdlog::warn("init: losing the passphrase or keyfile means losing all data");
    return result;
}

}  // namespace telega::meta
