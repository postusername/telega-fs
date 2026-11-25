#include "app/maint_session.h"

#include <spdlog/spdlog.h>

#include "crypto/keys.h"

namespace telega::app {

MaintSession::MaintSession(const GlobalArgs& global)
    : global_(global), loaded_(load_profile(global)) {
    paths_ = meta::FsPaths{loaded_.paths.data_dir};
    ensure_profile_dirs(loaded_.paths);
    // Обслуживающая команда держит lock; при смонтированной ФС он занят.
    lock_ = std::make_unique<ProfileLock>(loaded_.paths.lock());

    if (!loaded_.profile.keyfile.empty()) {
        key_.keyfile = loaded_.profile.keyfile;
    } else if (loaded_.profile.store == StoreKind::telegram) {
        // Keyring уже есть (иначе хранилище не открыть): вид источника ключа берётся из него.
        const crypto::KeyringFile keyring = load_keyring_for(loaded_, global_);
        key_ = keyring_key_source(keyring, loaded_.profile.keyfile, passphrase_, "Passphrase: ");
    } else {
        read_secret("Passphrase: ", passphrase_);
        key_.passphrase = passphrase_.value();
    }
}

MaintSession::~MaintSession() { wipe_secrets(); }

void MaintSession::wipe_secrets() {
    passphrase_.wipe();
    wipe(key_.passphrase);
}

store::Store& MaintSession::open_store(bool read_only) {
    if (store_ != nullptr) {
        return *store_;
    }
    const Profile& p = loaded_.profile;
    if (p.store == StoreKind::telegram) {
        if (!p.chat_id) {
            throw CommandError("profile '" + global_.profile +
                               "' has no chat_id: run `telega-fs init` first");
        }
        const crypto::KeyringFile keyring = load_keyring_for(loaded_, global_);
        const crypto::Key tdlib_key = unlock_tdlib_key_or_throw(keyring, key_);
        telegram_ = std::make_unique<TelegramBackend>(loaded_, tdlib_key);
        telegram_->connect();
        telegram_->open_store(*p.chat_id, read_only);
        store_ = &telegram_->store();
    } else {
        local_ = std::make_unique<store::LocalDirStore>(p.local_dir);
        try {
            local_->set_faults(store::fault_rules_from_env());
        } catch (const std::invalid_argument& e) {
            throw CommandError(std::string("invalid ") + store::kFaultsEnvVar + ": " + e.what());
        }
        store_ = local_.get();
    }
    return *store_;
}

void MaintSession::start_read_cache() {
    if (telegram_) {
        telegram_->start_read_cache();
    }
}

}  // namespace telega::app
