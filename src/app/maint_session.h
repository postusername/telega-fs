#pragma once

// Сессия обслуживающей команды без FUSE (fsck, recover): профиль, lock на всё время
// команды, источник ключа, хранилище канала (локальное или Telegram). Без TDLib не линкуется:
// транспорт приходит через set_transport_factory (app/tg_session.h).
//
// Порядок уничтожения: хранилище и клиент TDLib живут в самой сессии, поэтому всё, что на них
// ссылается (MountedFs и подобное), команда обязана уничтожить раньше.

#include <memory>
#include <string>

#include "app/cli_util.h"
#include "app/config.h"
#include "app/telegram_cmds.h"
#include "meta/mount.h"
#include "store/local_dir_store.h"
#include "store/store.h"

namespace telega::app {

class MaintSession {
  public:
    // Профиль, lock (занят — ConfigError «профиль уже используется»), источник ключа (keyfile из
    // профиля или пароль со stdin). Хранилище открывается отдельно: open_store.
    explicit MaintSession(const GlobalArgs& global);
    MaintSession(const MaintSession&) = delete;
    MaintSession& operator=(const MaintSession&) = delete;
    ~MaintSession();

    [[nodiscard]] const GlobalArgs& global() const { return global_; }
    [[nodiscard]] const Loaded& loaded() const { return loaded_; }
    [[nodiscard]] const meta::FsPaths& paths() const { return paths_; }
    [[nodiscard]] const meta::KeySource& key() const { return key_; }
    [[nodiscard]] const Profile& profile() const { return loaded_.profile; }

    // Открыть хранилище. Для Telegram: сессия TDLib без ввода и getChat; read_only — учёт кэша
    // чтения не пишется в state.sqlite. Ошибка — исключение (CommandError, StoreError,
    // TdException).
    store::Store& open_store(bool read_only);
    // Хранилище или nullptr, если не открыто.
    [[nodiscard]] store::Store* store() { return store_; }
    // Запустить обслуживание кэша чтения Telegram (вытеснение по cache_limit). Без Telegram —
    // ничего.
    void start_read_cache();

    // Затереть пароль в памяти (после разблокировки ключей).
    void wipe_secrets();

  private:
    GlobalArgs global_;
    Loaded loaded_;
    meta::FsPaths paths_;
    std::unique_ptr<ProfileLock> lock_;
    Secret passphrase_;
    meta::KeySource key_;
    std::unique_ptr<TelegramBackend> telegram_;
    std::unique_ptr<store::LocalDirStore> local_;
    store::Store* store_ = nullptr;
};

}  // namespace telega::app
