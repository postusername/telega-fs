#pragma once

// Telegram в приложении: хранилище профиля (сессия TDLib, лимитер, TelegramStore), разблокировка
// keyring до init (SPEC 11) и команды login, logout, init для store = "telegram" (SPEC 12.3).
// Без TDLib: транспорт приходит через set_transport_factory (app/tg_session.h).

#include <filesystem>
#include <memory>
#include <string>

#include "app/cli_util.h"
#include "app/commands.h"
#include "app/tg_session.h"
#include "crypto/keyring.h"
#include "meta/init.h"
#include "meta/mount.h"
#include "store/rate_limiter.h"
#include "store/store.h"
#include "store/telegram_store.h"

namespace telega::app {

// Keyring профиля, который login создал до init. CommandError с подсказкой, если его нет.
crypto::KeyringFile load_keyring_for(const Loaded& l, const GlobalArgs& global);

// Источник ключа для keyring: keyfile (из профиля или аргумента), иначе пароль с терминала.
// Вид источника сверяется с KDF keyring: несовпадение — CommandError с понятным текстом.
meta::KeySource keyring_key_source(const crypto::KeyringFile& keyring,
                                   const std::filesystem::path& keyfile, Secret& passphrase,
                                   const char* prompt);

// tdlib_db_key из keyring (с ФС или без) под источником ключа. Неверный пароль — CommandError.
crypto::Key unlock_tdlib_key_or_throw(const crypto::KeyringFile& keyring,
                                      const meta::KeySource& source);

// Telegram-хранилище профиля. Деструктор закрывает в обратном порядке: TelegramStore, лимитер,
// клиент TDLib (close) и TdRuntime (SPEC 10.1).
class TelegramBackend {
  public:
    TelegramBackend(const Loaded& loaded, const crypto::Key& tdlib_db_key);
    TelegramBackend(const TelegramBackend&) = delete;
    TelegramBackend& operator=(const TelegramBackend&) = delete;
    ~TelegramBackend();

    // Авторизация без ввода. Не авторизована — CommandError «run telega-fs login».
    void connect();
    // Лимитер и TelegramStore для канала chat_id; open() проверяет доступ, автоудаление и права
    // (SPEC 7.2 шаг 3, 12.3).
    void open_store(int64_t chat_id);

    [[nodiscard]] TgSession& session() { return session_; }
    [[nodiscard]] store::Store& store();

  private:
    const Loaded& loaded_;
    TgSession session_;
    std::unique_ptr<store::RateLimiter> limiter_;
    std::unique_ptr<store::TelegramStore> store_;
};

// Параметры лимитера и хранилища из профиля (SPEC 10.2, 12.2).
store::RateLimiterOptions limiter_options(const Profile& p);
store::TelegramStoreOptions store_options(const Profile& p, int64_t chat_id);

// «bot @name (id N)» или «user @name (id N)»; телефон не печатается.
std::string describe_account(const td::Json& me);

// init для store = "telegram": канал по --chat или --title, проверки, init_fs, chat_id в профиль.
int init_telegram(const Loaded& l, const InitArgs& args, const meta::InitOptions& opts);

}  // namespace telega::app
