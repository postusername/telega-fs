#pragma once

// init: новая ФС в пустом канале. Ключи, keyring, пустой индекс,
// state.sqlite и первый коммит — снимок поколения 1, закреплённый как голова.

#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>

#include "crypto/keys.h"
#include "meta/mount.h"
#include "store/store.h"

namespace telega::meta {

class InitError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

struct InitOptions {
    KeySource key;
    // Параметры Argon2id для пароля; nullopt — passphrase_kdf_params() (MODERATE).
    // Тесты передают MIN. Для keyfile не используется.
    std::optional<crypto::KdfParams> kdf;

    // Неизменные после init.
    int64_t chunk_size = 8LL << 20;
    int64_t inline_max = 4LL << 10;
    int64_t inline_budget = 16LL << 20;
    int64_t pack_threshold = 4LL << 20;
    int64_t pack_target = 8LL << 20;

    // Кэш TDLib: при повторе прерванного init и при откате после сбоя `set_head`
    // пустая голова принимается как «канала нет» только после этого окна; чтение повторяется
    // раз в `stale_head_period`. 0 — одно чтение (хранилища без кэша).
    std::chrono::milliseconds stale_head_window{0};
    std::chrono::milliseconds stale_head_period{10'000};
};

struct InitResult {
    FsUuid fs_uuid{};
    int64_t head_msg_id = 0;
};

// Состояние профиля перед init. Нет keyring или он без ФС, а базы есть, либо keyring
// с ФС, уже закоммиченной (base_generation > 0), — InitError.
enum class InitProfileState : uint8_t {
    fresh,  // keyring нет или он без ФС (login до init), баз нет
    resumable,  // keyring с ФС, но ФС ни разу не закоммичена: прерванный init
};
InitProfileState inspect_profile_for_init(const FsPaths& paths);

// Канал не пуст (read_head() != nullopt) или профиль не годится для init (см.
// inspect_profile_for_init) — InitError. Прерванный init (keyring с ФС, base_generation = 0)
// повторяется: пароль проверяется по wrapped_tdlib, голова канала должна быть пуста всё окно
// stale_head_window, базы пересоздаются, ФС генерируется заново (новые fs_uuid и master_key),
// wrapped_tdlib сохраняется.
// Если первый коммит не удался: блоб не залит или голова пуста всё окно — созданные локальные
// файлы удаляются (keyring возвращается как был). Если голова — наш блоб (pin состоялся, а
// упал set_head или шаг 8), init считается успешным: keyring и базы остаются, монтирование
// доделает коммит (G_r = G_l + 1). Чужая голова или сбой проверки — файлы остаются,
// ошибка пробрасывается.
// Ошибки Store пробрасываются как есть (или CommitError из коммита).
InitResult init_fs(store::Store& store, const FsPaths& paths, const InitOptions& options);

}  // namespace telega::meta
