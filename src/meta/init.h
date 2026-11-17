#pragma once

// init (SPEC 12.3, 11): новая ФС в пустом канале. Ключи, keyring, пустой индекс,
// state.sqlite и первый коммит — снимок поколения 1, закреплённый как голова.

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
    // Параметры Argon2id для пароля; nullopt — passphrase_kdf_params() (MODERATE, SPEC 11).
    // Тесты передают MIN. Для keyfile не используется.
    std::optional<crypto::KdfParams> kdf;

    // Неизменные после init (SPEC 12.2).
    int64_t chunk_size = 16LL << 20;
    int64_t inline_max = 4LL << 10;
    int64_t inline_budget = 16LL << 20;
    int64_t pack_threshold = 1LL << 20;
    int64_t pack_target = 16LL << 20;
};

struct InitResult {
    FsUuid fs_uuid{};
    int64_t head_msg_id = 0;
};

// Канал не пуст (read_head() != nullopt) или в профиле уже есть keyring, index.sqlite или
// state.sqlite — InitError. Если первый коммит не удался и в канале по-прежнему нет головы,
// созданные локальные файлы удаляются. Если голова там уже есть (pin состоялся, а упал
// шаг 8) или проверить не удалось, keyring и базы остаются: канал закрепляет наш блоб, и
// монтирование доделает коммит (SPEC 7.2, G_r = G_l + 1).
// Ошибки Store пробрасываются как есть (или CommitError из коммита).
InitResult init_fs(store::Store& store, const FsPaths& paths, const InitOptions& options);

}  // namespace telega::meta
