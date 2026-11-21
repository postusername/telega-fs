#pragma once

// Локальный keyring (SPEC 11): обёрнутые master_key и tdlib_db_key, 256 байт.

#include <filesystem>
#include <optional>

#include "common/bytes.h"
#include "crypto/keys.h"

namespace telega::crypto {

inline constexpr size_t kKeyringSize = 256;

struct KeyringFile {
    FsUuid fs_uuid{};
    KdfParams kdf;
    WrappedKey wrapped_master{};
    WrappedKey wrapped_tdlib{};

    bool operator==(const KeyringFile&) const = default;
};

// Keyring без ФС (SPEC 11, «Keyring до init»): fs_uuid и wrapped_master нулевые, KDF и
// wrapped_tdlib настоящие. Его создаёт login, init дописывает ФС, монтирование на новой машине
// разворачивает master_key из головы канала.
[[nodiscard]] bool keyring_has_fs(const KeyringFile& keyring);

struct UnlockedKeys {
    Key master;
    Key tdlib_db;
};

// Новый keyring: wrapped_* под kek.
KeyringFile make_keyring(const FsUuid& fs_uuid, const KdfParams& kdf, const Key& kek,
                         const Key& master, const Key& tdlib_db);

std::array<std::byte, kKeyringSize> serialize_keyring(const KeyringFile& keyring);
KeyringFile parse_keyring(ByteSpan raw);  // CorruptError при неверном формате

// Запись атомарно с правами 0600 (write_file_atomic); чтение с проверкой формата.
void save_keyring(const std::filesystem::path& path, const KeyringFile& keyring);
KeyringFile load_keyring(const std::filesystem::path& path);

// Keyring без ФС: нулевые fs_uuid и wrapped_master, wrapped_tdlib под kek.
KeyringFile make_keyring_without_fs(const KdfParams& kdf, const Key& kek, const Key& tdlib_db);

// Keyring с ФС: тот же KDF и wrapped_tdlib, что у keyring без ФС, плюс fs_uuid и master под kek
// (init, монтирование на новой машине). Нулевой fs_uuid — std::invalid_argument.
KeyringFile attach_fs(const KeyringFile& keyring, const FsUuid& fs_uuid, const Key& kek,
                      const Key& master);

// nullopt — неверный пароль или keyfile. keyring должен быть с ФС (keyring_has_fs), иначе
// std::logic_error: для keyring без ФС есть unlock_tdlib_key.
std::optional<UnlockedKeys> unlock_keyring(const KeyringFile& keyring, const Key& kek);

// Проверка пароля по wrapped_tdlib (работает и для keyring без ФС). nullopt — неверный пароль
// или keyfile.
std::optional<Key> unlock_tdlib_key(const KeyringFile& keyring, const Key& kek);

}  // namespace telega::crypto
