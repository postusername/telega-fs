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

// nullopt — неверный пароль или keyfile.
std::optional<UnlockedKeys> unlock_keyring(const KeyringFile& keyring, const Key& kek);

}  // namespace telega::crypto
