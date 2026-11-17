#pragma once

// Ключи и их обёртка (SPEC 11).

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string_view>

#include "common/bytes.h"

namespace telega::crypto {

// Порча или подмена данных: ошибка проверки тега, неверный magic, несогласованные поля.
// Наверху превращается в EIO (SPEC 5) или в отказ монтирования (SPEC 6.6, 7.2).
class CorruptError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// sodium_init(); вызывать до любых функций модуля. Повторный вызов безопасен.
void init();

// 32-байтный секрет. Обнуляется в деструкторе (sodium_memzero).
class Key {
  public:
    static constexpr size_t kSize = 32;

    Key() = default;
    explicit Key(ByteSpan raw);  // raw.size() == 32, иначе std::invalid_argument
    Key(const Key&) = default;
    Key& operator=(const Key&) = default;
    ~Key();

    static Key random();

    [[nodiscard]] const std::byte* data() const noexcept { return bytes_.data(); }
    [[nodiscard]] ByteSpan span() const noexcept { return bytes_; }
    bool operator==(const Key& other) const noexcept;  // sodium_memcmp

  private:
    std::array<std::byte, kSize> bytes_{};
};

// Подключи из master_key: crypto_kdf_derive_from_key, контекст "TGFS_KDF".
struct SubKeys {
    Key data;   // id 1, K_data — чанки (SPEC 5)
    Key index;  // id 2, K_index — блобы индекса (SPEC 6.3)
    Key mac;    // id 3, K_mac — content_mac (SPEC 6.2)
};
SubKeys derive_subkeys(const Key& master);

FsUuid random_fs_uuid();

enum class KdfAlg : uint8_t {
    keyfile = 0,     // KEK = содержимое keyfile, параметры нулевые
    argon2id13 = 1,  // KEK = Argon2id(passphrase, salt, opslimit, memlimit)
};

struct KdfParams {
    KdfAlg alg = KdfAlg::keyfile;
    uint64_t opslimit = 0;
    uint64_t memlimit = 0;
    std::array<std::byte, 16> salt{};

    bool operator==(const KdfParams&) const = default;
};

// Параметры для пароля: случайная соль, OPSLIMIT_MODERATE и MEMLIMIT_MODERATE (SPEC 11).
// Тесты могут передать свои (MIN), чтобы не тратить 256 МиБ и секунды.
KdfParams passphrase_kdf_params();
KdfParams passphrase_kdf_params(uint64_t opslimit, uint64_t memlimit);
KdfParams keyfile_kdf_params();

// KEK из пароля по params (alg должен быть argon2id13).
Key derive_kek(std::string_view passphrase, const KdfParams& params);

// KEK из keyfile: файл ровно из 32 байт, иначе std::runtime_error без содержимого файла.
Key read_keyfile(const std::filesystem::path& path);

// secretbox(KEK): nonce(24) || mac(16) || key(32) (SPEC 6.3, 11).
using WrappedKey = std::array<std::byte, 72>;
WrappedKey wrap_key(const Key& kek, const Key& key);
// nullopt — неверный KEK или порча (их не различить).
std::optional<Key> unwrap_key(const Key& kek, const WrappedKey& wrapped);

// BLAKE2b-256.
using Hash32 = std::array<std::byte, 32>;
Hash32 blake2b256(ByteSpan data);  // без ключа: prev_hash, head_hash (SPEC 6.3)
Hash32 blake2b256_keyed(const Key& key, ByteSpan data);

// content_mac (SPEC 6.2): BLAKE2b-256 с ключом K_mac от size байт файла fd начиная с 0,
// читается потоково. Если файл короче size, недостающее считается нулями.
Hash32 content_mac(const Key& k_mac, int fd, uint64_t size);

}  // namespace telega::crypto
