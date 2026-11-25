#include "crypto/keyring.h"

#include <algorithm>
#include <stdexcept>

#include "common/files.h"
#include "crypto/detail.h"

namespace telega::crypto {

namespace {

// Раскладка keyring. Всё, что после wrapped_tdlib, — нули.
constexpr std::array<char, 8> kMagic{'T', 'G', 'F', 'S', 'K', 'E', 'Y', '1'};
constexpr uint8_t kVersion = 1;
constexpr size_t kOffVersion = 8;
constexpr size_t kOffKdfAlg = 9;
constexpr size_t kOffReserved = 10;
constexpr size_t kReservedSize = 6;
constexpr size_t kOffFsUuid = 16;
constexpr size_t kOffKdfLimits = 32;  // opslimit(8) || memlimit(8) || salt(16)
constexpr size_t kOffWrappedMaster = kOffKdfLimits + detail::kKdfLimitsSize;            // 64
constexpr size_t kOffWrappedTdlib = kOffWrappedMaster + std::tuple_size_v<WrappedKey>;  // 136
constexpr size_t kOffPadding = kOffWrappedTdlib + std::tuple_size_v<WrappedKey>;        // 208

static_assert(kOffWrappedMaster == 64 && kOffWrappedTdlib == 136 && kOffPadding == 208);
static_assert(kOffPadding <= kKeyringSize);

}  // namespace

KeyringFile make_keyring(const FsUuid& fs_uuid, const KdfParams& kdf, const Key& kek,
                         const Key& master, const Key& tdlib_db) {
    return KeyringFile{.fs_uuid = fs_uuid,
                       .kdf = kdf,
                       .wrapped_master = wrap_key(kek, master),
                       .wrapped_tdlib = wrap_key(kek, tdlib_db)};
}

bool keyring_has_fs(const KeyringFile& keyring) { return !detail::all_zero(keyring.fs_uuid); }

KeyringFile make_keyring_without_fs(const KdfParams& kdf, const Key& kek, const Key& tdlib_db) {
    return KeyringFile{
        .fs_uuid = {}, .kdf = kdf, .wrapped_master = {}, .wrapped_tdlib = wrap_key(kek, tdlib_db)};
}

KeyringFile attach_fs(const KeyringFile& keyring, const FsUuid& fs_uuid, const Key& kek,
                      const Key& master) {
    if (detail::all_zero(fs_uuid)) {
        throw std::invalid_argument("keyring: fs_uuid must not be zero");
    }
    KeyringFile out = keyring;
    out.fs_uuid = fs_uuid;
    out.wrapped_master = wrap_key(kek, master);
    return out;
}

std::array<std::byte, kKeyringSize> serialize_keyring(const KeyringFile& keyring) {
    std::array<std::byte, kKeyringSize> raw{};
    const MutableByteSpan out{raw};
    std::transform(kMagic.begin(), kMagic.end(), raw.begin(),
                   [](char c) { return static_cast<std::byte>(c); });
    raw[kOffVersion] = std::byte{kVersion};
    raw[kOffKdfAlg] = static_cast<std::byte>(keyring.kdf.alg);
    std::copy(keyring.fs_uuid.begin(), keyring.fs_uuid.end(), raw.begin() + kOffFsUuid);
    detail::put_kdf_limits(out.subspan(kOffKdfLimits, detail::kKdfLimitsSize), keyring.kdf);
    std::copy(keyring.wrapped_master.begin(), keyring.wrapped_master.end(),
              raw.begin() + kOffWrappedMaster);
    std::copy(keyring.wrapped_tdlib.begin(), keyring.wrapped_tdlib.end(),
              raw.begin() + kOffWrappedTdlib);
    return raw;
}

KeyringFile parse_keyring(ByteSpan raw) {
    if (raw.size() != kKeyringSize) {
        throw CorruptError("keyring: wrong size");
    }
    if (!std::equal(kMagic.begin(), kMagic.end(), raw.begin(),
                    [](char c, std::byte b) { return static_cast<std::byte>(c) == b; })) {
        throw CorruptError("keyring: bad magic");
    }
    if (raw[kOffVersion] != std::byte{kVersion}) {
        throw CorruptError("keyring: unsupported version");
    }
    if (!detail::all_zero(raw.subspan(kOffReserved, kReservedSize))) {
        throw CorruptError("keyring: non-zero reserved bytes");
    }
    if (!detail::all_zero(raw.subspan(kOffPadding))) {
        throw CorruptError("keyring: non-zero padding");
    }
    KeyringFile keyring;
    keyring.kdf = detail::parse_kdf(static_cast<uint8_t>(raw[kOffKdfAlg]),
                                    raw.subspan(kOffKdfLimits, detail::kKdfLimitsSize));
    std::copy_n(raw.begin() + kOffFsUuid, keyring.fs_uuid.size(), keyring.fs_uuid.begin());
    std::copy_n(raw.begin() + kOffWrappedMaster, keyring.wrapped_master.size(),
                keyring.wrapped_master.begin());
    std::copy_n(raw.begin() + kOffWrappedTdlib, keyring.wrapped_tdlib.size(),
                keyring.wrapped_tdlib.begin());
    if (detail::all_zero(keyring.fs_uuid) != detail::all_zero(keyring.wrapped_master)) {
        // Keyring без ФС — оба поля нулевые; одно из двух — порча.
        throw CorruptError("keyring: fs_uuid and wrapped_master disagree");
    }
    return keyring;
}

void save_keyring(const std::filesystem::path& path, const KeyringFile& keyring) {
    write_file_atomic(path, serialize_keyring(keyring), 0600);
}

KeyringFile load_keyring(const std::filesystem::path& path) {
    return parse_keyring(read_file(path));
}

std::optional<Key> unlock_tdlib_key(const KeyringFile& keyring, const Key& kek) {
    return unwrap_key(kek, keyring.wrapped_tdlib);
}

std::optional<UnlockedKeys> unlock_keyring(const KeyringFile& keyring, const Key& kek) {
    if (!keyring_has_fs(keyring)) {
        throw std::logic_error("keyring without a file system has no master key");
    }
    // Неверный пароль или keyfile распознаётся по wrapped_master.
    std::optional<Key> master = unwrap_key(kek, keyring.wrapped_master);
    if (!master) {
        return std::nullopt;
    }
    // KEK подошёл к master, но не к tdlib — это уже порча файла, а не неверный пароль.
    std::optional<Key> tdlib = unwrap_key(kek, keyring.wrapped_tdlib);
    if (!tdlib) {
        throw CorruptError("keyring: wrapped_tdlib authentication failed");
    }
    return UnlockedKeys{.master = *master, .tdlib_db = *tdlib};
}

}  // namespace telega::crypto
