#pragma once

// Внутренние помощники модуля crypto: приведение указателей для libsodium и общий
// формат параметров KDF в заголовке блоба индекса и в keyring.
// Вне src/crypto не используется.

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "common/bytes.h"
#include "crypto/keys.h"

namespace telega::crypto::detail {

// libsodium и zstd работают с unsigned char; std::byte с ним совместим по представлению.
inline unsigned char* uc(std::byte* p) noexcept { return reinterpret_cast<unsigned char*>(p); }
inline const unsigned char* uc(const std::byte* p) noexcept {
    return reinterpret_cast<const unsigned char*>(p);
}

// Размер блока opslimit(8) || memlimit(8) || salt(16) — так он лежит и в заголовке блоба,
// и в keyring. kdf_alg в обоих форматах хранится отдельно.
inline constexpr size_t kKdfLimitsSize = 32;

inline void put_kdf_limits(MutableByteSpan out, const KdfParams& kdf) {
    put_u64_le(out.subspan(0, 8), kdf.opslimit);
    put_u64_le(out.subspan(8, 8), kdf.memlimit);
    std::copy(kdf.salt.begin(), kdf.salt.end(), out.begin() + 16);
}

inline bool all_zero(ByteSpan data) noexcept {
    return std::all_of(data.begin(), data.end(), [](std::byte b) { return b == std::byte{0}; });
}

// Разобрать kdf_alg и блок limits. Неизвестный алгоритм или ненулевые параметры у keyfile —
// CorruptError (при kdf_alg = 0 opslimit, memlimit и salt должны быть нулевыми).
inline KdfParams parse_kdf(uint8_t alg, ByteSpan limits) {
    KdfParams kdf;
    if (alg == static_cast<uint8_t>(KdfAlg::keyfile)) {
        kdf.alg = KdfAlg::keyfile;
    } else if (alg == static_cast<uint8_t>(KdfAlg::argon2id13)) {
        kdf.alg = KdfAlg::argon2id13;
    } else {
        throw CorruptError("unknown kdf_alg");
    }
    kdf.opslimit = get_u64_le(limits.subspan(0, 8));
    kdf.memlimit = get_u64_le(limits.subspan(8, 8));
    std::copy_n(limits.begin() + 16, kdf.salt.size(), kdf.salt.begin());
    if (kdf.alg == KdfAlg::keyfile && !all_zero(limits.subspan(0, kKdfLimitsSize))) {
        throw CorruptError("kdf parameters must be zero for keyfile");
    }
    return kdf;
}

}  // namespace telega::crypto::detail
