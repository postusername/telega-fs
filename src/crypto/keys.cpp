#include "crypto/keys.h"

#include <fcntl.h>
#include <sodium.h>

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>

#include "common/files.h"
#include "crypto/detail.h"

namespace telega::crypto {

using detail::uc;

namespace {

// Контекст crypto_kdf ровно из crypto_kdf_CONTEXTBYTES (8) символов, без завершающего нуля.
constexpr std::array<char, crypto_kdf_CONTEXTBYTES> kKdfContext{'T', 'G', 'F', 'S',
                                                                '_', 'K', 'D', 'F'};
constexpr uint64_t kSubkeyData = 1;
constexpr uint64_t kSubkeyIndex = 2;
constexpr uint64_t kSubkeyMac = 3;

static_assert(Key::kSize == crypto_kdf_KEYBYTES);
static_assert(Key::kSize == crypto_secretbox_KEYBYTES);
static_assert(Key::kSize == crypto_generichash_KEYBYTES);
static_assert(sizeof(KdfParams{}.salt) == crypto_pwhash_SALTBYTES);
static_assert(std::tuple_size_v<WrappedKey> ==
              crypto_secretbox_NONCEBYTES + crypto_secretbox_MACBYTES + Key::kSize);

// Временный буфер под секрет: обнуляется при выходе из области видимости.
struct SecretBuf {
    std::array<std::byte, Key::kSize> bytes{};
    SecretBuf() = default;
    SecretBuf(const SecretBuf&) = delete;
    SecretBuf& operator=(const SecretBuf&) = delete;
    ~SecretBuf() { sodium_memzero(bytes.data(), bytes.size()); }
};

Key derive_one(const Key& master, uint64_t id) {
    SecretBuf out;
    if (crypto_kdf_derive_from_key(uc(out.bytes.data()), out.bytes.size(), id, kKdfContext.data(),
                                   uc(master.data())) != 0) {
        throw std::runtime_error("crypto_kdf_derive_from_key failed");
    }
    return Key{out.bytes};
}

}  // namespace

void init() {
    if (sodium_init() < 0) {
        throw std::runtime_error("sodium_init failed");
    }
}

Key::Key(ByteSpan raw) {
    if (raw.size() != kSize) {
        throw std::invalid_argument("key must be exactly 32 bytes");
    }
    std::copy(raw.begin(), raw.end(), bytes_.begin());
}

Key::~Key() { sodium_memzero(bytes_.data(), bytes_.size()); }

Key Key::random() {
    SecretBuf buf;
    randombytes_buf(buf.bytes.data(), buf.bytes.size());
    return Key{buf.bytes};
}

bool Key::operator==(const Key& other) const noexcept {
    return sodium_memcmp(bytes_.data(), other.bytes_.data(), kSize) == 0;
}

SubKeys derive_subkeys(const Key& master) {
    return SubKeys{.data = derive_one(master, kSubkeyData),
                   .index = derive_one(master, kSubkeyIndex),
                   .mac = derive_one(master, kSubkeyMac)};
}

FsUuid random_fs_uuid() {
    FsUuid uuid{};
    randombytes_buf(uuid.data(), uuid.size());
    return uuid;
}

KdfParams passphrase_kdf_params() {
    return passphrase_kdf_params(crypto_pwhash_OPSLIMIT_MODERATE, crypto_pwhash_MEMLIMIT_MODERATE);
}

KdfParams passphrase_kdf_params(uint64_t opslimit, uint64_t memlimit) {
    KdfParams params;
    params.alg = KdfAlg::argon2id13;
    params.opslimit = opslimit;
    params.memlimit = memlimit;
    randombytes_buf(params.salt.data(), params.salt.size());
    return params;
}

KdfParams keyfile_kdf_params() { return KdfParams{}; }

Key derive_kek(std::string_view passphrase, const KdfParams& params) {
    if (params.alg != KdfAlg::argon2id13) {
        throw std::invalid_argument("derive_kek: kdf_alg is not argon2id13");
    }
    // Параметры приходят и из заголовка блоба, поэтому границы проверяются явно,
    // а не только внутри libsodium.
    if (params.opslimit < crypto_pwhash_OPSLIMIT_MIN ||
        params.opslimit > crypto_pwhash_OPSLIMIT_MAX ||
        params.memlimit < crypto_pwhash_MEMLIMIT_MIN ||
        params.memlimit > crypto_pwhash_MEMLIMIT_MAX ||
        params.memlimit > std::numeric_limits<size_t>::max()) {
        throw std::invalid_argument("derive_kek: argon2id parameters out of range");
    }
    SecretBuf out;
    if (crypto_pwhash(uc(out.bytes.data()), out.bytes.size(), passphrase.data(), passphrase.size(),
                      uc(params.salt.data()), params.opslimit, static_cast<size_t>(params.memlimit),
                      crypto_pwhash_ALG_ARGON2ID13) != 0) {
        throw std::runtime_error("argon2id failed (out of memory?)");
    }
    return Key{out.bytes};
}

Key read_keyfile(const std::filesystem::path& path) {
    const Fd fd = Fd::open(path, O_RDONLY);
    // На байт больше, чтобы отличить слишком длинный файл от файла ровно в 32 байта.
    std::array<std::byte, Key::kSize + 1> buf{};
    const size_t n = pread_full(fd.get(), buf, 0);
    if (n != Key::kSize) {
        sodium_memzero(buf.data(), buf.size());
        throw std::runtime_error("keyfile must be exactly 32 bytes");
    }
    const Key key{ByteSpan{buf}.first(Key::kSize)};
    sodium_memzero(buf.data(), buf.size());
    return key;
}

WrappedKey wrap_key(const Key& kek, const Key& key) {
    WrappedKey out{};
    std::byte* nonce = out.data();
    randombytes_buf(nonce, crypto_secretbox_NONCEBYTES);
    // crypto_secretbox_easy пишет mac(16) || ciphertext(32) — как раз раскладка из SPEC 11.
    if (crypto_secretbox_easy(uc(out.data() + crypto_secretbox_NONCEBYTES), uc(key.data()),
                              Key::kSize, uc(nonce), uc(kek.data())) != 0) {
        throw std::runtime_error("crypto_secretbox_easy failed");
    }
    return out;
}

std::optional<Key> unwrap_key(const Key& kek, const WrappedKey& wrapped) {
    SecretBuf out;
    if (crypto_secretbox_open_easy(
            uc(out.bytes.data()), uc(wrapped.data() + crypto_secretbox_NONCEBYTES),
            crypto_secretbox_MACBYTES + Key::kSize, uc(wrapped.data()), uc(kek.data())) != 0) {
        return std::nullopt;
    }
    return Key{out.bytes};
}

Hash32 blake2b256(ByteSpan data) {
    Hash32 out{};
    crypto_generichash(uc(out.data()), out.size(), uc(data.data()), data.size(), nullptr, 0);
    return out;
}

Hash32 blake2b256_keyed(const Key& key, ByteSpan data) {
    Hash32 out{};
    crypto_generichash(uc(out.data()), out.size(), uc(data.data()), data.size(), uc(key.data()),
                       Key::kSize);
    return out;
}

Hash32 content_mac(const Key& k_mac, int fd, uint64_t size) {
    crypto_generichash_state state;
    crypto_generichash_init(&state, uc(k_mac.data()), Key::kSize, std::tuple_size_v<Hash32>);
    constexpr size_t kBufSize = size_t{256} * 1024;
    Bytes buf(kBufSize);
    uint64_t pos = 0;
    while (pos < size) {
        const auto want = static_cast<size_t>(std::min<uint64_t>(kBufSize, size - pos));
        const MutableByteSpan part{buf.data(), want};
        const size_t got = pread_full(fd, part, pos);
        // За концом файла — нули (разреженный или ещё не дописанный staging-файл).
        std::fill(part.begin() + static_cast<std::ptrdiff_t>(got), part.end(), std::byte{0});
        crypto_generichash_update(&state, uc(part.data()), part.size());
        pos += want;
    }
    Hash32 out{};
    crypto_generichash_final(&state, uc(out.data()), out.size());
    sodium_memzero(&state, sizeof(state));
    return out;
}

}  // namespace telega::crypto
