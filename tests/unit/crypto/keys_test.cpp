// crypto/keys: ключи, подключи, KDF, keyfile, обёртка ключей, BLAKE2b, content_mac (SPEC 11, 6.2).

#include "crypto/keys.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sodium.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>

#include "common/files.h"
#include "test_util.h"

namespace telega::crypto {
namespace {

using test::CryptoTest;
using test::flip;
using test::random_bytes;

using KeysTest = CryptoTest;

Hash32 hash_from_hex(const std::string& hex) {
    Hash32 out{};
    size_t bin_len = 0;
    if (sodium_hex2bin(reinterpret_cast<unsigned char*>(out.data()), out.size(), hex.data(),
                       hex.size(), nullptr, &bin_len, nullptr) != 0 ||
        bin_len != out.size()) {
        throw std::invalid_argument("bad hex");
    }
    return out;
}

void write_raw(const std::filesystem::path& path, ByteSpan data) {
    const Fd fd = Fd::open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    pwrite_full(fd.get(), data, 0);
}

TEST_F(KeysTest, KeyRequiresExactly32Bytes) {
    EXPECT_THROW(Key{Bytes(31)}, std::invalid_argument);
    EXPECT_THROW(Key{Bytes(33)}, std::invalid_argument);
    EXPECT_THROW(Key{Bytes{}}, std::invalid_argument);
    const Bytes raw = random_bytes(32);
    const Key key{raw};
    EXPECT_TRUE(std::equal(raw.begin(), raw.end(), key.span().begin(), key.span().end()));
}

TEST_F(KeysTest, RandomKeysDifferAndCompare) {
    const Key a = Key::random();
    const Key b = Key::random();
    EXPECT_FALSE(a == b);
    const Key a_copy = a;  // NOLINT(performance-unnecessary-copy-initialization): проверяем копию
    EXPECT_TRUE(a_copy == a);
    EXPECT_FALSE(Key{} == a);
}

TEST_F(KeysTest, SubkeysMatchLibsodiumKdfWithSpecContextAndIds) {
    const Key master = Key::random();
    const SubKeys sub = derive_subkeys(master);

    // Ожидаемые значения — напрямую из libsodium: контекст "TGFS_KDF", id 1/2/3 (SPEC 11).
    const auto expect = [&](uint64_t id) {
        std::array<std::byte, 32> out{};
        crypto_kdf_derive_from_key(reinterpret_cast<unsigned char*>(out.data()), out.size(), id,
                                   "TGFS_KDF",
                                   reinterpret_cast<const unsigned char*>(master.data()));
        return Key{out};
    };
    EXPECT_TRUE(sub.data == expect(1));
    EXPECT_TRUE(sub.index == expect(2));
    EXPECT_TRUE(sub.mac == expect(3));
    EXPECT_FALSE(sub.data == sub.index);
    EXPECT_FALSE(sub.index == sub.mac);
    EXPECT_FALSE(sub.data == master);

    // Детерминированность.
    const SubKeys again = derive_subkeys(master);
    EXPECT_TRUE(again.data == sub.data);
    EXPECT_TRUE(again.index == sub.index);
    EXPECT_TRUE(again.mac == sub.mac);
}

TEST_F(KeysTest, RandomFsUuid) { EXPECT_NE(random_fs_uuid(), random_fs_uuid()); }

TEST_F(KeysTest, KdfParams) {
    const KdfParams kf = keyfile_kdf_params();
    EXPECT_EQ(kf.alg, KdfAlg::keyfile);
    EXPECT_EQ(kf.opslimit, 0U);
    EXPECT_EQ(kf.memlimit, 0U);
    EXPECT_EQ(kf.salt, (std::array<std::byte, 16>{}));

    const KdfParams pw = passphrase_kdf_params();
    EXPECT_EQ(pw.alg, KdfAlg::argon2id13);
    EXPECT_EQ(pw.opslimit, crypto_pwhash_OPSLIMIT_MODERATE);
    EXPECT_EQ(pw.memlimit, crypto_pwhash_MEMLIMIT_MODERATE);
    EXPECT_NE(pw.salt, passphrase_kdf_params().salt);

    const KdfParams small = passphrase_kdf_params(5, 12345);
    EXPECT_EQ(small.alg, KdfAlg::argon2id13);
    EXPECT_EQ(small.opslimit, 5U);
    EXPECT_EQ(small.memlimit, 12345U);
}

TEST_F(KeysTest, DeriveKekIsArgon2idAndDependsOnInputs) {
    const KdfParams params =
        passphrase_kdf_params(crypto_pwhash_OPSLIMIT_MIN, crypto_pwhash_MEMLIMIT_MIN);
    const Key kek = derive_kek("correct horse", params);
    EXPECT_TRUE(derive_kek("correct horse", params) == kek);

    std::array<std::byte, 32> expected{};
    const std::string pass = "correct horse";
    ASSERT_EQ(crypto_pwhash(reinterpret_cast<unsigned char*>(expected.data()), expected.size(),
                            pass.data(), pass.size(),
                            reinterpret_cast<const unsigned char*>(params.salt.data()),
                            params.opslimit, params.memlimit, crypto_pwhash_ALG_ARGON2ID13),
              0);
    EXPECT_TRUE(kek == Key{expected});

    EXPECT_FALSE(derive_kek("correct horsf", params) == kek);
    KdfParams other_salt = params;
    other_salt.salt[0] ^= std::byte{1};
    EXPECT_FALSE(derive_kek("correct horse", other_salt) == kek);
    KdfParams other_ops = params;
    other_ops.opslimit += 1;
    EXPECT_FALSE(derive_kek("correct horse", other_ops) == kek);
}

TEST_F(KeysTest, DeriveKekRejectsBadParams) {
    EXPECT_THROW(derive_kek("x", keyfile_kdf_params()), std::invalid_argument);
    EXPECT_THROW(derive_kek("x", passphrase_kdf_params(0, crypto_pwhash_MEMLIMIT_MIN)),
                 std::invalid_argument);
    EXPECT_THROW(derive_kek("x", passphrase_kdf_params(crypto_pwhash_OPSLIMIT_MIN, 1)),
                 std::invalid_argument);
}

TEST_F(KeysTest, ReadKeyfileExactly32Bytes) {
    const Bytes raw = random_bytes(32);
    write_raw(tmp / "key", raw);
    EXPECT_TRUE(read_keyfile(tmp / "key") == Key{raw});
}

TEST_F(KeysTest, ReadKeyfileWrongSizeFailsWithoutLeakingContent) {
    for (const size_t size : {size_t{0}, size_t{1}, size_t{31}, size_t{33}, size_t{64}}) {
        SCOPED_TRACE(size);
        const std::string content(size, 'Q');
        write_raw(tmp / "key", as_bytes(content));
        try {
            (void)read_keyfile(tmp / "key");
            ADD_FAILURE() << "expected an exception";
        } catch (const std::runtime_error& e) {
            EXPECT_EQ(std::string(e.what()).find("QQ"), std::string::npos);
        }
    }
    EXPECT_THROW((void)read_keyfile(tmp / "missing"), SysError);
}

TEST_F(KeysTest, WrapUnwrapRoundTrip) {
    const Key kek = Key::random();
    const Key key = Key::random();
    const WrappedKey wrapped = wrap_key(kek, key);
    const std::optional<Key> back = unwrap_key(kek, wrapped);
    EXPECT_TRUE(back == std::optional<Key>{key});

    // Случайный nonce: две обёртки одного ключа различны, обе разворачиваются.
    const WrappedKey wrapped2 = wrap_key(kek, key);
    EXPECT_NE(wrapped, wrapped2);
    EXPECT_TRUE(unwrap_key(kek, wrapped2) == std::optional<Key>{key});

    // Раскладка nonce(24) || mac(16) || key(32): проверяем через crypto_secretbox_open_easy.
    std::array<unsigned char, 32> plain{};
    ASSERT_EQ(crypto_secretbox_open_easy(
                  plain.data(), reinterpret_cast<const unsigned char*>(wrapped.data() + 24), 48,
                  reinterpret_cast<const unsigned char*>(wrapped.data()),
                  reinterpret_cast<const unsigned char*>(kek.data())),
              0);
    EXPECT_EQ(std::memcmp(plain.data(), key.data(), 32), 0);
}

TEST_F(KeysTest, UnwrapWithWrongKekOrCorruptionFails) {
    const Key kek = Key::random();
    const Key key = Key::random();
    const WrappedKey wrapped = wrap_key(kek, key);
    EXPECT_FALSE(unwrap_key(Key::random(), wrapped).has_value());
    // Любой байт: nonce, mac, шифртекст.
    for (size_t i = 0; i < wrapped.size(); ++i) {
        SCOPED_TRACE(i);
        WrappedKey bad = wrapped;
        bad.at(i) ^= std::byte{0x80};
        EXPECT_FALSE(unwrap_key(kek, bad).has_value());
    }
}

TEST_F(KeysTest, Blake2b256KnownVectors) {
    EXPECT_EQ(blake2b256({}),
              hash_from_hex("0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"));
    EXPECT_EQ(blake2b256(as_bytes(std::string("abc"))),
              hash_from_hex("bddd813c634239723171ef3fee98579b94964e3bb1cb3e427262c8c068d52319"));
}

TEST_F(KeysTest, Blake2b256Keyed) {
    const Bytes data = random_bytes(1000);
    const Key k1 = Key::random();
    const Key k2 = Key::random();
    const Hash32 h1 = blake2b256_keyed(k1, data);
    EXPECT_EQ(h1, blake2b256_keyed(k1, data));
    EXPECT_NE(h1, blake2b256_keyed(k2, data));
    EXPECT_NE(h1, blake2b256(data));

    Hash32 expected{};
    crypto_generichash(reinterpret_cast<unsigned char*>(expected.data()), expected.size(),
                       reinterpret_cast<const unsigned char*>(data.data()), data.size(),
                       reinterpret_cast<const unsigned char*>(k1.data()), Key::kSize);
    EXPECT_EQ(h1, expected);
}

TEST_F(KeysTest, ContentMacMatchesKeyedHashOfFile) {
    const Key k = Key::random();
    // Больше внутреннего буфера, чтобы пройти несколько итераций чтения.
    const Bytes data = random_bytes(700001);
    write_raw(tmp / "f", data);
    const Fd fd = Fd::open(tmp / "f", O_RDONLY);

    EXPECT_EQ(content_mac(k, fd.get(), data.size()), blake2b256_keyed(k, data));
    EXPECT_EQ(content_mac(k, fd.get(), 0), blake2b256_keyed(k, {}));
    // Префикс файла.
    EXPECT_EQ(content_mac(k, fd.get(), 1234), blake2b256_keyed(k, ByteSpan{data}.first(1234)));
}

TEST_F(KeysTest, ContentMacPadsShortFileWithZeros) {
    const Key k = Key::random();
    const Bytes data = random_bytes(5000);
    write_raw(tmp / "f", data);
    const Fd fd = Fd::open(tmp / "f", O_RDONLY);
    Bytes padded = data;
    padded.resize(600000);
    EXPECT_EQ(content_mac(k, fd.get(), padded.size()), blake2b256_keyed(k, padded));

    // Разреженный файл: дыра в середине читается нулями.
    const Fd sparse = Fd::open(tmp / "s", O_RDWR | O_CREAT, 0600);
    pwrite_full(sparse.get(), data, 300000);
    Bytes expected(300000 + data.size() + 100);
    std::copy(data.begin(), data.end(), expected.begin() + 300000);
    EXPECT_EQ(content_mac(k, sparse.get(), expected.size()), blake2b256_keyed(k, expected));
}

TEST_F(KeysTest, ContentMacDependsOnContent) {
    const Key k = Key::random();
    Bytes data = random_bytes(100000);
    write_raw(tmp / "a", data);
    flip(data, 77777);
    write_raw(tmp / "b", data);
    const Fd a = Fd::open(tmp / "a", O_RDONLY);
    const Fd b = Fd::open(tmp / "b", O_RDONLY);
    EXPECT_NE(content_mac(k, a.get(), data.size()), content_mac(k, b.get(), data.size()));
}

}  // namespace
}  // namespace telega::crypto
