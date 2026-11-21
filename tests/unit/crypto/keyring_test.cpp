// crypto/keyring: локальный keyring (SPEC 11).

#include "crypto/keyring.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sodium.h>

#include <algorithm>
#include <filesystem>
#include <string>

#include "common/files.h"
#include "test_util.h"

namespace telega::crypto {
namespace {

using test::CryptoTest;
using test::flip;
using test::random_bytes;

class KeyringTest : public CryptoTest {
  protected:
    // Быстрые параметры Argon2 для тестов.
    static KdfParams fast_params() {
        return passphrase_kdf_params(crypto_pwhash_OPSLIMIT_MIN, crypto_pwhash_MEMLIMIT_MIN);
    }

    FsUuid fs_uuid = random_fs_uuid();
    Key master = Key::random();
    Key tdlib = Key::random();
};

TEST_F(KeyringTest, ExactLayout) {
    const KdfParams kdf = passphrase_kdf_params(0x0102030405060708ULL, 0x1112131415161718ULL);
    const KeyringFile kr = make_keyring(fs_uuid, kdf, Key::random(), master, tdlib);
    const auto raw = serialize_keyring(kr);
    const ByteSpan s{raw};
    ASSERT_EQ(raw.size(), 256U);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(raw.data()), 8), "TGFSKEY1");
    EXPECT_EQ(raw[8], std::byte{1});
    EXPECT_EQ(raw[9], std::byte{1});  // argon2id13
    EXPECT_TRUE(std::all_of(raw.begin() + 10, raw.begin() + 16,
                            [](std::byte b) { return b == std::byte{0}; }));
    EXPECT_TRUE(std::equal(fs_uuid.begin(), fs_uuid.end(), raw.begin() + 16));
    EXPECT_EQ(get_u64_le(s.subspan(32, 8)), kdf.opslimit);
    EXPECT_EQ(get_u64_le(s.subspan(40, 8)), kdf.memlimit);
    EXPECT_TRUE(std::equal(kdf.salt.begin(), kdf.salt.end(), raw.begin() + 48));
    EXPECT_TRUE(std::equal(kr.wrapped_master.begin(), kr.wrapped_master.end(), raw.begin() + 64));
    EXPECT_TRUE(std::equal(kr.wrapped_tdlib.begin(), kr.wrapped_tdlib.end(), raw.begin() + 136));
    EXPECT_TRUE(
        std::all_of(raw.begin() + 208, raw.end(), [](std::byte b) { return b == std::byte{0}; }));
}

TEST_F(KeyringTest, SerializeParseRoundTrip) {
    for (const KdfParams& kdf : {keyfile_kdf_params(), passphrase_kdf_params(3, 1 << 20)}) {
        const KeyringFile kr = make_keyring(fs_uuid, kdf, Key::random(), master, tdlib);
        EXPECT_EQ(parse_keyring(serialize_keyring(kr)), kr);
    }
}

TEST_F(KeyringTest, ParseRejectsBadFormat) {
    const KeyringFile kr =
        make_keyring(fs_uuid, keyfile_kdf_params(), Key::random(), master, tdlib);
    const auto arr = serialize_keyring(kr);
    const Bytes raw(arr.begin(), arr.end());

    EXPECT_THROW(parse_keyring(ByteSpan{raw}.first(255)), CorruptError);
    Bytes longer = raw;
    longer.push_back(std::byte{0});
    EXPECT_THROW(parse_keyring(longer), CorruptError);
    EXPECT_THROW(parse_keyring({}), CorruptError);

    for (size_t i = 0; i < 8; ++i) {
        Bytes bad = raw;
        flip(bad, i);
        EXPECT_THROW(parse_keyring(bad), CorruptError) << "magic " << i;
    }
    for (const int v : {0, 2, 255}) {
        Bytes bad = raw;
        bad[8] = static_cast<std::byte>(v);
        EXPECT_THROW(parse_keyring(bad), CorruptError) << "version " << v;
    }
    for (const int v : {2, 255}) {
        Bytes bad = raw;
        bad[9] = static_cast<std::byte>(v);
        EXPECT_THROW(parse_keyring(bad), CorruptError) << "kdf_alg " << v;
    }
    for (size_t i = 10; i < 16; ++i) {
        Bytes bad = raw;
        bad[i] = std::byte{1};
        EXPECT_THROW(parse_keyring(bad), CorruptError) << "reserved " << i;
    }
    // keyfile: opslimit, memlimit и salt нулевые.
    for (size_t i = 32; i < 64; ++i) {
        Bytes bad = raw;
        bad[i] = std::byte{1};
        EXPECT_THROW(parse_keyring(bad), CorruptError) << "kdf param " << i;
    }
    for (size_t i = 208; i < kKeyringSize; ++i) {
        Bytes bad = raw;
        bad[i] = std::byte{1};
        EXPECT_THROW(parse_keyring(bad), CorruptError) << "padding " << i;
    }
}

TEST_F(KeyringTest, UnlockWithRightAndWrongKek) {
    const Key kek = Key::random();
    const KeyringFile kr = make_keyring(fs_uuid, keyfile_kdf_params(), kek, master, tdlib);
    const auto keys = unlock_keyring(kr, kek);
    if (!keys.has_value()) {
        FAIL() << "unlock failed";
    }
    EXPECT_TRUE(keys->master == master);
    EXPECT_TRUE(keys->tdlib_db == tdlib);
    EXPECT_FALSE(unlock_keyring(kr, Key::random()).has_value());
}

TEST_F(KeyringTest, PassphraseFlowThroughDisk) {
    const auto path = tmp / "keyring";
    {
        const KdfParams kdf = fast_params();
        const Key kek = derive_kek("open sesame", kdf);
        save_keyring(path, make_keyring(fs_uuid, kdf, kek, master, tdlib));
    }
    const KeyringFile loaded = load_keyring(path);
    EXPECT_EQ(loaded.fs_uuid, fs_uuid);
    EXPECT_EQ(loaded.kdf.alg, KdfAlg::argon2id13);

    const auto keys = unlock_keyring(loaded, derive_kek("open sesame", loaded.kdf));
    if (!keys.has_value()) {
        FAIL() << "unlock failed";
    }
    EXPECT_TRUE(keys->master == master);
    EXPECT_TRUE(keys->tdlib_db == tdlib);

    EXPECT_FALSE(unlock_keyring(loaded, derive_kek("open sesamE", loaded.kdf)).has_value());
}

TEST_F(KeyringTest, KeyfileFlow) {
    const Bytes raw = random_bytes(32);
    {
        const Fd fd = Fd::open(tmp / "keyfile", O_WRONLY | O_CREAT | O_TRUNC, 0600);
        pwrite_full(fd.get(), raw, 0);
    }
    const Key kek = read_keyfile(tmp / "keyfile");
    save_keyring(tmp / "keyring", make_keyring(fs_uuid, keyfile_kdf_params(), kek, master, tdlib));
    const auto keys = unlock_keyring(load_keyring(tmp / "keyring"), read_keyfile(tmp / "keyfile"));
    if (!keys.has_value()) {
        FAIL() << "unlock failed";
    }
    EXPECT_TRUE(keys->master == master);
}

TEST_F(KeyringTest, SaveIsAtomicWith0600) {
    const auto path = tmp / "keyring";
    const Key kek = Key::random();
    const KeyringFile first = make_keyring(fs_uuid, keyfile_kdf_params(), kek, master, tdlib);
    save_keyring(path, first);
    EXPECT_EQ(std::filesystem::file_size(path), kKeyringSize);
    EXPECT_EQ(std::filesystem::status(path).permissions() & std::filesystem::perms::all,
              std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
    EXPECT_FALSE(std::filesystem::exists(tmp / "keyring.tmp"));

    // Перезапись (passwd): новый KEK, тот же master.
    const Key kek2 = Key::random();
    const KeyringFile second = make_keyring(fs_uuid, keyfile_kdf_params(), kek2, master, tdlib);
    save_keyring(path, second);
    EXPECT_EQ(load_keyring(path), second);
    EXPECT_FALSE(unlock_keyring(load_keyring(path), kek).has_value());
    EXPECT_TRUE(unlock_keyring(load_keyring(path), kek2).has_value());
}

TEST_F(KeyringTest, CorruptedFileOnDisk) {
    const auto path = tmp / "keyring";
    const Key kek = Key::random();
    save_keyring(path, make_keyring(fs_uuid, keyfile_kdf_params(), kek, master, tdlib));
    const Bytes good = read_file(path);

    // Формат: magic и обрезка.
    Bytes bad = good;
    flip(bad, 0);
    write_file_atomic(path, bad);
    EXPECT_THROW(load_keyring(path), CorruptError);
    write_file_atomic(path, ByteSpan{good}.first(200));
    EXPECT_THROW(load_keyring(path), CorruptError);
    EXPECT_THROW(load_keyring(tmp / "missing"), SysError);

    // Порча wrapped_master неотличима от неверного пароля.
    for (const size_t pos : {size_t{64}, size_t{64 + 24}, size_t{64 + 71}}) {
        bad = good;
        flip(bad, pos);
        write_file_atomic(path, bad);
        EXPECT_FALSE(unlock_keyring(load_keyring(path), kek).has_value()) << pos;
    }
    // Порча fs_uuid не мешает разблокировке (fs_uuid сверяется с индексом выше), но видна.
    bad = good;
    flip(bad, 16);
    EXPECT_NE(parse_keyring(bad).fs_uuid, fs_uuid);

    // Порча wrapped_tdlib при верном KEK — порча файла.
    for (const size_t pos : {size_t{136}, size_t{136 + 24}, size_t{136 + 71}}) {
        bad = good;
        flip(bad, pos);
        EXPECT_THROW((void)unlock_keyring(parse_keyring(bad), kek), CorruptError) << pos;
        // С неверным KEK — по-прежнему просто «неверный пароль».
        EXPECT_FALSE(unlock_keyring(parse_keyring(bad), Key::random()).has_value());
    }
}

// --- keyring без ФС (SPEC 11, «Keyring до init») ---

TEST_F(KeyringTest, WithoutFsHasZeroFsFieldsAndRealTdlibKey) {
    const KdfParams kdf = fast_params();
    const Key kek = derive_kek("pw", kdf);
    const KeyringFile bare = make_keyring_without_fs(kdf, kek, tdlib);
    EXPECT_FALSE(keyring_has_fs(bare));
    EXPECT_EQ(bare.kdf, kdf);

    const auto raw = serialize_keyring(bare);
    ASSERT_EQ(raw.size(), 256U);
    // fs_uuid (16..32) и wrapped_master (64..136) нулевые, wrapped_tdlib (136..208) нет.
    EXPECT_TRUE(std::all_of(raw.begin() + 16, raw.begin() + 32,
                            [](std::byte b) { return b == std::byte{0}; }));
    EXPECT_TRUE(std::all_of(raw.begin() + 64, raw.begin() + 136,
                            [](std::byte b) { return b == std::byte{0}; }));
    EXPECT_FALSE(std::all_of(raw.begin() + 136, raw.begin() + 208,
                             [](std::byte b) { return b == std::byte{0}; }));
    EXPECT_EQ(parse_keyring(raw), bare);
}

TEST_F(KeyringTest, WithoutFsPasswordCheckGoesThroughWrappedTdlib) {
    const KdfParams kdf = fast_params();
    const Key kek = derive_kek("pw", kdf);
    const KeyringFile bare = make_keyring_without_fs(kdf, kek, tdlib);
    const auto ok = unlock_tdlib_key(bare, kek);
    if (!ok.has_value()) {
        FAIL() << "unlock failed";
    }
    EXPECT_TRUE(*ok == tdlib);
    EXPECT_FALSE(unlock_tdlib_key(bare, derive_kek("PW", kdf)).has_value());
    // master у такого keyring нет.
    EXPECT_THROW((void)unlock_keyring(bare, kek), std::logic_error);
    // Для keyring с ФС unlock_tdlib_key тоже работает.
    const KeyringFile full = make_keyring(fs_uuid, kdf, kek, master, tdlib);
    ASSERT_TRUE(unlock_tdlib_key(full, kek).has_value());
    EXPECT_FALSE(unlock_tdlib_key(full, Key::random()).has_value());
}

TEST_F(KeyringTest, AttachFsKeepsKdfAndTdlibKey) {
    const KdfParams kdf = fast_params();
    const Key kek = derive_kek("pw", kdf);
    const KeyringFile bare = make_keyring_without_fs(kdf, kek, tdlib);
    const KeyringFile full = attach_fs(bare, fs_uuid, kek, master);
    EXPECT_TRUE(keyring_has_fs(full));
    EXPECT_EQ(full.fs_uuid, fs_uuid);
    EXPECT_EQ(full.kdf, bare.kdf);
    EXPECT_EQ(full.wrapped_tdlib, bare.wrapped_tdlib);
    const auto keys = unlock_keyring(full, kek);
    if (!keys.has_value()) {
        FAIL() << "unlock failed";
    }
    EXPECT_TRUE(keys->master == master);
    EXPECT_TRUE(keys->tdlib_db == tdlib);
    EXPECT_THROW((void)attach_fs(bare, FsUuid{}, kek, master), std::invalid_argument);
}

TEST_F(KeyringTest, ParseRejectsHalfEmptyFsFields) {
    const Key kek = Key::random();
    const KeyringFile full = make_keyring(fs_uuid, keyfile_kdf_params(), kek, master, tdlib);
    KeyringFile no_uuid = full;
    no_uuid.fs_uuid = FsUuid{};
    EXPECT_THROW((void)parse_keyring(serialize_keyring(no_uuid)), CorruptError);
    KeyringFile no_master = full;
    no_master.wrapped_master = WrappedKey{};
    EXPECT_THROW((void)parse_keyring(serialize_keyring(no_master)), CorruptError);
}

TEST_F(KeyringTest, WithoutFsWorksWithKeyfileKdf) {
    const Key kek = Key::random();
    const KeyringFile bare = make_keyring_without_fs(keyfile_kdf_params(), kek, tdlib);
    save_keyring(tmp / "keyring", bare);
    const KeyringFile loaded = load_keyring(tmp / "keyring");
    EXPECT_FALSE(keyring_has_fs(loaded));
    ASSERT_TRUE(unlock_tdlib_key(loaded, kek).has_value());
}

}  // namespace
}  // namespace telega::crypto
