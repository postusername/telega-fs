// Keyring до init: login создаёт keyring без ФС, init дописывает ФС, монтирование на
// новой машине разворачивает master_key из головы канала и сохраняет его.

#include <gtest/gtest.h>
#include <sodium.h>

#include <string>

#include "commit_test_util.h"
#include "crypto/keyring.h"

namespace telega::meta {
namespace {

using namespace std::chrono_literals;

using store::FaultAction;
using store::StoreOp;
using test::CommitEnv;
using test::MaskedHeadStore;

class KeyringFlow : public CommitEnv {
  protected:
    static crypto::KdfParams fast_kdf() {
        return crypto::passphrase_kdf_params(crypto_pwhash_OPSLIMIT_MIN,
                                             crypto_pwhash_MEMLIMIT_MIN);
    }

    // Keyring без ФС, как после login: возвращает tdlib_db_key.
    static crypto::Key make_bare(const FsPaths& paths, const std::string& passphrase) {
        ensure_fs_dirs(paths);
        const crypto::KdfParams kdf = fast_kdf();
        const crypto::Key kek = crypto::derive_kek(passphrase, kdf);
        const crypto::Key tdlib = crypto::Key::random();
        crypto::save_keyring(paths.keyring(), crypto::make_keyring_without_fs(kdf, kek, tdlib));
        return tdlib;
    }

    static InitOptions init_with(const InitOptions& base, const std::string& passphrase) {
        InitOptions o = base;
        o.key = KeySource{passphrase, {}};
        o.kdf = fast_kdf();
        return o;
    }

    [[nodiscard]] MountOptions mount_with(const std::string& passphrase) const {
        MountOptions o = mount_options();
        o.key = KeySource{passphrase, {}};
        return o;
    }

    static Bytes file_bytes(const FsPaths& paths) { return read_file(paths.keyring()); }
};

TEST_F(KeyringFlow, InitAppendsFsToBareKeyring) {
    const crypto::Key tdlib = make_bare(a_, "pw");
    const crypto::KeyringFile before = crypto::load_keyring(a_.keyring());

    const InitResult r = init_fs(*store_, a_, init_with(init_options(), "pw"));

    const crypto::KeyringFile after = crypto::load_keyring(a_.keyring());
    ASSERT_TRUE(crypto::keyring_has_fs(after));
    EXPECT_EQ(after.fs_uuid, r.fs_uuid);
    EXPECT_EQ(after.kdf, before.kdf);  // тот же KDF, та же соль
    EXPECT_EQ(after.wrapped_tdlib, before.wrapped_tdlib);
    const crypto::Key kek = crypto::derive_kek("pw", after.kdf);
    const auto keys = crypto::unlock_keyring(after, kek);
    if (!keys.has_value()) {
        FAIL() << "unlock failed";
    }
    EXPECT_TRUE(keys->tdlib_db == tdlib);
    // Заголовок блоба несёт тот же KDF и wrapped_master, что keyring.
    const auto h = head_header();
    EXPECT_EQ(h.fs_uuid, after.fs_uuid);
    EXPECT_EQ(h.kdf, after.kdf);
    EXPECT_EQ(h.wrapped_master, after.wrapped_master);

    // И такая ФС монтируется тем же паролем.
    auto fs = mount(a_, mount_with("pw"));
    EXPECT_TRUE(fs->tdlib_db_key() == tdlib);
    fs->unmount();
}

TEST_F(KeyringFlow, InitWithBareKeyringAndKeyfile) {
    ensure_fs_dirs(a_);
    const crypto::Key tdlib = crypto::Key::random();
    crypto::save_keyring(a_.keyring(),
                         crypto::make_keyring_without_fs(crypto::keyfile_kdf_params(),
                                                         crypto::read_keyfile(keyfile_), tdlib));
    init();  // keyfile из окружения теста
    auto fs = mount(a_);
    EXPECT_TRUE(fs->tdlib_db_key() == tdlib);
    fs->unmount();
}

TEST_F(KeyringFlow, InitWithWrongPasswordIsRefusedAndKeyringStaysBare) {
    make_bare(a_, "right");
    const Bytes before = file_bytes(a_);
    EXPECT_THROW(init_fs(*store_, a_, init_with(init_options(), "wrong")), InitError);
    EXPECT_EQ(file_bytes(a_), before);
    EXPECT_FALSE(store_->read_head().has_value());
    EXPECT_FALSE(std::filesystem::exists(a_.index_db()));
}

TEST_F(KeyringFlow, InitRefusesKeyringWithFs) {
    make_bare(a_, "pw");
    init_fs(*store_, a_, init_with(init_options(), "pw"));
    // Второй init в тот же профиль (даже в другой канал) — отказ: keyring уже с ФС.
    store::LocalDirStore other{tmp_ / "other-channel"};
    const Bytes before = file_bytes(a_);
    EXPECT_THROW(init_fs(other, a_, init_with(init_options(), "pw")), InitError);
    EXPECT_EQ(file_bytes(a_), before);
}

TEST_F(KeyringFlow, FailedInitRestoresBareKeyring) {
    make_bare(a_, "pw");
    const Bytes before = file_bytes(a_);
    fault_next(StoreOp::put_index, FaultAction::permanent);
    EXPECT_THROW(init_fs(*store_, a_, init_with(init_options(), "pw")), CommitError);
    EXPECT_EQ(file_bytes(a_), before);  // не удалён и не с ФС
    EXPECT_FALSE(crypto::keyring_has_fs(crypto::load_keyring(a_.keyring())));
    EXPECT_FALSE(std::filesystem::exists(a_.index_db()));
    EXPECT_FALSE(std::filesystem::exists(a_.state_db()));

    // После сбоя init можно повторить.
    reopen_store();
    init_fs(*store_, a_, init_with(init_options(), "pw"));
    EXPECT_TRUE(crypto::keyring_has_fs(crypto::load_keyring(a_.keyring())));
}

// --- прерванный init ---

// init падает между записью keyring с ФС и pin: keyring уже с ФС, головы в канале нет.
// Повтор (воспроизведение ревьюера) раньше отказывал навсегда.
TEST_F(KeyringFlow, InterruptedInitCanBeRepeated) {
    const crypto::Key tdlib = make_bare(a_, "pw");
    const crypto::KeyringFile bare = crypto::load_keyring(a_.keyring());
    InitOptions o = init_with(init_options(), "pw");
    o.stale_head_window = 40ms;
    o.stale_head_period = 10ms;

    fault_next(StoreOp::put_index, FaultAction::crash_before);
    EXPECT_THROW(init_fs(*store_, a_, o), store::CrashInjected);
    const crypto::KeyringFile stuck = crypto::load_keyring(a_.keyring());
    ASSERT_TRUE(crypto::keyring_has_fs(stuck));  // ровно то, что блокировало профиль
    EXPECT_FALSE(store_->read_head().has_value());
    reopen_store();

    MaskedHeadStore counted{*store_};
    const InitResult r = init_fs(counted, a_, o);
    EXPECT_GE(counted.head_reads(), 3);  // голова проверялась всё окно, а не один раз
    const crypto::KeyringFile after = crypto::load_keyring(a_.keyring());
    ASSERT_TRUE(crypto::keyring_has_fs(after));
    EXPECT_EQ(after.fs_uuid, r.fs_uuid);
    EXPECT_NE(after.fs_uuid, stuck.fs_uuid);  // ФС сгенерирована заново
    EXPECT_NE(after.wrapped_master, stuck.wrapped_master);
    EXPECT_EQ(after.wrapped_tdlib, bare.wrapped_tdlib);  // ключ TDLib сохранён
    EXPECT_EQ(after.kdf, bare.kdf);
    EXPECT_EQ(head_header().generation, 1U);
    EXPECT_EQ(head_header().fs_uuid, r.fs_uuid);

    auto fs = mount(a_, mount_with("pw"));
    EXPECT_TRUE(fs->tdlib_db_key() == tdlib);
    EXPECT_EQ(state_int(*fs, state_key::base_generation), 1);
    fs->unmount();
}

TEST_F(KeyringFlow, RepeatedInitReplacesDatabasesOfTheInterruptedOne) {
    make_bare(a_, "pw");
    const InitOptions o = init_with(init_options(), "pw");
    // Упасть позже: базы уже созданы, base_generation = 0.
    // Первое чтение головы — проверка пустоты канала (до keyring). Второе — CAS коммита.
    store_->set_faults({store::FaultRule{.op = StoreOp::read_head,
                                         .nth = store_->call_count(StoreOp::read_head) + 2,
                                         .action = FaultAction::crash_before}});
    EXPECT_THROW(init_fs(*store_, a_, o), store::CrashInjected);
    ASSERT_TRUE(std::filesystem::exists(a_.index_db()));
    ASSERT_TRUE(std::filesystem::exists(a_.state_db()));
    const FsUuid old_uuid = crypto::load_keyring(a_.keyring()).fs_uuid;
    reopen_store();

    const InitResult r = init_fs(*store_, a_, o);
    EXPECT_NE(r.fs_uuid, old_uuid);
    auto fs = mount(a_, mount_with("pw"));
    EXPECT_EQ(fs->fs_uuid(), r.fs_uuid);  // индекс новый, а не от прерванного init
    fs->unmount();
}

TEST_F(KeyringFlow, RepeatedInitChecksThePasswordByWrappedTdlib) {
    make_bare(a_, "pw");
    fault_next(StoreOp::put_index, FaultAction::crash_before);
    EXPECT_THROW(init_fs(*store_, a_, init_with(init_options(), "pw")), store::CrashInjected);
    reopen_store();
    const Bytes before = file_bytes(a_);
    EXPECT_THROW(init_fs(*store_, a_, init_with(init_options(), "wrong")), InitError);
    EXPECT_EQ(file_bytes(a_), before);  // keyring не тронут
}

// Если прерванный init дошёл до pin, а кэш прячет голову, повтор не должен затереть ФС:
// пустой ответ принимается только после всего окна.
TEST_F(KeyringFlow, RepeatedInitDoesNotTrustAHeadHiddenByTheCache) {
    make_bare(a_, "pw");
    InitOptions o = init_with(init_options(), "pw");
    o.stale_head_window = 200ms;
    o.stale_head_period = 10ms;
    fault_next(StoreOp::set_head, FaultAction::crash_after);  // pin состоялся, шаг 8 — нет
    EXPECT_THROW(init_fs(*store_, a_, o), store::CrashInjected);
    const auto pinned = test::must(store_->read_head());
    const Bytes keyring_before = file_bytes(a_);
    reopen_store();

    MaskedHeadStore hidden{*store_};
    hidden.mask_next(3);  // три чтения кэш отдаёт «нет головы»
    try {
        init_fs(hidden, a_, o);
        FAIL() << "init must see the pinned head";
    } catch (const InitError& e) {
        EXPECT_NE(std::string(e.what()).find("mount"), std::string::npos);  // подсказка
    }
    EXPECT_GE(hidden.head_reads(), 4);
    EXPECT_EQ(file_bytes(a_), keyring_before);  // ключи прерванного init целы
    EXPECT_EQ(test::must(store_->read_head()).msg_id, pinned.msg_id);

    // И эта ФС доделывается монтированием.
    auto fs = mount(a_, mount_with("pw"));
    EXPECT_TRUE(fs->report().resumed_commit);
    fs->unmount();
}

TEST_F(KeyringFlow, MountWithBareKeyringUnwrapsMasterFromHead) {
    // Машина A: init с паролем. Машина B: после login (keyring без ФС), другая соль.
    init_fs(*store_, a_, init_with(init_options(), "pw"));
    const crypto::Key tdlib_b = make_bare(b_, "pw");
    const crypto::KeyringFile bare = crypto::load_keyring(b_.keyring());
    ASSERT_NE(bare.kdf.salt, crypto::load_keyring(a_.keyring()).kdf.salt);

    {
        auto fb = mount(b_, mount_with("pw"));
        EXPECT_EQ(fb->report().situation, MountSituation::built_fresh);
        EXPECT_TRUE(fb->tdlib_db_key() == tdlib_b);  // свой ключ TDLib, не чужой
        fb->unmount();
    }

    // keyring сохранён: с ФС, свой KDF и свой wrapped_tdlib, master тот же, что у машины A.
    const crypto::KeyringFile saved = crypto::load_keyring(b_.keyring());
    const crypto::KeyringFile a = crypto::load_keyring(a_.keyring());
    ASSERT_TRUE(crypto::keyring_has_fs(saved));
    EXPECT_EQ(saved.fs_uuid, a.fs_uuid);
    EXPECT_EQ(saved.kdf, bare.kdf);
    EXPECT_EQ(saved.wrapped_tdlib, bare.wrapped_tdlib);
    const auto kb = crypto::unlock_keyring(saved, crypto::derive_kek("pw", saved.kdf));
    const auto ka = crypto::unlock_keyring(a, crypto::derive_kek("pw", a.kdf));
    if (!kb.has_value() || !ka.has_value()) {
        FAIL() << "unlock failed";
    }
    EXPECT_TRUE(kb->master == ka->master);
    EXPECT_TRUE(kb->tdlib_db == tdlib_b);

    // Повторное монтирование идёт уже по keyring с ФС и новой дельте.
    auto again = mount(b_, mount_with("pw"));
    EXPECT_EQ(again->report().situation, MountSituation::same_generation);
    mkfile(*again, "from-b");
    again->committer().commit_now();
    again->unmount();
    // Блобы, залитые машиной B, несут KDF и wrapped_master из её keyring и читаются с A.
    auto fa = mount(a_, mount_with("pw"));
    EXPECT_NE(fa->report().situation, MountSituation::same_generation);
    fa->unmount();
}

TEST_F(KeyringFlow, MountWithBareKeyringNeedsPasswordFittingBoth) {
    init_fs(*store_, a_, init_with(init_options(), "head-pw"));
    make_bare(b_, "keyring-pw");
    const Bytes before = file_bytes(b_);

    // Подходит к голове, но не к keyring.
    try {
        mount(b_, mount_with("head-pw"));
        FAIL() << "mount must fail";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::wrong_password);
        EXPECT_EQ(std::string(e.what()), "wrong passphrase or keyfile");
    }
    // Подходит к keyring, но не к голове.
    try {
        mount(b_, mount_with("keyring-pw"));
        FAIL() << "mount must fail";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::wrong_password);
    }
    EXPECT_EQ(file_bytes(b_), before);  // keyring остался без ФС
    EXPECT_FALSE(std::filesystem::exists(b_.index_db()));
}

// --- --ro и порядок сохранения keyring ---

TEST_F(KeyringFlow, ReadOnlyMountKeepsBareKeyringUntouched) {
    init_fs(*store_, a_, init_with(init_options(), "pw"));
    const crypto::Key tdlib_b = make_bare(b_, "pw");
    const Bytes before = file_bytes(b_);
    {
        MountOptions o = mount_with("pw");
        o.read_only = true;
        auto fb = mount(b_, o);
        EXPECT_EQ(fb->report().situation, MountSituation::built_fresh);
        EXPECT_TRUE(fb->tdlib_db_key() == tdlib_b);
        fb->unmount();
    }
    EXPECT_EQ(file_bytes(b_), before);  // master_key только в памяти
    EXPECT_FALSE(crypto::keyring_has_fs(crypto::load_keyring(b_.keyring())));

    // Обычное монтирование после этого keyring сохраняет.
    auto fb = mount(b_, mount_with("pw"));
    fb->unmount();
    EXPECT_TRUE(crypto::keyring_has_fs(crypto::load_keyring(b_.keyring())));
}

TEST_F(KeyringFlow, ReadOnlyMountWithoutKeyringDoesNotCreateOne) {
    init();  // keyfile
    {
        auto fb = mount_ro(b_);
        EXPECT_EQ(fb->report().situation, MountSituation::built_fresh);
        fb->unmount();
    }
    EXPECT_FALSE(std::filesystem::exists(b_.keyring()));
    auto fb = mount(b_);
    fb->unmount();
    EXPECT_TRUE(crypto::keyring_has_fs(crypto::load_keyring(b_.keyring())));
}

// Локальные файлы другой ФС: монтирование откажет, и keyring не должен привязаться к ФС канала.
class KeyringOrder : public KeyringFlow {
  protected:
    // Профиль b_: keyring без ФС и файлы ФС из другого канала (машина c_).
    void make_profile_with_foreign_files() {
        store::LocalDirStore other{tmp_ / "other-channel"};
        init_fs(other, c_, init_with(init_options(), "pw"));
        make_bare(b_, "pw");
        std::filesystem::copy_file(c_.index_db(), b_.index_db());
        std::filesystem::copy_file(c_.state_db(), b_.state_db());
    }
    FsPaths c_{tmp_ / "c"};
};

TEST_F(KeyringOrder, ForeignLocalStateDoesNotAttachTheKeyring) {
    init_fs(*store_, a_, init_with(init_options(), "pw"));
    make_profile_with_foreign_files();
    const Bytes before = file_bytes(b_);
    try {
        mount(b_, mount_with("pw"));
        FAIL() << "mount must fail";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::foreign_fs);
    }
    EXPECT_EQ(file_bytes(b_), before);
    EXPECT_FALSE(crypto::keyring_has_fs(crypto::load_keyring(b_.keyring())));
}

TEST_F(KeyringOrder, ForeignLocalStateWithoutKeyringDoesNotCreateOne) {
    init();  // keyfile, канал A
    store::LocalDirStore other{tmp_ / "other-channel"};
    init_fs(other, c_, init_options());
    ensure_fs_dirs(b_);
    std::filesystem::copy_file(c_.index_db(), b_.index_db());
    std::filesystem::copy_file(c_.state_db(), b_.state_db());
    EXPECT_THROW(mount(b_), MountError);
    EXPECT_FALSE(std::filesystem::exists(b_.keyring()));
}

// --- пароль и keyfile одни на keyring и ФС ---

TEST_F(KeyringFlow, KeyringWithKeyfileAndFsWithPassphraseGivesAClearMessage) {
    init_fs(*store_, a_, init_with(init_options(), "pw"));
    ensure_fs_dirs(b_);
    crypto::save_keyring(b_.keyring(), crypto::make_keyring_without_fs(
                                           crypto::keyfile_kdf_params(),
                                           crypto::read_keyfile(keyfile_), crypto::Key::random()));
    try {
        mount(b_);  // keyfile
        FAIL() << "mount must fail";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::bad_key_source);
        const std::string what = e.what();
        EXPECT_NE(what.find("keyring is protected by a keyfile"), std::string::npos) << what;
        EXPECT_NE(what.find("file system in the channel by a passphrase"), std::string::npos)
            << what;
    }
    EXPECT_FALSE(crypto::keyring_has_fs(crypto::load_keyring(b_.keyring())));
}

TEST_F(KeyringFlow, KeyringWithPassphraseAndFsWithKeyfileGivesAClearMessage) {
    init();  // keyfile
    make_bare(b_, "pw");
    try {
        mount(b_, mount_with("pw"));
        FAIL() << "mount must fail";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::bad_key_source);
        const std::string what = e.what();
        EXPECT_NE(what.find("keyring is protected by a passphrase"), std::string::npos) << what;
        EXPECT_NE(what.find("file system in the channel by a keyfile"), std::string::npos) << what;
    }
}

TEST_F(KeyringFlow, WrongKindOfSourceForTheKeyringNamesTheKeyring) {
    make_bare(b_, "pw");
    init();  // keyfile
    try {
        mount(b_);  // дан keyfile, а keyring под паролем
        FAIL() << "mount must fail";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::bad_key_source);
        EXPECT_NE(std::string(e.what()).find("keyring is protected by a passphrase"),
                  std::string::npos)
            << e.what();
    }
}

TEST_F(KeyringFlow, MountRefusesKeyringOfAnotherFs) {
    init_fs(*store_, a_, init_with(init_options(), "pw"));
    // Другая ФС в другом канале и тот же профиль машины B.
    store::LocalDirStore other{tmp_ / "other-channel"};
    init_fs(other, b_, init_with(init_options(), "pw"));
    try {
        mount(b_, mount_with("pw"));  // голова канала A, keyring от ФС B
        FAIL() << "mount must fail";
    } catch (const MountError& e) {
        EXPECT_EQ(e.kind(), MountErrorKind::foreign_fs);
    }
}

TEST_F(KeyringFlow, MountWithBareKeyringAndKeyfile) {
    init();  // keyfile
    ensure_fs_dirs(b_);
    const crypto::Key tdlib = crypto::Key::random();
    crypto::save_keyring(b_.keyring(),
                         crypto::make_keyring_without_fs(crypto::keyfile_kdf_params(),
                                                         crypto::read_keyfile(keyfile_), tdlib));
    auto fb = mount(b_);
    EXPECT_TRUE(fb->tdlib_db_key() == tdlib);
    fb->unmount();
    EXPECT_TRUE(crypto::keyring_has_fs(crypto::load_keyring(b_.keyring())));
}

TEST_F(KeyringFlow, MountWithoutAnyKeyringStillWorks) {
    // Прежнее поведение (store = "local" на новой машине): keyring создаётся целиком.
    init();
    auto fb = mount(b_);
    fb->unmount();
    EXPECT_TRUE(crypto::keyring_has_fs(crypto::load_keyring(b_.keyring())));
}

}  // namespace
}  // namespace telega::meta
