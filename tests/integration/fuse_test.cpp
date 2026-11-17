// Интеграционные FUSE-тесты на LocalDirStore (SPEC 14): настоящий бинарник telega-fs,
// настоящие монтирования. Без /dev/fuse или fusermount3 тесты пропускаются (GTEST_SKIP).

#include <dirent.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <random>
#include <set>

#include "harness.h"

namespace telega::itest {
namespace {

using namespace std::chrono_literals;

constexpr uint64_t kMiB = uint64_t{1024} * 1024;

class FuseTest : public ::testing::Test {
  protected:
    void SetUp() override {
        if (const std::string why = fuse_skip_reason(); !why.empty()) {
            GTEST_SKIP() << why;
        }
    }

    void TearDown() override {
        if (HasFailure() && env_) {
            std::cerr << "---- telega-fs.log (last machine) ----\n" << env_->log_text() << '\n';
        }
    }

    // Новое окружение и init.
    Env& start(ProfileSpec spec = {}) {
        env_ = std::make_unique<Env>(std::move(spec));
        env_->init();
        return *env_;
    }
    Env& env() { return *env_; }

    std::unique_ptr<Env> env_;
};

// Содержимое каталога без "." и "..", по возрастанию.
std::vector<std::string> list_dir(const fs::path& dir) {
    std::vector<std::string> names;
    for (const auto& e : fs::directory_iterator(dir)) {
        names.push_back(e.path().filename().string());
    }
    std::sort(names.begin(), names.end());
    return names;
}

int rename_flags(const fs::path& from, const fs::path& to, unsigned flags) {
    return errno_of(
        [&] { return ::renameat2(AT_FDCWD, from.c_str(), AT_FDCWD, to.c_str(), flags); });
}

// --- монтирование и размонтирование ---

TEST_F(FuseTest, BackgroundMountIsReadyOnReturnAndUnmountsCleanly) {
    Env& e = start();
    auto m = e.mount({"-o", "noexec"});
    // После возврата mount ФС уже смонтирована и обслуживает запросы.
    ASSERT_TRUE(is_mounted(m->dir()));
    EXPECT_NE(mount_options_of(m->dir()).find("noexec"), std::string::npos);
    EXPECT_NE(mount_options_of(m->dir()).find("default_permissions"), std::string::npos);
    struct statvfs sv {};
    ASSERT_EQ(::statvfs(m->dir().c_str(), &sv), 0);
    EXPECT_EQ(sv.f_namemax, 255U);
    EXPECT_EQ(sv.f_bsize, 4096U);

    write_file(m->path("hello.txt"), {'h', 'i', '\n'});
    EXPECT_EQ(read_file(m->path("hello.txt")), (std::vector<uint8_t>{'h', 'i', '\n'}));

    const Exit x = m->unmount();
    EXPECT_TRUE(x.ok());
    EXPECT_FALSE(is_mounted(m->dir()));
    EXPECT_NE(e.log_text().find("unmounted"), std::string::npos);

    // Профиль после чистого размонтирования монтируется снова, данные на месте.
    auto m2 = e.mount();
    EXPECT_EQ(read_file(m2->path("hello.txt")), (std::vector<uint8_t>{'h', 'i', '\n'}));
    EXPECT_TRUE(m2->unmount().ok());
}

TEST_F(FuseTest, ForegroundMountStopsOnFusermount) {
    Env& e = start();
    auto m = e.mount_fg();
    write_file(m->path("a"), pattern(0, 10'000, 1));
    const Exit x = m->unmount();
    EXPECT_EQ(x.signal, 0);
    EXPECT_EQ(x.code, 0);
    auto m2 = e.mount_fg();
    EXPECT_EQ(read_file(m2->path("a")), pattern(0, 10'000, 1));
    EXPECT_TRUE(m2->unmount().ok());
}

TEST_F(FuseTest, SigtermUnmountsCleanlyAndKeepsData) {
    Env& e = start();
    auto m = e.mount_fg();
    write_pattern_file(m->path("big"), 3 * kMiB + 123, 5);
    const Exit x = m->signal_and_wait(SIGTERM);
    EXPECT_EQ(x.signal, 0);
    EXPECT_EQ(x.code, 0) << m->output();
    EXPECT_FALSE(is_mounted(m->dir()));
    // Финальный коммит сделан: другая машина видит данные.
    e.new_machine();
    auto m2 = e.mount();
    EXPECT_EQ(verify_pattern_file(m2->path("big"), 3 * kMiB + 123, 5), "");
    EXPECT_TRUE(m2->unmount().ok());
}

TEST_F(FuseTest, SecondMountOfTheSameProfileIsRefused) {
    Env& e = start();
    auto m = e.mount();
    const RunResult r = e.run({"mount", e.mountpoint("mnt2").string()});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("in use"), std::string::npos) << r.output;
    EXPECT_FALSE(is_mounted(e.mountpoint("mnt2")));
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, MountErrorsAreReportedWithNonZeroExit) {
    Env& e = start();
    // Неверный keyfile.
    e.replace_keyfile();
    RunResult r = e.run({"mount", e.mountpoint().string()});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("wrong passphrase or keyfile"), std::string::npos) << r.output;
    EXPECT_FALSE(is_mounted(e.mountpoint()));
    // Точки монтирования нет.
    r = e.run({"mount", (e.root() / "nonexistent").string()});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("not a directory"), std::string::npos) << r.output;
    // Профиля нет.
    r = e.run({"mount", "-p", "nope", e.mountpoint().string()});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("nope"), std::string::npos) << r.output;
    // Неизвестная опция FUSE.
    Env& e2 = start();
    r = e2.run({"mount", "-o", "no_such_option_xyz", e2.mountpoint().string()});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_FALSE(is_mounted(e2.mountpoint()));
    // После всех отказов профиль не залочен и монтируется.
    auto m = e2.mount();
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, InitTwiceAndTelegramStoreAreRefused) {
    const Env& e = start();
    RunResult r = e.run({"init"});
    EXPECT_NE(r.exit.code, 0) << "init over an existing file system must fail";
    EXPECT_NE(r.output.find("init failed"), std::string::npos) << r.output;

    ProfileSpec tg;
    tg.store = "telegram";
    Env t{tg};
    r = t.run({"init"});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("implemented in M2"), std::string::npos) << r.output;
    r = t.run({"mount", t.mountpoint().string()});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("implemented in M2"), std::string::npos) << r.output;
}

TEST_F(FuseTest, PassphraseProfile) {
    ProfileSpec spec;
    spec.passphrase = true;
    env_ = std::make_unique<Env>(spec);
    Env& e = env();
    // Расхождение двух вводов — отказ; неверный пароль при монтировании — отказ.
    EXPECT_NE(e.run({"init"}, {}, "one\ntwo\n").exit.code, 0);
    e.init("correct horse\ncorrect horse\n");
    const RunResult r = e.run({"mount", e.mountpoint().string()}, {}, "wrong\n");
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("wrong passphrase or keyfile"), std::string::npos) << r.output;
    EXPECT_EQ(r.output.find("wrong\n"), std::string::npos);
    auto m = e.mount({}, {}, "correct horse\n");
    write_file(m->path("f"), {1, 2, 3});
    EXPECT_TRUE(m->unmount().ok());
    // Ни пароль, ни ключи не попадают в лог.
    EXPECT_EQ(e.log_text().find("correct horse"), std::string::npos);
}

// --- сценарии SPEC 14 ---

TEST_F(FuseTest, LargeSequentialFileSurvivesRemount) {
    Env& e = start();
    constexpr uint64_t kSize = 40 * kMiB + 777;  // чанки по 1 МиБ, хвост не кратен
    auto m = e.mount();
    write_pattern_file(m->path("big.bin"), kSize, 11);
    EXPECT_EQ(verify_pattern_file(m->path("big.bin"), kSize, 11), "");
    {
        const File f = File::open(m->path("big.bin"), O_RDONLY);
        ASSERT_EQ(::fsync(f.fd()), 0);
    }
    EXPECT_EQ(verify_pattern_file(m->path("big.bin"), kSize, 11), "");
    ASSERT_TRUE(m->unmount().ok());

    m = e.mount();
    EXPECT_EQ(verify_pattern_file(m->path("big.bin"), kSize, 11), "");
    EXPECT_TRUE(m->unmount().ok());

    // «Другая машина»: пустой локальный каталог данных, тот же канал и keyfile.
    e.new_machine();
    m = e.mount();
    EXPECT_EQ(verify_pattern_file(m->path("big.bin"), kSize, 11), "");
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, RandomOverwritesMatchModel) {
    Env& e = start();
    auto m = e.mount();
    const fs::path path = m->path("rw.bin");
    std::vector<uint8_t> model = pattern(0, 6 * kMiB, 3);
    write_file(path, model);

    std::mt19937_64 rng{12345};
    {
        const File f = File::open(path, O_RDWR);
        for (int i = 0; i < 250; ++i) {
            const uint64_t off = rng() % (8 * kMiB);
            const uint64_t len = 1 + rng() % (rng() % 4 == 0 ? 2 * kMiB + 5 : 70'000);
            const std::vector<uint8_t> data = pattern(0, len, 1000 + static_cast<uint64_t>(i));
            f.pwrite_all(data.data(), data.size(), off);
            if (model.size() < off + len) {
                model.resize(off + len, 0);
            }
            std::copy(data.begin(), data.end(), model.begin() + static_cast<std::ptrdiff_t>(off));
            if (i % 50 == 49) {
                ASSERT_EQ(read_file(path), model) << "after write " << i;
            }
        }
        ASSERT_EQ(::fsync(f.fd()), 0);
    }
    ASSERT_EQ(read_file(path), model);
    ASSERT_TRUE(m->unmount().ok());
    m = e.mount();
    EXPECT_EQ(read_file(m->path("rw.bin")), model);
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, RenameOverExisting) {
    Env& e = start();
    auto m = e.mount();
    write_file(m->path("a"), pattern(0, 2 * kMiB + 5, 1));
    write_file(m->path("b"), pattern(0, 100, 2));
    ASSERT_EQ(::rename(m->path("a").c_str(), m->path("b").c_str()), 0);
    EXPECT_FALSE(fs::exists(m->path("a")));
    EXPECT_EQ(verify_pattern_file(m->path("b"), 2 * kMiB + 5, 1), "");

    // Между каталогами, поверх пустого каталога, поверх непустого.
    ASSERT_EQ(::mkdir(m->path("d1").c_str(), 0755), 0);
    ASSERT_EQ(::mkdir(m->path("d2").c_str(), 0755), 0);
    ASSERT_EQ(::mkdir(m->path("d2/sub").c_str(), 0755), 0);
    write_file(m->path("d2/sub/x"), {1});
    ASSERT_EQ(::rename(m->path("b").c_str(), m->path("d1/moved").c_str()), 0);
    EXPECT_EQ(rename_flags(m->path("d1"), m->path("d2"), 0), ENOTEMPTY);
    ASSERT_EQ(::mkdir(m->path("empty").c_str(), 0755), 0);
    ASSERT_EQ(::rename(m->path("d1").c_str(), m->path("empty").c_str()), 0);
    EXPECT_EQ(verify_pattern_file(m->path("empty/moved"), 2 * kMiB + 5, 1), "");
    EXPECT_EQ(rename_flags(m->path("d2"), m->path("empty/moved"), 0), ENOTDIR);
    EXPECT_EQ(rename_flags(m->path("nope"), m->path("x"), 0), ENOENT);

    // RENAME_NOREPLACE и RENAME_EXCHANGE.
    write_file(m->path("n1"), {1});
    write_file(m->path("n2"), {2});
    EXPECT_EQ(rename_flags(m->path("n1"), m->path("n2"), RENAME_NOREPLACE), EEXIST);
    EXPECT_EQ(rename_flags(m->path("n1"), m->path("n3"), RENAME_NOREPLACE), 0);
    EXPECT_EQ(rename_flags(m->path("n3"), m->path("n2"), RENAME_EXCHANGE), EINVAL);

    ASSERT_TRUE(m->unmount().ok());
    m = e.mount();
    EXPECT_EQ(verify_pattern_file(m->path("empty/moved"), 2 * kMiB + 5, 1), "");
    EXPECT_EQ(read_file(m->path("n3")), std::vector<uint8_t>{1});
    EXPECT_EQ(read_file(m->path("n2")), std::vector<uint8_t>{2});
    EXPECT_FALSE(fs::exists(m->path("a")));
    EXPECT_FALSE(fs::exists(m->path("d1")));
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, UnlinkOpenFileKeepsDataUntilClose) {
    Env& e = start();
    auto m = e.mount();
    const fs::path path = m->path("victim");
    constexpr uint64_t kSize = 3 * kMiB + 17;
    File f = File::open(path, O_RDWR | O_CREAT, 0644);
    const std::vector<uint8_t> data = pattern(0, kSize, 7);
    f.pwrite_all(data.data(), data.size(), 0);
    ASSERT_EQ(::unlink(path.c_str()), 0);
    struct stat st {};
    EXPECT_EQ(::stat(path.c_str(), &st), -1);
    EXPECT_EQ(errno, ENOENT);
    ASSERT_EQ(::fstat(f.fd(), &st), 0);
    EXPECT_EQ(st.st_nlink, 0U);
    EXPECT_EQ(static_cast<uint64_t>(st.st_size), kSize);
    // Чтение после unlink до close; запись тоже работает.
    std::vector<uint8_t> back(kSize);
    f.pread_exact(back.data(), back.size(), 0);
    EXPECT_EQ(back, data);
    const std::vector<uint8_t> more = pattern(0, 1000, 8);
    f.pwrite_all(more.data(), more.size(), kSize);
    std::vector<uint8_t> tail(1000);
    f.pread_exact(tail.data(), tail.size(), kSize);
    EXPECT_EQ(tail, more);
    // Новый файл с тем же именем не мешает.
    write_file(path, {9});
    EXPECT_EQ(read_file(path), std::vector<uint8_t>{9});
    f.pread_exact(back.data(), back.size(), 0);
    EXPECT_EQ(back, data);
    f.close();
    ASSERT_EQ(::unlink(path.c_str()), 0);
    EXPECT_TRUE(list_dir(m->dir()).empty());

    // Файл, удалённый при открытом дескрипторе, и размонтирование: после перемонтирования его нет.
    File g = File::open(m->path("gone"), O_RDWR | O_CREAT, 0644);
    g.pwrite_all(data.data(), 5000, 0);
    ASSERT_EQ(::unlink(m->path("gone").c_str()), 0);
    g.close();
    ASSERT_TRUE(m->unmount().ok());
    m = e.mount();
    EXPECT_TRUE(list_dir(m->dir()).empty());
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, DirectoriesSymlinksAndReaddir) {
    Env& e = start();
    auto m = e.mount();
    ASSERT_EQ(::mkdir(m->path("a").c_str(), 0755), 0);
    ASSERT_EQ(::mkdir(m->path("a/b").c_str(), 0700), 0);
    ASSERT_EQ(::mkdir(m->path("a/b/c").c_str(), 0755), 0);
    EXPECT_EQ(errno_of([&] { return ::mkdir(m->path("a").c_str(), 0755); }), EEXIST);
    EXPECT_EQ(errno_of([&] { return ::rmdir(m->path("a/b").c_str()); }), ENOTEMPTY);
    EXPECT_EQ(errno_of([&] { return ::rmdir(m->path("nope").c_str()); }), ENOENT);
    write_file(m->path("a/file"), {1, 2, 3});
    EXPECT_EQ(errno_of([&] { return ::rmdir(m->path("a/file").c_str()); }), ENOTDIR);
    EXPECT_EQ(errno_of([&] { return ::unlink(m->path("a/b").c_str()); }), EISDIR);

    // Симлинки: относительный, абсолютный, висячий.
    ASSERT_EQ(::symlink("file", m->path("a/rel").c_str()), 0);
    ASSERT_EQ(::symlink("/nonexistent/target", m->path("a/dangling").c_str()), 0);
    ASSERT_EQ(::symlink("b/c", m->path("a/dirlink").c_str()), 0);
    std::array<char, 256> buf{};
    ssize_t n = ::readlink(m->path("a/rel").c_str(), buf.data(), buf.size());
    ASSERT_GT(n, 0);
    EXPECT_EQ(std::string(buf.data(), static_cast<size_t>(n)), "file");
    EXPECT_EQ(read_file(m->path("a/rel")), (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_TRUE(fs::is_directory(m->path("a/dirlink")));
    struct stat st {};
    ASSERT_EQ(::lstat(m->path("a/dangling").c_str(), &st), 0);
    EXPECT_TRUE(S_ISLNK(st.st_mode));
    EXPECT_EQ(::stat(m->path("a/dangling").c_str(), &st), -1);

    // Много записей: readdir(plus) идёт в несколько запросов, имена не теряются и не дублируются.
    ASSERT_EQ(::mkdir(m->path("many").c_str(), 0755), 0);
    std::set<std::string> expected;
    for (int i = 0; i < 1500; ++i) {
        const std::string name = "entry-with-a-longish-name-" + std::to_string(i);
        write_file(m->path("many/" + name), {static_cast<uint8_t>(i)});
        expected.insert(name);
    }
    std::set<std::string> seen;
    DIR* d = ::opendir(m->path("many").c_str());
    ASSERT_NE(d, nullptr);
    bool dot = false;
    bool dotdot = false;
    while (const dirent* de = ::readdir(d)) {  // NOLINT(concurrency-mt-unsafe): поток один
        const std::string name = de->d_name;
        if (name == ".") {
            dot = true;
        } else if (name == "..") {
            dotdot = true;
        } else {
            EXPECT_TRUE(seen.insert(name).second) << "duplicate " << name;
            EXPECT_EQ(de->d_type, DT_REG);
        }
    }
    ::closedir(d);
    EXPECT_TRUE(dot);
    EXPECT_TRUE(dotdot);
    EXPECT_EQ(seen, expected);
    // Удаление во время обхода не ломает курсор.
    d = ::opendir(m->path("many").c_str());
    ASSERT_NE(d, nullptr);
    size_t count = 0;
    while (const dirent* de = ::readdir(d)) {  // NOLINT(concurrency-mt-unsafe): поток один
        if (de->d_name[0] != '.') {
            ::unlinkat(::dirfd(d), de->d_name, 0);
            ++count;
        }
    }
    ::closedir(d);
    EXPECT_GE(count, 1U);

    // Неподдерживаемое.
    EXPECT_EQ(errno_of([&] { return ::mkfifo(m->path("fifo").c_str(), 0644); }), EPERM);
    EXPECT_EQ(errno_of([&] { return ::link(m->path("a/file").c_str(), m->path("hl").c_str()); }),
              EPERM);
    EXPECT_EQ(errno_of([&] { return ::setxattr(m->path("a/file").c_str(), "user.x", "1", 1, 0); }),
              ENOTSUP);
    // Повторно и для чтения: ядро запомнило ENOSYS, приложение по-прежнему видит ENOTSUP.
    EXPECT_EQ(errno_of([&] { return ::setxattr(m->path("a/file").c_str(), "user.x", "1", 1, 0); }),
              ENOTSUP);
    {
        std::array<char, 16> xbuf{};
        EXPECT_EQ(errno_of([&] {
                      return static_cast<int>(::getxattr(m->path("a/file").c_str(), "user.x",
                                                         xbuf.data(), xbuf.size()));
                  }),
                  ENOTSUP);
        EXPECT_EQ(errno_of([&] {
                      return static_cast<int>(
                          ::listxattr(m->path("a/file").c_str(), xbuf.data(), xbuf.size()));
                  }),
                  ENOTSUP);
    }
    {
        const File f = File::open(m->path("a/file"), O_RDWR);
        EXPECT_EQ(errno_of([&] { return ::fallocate(f.fd(), 0, 0, 4096); }), EOPNOTSUPP);
    }
    EXPECT_EQ(errno_of([&] {
                  return ::open(m->path(std::string(256, 'x')).c_str(), O_CREAT | O_WRONLY, 0644);
              }),
              ENAMETOOLONG);
    write_file(m->path(std::string(255, 'y')), {1});

    ASSERT_EQ(::rmdir(m->path("a/b/c").c_str()), 0);
    ASSERT_TRUE(m->unmount().ok());
    m = e.mount();
    EXPECT_FALSE(fs::exists(m->path("a/b/c")));
    EXPECT_TRUE(fs::is_directory(m->path("a/b")));
    n = ::readlink(m->path("a/dangling").c_str(), buf.data(), buf.size());
    ASSERT_GT(n, 0);
    EXPECT_EQ(std::string(buf.data(), static_cast<size_t>(n)), "/nonexistent/target");
    EXPECT_EQ(read_file(m->path("a/rel")), (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_TRUE(fs::exists(m->path(std::string(255, 'y'))));
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, RemountKeepsDataAndAttributes) {
    Env& e = start();
    auto m = e.mount();
    write_file(m->path("tiny"), {'x'});  // инлайн
    write_file(m->path("mid"), pattern(0, 300'000, 2));
    write_pattern_file(m->path("big"), 5 * kMiB + 1, 3);
    ASSERT_EQ(::mkdir(m->path("dir").c_str(), 0750), 0);
    write_file(m->path("dir/inner"), pattern(0, 5000, 4));
    ASSERT_EQ(::symlink("dir/inner", m->path("link").c_str()), 0);
    ASSERT_EQ(::chmod(m->path("mid").c_str(), 0640), 0);
    const std::array<timespec, 2> times = {
        {{1'700'000'000, 123'456'789}, {1'600'000'000, 987'654'321}}};
    ASSERT_EQ(::utimensat(AT_FDCWD, m->path("mid").c_str(), times.data(), 0), 0);
    ASSERT_EQ(::truncate(m->path("big").c_str(), 4 * kMiB + 100), 0);  // укорачивание
    ASSERT_EQ(::truncate(m->path("tiny").c_str(), 70'000), 0);  // расширение нулями
    ASSERT_TRUE(m->unmount().ok());

    const auto check = [&](Mount& mnt) {
        EXPECT_EQ(verify_pattern_file(mnt.path("mid"), 300'000, 2), "");
        EXPECT_EQ(verify_pattern_file(mnt.path("big"), 4 * kMiB + 100, 3), "");
        EXPECT_EQ(verify_pattern_file(mnt.path("dir/inner"), 5000, 4), "");
        std::vector<uint8_t> tiny = read_file(mnt.path("tiny"));
        ASSERT_EQ(tiny.size(), 70'000U);
        EXPECT_EQ(tiny[0], 'x');
        EXPECT_EQ(std::count(tiny.begin() + 1, tiny.end(), 0), 69'999);
        struct stat st {};
        ASSERT_EQ(::stat(mnt.path("mid").c_str(), &st), 0);
        EXPECT_EQ(st.st_mode & 07777U, 0640U);
        EXPECT_EQ(st.st_mtim.tv_sec, 1'600'000'000);
        EXPECT_EQ(st.st_mtim.tv_nsec, 987'654'321);
        EXPECT_EQ(st.st_atim.tv_sec, 1'700'000'000);
        EXPECT_EQ(st.st_uid, ::getuid());
        ASSERT_EQ(::stat(mnt.path("dir").c_str(), &st), 0);
        EXPECT_EQ(st.st_mode & 07777U, 0750U);
        std::array<char, 64> buf{};
        const ssize_t n = ::readlink(mnt.path("link").c_str(), buf.data(), buf.size());
        ASSERT_GT(n, 0);
        EXPECT_EQ(std::string(buf.data(), static_cast<size_t>(n)), "dir/inner");
        EXPECT_EQ(list_dir(mnt.dir()),
                  (std::vector<std::string>{"big", "dir", "link", "mid", "tiny"}));
    };
    m = e.mount();
    check(*m);
    ASSERT_TRUE(m->unmount().ok());
    e.new_machine();
    m = e.mount();
    check(*m);
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, ReadOnlyMountRejectsChanges) {
    Env& e = start();
    auto m = e.mount();
    write_file(m->path("f"), pattern(0, 2 * kMiB, 1));
    ASSERT_EQ(::mkdir(m->path("d").c_str(), 0755), 0);
    ASSERT_TRUE(m->unmount().ok());

    m = e.mount({"--ro"});
    const std::string opts = mount_options_of(m->dir());
    EXPECT_NE(opts.find("ro"), std::string::npos);
    EXPECT_EQ(verify_pattern_file(m->path("f"), 2 * kMiB, 1), "");
    EXPECT_EQ(errno_of([&] { return ::open(m->path("f").c_str(), O_WRONLY); }), EROFS);
    EXPECT_EQ(errno_of([&] { return ::open(m->path("g").c_str(), O_CREAT | O_WRONLY, 0644); }),
              EROFS);
    EXPECT_EQ(errno_of([&] { return ::mkdir(m->path("e").c_str(), 0755); }), EROFS);
    EXPECT_EQ(errno_of([&] { return ::unlink(m->path("f").c_str()); }), EROFS);
    EXPECT_EQ(errno_of([&] { return ::rmdir(m->path("d").c_str()); }), EROFS);
    EXPECT_EQ(errno_of([&] { return ::rename(m->path("f").c_str(), m->path("h").c_str()); }),
              EROFS);
    EXPECT_EQ(errno_of([&] { return ::symlink("x", m->path("s").c_str()); }), EROFS);
    EXPECT_EQ(errno_of([&] { return ::truncate(m->path("f").c_str(), 0); }), EROFS);
    EXPECT_EQ(errno_of([&] { return ::chmod(m->path("f").c_str(), 0600); }), EROFS);
    ASSERT_TRUE(m->unmount().ok());

    // --ro ничего не изменил: после обычного монтирования всё как было.
    m = e.mount();
    EXPECT_EQ(verify_pattern_file(m->path("f"), 2 * kMiB, 1), "");
    EXPECT_EQ(list_dir(m->dir()), (std::vector<std::string>{"d", "f"}));
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(FuseTest, MmapReadWriteWorks) {
    Env& e = start();
    auto m = e.mount();
    constexpr size_t kSize = 3 * kMiB + 4096;
    write_file(m->path("map"), pattern(0, kSize, 9));
    {
        const File f = File::open(m->path("map"), O_RDWR);
        void* p = ::mmap(nullptr, kSize, PROT_READ | PROT_WRITE, MAP_SHARED, f.fd(), 0);
        ASSERT_NE(p, MAP_FAILED);
        const std::vector<uint8_t> want = pattern(0, kSize, 9);
        EXPECT_EQ(std::memcmp(p, want.data(), kSize), 0);
        std::memset(static_cast<uint8_t*>(p) + kMiB - 10, 0xAB, 20);  // через границу чанка
        ASSERT_EQ(::msync(p, kSize, MS_SYNC), 0);
        ASSERT_EQ(::munmap(p, kSize), 0);
    }
    std::vector<uint8_t> want = pattern(0, kSize, 9);
    std::fill(want.begin() + static_cast<std::ptrdiff_t>(kMiB) - 10,
              want.begin() + static_cast<std::ptrdiff_t>(kMiB) + 10, 0xAB);
    EXPECT_EQ(read_file(m->path("map")), want);
    ASSERT_TRUE(m->unmount().ok());
    m = e.mount();
    EXPECT_EQ(read_file(m->path("map")), want);
    EXPECT_TRUE(m->unmount().ok());
}

// --- сбои ---

TEST_F(FuseTest, WorksWithoutNetworkAndUploadsLater) {
    ProfileSpec spec;
    spec.sync_timeout = "2s";
    Env& e = start(spec);
    constexpr uint64_t kBig = 3 * kMiB + 5;
    {
        auto m = e.mount_fg({}, {{"TELEGA_FS_FAULTS", "put_chunk:*:transient"}});
        write_pattern_file(m->path("big"), kBig, 21);
        write_file(m->path("small"), {'s', 'm', 'a', 'l', 'l'});
        // Запись и чтение работают из overlay.
        EXPECT_EQ(verify_pattern_file(m->path("big"), kBig, 21), "");
        EXPECT_EQ(read_file(m->path("small")).size(), 5U);
        // fsync без сети возвращает EIO после sync_timeout, а не виснет.
        File f = File::open(m->path("big"), O_RDWR);
        const auto t0 = std::chrono::steady_clock::now();
        EXPECT_EQ(errno_of([&] { return ::fsync(f.fd()); }), EIO);
        const auto waited = std::chrono::steady_clock::now() - t0;
        EXPECT_GE(waited, 1500ms);
        EXPECT_LT(waited, 30s);
        // Данные по-прежнему читаются, ФС не «сломалась».
        EXPECT_EQ(verify_pattern_file(m->path("big"), kBig, 21), "");
        write_file(m->path("after"), {1});
        // Размонтирование не виснет: ждёт не дольше sync_timeout и выходит с ошибкой в логе.
        f.pwrite_all("z", 1, kBig);  // ещё одна запись перед размонтированием
        f.close();  // открытый файл мешал бы размонтированию (EBUSY)
        const auto t1 = std::chrono::steady_clock::now();
        const Exit x = m->unmount(60s);
        EXPECT_LT(std::chrono::steady_clock::now() - t1, 30s);
        EXPECT_EQ(x.signal, 0);
        EXPECT_EQ(x.code, 1) << "unmount with unsent data reports the failure";
        EXPECT_NE(m->output().find("flush on unmount failed"), std::string::npos) << m->output();
    }
    // Сеть вернулась: незалитое осталось в overlay и дозаливается.
    {
        auto m = e.mount();
        EXPECT_EQ(read_file(m->path("small")), (std::vector<uint8_t>{'s', 'm', 'a', 'l', 'l'}));
        {
            const File f = File::open(m->path("big"), O_RDWR);
            EXPECT_EQ(::fsync(f.fd()), 0);
        }
        ASSERT_TRUE(m->unmount().ok());
    }
    e.new_machine();
    auto m = e.mount();
    std::vector<uint8_t> want = pattern(0, kBig, 21);
    want.push_back('z');
    EXPECT_EQ(read_file(m->path("big")), want);
    EXPECT_EQ(read_file(m->path("small")), (std::vector<uint8_t>{'s', 'm', 'a', 'l', 'l'}));
    EXPECT_TRUE(fs::exists(m->path("after")));
    EXPECT_TRUE(m->unmount().ok());
}

// Находка 4: данные залиты (инлайн-файлов заливать нечего), не удался только финальный коммит.
// Размонтирование всё равно доводит остановку до конца, но код выхода 1 и сообщение в логе.
TEST_F(FuseTest, FailedFinalCommitGivesExitCodeOne) {
    ProfileSpec spec;
    spec.sync_timeout = "5s";
    Env& e = start(spec);
    {
        auto m = e.mount_fg({}, {{"TELEGA_FS_FAULTS", "put_index:*:permanent"}});
        write_file(m->path("small"), {'s', 'm', 'a', 'l', 'l'});
        const Exit x = m->unmount(60s);
        EXPECT_EQ(x.signal, 0);
        EXPECT_EQ(x.code, 1) << "unmount with a failed final commit reports the failure";
        EXPECT_NE(m->output().find("final commit failed"), std::string::npos) << m->output();
    }
    // Незакоммиченное осталось в локальном состоянии и коммитится следующим монтированием.
    {
        auto m = e.mount();
        EXPECT_EQ(read_file(m->path("small")), (std::vector<uint8_t>{'s', 'm', 'a', 'l', 'l'}));
        ASSERT_TRUE(m->unmount().ok());
    }
    e.new_machine();
    auto m = e.mount();
    EXPECT_EQ(read_file(m->path("small")), (std::vector<uint8_t>{'s', 'm', 'a', 'l', 'l'}));
    EXPECT_TRUE(m->unmount().ok());
}

struct CrashCase {
    const char* name;
    const char* faults;
};

class FuseCrashTest : public FuseTest, public ::testing::WithParamInterface<CrashCase> {};

// SPEC 7.3: данные, записанные до последнего успешного fsync, переживают падение процесса.
TEST_P(FuseCrashTest, DataBeforeLastFsyncSurvives) {
    Env& e = start();
    constexpr uint64_t kA = kMiB + kMiB / 2;  // 2 чанка: put_chunk #1, #2
    constexpr uint64_t kB = 3 * kMiB;  // 3 чанка: put_chunk #3.., затем коммит №2
    {
        auto m = e.mount_fg({}, {{"TELEGA_FS_FAULTS", GetParam().faults}});
        write_pattern_file(m->path("A"), kA, 31);
        {
            const File f = File::open(m->path("A"), O_RDWR);
            ASSERT_EQ(::fsync(f.fd()), 0) << "the first fsync must succeed (faults hit later)";
        }
        // Заливка полных чанков идёт уже во время записи, поэтому процесс может упасть и до
        // fsync: запись и fsync получают ошибку соединения (ECONNABORTED/ENOTCONN) или EIO.
        try {
            write_pattern_file(m->path("B"), kB, 32);
            const File f = File::open(m->path("B"), O_RDWR);
            EXPECT_NE(::fsync(f.fd()), 0);
        } catch (const std::system_error&) {  // NOLINT(bugprone-empty-catch): так и задумано
        }
        const Exit x = m->wait_exit(60s);
        EXPECT_EQ(x.signal, 0);
        EXPECT_EQ(x.code, 86) << "injected crash exits with code 86";
        m->kill_hard();  // fusermount3 -uz
        EXPECT_FALSE(is_mounted(m->dir()));
    }
    {
        auto m = e.mount();
        EXPECT_EQ(verify_pattern_file(m->path("A"), kA, 31), "");
        // B: либо нет, либо читается без ошибок (overlay или закоммиченные данные).
        if (fs::exists(m->path("B"))) {
            EXPECT_NO_THROW((void)read_file(m->path("B")));
            const File f = File::open(m->path("B"), O_RDWR);
            EXPECT_EQ(::fsync(f.fd()), 0);
        }
        ASSERT_TRUE(m->unmount().ok());
    }
    e.new_machine();
    auto m = e.mount();
    EXPECT_EQ(verify_pattern_file(m->path("A"), kA, 31), "");
    EXPECT_TRUE(m->unmount().ok());
}

INSTANTIATE_TEST_SUITE_P(
    Faults, FuseCrashTest,
    ::testing::Values(CrashCase{"PutChunkCrashBefore", "put_chunk:3:crash_before"},
                      CrashCase{"PutChunkCrashAfter", "put_chunk:3:crash_after"},
                      CrashCase{"SetHeadCrashBefore", "set_head:2:crash_before"},
                      CrashCase{"SetHeadCrashAfter", "set_head:2:crash_after"},
                      CrashCase{"PutIndexCrashAfter", "put_index:2:crash_after"}),
    [](const ::testing::TestParamInfo<CrashCase>& p) { return std::string(p.param.name); });

}  // namespace
}  // namespace telega::itest
