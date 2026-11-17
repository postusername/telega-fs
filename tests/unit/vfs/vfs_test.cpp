// Vfs: пространство имён — ошибки из таблицы SPEC 9, rename во всех вариантах, symlink,
// readdir с курсором, nlink, setattr, statfs, read-only.

#include "vfs/vfs.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <linux/fs.h>
#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

class VfsNamespace : public ::testing::Test {
  protected:
    void SetUp() override { env.start(); }
    [[nodiscard]] Vfs& v() const { return *env.vfs; }

    std::vector<std::string> list(int64_t dir, size_t batch = 1000) {
        const uint64_t dh = v().opendir(dir);
        std::vector<std::string> out;
        uint64_t off = 0;
        for (;;) {
            const auto part = v().readdir(dh, off, batch, false);
            if (part.empty()) {
                break;
            }
            for (const DirEntry& e : part) {
                out.push_back(str_of(e.name));
                off = e.off;
            }
        }
        v().releasedir(dh);
        return out;
    }

    VfsEnv env;
};

TEST_F(VfsNamespace, LookupAndGetattr) {
    const int64_t f = env.mkfile("a");
    const Attr a = v().lookup(kRoot, bytes_of("a"));
    EXPECT_EQ(a.ino, f);
    EXPECT_EQ(a.kind, meta::InodeKind::file);
    EXPECT_EQ(a.mode, 0644U);
    EXPECT_EQ(a.nlink, 1U);
    EXPECT_EQ(a.uid, 1000U);
    EXPECT_EQ(a.blksize, 4096U);
    EXPECT_EQ(a.size, 0);
    EXPECT_EQ(err_of([&] { v().lookup(kRoot, bytes_of("missing")); }), ENOENT);
    EXPECT_EQ(err_of([&] { v().lookup(f, bytes_of("x")); }), ENOTDIR);
    EXPECT_EQ(err_of([&] { v().lookup(kRoot, bytes_of(std::string(256, 'x'))); }), ENAMETOOLONG);
    EXPECT_EQ(err_of([&] { v().getattr(9999); }), ENOENT);

    struct stat st {};
    fill_stat(v().getattr(kRoot), st);
    EXPECT_TRUE(S_ISDIR(st.st_mode));
    EXPECT_EQ(st.st_nlink, 2U);
}

TEST_F(VfsNamespace, BlocksAreRoundedUp) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, bytes_of("x"));
    Attr a = v().getattr(f);
    EXPECT_EQ(a.size, 1);
    EXPECT_EQ(a.blocks, 8);  // 4096 байт в единицах 512
    env.write(f, 4096, bytes_of("y"));
    a = v().getattr(f);
    EXPECT_EQ(a.blocks, 16);
}

TEST_F(VfsNamespace, CreateErrors) {
    env.mkfile("a");
    EXPECT_EQ(err_of([&] { env.mkfile("a"); }), EEXIST);
    EXPECT_EQ(err_of([&] { env.mkdir("a"); }), EEXIST);
    EXPECT_EQ(err_of([&] { env.mkfile(std::string(256, 'n')); }), ENAMETOOLONG);
    EXPECT_EQ(err_of([&] { env.mkfile("x", 9999); }), ENOENT);
    const int64_t f = v().lookup(kRoot, bytes_of("a")).ino;
    EXPECT_EQ(err_of([&] { env.mkfile("x", f); }), ENOTDIR);
    EXPECT_EQ(err_of([&] { env.mkfile("a/b"); }), EINVAL);
    EXPECT_EQ(err_of([&] { env.mkfile(""); }), EINVAL);
    EXPECT_EQ(err_of([&] { env.mkfile("."); }), EINVAL);
    EXPECT_NO_THROW(env.mkfile(std::string(255, 'n')));
}

TEST_F(VfsNamespace, MkdirRmdirAndNlink) {
    const int64_t d = env.mkdir("d");
    EXPECT_EQ(v().getattr(kRoot).nlink, 3U);
    EXPECT_EQ(v().getattr(d).nlink, 2U);
    env.mkdir("s1", d);
    env.mkdir("s2", d);
    env.mkfile("f", d);
    EXPECT_EQ(v().getattr(d).nlink, 4U);  // файлы не считаются

    EXPECT_EQ(err_of([&] { v().rmdir(kRoot, bytes_of("d")); }), ENOTEMPTY);
    EXPECT_EQ(err_of([&] { v().rmdir(d, bytes_of("f")); }), ENOTDIR);
    EXPECT_EQ(err_of([&] { v().rmdir(d, bytes_of("nope")); }), ENOENT);
    EXPECT_EQ(err_of([&] { v().unlink(d, bytes_of("s1")); }), EISDIR);
    v().rmdir(d, bytes_of("s1"));
    EXPECT_EQ(v().getattr(d).nlink, 3U);
    v().rmdir(d, bytes_of("s2"));
    v().unlink(d, bytes_of("f"));
    v().rmdir(kRoot, bytes_of("d"));
    EXPECT_EQ(v().getattr(kRoot).nlink, 2U);
    EXPECT_EQ(err_of([&] { v().lookup(kRoot, bytes_of("d")); }), ENOENT);
}

TEST_F(VfsNamespace, UnlinkErrors) {
    EXPECT_EQ(err_of([&] { v().unlink(kRoot, bytes_of("nope")); }), ENOENT);
    const int64_t f = env.mkfile("f");
    EXPECT_EQ(err_of([&] { v().unlink(f, bytes_of("x")); }), ENOTDIR);
    v().unlink(kRoot, bytes_of("f"));
    EXPECT_EQ(err_of([&] { v().lookup(kRoot, bytes_of("f")); }), ENOENT);
}

TEST_F(VfsNamespace, UnsupportedOperations) {
    EXPECT_EQ(err_of([] { Vfs::link(); }), EPERM);
    EXPECT_EQ(err_of([] { Vfs::xattr(); }), ENOTSUP);
    EXPECT_EQ(err_of([] { Vfs::fallocate(); }), EOPNOTSUPP);
    EXPECT_EQ(err_of([&] { v().mknod(kRoot, bytes_of("fifo"), S_IFIFO | 0644, 0, 0); }), EPERM);
    EXPECT_EQ(err_of([&] { v().mknod(kRoot, bytes_of("chr"), S_IFCHR | 0644, 0, 0); }), EPERM);
    const Attr a = v().mknod(kRoot, bytes_of("reg"), S_IFREG | 0600, 1, 2);
    EXPECT_EQ(a.kind, meta::InodeKind::file);
    EXPECT_EQ(a.mode, 0600U);
    EXPECT_EQ(
        err_of([&] { v().rename(kRoot, bytes_of("reg"), kRoot, bytes_of("x"), RENAME_EXCHANGE); }),
        EINVAL);
}

TEST_F(VfsNamespace, Symlink) {
    const Attr s = v().symlink(kRoot, bytes_of("l"), bytes_of("../some/target"), 1000, 1000);
    EXPECT_EQ(s.kind, meta::InodeKind::symlink);
    EXPECT_EQ(s.size, 14);
    EXPECT_EQ(str_of(v().readlink(s.ino)), "../some/target");
    const int64_t f = env.mkfile("f");
    EXPECT_EQ(err_of([&] { v().readlink(f); }), EINVAL);
    EXPECT_EQ(err_of([&] { v().symlink(kRoot, bytes_of("l"), bytes_of("t"), 0, 0); }), EEXIST);
    EXPECT_EQ(err_of([&] { v().symlink(kRoot, bytes_of("e"), bytes_of(""), 0, 0); }), EINVAL);
    EXPECT_EQ(err_of([&] {
                  v().symlink(kRoot, bytes_of("big"), bytes_of(std::string(4096, 't')), 0, 0);
              }),
              ENAMETOOLONG);
    v().unlink(kRoot, bytes_of("l"));
    EXPECT_EQ(err_of([&] { v().lookup(kRoot, bytes_of("l")); }), ENOENT);
}

TEST_F(VfsNamespace, RenameFileMovesAndReplaces) {
    const int64_t d = env.mkdir("d");
    const int64_t a = env.mkfile("a");
    env.write(a, 0, bytes_of("AAA"));
    v().rename(kRoot, bytes_of("a"), d, bytes_of("b"), 0);
    EXPECT_EQ(err_of([&] { v().lookup(kRoot, bytes_of("a")); }), ENOENT);
    EXPECT_EQ(v().lookup(d, bytes_of("b")).ino, a);

    // Файл поверх файла: старый освобождается вместе с данными.
    const int64_t c = env.mkfile("c");
    env.write(c, 0, pattern(3 * kChunk / 2, 1));
    v().fsync(c);
    EXPECT_GT(env.live_chunks(), 0);
    v().forget(c, 1);  // ядро забыло c (create дал lookup)
    v().rename(d, bytes_of("b"), kRoot, bytes_of("c"), 0);
    EXPECT_EQ(v().lookup(kRoot, bytes_of("c")).ino, a);
    EXPECT_FALSE(env.inode(c));
    EXPECT_EQ(env.live_chunks(), 0);
    EXPECT_EQ(str_of(env.read_all(a)), "AAA");
    EXPECT_TRUE(env.accounting_ok());

    // Тот же источник и цель — ничего не происходит.
    v().rename(kRoot, bytes_of("c"), kRoot, bytes_of("c"), 0);
    EXPECT_EQ(v().lookup(kRoot, bytes_of("c")).ino, a);
}

TEST_F(VfsNamespace, RenameNoreplaceAndErrors) {
    env.mkfile("a");
    env.mkfile("b");
    const int64_t d = env.mkdir("d");
    const int64_t e = env.mkdir("e");
    env.mkfile("inner", e);
    EXPECT_EQ(
        err_of([&] { v().rename(kRoot, bytes_of("a"), kRoot, bytes_of("b"), RENAME_NOREPLACE); }),
        EEXIST);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("nope"), kRoot, bytes_of("x"), 0); }),
              ENOENT);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("a"), kRoot, bytes_of("d"), 0); }), EISDIR);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("d"), kRoot, bytes_of("a"), 0); }), ENOTDIR);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("d"), kRoot, bytes_of("e"), 0); }),
              ENOTEMPTY);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("a"), 9999, bytes_of("x"), 0); }), ENOENT);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("a"), kRoot, bytes_of("a/b"), 0); }), EINVAL);
    // Каталог внутрь своего поддерева.
    const int64_t sub = env.mkdir("sub", d);
    const int64_t deep = env.mkdir("deep", sub);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("d"), deep, bytes_of("x"), 0); }), EINVAL);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("d"), d, bytes_of("x"), 0); }), EINVAL);
    // Всё на месте после ошибок.
    EXPECT_EQ(list(kRoot), (std::vector<std::string>{".", "..", "a", "b", "d", "e"}));
    // NOREPLACE на свободное имя работает.
    v().rename(kRoot, bytes_of("a"), kRoot, bytes_of("z"), RENAME_NOREPLACE);
    EXPECT_NO_THROW(v().lookup(kRoot, bytes_of("z")));
}

TEST_F(VfsNamespace, RenameDirectoryOverEmptyDirectory) {
    const int64_t d = env.mkdir("d");
    env.mkfile("f", d);
    const int64_t sub = env.mkdir("sub", d);
    const int64_t e = env.mkdir("e");
    v().rename(kRoot, bytes_of("d"), kRoot, bytes_of("e"), 0);
    EXPECT_EQ(v().lookup(kRoot, bytes_of("e")).ino, d);
    EXPECT_FALSE(env.inode(e));
    EXPECT_EQ(err_of([&] { v().lookup(kRoot, bytes_of("d")); }), ENOENT);
    EXPECT_EQ(list(d), (std::vector<std::string>{".", "..", "f", "sub"}));
    EXPECT_EQ(v().getattr(kRoot).nlink, 3U);
    // Каталог в другой каталог: nlink обоих родителей меняется.
    const int64_t other = env.mkdir("other");
    v().rename(d, bytes_of("sub"), other, bytes_of("moved"), 0);
    EXPECT_EQ(v().getattr(d).nlink, 2U);
    EXPECT_EQ(v().getattr(other).nlink, 3U);
    EXPECT_EQ(v().lookup(other, bytes_of("moved")).ino, sub);
    // ".." перенесённого каталога указывает на нового родителя.
    const uint64_t dh = v().opendir(sub);
    const auto entries = v().readdir(dh, 0, 10, false);
    v().releasedir(dh);
    ASSERT_GE(entries.size(), 2U);
    EXPECT_EQ(str_of(entries[1].name), "..");
    EXPECT_EQ(entries[1].ino, other);
}

TEST_F(VfsNamespace, RenameOverOpenFileDetachesIt) {
    const int64_t a = env.mkfile("a");
    env.write(a, 0, bytes_of("new"));
    const Created old = v().create(kRoot, bytes_of("old"), 0644, O_RDWR, 1000, 1000);
    env.write(old.attr.ino, 0, bytes_of("old data"));
    v().fsync(old.attr.ino);
    v().rename(kRoot, bytes_of("a"), kRoot, bytes_of("old"), 0);
    EXPECT_EQ(v().lookup(kRoot, bytes_of("old")).ino, a);
    // Открытый файл живёт без имени.
    EXPECT_EQ(str_of(v().read(old.attr.ino, 0, 100)), "old data");
    EXPECT_EQ(v().getattr(old.attr.ino).nlink, 0U);
    v().release(old.fh);
    EXPECT_TRUE(env.inode(old.attr.ino));  // ядро ещё помнит inode (lookup от create)
    v().forget(old.attr.ino, 1);
    EXPECT_FALSE(env.inode(old.attr.ino));
    EXPECT_TRUE(env.accounting_ok());
}

TEST_F(VfsNamespace, ReaddirOrderAndCursor) {
    for (const char* n : {"c", "a", "e", "b", "d"}) {
        env.mkfile(n);
    }
    EXPECT_EQ(list(kRoot), (std::vector<std::string>{".", "..", "a", "b", "c", "d", "e"}));
    EXPECT_EQ(list(kRoot, 2), (std::vector<std::string>{".", "..", "a", "b", "c", "d", "e"}));

    // Изменения каталога между вызовами: курсор по имени.
    const uint64_t dh = v().opendir(kRoot);
    auto part = v().readdir(dh, 0, 4, false);  // ., .., a, b
    ASSERT_EQ(part.size(), 4U);
    EXPECT_EQ(str_of(part[3].name), "b");
    EXPECT_EQ(part[3].off, 4U);
    v().unlink(kRoot, bytes_of("c"));  // ещё не отдан — пропадёт
    v().unlink(kRoot, bytes_of("a"));  // уже отдан
    env.mkfile("bb");                  // после курсора — появится
    env.mkfile("0");                   // до курсора — не появится
    part = v().readdir(dh, 4, 100, false);
    std::vector<std::string> names;
    names.reserve(part.size());
    for (const auto& e : part) {
        names.push_back(str_of(e.name));
    }
    EXPECT_EQ(names, (std::vector<std::string>{"bb", "d", "e"}));
    EXPECT_EQ(part.front().off, 5U);
    // seekdir назад по номеру: продолжение после "a" (номер 3).
    part = v().readdir(dh, 3, 1, false);
    ASSERT_EQ(part.size(), 1U);
    EXPECT_EQ(str_of(part[0].name), "b");
    // С начала.
    part = v().readdir(dh, 0, 1, false);
    EXPECT_EQ(str_of(part[0].name), ".");
    // Номер за концом — пусто.
    EXPECT_TRUE(v().readdir(dh, 100, 10, false).empty());
    v().releasedir(dh);
    EXPECT_EQ(err_of([&] { v().readdir(dh, 0, 10, false); }), EBADF);
    EXPECT_EQ(err_of([&] { v().opendir(v().lookup(kRoot, bytes_of("d")).ino); }), ENOTDIR);
}

TEST_F(VfsNamespace, ReaddirPlusGivesAttributes) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, bytes_of("12345"));
    env.mkdir("d");
    const uint64_t dh = v().opendir(kRoot);
    const auto entries = v().readdir(dh, 0, 10, true);
    v().releasedir(dh);
    ASSERT_EQ(entries.size(), 4U);
    for (const auto& e : entries) {
        EXPECT_EQ(meta::test::must(e.attr).ino, e.ino);
    }
    EXPECT_EQ(entries[0].ino, kRoot);
    EXPECT_EQ(entries[1].ino, kRoot);  // ".." корня — сам корень
    EXPECT_EQ(str_of(entries[3].name), "f");
    EXPECT_EQ(meta::test::must(entries[3].attr).size, 5);  // размер из overlay
    EXPECT_EQ(entries[2].kind, meta::InodeKind::dir);
}

// Находка 3: lookup и readdirplus берут dirty_attrs до снимка индекса. Писатель непрерывно
// растит файл с fsync (применение залитого чанка: индекс, потом очистка dirty_attrs), читатель
// видит размер, который никогда не уменьшается. Окно гонки узкое, поэтому тест — стресс:
// детерминированно воспроизвести его без хуков между фазами нельзя.
TEST_F(VfsNamespace, LookupAndReaddirPlusNeverShowStaleSize) {
    for (int i = 0; i < 4000; ++i) {  // длинный каталог растягивает снимок readdirplus
        env.mkfile("a" + std::to_string(1000 + i));
    }
    const int64_t f = env.mkfile("zzz");
    std::atomic<bool> stop{false};
    std::atomic<int64_t> committed{0};  // размер, о котором fsync уже вернулся
    std::thread writer([&] {
        for (int64_t i = 1; i <= 250 && !stop.load(); ++i) {
            const Bytes data = pattern(8192, static_cast<uint64_t>(i));
            v().write(f, static_cast<uint64_t>((i - 1) * 8192), data);
            v().fsync(f);
            committed.store(i * 8192);
        }
        stop.store(true);
    });
    int stale = 0;
    int64_t seen = 0;
    while (!stop.load()) {
        const int64_t floor = committed.load();
        const Attr a = v().lookup(kRoot, bytes_of("zzz"));
        v().forget(f, 1);
        if (a.size < floor || a.size < seen) {
            ++stale;
        }
        seen = std::max(seen, a.size);
        const uint64_t dh = v().opendir(kRoot);
        uint64_t off = 0;
        for (;;) {
            const auto part = v().readdir(dh, off, 10'000, true);
            if (part.empty()) {
                break;
            }
            for (const DirEntry& e : part) {
                off = e.off;
                if (e.ino == f) {
                    const int64_t size = meta::test::must(e.attr).size;
                    if (size < seen) {
                        ++stale;
                    }
                    seen = std::max(seen, size);
                }
            }
        }
        v().releasedir(dh);
    }
    writer.join();
    EXPECT_EQ(stale, 0);
}

TEST_F(VfsNamespace, SetattrModeOwnerTimes) {
    const int64_t f = env.mkfile("f");
    const Attr before = v().getattr(f);
    SetAttr s;
    s.mode = 0100755;  // тип отбрасывается
    s.uid = 42;
    s.gid = 43;
    s.atime_ns = 5'000'000'000;
    s.mtime_ns = 6'000'000'000;
    Attr a = v().setattr(f, s);
    EXPECT_EQ(a.mode, 0755U);
    EXPECT_EQ(a.uid, 42U);
    EXPECT_EQ(a.gid, 43U);
    EXPECT_EQ(a.atime_ns, 5'000'000'000);
    EXPECT_EQ(a.mtime_ns, 6'000'000'000);
    EXPECT_GE(a.ctime_ns, before.ctime_ns);

    // mtime грязного файла живёт в dirty_attrs и переживает заливку.
    env.write(f, 0, bytes_of("data"));
    SetAttr t;
    t.mtime_ns = 7'000'000'000;
    a = v().setattr(f, t);
    EXPECT_EQ(a.mtime_ns, 7'000'000'000);
    EXPECT_EQ(a.size, 4);
    v().fsync(f);
    EXPECT_EQ(v().getattr(f).mtime_ns, 7'000'000'000);
    EXPECT_EQ(env.must_inode(f).mtime_ns, 7'000'000'000);

    SetAttr now;
    now.mtime_now = true;
    now.atime_now = true;
    a = v().setattr(f, now);
    EXPECT_GT(a.mtime_ns, 7'000'000'000);
    EXPECT_GT(a.atime_ns, 5'000'000'000);

    const int64_t d = env.mkdir("d");
    SetAttr sz;
    sz.size = 0;
    EXPECT_EQ(err_of([&] { v().setattr(d, sz); }), EISDIR);
    sz.size = -1;
    EXPECT_EQ(err_of([&] { v().setattr(f, sz); }), EINVAL);
}

TEST_F(VfsNamespace, OpenTruncAndDirectoryChecks) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, bytes_of("hello"));
    const uint64_t fh = v().open(f, O_RDWR | O_TRUNC);
    EXPECT_EQ(v().getattr(f).size, 0);
    v().release(fh);
    const uint64_t ro = v().open(f, O_RDONLY | O_TRUNC);  // без записи O_TRUNC не действует
    v().release(ro);
    const int64_t d = env.mkdir("d");
    EXPECT_EQ(err_of([&] { v().open(d, O_WRONLY); }), EISDIR);
    EXPECT_EQ(err_of([&] { v().read(d, 0, 10); }), EISDIR);
    EXPECT_EQ(err_of([&] { v().write(d, 0, bytes_of("x")); }), EISDIR);
    EXPECT_EQ(err_of([&] { v().open(9999, O_RDONLY); }), ENOENT);
}

TEST_F(VfsNamespace, StatfsAndFsyncdir) {
    const StatFs s = v().statfs();
    EXPECT_EQ(s.namemax, 255U);
    EXPECT_EQ(s.bsize, 4096U);
    EXPECT_EQ(s.blocks, (uint64_t{1} << 50U) / 4096);
    EXPECT_EQ(s.bfree, s.blocks);
    const int64_t f = env.mkfile("f");
    env.write(f, 0, pattern(10000, 3));
    v().fsync(f);
    EXPECT_LT(v().statfs().bfree, s.blocks);

    const uint64_t commits = env.committer.commits.load();
    v().fsyncdir(kRoot);
    EXPECT_EQ(env.committer.commits.load(), commits + 1);
    env.committer.fail_commit = true;
    EXPECT_EQ(err_of([&] { v().fsyncdir(kRoot); }), EIO);
}

TEST_F(VfsNamespace, ReadOnlyRejectsChanges) {
    const int64_t f = env.mkfile("f");
    env.write(f, 0, bytes_of("data"));
    env.mkdir("d");
    env.committer.ro = true;
    EXPECT_EQ(err_of([&] { env.mkfile("g"); }), EROFS);
    EXPECT_EQ(err_of([&] { env.mkdir("e"); }), EROFS);
    EXPECT_EQ(err_of([&] { v().write(f, 0, bytes_of("x")); }), EROFS);
    EXPECT_EQ(err_of([&] { v().unlink(kRoot, bytes_of("f")); }), EROFS);
    EXPECT_EQ(err_of([&] { v().rmdir(kRoot, bytes_of("d")); }), EROFS);
    EXPECT_EQ(err_of([&] { v().rename(kRoot, bytes_of("f"), kRoot, bytes_of("g"), 0); }), EROFS);
    EXPECT_EQ(err_of([&] { v().symlink(kRoot, bytes_of("l"), bytes_of("t"), 0, 0); }), EROFS);
    EXPECT_EQ(err_of([&] { v().open(f, O_WRONLY); }), EROFS);
    SetAttr s;
    s.mode = 0600;
    EXPECT_EQ(err_of([&] { v().setattr(f, s); }), EROFS);
    // Чтение работает, в том числе грязных данных.
    EXPECT_EQ(str_of(v().read(f, 0, 10)), "data");
    const uint64_t fh = v().open(f, O_RDONLY);
    v().release(fh);
    // После CAS-конфликта закоммитить нельзя: fsync, fsyncdir, sync и flush_all — EIO
    // (SPEC 7.3), а не молчаливый успех.
    const uint64_t commits = env.committer.commits.load();
    EXPECT_EQ(err_of([&] { v().fsync(f); }), EIO);
    EXPECT_EQ(err_of([&] { v().fsyncdir(kRoot); }), EIO);
    EXPECT_EQ(err_of([&] { v().sync(); }), EIO);
    EXPECT_EQ(err_of([&] { v().flush_all(); }), EIO);
    EXPECT_EQ(env.committer.commits.load(), commits);

    // Опция read_only — то же самое для записи; но fsync read-only дескриптора — не ошибка:
    // это явный выбор пользователя, писать нечего.
    env.committer.ro = false;
    VfsOptions o = fast_options();
    o.read_only = true;
    Vfs& r = env.restart(o);
    EXPECT_EQ(err_of([&] { r.write(f, 0, bytes_of("x")); }), EROFS);
    EXPECT_EQ(str_of(r.read(f, 0, 10)), "data");
    EXPECT_EQ(err_of([&] { r.fsync(f); }), 0);
    EXPECT_EQ(err_of([&] { r.fsyncdir(kRoot); }), 0);
    EXPECT_EQ(err_of([&] { r.flush_all(); }), 0);
}

// Если коммитер стал read-only во время самого коммита (CAS-конфликт), это тоже EIO, а не EROFS.
TEST_F(VfsNamespace, CommitFailureIsEioEvenIfCommitterTurnedReadOnly) {
    env.committer.fail_commit = true;
    env.committer.ro = false;
    EXPECT_EQ(err_of([&] { v().fsyncdir(kRoot); }), EIO);
    EXPECT_EQ(err_of([&] { v().sync(); }), EIO);
}

TEST_F(VfsNamespace, NamespaceSurvivesRestart) {
    const int64_t d = env.mkdir("d");
    const int64_t f = env.mkfile("f", d);
    env.write(f, 0, bytes_of("abc"));
    v().symlink(d, bytes_of("l"), bytes_of("f"), 0, 0);
    Vfs& r = env.restart();
    EXPECT_EQ(r.lookup(kRoot, bytes_of("d")).ino, d);
    EXPECT_EQ(r.lookup(d, bytes_of("f")).ino, f);
    EXPECT_EQ(str_of(r.readlink(r.lookup(d, bytes_of("l")).ino)), "f");
    EXPECT_EQ(str_of(env.read_all(f)), "abc");
}

}  // namespace
}  // namespace telega::vfs::test
