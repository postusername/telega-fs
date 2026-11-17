#pragma once

// Общая обвязка тестов Vfs: index.sqlite и state.sqlite во временном каталоге, LocalDirStore,
// фейковый Committer, «перезапуск» (Vfs, MetaStore и StateStore создаются заново на тех же
// файлах).

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../meta/test_util.h"
#include "common/bytes.h"
#include "crypto/keys.h"
#include "meta/committer.h"
#include "meta/errors.h"
#include "meta/index_db.h"
#include "meta/index_ops.h"
#include "meta/state_db.h"
#include "store/local_dir_store.h"
#include "vfs/vfs.h"

namespace telega::vfs::test {

using meta::test::bytes_of;
using meta::test::TempDir;
using std::chrono::milliseconds;

inline constexpr int64_t kChunk = 1 << 20;  // минимально допустимый chunk_size
inline constexpr int64_t kRoot = meta::kRootIno;

// Фейковый Committer: считает вызовы, поколение задаётся тестом.
class FakeCommitter final : public meta::Committer {
  public:
    void commit_now() override {
        commits.fetch_add(1);
        if (fail_commit.load()) {
            throw meta::CommitError("injected commit failure");
        }
    }
    void chunk_uploaded() override { uploads.fetch_add(1); }
    [[nodiscard]] uint64_t base_generation() const override { return generation.load(); }
    [[nodiscard]] bool read_only() const override { return ro.load(); }

    std::atomic<uint64_t> commits{0};
    std::atomic<uint64_t> uploads{0};
    std::atomic<uint64_t> generation{1};
    std::atomic<bool> ro{false};
    std::atomic<bool> fail_commit{false};
};

inline std::string str_of(ByteSpan b) {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

// Детерминированные псевдослучайные данные.
// Генерация по 8 байт: под TSan каждая запись инструментирована, побайтно это медленно.
inline Bytes pattern(size_t n, uint64_t seed) {
    Bytes out(n);
    uint64_t x = seed * 0x9E3779B97F4A7C15ULL + 1;
    for (size_t i = 0; i < n; i += sizeof(x)) {
        x ^= x << 13U;
        x ^= x >> 7U;
        x ^= x << 17U;
        std::memcpy(out.data() + i, &x, std::min(sizeof(x), n - i));
    }
    return out;
}

// errno исключения, брошенного fn, или 0.
template <typename F>
int err_of(F&& fn) {
    try {
        fn();
    } catch (const std::exception& e) {
        return to_errno(e);
    }
    return 0;
}

inline VfsOptions fast_options() {
    VfsOptions o;
    o.seal_delay = milliseconds(3'600'000);  // таймер не мешает: запечатывают тесты сами
    o.upload_concurrency = 2;
    o.download_concurrency = 4;
    o.plain_cache = uint64_t{8} << 20U;
    o.read_timeout = milliseconds(2'000);
    o.retry_initial = milliseconds(5);
    o.retry_max = milliseconds(20);
    return o;
}

class VfsEnv {
  public:
    explicit VfsEnv(int64_t inline_max = 4096, int64_t inline_budget = 16 << 20) {
        meta::InitParams p;
        for (size_t i = 0; i < p.fs_uuid.size(); ++i) {
            p.fs_uuid[i] = static_cast<std::byte>(0x30 + i);
        }
        p.chunk_size = kChunk;
        p.inline_max = inline_max;
        p.inline_budget = inline_budget;
        p.pack_threshold = 1 << 16;
        p.pack_target = kChunk;
        p.root_uid = 1000;
        p.root_gid = 1000;
        p.now_ns = 1'000'000'000;
        meta::MetaStore::create(index_path(), p);
        meta::StateStore::create(state_path());
        crypto::init();
        keys = crypto::derive_subkeys(crypto::Key::random());
        store = std::make_unique<store::LocalDirStore>(dir.path() / "store");
        store->set_crash_handler([] { throw store::CrashInjected(); });
        open_dbs();
    }
    VfsEnv(const VfsEnv&) = delete;
    VfsEnv& operator=(const VfsEnv&) = delete;
    VfsEnv(VfsEnv&&) = delete;
    VfsEnv& operator=(VfsEnv&&) = delete;
    ~VfsEnv() {
        vfs.reset();
        state.reset();
        meta.reset();
    }

    [[nodiscard]] std::filesystem::path index_path() const { return dir.path() / "index.sqlite"; }
    [[nodiscard]] std::filesystem::path state_path() const { return dir.path() / "state.sqlite"; }
    [[nodiscard]] std::filesystem::path staging() const { return dir.path() / "staging"; }
    [[nodiscard]] std::filesystem::path tmp() const { return dir.path() / "tmp"; }

    void open_dbs() {
        meta = std::make_unique<meta::MetaStore>(index_path());
        state = std::make_unique<meta::StateStore>(state_path());
    }

    Vfs& start(VfsOptions o = fast_options()) {
        options = o;
        vfs = std::make_unique<Vfs>(*meta, *state, *store, committer, keys,
                                    VfsPaths{staging(), tmp()}, o);
        return *vfs;
    }

    // «Падение» и перезапуск: Vfs уничтожается без заливки, базы открываются заново.
    // Как монтирование (SPEC 7.2, шаг 8), удаляет отсоединённые inode.
    Vfs& restart() { return restart(options); }
    Vfs& restart(VfsOptions o) {
        vfs.reset();
        state.reset();
        meta.reset();
        open_dbs();
        meta->write([&](meta::Db& db) {
            for (const int64_t ino : meta::list_detached(db)) {
                meta::delete_inode(db, ino, static_cast<int64_t>(committer.generation.load()));
            }
        });
        return start(o);
    }

    // --- удобные обёртки ---

    // NOLINTNEXTLINE(modernize-use-nodiscard): тестам часто нужен только сам файл
    int64_t mkfile(std::string_view name, int64_t parent = kRoot) const {
        const Created c = vfs->create(parent, bytes_of(name), 0644, O_RDWR, 1000, 1000);
        vfs->release(c.fh);
        return c.attr.ino;
    }
    // NOLINTNEXTLINE(modernize-use-nodiscard): тестам часто нужен только сам каталог
    int64_t mkdir(std::string_view name, int64_t parent = kRoot) const {
        return vfs->mkdir(parent, bytes_of(name), 0755, 1000, 1000).ino;
    }
    void write(int64_t ino, uint64_t off, ByteSpan data) const {
        ASSERT_EQ(vfs->write(ino, off, data), data.size());
    }
    [[nodiscard]] Bytes read_all(int64_t ino) const {
        const Attr a = vfs->getattr(ino);
        Bytes out = vfs->read(ino, 0, static_cast<size_t>(a.size) + 10);
        EXPECT_EQ(static_cast<int64_t>(out.size()), a.size);
        return out;
    }
    [[nodiscard]] std::optional<meta::Inode> inode(int64_t ino) const {
        return meta->read([&](meta::Db& db) { return meta::get_inode(db, ino); });
    }
    // Inode, который обязан существовать.
    [[nodiscard]] meta::Inode must_inode(int64_t ino) const { return meta::test::must(inode(ino)); }
    [[nodiscard]] std::vector<meta::Extent> extents(int64_t ino) const {
        return meta->read([&](meta::Db& db) { return meta::list_extents(db, ino); });
    }
    [[nodiscard]] std::optional<Bytes> inline_data(int64_t ino) const {
        return meta->read([&](meta::Db& db) { return meta::get_inline(db, ino); });
    }
    [[nodiscard]] int64_t live_chunks() const {
        return meta->read([](meta::Db& db) {
            meta::Stmt st{db, "SELECT COUNT(*) FROM chunks WHERE refcount > 0"};
            st.step();
            return st.column_int64(0);
        });
    }
    ::testing::AssertionResult accounting_ok() const {
        return meta->read([](meta::Db& db) { return meta::test::chunk_accounting_consistent(db); });
    }
    [[nodiscard]] size_t staging_files() const {
        size_t n = 0;
        for (const auto& de : std::filesystem::directory_iterator(staging())) {
            (void)de;
            ++n;
        }
        return n;
    }

    TempDir dir;
    crypto::SubKeys keys;
    FakeCommitter committer;
    std::unique_ptr<store::LocalDirStore> store;
    std::unique_ptr<meta::MetaStore> meta;
    std::unique_ptr<meta::StateStore> state;
    VfsOptions options;
    std::unique_ptr<Vfs> vfs;
};

}  // namespace telega::vfs::test
