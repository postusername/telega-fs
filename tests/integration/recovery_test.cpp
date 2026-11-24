// Интеграционные тесты восстановления (SPEC 7.2, 12.3, 16 M4) на LocalDirStore: потеря головы и
// mount --force-local, fsck и recover --export-dirty настоящим бинарником telega-fs. Без
// /dev/fuse пропускаются (GTEST_SKIP).

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sodium.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <string_view>

#include "harness.h"

namespace telega::itest {
namespace {

using namespace std::chrono_literals;

constexpr uint64_t kMiB = uint64_t{1024} * 1024;

std::string sha256_of(const fs::path& path) {
    const std::vector<uint8_t> data = read_file(path);
    std::array<unsigned char, crypto_hash_sha256_BYTES> d{};
    crypto_hash_sha256(d.data(), data.data(), data.size());
    std::string hex;
    constexpr std::string_view kHex = "0123456789abcdef";
    for (const unsigned char b : d) {
        hex += kHex[b >> 4U];
        hex += kHex[b & 0xFU];
    }
    return hex;
}

class RecoveryTest : public ::testing::Test {
  protected:
    void SetUp() override {
        if (const std::string why = fuse_skip_reason(); !why.empty()) {
            GTEST_SKIP() << why;
        }
        ASSERT_GE(sodium_init(), 0);
    }
    void TearDown() override {
        if (HasFailure() && env_) {
            std::cerr << "---- telega-fs.log ----\n" << env_->log_text() << '\n';
        }
    }

    Env& start() {
        env_ = std::make_unique<Env>(ProfileSpec{});
        env_->init();
        return *env_;
    }

    // Файлы всех классов хранения: инлайн, пакеты, позиционные чанки (chunk_size 1 МиБ).
    static void write_files(Mount& m) {
        write_pattern_file(m.path("big.bin"), 3 * kMiB + 17, 7);
        write_file(m.path("tiny.txt"), {'t', 'i', 'n', 'y'});
        fs::create_directory(m.path("dir"));
        for (int i = 0; i < 20; ++i) {
            write_file(m.path("dir/small" + std::to_string(i)),
                       pattern(0, 2000 + static_cast<uint64_t>(i), 100 + static_cast<uint64_t>(i)));
        }
    }
    static std::map<std::string, std::string> hashes(Mount& m) {
        std::map<std::string, std::string> out;
        for (const std::string name : {"big.bin", "tiny.txt"}) {
            out[name] = sha256_of(m.path(name));
        }
        for (int i = 0; i < 20; ++i) {
            const std::string name = "dir/small" + std::to_string(i);
            out[name] = sha256_of(m.path(name));
        }
        return out;
    }

    // Голова LocalDirStore: последняя строка pinned.
    [[nodiscard]] int64_t head_id() const {
        std::ifstream in(env_->local_dir() / "pinned");
        int64_t id = 0;
        int64_t cur = 0;
        while (in >> cur) {
            id = cur;
        }
        return id;
    }

    std::unique_ptr<Env> env_;
};

// Критерий M4 (SPEC 16): закреп снят, сообщение головы удалено; mount отказывает с подсказкой,
// mount --ro показывает локальный индекс, mount --force-local возвращает всё на место.
TEST_F(RecoveryTest, LostHeadIsRefusedThenForceLocalBringsEverythingBack) {
    Env& e = start();
    std::map<std::string, std::string> want;
    {
        auto m = e.mount();
        write_files(*m);
        want = hashes(*m);
        ASSERT_TRUE(m->unmount().ok());
    }
    const int64_t head = head_id();
    ASSERT_GT(head, 0);
    {  // снять закреп и удалить сообщение головы
        write_file(e.local_dir() / "pinned", {});
        ASSERT_TRUE(fs::remove(e.local_dir() / "msgs" / std::to_string(head)));
    }

    const RunResult refused = e.run({"mount", e.mountpoint().string()});
    EXPECT_NE(refused.exit.code, 0);
    EXPECT_NE(refused.output.find("--force-local"), std::string::npos) << refused.output;
    EXPECT_NE(refused.output.find("--ro"), std::string::npos) << refused.output;
    EXPECT_FALSE(is_mounted(e.mountpoint()));

    {
        auto ro = e.mount({"--ro"});
        EXPECT_EQ(hashes(*ro), want);
        ASSERT_TRUE(ro->unmount().ok());
    }
    {
        auto m = e.mount({"--force-local"});
        EXPECT_EQ(hashes(*m), want);
        write_file(m->path("after.txt"), {'o', 'k'});
        ASSERT_TRUE(m->unmount().ok());
    }
    // Голова новая и обычная: монтируется без флагов, другая машина её видит.
    {
        auto m = e.mount();
        EXPECT_EQ(hashes(*m), want);
        EXPECT_EQ(read_file(m->path("after.txt")), (std::vector<uint8_t>{'o', 'k'}));
        ASSERT_TRUE(m->unmount().ok());
    }
    e.new_machine();
    auto m = e.mount();
    EXPECT_EQ(hashes(*m), want);
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(RecoveryTest, LostHeadWithoutLocalIndexHintsAtInit) {
    Env& e = start();
    {
        auto m = e.mount();
        write_file(m->path("a"), {'a'});
        ASSERT_TRUE(m->unmount().ok());
    }
    write_file(e.local_dir() / "pinned", {});
    e.new_machine();  // новый data-каталог: локального индекса нет
    const RunResult r = e.run({"mount", e.mountpoint().string()});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("init"), std::string::npos) << r.output;
    EXPECT_EQ(r.output.find("--force-local"), std::string::npos) << r.output;
}

TEST_F(RecoveryTest, FsckReportsAndRepairsAndNeverTouchesTheChannel) {
    Env& e = start();
    {
        auto m = e.mount();
        write_files(*m);
        ASSERT_TRUE(m->unmount().ok());
    }
    RunResult r = e.run({"fsck"});
    EXPECT_EQ(r.exit.code, 0) << r.output;
    EXPECT_NE(r.output.find("OK"), std::string::npos) << r.output;
    r = e.run({"fsck", "--verify-data"});
    EXPECT_EQ(r.exit.code, 0) << r.output;

    // Пропало сообщение чанка big.bin: отчёт находит, код выхода 2, --repair помечает и печатает
    // файл.
    std::vector<fs::path> chunk_messages;
    for (const auto& entry : fs::directory_iterator(e.local_dir() / "msgs")) {
        if (fs::file_size(entry.path()) > static_cast<uint64_t>(kMiB)) {
            chunk_messages.push_back(entry.path());
        }
    }
    ASSERT_FALSE(chunk_messages.empty());
    const std::string victim = chunk_messages.front().filename().string();
    const auto before = [&] {
        std::vector<std::string> names;
        for (const auto& entry : fs::directory_iterator(e.local_dir() / "msgs")) {
            names.push_back(entry.path().filename().string());
        }
        std::sort(names.begin(), names.end());
        return names;
    };
    fs::remove(chunk_messages.front());
    const auto channel_before = before();

    r = e.run({"fsck"});
    EXPECT_EQ(r.exit.code, 2) << r.output;
    EXPECT_NE(r.output.find("message-missing"), std::string::npos) << r.output;

    r = e.run({"fsck", "--repair"});
    EXPECT_NE(r.output.find("/big.bin"), std::string::npos) << r.output;
    EXPECT_EQ(r.exit.code, 0) << r.output;
    // Коммит новой головы добавил блоб индекса, но ни одного сообщения не удалено.
    const auto channel_after = before();
    for (const std::string& name : channel_before) {
        EXPECT_NE(std::find(channel_after.begin(), channel_after.end(), name), channel_after.end())
            << name;
    }
    r = e.run({"fsck", "--verify-data"});
    EXPECT_EQ(r.exit.code, 0) << r.output;
    EXPECT_NE(r.output.find("/big.bin"), std::string::npos) << r.output;  // известный битый чанк
    (void)victim;

    // Чтение потерянного диапазона даёт EIO, остальные файлы читаются.
    auto m = e.mount();
    EXPECT_ANY_THROW((void)read_file(m->path("big.bin")));
    EXPECT_NO_THROW((void)read_file(m->path("dir/small3")));
    EXPECT_EQ(read_file(m->path("tiny.txt")), (std::vector<uint8_t>{'t', 'i', 'n', 'y'}));
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(RecoveryTest, FsckRefusesWhileMounted) {
    Env& e = start();
    auto m = e.mount();
    const RunResult r = e.run({"fsck"});
    EXPECT_NE(r.exit.code, 0);
    EXPECT_NE(r.output.find("in use"), std::string::npos) << r.output;
    EXPECT_TRUE(m->unmount().ok());
}

// recover --export-dirty после падения процесса: незаливавшиеся данные overlay выгружаются файлами.
TEST_F(RecoveryTest, ExportDirtyAfterACrashWritesTheOverlayFiles) {
    Env& e = start();
    {
        auto m = e.mount();
        write_pattern_file(m->path("base.bin"), 2 * kMiB, 1);
        ASSERT_TRUE(m->unmount().ok());
    }
    auto m = e.mount();
    const File keep = File::open(m->path("dirty.bin"), O_RDWR | O_CREAT);
    const std::vector<uint8_t> data = pattern(0, 3000, 55);
    keep.pwrite_all(data.data(), data.size(), 0);
    // Падение без fsync и без размонтирования: overlay остаётся на диске.
    m->kill_hard();
    const fs::path out = e.root() / "export";
    const RunResult r = e.run({"recover", "--export-dirty", out.string()});
    EXPECT_EQ(r.exit.code, 0) << r.output;
    EXPECT_EQ(read_file(out / "dirty.bin"), data) << r.output;
}

}  // namespace
}  // namespace telega::itest
